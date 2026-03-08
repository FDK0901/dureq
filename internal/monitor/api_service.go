package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
)

// Dispatcher is the subset of *dispatcher.Dispatcher used by the service layer.
type Dispatcher interface {
	Dispatch(ctx context.Context, job *types.Job, attempt int) error
	PublishEvent(event types.JobEvent)
}

// ApiError is a service-layer error that carries an HTTP status code.
type ApiError struct {
	Msg        string
	StatusCode int
}

func (e *ApiError) Error() string { return e.Msg }

// APIService holds all transport-agnostic business logic for the monitoring API.
type APIService struct {
	store      *store.RedisStore
	dispatcher Dispatcher
	logger     gochainedlog.Logger
	hub        Hub
	hubCancel  context.CancelFunc

	syncRetryStats SyncRetryStatsFunc
}

// NewAPIService creates a new APIService, starting the hub's Redis subscription.
func NewAPIService(s *store.RedisStore, disp Dispatcher, logger gochainedlog.Logger) *APIService {
	if logger == nil {
		logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}

	h := newHub(logger)
	hubCtx, hubCancel := context.WithCancel(context.Background())
	go h.SubscribeRedis(hubCtx, s.Client(), store.EventsPubSubChannel(s.Prefix()))

	return &APIService{
		store:      s,
		dispatcher: disp,
		logger:     logger,
		hub:        h,
		hubCancel:  hubCancel,
	}
}

// --- Accessors ---

func (a *APIService) Store() *store.RedisStore    { return a.store }
func (a *APIService) Hub() Hub                    { return a.hub }
func (a *APIService) Logger() gochainedlog.Logger { return a.logger }

func (a *APIService) SetSyncRetryStatsFunc(fn SyncRetryStatsFunc) {
	a.syncRetryStats = fn
}

func (a *APIService) Shutdown() {
	if a.hubCancel != nil {
		a.hubCancel()
	}
}

// --- Helper: signal cancel to active runs ---

func (a *APIService) signalCancelActiveRuns(ctx context.Context, jobID string) {
	runs, _ := a.store.ListActiveRunsByJobID(ctx, jobID)
	for _, run := range runs {
		a.store.Client().Do(ctx, a.store.Client().B().Publish().Channel(a.store.CancelChannel()).Message(run.ID).Build())
	}
}

// ===================== Jobs =====================

func (a *APIService) ListJobs(ctx context.Context, filter store.JobFilter) ([]types.Job, int, error) {
	return a.store.ListJobsPaginated(ctx, filter)
}

func (a *APIService) GetJob(ctx context.Context, jobID string) (*types.Job, error) {
	job, _, err := a.store.GetJob(ctx, jobID)
	if err != nil {
		return nil, &ApiError{Msg: "job not found: " + err.Error(), StatusCode: http.StatusNotFound}
	}
	return job, nil
}

func (a *APIService) CancelJob(ctx context.Context, jobID string) error {
	job, rev, err := a.store.GetJob(ctx, jobID)
	if err != nil {
		return &ApiError{Msg: "job not found: " + err.Error(), StatusCode: http.StatusNotFound}
	}
	if job.Status.IsTerminal() {
		return &ApiError{Msg: "job is already in terminal state", StatusCode: http.StatusBadRequest}
	}

	now := time.Now()
	job.Status = types.JobStatusCancelled
	job.UpdatedAt = now

	if _, err := a.store.UpdateJob(ctx, job, rev); err != nil {
		return err
	}

	a.store.DeleteSchedule(ctx, jobID)
	a.signalCancelActiveRuns(ctx, jobID)

	a.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventJobCancelled,
		JobID:     jobID,
		Timestamp: now,
	})
	return nil
}

func (a *APIService) RetryJob(ctx context.Context, jobID string) error {
	job, rev, err := a.store.GetJob(ctx, jobID)
	if err != nil {
		return &ApiError{Msg: "job not found: " + err.Error(), StatusCode: http.StatusNotFound}
	}

	if !job.Status.IsTerminal() {
		return &ApiError{Msg: "job is in non-terminal state: " + string(job.Status), StatusCode: http.StatusConflict}
	}

	now := time.Now()
	prevStatus := job.Status
	job.Status = types.JobStatusPending
	job.Attempt = 0
	job.LastError = nil
	job.UpdatedAt = now

	newRev, err := a.store.UpdateJob(ctx, job, rev)
	if err != nil {
		return err
	}

	if err := a.dispatcher.Dispatch(ctx, job, 0); err != nil {
		// Rollback: restore previous status.
		job.Status = prevStatus
		job.UpdatedAt = time.Now()
		a.store.UpdateJob(ctx, job, newRev)
		return err
	}

	a.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventJobRetrying,
		JobID:     jobID,
		Timestamp: now,
	})
	return nil
}

func (a *APIService) DeleteJob(ctx context.Context, jobID string) error {
	if err := a.store.DeleteJob(ctx, jobID); err != nil {
		return err
	}
	a.store.DeleteSchedule(ctx, jobID)
	return nil
}

func (a *APIService) ListJobEvents(ctx context.Context, filter store.EventFilter) ([]types.JobEvent, int, error) {
	return a.store.ListJobEvents(ctx, filter)
}

func (a *APIService) ListJobRuns(ctx context.Context, filter store.RunFilter) ([]types.JobRun, int, error) {
	return a.store.ListJobRuns(ctx, filter)
}

func (a *APIService) UpdateJobPayload(ctx context.Context, jobID string, payload json.RawMessage) error {
	if len(payload) == 0 {
		return &ApiError{Msg: "payload is required", StatusCode: http.StatusBadRequest}
	}

	job, rev, err := a.store.GetJob(ctx, jobID)
	if err != nil {
		return &ApiError{Msg: "job not found: " + err.Error(), StatusCode: http.StatusNotFound}
	}

	if job.Status.IsTerminal() || job.Status == types.JobStatusRunning {
		return &ApiError{Msg: "can only update payload of pending or scheduled jobs", StatusCode: http.StatusBadRequest}
	}

	job.Payload = payload
	job.UpdatedAt = time.Now()

	if _, err := a.store.UpdateJob(ctx, job, rev); err != nil {
		return err
	}
	return nil
}

// ===================== Schedules =====================

func (a *APIService) ListSchedules(ctx context.Context, filter store.ScheduleFilter) ([]types.ScheduleEntry, int, error) {
	return a.store.ListSchedulesPaginated(ctx, filter)
}

func (a *APIService) GetSchedule(ctx context.Context, jobID string) (*types.ScheduleEntry, error) {
	sched, _, err := a.store.GetSchedule(ctx, jobID)
	if err != nil {
		return nil, &ApiError{Msg: "schedule not found: " + err.Error(), StatusCode: http.StatusNotFound}
	}
	return sched, nil
}

// ===================== Nodes =====================

func (a *APIService) ListNodes(ctx context.Context) ([]*types.NodeInfo, error) {
	return a.store.ListNodes(ctx)
}

func (a *APIService) GetNode(ctx context.Context, nodeID string) (*types.NodeInfo, error) {
	node, err := a.store.GetNode(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, &ApiError{Msg: "node not found", StatusCode: http.StatusNotFound}
	}
	return node, nil
}

// ===================== Runs =====================

func (a *APIService) ListActiveRuns(ctx context.Context) ([]*types.JobRun, error) {
	return a.store.ListRuns(ctx)
}

func (a *APIService) GetRun(ctx context.Context, runID string) (*types.JobRun, error) {
	run, _, err := a.store.GetRun(ctx, runID)
	if err != nil {
		return nil, &ApiError{Msg: "run not found: " + err.Error(), StatusCode: http.StatusNotFound}
	}
	return run, nil
}

func (a *APIService) GetJobActiveRuns(ctx context.Context, jobID string) ([]*types.JobRun, error) {
	return a.store.ListActiveRunsByJobID(ctx, jobID)
}

// ===================== History =====================

func (a *APIService) ListHistoryRuns(ctx context.Context, filter store.RunFilter) ([]types.JobRun, int, error) {
	return a.store.ListJobRuns(ctx, filter)
}

func (a *APIService) ListHistoryEvents(ctx context.Context, filter store.EventFilter) ([]types.JobEvent, int, error) {
	return a.store.ListJobEvents(ctx, filter)
}

// ===================== DLQ =====================

func (a *APIService) ListDLQ(ctx context.Context, limit, offset int) ([]*types.WorkMessage, int, error) {
	msgs, err := a.store.ListDLQ(ctx, 1000)
	if err != nil {
		return nil, 0, err
	}
	total := len(msgs)
	if offset < len(msgs) {
		msgs = msgs[offset:]
	} else {
		msgs = nil
	}
	if limit > 0 && len(msgs) > limit {
		msgs = msgs[:limit]
	}
	return msgs, total, nil
}

// ===================== Workflows =====================

func (a *APIService) ListWorkflows(ctx context.Context, filter store.WorkflowFilter) ([]types.WorkflowInstance, int, error) {
	return a.store.ListWorkflowInstances(ctx, filter)
}

func (a *APIService) GetWorkflow(ctx context.Context, wfID string) (*types.WorkflowInstance, error) {
	wf, _, err := a.store.GetWorkflow(ctx, wfID)
	if err != nil {
		return nil, &ApiError{Msg: "workflow not found: " + err.Error(), StatusCode: http.StatusNotFound}
	}
	return wf, nil
}

func (a *APIService) CancelWorkflow(ctx context.Context, wfID string) error {
	const maxCASRetries = 5
	for attempt := 0; attempt < maxCASRetries; attempt++ {
		wf, rev, err := a.store.GetWorkflow(ctx, wfID)
		if err != nil {
			return &ApiError{Msg: "workflow not found: " + err.Error(), StatusCode: http.StatusNotFound}
		}
		if wf.Status.IsTerminal() {
			return &ApiError{Msg: "workflow is already in terminal state", StatusCode: http.StatusBadRequest}
		}

		now := time.Now()
		wf.Status = types.WorkflowStatusCancelled
		wf.CompletedAt = &now
		wf.UpdatedAt = now

		for name, state := range wf.Tasks {
			if !state.Status.IsTerminal() {
				state.Status = types.JobStatusCancelled
				state.FinishedAt = &now
				wf.Tasks[name] = state

				if state.JobID != "" {
					if childJob, childRev, err := a.store.GetJob(ctx, state.JobID); err == nil {
						if !childJob.Status.IsTerminal() {
							childJob.Status = types.JobStatusCancelled
							childJob.UpdatedAt = now
							a.store.UpdateJob(ctx, childJob, childRev)
						}
					}
					a.signalCancelActiveRuns(ctx, state.JobID)
				}
			}
		}

		if _, err := a.store.UpdateWorkflow(ctx, wf, rev); err != nil {
			if attempt < maxCASRetries-1 {
				continue
			}
			return err
		}

		a.dispatcher.PublishEvent(types.JobEvent{
			Type:      types.EventWorkflowCancelled,
			JobID:     wfID,
			Timestamp: now,
		})
		return nil
	}
	return fmt.Errorf("cancel workflow: max CAS retries exceeded")
}

func (a *APIService) RetryWorkflow(ctx context.Context, wfID string) error {
	wf, rev, err := a.store.GetWorkflow(ctx, wfID)
	if err != nil {
		return &ApiError{Msg: "workflow not found: " + err.Error(), StatusCode: http.StatusNotFound}
	}

	now := time.Now()
	wf.Attempt++
	wf.Status = types.WorkflowStatusRunning
	wf.UpdatedAt = now
	wf.CompletedAt = nil

	// Reset only failed/dead/cancelled tasks — keep completed tasks.
	for name, state := range wf.Tasks {
		if state.Status == types.JobStatusFailed || state.Status == types.JobStatusDead || state.Status == types.JobStatusCancelled {
			state.Status = types.JobStatusPending
			state.JobID = ""
			state.Error = nil
			state.StartedAt = nil
			state.FinishedAt = nil
			wf.Tasks[name] = state
		}
	}

	if _, err := a.store.UpdateWorkflow(ctx, wf, rev); err != nil {
		return err
	}

	a.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventWorkflowRetrying,
		JobID:     wfID,
		Attempt:   wf.Attempt,
		Timestamp: now,
	})
	return nil
}

// ===================== Batches =====================

func (a *APIService) ListBatches(ctx context.Context, filter store.BatchFilter) ([]types.BatchInstance, int, error) {
	return a.store.ListBatchInstances(ctx, filter)
}

func (a *APIService) GetBatch(ctx context.Context, batchID string) (*types.BatchInstance, error) {
	batch, _, err := a.store.GetBatch(ctx, batchID)
	if err != nil {
		return nil, &ApiError{Msg: "batch not found: " + err.Error(), StatusCode: http.StatusNotFound}
	}
	return batch, nil
}

func (a *APIService) GetBatchResults(ctx context.Context, batchID string) ([]*types.BatchItemResult, error) {
	return a.store.ListBatchItemResults(ctx, batchID)
}

func (a *APIService) GetBatchItemResult(ctx context.Context, batchID, itemID string) (*types.BatchItemResult, error) {
	result, err := a.store.GetBatchItemResult(ctx, batchID, itemID)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, &ApiError{Msg: "batch item result not found", StatusCode: http.StatusNotFound}
	}
	return result, nil
}

func (a *APIService) CancelBatch(ctx context.Context, batchID string) error {
	const maxCASRetries = 5
	for attempt := 0; attempt < maxCASRetries; attempt++ {
		batch, rev, err := a.store.GetBatch(ctx, batchID)
		if err != nil {
			return &ApiError{Msg: "batch not found: " + err.Error(), StatusCode: http.StatusNotFound}
		}
		if batch.Status.IsTerminal() {
			return &ApiError{Msg: "batch is already in terminal state", StatusCode: http.StatusBadRequest}
		}

		now := time.Now()
		batch.Status = types.WorkflowStatusCancelled
		batch.CompletedAt = &now
		batch.UpdatedAt = now

		for id, state := range batch.ItemStates {
			if !state.Status.IsTerminal() {
				state.Status = types.JobStatusCancelled
				state.FinishedAt = &now
				batch.ItemStates[id] = state

				if state.JobID != "" {
					if childJob, childRev, err := a.store.GetJob(ctx, state.JobID); err == nil {
						if !childJob.Status.IsTerminal() {
							childJob.Status = types.JobStatusCancelled
							childJob.UpdatedAt = now
							a.store.UpdateJob(ctx, childJob, childRev)
						}
					}
					a.signalCancelActiveRuns(ctx, state.JobID)
				}
			}
		}

		if _, err := a.store.UpdateBatch(ctx, batch, rev); err != nil {
			if attempt < maxCASRetries-1 {
				continue
			}
			return err
		}

		a.dispatcher.PublishEvent(types.JobEvent{
			Type:      types.EventBatchCancelled,
			JobID:     batchID,
			Timestamp: now,
		})
		return nil
	}
	return fmt.Errorf("cancel batch: max CAS retries exceeded")
}

func (a *APIService) RetryBatch(ctx context.Context, batchID string, retryFailedOnly bool) error {
	batch, rev, err := a.store.GetBatch(ctx, batchID)
	if err != nil {
		return &ApiError{Msg: "batch not found: " + err.Error(), StatusCode: http.StatusNotFound}
	}

	now := time.Now()
	batch.Attempt++
	batch.Status = types.WorkflowStatusRunning
	batch.UpdatedAt = now
	batch.CompletedAt = nil

	batch.FailedItems = 0
	batch.RunningItems = 0
	batch.PendingItems = 0
	batch.CompletedItems = 0

	for id, state := range batch.ItemStates {
		if retryFailedOnly && state.Status != types.JobStatusFailed {
			if state.Status == types.JobStatusCompleted {
				batch.CompletedItems++
			}
			continue
		}
		state.Status = types.JobStatusPending
		state.JobID = ""
		state.Error = nil
		state.StartedAt = nil
		state.FinishedAt = nil
		batch.ItemStates[id] = state
		batch.PendingItems++
	}
	batch.NextChunkIndex = 0

	// Reset onetime state if it hasn't completed (orchestrator will re-dispatch).
	if batch.Definition.OnetimeTaskType != nil && *batch.Definition.OnetimeTaskType != "" &&
		(batch.OnetimeState == nil || batch.OnetimeState.Status != types.JobStatusCompleted) {
		batch.OnetimeState = nil
	}

	if _, err := a.store.UpdateBatch(ctx, batch, rev); err != nil {
		return err
	}

	a.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventBatchRetrying,
		JobID:     batchID,
		Attempt:   batch.Attempt,
		Timestamp: now,
	})
	return nil
}

// ===================== Groups =====================

// GroupInfo describes a single aggregation group.
type GroupInfo struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

func (a *APIService) ListGroups(ctx context.Context) ([]GroupInfo, error) {
	groups, err := a.store.ListGroups(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]GroupInfo, 0, len(groups))
	for _, g := range groups {
		size, _ := a.store.GetGroupSize(ctx, g)
		result = append(result, GroupInfo{Name: g, Size: size})
	}
	return result, nil
}

// ===================== Queues =====================

// QueueInfo describes the state of a single queue/tier.
type QueueInfo struct {
	Name       string  `json:"name"`
	Weight     int     `json:"weight"`
	FetchBatch int     `json:"fetch_batch"`
	RateLimit  float64 `json:"rate_limit,omitempty"`
	RateBurst  int     `json:"rate_burst,omitempty"`
	Paused     bool    `json:"paused"`
	Size       int64   `json:"size"`
}

func (a *APIService) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	tiers := a.store.Config().Tiers
	paused, _ := a.store.ListPausedQueues(ctx)
	pausedSet := make(map[string]bool, len(paused))
	for _, p := range paused {
		pausedSet[p] = true
	}

	queues := make([]QueueInfo, 0, len(tiers))
	for _, tier := range tiers {
		size, _ := a.store.Client().Do(ctx, a.store.Client().B().Xlen().Key(store.WorkStreamKey(a.store.Prefix(), tier.Name)).Build()).AsInt64()
		queues = append(queues, QueueInfo{
			Name:       tier.Name,
			Weight:     tier.Weight,
			FetchBatch: tier.FetchBatch,
			RateLimit:  tier.RateLimit,
			RateBurst:  tier.RateBurst,
			Paused:     pausedSet[tier.Name],
			Size:       size,
		})
	}
	return queues, nil
}

func (a *APIService) PauseQueue(ctx context.Context, tierName string) error {
	return a.store.PauseQueue(ctx, tierName)
}

func (a *APIService) ResumeQueue(ctx context.Context, tierName string) error {
	return a.store.UnpauseQueue(ctx, tierName)
}

// ===================== Stats =====================

// StatsResponse holds overall system statistics.
type StatsResponse struct {
	JobCounts       map[types.JobStatus]int `json:"job_counts"`
	ActiveSchedules int                     `json:"active_schedules"`
	ActiveRuns      int                     `json:"active_runs"`
	ActiveNodes     int                     `json:"active_nodes"`
}

func (a *APIService) GetStats(ctx context.Context) (*StatsResponse, error) {
	jobCounts, err := a.store.GetActiveJobStats(ctx)
	if err != nil {
		return nil, err
	}

	schedules, _ := a.store.ListSchedules(ctx)
	runs, _ := a.store.ListRuns(ctx)
	nodes, _ := a.store.ListNodes(ctx)

	return &StatsResponse{
		JobCounts:       jobCounts,
		ActiveSchedules: len(schedules),
		ActiveRuns:      len(runs),
		ActiveNodes:     len(nodes),
	}, nil
}

// DailyStatsEntry represents aggregated stats for one day.
type DailyStatsEntry struct {
	Date      string `json:"date"`
	Processed int64  `json:"processed"`
	Failed    int64  `json:"failed"`
}

func (a *APIService) GetDailyStats(ctx context.Context, days int) ([]DailyStatsEntry, error) {
	if days <= 0 {
		days = 7
	}
	if days > 90 {
		days = 90
	}

	entries, err := a.store.GetDailyStats(ctx, days)
	if err != nil {
		return nil, err
	}

	result := make([]DailyStatsEntry, 0, len(entries))
	for _, e := range entries {
		result = append(result, DailyStatsEntry{
			Date:      e.Date,
			Processed: e.Processed,
			Failed:    e.Failed,
		})
	}
	return result, nil
}

func (a *APIService) GetRedisInfo(ctx context.Context, section string) (map[string]map[string]string, error) {
	if section == "" {
		section = "all"
	}

	info, err := a.store.Client().Do(ctx, a.store.Client().B().Info().Section(section).Build()).ToString()
	if err != nil {
		return nil, err
	}

	return store.ParseRedisInfo(info), nil
}

func (a *APIService) GetSyncRetries() any {
	if a.syncRetryStats == nil {
		return map[string]any{"pending": 0, "failed": 0, "recent": []any{}}
	}
	return a.syncRetryStats()
}

// ===================== Search =====================

func (a *APIService) SearchJobsByPayload(ctx context.Context, path, value string) (*types.Job, error) {
	if path == "" || value == "" {
		return nil, &ApiError{Msg: "path and value are required", StatusCode: http.StatusBadRequest}
	}
	job, _, err := a.store.FindJobByPayloadPath(ctx, path, value)
	if err != nil {
		return nil, &ApiError{Msg: err.Error(), StatusCode: http.StatusNotFound}
	}
	return job, nil
}

func (a *APIService) SearchWorkflowsByPayload(ctx context.Context, path, value string) (*types.WorkflowInstance, error) {
	if path == "" || value == "" {
		return nil, &ApiError{Msg: "path and value are required", StatusCode: http.StatusBadRequest}
	}
	wf, _, err := a.store.FindWorkflowByTaskPayloadPath(ctx, path, value)
	if err != nil {
		return nil, &ApiError{Msg: err.Error(), StatusCode: http.StatusNotFound}
	}
	return wf, nil
}

func (a *APIService) SearchBatchesByPayload(ctx context.Context, path, value string) (*types.BatchInstance, error) {
	if path == "" || value == "" {
		return nil, &ApiError{Msg: "path and value are required", StatusCode: http.StatusBadRequest}
	}
	batch, _, err := a.store.FindBatchByPayloadPath(ctx, path, value)
	if err != nil {
		return nil, &ApiError{Msg: err.Error(), StatusCode: http.StatusNotFound}
	}
	return batch, nil
}

// ===================== Unique Keys =====================

type UniqueKeyResult struct {
	Exists bool   `json:"exists"`
	JobID  string `json:"job_id"`
}

func (a *APIService) CheckUniqueKey(ctx context.Context, key string) (*UniqueKeyResult, error) {
	jobID, exists, err := a.store.CheckUniqueKey(ctx, key)
	if err != nil {
		return nil, err
	}
	return &UniqueKeyResult{Exists: exists, JobID: jobID}, nil
}

func (a *APIService) DeleteUniqueKey(ctx context.Context, key string) error {
	return a.store.DeleteUniqueKey(ctx, key)
}

// ===================== Bulk =====================

// BulkRequest is the common shape for bulk operation requests.
type BulkRequest struct {
	IDs    []string `json:"ids"`
	Status string   `json:"status"`
}

// BulkResult holds the count of affected entities.
type BulkResult struct {
	Affected int `json:"affected"`
}

func (a *APIService) BulkCancelJobs(ctx context.Context, req BulkRequest) (*BulkResult, error) {
	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		jobs, _, err := a.store.ListJobsPaginated(ctx, jobFilterByStatus(req.Status, maxPageLimit))
		if err != nil {
			return nil, err
		}
		for _, j := range jobs {
			ids = append(ids, j.ID)
		}
	}
	if len(ids) > maxPageLimit {
		ids = ids[:maxPageLimit]
	}

	affected := 0
	now := time.Now()
	for _, id := range ids {
		job, rev, err := a.store.GetJob(ctx, id)
		if err != nil || job.Status.IsTerminal() {
			continue
		}
		job.Status = types.JobStatusCancelled
		job.UpdatedAt = now
		if _, err := a.store.UpdateJob(ctx, job, rev); err == nil {
			a.store.DeleteSchedule(ctx, id)
			a.dispatcher.PublishEvent(types.JobEvent{
				Type: types.EventJobCancelled, JobID: id, Timestamp: now,
			})
			affected++
		}
	}
	return &BulkResult{Affected: affected}, nil
}

func (a *APIService) BulkRetryJobs(ctx context.Context, req BulkRequest) (*BulkResult, error) {
	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		jobs, _, err := a.store.ListJobsPaginated(ctx, jobFilterByStatus(req.Status, maxPageLimit))
		if err != nil {
			return nil, err
		}
		for _, j := range jobs {
			ids = append(ids, j.ID)
		}
	}
	if len(ids) > maxPageLimit {
		ids = ids[:maxPageLimit]
	}

	affected := 0
	now := time.Now()
	for _, id := range ids {
		job, rev, err := a.store.GetJob(ctx, id)
		if err != nil {
			continue
		}
		job.Status = types.JobStatusPending
		job.Attempt = 0
		job.LastError = nil
		job.UpdatedAt = now
		if _, err := a.store.UpdateJob(ctx, job, rev); err != nil {
			continue
		}
		if err := a.dispatcher.Dispatch(ctx, job, 0); err != nil {
			continue
		}
		a.dispatcher.PublishEvent(types.JobEvent{
			Type: types.EventJobRetrying, JobID: id, Timestamp: now,
		})
		affected++
	}
	return &BulkResult{Affected: affected}, nil
}

func (a *APIService) BulkDeleteJobs(ctx context.Context, req BulkRequest) (*BulkResult, error) {
	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		jobs, _, err := a.store.ListJobsPaginated(ctx, jobFilterByStatus(req.Status, maxPageLimit))
		if err != nil {
			return nil, err
		}
		for _, j := range jobs {
			ids = append(ids, j.ID)
		}
	}
	if len(ids) > maxPageLimit {
		ids = ids[:maxPageLimit]
	}

	affected := 0
	for _, id := range ids {
		if err := a.store.DeleteJob(ctx, id); err == nil {
			a.store.DeleteSchedule(ctx, id)
			affected++
		}
	}
	return &BulkResult{Affected: affected}, nil
}

func (a *APIService) BulkCancelWorkflows(ctx context.Context, req BulkRequest) (*BulkResult, error) {
	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		status := types.WorkflowStatus(req.Status)
		wfs, _, err := a.store.ListWorkflowInstances(ctx, workflowFilterByStatus(&status, maxPageLimit))
		if err != nil {
			return nil, err
		}
		for _, wf := range wfs {
			ids = append(ids, wf.ID)
		}
	}
	if len(ids) > maxPageLimit {
		ids = ids[:maxPageLimit]
	}

	affected := 0
	now := time.Now()
	for _, id := range ids {
		wf, rev, err := a.store.GetWorkflow(ctx, id)
		if err != nil || wf.Status.IsTerminal() {
			continue
		}
		wf.Status = types.WorkflowStatusCancelled
		wf.CompletedAt = &now
		wf.UpdatedAt = now
		for name, state := range wf.Tasks {
			if !state.Status.IsTerminal() {
				state.Status = types.JobStatusCancelled
				state.FinishedAt = &now
				wf.Tasks[name] = state

				if state.JobID != "" {
					a.signalCancelActiveRuns(ctx, state.JobID)
					if childJob, childRev, err := a.store.GetJob(ctx, state.JobID); err == nil {
						if !childJob.Status.IsTerminal() {
							childJob.Status = types.JobStatusCancelled
							childJob.UpdatedAt = now
							a.store.UpdateJob(ctx, childJob, childRev)
						}
					}
				}
			}
		}
		if _, err := a.store.UpdateWorkflow(ctx, wf, rev); err == nil {
			a.dispatcher.PublishEvent(types.JobEvent{
				Type: types.EventWorkflowCancelled, JobID: id, Timestamp: now,
			})
			affected++
		}
	}
	return &BulkResult{Affected: affected}, nil
}

func (a *APIService) BulkRetryWorkflows(ctx context.Context, req BulkRequest) (*BulkResult, error) {
	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		status := types.WorkflowStatus(req.Status)
		wfs, _, err := a.store.ListWorkflowInstances(ctx, workflowFilterByStatus(&status, maxPageLimit))
		if err != nil {
			return nil, err
		}
		for _, wf := range wfs {
			ids = append(ids, wf.ID)
		}
	}
	if len(ids) > maxPageLimit {
		ids = ids[:maxPageLimit]
	}

	affected := 0
	now := time.Now()
	for _, id := range ids {
		wf, rev, err := a.store.GetWorkflow(ctx, id)
		if err != nil {
			continue
		}
		wf.Attempt++
		wf.Status = types.WorkflowStatusRunning
		wf.UpdatedAt = now
		wf.CompletedAt = nil
		for name, state := range wf.Tasks {
			state.Status = types.JobStatusPending
			state.JobID = ""
			state.Error = nil
			state.StartedAt = nil
			state.FinishedAt = nil
			wf.Tasks[name] = state
		}
		if _, err := a.store.UpdateWorkflow(ctx, wf, rev); err == nil {
			a.dispatcher.PublishEvent(types.JobEvent{
				Type: types.EventWorkflowRetrying, JobID: id, Attempt: wf.Attempt, Timestamp: now,
			})
			affected++
		}
	}
	return &BulkResult{Affected: affected}, nil
}

func (a *APIService) BulkDeleteWorkflows(ctx context.Context, req BulkRequest) (*BulkResult, error) {
	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		status := types.WorkflowStatus(req.Status)
		wfs, _, err := a.store.ListWorkflowInstances(ctx, workflowFilterByStatus(&status, maxPageLimit))
		if err != nil {
			return nil, err
		}
		for _, wf := range wfs {
			ids = append(ids, wf.ID)
		}
	}
	if len(ids) > maxPageLimit {
		ids = ids[:maxPageLimit]
	}

	affected := 0
	for _, id := range ids {
		if err := a.store.DeleteWorkflow(ctx, id); err == nil {
			affected++
		}
	}
	return &BulkResult{Affected: affected}, nil
}

func (a *APIService) BulkCancelBatches(ctx context.Context, req BulkRequest) (*BulkResult, error) {
	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		status := types.WorkflowStatus(req.Status)
		batches, _, err := a.store.ListBatchInstances(ctx, batchFilterByStatus(&status, maxPageLimit))
		if err != nil {
			return nil, err
		}
		for _, b := range batches {
			ids = append(ids, b.ID)
		}
	}
	if len(ids) > maxPageLimit {
		ids = ids[:maxPageLimit]
	}

	affected := 0
	now := time.Now()
	for _, id := range ids {
		batch, rev, err := a.store.GetBatch(ctx, id)
		if err != nil || batch.Status.IsTerminal() {
			continue
		}
		batch.Status = types.WorkflowStatusCancelled
		batch.CompletedAt = &now
		batch.UpdatedAt = now

		for itemID, state := range batch.ItemStates {
			if !state.Status.IsTerminal() {
				state.Status = types.JobStatusCancelled
				state.FinishedAt = &now
				batch.ItemStates[itemID] = state

				if state.JobID != "" {
					a.signalCancelActiveRuns(ctx, state.JobID)
					if childJob, childRev, err := a.store.GetJob(ctx, state.JobID); err == nil {
						if !childJob.Status.IsTerminal() {
							childJob.Status = types.JobStatusCancelled
							childJob.UpdatedAt = now
							a.store.UpdateJob(ctx, childJob, childRev)
						}
					}
				}
			}
		}

		// Signal cancel for onetime job if running.
		if batch.OnetimeState != nil && !batch.OnetimeState.Status.IsTerminal() && batch.OnetimeState.JobID != "" {
			a.signalCancelActiveRuns(ctx, batch.OnetimeState.JobID)
		}

		if _, err := a.store.UpdateBatch(ctx, batch, rev); err == nil {
			a.dispatcher.PublishEvent(types.JobEvent{
				Type: types.EventBatchCancelled, JobID: id, Timestamp: now,
			})
			affected++
		}
	}
	return &BulkResult{Affected: affected}, nil
}

func (a *APIService) BulkRetryBatches(ctx context.Context, req BulkRequest) (*BulkResult, error) {
	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		status := types.WorkflowStatus(req.Status)
		batches, _, err := a.store.ListBatchInstances(ctx, batchFilterByStatus(&status, maxPageLimit))
		if err != nil {
			return nil, err
		}
		for _, b := range batches {
			ids = append(ids, b.ID)
		}
	}
	if len(ids) > maxPageLimit {
		ids = ids[:maxPageLimit]
	}

	affected := 0
	now := time.Now()
	for _, id := range ids {
		batch, rev, err := a.store.GetBatch(ctx, id)
		if err != nil {
			continue
		}
		batch.Attempt++
		batch.Status = types.WorkflowStatusRunning
		batch.UpdatedAt = now
		batch.CompletedAt = nil
		batch.FailedItems = 0
		batch.RunningItems = 0
		batch.PendingItems = batch.TotalItems
		batch.CompletedItems = 0
		for itemID, state := range batch.ItemStates {
			state.Status = types.JobStatusPending
			state.JobID = ""
			state.Error = nil
			state.StartedAt = nil
			state.FinishedAt = nil
			batch.ItemStates[itemID] = state
		}
		batch.NextChunkIndex = 0
		if _, err := a.store.UpdateBatch(ctx, batch, rev); err == nil {
			a.dispatcher.PublishEvent(types.JobEvent{
				Type: types.EventBatchRetrying, JobID: id, Attempt: batch.Attempt, Timestamp: now,
			})
			affected++
		}
	}
	return &BulkResult{Affected: affected}, nil
}

func (a *APIService) BulkDeleteBatches(ctx context.Context, req BulkRequest) (*BulkResult, error) {
	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		status := types.WorkflowStatus(req.Status)
		batches, _, err := a.store.ListBatchInstances(ctx, batchFilterByStatus(&status, maxPageLimit))
		if err != nil {
			return nil, err
		}
		for _, b := range batches {
			ids = append(ids, b.ID)
		}
	}
	if len(ids) > maxPageLimit {
		ids = ids[:maxPageLimit]
	}

	affected := 0
	for _, id := range ids {
		if err := a.store.DeleteBatch(ctx, id); err == nil {
			affected++
		}
	}
	return &BulkResult{Affected: affected}, nil
}

// ===================== Audit Trail =====================

func (a *APIService) GetJobAuditTrail(ctx context.Context, jobID string) ([]store.AuditEntry, error) {
	return a.store.GetJobAuditTrail(ctx, jobID)
}

// GetJobAuditCounts returns audit trail entry counts for multiple jobs.
func (a *APIService) GetJobAuditCounts(ctx context.Context, jobIDs []string) (map[string]int64, error) {
	return a.store.GetJobAuditCounts(ctx, jobIDs)
}

// WorkflowAuditEntry is an audit entry annotated with task name and job ID.
type WorkflowAuditEntry struct {
	TaskName  string    `json:"task_name"`
	JobID     string    `json:"job_id"`
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	Error     string    `json:"error,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// GetWorkflowAuditTrail aggregates audit entries from all task jobs in a workflow.
func (a *APIService) GetWorkflowAuditTrail(ctx context.Context, wfID string) ([]WorkflowAuditEntry, error) {
	wf, _, err := a.store.GetWorkflow(ctx, wfID)
	if err != nil {
		return nil, &ApiError{Msg: "workflow not found: " + err.Error(), StatusCode: http.StatusNotFound}
	}

	var result []WorkflowAuditEntry
	for taskName, state := range wf.Tasks {
		if state.JobID == "" {
			continue
		}
		entries, err := a.store.GetJobAuditTrail(ctx, state.JobID)
		if err != nil {
			continue
		}
		for _, e := range entries {
			result = append(result, WorkflowAuditEntry{
				TaskName:  taskName,
				JobID:     state.JobID,
				ID:        e.ID,
				Status:    e.Status,
				Error:     e.Error,
				Timestamp: e.Timestamp,
			})
		}
	}

	// Sort by timestamp descending (newest first)
	for i := 0; i < len(result); i++ {
		for j := i + 1; j < len(result); j++ {
			if result[j].Timestamp.After(result[i].Timestamp) {
				result[i], result[j] = result[j], result[i]
			}
		}
	}
	return result, nil
}

// ===================== Node Drain =====================

func (a *APIService) SetNodeDrain(ctx context.Context, nodeID string, drain bool) error {
	return a.store.SetNodeDrain(ctx, nodeID, drain)
}

func (a *APIService) IsNodeDraining(ctx context.Context, nodeID string) (bool, error) {
	return a.store.IsNodeDraining(ctx, nodeID)
}
