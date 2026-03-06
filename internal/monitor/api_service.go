package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
)

// Dispatcher abstracts job dispatch and event publishing for the monitor API.
// v1server provides *dispatcher.Dispatcher; v2 actor server provides actorDispatcher.
type Dispatcher interface {
	Dispatch(ctx context.Context, job *types.Job, attempt int) error
	PublishEvent(event types.JobEvent)
}

// SyncRetryStatsFunc is a function that returns the current SyncRetrier stats
// as a JSON-serializable value. Set via SetSyncRetryStatsFunc.
type SyncRetryStatsFunc func() any

// APIService is a transport-agnostic service layer for the monitor API.
// It encapsulates all business logic without any HTTP dependency.
// Users can build custom handlers on top of this service.
type APIService struct {
	store      *store.RedisStore
	dispatcher Dispatcher
	logger     gochainedlog.Logger

	hub       Hub
	hubCancel context.CancelFunc

	syncRetryStats SyncRetryStatsFunc
}

// NewAPIService creates a new monitoring service backed by Redis.
func NewAPIService(s *store.RedisStore, disp Dispatcher, logger gochainedlog.Logger) *APIService {
	if logger == nil {
		logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}

	hubCtx, hubCancel := context.WithCancel(context.Background())
	h := newHub(logger)
	go h.SubscribeRedis(hubCtx, s.Client(), store.EventsPubSubChannel(s.Prefix()))

	return &APIService{
		store:      s,
		dispatcher: disp,
		logger:     logger,
		hub:        h,
		hubCancel:  hubCancel,
	}
}

// Store returns the underlying Redis store.
func (a *APIService) Store() *store.RedisStore { return a.store }

// Hub returns the WebSocket hub for real-time event broadcasting.
func (a *APIService) Hub() Hub { return a.hub }

// Logger returns the service logger.
func (a *APIService) Logger() gochainedlog.Logger { return a.logger }

// SetSyncRetryStatsFunc sets the function used to retrieve SyncRetrier stats.
func (a *APIService) SetSyncRetryStatsFunc(fn SyncRetryStatsFunc) {
	a.syncRetryStats = fn
}

// Shutdown stops the WebSocket hub's Redis subscription.
func (a *APIService) Shutdown() {
	a.hubCancel()
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

func (a *APIService) ListJobs(ctx context.Context, filter store.JobFilter) ([]types.Job, int, error) {
	jobs, total, err := a.store.ListJobsPaginated(ctx, filter)
	if err != nil {
		return nil, 0, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}
	return jobs, total, nil
}

func (a *APIService) GetJob(ctx context.Context, jobID string) (*types.Job, uint64, error) {
	return a.store.GetJob(ctx, jobID)
}

func (a *APIService) CancelJob(ctx context.Context, jobID string) error {
	job, rev, err := a.store.GetJob(ctx, jobID)
	if err != nil {
		return &ApiError{Msg: "job not found", StatusCode: http.StatusNotFound}
	}
	if job.Status.IsTerminal() {
		return &ApiError{Msg: "job is already in terminal state", StatusCode: http.StatusBadRequest}
	}

	now := time.Now()
	job.Status = types.JobStatusCancelled
	job.UpdatedAt = now

	if _, err := a.store.UpdateJob(ctx, job, rev); err != nil {
		return &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}

	a.store.DeleteSchedule(ctx, jobID)

	// Publish cancel signals to in-flight workers via Redis Pub/Sub.
	runs, _ := a.store.ListActiveRunsByJobID(ctx, jobID)
	for _, run := range runs {
		a.store.Client().Do(ctx, a.store.Client().B().Publish().Channel(a.store.CancelChannel()).Message(run.ID).Build())
	}

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
		return &ApiError{Msg: err.Error(), StatusCode: http.StatusNotFound}
	}

	now := time.Now()
	job.Status = types.JobStatusPending
	job.Attempt = 0
	job.LastError = nil
	job.UpdatedAt = now

	if _, err := a.store.UpdateJob(ctx, job, rev); err != nil {
		return &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}

	if err := a.dispatcher.Dispatch(ctx, job, 0); err != nil {
		return &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
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
		return &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}
	a.store.DeleteSchedule(ctx, jobID)
	return nil
}

func (a *APIService) ListJobEvents(ctx context.Context, jobID string, limit int) ([]types.JobEvent, error) {
	if limit < 1 {
		limit = 50
	}

	events, _, err := a.store.ListJobEvents(ctx, store.EventFilter{
		JobID: &jobID,
		Limit: limit,
	})
	if err != nil {
		return nil, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}
	return events, nil
}

func (a *APIService) ListJobRuns(ctx context.Context, jobID string, limit int) ([]types.JobRun, error) {
	if limit < 1 {
		limit = 50
	}
	runs, _, err := a.store.ListJobRuns(ctx, store.RunFilter{
		JobID: &jobID,
		Limit: limit,
	})
	if err != nil {
		return nil, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}
	return runs, nil
}

func (a *APIService) UpdateJobPayload(ctx context.Context, jobID string, payload json.RawMessage) error {
	if len(payload) == 0 {
		return &ApiError{Msg: "payload is required", StatusCode: http.StatusBadRequest}
	}

	job, rev, err := a.store.GetJob(ctx, jobID)
	if err != nil {
		return &ApiError{Msg: err.Error(), StatusCode: http.StatusNotFound}
	}

	// Only allow payload update for non-running, non-terminal jobs.
	if job.Status.IsTerminal() || job.Status == types.JobStatusRunning {
		return &ApiError{Msg: "can only update payload of pending or scheduled jobs", StatusCode: http.StatusBadRequest}
	}

	job.Payload = payload
	job.UpdatedAt = time.Now()

	if _, err := a.store.UpdateJob(ctx, job, rev); err != nil {
		return &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Schedules
// ---------------------------------------------------------------------------

func (a *APIService) ListSchedules(ctx context.Context, sort string, limit, offset int) ([]types.ScheduleEntry, int, error) {
	schedules, total, err := a.store.ListSchedulesPaginated(ctx, store.ScheduleFilter{
		Sort:   sort,
		Limit:  limit,
		Offset: offset,
	})
	if err != nil {
		return nil, 0, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}
	return schedules, total, nil
}

func (a *APIService) GetSchedule(ctx context.Context, jobID string) (*types.ScheduleEntry, error) {
	sched, _, err := a.store.GetSchedule(ctx, jobID)
	if err != nil {
		return nil, &ApiError{Msg: err.Error(), StatusCode: http.StatusNotFound}
	}
	return sched, nil
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

func (a *APIService) ListNodes(ctx context.Context) ([]*types.NodeInfo, error) {
	nodes, err := a.store.ListNodes(ctx)
	if err != nil {
		return nil, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}
	return nodes, nil
}

func (a *APIService) GetNode(ctx context.Context, nodeID string) (*types.NodeInfo, error) {
	node, err := a.store.GetNode(ctx, nodeID)
	if err != nil {
		return nil, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}
	if node == nil {
		return nil, &ApiError{Msg: "node not found", StatusCode: http.StatusNotFound}
	}
	return node, nil
}

// ---------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------

func (a *APIService) ListActiveRuns(ctx context.Context) ([]*types.JobRun, error) {
	runs, err := a.store.ListRuns(ctx)
	if err != nil {
		return nil, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}
	return runs, nil
}

func (a *APIService) GetRun(ctx context.Context, runID string) (*types.JobRun, error) {
	run, _, err := a.store.GetRun(ctx, runID)
	if err != nil {
		return nil, &ApiError{Msg: err.Error(), StatusCode: http.StatusNotFound}
	}
	return run, nil
}

func (a *APIService) GetJobActiveRuns(ctx context.Context, jobID string) ([]*types.JobRun, error) {
	runs, err := a.store.ListActiveRunsByJobID(ctx, jobID)
	if err != nil {
		return nil, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}
	return runs, nil
}

// ---------------------------------------------------------------------------
// History
// ---------------------------------------------------------------------------

func (a *APIService) ListHistoryRuns(ctx context.Context, filter store.RunFilter) ([]types.JobRun, int, error) {
	runs, total, err := a.store.ListJobRuns(ctx, filter)
	if err != nil {
		return nil, 0, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}
	return runs, total, nil
}

func (a *APIService) ListHistoryEvents(ctx context.Context, filter store.EventFilter) ([]types.JobEvent, int, error) {
	events, total, err := a.store.ListJobEvents(ctx, filter)
	if err != nil {
		return nil, 0, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}
	return events, total, nil
}

// ---------------------------------------------------------------------------
// DLQ
// ---------------------------------------------------------------------------

func (a *APIService) ListDLQ(ctx context.Context, limit, offset int) ([]*types.WorkMessage, int, error) {
	msgs, err := a.store.ListDLQ(ctx, 1000)
	if err != nil {
		return nil, 0, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
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

// ---------------------------------------------------------------------------
// Workflows
// ---------------------------------------------------------------------------

func (a *APIService) ListWorkflows(ctx context.Context, filter store.WorkflowFilter) ([]types.WorkflowInstance, int, error) {
	workflows, total, err := a.store.ListWorkflowInstances(ctx, filter)
	if err != nil {
		return nil, 0, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}
	return workflows, total, nil
}

func (a *APIService) GetWorkflow(ctx context.Context, wfID string) (*types.WorkflowInstance, error) {
	wf, _, err := a.store.GetWorkflow(ctx, wfID)
	if err != nil {
		return nil, &ApiError{Msg: err.Error(), StatusCode: http.StatusNotFound}
	}
	return wf, nil
}

func (a *APIService) CancelWorkflow(ctx context.Context, wfID string) error {
	wf, rev, err := a.store.GetWorkflow(ctx, wfID)
	if err != nil {
		return &ApiError{Msg: err.Error(), StatusCode: http.StatusNotFound}
	}
	if wf.Status.IsTerminal() {
		return &ApiError{Msg: "workflow is already in terminal state", StatusCode: http.StatusBadRequest}
	}

	now := time.Now()
	wf.Status = types.WorkflowStatusCancelled
	wf.CompletedAt = &now
	wf.UpdatedAt = now

	// Cancel non-terminal tasks.
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

						// Signal active runs for this child job.
						if runs, _ := a.store.ListActiveRunsByJobID(ctx, state.JobID); len(runs) > 0 {
							for _, run := range runs {
								a.store.Client().Do(ctx, a.store.Client().B().Publish().Channel(a.store.CancelChannel()).Message(run.ID).Build())
							}
						}
					}
				}
			}
		}
	}

	if _, err := a.store.UpdateWorkflow(ctx, wf, rev); err != nil {
		return &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}

	a.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventWorkflowCancelled,
		JobID:     wfID,
		Timestamp: now,
	})
	return nil
}

func (a *APIService) RetryWorkflow(ctx context.Context, wfID string) error {
	wf, rev, err := a.store.GetWorkflow(ctx, wfID)
	if err != nil {
		return &ApiError{Msg: err.Error(), StatusCode: http.StatusNotFound}
	}

	now := time.Now()
	wf.Attempt++
	wf.Status = types.WorkflowStatusRunning
	wf.UpdatedAt = now
	wf.CompletedAt = nil

	// Reset all tasks to pending.
	for name, state := range wf.Tasks {
		state.Status = types.JobStatusPending
		state.JobID = ""
		state.Error = nil
		state.StartedAt = nil
		state.FinishedAt = nil
		wf.Tasks[name] = state
	}

	if _, err := a.store.UpdateWorkflow(ctx, wf, rev); err != nil {
		return &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}

	a.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventWorkflowRetrying,
		JobID:     wfID,
		Attempt:   wf.Attempt,
		Timestamp: now,
	})
	return nil
}

// ---------------------------------------------------------------------------
// Batches
// ---------------------------------------------------------------------------

func (a *APIService) ListBatches(ctx context.Context, filter store.BatchFilter) ([]types.BatchInstance, int, error) {
	batches, total, err := a.store.ListBatchInstances(ctx, filter)
	if err != nil {
		return nil, 0, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}
	return batches, total, nil
}

func (a *APIService) GetBatch(ctx context.Context, batchID string) (*types.BatchInstance, error) {
	batch, _, err := a.store.GetBatch(ctx, batchID)
	if err != nil {
		return nil, &ApiError{Msg: err.Error(), StatusCode: http.StatusNotFound}
	}
	return batch, nil
}

func (a *APIService) GetBatchResults(ctx context.Context, batchID string) ([]*types.BatchItemResult, error) {
	results, err := a.store.ListBatchItemResults(ctx, batchID)
	if err != nil {
		return nil, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}
	return results, nil
}

func (a *APIService) GetBatchItemResult(ctx context.Context, batchID, itemID string) (*types.BatchItemResult, error) {
	result, err := a.store.GetBatchItemResult(ctx, batchID, itemID)
	if err != nil {
		return nil, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}
	if result == nil {
		return nil, &ApiError{Msg: "batch item result not found", StatusCode: http.StatusNotFound}
	}
	return result, nil
}

func (a *APIService) CancelBatch(ctx context.Context, batchID string) error {
	batch, rev, err := a.store.GetBatch(ctx, batchID)
	if err != nil {
		return &ApiError{Msg: err.Error(), StatusCode: http.StatusNotFound}
	}
	if batch.Status.IsTerminal() {
		return &ApiError{Msg: "batch is already in terminal state", StatusCode: http.StatusBadRequest}
	}

	now := time.Now()
	batch.Status = types.WorkflowStatusCancelled
	batch.CompletedAt = &now
	batch.UpdatedAt = now

	// Cancel in-flight child jobs.
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

						// Signal active runs for this child job.
						if runs, _ := a.store.ListActiveRunsByJobID(ctx, state.JobID); len(runs) > 0 {
							for _, run := range runs {
								a.store.Client().Do(ctx, a.store.Client().B().Publish().Channel(a.store.CancelChannel()).Message(run.ID).Build())
							}
						}
					}
				}
			}
		}
	}

	if _, err := a.store.UpdateBatch(ctx, batch, rev); err != nil {
		return &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}

	a.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventBatchCancelled,
		JobID:     batchID,
		Timestamp: now,
	})
	return nil
}

func (a *APIService) RetryBatch(ctx context.Context, batchID string, retryFailedOnly bool) error {
	batch, rev, err := a.store.GetBatch(ctx, batchID)
	if err != nil {
		return &ApiError{Msg: err.Error(), StatusCode: http.StatusNotFound}
	}

	now := time.Now()
	batch.Attempt++
	batch.Status = types.WorkflowStatusRunning
	batch.UpdatedAt = now
	batch.CompletedAt = nil

	// Reset items.
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

	if _, err := a.store.UpdateBatch(ctx, batch, rev); err != nil {
		return &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}

	a.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventBatchRetrying,
		JobID:     batchID,
		Attempt:   batch.Attempt,
		Timestamp: now,
	})
	return nil
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

// GroupInfo describes an active aggregation group.
type GroupInfo struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

func (a *APIService) ListGroups(ctx context.Context) ([]GroupInfo, error) {
	groups, err := a.store.ListGroups(ctx)
	if err != nil {
		return nil, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}

	result := make([]GroupInfo, 0, len(groups))
	for _, g := range groups {
		size, _ := a.store.GetGroupSize(ctx, g)
		result = append(result, GroupInfo{Name: g, Size: size})
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Queues
// ---------------------------------------------------------------------------

// QueueInfo describes the state of a single queue/tier.
type QueueInfo struct {
	Name       string `json:"name"`
	Weight     int    `json:"weight"`
	FetchBatch int    `json:"fetch_batch"`
	Paused     bool   `json:"paused"`
	Size       int64  `json:"size"`
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
			Paused:     pausedSet[tier.Name],
			Size:       size,
		})
	}
	return queues, nil
}

func (a *APIService) PauseQueue(ctx context.Context, tierName string) error {
	if err := a.store.PauseQueue(ctx, tierName); err != nil {
		return &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}
	return nil
}

func (a *APIService) ResumeQueue(ctx context.Context, tierName string) error {
	if err := a.store.UnpauseQueue(ctx, tierName); err != nil {
		return &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

// StatsResponse contains cluster-wide statistics.
type StatsResponse struct {
	JobCounts       map[types.JobStatus]int `json:"job_counts"`
	ActiveSchedules int                     `json:"active_schedules"`
	ActiveRuns      int                     `json:"active_runs"`
	ActiveNodes     int                     `json:"active_nodes"`
}

func (a *APIService) GetStats(ctx context.Context) (*StatsResponse, error) {
	jobCounts, err := a.store.GetActiveJobStats(ctx)
	if err != nil {
		return nil, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
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
	entries, err := a.store.GetDailyStats(ctx, days)
	if err != nil {
		return nil, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
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
		return nil, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
	}

	return store.ParseRedisInfo(info), nil
}

func (a *APIService) GetSyncRetries() any {
	if a.syncRetryStats == nil {
		return map[string]any{"pending": 0, "failed": 0, "recent": []any{}}
	}
	return a.syncRetryStats()
}

// ---------------------------------------------------------------------------
// Bulk operations
// ---------------------------------------------------------------------------

// BulkRequest is the common shape for bulk operation requests.
type BulkRequest struct {
	// IDs specifies individual IDs to operate on.
	IDs []string `json:"ids"`
	// Status selects all entities in a given status (alternative to IDs).
	Status string `json:"status"`
}

func jobFilterByStatus(status string, limit int) store.JobFilter {
	s := types.JobStatus(status)
	return store.JobFilter{Status: &s, Limit: limit}
}

func workflowFilterByStatus(status *types.WorkflowStatus, limit int) store.WorkflowFilter {
	return store.WorkflowFilter{Status: status, Limit: limit}
}

func batchFilterByStatus(status *types.WorkflowStatus, limit int) store.BatchFilter {
	return store.BatchFilter{Status: status, Limit: limit}
}

func (a *APIService) BulkCancelJobs(ctx context.Context, req BulkRequest) (int, error) {
	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		jobs, _, err := a.store.ListJobsPaginated(ctx, jobFilterByStatus(req.Status, maxPageLimit))
		if err != nil {
			return 0, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
		}
		for _, j := range jobs {
			ids = append(ids, j.ID)
		}
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
	return affected, nil
}

func (a *APIService) BulkRetryJobs(ctx context.Context, req BulkRequest) (int, error) {
	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		jobs, _, err := a.store.ListJobsPaginated(ctx, jobFilterByStatus(req.Status, maxPageLimit))
		if err != nil {
			return 0, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
		}
		for _, j := range jobs {
			ids = append(ids, j.ID)
		}
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
	return affected, nil
}

func (a *APIService) BulkDeleteJobs(ctx context.Context, req BulkRequest) (int, error) {
	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		jobs, _, err := a.store.ListJobsPaginated(ctx, jobFilterByStatus(req.Status, maxPageLimit))
		if err != nil {
			return 0, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
		}
		for _, j := range jobs {
			ids = append(ids, j.ID)
		}
	}

	affected := 0
	for _, id := range ids {
		if err := a.store.DeleteJob(ctx, id); err == nil {
			a.store.DeleteSchedule(ctx, id)
			affected++
		}
	}
	return affected, nil
}

func (a *APIService) BulkCancelWorkflows(ctx context.Context, req BulkRequest) (int, error) {
	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		status := types.WorkflowStatus(req.Status)
		wfs, _, err := a.store.ListWorkflowInstances(ctx, workflowFilterByStatus(&status, maxPageLimit))
		if err != nil {
			return 0, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
		}
		for _, wf := range wfs {
			ids = append(ids, wf.ID)
		}
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
	return affected, nil
}

func (a *APIService) BulkRetryWorkflows(ctx context.Context, req BulkRequest) (int, error) {
	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		status := types.WorkflowStatus(req.Status)
		wfs, _, err := a.store.ListWorkflowInstances(ctx, workflowFilterByStatus(&status, maxPageLimit))
		if err != nil {
			return 0, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
		}
		for _, wf := range wfs {
			ids = append(ids, wf.ID)
		}
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
	return affected, nil
}

func (a *APIService) BulkDeleteWorkflows(ctx context.Context, req BulkRequest) (int, error) {
	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		status := types.WorkflowStatus(req.Status)
		wfs, _, err := a.store.ListWorkflowInstances(ctx, workflowFilterByStatus(&status, maxPageLimit))
		if err != nil {
			return 0, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
		}
		for _, wf := range wfs {
			ids = append(ids, wf.ID)
		}
	}

	affected := 0
	for _, id := range ids {
		if err := a.store.DeleteWorkflow(ctx, id); err == nil {
			affected++
		}
	}
	return affected, nil
}

func (a *APIService) BulkCancelBatches(ctx context.Context, req BulkRequest) (int, error) {
	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		status := types.WorkflowStatus(req.Status)
		batches, _, err := a.store.ListBatchInstances(ctx, batchFilterByStatus(&status, maxPageLimit))
		if err != nil {
			return 0, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
		}
		for _, b := range batches {
			ids = append(ids, b.ID)
		}
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

		if _, err := a.store.UpdateBatch(ctx, batch, rev); err == nil {
			a.dispatcher.PublishEvent(types.JobEvent{
				Type: types.EventBatchCancelled, JobID: id, Timestamp: now,
			})
			affected++
		}
	}
	return affected, nil
}

func (a *APIService) BulkRetryBatches(ctx context.Context, req BulkRequest) (int, error) {
	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		status := types.WorkflowStatus(req.Status)
		batches, _, err := a.store.ListBatchInstances(ctx, batchFilterByStatus(&status, maxPageLimit))
		if err != nil {
			return 0, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
		}
		for _, b := range batches {
			ids = append(ids, b.ID)
		}
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
	return affected, nil
}

func (a *APIService) BulkDeleteBatches(ctx context.Context, req BulkRequest) (int, error) {
	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		status := types.WorkflowStatus(req.Status)
		batches, _, err := a.store.ListBatchInstances(ctx, batchFilterByStatus(&status, maxPageLimit))
		if err != nil {
			return 0, &ApiError{Msg: err.Error(), StatusCode: http.StatusInternalServerError}
		}
		for _, b := range batches {
			ids = append(ids, b.ID)
		}
	}

	affected := 0
	for _, id := range ids {
		if err := a.store.DeleteBatch(ctx, id); err == nil {
			affected++
		}
	}
	return affected, nil
}
