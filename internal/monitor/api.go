package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
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

// API serves the monitoring HTTP endpoints for the React UI.
type API struct {
	store      *store.RedisStore
	dispatcher Dispatcher
	logger     gochainedlog.Logger
	mux        *http.ServeMux

	hub       *Hub
	hubCancel context.CancelFunc

	syncRetryStats SyncRetryStatsFunc
}

// NewAPI creates a new monitoring API backed by Redis.
func NewAPI(s *store.RedisStore, disp Dispatcher, logger gochainedlog.Logger) *API {
	if logger == nil {
		logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}

	hubCtx, hubCancel := context.WithCancel(context.Background())
	hub := newHub(logger)
	go hub.subscribeRedis(hubCtx, s.Client(), store.EventsPubSubChannel(s.Prefix()))

	api := &API{
		store:      s,
		dispatcher: disp,
		logger:     logger,
		mux:        http.NewServeMux(),
		hub:        hub,
		hubCancel:  hubCancel,
	}
	api.registerRoutes()
	return api
}

// SetSyncRetryStatsFunc sets the function used to retrieve SyncRetrier stats.
func (a *API) SetSyncRetryStatsFunc(fn SyncRetryStatsFunc) {
	a.syncRetryStats = fn
}

// Handler returns the HTTP handler for mounting in a server.
func (a *API) Handler() http.Handler {
	return corsMiddleware(a.mux)
}

func (a *API) registerRoutes() {
	// Jobs
	a.mux.HandleFunc("GET /api/jobs", a.listJobs)
	a.mux.HandleFunc("GET /api/jobs/{jobID}", a.getJob)
	a.mux.HandleFunc("DELETE /api/jobs/{jobID}", a.deleteJob)
	a.mux.HandleFunc("POST /api/jobs/{jobID}/cancel", a.cancelJob)
	a.mux.HandleFunc("POST /api/jobs/{jobID}/retry", a.retryJob)

	// Job history
	a.mux.HandleFunc("GET /api/jobs/{jobID}/events", a.listJobEvents)
	a.mux.HandleFunc("GET /api/jobs/{jobID}/runs", a.listJobRuns)

	// Schedules
	a.mux.HandleFunc("GET /api/schedules", a.listSchedules)
	a.mux.HandleFunc("GET /api/schedules/{jobID}", a.getSchedule)

	// Nodes
	a.mux.HandleFunc("GET /api/nodes", a.listNodes)
	a.mux.HandleFunc("GET /api/nodes/{nodeID}", a.getNode)

	// Runs (active)
	a.mux.HandleFunc("GET /api/runs", a.listRuns)
	a.mux.HandleFunc("GET /api/runs/{runID}", a.getRun)
	a.mux.HandleFunc("GET /api/runs/{runID}/progress", a.getRunProgress)
	a.mux.HandleFunc("GET /api/jobs/{jobID}/progress", a.getJobProgress)

	// History
	a.mux.HandleFunc("GET /api/history/runs", a.listHistoryRuns)
	a.mux.HandleFunc("GET /api/history/events", a.listHistoryEvents)

	// DLQ
	a.mux.HandleFunc("GET /api/dlq", a.listDLQ)

	// Workflows
	a.mux.HandleFunc("GET /api/workflows", a.listWorkflows)
	a.mux.HandleFunc("GET /api/workflows/{workflowID}", a.getWorkflow)
	a.mux.HandleFunc("POST /api/workflows/{workflowID}/cancel", a.cancelWorkflow)
	a.mux.HandleFunc("POST /api/workflows/{workflowID}/retry", a.retryWorkflow)

	// Batches
	a.mux.HandleFunc("GET /api/batches", a.listBatches)
	a.mux.HandleFunc("GET /api/batches/{batchID}", a.getBatch)
	a.mux.HandleFunc("GET /api/batches/{batchID}/results", a.getBatchResults)
	a.mux.HandleFunc("GET /api/batches/{batchID}/results/{itemID}", a.getBatchItemResult)
	a.mux.HandleFunc("POST /api/batches/{batchID}/cancel", a.cancelBatch)
	a.mux.HandleFunc("POST /api/batches/{batchID}/retry", a.retryBatch)

	// Job payload update
	a.mux.HandleFunc("PUT /api/jobs/{jobID}/payload", a.updateJobPayload)

	// Groups (aggregation)
	a.mux.HandleFunc("GET /api/groups", a.listGroups)

	// Queues
	a.mux.HandleFunc("GET /api/queues", a.listQueues)
	a.mux.HandleFunc("POST /api/queues/{tierName}/pause", a.pauseQueue)
	a.mux.HandleFunc("POST /api/queues/{tierName}/resume", a.resumeQueue)

	// Bulk operations
	a.mux.HandleFunc("POST /api/jobs/bulk/cancel", a.bulkCancelJobs)
	a.mux.HandleFunc("POST /api/jobs/bulk/retry", a.bulkRetryJobs)
	a.mux.HandleFunc("POST /api/jobs/bulk/delete", a.bulkDeleteJobs)
	a.mux.HandleFunc("POST /api/workflows/bulk/cancel", a.bulkCancelWorkflows)
	a.mux.HandleFunc("POST /api/workflows/bulk/retry", a.bulkRetryWorkflows)
	a.mux.HandleFunc("POST /api/workflows/bulk/delete", a.bulkDeleteWorkflows)
	a.mux.HandleFunc("POST /api/batches/bulk/cancel", a.bulkCancelBatches)
	a.mux.HandleFunc("POST /api/batches/bulk/retry", a.bulkRetryBatches)
	a.mux.HandleFunc("POST /api/batches/bulk/delete", a.bulkDeleteBatches)

	// Daily stats
	a.mux.HandleFunc("GET /api/stats/daily", a.getDailyStats)

	// Redis info
	a.mux.HandleFunc("GET /api/redis/info", a.getRedisInfo)

	// Stats
	a.mux.HandleFunc("GET /api/stats", a.getStats)

	// Sync Retries
	a.mux.HandleFunc("GET /api/sync-retries", a.getSyncRetries)

	// Health
	a.mux.HandleFunc("GET /api/health", a.health)

	// WebSocket (real-time events)
	a.mux.HandleFunc("/api/ws", handleWebSocket(a.hub, a.logger))
}

// Shutdown stops the WebSocket hub's Redis subscription.
func (a *API) Shutdown() {
	a.hubCancel()
}

// --- Jobs ---

func (a *API) listJobs(w http.ResponseWriter, r *http.Request) {
	pp := parsePageParams(r)

	filter := store.JobFilter{
		Sort:   pp.Sort,
		Limit:  pp.Limit,
		Offset: pp.Offset,
	}
	if s := r.URL.Query().Get("status"); s != "" {
		status := types.JobStatus(s)
		filter.Status = &status
	}
	if tt := r.URL.Query().Get("task_type"); tt != "" {
		taskType := types.TaskType(tt)
		filter.TaskType = &taskType
	}
	if tag := r.URL.Query().Get("tag"); tag != "" {
		filter.Tag = &tag
	}
	if search := r.URL.Query().Get("search"); search != "" {
		filter.Search = &search
	}

	jobs, total, err := a.store.ListJobsPaginated(r.Context(), filter)
	if err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}
	a.jsonOK(w, paginatedResponse{Data: jobs, Total: total})
}

func (a *API) getJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")
	job, _, err := a.store.GetJob(r.Context(), jobID)
	if err != nil {
		a.jsonError(w, err, http.StatusNotFound)
		return
	}
	a.jsonOK(w, job)
}

func (a *API) cancelJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	jobID := r.PathValue("jobID")

	job, rev, err := a.store.GetJob(ctx, jobID)
	if err != nil {
		a.jsonError(w, err, http.StatusNotFound)
		return
	}
	if job.Status.IsTerminal() {
		a.jsonError(w, &apiError{msg: "job is already in terminal state"}, http.StatusBadRequest)
		return
	}

	now := time.Now()
	job.Status = types.JobStatusCancelled
	job.UpdatedAt = now

	if _, err := a.store.UpdateJob(ctx, job, rev); err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
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

	a.jsonOK(w, map[string]string{"status": "cancelled", "job_id": jobID})
}

func (a *API) retryJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	jobID := r.PathValue("jobID")

	job, rev, err := a.store.GetJob(ctx, jobID)
	if err != nil {
		a.jsonError(w, err, http.StatusNotFound)
		return
	}

	now := time.Now()
	job.Status = types.JobStatusPending
	job.Attempt = 0
	job.LastError = nil
	job.UpdatedAt = now

	if _, err := a.store.UpdateJob(ctx, job, rev); err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}

	if err := a.dispatcher.Dispatch(ctx, job, 0); err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}

	a.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventJobRetrying,
		JobID:     jobID,
		Timestamp: now,
	})

	a.jsonOK(w, map[string]string{"status": "retried", "job_id": jobID})
}

func (a *API) deleteJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	jobID := r.PathValue("jobID")

	if err := a.store.DeleteJob(ctx, jobID); err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}
	a.store.DeleteSchedule(ctx, jobID)

	a.jsonOK(w, map[string]string{"status": "deleted", "job_id": jobID})
}

func (a *API) listJobEvents(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")
	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if limit > maxPageLimit {
		limit = maxPageLimit
	}
	events, _, err := a.store.ListJobEvents(r.Context(), store.EventFilter{
		JobID: &jobID,
		Limit: limit,
	})
	if err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}
	a.jsonOK(w, events)
}

func (a *API) listJobRuns(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")
	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if limit > maxPageLimit {
		limit = maxPageLimit
	}
	runs, _, err := a.store.ListJobRuns(r.Context(), store.RunFilter{
		JobID: &jobID,
		Limit: limit,
	})
	if err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}
	a.jsonOK(w, runs)
}

// --- Schedules ---

func (a *API) listSchedules(w http.ResponseWriter, r *http.Request) {
	pp := parsePageParams(r)

	filter := store.ScheduleFilter{
		Sort:   pp.Sort,
		Limit:  pp.Limit,
		Offset: pp.Offset,
	}
	schedules, total, err := a.store.ListSchedulesPaginated(r.Context(), filter)
	if err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}
	a.jsonOK(w, paginatedResponse{Data: schedules, Total: total})
}

func (a *API) getSchedule(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")
	sched, _, err := a.store.GetSchedule(r.Context(), jobID)
	if err != nil {
		a.jsonError(w, err, http.StatusNotFound)
		return
	}
	a.jsonOK(w, sched)
}

// --- Nodes ---

func (a *API) listNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := a.store.ListNodes(r.Context())
	if err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}
	a.jsonOK(w, nodes)
}

func (a *API) getNode(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("nodeID")
	node, err := a.store.GetNode(r.Context(), nodeID)
	if err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}
	if node == nil {
		a.jsonError(w, types.ErrJobNotFound, http.StatusNotFound)
		return
	}
	a.jsonOK(w, node)
}

// --- Runs ---

func (a *API) listRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := a.store.ListRuns(r.Context())
	if err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}
	a.jsonOK(w, runs)
}

func (a *API) getRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("runID")
	run, _, err := a.store.GetRun(r.Context(), runID)
	if err != nil {
		a.jsonError(w, err, http.StatusNotFound)
		return
	}
	a.jsonOK(w, run)
}

func (a *API) getRunProgress(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("runID")
	run, _, err := a.store.GetRun(r.Context(), runID)
	if err != nil {
		a.jsonError(w, err, http.StatusNotFound)
		return
	}
	a.jsonOK(w, map[string]any{
		"run_id":            run.ID,
		"status":            run.Status,
		"progress":          run.Progress,
		"last_heartbeat_at": run.LastHeartbeatAt,
	})
}

func (a *API) getJobProgress(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")
	runs, err := a.store.ListActiveRunsByJobID(r.Context(), jobID)
	if err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}
	result := make([]map[string]any, 0, len(runs))
	for _, run := range runs {
		result = append(result, map[string]any{
			"run_id":            run.ID,
			"status":            run.Status,
			"progress":          run.Progress,
			"last_heartbeat_at": run.LastHeartbeatAt,
		})
	}
	a.jsonOK(w, result)
}

// --- History ---

func (a *API) listHistoryRuns(w http.ResponseWriter, r *http.Request) {
	pp := parsePageParams(r)

	filter := store.RunFilter{
		Sort:   pp.Sort,
		Limit:  pp.Limit,
		Offset: pp.Offset,
	}
	if s := r.URL.Query().Get("status"); s != "" {
		status := types.RunStatus(s)
		filter.Status = &status
	}
	if jid := r.URL.Query().Get("job_id"); jid != "" {
		filter.JobID = &jid
	}
	runs, total, err := a.store.ListJobRuns(r.Context(), filter)
	if err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}
	a.jsonOK(w, paginatedResponse{Data: runs, Total: total})
}

func (a *API) listHistoryEvents(w http.ResponseWriter, r *http.Request) {
	limit := 50
	offset := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if limit > maxPageLimit {
		limit = maxPageLimit
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil && parsed >= 0 {
			offset = parsed
		}
	}
	filter := store.EventFilter{Limit: limit, Offset: offset}
	if jid := r.URL.Query().Get("job_id"); jid != "" {
		filter.JobID = &jid
	}
	events, total, err := a.store.ListJobEvents(r.Context(), filter)
	if err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}
	a.jsonOK(w, map[string]any{"data": events, "total": total})
}

// --- DLQ ---

func (a *API) listDLQ(w http.ResponseWriter, r *http.Request) {
	pp := parsePageParams(r)
	msgs, err := a.store.ListDLQ(r.Context(), 1000)
	if err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}
	total := len(msgs)
	if pp.Offset < len(msgs) {
		msgs = msgs[pp.Offset:]
	} else {
		msgs = nil
	}
	if pp.Limit > 0 && len(msgs) > pp.Limit {
		msgs = msgs[:pp.Limit]
	}
	a.jsonOK(w, paginatedResponse{Data: msgs, Total: total})
}

// --- Workflows ---

func (a *API) listWorkflows(w http.ResponseWriter, r *http.Request) {
	pp := parsePageParams(r)

	filter := store.WorkflowFilter{
		Sort:   pp.Sort,
		Limit:  pp.Limit,
		Offset: pp.Offset,
	}
	if s := r.URL.Query().Get("status"); s != "" {
		status := types.WorkflowStatus(s)
		filter.Status = &status
	}
	workflows, total, err := a.store.ListWorkflowInstances(r.Context(), filter)
	if err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}
	a.jsonOK(w, paginatedResponse{Data: workflows, Total: total})
}

func (a *API) getWorkflow(w http.ResponseWriter, r *http.Request) {
	wfID := r.PathValue("workflowID")
	wf, _, err := a.store.GetWorkflow(r.Context(), wfID)
	if err != nil {
		a.jsonError(w, err, http.StatusNotFound)
		return
	}
	a.jsonOK(w, wf)
}

func (a *API) cancelWorkflow(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	wfID := r.PathValue("workflowID")

	wf, rev, err := a.store.GetWorkflow(ctx, wfID)
	if err != nil {
		a.jsonError(w, err, http.StatusNotFound)
		return
	}
	if wf.Status.IsTerminal() {
		a.jsonError(w, &apiError{msg: "workflow is already in terminal state"}, http.StatusBadRequest)
		return
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

			// Cancel the child job if it exists.
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

	if _, err := a.store.UpdateWorkflow(ctx, wf, rev); err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}

	a.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventWorkflowCancelled,
		JobID:     wfID,
		Timestamp: now,
	})

	a.jsonOK(w, map[string]string{"status": "cancelled", "workflow_id": wfID})
}

func (a *API) retryWorkflow(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	wfID := r.PathValue("workflowID")

	wf, rev, err := a.store.GetWorkflow(ctx, wfID)
	if err != nil {
		a.jsonError(w, err, http.StatusNotFound)
		return
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
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}

	a.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventWorkflowRetrying,
		JobID:     wfID,
		Attempt:   wf.Attempt,
		Timestamp: now,
	})

	a.jsonOK(w, map[string]string{"status": "retried", "workflow_id": wfID})
}

// --- Batches ---

func (a *API) listBatches(w http.ResponseWriter, r *http.Request) {
	pp := parsePageParams(r)

	filter := store.BatchFilter{
		Sort:   pp.Sort,
		Limit:  pp.Limit,
		Offset: pp.Offset,
	}
	if s := r.URL.Query().Get("status"); s != "" {
		status := types.WorkflowStatus(s)
		filter.Status = &status
	}
	batches, total, err := a.store.ListBatchInstances(r.Context(), filter)
	if err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}
	a.jsonOK(w, paginatedResponse{Data: batches, Total: total})
}

func (a *API) getBatch(w http.ResponseWriter, r *http.Request) {
	batchID := r.PathValue("batchID")
	batch, _, err := a.store.GetBatch(r.Context(), batchID)
	if err != nil {
		a.jsonError(w, err, http.StatusNotFound)
		return
	}
	a.jsonOK(w, batch)
}

func (a *API) getBatchResults(w http.ResponseWriter, r *http.Request) {
	batchID := r.PathValue("batchID")
	results, err := a.store.ListBatchItemResults(r.Context(), batchID)
	if err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}
	a.jsonOK(w, results)
}

func (a *API) getBatchItemResult(w http.ResponseWriter, r *http.Request) {
	batchID := r.PathValue("batchID")
	itemID := r.PathValue("itemID")
	result, err := a.store.GetBatchItemResult(r.Context(), batchID, itemID)
	if err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}
	if result == nil {
		a.jsonError(w, types.ErrBatchNotFound, http.StatusNotFound)
		return
	}
	a.jsonOK(w, result)
}

func (a *API) cancelBatch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	batchID := r.PathValue("batchID")

	batch, rev, err := a.store.GetBatch(ctx, batchID)
	if err != nil {
		a.jsonError(w, err, http.StatusNotFound)
		return
	}
	if batch.Status.IsTerminal() {
		a.jsonError(w, &apiError{msg: "batch is already in terminal state"}, http.StatusBadRequest)
		return
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
					}
				}
			}
		}
	}

	if _, err := a.store.UpdateBatch(ctx, batch, rev); err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}

	a.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventBatchCancelled,
		JobID:     batchID,
		Timestamp: now,
	})

	a.jsonOK(w, map[string]string{"status": "cancelled", "batch_id": batchID})
}

func (a *API) retryBatch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	batchID := r.PathValue("batchID")

	// Parse optional body for retry_failed_only flag.
	retryFailedOnly := false
	if r.Body != nil {
		var body struct {
			RetryFailedOnly bool `json:"retry_failed_only"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			retryFailedOnly = body.RetryFailedOnly
		}
	}

	batch, rev, err := a.store.GetBatch(ctx, batchID)
	if err != nil {
		a.jsonError(w, err, http.StatusNotFound)
		return
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
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}

	a.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventBatchRetrying,
		JobID:     batchID,
		Attempt:   batch.Attempt,
		Timestamp: now,
	})

	a.jsonOK(w, map[string]string{"status": "retried", "batch_id": batchID})
}

// --- Stats ---

type statsResponse struct {
	JobCounts       map[types.JobStatus]int `json:"job_counts"`
	ActiveSchedules int                     `json:"active_schedules"`
	ActiveRuns      int                     `json:"active_runs"`
	ActiveNodes     int                     `json:"active_nodes"`
}

func (a *API) getStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	jobCounts, err := a.store.GetActiveJobStats(ctx)
	if err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}

	schedules, _ := a.store.ListSchedules(ctx)
	runs, _ := a.store.ListRuns(ctx)
	nodes, _ := a.store.ListNodes(ctx)

	a.jsonOK(w, statsResponse{
		JobCounts:       jobCounts,
		ActiveSchedules: len(schedules),
		ActiveRuns:      len(runs),
		ActiveNodes:     len(nodes),
	})
}

// --- Sync Retries ---

func (a *API) getSyncRetries(w http.ResponseWriter, _ *http.Request) {
	if a.syncRetryStats == nil {
		a.jsonOK(w, map[string]any{"pending": 0, "failed": 0, "recent": []any{}})
		return
	}
	a.jsonOK(w, a.syncRetryStats())
}

// --- Health ---

func (a *API) health(w http.ResponseWriter, _ *http.Request) {
	a.jsonOK(w, map[string]string{"status": "ok"})
}

// --- Pagination ---

const maxPageLimit = 100

type pageParams struct {
	Limit  int
	Offset int
	Sort   string // "newest" or "oldest"
}

func parsePageParams(r *http.Request) pageParams {
	p := pageParams{Limit: 10, Sort: "newest"}
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			p.Limit = parsed
		}
	}
	if p.Limit > maxPageLimit {
		p.Limit = maxPageLimit
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil && parsed >= 0 {
			p.Offset = parsed
		}
	}
	if s := r.URL.Query().Get("sort"); s == "oldest" {
		p.Sort = "oldest"
	}
	return p
}

type paginatedResponse struct {
	Data  any `json:"data"`
	Total int `json:"total"`
}

type apiError struct{ msg string }

func (e *apiError) Error() string { return e.msg }

// --- Helpers ---

func (a *API) jsonOK(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (a *API) jsonError(w http.ResponseWriter, err error, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
