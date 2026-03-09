package monitor

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
)

// SyncRetryStatsFunc is a function that returns the current SyncRetrier stats
// as a JSON-serializable value. Set via SetSyncRetryStatsFunc.
type SyncRetryStatsFunc func() any

// API serves the monitoring HTTP endpoints for the React UI.
type API struct {
	svc *APIService
	mux *http.ServeMux
}

// NewAPI creates a new monitoring API backed by Redis.
func NewAPI(s *store.RedisStore, disp Dispatcher, logger gochainedlog.Logger) *API {
	svc := NewAPIService(s, disp, logger)
	api := &API{svc: svc, mux: http.NewServeMux()}
	api.registerRoutes()
	return api
}

// SetSyncRetryStatsFunc sets the function used to retrieve SyncRetrier stats.
func (a *API) SetSyncRetryStatsFunc(fn SyncRetryStatsFunc) {
	a.svc.SetSyncRetryStatsFunc(fn)
}

// Handler returns the HTTP handler for mounting in a server.
func (a *API) Handler() http.Handler {
	return corsMiddleware(a.mux)
}

// Shutdown cancels the hub's Redis subscription.
func (a *API) Shutdown() {
	a.svc.Shutdown()
}

// Service returns the underlying APIService (used by gRPC server).
func (a *API) Service() *APIService {
	return a.svc
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

	// Search (payload JSONPath)
	a.mux.HandleFunc("GET /api/search/jobs", a.searchJobs)
	a.mux.HandleFunc("GET /api/search/workflows", a.searchWorkflows)
	a.mux.HandleFunc("GET /api/search/batches", a.searchBatches)

	// Unique keys
	a.mux.HandleFunc("GET /api/unique-keys/{key}", a.getUniqueKey)
	a.mux.HandleFunc("DELETE /api/unique-keys/{key}", a.deleteUniqueKey)

	// Audit trail
	a.mux.HandleFunc("GET /api/jobs/{jobID}/audit", a.getJobAuditTrail)
	a.mux.HandleFunc("POST /api/audit/counts", a.getAuditCounts)
	a.mux.HandleFunc("GET /api/workflows/{workflowID}/audit", a.getWorkflowAuditTrail)

	// Node drain
	a.mux.HandleFunc("POST /api/nodes/{nodeID}/drain", a.drainNode)
	a.mux.HandleFunc("DELETE /api/nodes/{nodeID}/drain", a.undrainNode)
	a.mux.HandleFunc("GET /api/nodes/{nodeID}/drain", a.getNodeDrainStatus)

	// WebSocket
	a.mux.HandleFunc("GET /api/ws", HandleWebSocket(a.svc.Hub(), a.svc.Logger()))

	// Health
	a.mux.HandleFunc("GET /api/health", a.health)
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

	jobs, total, err := a.svc.ListJobs(r.Context(), filter)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, paginatedResponse{Data: jobs, Total: total})
}

func (a *API) getJob(w http.ResponseWriter, r *http.Request) {
	job, err := a.svc.GetJob(r.Context(), r.PathValue("jobID"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, job)
}

func (a *API) cancelJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")
	if err := a.svc.CancelJob(r.Context(), jobID); err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, map[string]string{"status": "cancelled", "job_id": jobID})
}

func (a *API) retryJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")
	if err := a.svc.RetryJob(r.Context(), jobID); err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, map[string]string{"status": "retried", "job_id": jobID})
}

func (a *API) deleteJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")
	if err := a.svc.DeleteJob(r.Context(), jobID); err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, map[string]string{"status": "deleted", "job_id": jobID})
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
	events, _, err := a.svc.ListJobEvents(r.Context(), store.EventFilter{
		JobID: &jobID,
		Limit: limit,
	})
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, events)
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
	runs, _, err := a.svc.ListJobRuns(r.Context(), store.RunFilter{
		JobID: &jobID,
		Limit: limit,
	})
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, runs)
}

// --- Schedules ---

func (a *API) listSchedules(w http.ResponseWriter, r *http.Request) {
	pp := parsePageParams(r)

	filter := store.ScheduleFilter{
		Sort:   pp.Sort,
		Limit:  pp.Limit,
		Offset: pp.Offset,
	}
	schedules, total, err := a.svc.ListSchedules(r.Context(), filter)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, paginatedResponse{Data: schedules, Total: total})
}

func (a *API) getSchedule(w http.ResponseWriter, r *http.Request) {
	sched, err := a.svc.GetSchedule(r.Context(), r.PathValue("jobID"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, sched)
}

// --- Nodes ---

func (a *API) listNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := a.svc.ListNodes(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, nodes)
}

func (a *API) getNode(w http.ResponseWriter, r *http.Request) {
	node, err := a.svc.GetNode(r.Context(), r.PathValue("nodeID"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, node)
}

// --- Runs ---

func (a *API) listRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := a.svc.ListActiveRuns(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, runs)
}

func (a *API) getRun(w http.ResponseWriter, r *http.Request) {
	run, err := a.svc.GetRun(r.Context(), r.PathValue("runID"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, run)
}

func (a *API) getRunProgress(w http.ResponseWriter, r *http.Request) {
	run, err := a.svc.GetRun(r.Context(), r.PathValue("runID"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, map[string]any{
		"run_id":            run.ID,
		"status":            run.Status,
		"progress":          run.Progress,
		"last_heartbeat_at": run.LastHeartbeatAt,
	})
}

func (a *API) getJobProgress(w http.ResponseWriter, r *http.Request) {
	runs, err := a.svc.GetJobActiveRuns(r.Context(), r.PathValue("jobID"))
	if err != nil {
		writeServiceError(w, err)
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
	jsonOK(w, result)
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
	if nid := r.URL.Query().Get("node_id"); nid != "" {
		filter.NodeID = &nid
	}
	if tt := r.URL.Query().Get("task_type"); tt != "" {
		taskType := types.TaskType(tt)
		filter.TaskType = &taskType
	}
	if since := r.URL.Query().Get("since"); since != "" {
		if t, err := time.Parse(time.RFC3339, since); err == nil {
			filter.Since = &t
		}
	}
	if until := r.URL.Query().Get("until"); until != "" {
		if t, err := time.Parse(time.RFC3339, until); err == nil {
			filter.Until = &t
		}
	}
	runs, total, err := a.svc.ListHistoryRuns(r.Context(), filter)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, paginatedResponse{Data: runs, Total: total})
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
	events, total, err := a.svc.ListHistoryEvents(r.Context(), filter)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, map[string]any{"data": events, "total": total})
}

// --- DLQ ---

func (a *API) listDLQ(w http.ResponseWriter, r *http.Request) {
	pp := parsePageParams(r)
	msgs, total, err := a.svc.ListDLQ(r.Context(), pp.Limit, pp.Offset)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, paginatedResponse{Data: msgs, Total: total})
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
	if name := r.URL.Query().Get("name"); name != "" {
		filter.WorkflowName = &name
	}
	workflows, total, err := a.svc.ListWorkflows(r.Context(), filter)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, paginatedResponse{Data: workflows, Total: total})
}

func (a *API) getWorkflow(w http.ResponseWriter, r *http.Request) {
	wf, err := a.svc.GetWorkflow(r.Context(), r.PathValue("workflowID"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, wf)
}

func (a *API) cancelWorkflow(w http.ResponseWriter, r *http.Request) {
	wfID := r.PathValue("workflowID")
	if err := a.svc.CancelWorkflow(r.Context(), wfID); err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, map[string]string{"status": "cancelled", "workflow_id": wfID})
}

func (a *API) retryWorkflow(w http.ResponseWriter, r *http.Request) {
	wfID := r.PathValue("workflowID")
	if err := a.svc.RetryWorkflow(r.Context(), wfID); err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, map[string]string{"status": "retried", "workflow_id": wfID})
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
	if name := r.URL.Query().Get("name"); name != "" {
		filter.Name = &name
	}
	batches, total, err := a.svc.ListBatches(r.Context(), filter)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, paginatedResponse{Data: batches, Total: total})
}

func (a *API) getBatch(w http.ResponseWriter, r *http.Request) {
	batch, err := a.svc.GetBatch(r.Context(), r.PathValue("batchID"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, batch)
}

func (a *API) getBatchResults(w http.ResponseWriter, r *http.Request) {
	results, err := a.svc.GetBatchResults(r.Context(), r.PathValue("batchID"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, results)
}

func (a *API) getBatchItemResult(w http.ResponseWriter, r *http.Request) {
	result, err := a.svc.GetBatchItemResult(r.Context(), r.PathValue("batchID"), r.PathValue("itemID"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, result)
}

func (a *API) cancelBatch(w http.ResponseWriter, r *http.Request) {
	batchID := r.PathValue("batchID")
	if err := a.svc.CancelBatch(r.Context(), batchID); err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, map[string]string{"status": "cancelled", "batch_id": batchID})
}

func (a *API) retryBatch(w http.ResponseWriter, r *http.Request) {
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

	if err := a.svc.RetryBatch(r.Context(), batchID, retryFailedOnly); err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, map[string]string{"status": "retried", "batch_id": batchID})
}

// --- Stats ---

func (a *API) getStats(w http.ResponseWriter, r *http.Request) {
	stats, err := a.svc.GetStats(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, stats)
}

func (a *API) getDailyStats(w http.ResponseWriter, r *http.Request) {
	days := 7
	if d := r.URL.Query().Get("days"); d != "" {
		if parsed, err := strconv.Atoi(d); err == nil && parsed > 0 && parsed <= 90 {
			days = parsed
		}
	}
	entries, err := a.svc.GetDailyStats(r.Context(), days)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, entries)
}

func (a *API) getRedisInfo(w http.ResponseWriter, r *http.Request) {
	sections, err := a.svc.GetRedisInfo(r.Context(), r.URL.Query().Get("section"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, sections)
}

func (a *API) getSyncRetries(w http.ResponseWriter, _ *http.Request) {
	jsonOK(w, a.svc.GetSyncRetries())
}

// --- Groups ---

func (a *API) listGroups(w http.ResponseWriter, r *http.Request) {
	groups, err := a.svc.ListGroups(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, groups)
}

// --- Queues ---

func (a *API) listQueues(w http.ResponseWriter, r *http.Request) {
	queues, err := a.svc.ListQueues(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, queues)
}

func (a *API) pauseQueue(w http.ResponseWriter, r *http.Request) {
	tierName := r.PathValue("tierName")
	if err := a.svc.PauseQueue(r.Context(), tierName); err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, map[string]string{"status": "paused", "queue": tierName})
}

func (a *API) resumeQueue(w http.ResponseWriter, r *http.Request) {
	tierName := r.PathValue("tierName")
	if err := a.svc.ResumeQueue(r.Context(), tierName); err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, map[string]string{"status": "resumed", "queue": tierName})
}

// --- Job Payload Update ---

func (a *API) updateJobPayload(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")

	var body struct {
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeServiceError(w, &ApiError{Msg: err.Error(), StatusCode: http.StatusBadRequest})
		return
	}

	if err := a.svc.UpdateJobPayload(r.Context(), jobID, body.Payload); err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, map[string]string{"status": "updated", "job_id": jobID})
}

// --- Search ---

func (a *API) searchJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	job, err := a.svc.SearchJobsByPayload(r.Context(), q.Get("path"), q.Get("value"), q.Get("task_type"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, job)
}

func (a *API) searchWorkflows(w http.ResponseWriter, r *http.Request) {
	wf, err := a.svc.SearchWorkflowsByPayload(r.Context(), r.URL.Query().Get("path"), r.URL.Query().Get("value"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, wf)
}

func (a *API) searchBatches(w http.ResponseWriter, r *http.Request) {
	batch, err := a.svc.SearchBatchesByPayload(r.Context(), r.URL.Query().Get("path"), r.URL.Query().Get("value"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, batch)
}

// --- Unique Keys ---

func (a *API) getUniqueKey(w http.ResponseWriter, r *http.Request) {
	result, err := a.svc.CheckUniqueKey(r.Context(), r.PathValue("key"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, result)
}

func (a *API) deleteUniqueKey(w http.ResponseWriter, r *http.Request) {
	if err := a.svc.DeleteUniqueKey(r.Context(), r.PathValue("key")); err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, map[string]string{"status": "deleted"})
}

// --- Bulk ---

func (a *API) parseBulkRequest(r *http.Request) (BulkRequest, error) {
	var req BulkRequest
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return req, err
		}
	}
	if len(req.IDs) > maxPageLimit {
		req.IDs = req.IDs[:maxPageLimit]
	}
	return req, nil
}

func (a *API) bulkCancelJobs(w http.ResponseWriter, r *http.Request) {
	req, err := a.parseBulkRequest(r)
	if err != nil {
		writeServiceError(w, &ApiError{Msg: err.Error(), StatusCode: http.StatusBadRequest})
		return
	}
	result, err := a.svc.BulkCancelJobs(r.Context(), req)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, result)
}

func (a *API) bulkRetryJobs(w http.ResponseWriter, r *http.Request) {
	req, err := a.parseBulkRequest(r)
	if err != nil {
		writeServiceError(w, &ApiError{Msg: err.Error(), StatusCode: http.StatusBadRequest})
		return
	}
	result, err := a.svc.BulkRetryJobs(r.Context(), req)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, result)
}

func (a *API) bulkDeleteJobs(w http.ResponseWriter, r *http.Request) {
	req, err := a.parseBulkRequest(r)
	if err != nil {
		writeServiceError(w, &ApiError{Msg: err.Error(), StatusCode: http.StatusBadRequest})
		return
	}
	result, err := a.svc.BulkDeleteJobs(r.Context(), req)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, result)
}

func (a *API) bulkCancelWorkflows(w http.ResponseWriter, r *http.Request) {
	req, err := a.parseBulkRequest(r)
	if err != nil {
		writeServiceError(w, &ApiError{Msg: err.Error(), StatusCode: http.StatusBadRequest})
		return
	}
	result, err := a.svc.BulkCancelWorkflows(r.Context(), req)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, result)
}

func (a *API) bulkRetryWorkflows(w http.ResponseWriter, r *http.Request) {
	req, err := a.parseBulkRequest(r)
	if err != nil {
		writeServiceError(w, &ApiError{Msg: err.Error(), StatusCode: http.StatusBadRequest})
		return
	}
	result, err := a.svc.BulkRetryWorkflows(r.Context(), req)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, result)
}

func (a *API) bulkDeleteWorkflows(w http.ResponseWriter, r *http.Request) {
	req, err := a.parseBulkRequest(r)
	if err != nil {
		writeServiceError(w, &ApiError{Msg: err.Error(), StatusCode: http.StatusBadRequest})
		return
	}
	result, err := a.svc.BulkDeleteWorkflows(r.Context(), req)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, result)
}

func (a *API) bulkCancelBatches(w http.ResponseWriter, r *http.Request) {
	req, err := a.parseBulkRequest(r)
	if err != nil {
		writeServiceError(w, &ApiError{Msg: err.Error(), StatusCode: http.StatusBadRequest})
		return
	}
	result, err := a.svc.BulkCancelBatches(r.Context(), req)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, result)
}

func (a *API) bulkRetryBatches(w http.ResponseWriter, r *http.Request) {
	req, err := a.parseBulkRequest(r)
	if err != nil {
		writeServiceError(w, &ApiError{Msg: err.Error(), StatusCode: http.StatusBadRequest})
		return
	}
	result, err := a.svc.BulkRetryBatches(r.Context(), req)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, result)
}

func (a *API) bulkDeleteBatches(w http.ResponseWriter, r *http.Request) {
	req, err := a.parseBulkRequest(r)
	if err != nil {
		writeServiceError(w, &ApiError{Msg: err.Error(), StatusCode: http.StatusBadRequest})
		return
	}
	result, err := a.svc.BulkDeleteBatches(r.Context(), req)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, result)
}

// --- Health ---

func (a *API) health(w http.ResponseWriter, _ *http.Request) {
	jsonOK(w, map[string]string{"status": "ok"})
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

// --- Helpers ---

func jsonOK(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func writeServiceError(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	if apiErr, ok := err.(*ApiError); ok {
		code = apiErr.StatusCode
	}
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

// --- Filter helpers (used by both api_service.go and grpc) ---

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

// --- Audit Trail ---

func (a *API) getJobAuditTrail(w http.ResponseWriter, r *http.Request) {
	entries, err := a.svc.GetJobAuditTrail(r.Context(), r.PathValue("jobID"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, entries)
}

func (a *API) getAuditCounts(w http.ResponseWriter, r *http.Request) {
	var body struct {
		IDs []string `json:"ids"`
	}
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeServiceError(w, &ApiError{Msg: err.Error(), StatusCode: http.StatusBadRequest})
			return
		}
	}
	if len(body.IDs) > maxPageLimit {
		body.IDs = body.IDs[:maxPageLimit]
	}
	counts, err := a.svc.GetJobAuditCounts(r.Context(), body.IDs)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, counts)
}

func (a *API) getWorkflowAuditTrail(w http.ResponseWriter, r *http.Request) {
	entries, err := a.svc.GetWorkflowAuditTrail(r.Context(), r.PathValue("workflowID"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, entries)
}

// --- Node Drain ---

func (a *API) drainNode(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("nodeID")
	if err := a.svc.SetNodeDrain(r.Context(), nodeID, true); err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, map[string]string{"status": "draining", "node_id": nodeID})
}

func (a *API) undrainNode(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("nodeID")
	if err := a.svc.SetNodeDrain(r.Context(), nodeID, false); err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, map[string]string{"status": "active", "node_id": nodeID})
}

func (a *API) getNodeDrainStatus(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("nodeID")
	draining, err := a.svc.IsNodeDraining(r.Context(), nodeID)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	jsonOK(w, map[string]any{"node_id": nodeID, "draining": draining})
}
