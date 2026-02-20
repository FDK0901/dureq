package monitor

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
)

// bulkRequest is the common shape for bulk operation requests.
type bulkRequest struct {
	// IDs specifies individual IDs to operate on.
	IDs []string `json:"ids"`
	// Status selects all entities in a given status (alternative to IDs).
	Status string `json:"status"`
}

type bulkResult struct {
	Affected int `json:"affected"`
}

func (a *API) parseBulkRequest(r *http.Request) (bulkRequest, error) {
	var req bulkRequest
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

// --- Job bulk ---

func (a *API) bulkCancelJobs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := a.parseBulkRequest(r)
	if err != nil {
		a.jsonError(w, err, http.StatusBadRequest)
		return
	}

	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		jobs, _, err := a.store.ListJobsPaginated(ctx, jobFilterByStatus(req.Status, maxPageLimit))
		if err != nil {
			a.jsonError(w, err, http.StatusInternalServerError)
			return
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
	a.jsonOK(w, bulkResult{Affected: affected})
}

func (a *API) bulkRetryJobs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := a.parseBulkRequest(r)
	if err != nil {
		a.jsonError(w, err, http.StatusBadRequest)
		return
	}

	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		jobs, _, err := a.store.ListJobsPaginated(ctx, jobFilterByStatus(req.Status, maxPageLimit))
		if err != nil {
			a.jsonError(w, err, http.StatusInternalServerError)
			return
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
	a.jsonOK(w, bulkResult{Affected: affected})
}

func (a *API) bulkDeleteJobs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := a.parseBulkRequest(r)
	if err != nil {
		a.jsonError(w, err, http.StatusBadRequest)
		return
	}

	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		jobs, _, err := a.store.ListJobsPaginated(ctx, jobFilterByStatus(req.Status, maxPageLimit))
		if err != nil {
			a.jsonError(w, err, http.StatusInternalServerError)
			return
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
	a.jsonOK(w, bulkResult{Affected: affected})
}

// --- Workflow bulk ---

func (a *API) bulkCancelWorkflows(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := a.parseBulkRequest(r)
	if err != nil {
		a.jsonError(w, err, http.StatusBadRequest)
		return
	}

	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		status := types.WorkflowStatus(req.Status)
		wfs, _, err := a.store.ListWorkflowInstances(ctx, workflowFilterByStatus(&status, maxPageLimit))
		if err != nil {
			a.jsonError(w, err, http.StatusInternalServerError)
			return
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
	a.jsonOK(w, bulkResult{Affected: affected})
}

func (a *API) bulkRetryWorkflows(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := a.parseBulkRequest(r)
	if err != nil {
		a.jsonError(w, err, http.StatusBadRequest)
		return
	}

	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		status := types.WorkflowStatus(req.Status)
		wfs, _, err := a.store.ListWorkflowInstances(ctx, workflowFilterByStatus(&status, maxPageLimit))
		if err != nil {
			a.jsonError(w, err, http.StatusInternalServerError)
			return
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
	a.jsonOK(w, bulkResult{Affected: affected})
}

func (a *API) bulkDeleteWorkflows(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := a.parseBulkRequest(r)
	if err != nil {
		a.jsonError(w, err, http.StatusBadRequest)
		return
	}

	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		status := types.WorkflowStatus(req.Status)
		wfs, _, err := a.store.ListWorkflowInstances(ctx, workflowFilterByStatus(&status, maxPageLimit))
		if err != nil {
			a.jsonError(w, err, http.StatusInternalServerError)
			return
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
	a.jsonOK(w, bulkResult{Affected: affected})
}

// --- Batch bulk ---

func (a *API) bulkCancelBatches(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := a.parseBulkRequest(r)
	if err != nil {
		a.jsonError(w, err, http.StatusBadRequest)
		return
	}

	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		status := types.WorkflowStatus(req.Status)
		batches, _, err := a.store.ListBatchInstances(ctx, batchFilterByStatus(&status, maxPageLimit))
		if err != nil {
			a.jsonError(w, err, http.StatusInternalServerError)
			return
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

		// Cancel in-flight child jobs.
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
	a.jsonOK(w, bulkResult{Affected: affected})
}

func (a *API) bulkRetryBatches(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := a.parseBulkRequest(r)
	if err != nil {
		a.jsonError(w, err, http.StatusBadRequest)
		return
	}

	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		status := types.WorkflowStatus(req.Status)
		batches, _, err := a.store.ListBatchInstances(ctx, batchFilterByStatus(&status, maxPageLimit))
		if err != nil {
			a.jsonError(w, err, http.StatusInternalServerError)
			return
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
	a.jsonOK(w, bulkResult{Affected: affected})
}

func (a *API) bulkDeleteBatches(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := a.parseBulkRequest(r)
	if err != nil {
		a.jsonError(w, err, http.StatusBadRequest)
		return
	}

	ids := req.IDs
	if req.Status != "" && len(ids) == 0 {
		status := types.WorkflowStatus(req.Status)
		batches, _, err := a.store.ListBatchInstances(ctx, batchFilterByStatus(&status, maxPageLimit))
		if err != nil {
			a.jsonError(w, err, http.StatusInternalServerError)
			return
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
	a.jsonOK(w, bulkResult{Affected: affected})
}

// --- Helpers ---

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
