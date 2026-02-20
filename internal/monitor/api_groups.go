package monitor

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/FDK0901/dureq/pkg/types"
)

// updateJobPayload replaces the payload of a pending/scheduled job.
func (a *API) updateJobPayload(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	jobID := r.PathValue("jobID")

	var body struct {
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		a.jsonError(w, err, http.StatusBadRequest)
		return
	}
	if len(body.Payload) == 0 {
		a.jsonError(w, &apiError{msg: "payload is required"}, http.StatusBadRequest)
		return
	}

	job, rev, err := a.store.GetJob(ctx, jobID)
	if err != nil {
		a.jsonError(w, err, http.StatusNotFound)
		return
	}

	// Only allow payload update for non-running, non-terminal jobs.
	if job.Status.IsTerminal() || job.Status == types.JobStatusRunning {
		a.jsonError(w, &apiError{msg: "can only update payload of pending or scheduled jobs"}, http.StatusBadRequest)
		return
	}

	job.Payload = body.Payload
	job.UpdatedAt = time.Now()

	if _, err := a.store.UpdateJob(ctx, job, rev); err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}

	a.jsonOK(w, map[string]string{"status": "updated", "job_id": jobID})
}

// listGroups returns all active aggregation groups.
func (a *API) listGroups(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	groups, err := a.store.ListGroups(ctx)
	if err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}

	type groupInfo struct {
		Name string `json:"name"`
		Size int64  `json:"size"`
	}

	result := make([]groupInfo, 0, len(groups))
	for _, g := range groups {
		size, _ := a.store.GetGroupSize(ctx, g)
		result = append(result, groupInfo{Name: g, Size: size})
	}

	a.jsonOK(w, result)
}
