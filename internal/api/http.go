package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/FDK0901/dureq/pkg/types"
)

// HTTPApiClient talks to the dureq monitoring HTTP API.
type HTTPApiClient struct {
	base   string
	client *http.Client
}

func NewHTTPApiClient(baseURL string) *HTTPApiClient {
	return &HTTPApiClient{
		base:   baseURL,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// StatsResponse mirrors monitor.statsResponse.
type StatsResponse struct {
	JobCounts       map[string]int `json:"job_counts"`
	ActiveSchedules int            `json:"active_schedules"`
	ActiveRuns      int            `json:"active_runs"`
	ActiveNodes     int            `json:"active_nodes"`
}

// QueueInfo mirrors monitor.QueueInfo.
type QueueInfo struct {
	Name       string `json:"name"`
	Weight     int    `json:"weight"`
	FetchBatch int    `json:"fetch_batch"`
	Paused     bool   `json:"paused"`
	Size       int64  `json:"size"`
}

func (a *HTTPApiClient) GetStats() (*StatsResponse, error) {
	var s StatsResponse
	if err := a.getJSON("/api/stats", &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// --- Jobs ---

func (a *HTTPApiClient) ListJobs(status string) ([]*types.Job, error) {
	path := "/api/jobs?limit=100"
	if status != "" {
		path += "&status=" + status
	}
	var jobs []*types.Job
	if err := a.getPaginatedJSON(path, &jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

func (a *HTTPApiClient) GetJob(id string) (*types.Job, error) {
	var job types.Job
	if err := a.getJSON("/api/jobs/"+id, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

func (a *HTTPApiClient) CancelJob(id string) error {
	return a.postAction("/api/jobs/" + id + "/cancel")
}

func (a *HTTPApiClient) RetryJob(id string) error {
	return a.postAction("/api/jobs/" + id + "/retry")
}

// --- Nodes ---

func (a *HTTPApiClient) ListNodes() ([]*types.NodeInfo, error) {
	var nodes []*types.NodeInfo
	if err := a.getJSON("/api/nodes", &nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

// --- Schedules ---

func (a *HTTPApiClient) ListSchedules() ([]*types.ScheduleEntry, error) {
	var schedules []*types.ScheduleEntry
	if err := a.getPaginatedJSON("/api/schedules?limit=100", &schedules); err != nil {
		return nil, err
	}
	return schedules, nil
}

// --- DLQ ---

func (a *HTTPApiClient) ListDLQ(limit int) ([]json.RawMessage, error) {
	path := fmt.Sprintf("/api/dlq?limit=%d", limit)
	var msgs []json.RawMessage
	if err := a.getPaginatedJSON(path, &msgs); err != nil {
		return nil, err
	}
	return msgs, nil
}

// --- Workflows ---

func (a *HTTPApiClient) ListWorkflows(status string) ([]*types.WorkflowInstance, error) {
	path := "/api/workflows?limit=100"
	if status != "" {
		path += "&status=" + status
	}
	var wfs []*types.WorkflowInstance
	if err := a.getPaginatedJSON(path, &wfs); err != nil {
		return nil, err
	}
	return wfs, nil
}

func (a *HTTPApiClient) GetWorkflow(id string) (*types.WorkflowInstance, error) {
	var wf types.WorkflowInstance
	if err := a.getJSON("/api/workflows/"+id, &wf); err != nil {
		return nil, err
	}
	return &wf, nil
}

func (a *HTTPApiClient) CancelWorkflow(id string) error {
	return a.postAction("/api/workflows/" + id + "/cancel")
}

func (a *HTTPApiClient) RetryWorkflow(id string) error {
	return a.postAction("/api/workflows/" + id + "/retry")
}

// --- Batches ---

func (a *HTTPApiClient) ListBatches(status string) ([]*types.BatchInstance, error) {
	path := "/api/batches?limit=100"
	if status != "" {
		path += "&status=" + status
	}
	var batches []*types.BatchInstance
	if err := a.getPaginatedJSON(path, &batches); err != nil {
		return nil, err
	}
	return batches, nil
}

func (a *HTTPApiClient) GetBatch(id string) (*types.BatchInstance, error) {
	var b types.BatchInstance
	if err := a.getJSON("/api/batches/"+id, &b); err != nil {
		return nil, err
	}
	return &b, nil
}

func (a *HTTPApiClient) CancelBatch(id string) error {
	return a.postAction("/api/batches/" + id + "/cancel")
}

func (a *HTTPApiClient) RetryBatch(id string) error {
	return a.postAction("/api/batches/" + id + "/retry")
}

// --- Queues ---

func (a *HTTPApiClient) ListQueues() ([]QueueInfo, error) {
	var queues []QueueInfo
	if err := a.getJSON("/api/queues", &queues); err != nil {
		return nil, err
	}
	return queues, nil
}

func (a *HTTPApiClient) PauseQueue(tierName string) error {
	return a.postAction("/api/queues/" + tierName + "/pause")
}

func (a *HTTPApiClient) ResumeQueue(tierName string) error {
	return a.postAction("/api/queues/" + tierName + "/resume")
}

// --- History Runs ---

func (a *HTTPApiClient) ListHistoryRuns(status, jobID string) ([]*types.JobRun, error) {
	path := "/api/history/runs?limit=100"
	if status != "" {
		path += "&status=" + status
	}
	if jobID != "" {
		path += "&job_id=" + jobID
	}
	var runs []*types.JobRun
	if err := a.getPaginatedJSON(path, &runs); err != nil {
		return nil, err
	}
	return runs, nil
}

// --- Health ---

func (a *HTTPApiClient) Health() error {
	_, err := a.client.Get(a.base + "/api/health")
	return err
}

// --- Helpers ---

func (a *HTTPApiClient) getJSON(path string, out any) error {
	resp, err := a.client.Get(a.base + path)
	if err != nil {
		return fmt.Errorf("GET %s: %w", path, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return json.Unmarshal(body, out)
}

// getPaginatedJSON parses responses in the form {"data": [...], "total": N}.
func (a *HTTPApiClient) getPaginatedJSON(path string, out any) error {
	var wrapper struct {
		Data  json.RawMessage `json:"data"`
		Total int             `json:"total"`
	}
	if err := a.getJSON(path, &wrapper); err != nil {
		return err
	}
	return json.Unmarshal(wrapper.Data, out)
}

func (a *HTTPApiClient) postAction(path string) error {
	resp, err := a.client.Post(a.base+path, "application/json", nil)
	if err != nil {
		return fmt.Errorf("POST %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}
