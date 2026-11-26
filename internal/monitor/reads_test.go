package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/FDK0901/dureq/pkg/types"
)

func TestETag(t *testing.T) {
	_, srv := newTestAPI(t, nil)
	fetch := func(etag string) (int, string, string) {
		t.Helper()
		req, _ := http.NewRequest("GET", srv.URL+"/api/stats", nil)
		if etag != "" {
			req.Header.Set("If-None-Match", etag)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, resp.Header.Get("ETag"), string(body)
	}
	code, etag, body := fetch("")
	if code != 200 || etag == "" || !json.Valid([]byte(body)) {
		t.Fatalf("%d %q %q", code, etag, body)
	}
	if code, again, body := fetch(etag); code != 304 || again != etag || body != "" {
		t.Errorf("unchanged: %d %q %q", code, again, body)
	}
	if code, _, body := fetch(`W/"old"`); code != 200 || body == "" {
		t.Errorf("changed: %d %q", code, body)
	}
	if code := call(t, srv, "GET", "/api/jobs/none", nil); code != 404 {
		t.Errorf("an error answers %d", code)
	}
}

// What a list has to look up for its rows takes one request.
func TestByIDs(t *testing.T) {
	st, srv := newTestAPI(t, nil)
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		createJob(t, st, id, "t", types.JobStatusPending, 0)
		for i := range 3 {
			run := &types.JobRun{ID: fmt.Sprintf("%s%d", id, i), JobID: id, Status: types.RunStatusSucceeded, StartedAt: time.Now().Add(time.Duration(i) * time.Second)}
			if err := st.SaveJobRun(ctx, run); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := st.SaveRun(ctx, &types.JobRun{ID: id + "-active", JobID: id, Status: types.RunStatusRunning, StartedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	var jobs listPage[types.Job]
	post(t, srv, "/api/jobs/by-ids", `{"ids":["b","none","a"]}`, &jobs)
	if len(jobs.Data) != 2 || jobs.Data[0].ID != "b" || jobs.Data[1].ID != "a" {
		t.Errorf("jobs %+v", jobs.Data)
	}
	var runs struct {
		Data map[string][]types.JobRun `json:"data"`
	}
	post(t, srv, "/api/runs/by-jobs", `{"job_ids":["a","c","none"],"limit":2}`, &runs)
	if len(runs.Data) != 2 || len(runs.Data["a"]) != 2 || runs.Data["a"][0].ID != "a2" || runs.Data["c"][1].ID != "c1" {
		t.Errorf("runs %+v", runs.Data)
	}
	post(t, srv, "/api/runs/by-jobs", `{"job_ids":[]}`, &runs)
	var active []types.JobRun
	get(t, srv, "/api/runs?job_ids=a,c", &active)
	if len(active) != 2 {
		t.Errorf("%d active runs of two jobs", len(active))
	}
	get(t, srv, "/api/runs?limit=1", &active)
	if len(active) != 1 {
		t.Errorf("%d active runs, want 1", len(active))
	}
	get(t, srv, "/api/runs", &active)
	if len(active) != 3 {
		t.Errorf("%d active runs, want all 3", len(active))
	}
}

func TestSummaries(t *testing.T) {
	st, srv := newTestAPI(t, nil)
	ctx := context.Background()
	if _, err := st.SaveWorkflow(ctx, &types.WorkflowInstance{ID: "w", Status: types.WorkflowStatusRunning, CreatedAt: time.Now(), Input: json.RawMessage(`{"x":1}`),
		Definition: types.WorkflowDefinition{Name: "n", Tasks: []types.WorkflowTask{{Name: "a"}}}, Tasks: map[string]types.WorkflowTaskState{"a": {Name: "a"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SaveBatch(ctx, &types.BatchInstance{ID: "b", Status: types.WorkflowStatusRunning, CreatedAt: time.Now(), TotalItems: 1,
		Definition: types.BatchDefinition{Name: "n", Items: []types.BatchItem{{ID: "i"}}}, ItemStates: map[string]types.BatchItemState{"i": {ItemID: "i"}}}); err != nil {
		t.Fatal(err)
	}
	var wfs listPage[types.WorkflowInstance]
	get(t, srv, "/api/workflows?view=summary", &wfs)
	if wf := wfs.Data[0]; len(wf.Definition.Tasks) != 0 || wf.Input != nil || len(wf.Tasks) != 1 || wf.Definition.Name != "n" {
		t.Errorf("workflow %+v", wf)
	}
	var batches listPage[types.BatchInstance]
	get(t, srv, "/api/batches?view=summary", &batches)
	if b := batches.Data[0]; len(b.Definition.Items) != 0 || len(b.ItemStates) != 0 || b.TotalItems != 1 {
		t.Errorf("batch %+v", b)
	}
	get(t, srv, "/api/batches", &batches)
	if b := batches.Data[0]; len(b.Definition.Items) != 1 || len(b.ItemStates) != 1 {
		t.Errorf("in full: %+v", b)
	}
}

func TestNodes_Draining(t *testing.T) {
	st, srv := newTestAPI(t, nil)
	ctx := context.Background()
	for _, id := range []string{"a", "b"} {
		if _, err := st.SaveNode(ctx, &types.NodeInfo{NodeID: id}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.SetNodeDrain(ctx, "b", true); err != nil {
		t.Fatal(err)
	}
	var nodes []types.NodeInfo
	get(t, srv, "/api/nodes", &nodes)
	if len(nodes) != 2 {
		t.Fatalf("%d nodes", len(nodes))
	}
	for _, n := range nodes {
		if n.Draining != (n.NodeID == "b") {
			t.Errorf("node %s draining: %v", n.NodeID, n.Draining)
		}
	}
	// One node says so, too.
	for _, id := range []string{"a", "b"} {
		var n types.NodeInfo
		get(t, srv, "/api/nodes/"+id, &n)
		if n.Draining != (id == "b") {
			t.Errorf("GET /api/nodes/%s: draining %v", id, n.Draining)
		}
	}
}

// Events of one job can be listed without counting them all.
func TestHistoryEvents_WithoutTotal(t *testing.T) {
	st, srv := newTestAPI(t, nil)
	for range 5 {
		publish(t, st, types.JobEvent{Type: types.EventJobStarted, JobID: "a", Timestamp: time.Now()})
		publish(t, st, types.JobEvent{Type: types.EventJobStarted, JobID: "b", Timestamp: time.Now()})
	}
	for _, tc := range []struct {
		query string
		count int
		more  bool
	}{{"limit=3", 3, true}, {"limit=3&offset=3", 2, false}, {"limit=5", 5, false}} {
		var page struct {
			Data    []types.JobEvent `json:"data"`
			Total   *int             `json:"total"`
			HasMore bool             `json:"has_more"`
		}
		get(t, srv, "/api/history/events?job_id=a&total=false&"+tc.query, &page)
		if len(page.Data) != tc.count || page.HasMore != tc.more || page.Total != nil {
			t.Errorf("%s: %d events, more: %v, total %v", tc.query, len(page.Data), page.HasMore, page.Total)
		}
	}
}
