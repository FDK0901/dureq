package monitor

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/FDK0901/dureq/pkg/types"
)

func post(t *testing.T, srv *httptest.Server, path, body string, out any) {
	t.Helper()
	resp, err := http.Post(srv.URL+path, "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("POST %s: %d", path, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatal(err)
	}
}

// A job that is deleted or changed by hand is announced.
func TestJobChanges_AreAnnounced(t *testing.T) {
	st, srv := newTestAPI(t, nil)
	createJob(t, st, "changed", "t", types.JobStatusScheduled, 0)
	createJob(t, st, "deleted", "t", types.JobStatusCompleted, 0)
	createJob(t, st, "bulk", "t", types.JobStatusCompleted, 0)
	req, _ := http.NewRequest("PUT", srv.URL+"/api/jobs/changed/payload", strings.NewReader(`{"payload":{"a":1}}`))
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != 200 {
		t.Fatalf("update: %v %v", resp, err)
	}
	if code := call(t, srv, "DELETE", "/api/jobs/deleted", nil); code != 200 {
		t.Fatalf("delete: %d", code)
	}
	var res BulkResult
	post(t, srv, "/api/jobs/bulk/delete", `{"ids":["bulk","none"]}`, &res)
	for id, want := range map[string]types.EventType{"changed": types.EventJobUpdated, "deleted": types.EventJobDeleted, "bulk": types.EventJobDeleted} {
		var events []types.JobEvent
		get(t, srv, "/api/jobs/"+id+"/events", &events)
		if len(events) != 1 || events[0].Type != want || events[0].TaskType != "t" {
			t.Errorf("job %s: events %+v, want %s", id, events, want)
		}
	}
	var events []types.JobEvent
	get(t, srv, "/api/jobs/changed/events", &events)
	if events[0].Status != types.JobStatusScheduled {
		t.Errorf("status %q", events[0].Status)
	}
}
