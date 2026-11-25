package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/dispatcher"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/redis/rueidis"
	"github.com/rs/xid"
)

// newTestAPI serves the API on the standalone Redis at REDIS_URL (password
// REDIS_PASSWORD) under a unique key prefix. Skips if Redis is unavailable.
func newTestAPI(t *testing.T, disp Dispatcher) (*store.RedisStore, *httptest.Server) {
	t.Helper()
	addr := strings.TrimPrefix(os.Getenv("REDIS_URL"), "redis://")
	if addr == "" {
		t.Skip("REDIS_URL not set")
	}
	password := os.Getenv("REDIS_PASSWORD")
	if password == "" {
		password = "your-password"
	}
	rdb, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, Password: password})
	if err != nil {
		t.Skipf("redis unavailable: %v", err)
	}
	t.Cleanup(rdb.Close)
	st, err := store.NewRedisStore(rdb, store.RedisStoreConfig{
		KeyPrefix: "test_" + xid.New().String(),
		Tiers:     []store.TierConfig{{Name: "normal", Weight: 1, FetchBatch: 10}},
	}, nil)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	if disp == nil {
		disp = dispatcher.New(st, nil)
	}
	api := NewAPI(st, disp, nil)
	t.Cleanup(api.Shutdown)
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	return st, srv
}

// call requests path and decodes the response into out, if it is one of a 2xx.
func call(t *testing.T, srv *httptest.Server, method, path string, out any) int {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	if out != nil && resp.StatusCode < 300 {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
	}
	return resp.StatusCode
}

func get(t *testing.T, srv *httptest.Server, path string, out any) {
	t.Helper()
	if code := call(t, srv, http.MethodGet, path, out); code != http.StatusOK {
		t.Fatalf("GET %s: status %d", path, code)
	}
}

func createJob(t *testing.T, st *store.RedisStore, id string, taskType types.TaskType, status types.JobStatus, age time.Duration) *types.Job {
	t.Helper()
	job := &types.Job{
		ID:        id,
		TaskType:  taskType,
		Payload:   json.RawMessage(`{"eventId":"e1","n":4300000}`),
		Status:    status,
		Schedule:  types.Schedule{Type: types.ScheduleImmediate},
		CreatedAt: time.Now().Add(-age),
	}
	if _, err := st.CreateJob(context.Background(), job); err != nil {
		t.Fatalf("create job: %v", err)
	}
	return job
}

func TestStats_CountPausedJobs(t *testing.T) {
	st, srv := newTestAPI(t, nil)
	createJob(t, st, "a", "t", types.JobStatusPaused, 0)
	createJob(t, st, "b", "t", types.JobStatusPaused, 0)
	createJob(t, st, "c", "t", types.JobStatusDead, 0)

	var stats struct {
		JobCounts map[string]int `json:"job_counts"`
	}
	get(t, srv, "/api/stats", &stats)
	if stats.JobCounts["paused"] != 2 || stats.JobCounts["dead"] != 1 {
		t.Fatalf("job counts %v, want 2 paused and 1 dead", stats.JobCounts)
	}
}

// failingDispatcher cannot dispatch.
type failingDispatcher struct{}

func (failingDispatcher) Dispatch(context.Context, *types.Job, int) error { return errors.New("down") }
func (failingDispatcher) PublishEvent(types.JobEvent)                     {}

func TestResumeJob(t *testing.T) {
	st, srv := newTestAPI(t, nil)
	ctx := context.Background()
	if err := st.EnsureStreams(ctx); err != nil {
		t.Fatal(err)
	}
	job := createJob(t, st, "paused", "t", types.JobStatusPending, 0)
	job.Attempt, job.ConsecutiveErrors = 2, 3
	job.Pause(time.Now(), time.Hour)
	if _, err := st.SaveJob(ctx, job); err != nil {
		t.Fatal(err)
	}

	if code := call(t, srv, http.MethodPost, "/api/jobs/paused/resume", nil); code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	got, _, err := st.GetJob(ctx, "paused")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.JobStatusRetrying || got.PausedAt != nil || got.ResumeAt != nil || got.ConsecutiveErrors != 0 || got.Attempt != 2 {
		t.Fatalf("resumed job: %+v", got)
	}
	if n, _ := st.Client().Do(ctx, st.Client().B().Xlen().Key(store.WorkStreamKey(st.Prefix(), "normal")).Build()).AsInt64(); n != 1 {
		t.Fatalf("%d messages dispatched, want 1", n)
	}
	if code := call(t, srv, http.MethodPost, "/api/jobs/paused/resume", nil); code != http.StatusConflict {
		t.Fatalf("resuming a job that is not paused: status %d, want 409", code)
	}
	if code := call(t, srv, http.MethodPost, "/api/jobs/none/resume", nil); code != http.StatusNotFound {
		t.Fatalf("resuming no job: status %d, want 404", code)
	}
}

func TestResumeJob_DispatchFails_StaysPaused(t *testing.T) {
	st, srv := newTestAPI(t, failingDispatcher{})
	ctx := context.Background()
	job := createJob(t, st, "paused", "t", types.JobStatusPending, 0)
	job.ConsecutiveErrors = 3
	job.Pause(time.Now(), time.Hour)
	if _, err := st.SaveJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	if code := call(t, srv, http.MethodPost, "/api/jobs/paused/resume", nil); code < 500 {
		t.Fatalf("status %d, want an error", code)
	}
	got, _, err := st.GetJob(ctx, "paused")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.JobStatusPaused || got.PausedAt == nil || got.ResumeAt == nil || got.ConsecutiveErrors != 3 {
		t.Fatalf("job after a failed resume: %+v", got)
	}
}
