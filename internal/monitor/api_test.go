package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func TestQueues_Depth(t *testing.T) {
	st, srv := newTestAPI(t, nil)
	ctx := context.Background()
	type queue struct {
		Size     int64  `json:"size"`
		Waiting  *int64 `json:"waiting"`
		InFlight *int64 `json:"in_flight"`
		Delayed  *int64 `json:"delayed"`
	}
	check := func(when string, size, waiting, inFlight, delayed int64) {
		t.Helper()
		var queues []queue
		get(t, srv, "/api/queues", &queues)
		q := queues[0]
		if q.Waiting == nil || q.InFlight == nil || q.Delayed == nil {
			t.Fatalf("%s: depth is missing: %+v", when, q)
		}
		if q.Size != size || *q.Waiting != waiting || *q.InFlight != inFlight || *q.Delayed != delayed {
			t.Fatalf("%s: size %d, waiting %d, in flight %d, delayed %d; want %d, %d, %d, %d",
				when, q.Size, *q.Waiting, *q.InFlight, *q.Delayed, size, waiting, inFlight, delayed)
		}
	}

	check("before there is a stream", 0, 0, 0, 0)
	if err := st.EnsureStreams(ctx); err != nil {
		t.Fatal(err)
	}
	check("empty", 0, 0, 0, 0)

	for i := range 5 {
		if _, err := st.DispatchWork(ctx, "normal", &types.WorkMessage{RunID: fmt.Sprint("run", i), JobID: "j", TaskType: "t"}); err != nil {
			t.Fatal(err)
		}
	}
	check("dispatched", 5, 5, 0, 0)

	rdb := st.Client()
	streams, err := rdb.Do(ctx, rdb.B().Xreadgroup().Group(store.ConsumerGroup, "n").Count(3).Streams().Key(store.WorkStreamKey(st.Prefix(), "normal")).Id(">").Build()).AsXRead()
	if err != nil {
		t.Fatal(err)
	}
	check("3 taken", 5, 2, 3, 0)

	entries := streams[store.WorkStreamKey(st.Prefix(), "normal")]
	if err := st.AckMessage(ctx, "normal", entries[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := st.AddDelayed(ctx, "normal", &types.WorkMessage{RunID: "later", JobID: "j"}, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	check("1 of them done, 1 to retry", 5, 2, 2, 1)
}

type runPage struct {
	Data  []types.JobRun `json:"data"`
	Total int            `json:"total"`
}

func saveRun(t *testing.T, st *store.RedisStore, id string, status types.RunStatus, age time.Duration) {
	t.Helper()
	if err := st.SaveJobRun(context.Background(), &types.JobRun{ID: id, JobID: "j", Status: status, StartedAt: time.Now().Add(-age)}); err != nil {
		t.Fatalf("save run: %v", err)
	}
}

// listRuns returns the IDs of a page of the history.
func listRuns(t *testing.T, srv *httptest.Server, query string) (string, int) {
	t.Helper()
	var page runPage
	get(t, srv, "/api/history/runs?"+query, &page)
	var ids []string
	for _, r := range page.Data {
		ids = append(ids, r.ID)
	}
	return strings.Join(ids, " "), page.Total
}

// A run that failed and succeeded when retried, under the same ID, is a success.
func TestHistoryRuns_RecoveredRunIsNotFailed(t *testing.T) {
	st, srv := newTestAPI(t, nil)
	saveRun(t, st, "failed", types.RunStatusFailed, time.Minute)
	saveRun(t, st, "recovered", types.RunStatusFailed, time.Second)
	saveRun(t, st, "recovered", types.RunStatusSucceeded, 0)
	completed := &types.JobRun{ID: "completed", JobID: "j", Status: types.RunStatusFailed, StartedAt: time.Now()}
	if err := st.SaveJobRun(context.Background(), completed); err != nil {
		t.Fatal(err)
	}
	completed.Status = types.RunStatusSucceeded
	if err := st.CompleteRun(context.Background(), &store.CompletionBatch{Run: completed, Result: types.WorkResult{JobID: "j"}, DailyStatField: "processed"}); err != nil {
		t.Fatal(err)
	}

	if ids, total := listRuns(t, srv, "status=failed"); ids != "failed" || total != 1 {
		t.Fatalf("failed runs: %q of %d", ids, total)
	}
	if ids, total := listRuns(t, srv, "status=succeeded"); ids != "completed recovered" || total != 2 {
		t.Fatalf("succeeded runs: %q of %d", ids, total)
	}
	if ids, total := listRuns(t, srv, "job_id=j"); ids != "completed recovered failed" || total != 3 {
		t.Fatalf("runs of the job: %q of %d", ids, total)
	}
}

// The index of versions before v0.2.6 has recovered runs among the failed ones.
func TestHistoryRuns_OldIndexIsCorrected(t *testing.T) {
	st, srv := newTestAPI(t, nil)
	ctx := context.Background()
	for i := range 30 {
		id := fmt.Sprintf("r%02d", i)
		status := types.RunStatusSucceeded
		if i%10 == 0 {
			status = types.RunStatusFailed
		}
		saveRun(t, st, id, status, time.Duration(i)*time.Minute)
		score := float64(time.Now().Add(-time.Duration(i) * time.Minute).UnixNano())
		if err := st.Client().Do(ctx, st.Client().B().Zadd().Key(store.HistoryRunsByStatusKey(st.Prefix(), "failed")).ScoreMember().ScoreMember(score, id).Build()).Error(); err != nil {
			t.Fatal(err)
		}
	}
	if ids, total := listRuns(t, srv, "status=failed&limit=5"); ids != "r00 r10 r20" || total != 3 {
		t.Fatalf("failed runs: %q of %d, want r00 r10 r20 of 3", ids, total)
	}
	if ids, total := listRuns(t, srv, "status=succeeded&limit=1"); ids != "r01" || total != 27 {
		t.Fatalf("succeeded runs: %q of %d, want r01 of 27", ids, total)
	}
}
