package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
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
	addr, password := testRedis(t)
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

// testRedis returns the address and password of the test Redis, or skips.
func testRedis(t *testing.T) (addr, password string) {
	t.Helper()
	addr = strings.TrimPrefix(os.Getenv("REDIS_URL"), "redis://")
	if addr == "" {
		t.Skip("REDIS_URL not set")
	}
	password = os.Getenv("REDIS_PASSWORD")
	if password == "" {
		password = "your-password"
	}
	return addr, password
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

func publish(t *testing.T, st *store.RedisStore, event types.JobEvent) {
	t.Helper()
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	if err := st.PublishEvent(context.Background(), event); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

type eventPage struct {
	Data  []types.JobEvent `json:"data"`
	Total int              `json:"total"`
}

func TestHistoryEvents(t *testing.T) {
	st, srv := newTestAPI(t, nil)
	var page eventPage
	get(t, srv, "/api/history/events", &page)
	if page.Total != 0 || len(page.Data) != 0 {
		t.Fatalf("no events yet, got %+v", page)
	}

	createJob(t, st, "a", "t", types.JobStatusPending, 0)
	for i := range 4 {
		publish(t, st, types.JobEvent{Type: types.EventJobStarted, JobID: "a", RunID: fmt.Sprint("a", i)})
		publish(t, st, types.JobEvent{Type: types.EventJobStarted, JobID: "b", RunID: fmt.Sprint("b", i)})
	}
	runs := func(path string) (string, int) {
		t.Helper()
		var page eventPage
		get(t, srv, path, &page)
		var ids []string
		for _, e := range page.Data {
			ids = append(ids, e.RunID)
		}
		return strings.Join(ids, " "), page.Total
	}
	for _, tc := range []struct {
		path, want string
		total      int
	}{
		{"/api/history/events", "b3 a3 b2 a2 b1 a1 b0 a0", 8},
		{"/api/history/events?limit=3", "b3 a3 b2", 8},
		{"/api/history/events?limit=3&offset=6", "b0 a0", 8},
		{"/api/history/events?job_id=a", "a3 a2 a1 a0", 4},
		{"/api/history/events?job_id=a&limit=2&offset=1", "a2 a1", 4},
		{"/api/history/events?job_id=b&limit=1", "b3", 4}, // there is no such job (any more)
		{"/api/history/events?job_id=none", "", 0},
	} {
		if got, total := runs(tc.path); got != tc.want || total != tc.total {
			t.Errorf("%s: %q of %d, want %q of %d", tc.path, got, total, tc.want, tc.total)
		}
	}

	// From a node whose clock is behind that of the client that created the job.
	job := createJob(t, st, "late", "t", types.JobStatusPending, 0)
	publish(t, st, types.JobEvent{Type: types.EventJobStarted, JobID: "late", RunID: "late0", Timestamp: job.CreatedAt.Add(-5 * time.Second)})
	if got, total := runs("/api/history/events?job_id=late"); got != "late0" || total != 1 {
		t.Errorf("event of a node with a clock that is behind: %q of %d", got, total)
	}

	var events []types.JobEvent
	get(t, srv, "/api/jobs/a/events?limit=2", &events)
	if len(events) != 2 || events[0].RunID != "a3" || events[1].RunID != "a2" {
		t.Fatalf("events of the job: %+v", events)
	}
}

// Events that a completed run leaves behind are written in a different way.
func TestHistoryEvents_CompletedRun(t *testing.T) {
	st, srv := newTestAPI(t, nil)
	createJob(t, st, "a", "t", types.JobStatusPending, 0)
	run := &types.JobRun{ID: "r", JobID: "a", Status: types.RunStatusSucceeded, StartedAt: time.Now()}
	err := st.CompleteRun(context.Background(), &store.CompletionBatch{
		Run:    run,
		Event:  types.JobEvent{Type: types.EventJobCompleted, JobID: "a", RunID: "r", Timestamp: time.Now()},
		Result: types.WorkResult{RunID: "r", JobID: "a", Success: true}, DailyStatField: "processed",
	})
	if err != nil {
		t.Fatal(err)
	}
	var events []types.JobEvent
	get(t, srv, "/api/jobs/a/events", &events)
	if len(events) != 1 || events[0].Type != types.EventJobCompleted {
		t.Fatalf("events %+v, want the completion", events)
	}
}

// Two jobs of one operation, like an execution and its retry: each has an event, a replay
// of one has none, and the operation counts once.
func TestCompleteRun_EventForEveryJob(t *testing.T) {
	st, srv := newTestAPI(t, nil)
	complete := func(jobID, runID string) {
		t.Helper()
		run := &types.JobRun{ID: runID, JobID: jobID, Status: types.RunStatusSucceeded, StartedAt: time.Now()}
		err := st.CompleteRun(context.Background(), &store.CompletionBatch{
			Run: run, OperationKey: "op", DailyStatField: "processed",
			Event:  types.JobEvent{Type: types.EventJobCompleted, JobID: jobID, RunID: runID, Timestamp: time.Now()},
			Result: types.WorkResult{RunID: runID, JobID: jobID, Success: true},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	complete("old", "r1")
	complete("old", "r1")  // again, after it was cut short
	complete("old", "r1b") // another execution of the same job
	complete("new", "r2")
	for jobID, want := range map[string]int{"old": 1, "new": 1} {
		var page eventPage
		get(t, srv, "/api/history/events?job_id="+jobID, &page)
		if page.Total != want {
			t.Errorf("job %s has %d events, want %d", jobID, page.Total, want)
		}
	}
	var days []struct {
		Processed int `json:"processed"`
	}
	get(t, srv, "/api/stats/daily?days=1", &days)
	if len(days) != 1 || days[0].Processed != 1 {
		t.Errorf("daily stats %+v, want it to count once", days)
	}
}

// failingRedis fails the pipeline that is the failAt'th one, counted from 1.
type failingRedis struct {
	rueidis.Client
	failAt, n int
}

func (f *failingRedis) DoMulti(ctx context.Context, multi ...rueidis.Completed) []rueidis.RedisResult {
	if f.n++; f.n == f.failAt {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		ctx = cancelled
	}
	return f.Client.DoMulti(ctx, multi...)
}

// A completion that is cut short by an error is finished when it is tried again: the message
// is acknowledged last, and the orchestrator learns of it.
func TestCompleteRun_TriedAgain(t *testing.T) {
	for failAt, name := range []string{1: "bookkeeping fails", 2: "acknowledgement fails"} {
		if name == "" {
			continue
		}
		t.Run(name, func(t *testing.T) {
			base, srv := newTestAPI(t, nil)
			ctx := context.Background()
			rdb := &failingRedis{Client: base.Client()}
			st, err := store.NewRedisStore(rdb, base.Config(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := base.EnsureStreams(ctx); err != nil {
				t.Fatal(err)
			}
			if err := base.ReenqueueWork(ctx, "normal", &types.WorkMessage{RunID: "r", JobID: "j"}, 0); err != nil {
				t.Fatal(err)
			}
			work := store.WorkStreamKey(base.Prefix(), "normal")
			taken, err := base.Client().Do(ctx, base.Client().B().Xreadgroup().Group(store.ConsumerGroup, "n").Streams().Key(work).Id(">").Build()).AsXRead()
			if err != nil {
				t.Fatal(err)
			}
			pending := func() int64 {
				t.Helper()
				p, err := base.Client().Do(ctx, base.Client().B().Xpending().Key(work).Group(store.ConsumerGroup).Build()).ToArray()
				if err != nil {
					t.Fatal(err)
				}
				n, _ := p[0].AsInt64()
				return n
			}

			batch := &store.CompletionBatch{
				Run:          &types.JobRun{ID: "r", JobID: "j", Status: types.RunStatusSucceeded, StartedAt: time.Now()},
				OperationKey: "op", DailyStatField: "processed", AckTierName: "normal", AckMessageID: taken[work][0].ID,
				Event:  types.JobEvent{Type: types.EventJobCompleted, JobID: "j", RunID: "r", Timestamp: time.Now()},
				Result: types.WorkResult{RunID: "r", JobID: "j", Success: true},
			}
			rdb.n, rdb.failAt = 0, failAt
			if err := st.CompleteRun(ctx, batch); err == nil {
				t.Fatal("the failure is not reported")
			}
			if n := pending(); n != 1 {
				t.Errorf("%d messages pending after the failure, want 1", n)
			}
			if err := st.CompleteRun(ctx, batch); err != nil {
				t.Fatal(err)
			}
			if n := pending(); n != 0 {
				t.Errorf("%d messages pending, want none", n)
			}
			var page eventPage
			get(t, srv, "/api/history/events?job_id=j", &page)
			given, _ := base.Client().Do(ctx, base.Client().B().Xlen().Key(store.OrchestratorStreamKey(base.Prefix())).Build()).AsInt64()
			if page.Total != 1 || given != 1 {
				t.Errorf("%d events, %d for the orchestrator, want 1 each", page.Total, given)
			}
		})
	}
}

// A message that is put back or queued again is the same afterwards. One that has been
// acknowledged by then is not put back.
func TestWorkMessage_IsKeptAsItIs(t *testing.T) {
	st, _ := newTestAPI(t, nil)
	ctx := context.Background()
	rdb := st.Client()
	if err := st.EnsureStreams(ctx); err != nil {
		t.Fatal(err)
	}
	key := store.WorkStreamKey(st.Prefix(), "normal")
	read := func() rueidis.XRangeEntry {
		t.Helper()
		streams, err := rdb.Do(ctx, rdb.B().Xreadgroup().Group(store.ConsumerGroup, "n").Streams().Key(key).Id(">").Build()).AsXRead()
		if err != nil || len(streams[key]) != 1 {
			t.Fatalf("%d messages (%v), want 1", len(streams[key]), err)
		}
		return streams[key][0]
	}
	now := time.Now().Truncate(time.Millisecond).UTC()
	work := types.WorkMessage{
		RunID: "r", JobID: "j", TaskType: "t", Payload: json.RawMessage(`{"a":1}`), Attempt: 2, Deadline: now.Add(time.Hour), Priority: 7,
		DispatchedAt: now, Version: "v", OperationKey: "op", Headers: map[string]string{"h": "x"},
		ConcurrencyKeys: []types.ConcurrencyKey{{Key: "k", MaxConcurrency: 3}},
	}
	want, _ := json.Marshal(work)
	same := func(what string, e rueidis.XRangeEntry) {
		t.Helper()
		wm := store.WorkMessageFromStreamValues(e.FieldValues)
		wm.Deadline, wm.DispatchedAt = wm.Deadline.UTC(), wm.DispatchedAt.UTC()
		if got, _ := json.Marshal(wm); string(got) != string(want) {
			t.Errorf("%s:\n%s, want\n%s", what, got, want)
		}
	}

	if err := st.ReenqueueWork(ctx, "normal", &work, 0); err != nil {
		t.Fatal(err)
	}
	entry := read()
	same("queued again", entry)

	taken := store.WorkMessageFromStreamValues(entry.FieldValues)
	postponed := []store.PostponedMessage{{Work: &taken, MessageID: entry.ID, Until: time.Now()}}
	for range 2 { // the second time, it has been acknowledged
		if err := st.Postpone(ctx, "normal", postponed); err != nil {
			t.Fatal(err)
		}
	}
	pending, err := rdb.Do(ctx, rdb.B().Xpending().Key(key).Group(store.ConsumerGroup).Build()).ToArray()
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := pending[0].AsInt64(); n != 0 {
		t.Errorf("%d messages are pending, want none", n)
	}
	if n, err := st.MoveDelayedToStream(ctx, "normal", 10); err != nil || n != 1 {
		t.Fatalf("moved %d messages (%v), want 1", n, err)
	}
	same("put back", read())
}

// The dead letter queue keeps every job: for some it is all there is left of them.
func TestDLQ_KeepsEveryJob(t *testing.T) {
	st, _ := newTestAPI(t, nil)
	ctx := context.Background()
	rdb := st.Client()
	const n = 10500
	seed := make(rueidis.Commands, 0, n)
	for range n {
		seed = append(seed, rdb.B().Xadd().Key(store.DLQStreamKey(st.Prefix())).Id("*").FieldValue().FieldValue("job_id", "old").Build())
	}
	rdb.DoMulti(ctx, seed...)
	if err := st.DispatchToDLQ(ctx, &types.WorkMessage{JobID: "new"}); err != nil {
		t.Fatal(err)
	}
	got, err := rdb.Do(ctx, rdb.B().Xlen().Key(store.DLQStreamKey(st.Prefix())).Build()).AsInt64()
	if err != nil || got != n+1 {
		t.Errorf("%d entries (%v), want %d", got, err, n+1)
	}

	cfg := st.Config()
	cfg.DLQStreamMaxLen = 1000
	limited, err := store.NewRedisStore(rdb, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := limited.DispatchToDLQ(ctx, &types.WorkMessage{JobID: "new"}); err != nil {
		t.Fatal(err)
	}
	got, err = rdb.Do(ctx, rdb.B().Xlen().Key(store.DLQStreamKey(st.Prefix())).Build()).AsInt64()
	if err != nil || got > 1200 {
		t.Errorf("%d entries (%v) with a limit of 1000", got, err)
	}
}

// A scan goes over more than one chunk of the stream, and over the time span asked for.
func TestListJobEvents_LongStream(t *testing.T) {
	st, _ := newTestAPI(t, nil)
	ctx := context.Background()
	rdb := st.Client()
	const n = 2500
	cmds := make(rueidis.Commands, 0, n)
	start := time.Now().Add(-n * time.Second).Truncate(time.Second)
	for i := range n {
		at := start.Add(time.Duration(i) * time.Second)
		jobID := "other"
		if i%100 == 0 {
			jobID = "rare"
		}
		data, _ := json.Marshal(types.JobEvent{Type: types.EventJobStarted, JobID: jobID, RunID: fmt.Sprint(i), Timestamp: at})
		cmds = append(cmds, rdb.B().Xadd().Key(store.EventsStreamKey(st.Prefix())).Id(fmt.Sprintf("%d-0", at.UnixMilli())).FieldValue().
			FieldValue("type", string(types.EventJobStarted)).FieldValue("job_id", jobID).FieldValue("data", string(data)).Build())
	}
	for _, r := range rdb.DoMulti(ctx, cmds...) {
		if err := r.Error(); err != nil {
			t.Fatal(err)
		}
	}

	rare := "rare"
	events, total, err := st.ListJobEvents(ctx, store.EventFilter{JobID: &rare, Limit: 3, Offset: 20})
	if err != nil || total != 25 || len(events) != 3 || events[0].RunID != "400" || events[2].RunID != "200" {
		t.Fatalf("got %v of %d (%v), want the runs 400, 300, 200 of 25", events, total, err)
	}
	if _, total, _ = st.ListJobEvents(ctx, store.EventFilter{JobID: &rare, Limit: 3, SkipTotal: true}); total >= 25 {
		t.Fatalf("counted %d events although the total was to be skipped", total)
	}
	if events, total, err = st.ListJobEvents(ctx, store.EventFilter{Limit: 10}); err != nil || total != n || len(events) != 10 || events[0].RunID != "2499" {
		t.Fatalf("unfiltered: %d events of %d (%v)", len(events), total, err)
	}
	since, until := start.Add(1000*time.Second), start.Add(1010*time.Second)
	if events, total, err = st.ListJobEvents(ctx, store.EventFilter{Since: &since, Until: &until, Limit: 5}); err != nil || total != 11 || len(events) != 5 || events[0].RunID != "1010" {
		t.Fatalf("time span: %v of %d (%v), want 5 of 11 from 1010 on", events, total, err)
	}
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

// The index of versions before v0.3.1 has recovered runs among the failed ones.
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

func TestCleanupRunHistory(t *testing.T) {
	st, srv := newTestAPI(t, nil)
	saveRun(t, st, "old", types.RunStatusSucceeded, 50*time.Hour)
	saveRun(t, st, "new", types.RunStatusSucceeded, time.Hour)
	for _, query := range []string{"", "?older_than=", "?older_than=30", "?older_than=0s", "?older_than=-24h"} {
		if code := call(t, srv, http.MethodDelete, "/api/history/runs"+query, nil); code != http.StatusBadRequest {
			t.Errorf("%q: status %d, want 400", query, code)
		}
	}
	if ids, total := listRuns(t, srv, ""); ids != "new old" || total != 2 {
		t.Fatalf("runs after bad requests: %q of %d", ids, total)
	}

	var answer struct {
		Removed int `json:"removed"`
	}
	if code := call(t, srv, http.MethodDelete, "/api/history/runs?older_than=24h", &answer); code != http.StatusOK || answer.Removed != 1 {
		t.Fatalf("status %d, removed %d, want 200 and 1", code, answer.Removed)
	}
	if ids, total := listRuns(t, srv, ""); ids != "new" || total != 1 {
		t.Fatalf("runs left: %q of %d", ids, total)
	}
}

func TestGetRun_FinishedRun(t *testing.T) {
	st, srv := newTestAPI(t, nil)
	saveRun(t, st, "done", types.RunStatusFailed, time.Minute)
	var run types.JobRun
	get(t, srv, "/api/runs/done", &run)
	if run.ID != "done" || run.Status != types.RunStatusFailed {
		t.Fatalf("run %+v", run)
	}

	// Retried: it is the active one that counts.
	if _, err := st.SaveRun(context.Background(), &types.JobRun{ID: "done", JobID: "j", Status: types.RunStatusRunning, Attempt: 1, StartedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	get(t, srv, "/api/runs/done", &run)
	if run.Status != types.RunStatusRunning || run.Attempt != 1 {
		t.Fatalf("run %+v, want the active one", run)
	}
	if code := call(t, srv, http.MethodGet, "/api/runs/none", nil); code != http.StatusNotFound {
		t.Fatalf("no such run: status %d", code)
	}
}

func TestListJobs_SearchIsPagedAndCounted(t *testing.T) {
	st, srv := newTestAPI(t, nil)
	for i := range 30 {
		taskType := types.TaskType("mail.Send")
		if i%3 == 0 {
			taskType = "image.resize"
		}
		createJob(t, st, fmt.Sprintf("job-%02d", i), taskType, types.JobStatusCompleted, time.Duration(i)*time.Minute)
	}
	createJob(t, st, "resized-by-name", "other", types.JobStatusDead, time.Hour)

	find := func(query string) (string, int) {
		t.Helper()
		var page struct {
			Data  []types.Job `json:"data"`
			Total int         `json:"total"`
		}
		get(t, srv, "/api/jobs?"+query, &page)
		var ids []string
		for _, j := range page.Data {
			ids = append(ids, j.ID)
		}
		return strings.Join(ids, " "), page.Total
	}
	for _, tc := range []struct {
		query, want string
		total       int
	}{
		{"search=RESIZE&limit=4", "job-00 job-03 job-06 job-09", 11},
		{"search=resize&limit=4&offset=8", "job-24 job-27 resized-by-name", 11},
		{"search=resize&limit=2&sort=oldest", "resized-by-name job-27", 11},
		{"search=resize&status=dead", "resized-by-name", 1},
		{"search=job-1&limit=2", "job-10 job-11", 10},
		{"search=nothing", "", 0},
		{"limit=1", "job-00", 31},
	} {
		if got, total := find(tc.query); got != tc.want || total != tc.total {
			t.Errorf("%s: %q of %d, want %q of %d", tc.query, got, total, tc.want, tc.total)
		}
	}
}

// More jobs than a chunk, and many with the same score.
func TestListJobs_SearchOverManyJobs(t *testing.T) {
	st, srv := newTestAPI(t, nil)
	ctx := context.Background()
	const n = 1300
	created := time.Now()
	for i := range n {
		if _, err := st.CreateJob(ctx, &types.Job{ID: fmt.Sprintf("j%04d", i), TaskType: "t.x", Status: types.JobStatusCompleted, CreatedAt: created}); err != nil {
			t.Fatal(err)
		}
	}
	for _, sort := range []string{"newest", "oldest"} {
		var page struct {
			Data  []types.Job `json:"data"`
			Total int         `json:"total"`
		}
		get(t, srv, "/api/jobs?search=t.x&limit=100&offset=1250&sort="+sort, &page)
		if page.Total != n || len(page.Data) != 50 {
			t.Fatalf("%s: %d of %d, want 50 of %d", sort, len(page.Data), page.Total, n)
		}
	}
}

func TestSearchAllJobs(t *testing.T) {
	st, srv := newTestAPI(t, nil)
	ctx := context.Background()
	rdb := st.Client()
	if err := rdb.Do(ctx, rdb.B().JsonSet().Key(st.Prefix()+":probe").Path("$").Value("{}").Build()).Error(); err != nil {
		t.Skipf("no RedisJSON: %v", err)
	}
	createJob(t, st, "live", "update", types.JobStatusScheduled, time.Minute)
	createJob(t, st, "dead", "update", types.JobStatusDead, 2*time.Minute)
	createJob(t, st, "paused", "remind", types.JobStatusPaused, 3*time.Minute)

	for _, tc := range []struct {
		query string
		want  []string
		more  bool
	}{
		{"path=eventId&value=e1", []string{"live", "dead", "paused"}, false},
		{"path=n&value=4300000", []string{"live", "dead", "paused"}, false},
		{"path=eventId&value=e1&live=true", []string{"live", "paused"}, false},
		{"path=eventId&value=e1&task_type=update", []string{"live", "dead"}, false},
		{"path=eventId&value=e1&status=dead&status=paused", []string{"dead", "paused"}, false},
		{"path=eventId&value=e1&limit=2", []string{"live", "dead"}, true},
		{"path=eventId&value=e1&limit=3", []string{"live", "dead", "paused"}, false},
		{"path=eventId&value=other", nil, false},
	} {
		var page struct {
			Data    []types.Job `json:"data"`
			HasMore bool        `json:"has_more"`
		}
		get(t, srv, "/api/search/jobs/all?"+tc.query, &page)
		var ids []string
		for _, j := range page.Data {
			ids = append(ids, j.ID)
		}
		if !slices.Equal(ids, tc.want) || page.HasMore != tc.more {
			t.Errorf("%s: %v (more: %v), want %v (more: %v)", tc.query, ids, page.HasMore, tc.want, tc.more)
		}
	}

	// The search for one job is as it was.
	var job types.Job
	get(t, srv, "/api/search/jobs?path=eventId&value=e1", &job)
	if job.ID != "live" {
		t.Errorf("found %q, want the newest", job.ID)
	}
	for _, query := range []string{"path=eventId", "value=e1", "path=eventId&value=e1&live=maybe"} {
		if code := call(t, srv, http.MethodGet, "/api/search/jobs/all?"+query, nil); code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", query, code)
		}
	}
}

// Workflows of versions before v0.3.1 have their task names as keys only.
func TestWorkflow_TaskNames(t *testing.T) {
	st, srv := newTestAPI(t, nil)
	_, err := st.SaveWorkflow(context.Background(), &types.WorkflowInstance{
		ID: "wf", WorkflowName: "w", Status: types.WorkflowStatusRunning, CreatedAt: time.Now(),
		Tasks: map[string]types.WorkflowTaskState{"validate": {Status: types.JobStatusCompleted}, "ship": {Name: "ship", Status: types.JobStatusPending}},
	})
	if err != nil {
		t.Fatal(err)
	}
	check := func(wf types.WorkflowInstance) {
		t.Helper()
		for name, state := range wf.Tasks {
			if state.Name != name {
				t.Errorf("task %q has the name %q", name, state.Name)
			}
		}
		if len(wf.Tasks) != 2 || wf.Tasks["validate"].Status != types.JobStatusCompleted {
			t.Errorf("tasks %+v", wf.Tasks)
		}
	}
	var wf types.WorkflowInstance
	get(t, srv, "/api/workflows/wf", &wf)
	check(wf)
	var page struct {
		Data []types.WorkflowInstance `json:"data"`
	}
	get(t, srv, "/api/workflows", &page)
	check(page.Data[0])
	all, err := st.ListWorkflows(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	check(*all[0])
}

// Without RedisJSON payloads cannot be searched, and all searches say so in the same way.
func TestPayloadSearch_WithoutJSONModule(t *testing.T) {
	st, srv := newTestAPI(t, nil)
	ctx := context.Background()
	rdb := st.Client()
	if err := rdb.Do(ctx, rdb.B().JsonSet().Key(st.Prefix()+":probe").Path("$").Value("{}").Build()).Error(); err == nil {
		t.Skip("this Redis has RedisJSON")
	}
	if _, err := st.CreateJob(ctx, &types.Job{ID: "j1", TaskType: "t", Payload: json.RawMessage(`{"a":"b"}`), Status: types.JobStatusPending, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SaveWorkflow(ctx, &types.WorkflowInstance{ID: "w1", WorkflowName: "w", Status: types.WorkflowStatusRunning, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SaveBatch(ctx, &types.BatchInstance{ID: "b1", Name: "b", Status: types.WorkflowStatusRunning, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/api/search/jobs", "/api/search/jobs/all", "/api/search/workflows", "/api/search/batches"} {
		resp, err := http.Get(srv.URL + path + "?path=a&value=b")
		if err != nil {
			t.Fatal(err)
		}
		var e struct {
			Error string `json:"error"`
		}
		json.NewDecoder(resp.Body).Decode(&e)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotImplemented || !strings.Contains(e.Error, "RedisJSON") {
			t.Errorf("GET %s: %d %q", path, resp.StatusCode, e.Error)
		}
	}
	// Without path or value it is the request that is wrong.
	if code := call(t, srv, "GET", "/api/search/jobs/all", nil); code != http.StatusBadRequest {
		t.Errorf("without path: %d", code)
	}
}

// While Redis does not answer, a request is answered 503 with Retry-After once its time is up,
// not when Redis is back; and a call of a server of one's own gets the same error. A WebSocket
// is not limited.
func TestRequestTimeout_RedisDoesNotAnswer(t *testing.T) {
	px := newRedisProxy(t)
	st, err := store.NewRedisStore(px.client(t), store.RedisStoreConfig{
		KeyPrefix: "test_" + xid.New().String(),
		Tiers:     []store.TierConfig{{Name: "normal", Weight: 1, FetchBatch: 10}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	api := NewAPI(st, nil, nil)
	t.Cleanup(api.Shutdown)
	api.SetRequestTimeout(time.Second)
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	if code := call(t, srv, "GET", "/api/stats", nil); code != 200 {
		t.Fatalf("before: %d", code)
	}

	px.hang(true)
	start := time.Now()
	res, err := (&http.Client{Timeout: 20 * time.Second}).Get(srv.URL + "/api/stats")
	if err != nil {
		t.Fatalf("no answer after %v: %v", time.Since(start).Round(time.Millisecond), err)
	}
	var e struct {
		Error string `json:"error"`
	}
	json.NewDecoder(res.Body).Decode(&e)
	res.Body.Close()
	took := time.Since(start)
	if res.StatusCode != http.StatusServiceUnavailable || res.Header.Get("Retry-After") != "5" || e.Error == "" || took > 4*time.Second {
		t.Errorf("answered %d (Retry-After %q, %q) after %v; want 503 with Retry-After 5 after about a second", res.StatusCode, res.Header.Get("Retry-After"), e.Error, took)
	}
	t.Logf("answered %d after %v", res.StatusCode, took.Round(time.Millisecond))

	svc := api.Service()
	err = svc.Timed(context.Background(), func(ctx context.Context) error {
		_, err := svc.GetStats(ctx)
		return err
	})
	var apiErr *ApiError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("a timed call: %v, want 503", err)
	}
}

// A request that is in time is answered as it is, with its headers, and a WebSocket lasts.
func TestRequestTimeout_InTime(t *testing.T) {
	defer shorten(&wsHeartbeat, 200*time.Millisecond)()
	st, _ := newTestAPI(t, nil)
	api := NewAPI(st, nil, nil)
	t.Cleanup(api.Shutdown)
	api.SetRequestTimeout(300 * time.Millisecond)
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	res, err := http.Get(srv.URL + "/api/jobs/none")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 404 || res.Header.Get("Content-Type") != "application/json" || res.Header.Get("Cache-Control") != "private, no-store" {
		t.Errorf("%d %v", res.StatusCode, res.Header)
	}
	read := dial(t, srv, "?v=2")
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		if got := read(); !strings.Contains(got, "ws.heartbeat") {
			t.Fatalf("got %s", got)
		}
	}
}

// What the time limit of requests costs: GET /api/stats and GET /api/health, without and with it.
func BenchmarkRequestTimeout(b *testing.B) {
	addr, password := strings.TrimPrefix(os.Getenv("REDIS_URL"), "redis://"), os.Getenv("REDIS_PASSWORD")
	if addr == "" {
		b.Skip("REDIS_URL not set")
	}
	if password == "" {
		password = "your-password"
	}
	rdb, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, Password: password})
	if err != nil {
		b.Fatal(err)
	}
	defer rdb.Close()
	st, err := store.NewRedisStore(rdb, store.RedisStoreConfig{KeyPrefix: "bench_" + xid.New().String(), Tiers: []store.TierConfig{{Name: "normal", Weight: 1, FetchBatch: 10}}}, nil)
	if err != nil {
		b.Fatal(err)
	}
	for _, path := range []string{"/api/health", "/api/stats"} {
		for _, tc := range []struct {
			name    string
			timeout time.Duration
		}{{"none", 0}, {"30s", DefaultRequestTimeout}} {
			b.Run(path+"/"+tc.name, func(b *testing.B) {
				api := NewAPI(st, nil, nil)
				defer api.Shutdown()
				api.SetRequestTimeout(tc.timeout)
				h := api.Handler()
				b.ReportAllocs()
				for b.Loop() {
					w := httptest.NewRecorder()
					h.ServeHTTP(w, httptest.NewRequest("GET", "http://dureq.internal"+path, nil))
					if w.Code != 200 {
						b.Fatal(w.Code)
					}
				}
			})
		}
	}
}
