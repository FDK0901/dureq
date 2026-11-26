package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/dispatcher"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/redis/rueidis"
	"github.com/rs/xid"
)

// loseRun stores a run of the job that a worker of this version executed on a node
// that is gone, and checks for orphans.
func loseRun(t *testing.T, st *store.RedisStore, s *Scheduler, jobID string, attempt int, operationKey string) {
	t.Helper()
	run := &types.JobRun{
		ID:           xid.New().String(),
		JobID:        jobID,
		NodeID:       "gone-node",
		Status:       types.RunStatusRunning,
		Attempt:      attempt,
		StartedAt:    time.Now().Add(-time.Minute),
		OperationKey: operationKey,
	}
	if _, err := st.SaveRun(context.Background(), run); err != nil {
		t.Fatalf("save run: %v", err)
	}
	s.checkOrphanedRuns(context.Background())
}

// loseAllRuns loses every run of the job until it is not redispatched any more, and
// returns how many that took.
func loseAllRuns(t *testing.T, st *store.RedisStore, s *Scheduler, jobID string) int {
	t.Helper()
	attempt := 0
	for lost := 1; lost <= 20; lost++ {
		loseRun(t, st, s, jobID, attempt, jobID+":1")
		job := getJob(t, st, jobID)
		if job.Status != types.JobStatusPending {
			return lost
		}
		attempt = job.Attempt
	}
	t.Fatal("redispatched forever")
	return 0
}

// A firing that takes its worker down every time is not redispatched forever.
func TestCheckOrphanedRuns_RecurringJob_EndsAtTheLimit(t *testing.T) {
	two := 2
	for _, tc := range []struct {
		name          string
		keepOnFailure bool
		dlqAfter      *int
		wantLost      int
		wantStatus    types.JobStatus
	}{
		{"dead by default", false, nil, 4, types.JobStatusDead},
		{"dead after DLQAfter", false, &two, 3, types.JobStatusDead},
		{"kept on its schedule", true, nil, 4, types.JobStatusScheduled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := newTestStore(t)
			s := New(Config{Store: st, Dispatcher: dispatcher.New(st, nil)})
			entry := createScheduledJob(t, st, nil, time.Now().Add(time.Hour))
			editJob(t, st, entry.JobID, func(job *types.Job) {
				job.Schedule.KeepOnFailure = tc.keepOnFailure
				job.DLQAfter = tc.dlqAfter
			})

			if lost := loseAllRuns(t, st, s, entry.JobID); lost != tc.wantLost {
				t.Fatalf("ended after %d lost runs, want %d", lost, tc.wantLost)
			}
			job := getJob(t, st, entry.JobID)
			if job.Status != tc.wantStatus {
				t.Fatalf("status = %s, want %s", job.Status, tc.wantStatus)
			}
			if !tc.keepOnFailure {
				return
			}
			if job.Attempt != 0 {
				t.Fatalf("stored attempt = %d, want 0 for the next firing", job.Attempt)
			}
			if _, _, err := st.GetSchedule(context.Background(), entry.JobID); err != nil {
				t.Fatalf("schedule: %v", err)
			}
			rdb := st.Client()
			if n, err := rdb.Do(context.Background(), rdb.B().Xlen().Key(store.DLQStreamKey(st.Prefix())).Build()).AsInt64(); err != nil || n != 1 {
				t.Fatalf("%d messages in the DLQ (%v), want 1", n, err)
			}
		})
	}
}

// With its schedule gone there is nothing to keep.
func TestCheckOrphanedRuns_KeepOnFailure_NoSchedule_Dead(t *testing.T) {
	st := newTestStore(t)
	s := New(Config{Store: st, Dispatcher: dispatcher.New(st, nil)})
	entry := createScheduledJob(t, st, nil, time.Now().Add(time.Hour))
	editJob(t, st, entry.JobID, func(job *types.Job) { job.Schedule.KeepOnFailure = true })
	if err := st.DeleteSchedule(context.Background(), entry.JobID); err != nil {
		t.Fatalf("delete schedule: %v", err)
	}

	loseAllRuns(t, st, s, entry.JobID)

	if got := getJob(t, st, entry.JobID).Status; got != types.JobStatusDead {
		t.Fatalf("status = %s, want dead", got)
	}
}

// A recovered run is the same operation as the lost one, and different ones are
// different operations: the ledger must not take the second for the first.
func TestCheckOrphanedRuns_RecoveredRun_KeepsOperationKey(t *testing.T) {
	st := newTestStore(t)
	s := New(Config{Store: st, Dispatcher: dispatcher.New(st, nil)})
	entry := createScheduledJob(t, st, nil, time.Now().Add(time.Hour))
	want := []string{entry.JobID + ":1", entry.JobID + ":2"}
	for _, operationKey := range want {
		loseRun(t, st, s, entry.JobID, 0, operationKey)
	}

	entries := workEntries(t, st)
	if len(entries) != len(want) {
		t.Fatalf("%d redispatches, want %d", len(entries), len(want))
	}
	for i, e := range entries {
		if got := e.FieldValues["operation_key"]; got != want[i] {
			t.Fatalf("recovered run %d has operation key %q, want %q", i, got, want[i])
		}
	}
	if h := getJob(t, st, entry.JobID).Headers; len(h) != 0 {
		t.Fatalf("headers stored on the job: %v", h)
	}
}

// A one-off job is redispatched at the attempt it was on, as before.
func TestCheckOrphanedRuns_OneOffJob_LostRunIsNotCounted(t *testing.T) {
	st := newTestStore(t)
	s := New(Config{Store: st, Dispatcher: dispatcher.New(st, nil)})
	job := createOneOffJob(t, st, 0, "k")
	editJob(t, st, job.ID, func(j *types.Job) { j.RetryPolicy = &types.RetryPolicy{MaxAttempts: 1} })

	loseRun(t, st, s, job.ID, 0, job.ID)

	entries := workEntries(t, st)
	if len(entries) != 1 || entries[0].FieldValues["attempt"] != "0" || entries[0].FieldValues["operation_key"] != job.ID {
		t.Fatalf("redispatches: %v", entries)
	}
}

// lookingDispatcher looks, as each event is published, at what the event stands for, as a UI
// would that reads the job or its runs at once.
type lookingDispatcher struct {
	*dispatcher.Dispatcher
	st       *store.RedisStore
	events   []types.JobEvent
	problems []string
}

func (d *lookingDispatcher) PublishEvent(e types.JobEvent) {
	ctx := context.Background()
	d.events = append(d.events, e)
	note := func(format string, args ...any) {
		d.problems = append(d.problems, fmt.Sprintf("%s: ", e.Type)+fmt.Sprintf(format, args...))
	}
	if _, _, err := d.st.GetRun(ctx, e.RunID); err == nil {
		note("run %s is still active", e.RunID)
	}
	runs, _, err := d.st.ListJobRuns(ctx, store.RunFilter{JobID: &e.JobID, Limit: 5})
	if err != nil || !slices.ContainsFunc(runs, func(r types.JobRun) bool { return r.ID == e.RunID && r.Status == types.RunStatusFailed }) {
		note("run %s is not in the history as failed (%v)", e.RunID, err)
	}
	job, _, err := d.st.GetJob(ctx, e.JobID)
	if err != nil {
		note("get job: %v", err)
		return
	}
	switch e.Type {
	case types.EventJobFailed, types.EventJobDead, types.EventJobAutoRecovered:
		if e.Status == "" && e.Type != types.EventJobAutoRecovered {
			note("no status of the job")
		}
		if e.Status != "" && e.Status != job.Status {
			note("says the job is %s, it is %s", e.Status, job.Status)
		}
	}
	d.Dispatcher.PublishEvent(e)
}

// What orphan recovery announces has happened by then: the run is in the history and no longer
// active, and the job is as the event says.
func TestCheckOrphanedRuns_EventsComeAfterTheirEffects(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(job *types.Job)
		want  types.EventType
	}{
		{"redispatched", func(job *types.Job) {}, types.EventJobAutoRecovered},
		{"dead", func(job *types.Job) { job.RetryPolicy = &types.RetryPolicy{MaxAttempts: 1}; job.Attempt = 1 }, types.EventJobDead},
		{"task of a workflow", func(job *types.Job) {
			wf := "wf-" + job.ID
			job.WorkflowID = &wf
			job.Status = types.JobStatusRunning
		}, types.EventJobFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := newTestStore(t)
			d := &lookingDispatcher{Dispatcher: dispatcher.New(st, nil), st: st}
			s := New(Config{Store: st, Dispatcher: d})
			job := createOneOffJob(t, st, 0, "k")
			editJob(t, st, job.ID, tc.setup)

			loseRun(t, st, s, job.ID, 0, job.ID)

			var got []types.EventType
			for _, e := range d.events {
				got = append(got, e.Type)
			}
			if want := []types.EventType{types.EventNodeCrashDetected, tc.want}; !slices.Equal(got, want) {
				t.Fatalf("events %v, want %v", got, want)
			}
			for _, p := range d.problems {
				t.Error(p)
			}
		})
	}

	// Retries used up, and the job stays on its schedule.
	t.Run("kept on its schedule", func(t *testing.T) {
		st := newTestStore(t)
		d := &lookingDispatcher{Dispatcher: dispatcher.New(st, nil), st: st}
		s := New(Config{Store: st, Dispatcher: d})
		entry := createScheduledJob(t, st, nil, time.Now().Add(time.Hour))
		editJob(t, st, entry.JobID, func(job *types.Job) {
			job.Schedule.KeepOnFailure = true
			job.RetryPolicy = &types.RetryPolicy{MaxAttempts: 1}
		})
		loseRun(t, st, s, entry.JobID, 1, entry.JobID+":1")
		if n := len(d.events); n != 2 || d.events[1].Type != types.EventJobFailed {
			t.Fatalf("events %+v", d.events)
		}
		for _, p := range d.problems {
			t.Error(p)
		}
	})
}

// faultyRedis fails commands of the scheduler on cue.
type faultyRedis struct {
	rueidis.Client
	dead     atomic.Bool  // every command fails, as if the process had died
	failCAS  atomic.Int32 // so many updates of failJob fail
	failJob  string       // the key of the job whose updates fail
	dieAfter string       // a key after whose first write the process dies
	prefix   string
}

func (f *faultyRedis) fail(ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	return f.Client.Do(cancelled, cmd)
}

func (f *faultyRedis) Do(ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	if f.dead.Load() {
		return f.fail(ctx, cmd)
	}
	c := cmd.Commands()
	if (c[0] == "EVALSHA" || c[0] == "EVAL") && len(c) > 3 && c[3] == f.failJob && f.failCAS.Load() > 0 {
		f.failCAS.Add(-1)
		return f.fail(ctx, cmd)
	}
	r := f.Client.Do(ctx, cmd)
	if c[0] == "SET" && f.dieAfter != "" && c[1] == f.dieAfter {
		f.dead.Store(true)
	}
	return r
}

func (f *faultyRedis) DoMulti(ctx context.Context, cmds ...rueidis.Completed) []rueidis.RedisResult {
	if f.dead.Load() {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		return f.Client.DoMulti(cancelled, cmds...)
	}
	return f.Client.DoMulti(ctx, cmds...)
}

func newFaultyStore(t *testing.T) (*store.RedisStore, *faultyRedis) {
	t.Helper()
	healthy := newTestStore(t)
	f := &faultyRedis{Client: healthy.Client()}
	st, err := store.NewRedisStore(f, store.RedisStoreConfig{
		KeyPrefix: healthy.Prefix(),
		Tiers:     []store.TierConfig{{Name: "normal", Weight: 1, FetchBatch: 10}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return st, f
}

// failingDispatcher does something else instead of the first dispatch.
type failingDispatcher struct {
	*dispatcher.Dispatcher
	instead func(ctx context.Context, job *types.Job, attempt int) error
}

func (d *failingDispatcher) Dispatch(ctx context.Context, job *types.Job, attempt int) error {
	if f := d.instead; f != nil {
		d.instead = nil
		return f(ctx, job, attempt)
	}
	return d.Dispatcher.Dispatch(ctx, job, attempt)
}

// eventsOf counts the events of a job in the event stream, by type.
func eventsOf(t *testing.T, st *store.RedisStore, jobID string) map[types.EventType]int {
	t.Helper()
	rdb := st.Client()
	entries, err := rdb.Do(context.Background(), rdb.B().Xrange().Key(store.EventsStreamKey(st.Prefix())).Start("-").End("+").Build()).AsXRange()
	if err != nil {
		t.Fatal(err)
	}
	n := map[types.EventType]int{}
	for _, e := range entries {
		var ev types.JobEvent
		if json.Unmarshal([]byte(e.FieldValues["data"]), &ev) == nil && ev.JobID == jobID {
			n[ev.Type]++
		}
	}
	return n
}

// Recovering a lost run can fail half way: Redis does not take the update of the job, the
// dispatch fails, or the scheduler goes away in the middle. The next check finishes it, and
// what is done is done once.
func TestCheckOrphanedRuns_RecoversAfterFailure(t *testing.T) {
	for _, tc := range []struct {
		name  string
		dead  bool // the job has used up its retries
		twice bool // it may be dispatched again, under the same operation
		fault func(f *faultyRedis, d *failingDispatcher, runID string)
	}{
		{"update of the job fails", false, false, func(f *faultyRedis, d *failingDispatcher, _ string) { f.failCAS.Store(1) }},
		{"dispatch fails", false, false, func(f *faultyRedis, d *failingDispatcher, _ string) {
			d.instead = func(context.Context, *types.Job, int) error { return errors.New("refused") }
		}},
		{"gone between update and dispatch", false, false, func(f *faultyRedis, d *failingDispatcher, _ string) {
			d.instead = func(context.Context, *types.Job, int) error { f.dead.Store(true); return errors.New("gone") }
		}},
		// Before it has noted that the job is dealt with: the next check does it again.
		{"gone right after dispatch", false, true, func(f *faultyRedis, d *failingDispatcher, _ string) {
			d.instead = func(ctx context.Context, job *types.Job, attempt int) error {
				err := d.Dispatcher.Dispatch(ctx, job, attempt)
				f.dead.Store(true)
				return err
			}
		}},
		{"gone before the run is in the history", false, false, func(f *faultyRedis, d *failingDispatcher, runID string) {
			f.dieAfter = store.LostRunRecoveredKey(f.prefix, runID)
		}},
		{"update of a dead job fails", true, false, func(f *faultyRedis, d *failingDispatcher, _ string) { f.failCAS.Store(1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, f := newFaultyStore(t)
			ctx := context.Background()
			job := createOneOffJob(t, st, 0, "k-"+xid.New().String())
			editJob(t, st, job.ID, func(j *types.Job) {
				j.Status = types.JobStatusRunning
				if tc.dead {
					j.RetryPolicy = &types.RetryPolicy{MaxAttempts: 1}
					j.Attempt = 1
				}
			})
			run := &types.JobRun{ID: xid.New().String(), JobID: job.ID, NodeID: "gone-node", Status: types.RunStatusRunning, StartedAt: time.Now().Add(-time.Minute), OperationKey: job.ID}
			if _, err := st.SaveRun(ctx, run); err != nil {
				t.Fatal(err)
			}
			f.failJob = store.JobKey(st.Prefix(), job.ID)
			d := &failingDispatcher{Dispatcher: dispatcher.New(st, nil)}
			f.prefix = st.Prefix()
			tc.fault(f, d, run.ID)
			New(Config{Store: st, Dispatcher: d}).checkOrphanedRuns(ctx)
			f.dead.Store(false)
			f.failCAS.Store(0)

			// The next check, by this scheduler or by one that has taken over.
			New(Config{Store: st, Dispatcher: dispatcher.New(st, nil)}).checkOrphanedRuns(ctx)
			New(Config{Store: st, Dispatcher: dispatcher.New(st, nil)}).checkOrphanedRuns(ctx)

			got := getJob(t, st, job.ID)
			want, wantEntries, wantEvent := types.JobStatusPending, 1, types.EventJobAutoRecovered
			if tc.dead {
				want, wantEntries, wantEvent = types.JobStatusDead, 0, types.EventJobDead
			}
			if got.Status != want {
				t.Errorf("job is %s, want %s", got.Status, want)
			}
			entries := workEntries(t, st)
			if n := len(entries); n != wantEntries && !(tc.twice && n == 2) {
				t.Errorf("%d dispatches, want %d", n, wantEntries)
			}
			for _, e := range entries {
				if got := e.FieldValues["operation_key"]; got != run.OperationKey {
					t.Errorf("dispatched as operation %q, want %q", got, run.OperationKey)
				}
			}
			if _, _, err := st.GetRun(ctx, run.ID); !errors.Is(err, types.ErrRunNotFound) {
				t.Errorf("the lost run is still there (%v)", err)
			}
			if runs, _, _ := st.ListJobRuns(ctx, store.RunFilter{JobID: &job.ID, Limit: 5}); len(runs) != 1 || runs[0].Status != types.RunStatusFailed {
				t.Errorf("history: %+v", runs)
			}
			if n := eventsOf(t, st, job.ID); n[types.EventNodeCrashDetected] != 1 || n[wantEvent] != 1 {
				t.Errorf("events %v, want one %s and one %s", n, types.EventNodeCrashDetected, wantEvent)
			}
			// The job is not part of a workflow or batch: the orchestrator need not see them.
			rdb := st.Client()
			if n, err := rdb.Do(ctx, rdb.B().Xlen().Key(store.OrchestratorStreamKey(st.Prefix())).Build()).AsInt64(); err != nil || n != 0 {
				t.Errorf("%d events for the orchestrator (%v), want none", n, err)
			}
		})
	}
}
