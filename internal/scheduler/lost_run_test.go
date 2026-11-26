package scheduler

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/dispatcher"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
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
