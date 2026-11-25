package scheduler

import (
	"context"
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
		name       string
		dlqAfter   *int
		wantLost   int
		wantStatus types.JobStatus
	}{
		{"dead by default", nil, 4, types.JobStatusDead},
		{"dead after DLQAfter", &two, 3, types.JobStatusDead},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := newTestStore(t)
			s := New(Config{Store: st, Dispatcher: dispatcher.New(st, nil)})
			entry := createScheduledJob(t, st, nil, time.Now().Add(time.Hour))
			editJob(t, st, entry.JobID, func(job *types.Job) { job.DLQAfter = tc.dlqAfter })

			if lost := loseAllRuns(t, st, s, entry.JobID); lost != tc.wantLost {
				t.Fatalf("ended after %d lost runs, want %d", lost, tc.wantLost)
			}
			job := getJob(t, st, entry.JobID)
			if job.Status != tc.wantStatus {
				t.Fatalf("status = %s, want %s", job.Status, tc.wantStatus)
			}
		})
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
