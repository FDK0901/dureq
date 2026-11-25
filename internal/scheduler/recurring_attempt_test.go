package scheduler

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/dispatcher"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/rs/xid"
)

// editJob changes the stored job.
func editJob(t *testing.T, st *store.RedisStore, jobID string, edit func(job *types.Job)) {
	t.Helper()
	ctx := context.Background()
	job, rev, err := st.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	edit(job)
	if _, err := st.UpdateJob(ctx, job, rev); err != nil {
		t.Fatalf("update job: %v", err)
	}
}

func getJob(t *testing.T, st *store.RedisStore, jobID string) *types.Job {
	t.Helper()
	job, _, err := st.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	return job
}

// orphanRun stores a running run of the job on a node that is gone.
func orphanRun(t *testing.T, st *store.RedisStore, jobID string, attempt int) {
	t.Helper()
	run := &types.JobRun{
		ID:        xid.New().String(),
		JobID:     jobID,
		NodeID:    "gone-node",
		Status:    types.RunStatusRunning,
		Attempt:   attempt,
		StartedAt: time.Now().Add(-time.Minute),
	}
	if _, err := st.SaveRun(context.Background(), run); err != nil {
		t.Fatalf("save run: %v", err)
	}
}

// Each firing of a recurring job starts with a full retry budget, whatever the
// job went through before (63: counted up by a version that never reset it).
func TestProcessScheduleEntry_RecurringFiringStartsAtAttemptZero(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	s := New(Config{Store: st, Dispatcher: dispatcher.New(st, nil)})

	scheduledAt := time.Now().Add(-time.Second).Truncate(time.Millisecond)
	entry := createScheduledJob(t, st, nil, scheduledAt)
	editJob(t, st, entry.JobID, func(job *types.Job) { job.Attempt = 63 })

	s.processScheduleEntry(ctx, entry, scheduledAt.Add(500*time.Millisecond))

	entries := workEntries(t, st)
	if len(entries) != 1 {
		t.Fatalf("expected 1 stream entry, got %d", len(entries))
	}
	if got := entries[0].FieldValues["attempt"]; got != "0" {
		t.Fatalf("dispatched attempt = %s, want 0", got)
	}
	if got := getJob(t, st, entry.JobID).Attempt; got != 0 {
		t.Fatalf("stored attempt = %d, want 0", got)
	}
}

// A firing that turns out to be dispatched already leaves the attempt count of
// the earlier run alone.
func TestProcessScheduleEntry_DuplicateFiring_KeepsAttempt(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	s := New(Config{Store: st, Dispatcher: dispatcher.New(st, nil)})

	scheduledAt := time.Now().Add(-time.Second).Truncate(time.Millisecond)
	entry := createScheduledJob(t, st, nil, scheduledAt)
	staleEntry := *entry

	s.processScheduleEntry(ctx, entry, scheduledAt.Add(500*time.Millisecond))
	// The first run fails once in between.
	editJob(t, st, entry.JobID, func(job *types.Job) {
		job.Status = types.JobStatusRetrying
		job.Attempt = 1
	})
	s.processScheduleEntry(ctx, &staleEntry, scheduledAt.Add(2500*time.Millisecond))

	if n := len(workEntries(t, st)); n != 1 {
		t.Fatalf("expected 1 stream entry, got %d", n)
	}
	job := getJob(t, st, entry.JobID)
	if job.Attempt != 1 || job.Status != types.JobStatusRetrying {
		t.Fatalf("job is %s at attempt %d, want retrying at attempt 1", job.Status, job.Attempt)
	}
}

// Orphan recovery of a recurring job goes by the attempts of the run that was
// lost, not by what is stored on the job.
func TestCheckOrphanedRuns_RecurringJob_GoesByRunAttempt(t *testing.T) {
	for _, tc := range []struct {
		name              string
		runAttempt        int
		jobAttempt        int
		consecutiveErrors int
		wantStatus        types.JobStatus
		wantAttempt       string // of the recovery message
	}{
		{"first attempt lost", 0, 0, 0, types.JobStatusPending, "0"},
		{"last attempt lost", 2, 2, 2, types.JobStatusPending, "2"},
		{"other firings failed meanwhile", 0, 3, 3, types.JobStatusPending, "0"},
		{"count carried over from before v0.2.5", 63, 63, 0, types.JobStatusPending, "0"},
		{"carried over, failed once since the last success", 63, 63, 1, types.JobStatusPending, "1"},
		{"used up", 3, 3, 3, types.JobStatusDead, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := newTestStore(t)
			s := New(Config{Store: st, Dispatcher: dispatcher.New(st, nil)})
			entry := createScheduledJob(t, st, nil, time.Now().Add(time.Hour))
			editJob(t, st, entry.JobID, func(job *types.Job) {
				job.Status = types.JobStatusPending
				job.Attempt = tc.jobAttempt
				job.ConsecutiveErrors = tc.consecutiveErrors
			})
			orphanRun(t, st, entry.JobID, tc.runAttempt)

			s.checkOrphanedRuns(context.Background())

			if got := getJob(t, st, entry.JobID).Status; got != tc.wantStatus {
				t.Fatalf("status = %s, want %s", got, tc.wantStatus)
			}
			entries := workEntries(t, st)
			if tc.wantAttempt == "" {
				if len(entries) != 0 {
					t.Fatalf("expected no redispatch, got %d stream entries", len(entries))
				}
				return
			}
			if len(entries) != 1 {
				t.Fatalf("expected 1 redispatch, got %d stream entries", len(entries))
			}
			if got := entries[0].FieldValues["attempt"]; got != tc.wantAttempt {
				t.Fatalf("redispatched attempt = %s, want %s", got, tc.wantAttempt)
			}
		})
	}
}

// createOneOffJob stores an immediate job.
func createOneOffJob(t *testing.T, st *store.RedisStore, attempt int, uniqueKey string) *types.Job {
	t.Helper()
	job := &types.Job{
		ID:        xid.New().String(),
		TaskType:  "test.orphan",
		Payload:   json.RawMessage(`{}`),
		Schedule:  types.Schedule{Type: types.ScheduleImmediate},
		Status:    types.JobStatusPending,
		Attempt:   attempt,
		UniqueKey: &uniqueKey,
	}
	if _, err := st.CreateJob(context.Background(), job); err != nil {
		t.Fatalf("create job: %v", err)
	}
	return job
}

// A one-off job keeps being judged by the attempts stored on the job.
func TestCheckOrphanedRuns_OneOffJob_GoesByJobAttempt(t *testing.T) {
	st := newTestStore(t)
	s := New(Config{Store: st, Dispatcher: dispatcher.New(st, nil)})
	job := createOneOffJob(t, st, 3, "k")
	orphanRun(t, st, job.ID, 0)

	s.checkOrphanedRuns(context.Background())

	if got := getJob(t, st, job.ID).Status; got != types.JobStatusDead {
		t.Fatalf("status = %s, want dead", got)
	}
}
