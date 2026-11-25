package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/dispatcher"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
)

func claimKey(t *testing.T, st *store.RedisStore, key, jobID string) {
	t.Helper()
	if err := st.SetUniqueKey(context.Background(), key, jobID); err != nil {
		t.Fatalf("set unique key: %v", err)
	}
}

func keyHolder(t *testing.T, st *store.RedisStore, key string) string {
	t.Helper()
	holder, _, err := st.CheckUniqueKey(context.Background(), key)
	if err != nil {
		t.Fatalf("check unique key: %v", err)
	}
	return holder
}

// Going dead on orphan recovery frees the unique key, but only the job's own.
func TestCheckOrphanedRuns_DeadJob_ReleasesOwnUniqueKey(t *testing.T) {
	for _, tc := range []struct {
		name  string
		other bool
	}{{"own", false}, {"held by another job", true}} {
		t.Run(tc.name, func(t *testing.T) {
			st := newTestStore(t)
			s := New(Config{Store: st, Dispatcher: dispatcher.New(st, nil)})
			job := createOneOffJob(t, st, 3, "k")
			holder, want := job.ID, ""
			if tc.other {
				holder, want = "other-job", "other-job"
			}
			claimKey(t, st, "k", holder)
			orphanRun(t, st, job.ID, 3)

			s.checkOrphanedRuns(context.Background())

			if got := getJob(t, st, job.ID).Status; got != types.JobStatusDead {
				t.Fatalf("status = %s, want dead", got)
			}
			if got := keyHolder(t, st, "k"); got != want {
				t.Fatalf("key holder = %q, want %q", got, want)
			}
		})
	}
}

// A dead or cancelled job has given up its unique key; losing a leftover run
// must not bring it back.
func TestCheckOrphanedRuns_EndedJob_IsNotRevived(t *testing.T) {
	for _, status := range []types.JobStatus{types.JobStatusDead, types.JobStatusCancelled} {
		t.Run(string(status), func(t *testing.T) {
			st := newTestStore(t)
			s := New(Config{Store: st, Dispatcher: dispatcher.New(st, nil)})
			entry := createScheduledJob(t, st, nil, time.Now().Add(time.Hour))
			editJob(t, st, entry.JobID, func(job *types.Job) { job.Status = status })
			orphanRun(t, st, entry.JobID, 0)

			s.checkOrphanedRuns(context.Background())

			if got := getJob(t, st, entry.JobID).Status; got != status {
				t.Fatalf("status = %s, want %s", got, status)
			}
			if n := len(workEntries(t, st)); n != 0 {
				t.Fatalf("expected no redispatch, got %d stream entries", n)
			}
			if runs, _ := st.ListRuns(context.Background()); len(runs) != 0 {
				t.Fatalf("orphaned run was not cleaned up: %d left", len(runs))
			}
		})
	}
}

// The scheduler also frees the key when it drops the schedule of a dead job,
// which covers a worker that stopped between the two writes.
func TestProcessScheduleEntry_DeadJob_ReleasesUniqueKey(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	s := New(Config{Store: st, Dispatcher: dispatcher.New(st, nil)})

	scheduledAt := time.Now().Add(-time.Second)
	entry := createScheduledJob(t, st, nil, scheduledAt)
	key := "k"
	editJob(t, st, entry.JobID, func(job *types.Job) {
		job.Status = types.JobStatusDead
		job.UniqueKey = &key
	})
	claimKey(t, st, key, entry.JobID)

	s.processScheduleEntry(ctx, entry, time.Now())

	if got := keyHolder(t, st, key); got != "" {
		t.Fatalf("dead job still holds the key: %s", got)
	}
	if _, _, err := st.GetSchedule(ctx, entry.JobID); err != types.ErrScheduleNotFound {
		t.Fatalf("schedule err = %v, want not found", err)
	}
}
