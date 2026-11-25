package archival

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
)

// saveRun stores a run that started and finished that long ago in the run history.
func saveRun(t *testing.T, st *store.RedisStore, id string, status types.RunStatus, started, finished time.Duration) {
	t.Helper()
	finishedAt := time.Now().Add(-finished)
	err := st.SaveJobRun(context.Background(), &types.JobRun{
		ID: id, JobID: "job-" + id[:1], Status: status, StartedAt: time.Now().Add(-started), FinishedAt: &finishedAt,
	})
	if err != nil {
		t.Fatalf("save run: %v", err)
	}
}

// runCount returns how many runs the index has.
func runCount(t *testing.T, st *store.RedisStore, index string) int64 {
	t.Helper()
	n, err := st.Client().Do(context.Background(), st.Client().B().Zcard().Key(index).Build()).AsInt64()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRunHistoryCleanup(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	saveRun(t, st, "a-old", types.RunStatusSucceeded, 50*time.Hour, 49*time.Hour)
	saveRun(t, st, "a-old-failed", types.RunStatusFailed, 50*time.Hour, 49*time.Hour)
	saveRun(t, st, "a-long", types.RunStatusSucceeded, 50*time.Hour, time.Hour) // it has only just finished
	saveRun(t, st, "a-new", types.RunStatusSucceeded, time.Hour, time.Hour)
	saveRun(t, st, "b-old", types.RunStatusSucceeded, 50*time.Hour, 49*time.Hour)
	if _, err := st.CreateJob(ctx, &types.Job{ID: "job-a", TaskType: "t", Status: types.JobStatusCompleted, UpdatedAt: time.Now().Add(-100 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SaveRun(ctx, &types.JobRun{ID: "active", JobID: "job-a", Status: types.RunStatusRunning, StartedAt: time.Now().Add(-100 * time.Hour)}); err != nil {
		t.Fatal(err)
	}

	removed, err := st.Cleanup(ctx, time.Now().Add(-24*time.Hour))
	if err != nil || removed != 3 {
		t.Fatalf("removed %d runs (%v), want 3", removed, err)
	}
	for _, id := range []string{"a-old", "a-old-failed", "b-old"} {
		if _, _, err := st.GetHistoryRun(ctx, id); err != types.ErrRunNotFound {
			t.Errorf("old run %s: %v, want it to be gone", id, err)
		}
	}
	for _, id := range []string{"a-long", "a-new"} {
		if _, _, err := st.GetHistoryRun(ctx, id); err != nil {
			t.Errorf("run %s: %v", id, err)
		}
	}
	for index, want := range map[string]int64{
		store.HistoryRunsKey(st.Prefix()):                      2,
		store.HistoryRunsByJobKey(st.Prefix(), "job-a"):        2,
		store.HistoryRunsByJobKey(st.Prefix(), "job-b"):        0,
		store.HistoryRunsByStatusKey(st.Prefix(), "succeeded"): 2,
		store.HistoryRunsByStatusKey(st.Prefix(), "failed"):    0,
	} {
		if got := runCount(t, st, index); got != want {
			t.Errorf("%s: %d runs, want %d", index, got, want)
		}
	}
	if _, _, err := st.GetJob(ctx, "job-a"); err != nil {
		t.Errorf("job: %v", err)
	}
	if _, _, err := st.GetRun(ctx, "active"); err != nil {
		t.Errorf("active run: %v", err)
	}
	if removed, err = st.Cleanup(ctx, time.Now().Add(-24*time.Hour)); err != nil || removed != 0 {
		t.Fatalf("removed %d more runs (%v), want none", removed, err)
	}
}

// More runs than are handled at once, with ones in between that stay.
func TestRunHistoryCleanup_ManyRuns(t *testing.T) {
	st := newTestStore(t)
	const n = 1700
	for i := range n {
		finished := 49 * time.Hour
		if i%10 == 0 {
			finished = time.Hour
		}
		saveRun(t, st, fmt.Sprint("a", i), types.RunStatusSucceeded, 50*time.Hour+time.Duration(i)*time.Second, finished)
	}
	removed, err := st.Cleanup(context.Background(), time.Now().Add(-24*time.Hour))
	if err != nil || removed != n-n/10 {
		t.Fatalf("removed %d runs (%v), want %d", removed, err, n-n/10)
	}
	if left := runCount(t, st, store.HistoryRunsKey(st.Prefix())); left != n/10 {
		t.Fatalf("%d runs left, want %d", left, n/10)
	}
}

// Runs are removed by the cleaner only if that is asked for, and then only they are.
func TestCleaner_RunHistoryIsOptIn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, tc := range []struct {
		name          string
		cfg           Config
		runs, jobs    int64
		startsNothing bool
	}{
		{"default", Config{}, 1, 1, true},
		{"negative", Config{RunHistoryRetention: -time.Hour}, 1, 1, true},
		{"job retention", Config{RetentionPeriod: 24 * time.Hour}, 1, 0, false},
		{"run history retention", Config{RunHistoryRetention: 24 * time.Hour}, 0, 1, false},
		{"both", Config{RetentionPeriod: 24 * time.Hour, RunHistoryRetention: 24 * time.Hour}, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := newTestStore(t)
			saveRun(t, st, "a-old", types.RunStatusSucceeded, 50*time.Hour, 49*time.Hour)
			if _, err := st.CreateJob(ctx, &types.Job{ID: "job-a", TaskType: "t", Status: types.JobStatusCompleted, UpdatedAt: time.Now().Add(-50 * time.Hour)}); err != nil {
				t.Fatal(err)
			}
			tc.cfg.Store = st
			c := New(tc.cfg)
			c.Start(ctx)
			defer c.Stop()
			if (c.cancel == nil) != tc.startsNothing {
				t.Fatalf("started: %v", c.cancel != nil)
			}
			runs, jobs := int64(1), int64(1)
			for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
				runs = runCount(t, st, store.HistoryRunsKey(st.Prefix()))
				jobs = runCount(t, st, store.JobsByCreatedKey(st.Prefix()))
				if !tc.startsNothing && runs == tc.runs && jobs == tc.jobs || tc.startsNothing && time.Until(deadline) < 2500*time.Millisecond {
					break
				}
			}
			if runs != tc.runs || jobs != tc.jobs {
				t.Fatalf("%d runs and %d jobs left, want %d and %d", runs, jobs, tc.runs, tc.jobs)
			}
		})
	}
}
