package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/dispatcher"
	"github.com/FDK0901/dureq/pkg/types"
)

func pause(job *types.Job) { job.Pause(time.Now(), 0) }

func resume(job *types.Job) { job.Resume(time.Now()) }

// A paused job does not fire; its schedule goes on.
func TestProcessScheduleEntry_PausedJob_DoesNotFire(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	s := New(Config{Store: st, Dispatcher: dispatcher.New(st, nil)})

	scheduledAt := time.Now().Add(-time.Second)
	entry := createScheduledJob(t, st, nil, scheduledAt)
	editJob(t, st, entry.JobID, pause)

	now := time.Now()
	s.processScheduleEntry(ctx, entry, now)

	if n := len(workEntries(t, st)); n != 0 {
		t.Fatalf("paused job was dispatched %d time(s)", n)
	}
	if got := getJob(t, st, entry.JobID); got.Status != types.JobStatusPaused || got.PausedAt == nil {
		t.Fatalf("job is %s (paused at %v), want it to stay paused", got.Status, got.PausedAt)
	}
	stored, _, err := st.GetSchedule(ctx, entry.JobID)
	if err != nil {
		t.Fatalf("schedule of a paused job is gone: %v", err)
	}
	if !stored.NextRunAt.After(now) {
		t.Fatalf("next run at %v, want it after %v", stored.NextRunAt, now)
	}
}

// What fell due during the pause is not made up for when the job is resumed, even if
// the schedule catches up on firings that were missed for lack of a scheduler.
func TestProcessScheduleEntry_ResumedJob_DoesNotCatchUpOnPause(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	s := New(Config{Store: st, Dispatcher: dispatcher.New(st, nil)})

	start := time.Now().Add(-10 * time.Minute).Truncate(time.Millisecond)
	entry := createScheduledJob(t, st, nil, start)
	window := types.Duration(time.Hour)
	entry.Schedule.CatchupWindow = &window
	entry.LastProcessedAt = &start
	editJob(t, st, entry.JobID, pause)

	for i := 1; i <= 5; i++ { // a firing a minute
		s.processScheduleEntry(ctx, entry, start.Add(time.Duration(i)*time.Minute))
	}
	editJob(t, st, entry.JobID, resume)
	s.processScheduleEntry(ctx, entry, start.Add(6*time.Minute))

	if n := len(workEntries(t, st)); n != 1 {
		t.Fatalf("%d firings dispatched after the pause, want 1", n)
	}
}

// The scheduler keeps jobs for some seconds. One that it kept from before the job was
// paused must not fire, and one from before it was resumed must not hold it back.
func TestProcessScheduleEntry_PauseIsSeenDespiteCache(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	s := New(Config{Store: st, Dispatcher: dispatcher.New(st, nil)})

	entry := createScheduledJob(t, st, nil, time.Now().Add(-time.Second))
	keep := func() {
		if _, _, err := s.cache.GetJob(ctx, entry.JobID); err != nil {
			t.Fatalf("get job: %v", err)
		}
	}

	keep()
	editJob(t, st, entry.JobID, pause)
	s.processScheduleEntry(ctx, entry, time.Now())
	if n := len(workEntries(t, st)); n != 0 {
		t.Fatalf("paused job was dispatched %d time(s)", n)
	}

	keep()
	editJob(t, st, entry.JobID, resume)
	s.processScheduleEntry(ctx, entry, time.Now())
	if n := len(workEntries(t, st)); n != 1 {
		t.Fatalf("resumed job was dispatched %d time(s), want 1", n)
	}
}
