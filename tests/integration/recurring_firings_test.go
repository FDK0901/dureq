// Package integration — firings of recurring jobs that are given up, buffered or
// backfilled.
package integration

import (
	"context"
	"testing"
	"time"

	"github.com/FDK0901/dureq/pkg/types"
)

// Every buffered firing runs: the second one is not taken for the first by the
// operation ledger.
func TestRecurring_OverlapBuffer_EveryFiringRuns(t *testing.T) {
	rec := &recorder{}
	_, cli := startRecorder(t, "rec.buffer", rec)
	job := mustEvery(t, cli, "rec.buffer", time.Hour, fastRetry(3), "rec-buffer")
	editStored(t, cli, job.ID, func(j *types.Job) { j.Schedule.OverlapPolicy = types.OverlapBufferOne })
	for round := 1; round <= 3; round++ {
		if err := cli.Store().SaveOverlapBuffer(context.Background(), job.ID, time.Now()); err != nil {
			t.Fatalf("buffer: %v", err)
		}
		dispatchFiring(t, cli, job, 0) // its success drains the buffer
		waitFor(t, 10*time.Second, "firing and buffered firing", func() bool { return len(rec.seen()) == 2*round })
	}
	for i, a := range rec.seen() {
		if a != 0 {
			t.Fatalf("run %d executed as attempt %d (attempts seen %v)", i, a, rec.seen())
		}
	}
}

// A firing stays in the buffer while another run of the job is active; that one
// drains it.
func TestRecurring_OverlapBuffer_KeptWhileAnotherRunIsActive(t *testing.T) {
	started := make(chan struct{}, 10)
	proceed := make(chan struct{})
	rec := &recorder{fail: func(run int) error {
		if run < 2 {
			started <- struct{}{}
			<-proceed
		}
		return nil
	}}
	_, cli := startRecorder(t, "rec.bufferkept", rec)
	job := mustEvery(t, cli, "rec.bufferkept", time.Hour, fastRetry(3), "rec-bufferkept")
	editStored(t, cli, job.ID, func(j *types.Job) { j.Schedule.OverlapPolicy = types.OverlapBufferOne })
	for range 2 {
		dispatchFiring(t, cli, job, 0)
		<-started
	}
	if err := cli.Store().SaveOverlapBuffer(context.Background(), job.ID, time.Now()); err != nil {
		t.Fatalf("buffer: %v", err)
	}

	proceed <- struct{}{} // one of the two is done, the other one still going
	time.Sleep(1500 * time.Millisecond)
	if n := len(rec.seen()); n != 2 {
		t.Fatalf("%d executions while a run is active, want 2", n)
	}
	proceed <- struct{}{}
	waitFor(t, 10*time.Second, "buffered firing", func() bool { return len(rec.seen()) == 3 })
}

// Every backfilled firing runs.
func TestRecurring_BackfillSchedule_EveryFiringRuns(t *testing.T) {
	rec := &recorder{}
	_, cli := startRecorder(t, "rec.backfillall", rec)
	job := mustEvery(t, cli, "rec.backfillall", time.Hour, fastRetry(3), "rec-backfillall")
	window := types.Duration(10 * time.Hour)
	editStored(t, cli, job.ID, func(j *types.Job) { j.Schedule.CatchupWindow = &window })
	now := time.Now()
	for round := 1; round <= 3; round++ {
		n, err := cli.BackfillSchedule(context.Background(), job.ID, now.Add(-90*time.Minute), now)
		if err != nil || n != 1 {
			t.Fatalf("backfill dispatched %d, err %v", n, err)
		}
		waitFor(t, 10*time.Second, "backfilled run", func() bool { return len(rec.seen()) == round })
		waitStatus(t, cli, job.ID, types.JobStatusScheduled)
	}
}
