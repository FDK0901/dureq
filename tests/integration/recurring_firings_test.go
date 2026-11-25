// Package integration — firings of recurring jobs that are given up, buffered or
// backfilled.
package integration

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/client"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
)

// keptEvery registers a recurring job that is kept when a firing is given up.
func keptEvery(t *testing.T, cli *client.Client, taskType types.TaskType, interval time.Duration, policy *types.RetryPolicy, overlap types.OverlapPolicy) *types.Job {
	t.Helper()
	iv := types.Duration(interval)
	job, err := cli.EnqueueScheduled(context.Background(), &client.EnqueueRequest{
		TaskType: taskType,
		Payload:  json.RawMessage(`{}`),
		Schedule: types.Schedule{
			Type:          types.ScheduleDuration,
			Interval:      &iv,
			KeepOnFailure: true,
			OverlapPolicy: overlap,
		},
		RetryPolicy: policy,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return job
}

func dlqLen(t *testing.T, cli *client.Client) int64 {
	t.Helper()
	rdb := cli.Store().Client()
	n, err := rdb.Do(context.Background(), rdb.B().Xlen().Key(store.DLQStreamKey(cli.Store().Prefix())).Build()).AsInt64()
	if err != nil {
		t.Fatalf("XLEN: %v", err)
	}
	return n
}

// With KeepOnFailure a firing that is given up goes to the DLQ, and the job goes on
// with its next firing.
func TestRecurring_KeepOnFailure_NextFiringRuns(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		failures int // executions of the first firing
	}{
		{"retries used up", errBoom, 2},
		{"non-retryable error", &types.NonRetryableError{Err: errBoom}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recorder{fail: func(run int) error {
				if run < tc.failures {
					return tc.err
				}
				return nil
			}}
			_, cli := startRecorder(t, "rec.keep", rec)
			job := keptEvery(t, cli, "rec.keep", 4*time.Second, fastRetry(2), "")

			waitFor(t, 25*time.Second, "next firing", func() bool {
				return len(rec.seen()) > tc.failures || statusOf(t, cli, job.ID) == types.JobStatusDead
			})
			if got := statusOf(t, cli, job.ID); got == types.JobStatusDead {
				t.Fatalf("the job died (attempts seen %v)", rec.seen())
			}
			if got := rec.seen()[tc.failures]; got != 0 {
				t.Fatalf("next firing executed as attempt %d, want 0 (attempts seen %v)", got, rec.seen())
			}
			waitStatus(t, cli, job.ID, types.JobStatusScheduled)
			if n := dlqLen(t, cli); n != 1 {
				t.Fatalf("%d messages in the DLQ, want 1", n)
			}
		})
	}
}

// Given up, the job is back on its schedule and can be told from one that succeeded.
func TestRecurring_KeepOnFailure_State(t *testing.T) {
	rec := &recorder{fail: func(int) error { return errBoom }}
	_, cli := startRecorder(t, "rec.keepstate", rec)
	job := keptEvery(t, cli, "rec.keepstate", time.Hour, fastRetry(2), "")
	dispatchFiring(t, cli, job, 0)

	waitFor(t, 10*time.Second, "DLQ", func() bool { return dlqLen(t, cli) == 1 })
	got, err := cli.GetJob(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if got.Status != types.JobStatusScheduled || got.Attempt != 0 {
		t.Fatalf("job is %s at attempt %d, want scheduled at 0", got.Status, got.Attempt)
	}
	if got.LastError == nil || got.ConsecutiveErrors != 2 || got.RunCount != 2 {
		t.Fatalf("last error %v, %d consecutive errors, %d runs; want an error, 2, 2", got.LastError, got.ConsecutiveErrors, got.RunCount)
	}
}

// With its schedule gone there is nothing to keep: the job dies.
func TestRecurring_KeepOnFailure_NoSchedule_JobDies(t *testing.T) {
	rec := &recorder{fail: func(int) error { return errBoom }}
	_, cli := startRecorder(t, "rec.keepgone", rec)
	job := keptEvery(t, cli, "rec.keepgone", time.Hour, fastRetry(2), "")
	if err := cli.Store().DeleteSchedule(context.Background(), job.ID); err != nil {
		t.Fatalf("delete schedule: %v", err)
	}
	dispatchFiring(t, cli, job, 0)
	waitStatus(t, cli, job.ID, types.JobStatusDead)
}

// A firing that was buffered behind one that is given up is not left behind.
func TestRecurring_KeepOnFailure_DrainsOverlapBuffer(t *testing.T) {
	rec := &recorder{fail: func(run int) error {
		if run == 0 {
			return &types.NonRetryableError{Err: errBoom}
		}
		return nil
	}}
	_, cli := startRecorder(t, "rec.keepbuffer", rec)
	job := keptEvery(t, cli, "rec.keepbuffer", time.Hour, fastRetry(2), types.OverlapBufferOne)
	if err := cli.Store().SaveOverlapBuffer(context.Background(), job.ID, time.Now()); err != nil {
		t.Fatalf("buffer: %v", err)
	}
	dispatchFiring(t, cli, job, 0)
	waitFor(t, 10*time.Second, "buffered firing", func() bool { return len(rec.seen()) == 2 })
}

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

// RunCount goes on counting where Attempt starts anew.
func TestRecurring_RunCount_CountsAllExecutions(t *testing.T) {
	rec := &recorder{fail: func(run int) error {
		if run == 1 {
			return errBoom
		}
		return nil
	}}
	_, cli := startRecorder(t, "rec.count", rec)
	job := mustEvery(t, cli, "rec.count", time.Hour, fastRetry(3), "rec-count")
	for executions, want := range []int{1, 3} { // the second firing is retried once
		dispatchFiring(t, cli, job, 0)
		waitFor(t, 10*time.Second, "run count", func() bool {
			j, err := cli.GetJob(context.Background(), job.ID)
			return err == nil && j.RunCount == want && j.Status == types.JobStatusScheduled
		})
		if n := len(rec.seen()); n != want {
			t.Fatalf("firing %d: %d executions, want %d", executions, n, want)
		}
	}
	if j, _ := cli.GetJob(context.Background(), job.ID); j.Attempt != 0 {
		t.Fatalf("attempt = %d, want 0", j.Attempt)
	}
}

// A one-off job keeps the attempts it took, and is counted too.
func TestOneOff_AttemptAndRunCount(t *testing.T) {
	rec := &recorder{fail: func(run int) error {
		if run == 0 {
			return errBoom
		}
		return nil
	}}
	_, cli := startRecorder(t, "rec.one", rec)
	job, err := cli.EnqueueScheduled(context.Background(), &client.EnqueueRequest{
		TaskType:    "rec.one",
		Payload:     json.RawMessage(`{}`),
		Schedule:    types.Schedule{Type: types.ScheduleImmediate},
		RetryPolicy: fastRetry(3),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitStatus(t, cli, job.ID, types.JobStatusCompleted)
	got, err := cli.GetJob(context.Background(), job.ID)
	if err != nil || got.Attempt != 2 || got.RunCount != 2 {
		t.Fatalf("attempt %d, run count %d, err %v; want 2, 2", got.Attempt, got.RunCount, err)
	}
}

// Without KeepOnFailure nothing changes: the job dies and its schedule ends.
func TestRecurring_NonRetryableError_JobDies(t *testing.T) {
	rec := &recorder{fail: func(int) error { return &types.NonRetryableError{Err: errBoom} }}
	_, cli := startRecorder(t, "rec.nonretry", rec)
	job := mustEvery(t, cli, "rec.nonretry", 3*time.Second, fastRetry(3), "rec-nonretry")
	waitStatus(t, cli, job.ID, types.JobStatusDead)
	waitFor(t, 15*time.Second, "schedule removed", func() bool {
		_, _, err := cli.Store().GetSchedule(context.Background(), job.ID)
		return errors.Is(err, types.ErrScheduleNotFound)
	})
}
