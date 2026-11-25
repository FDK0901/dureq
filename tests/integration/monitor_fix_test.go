// Package integration — paused recurring jobs, node totals, workflow task names.
package integration

import (
	"context"
	"testing"
	"time"

	"github.com/FDK0901/dureq/pkg/types"
)

// A recurring job that is paused after too many errors in a row stays so until it is
// resumed, and then runs what it was paused on and goes on with its schedule.
func TestRecurring_AutoPaused_DoesNotFireUntilResumed(t *testing.T) {
	const threshold = 2
	rec := &recorder{fail: func(run int) error {
		if run < threshold {
			return errBoom
		}
		return nil
	}}
	_, cli := startRecorder(t, "rec.paused", rec)
	policy := fastRetry(10)
	policy.PauseAfterErrCount = threshold
	job := mustEvery(t, cli, "rec.paused", time.Second, policy, "rec-paused")

	waitStatus(t, cli, job.ID, types.JobStatusPaused)
	time.Sleep(4 * time.Second) // four firings
	if got := len(rec.seen()); got != threshold {
		t.Fatalf("ran %d times, want it to stop at %d", got, threshold)
	}
	if got := statusOf(t, cli, job.ID); got != types.JobStatusPaused {
		t.Fatalf("job is %s, want it to stay paused", got)
	}

	if err := cli.ResumeJob(context.Background(), job.ID); err != nil {
		t.Fatalf("resume: %v", err)
	}
	waitFor(t, 10*time.Second, "runs after the resume", func() bool { return len(rec.seen()) >= threshold+3 })
	if got := statusOf(t, cli, job.ID); got == types.JobStatusPaused {
		t.Fatal("job is paused again")
	}
}

// The same for a pause that ends by itself.
func TestRecurring_AutoPaused_ResumesByItself(t *testing.T) {
	const threshold = 2
	rec := &recorder{fail: func(run int) error {
		if run < threshold {
			return errBoom
		}
		return nil
	}}
	_, cli := startRecorder(t, "rec.selfresume", rec)
	policy := fastRetry(10)
	policy.PauseAfterErrCount = threshold
	policy.PauseRetryDelay = 3 * time.Second
	job := mustEvery(t, cli, "rec.selfresume", time.Second, policy, "rec-selfresume")

	waitStatus(t, cli, job.ID, types.JobStatusPaused)
	waitFor(t, 60*time.Second, "runs after the pause", func() bool { return len(rec.seen()) >= threshold+3 })
}
