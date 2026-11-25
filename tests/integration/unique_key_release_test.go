// Package integration — the release of unique keys when a job ends.
package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/client"
	"github.com/FDK0901/dureq/internal/monitor"
	"github.com/FDK0901/dureq/pkg/types"
)

func holderOf(t *testing.T, cli *client.Client, key string) string {
	t.Helper()
	holder, _, err := cli.Store().CheckUniqueKey(context.Background(), key)
	if err != nil {
		t.Fatalf("check unique key: %v", err)
	}
	return holder
}

// A job that went dead can be registered again under the same key — for good,
// not only until a restart finds it missing.
func TestUniqueKey_DeadJob_CanBeRegisteredAgain(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"retries used up", errBoom},
		{"non-retryable error", &types.NonRetryableError{Err: errBoom}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recorder{fail: func(int) error { return tc.err }}
			_, cli := startRecorder(t, "rec.again", rec)
			job := mustEvery(t, cli, "rec.again", time.Hour, fastRetry(2), "rec-again")
			dispatchFiring(t, cli, job, 0)
			waitStatus(t, cli, job.ID, types.JobStatusDead)

			waitFor(t, 5*time.Second, "key released", func() bool { return holderOf(t, cli, "rec-again") == "" })
			again := mustEvery(t, cli, "rec.again", time.Hour, fastRetry(2), "rec-again")
			if got := holderOf(t, cli, "rec-again"); got != again.ID {
				t.Fatalf("key holder = %q, want %s", got, again.ID)
			}
		})
	}
}

// While a job is retrying it keeps its key.
func TestUniqueKey_RetryingJob_KeepsKey(t *testing.T) {
	rec := &recorder{fail: func(int) error { return errBoom }}
	_, cli := startRecorder(t, "rec.retrying", rec)
	policy := fastRetry(3)
	policy.InitialDelay, policy.MaxDelay = time.Minute, time.Minute
	job := mustEvery(t, cli, "rec.retrying", time.Hour, policy, "rec-retrying")
	dispatchFiring(t, cli, job, 0)
	waitStatus(t, cli, job.ID, types.JobStatusRetrying)

	if _, err := every(cli, "rec.retrying", time.Hour, policy, "rec-retrying"); !errors.Is(err, types.ErrDuplicateJob) {
		t.Fatalf("enqueue err = %v, want duplicate", err)
	}
}

// A job that completes normally keeps its key, as before.
func TestUniqueKey_CompletedOneOff_KeepsKey(t *testing.T) {
	_, cli := startRecorder(t, "rec.oneoff", &recorder{})
	key := "rec-oneoff"
	req := &client.EnqueueRequest{
		TaskType:  "rec.oneoff",
		Payload:   json.RawMessage(`{}`),
		Schedule:  types.Schedule{Type: types.ScheduleImmediate},
		UniqueKey: &key,
	}
	job, err := cli.EnqueueScheduled(context.Background(), req)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitStatus(t, cli, job.ID, types.JobStatusCompleted)
	if _, err := cli.EnqueueScheduled(context.Background(), req); !errors.Is(err, types.ErrDuplicateJob) {
		t.Fatalf("enqueue err = %v, want duplicate", err)
	}
}

// Cancelling a running job frees the key, so the run that was cut off must not
// bring the job back to life as a retry.
func TestUniqueKey_CancelledWhileRunning_IsNotRevived(t *testing.T) {
	flushDB(t)
	ctx := context.Background()
	srv := newServer(t, "recurring-cancel", 10)
	started := make(chan struct{}, 10)
	srv.RegisterHandler(types.HandlerDefinition{TaskType: "rec.cancel", Handler: func(ctx context.Context, _ json.RawMessage) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()
	cli := newClient(t)
	defer cli.Close()

	job := mustEvery(t, cli, "rec.cancel", time.Hour, fastRetry(3), "rec-cancel")
	dispatchFiring(t, cli, job, 0)
	<-started
	if err := cli.Cancel(ctx, job.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	again := mustEvery(t, cli, "rec.cancel", time.Hour, fastRetry(3), "rec-cancel")

	// A revived job would be retried within about a second.
	select {
	case <-started:
		t.Fatalf("the cancelled job was executed again")
	case <-time.After(4 * time.Second):
	}
	if got := statusOf(t, cli, job.ID); got != types.JobStatusCancelled {
		t.Fatalf("cancelled job is now %s", got)
	}
	if got := holderOf(t, cli, "rec-cancel"); got != again.ID {
		t.Fatalf("key holder = %q, want %s", got, again.ID)
	}
}

// No leftover run of a cancelled job changes its status: one that succeeds does not
// complete it, and so one that fails after that does not turn it into a retry.
func TestUniqueKey_CancelledJob_LeftoverRuns(t *testing.T) {
	rec := &recorder{fail: func(run int) error {
		if run == 0 {
			return nil
		}
		return errBoom
	}}
	_, cli := startRecorder(t, "rec.cancelled", rec)
	job := mustEvery(t, cli, "rec.cancelled", time.Hour, fastRetry(3), "rec-cancelled")
	if err := cli.Cancel(context.Background(), job.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	current := mustEvery(t, cli, "rec.cancelled", time.Hour, fastRetry(3), "rec-cancelled")

	for run := 1; run <= 2; run++ {
		dispatchFiring(t, cli, job, 0)
		waitFor(t, 10*time.Second, "leftover run", func() bool { return len(rec.seen()) == run })
	}
	time.Sleep(3 * time.Second)
	if got := statusOf(t, cli, job.ID); got != types.JobStatusCancelled {
		t.Fatalf("cancelled job is now %s", got)
	}
	if n := len(rec.seen()); n != 2 {
		t.Fatalf("%d executions, want 2", n)
	}
	if got := holderOf(t, cli, "rec-cancelled"); got != current.ID {
		t.Fatalf("key holder = %q, want %s", got, current.ID)
	}
}

// A dead job's leftover firing may put it back on its schedule only if the key
// is still free.
func TestUniqueKey_DeadJob_LeftoverFiring(t *testing.T) {
	outcomes := map[string]error{"succeeds": nil, "fails": errBoom, "pauses": &types.PauseError{Reason: "later"}}
	for outcome, result := range outcomes {
		t.Run("key taken, firing "+outcome, func(t *testing.T) {
			rec := &recorder{fail: func(int) error { return result }}
			_, cli := startRecorder(t, "rec.leftover", rec)
			job := mustEvery(t, cli, "rec.leftover", time.Hour, fastRetry(3), "rec-leftover")
			editStored(t, cli, job.ID, func(j *types.Job) { j.Status = types.JobStatusDead })
			if err := cli.Store().DeleteUniqueKey(context.Background(), "rec-leftover"); err != nil {
				t.Fatalf("delete key: %v", err)
			}
			current := mustEvery(t, cli, "rec.leftover", time.Hour, fastRetry(3), "rec-leftover")
			dispatchFiring(t, cli, job, 0)

			waitFor(t, 10*time.Second, "leftover run", func() bool { return len(rec.seen()) > 0 })
			time.Sleep(3 * time.Second)
			if got := statusOf(t, cli, job.ID); got != types.JobStatusDead {
				t.Fatalf("dead job is now %s", got)
			}
			if n := len(rec.seen()); n != 1 {
				t.Fatalf("%d executions, want 1", n)
			}
			if got := holderOf(t, cli, "rec-leftover"); got != current.ID {
				t.Fatalf("key holder = %q, want %s", got, current.ID)
			}
		})
	}

	// With its schedule gone there is nothing to go back to; the job must not take
	// the key with it into "completed".
	t.Run("key free, schedule gone", func(t *testing.T) {
		rec := &recorder{}
		_, cli := startRecorder(t, "rec.leftover", rec)
		job := mustEvery(t, cli, "rec.leftover", time.Hour, fastRetry(3), "rec-leftover")
		editStored(t, cli, job.ID, func(j *types.Job) { j.Status = types.JobStatusDead })
		if err := cli.Store().DeleteUniqueKey(context.Background(), "rec-leftover"); err != nil {
			t.Fatalf("delete key: %v", err)
		}
		if err := cli.Store().DeleteSchedule(context.Background(), job.ID); err != nil {
			t.Fatalf("delete schedule: %v", err)
		}
		dispatchFiring(t, cli, job, 0)

		waitFor(t, 10*time.Second, "leftover run", func() bool { return len(rec.seen()) > 0 })
		time.Sleep(time.Second)
		if got := statusOf(t, cli, job.ID); got != types.JobStatusDead {
			t.Fatalf("dead job is now %s", got)
		}
		if got := holderOf(t, cli, "rec-leftover"); got != "" {
			t.Fatalf("key holder = %q, want none", got)
		}
	})

	t.Run("key free, firing succeeds", func(t *testing.T) {
		_, cli := startRecorder(t, "rec.leftover", &recorder{})
		job := mustEvery(t, cli, "rec.leftover", time.Hour, fastRetry(3), "rec-leftover")
		editStored(t, cli, job.ID, func(j *types.Job) { j.Status = types.JobStatusDead })
		if err := cli.Store().DeleteUniqueKey(context.Background(), "rec-leftover"); err != nil {
			t.Fatalf("delete key: %v", err)
		}
		dispatchFiring(t, cli, job, 0)

		waitStatus(t, cli, job.ID, types.JobStatusScheduled)
		if got := holderOf(t, cli, "rec-leftover"); got != job.ID {
			t.Fatalf("key holder = %q, want the revived job %s", got, job.ID)
		}
	})
}

// The monitoring API cancels and retries by the same rules as the client.
func TestUniqueKey_MonitorAPI(t *testing.T) {
	setup := func(t *testing.T) (*monitor.APIService, *client.Client, *types.Job) {
		srv, cli := startRecorder(t, "rec.monitor", &recorder{})
		svc := monitor.NewAPIService(srv.Store(), srv.Dispatcher(), nil)
		return svc, cli, mustEvery(t, cli, "rec.monitor", time.Hour, fastRetry(3), "rec-monitor")
	}
	ctx := context.Background()

	t.Run("CancelJob releases", func(t *testing.T) {
		svc, cli, job := setup(t)
		if err := svc.CancelJob(ctx, job.ID); err != nil {
			t.Fatalf("cancel: %v", err)
		}
		mustEvery(t, cli, "rec.monitor", time.Hour, fastRetry(3), "rec-monitor")
	})
	t.Run("BulkCancelJobs releases", func(t *testing.T) {
		svc, cli, job := setup(t)
		if res, err := svc.BulkCancelJobs(ctx, monitor.BulkRequest{IDs: []string{job.ID}}); err != nil || res.Affected != 1 {
			t.Fatalf("bulk cancel: %+v, %v", res, err)
		}
		mustEvery(t, cli, "rec.monitor", time.Hour, fastRetry(3), "rec-monitor")
	})

	retries := map[string]func(svc *monitor.APIService, jobID string) bool{ // reports whether the retry went through
		"RetryJob": func(svc *monitor.APIService, jobID string) bool { return svc.RetryJob(ctx, jobID) == nil },
		"BulkRetryJobs": func(svc *monitor.APIService, jobID string) bool {
			res, err := svc.BulkRetryJobs(ctx, monitor.BulkRequest{IDs: []string{jobID}})
			return err == nil && res.Affected == 1
		},
		"RequeueRun": func(svc *monitor.APIService, jobID string) bool { return svc.RequeueRun(ctx, "run-"+jobID) == nil },
	}
	for name, retry := range retries {
		for _, taken := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s, key taken %v", name, taken), func(t *testing.T) {
				svc, cli, job := setup(t)
				// The finished run that RequeueRun starts from.
				if _, err := cli.Store().SaveRun(ctx, &types.JobRun{ID: "run-" + job.ID, JobID: job.ID, Status: types.RunStatusFailed, StartedAt: time.Now()}); err != nil {
					t.Fatalf("save run: %v", err)
				}
				// Without its schedule the job ends on its next success.
				if err := cli.Store().DeleteSchedule(ctx, job.ID); err != nil {
					t.Fatalf("delete schedule: %v", err)
				}
				editStored(t, cli, job.ID, func(j *types.Job) { j.Status = types.JobStatusDead })
				if err := cli.Store().DeleteUniqueKey(ctx, "rec-monitor"); err != nil {
					t.Fatalf("delete key: %v", err)
				}
				want := job.ID
				if taken {
					want = mustEvery(t, cli, "rec.monitor", time.Hour, fastRetry(3), "rec-monitor").ID
				}

				if ok := retry(svc, job.ID); ok == taken {
					t.Fatalf("retry went through: %v", ok)
				}
				if got := holderOf(t, cli, "rec-monitor"); got != want {
					t.Fatalf("key holder = %q, want %s", got, want)
				}
				if taken {
					time.Sleep(time.Second)
					if got := statusOf(t, cli, job.ID); got != types.JobStatusDead {
						t.Fatalf("dead job is now %s", got)
					}
				} else {
					waitStatus(t, cli, job.ID, types.JobStatusCompleted)
				}
			})
		}
	}
}
