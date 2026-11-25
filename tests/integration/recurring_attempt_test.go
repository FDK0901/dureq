// Package integration — the retry budget of recurring jobs.
package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/client"
	"github.com/FDK0901/dureq/internal/server"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/rs/xid"
)

// fastRetry keeps retries within a test's patience.
func fastRetry(maxAttempts int) *types.RetryPolicy {
	return &types.RetryPolicy{MaxAttempts: maxAttempts, InitialDelay: 100 * time.Millisecond, MaxDelay: 100 * time.Millisecond, Multiplier: 1}
}

// recorder is a handler that records the attempt of every execution and fails
// those that fail says it should.
type recorder struct {
	mu       sync.Mutex
	attempts []int
	fail     func(run int) error // run is 0-based
}

func (r *recorder) handle(ctx context.Context, _ json.RawMessage) error {
	r.mu.Lock()
	run := len(r.attempts)
	r.attempts = append(r.attempts, types.GetAttempt(ctx))
	r.mu.Unlock()
	if r.fail == nil {
		return nil
	}
	return r.fail(run)
}

func (r *recorder) seen() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.attempts...)
}

// startRecorder starts a server whose handler for taskType is rec.
func startRecorder(t *testing.T, taskType types.TaskType, rec *recorder) (*server.Server, *client.Client) {
	t.Helper()
	flushDB(t)
	srv := newServer(t, "recurring-"+xid.New().String(), 10)
	srv.RegisterHandler(types.HandlerDefinition{TaskType: taskType, Handler: rec.handle})
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { srv.Stop() })
	cli := newClient(t)
	t.Cleanup(cli.Close)
	return srv, cli
}

// every registers a recurring job under uniqueKey.
func every(cli *client.Client, taskType types.TaskType, interval time.Duration, policy *types.RetryPolicy, uniqueKey string) (*types.Job, error) {
	iv := types.Duration(interval)
	return cli.EnqueueScheduled(context.Background(), &client.EnqueueRequest{
		TaskType:    taskType,
		Payload:     json.RawMessage(`{}`),
		Schedule:    types.Schedule{Type: types.ScheduleDuration, Interval: &iv},
		RetryPolicy: policy,
		UniqueKey:   &uniqueKey,
	})
}

func mustEvery(t *testing.T, cli *client.Client, taskType types.TaskType, interval time.Duration, policy *types.RetryPolicy, uniqueKey string) *types.Job {
	t.Helper()
	job, err := every(cli, taskType, interval, policy, uniqueKey)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return job
}

func editStored(t *testing.T, cli *client.Client, jobID string, edit func(job *types.Job)) {
	t.Helper()
	ctx := context.Background()
	job, _, err := cli.Store().GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	edit(job)
	if _, err := cli.Store().SaveJob(ctx, job); err != nil {
		t.Fatalf("save job: %v", err)
	}
}

// dispatchFiring puts a firing of the job on the work stream, as a scheduler
// that gives it attempt would.
func dispatchFiring(t *testing.T, cli *client.Client, job *types.Job, attempt int) {
	t.Helper()
	firingID := fmt.Sprintf("%s:%d", job.ID, time.Now().UnixNano())
	_, err := cli.Store().DispatchWork(context.Background(), "normal", &types.WorkMessage{
		RunID:        xid.New().String(),
		JobID:        job.ID,
		TaskType:     job.TaskType,
		Payload:      job.Payload,
		Attempt:      attempt,
		Headers:      map[string]string{"x-dureq-firing-id": firingID},
		FiringID:     firingID,
		OperationKey: firingID,
		DispatchedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
}

func statusOf(t *testing.T, cli *client.Client, jobID string) types.JobStatus {
	t.Helper()
	job, err := cli.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	return job.Status
}

func waitStatus(t *testing.T, cli *client.Client, jobID string, want types.JobStatus) {
	t.Helper()
	waitFor(t, 20*time.Second, fmt.Sprintf("job %s", want), func() bool { return statusOf(t, cli, jobID) == want })
}

var errBoom = errors.New("boom")

// Successful runs do not eat into the retry budget: after more successes than
// MaxAttempts, a single failure is still retried.
func TestRecurring_SuccessesDoNotUseUpRetries(t *testing.T) {
	const failRun = 4
	rec := &recorder{fail: func(run int) error {
		if run == failRun {
			return errBoom
		}
		return nil
	}}
	_, cli := startRecorder(t, "rec.success", rec)
	job := mustEvery(t, cli, "rec.success", time.Second, fastRetry(3), "rec-success")

	waitFor(t, 40*time.Second, "runs after the failure", func() bool {
		return len(rec.seen()) >= failRun+4 || statusOf(t, cli, job.ID) == types.JobStatusDead
	})
	if got := statusOf(t, cli, job.ID); got == types.JobStatusDead {
		t.Fatalf("one failure after %d successes killed the job (attempts seen %v)", failRun, rec.seen())
	}
	// Only the retry of the failed run is a second attempt. Firings and the
	// retry may interleave, so look at the counts, not the order.
	retries := 0
	for i, a := range rec.seen() {
		switch a {
		case 0:
		case 1:
			retries++
		default:
			t.Fatalf("run %d executed as attempt %d (attempts seen %v)", i, a, rec.seen())
		}
	}
	if retries != 1 {
		t.Fatalf("%d retries, want 1 (attempts seen %v)", retries, rec.seen())
	}
	waitFor(t, 5*time.Second, "attempt reset", func() bool {
		j, err := cli.GetJob(context.Background(), job.ID)
		return err == nil && j.Status == types.JobStatusScheduled && j.Attempt == 0
	})
}

// A job whose stored Attempt was counted up by an older version (63 in
// production) survives its first failure after the upgrade.
func TestRecurring_CarriedOverAttempt_FirstFailureIsRetried(t *testing.T) {
	rec := &recorder{fail: func(run int) error {
		if run == 0 {
			return errBoom
		}
		return nil
	}}
	_, cli := startRecorder(t, "rec.carried", rec)
	job := mustEvery(t, cli, "rec.carried", 3*time.Second, fastRetry(3), "rec-carried")
	editStored(t, cli, job.ID, func(j *types.Job) { j.Attempt = 63 })

	waitFor(t, 25*time.Second, "retry of the failed run", func() bool {
		return len(rec.seen()) >= 2 || statusOf(t, cli, job.ID) == types.JobStatusDead
	})
	if got := rec.seen(); !reflect.DeepEqual(got[:min(len(got), 2)], []int{0, 1}) {
		t.Fatalf("attempts seen %v, want [0 1] (job is %s)", got, statusOf(t, cli, job.ID))
	}
	waitStatus(t, cli, job.ID, types.JobStatusScheduled)
}

// The same for a firing that an older scheduler dispatched just before the
// upgrade and that fails on an upgraded worker: it gets its full budget, and
// still dies when it uses that up.
func TestRecurring_CarriedOverAttempt_InFlightFiring(t *testing.T) {
	for _, tc := range []struct {
		name     string
		failures int
		attempt  int
		want     types.JobStatus
		seen     int
	}{
		{"one failure", 1, 63, types.JobStatusScheduled, 2},
		{"exactly at the limit", 1, 3, types.JobStatusScheduled, 2},
		{"all attempts fail", 99, 63, types.JobStatusDead, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recorder{fail: func(run int) error {
				if run < tc.failures {
					return errBoom
				}
				return nil
			}}
			_, cli := startRecorder(t, "rec.inflight", rec)
			job := mustEvery(t, cli, "rec.inflight", time.Hour, fastRetry(3), "rec-inflight")
			editStored(t, cli, job.ID, func(j *types.Job) {
				j.Attempt = tc.attempt
				j.Status = types.JobStatusPending
			})
			dispatchFiring(t, cli, job, tc.attempt)

			waitFor(t, 20*time.Second, "outcome", func() bool {
				s := statusOf(t, cli, job.ID)
				return s == types.JobStatusScheduled || s == types.JobStatusDead
			})
			if got := statusOf(t, cli, job.ID); got != tc.want {
				t.Fatalf("status = %s, want %s (attempts seen %v)", got, tc.want, rec.seen())
			}
			if n := len(rec.seen()); n != tc.seen {
				t.Fatalf("%d executions, want %d (attempts seen %v)", n, tc.seen, rec.seen())
			}
		})
	}
}

// An execution that resumes after an auto-pause at the limit is not a carried
// over count: its next failure is still the last one, as before.
func TestRecurring_ResumedAtLimit_StillDies(t *testing.T) {
	rec := &recorder{fail: func(int) error { return errBoom }}
	_, cli := startRecorder(t, "rec.resume", rec)
	policy := fastRetry(3)
	policy.PauseAfterErrCount = 3
	job := mustEvery(t, cli, "rec.resume", time.Hour, policy, "rec-resume")
	dispatchFiring(t, cli, job, 0)

	waitStatus(t, cli, job.ID, types.JobStatusPaused)
	if err := cli.ResumeJob(context.Background(), job.ID); err != nil {
		t.Fatalf("resume: %v", err)
	}
	waitStatus(t, cli, job.ID, types.JobStatusDead)
	if got := rec.seen(); !reflect.DeepEqual(got, []int{0, 1, 2, 3}) {
		t.Fatalf("attempts seen %v, want [0 1 2 3]", got)
	}
}

// A job that an older version paused is resumed with its carried over count and
// gets a full budget, too.
func TestRecurring_CarriedOverAttempt_ResumedJob(t *testing.T) {
	rec := &recorder{fail: func(run int) error {
		if run == 0 {
			return errBoom
		}
		return nil
	}}
	_, cli := startRecorder(t, "rec.resumed", rec)
	job := mustEvery(t, cli, "rec.resumed", time.Hour, fastRetry(3), "rec-resumed")
	editStored(t, cli, job.ID, func(j *types.Job) {
		j.Attempt = 63
		j.Status = types.JobStatusPaused
	})
	if err := cli.ResumeJob(context.Background(), job.ID); err != nil {
		t.Fatalf("resume: %v", err)
	}
	waitFor(t, 20*time.Second, "outcome", func() bool {
		s := statusOf(t, cli, job.ID)
		return s == types.JobStatusScheduled || s == types.JobStatusDead
	})
	if got := statusOf(t, cli, job.ID); got != types.JobStatusScheduled {
		t.Fatalf("status = %s, want scheduled (attempts seen %v)", got, rec.seen())
	}
}

// Using up the retries of one firing still ends the job (unchanged), after
// exactly MaxAttempts executions.
func TestRecurring_RetriesUsedUp_JobDies(t *testing.T) {
	rec := &recorder{fail: func(int) error { return errBoom }}
	_, cli := startRecorder(t, "rec.dead", rec)
	job := mustEvery(t, cli, "rec.dead", 8*time.Second, fastRetry(3), "rec-dead")

	waitStatus(t, cli, job.ID, types.JobStatusDead)
	if got := rec.seen(); !reflect.DeepEqual(got, []int{0, 1, 2}) {
		t.Fatalf("attempts seen %v, want [0 1 2]", got)
	}
	waitFor(t, 15*time.Second, "schedule removed", func() bool {
		_, _, err := cli.Store().GetSchedule(context.Background(), job.ID)
		return errors.Is(err, types.ErrScheduleNotFound)
	})
}

// Each backfilled firing is an execution of its own.
func TestRecurring_BackfillSchedule_StartsAtAttemptZero(t *testing.T) {
	rec := &recorder{}
	_, cli := startRecorder(t, "rec.backfill", rec)
	job := mustEvery(t, cli, "rec.backfill", time.Hour, fastRetry(3), "rec-backfill")
	window := types.Duration(3 * time.Hour)
	editStored(t, cli, job.ID, func(j *types.Job) {
		j.Attempt = 63
		j.Schedule.CatchupWindow = &window
	})

	now := time.Now()
	n, err := cli.BackfillSchedule(context.Background(), job.ID, now.Add(-90*time.Minute), now)
	if err != nil || n == 0 {
		t.Fatalf("backfill dispatched %d, err %v", n, err)
	}
	waitFor(t, 10*time.Second, "backfilled run", func() bool { return len(rec.seen()) > 0 })
	if got := rec.seen()[0]; got != 0 {
		t.Fatalf("backfilled run executed as attempt %d, want 0", got)
	}
}

// Overlapping firings have their own budgets: one that fails does not use up
// the attempts of the next.
func TestRecurring_OverlappingFirings_HaveOwnBudgets(t *testing.T) {
	rec := &recorder{fail: func(run int) error {
		if run < 2 { // the first try of each firing
			return errBoom
		}
		return nil
	}}
	_, cli := startRecorder(t, "rec.overlap", rec)
	job := mustEvery(t, cli, "rec.overlap", time.Hour, fastRetry(2), "rec-overlap")
	dispatchFiring(t, cli, job, 0)
	waitFor(t, 10*time.Second, "first failure", func() bool { return len(rec.seen()) == 1 })
	dispatchFiring(t, cli, job, 0)

	waitStatus(t, cli, job.ID, types.JobStatusScheduled)
	waitFor(t, 10*time.Second, "both retries", func() bool { return len(rec.seen()) == 4 })
	if got := statusOf(t, cli, job.ID); got == types.JobStatusDead {
		t.Fatalf("job died (attempts seen %v)", rec.seen())
	}
}
