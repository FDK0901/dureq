package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/redis/rueidis"
)

// hourly is a request for an hourly job under uniqueKey.
func hourly(uniqueKey string) *EnqueueRequest {
	interval := types.Duration(time.Hour)
	return &EnqueueRequest{
		TaskType:  "test.client",
		Payload:   json.RawMessage(`{}`),
		Schedule:  types.Schedule{Type: types.ScheduleDuration, Interval: &interval},
		UniqueKey: &uniqueKey,
	}
}

// liveJobs returns the IDs of all jobs that have not ended.
func liveJobs(t *testing.T, cli *Client) []string {
	t.Helper()
	jobs, _, err := cli.Store().ListJobsPaginated(context.Background(), store.JobFilter{Limit: 1000})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	var ids []string
	for _, job := range jobs {
		if !job.Status.IsTerminal() {
			ids = append(ids, job.ID)
		}
	}
	return ids
}

func hasSchedule(t *testing.T, cli *Client, jobID string) bool {
	t.Helper()
	_, _, err := cli.Store().GetSchedule(context.Background(), jobID)
	if err != nil && !errors.Is(err, types.ErrScheduleNotFound) {
		t.Fatalf("get schedule: %v", err)
	}
	return err == nil
}

// The new job takes the key over, whatever became of the one that held it. Only a
// live one is cancelled.
func TestEnqueueOrReplace_TakesKeyOver(t *testing.T) {
	for _, tc := range []struct {
		name string
		old  types.JobStatus // "" = none
		want types.JobStatus // of the old job
	}{
		{"nobody holds the key", "", ""},
		{"scheduled", types.JobStatusScheduled, types.JobStatusCancelled},
		{"running", types.JobStatusRunning, types.JobStatusCancelled},
		{"retrying", types.JobStatusRetrying, types.JobStatusCancelled},
		{"paused", types.JobStatusPaused, types.JobStatusCancelled},
		{"completed", types.JobStatusCompleted, types.JobStatusCompleted},
		{"dead, key not released", types.JobStatusDead, types.JobStatusDead},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli := newTestClient(t)
			var old *types.Job
			if tc.old != "" {
				old = enqueueAt(t, cli, "k", tc.old)
			}

			job, err := cli.EnqueueOrReplace(context.Background(), hourly("k"))
			if err != nil {
				t.Fatalf("enqueue or replace: %v", err)
			}
			if holder := keyHolder(t, cli, "k"); holder != job.ID {
				t.Fatalf("key holder = %q, want the new job %s", holder, job.ID)
			}
			if !hasSchedule(t, cli, job.ID) {
				t.Fatal("the new job has no schedule")
			}
			if old == nil {
				return
			}
			if got := jobStatus(t, cli, old.ID); got != tc.want {
				t.Fatalf("old job is %s, want %s", got, tc.want)
			}
			if tc.want == types.JobStatusCancelled && hasSchedule(t, cli, old.ID) {
				t.Fatal("the old job is still scheduled")
			}
		})
	}
}

// A key whose job does not exist any more does not block anybody.
func TestUniqueKey_OfMissingJob(t *testing.T) {
	ctx := context.Background()
	t.Run("EnqueueOrReplace", func(t *testing.T) {
		cli := newTestClient(t)
		if err := cli.Store().SetUniqueKey(ctx, "k", "gone"); err != nil {
			t.Fatal(err)
		}
		job, err := cli.EnqueueOrReplace(ctx, hourly("k"))
		if err != nil {
			t.Fatalf("enqueue or replace: %v", err)
		}
		if holder := keyHolder(t, cli, "k"); holder != job.ID {
			t.Fatalf("key holder = %q, want %s", holder, job.ID)
		}
	})
	t.Run("CancelByUniqueKey", func(t *testing.T) {
		cli := newTestClient(t)
		if err := cli.Store().SetUniqueKey(ctx, "k", "gone"); err != nil {
			t.Fatal(err)
		}
		if err := cli.CancelByUniqueKey(ctx, "k"); err != nil {
			t.Fatalf("cancel: %v", err)
		}
		if holder := keyHolder(t, cli, "k"); holder != "" {
			t.Fatalf("key holder = %q, want none", holder)
		}
	})
}

// Nothing is cancelled for a request that cannot be carried out anyway.
func TestEnqueueOrReplace_BadRequest_LeavesOldJob(t *testing.T) {
	cli := newTestClient(t)
	ctx := context.Background()
	old := enqueueAt(t, cli, "k", types.JobStatusScheduled)

	bad := hourly("k")
	cron := "not a cron expression"
	bad.Schedule = types.Schedule{Type: types.ScheduleCron, CronExpr: &cron}
	noKey := hourly("")
	nilKey := hourly("")
	nilKey.UniqueKey = nil
	for name, req := range map[string]*EnqueueRequest{"schedule": bad, "empty key": noKey, "no key": nilKey} {
		if _, err := cli.EnqueueOrReplace(ctx, req); err == nil {
			t.Fatalf("%s: no error", name)
		}
	}
	if got := jobStatus(t, cli, old.ID); got != types.JobStatusScheduled {
		t.Fatalf("old job is %s, want scheduled", got)
	}
	if holder := keyHolder(t, cli, "k"); holder != old.ID {
		t.Fatalf("key holder = %q, want the old job %s", holder, old.ID)
	}
}

// A repeated request returns its own job instead of replacing it.
func TestEnqueueOrReplace_SameRequestID_ReturnsJob(t *testing.T) {
	cli := newTestClient(t)
	ctx := context.Background()
	req := hourly("k")
	requestID := "r"
	req.RequestID = &requestID

	first, err := cli.EnqueueOrReplace(ctx, req)
	if err != nil {
		t.Fatalf("enqueue or replace: %v", err)
	}
	again, err := cli.EnqueueOrReplace(ctx, req)
	if err != nil || again.ID != first.ID {
		t.Fatalf("again = %v, %v; want job %s", again, err, first.ID)
	}
	if got := jobStatus(t, cli, first.ID); got != types.JobStatusScheduled {
		t.Fatalf("job is %s, want scheduled", got)
	}
}

// However replacing, enqueueing and cancelling interleave, at most one job is live
// under a key, and it is the one that holds the key.
func TestUniqueKey_Concurrent_OneLiveJob(t *testing.T) {
	for _, tc := range []struct {
		name  string
		calls func(ctx context.Context, cli *Client, i int) error
	}{
		{"replace", func(ctx context.Context, cli *Client, i int) error {
			_, err := cli.EnqueueOrReplace(ctx, hourly("k"))
			return err
		}},
		{"replace and enqueue", func(ctx context.Context, cli *Client, i int) error {
			if i%2 == 0 {
				_, err := cli.EnqueueOrReplace(ctx, hourly("k"))
				return err
			}
			_, err := cli.EnqueueScheduled(ctx, hourly("k"))
			return err
		}},
		{"replace, enqueue and cancel", func(ctx context.Context, cli *Client, i int) error {
			switch i % 3 {
			case 0:
				_, err := cli.EnqueueOrReplace(ctx, hourly("k"))
				return err
			case 1:
				_, err := cli.EnqueueScheduled(ctx, hourly("k"))
				return err
			}
			return cli.CancelByUniqueKey(ctx, "k")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for round := range 30 {
				cli := newTestClient(t)
				ctx := context.Background()
				var wg sync.WaitGroup
				errs := make(chan error, 24)
				for i := range 12 {
					wg.Go(func() {
						for range 2 {
							if err := tc.calls(ctx, cli, i); err != nil && !isDuplicate(err) && !errors.Is(err, store.ErrCASConflict) {
								errs <- err
							}
						}
					})
				}
				wg.Wait()
				close(errs)
				for err := range errs {
					t.Fatalf("round %d: %v", round, err)
				}

				live, holder := liveJobs(t, cli), keyHolder(t, cli, "k")
				if len(live) > 1 || (len(live) == 1 && live[0] != holder) {
					t.Fatalf("round %d: live jobs %v, key holder %q", round, live, holder)
				}
			}
		})
	}
}

// The same with the calls held up at the worst moment: the key has been taken, the
// job does not exist yet.
func TestUniqueKey_LostBeforeJobExists(t *testing.T) {
	cli := newTestClient(t)
	ctx := context.Background()
	req := hourly("k")

	slow := newJob(req, time.Now())
	if err := cli.store.SetUniqueKey(ctx, "k", slow.ID); err != nil {
		t.Fatal(err)
	}
	current, err := cli.EnqueueOrReplace(ctx, hourly("k"))
	if err != nil {
		t.Fatalf("enqueue or replace: %v", err)
	}
	nextRun, err := firstRun(slow)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cli.start(ctx, req, slow, nextRun); !isDuplicate(err) {
		t.Fatalf("start err = %v, want duplicate", err)
	}

	if live := liveJobs(t, cli); len(live) != 1 || live[0] != current.ID {
		t.Fatalf("live jobs %v, want only %s", live, current.ID)
	}
	if hasSchedule(t, cli, slow.ID) {
		t.Fatal("the job that lost its key is scheduled")
	}
	if holder := keyHolder(t, cli, "k"); holder != current.ID {
		t.Fatalf("key holder = %q, want %s", holder, current.ID)
	}
}

// Cancel gets through although the job is being written to.
func TestCancel_BusyJob(t *testing.T) {
	cli := newTestClient(t)
	ctx := context.Background()
	for i := range 30 {
		job := enqueueAt(t, cli, fmt.Sprint("k", i), types.JobStatusScheduled)
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					if j, rev, err := cli.Store().GetJob(ctx, job.ID); err == nil && j.Status != types.JobStatusCancelled {
						cli.Store().UpdateJob(ctx, j, rev)
					}
				}
			}
		})
		time.Sleep(2 * time.Millisecond)
		err := cli.Cancel(ctx, job.ID)
		close(stop)
		wg.Wait()
		if err != nil {
			t.Fatalf("cancel: %v", err)
		}
		if got := jobStatus(t, cli, job.ID); got != types.JobStatusCancelled {
			t.Fatalf("job is %s", got)
		}
	}
}

// hookedRedis calls before with every single command that is about to be sent.
type hookedRedis struct {
	rueidis.Client
	before func(cmd []string)
}

func (h *hookedRedis) Do(ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	if h.before != nil {
		h.before(cmd.Commands())
	}
	return h.Client.Do(ctx, cmd)
}

func (h *hookedRedis) DoMulti(ctx context.Context, multi ...rueidis.Completed) []rueidis.RedisResult {
	if h.before != nil {
		for _, cmd := range multi {
			h.before(cmd.Commands())
		}
	}
	return h.Client.DoMulti(ctx, multi...)
}

// newHookedClient returns a client like cli whose commands can be intercepted.
func newHookedClient(t *testing.T, cli *Client) (*Client, *hookedRedis) {
	t.Helper()
	hook := &hookedRedis{Client: cli.rdb}
	hooked, err := New(WithRedisClient(hook), WithKeyPrefix(cli.Store().Config().KeyPrefix))
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	return hooked, hook
}

// The job that holds the key comes into being just before the key is taken from it,
// after it was looked for in vain.
func TestEnqueueOrReplace_HolderAppearsLate(t *testing.T) {
	cli := newTestClient(t)
	ctx := context.Background()
	replacer, hook := newHookedClient(t, cli)

	req := hourly("k")
	late := newJob(req, time.Now())
	if err := cli.store.SetUniqueKey(ctx, "k", late.ID); err != nil {
		t.Fatal(err)
	}
	var startErr error
	started := false
	hook.before = func(cmd []string) {
		if started || !strings.HasPrefix(cmd[0], "EVAL") || !slices.Contains(cmd, late.ID) {
			return
		}
		started = true
		nextRun, _ := firstRun(late)
		_, startErr = cli.start(ctx, req, late, nextRun)
	}

	current, err := replacer.EnqueueOrReplace(ctx, hourly("k"))
	if err != nil {
		t.Fatalf("enqueue or replace: %v", err)
	}
	if !started || startErr != nil {
		t.Fatalf("the late job was not started (%v): %v", started, startErr)
	}
	if live := liveJobs(t, cli); len(live) != 1 || live[0] != current.ID {
		t.Fatalf("live jobs %v, want only %s", live, current.ID)
	}
	if hasSchedule(t, cli, late.ID) {
		t.Fatal("the replaced job is still scheduled")
	}
}

// The key is free by the time the cancel is announced, so that a listener can enqueue
// under it again.
func TestCancel_ReleasesUniqueKeyBeforeEvent(t *testing.T) {
	cli := newTestClient(t)
	canceller, hook := newHookedClient(t, cli)
	job := enqueueAt(t, cli, "k", types.JobStatusScheduled)

	announced := 0
	hook.before = func(cmd []string) {
		if cmd[0] != "PUBLISH" || !strings.Contains(cmd[2], string(types.EventJobCancelled)) {
			return
		}
		announced++
		if holder := keyHolder(t, cli, "k"); holder != "" {
			t.Errorf("key still held by %s when the cancel is announced", holder)
		}
	}
	if err := canceller.Cancel(context.Background(), job.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if announced != 1 {
		t.Fatalf("cancel was announced %d times, want 1", announced)
	}
}
