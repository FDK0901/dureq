package client

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
)

func keyHolder(t *testing.T, cli *Client, key string) string {
	t.Helper()
	holder, _, err := cli.Store().CheckUniqueKey(context.Background(), key)
	if err != nil {
		t.Fatalf("check unique key: %v", err)
	}
	return holder
}

// isDuplicate reports whether err is the duplicate unique key error.
func isDuplicate(err error) bool { return errors.Is(err, types.ErrDuplicateJob) }

func jobStatus(t *testing.T, cli *Client, jobID string) types.JobStatus {
	t.Helper()
	job, err := cli.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	return job.Status
}

func workCount(t *testing.T, cli *Client) int64 {
	t.Helper()
	rdb := cli.Store().Client()
	n, err := rdb.Do(context.Background(), rdb.B().Xlen().Key(store.WorkStreamKey(cli.Store().Prefix(), "normal")).Build()).AsInt64()
	if err != nil {
		t.Fatalf("XLEN: %v", err)
	}
	return n
}

// Cancel(id) frees the key, so "cancel by id, register again under the same
// key" works.
func TestCancel_ReleasesUniqueKey(t *testing.T) {
	cli := newTestClient(t)
	ctx := context.Background()
	job := enqueueAt(t, cli, "k", types.JobStatusScheduled)

	if err := cli.Cancel(ctx, job.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if holder := keyHolder(t, cli, "k"); holder != "" {
		t.Fatalf("cancelled job still holds the key: %s", holder)
	}
	enqueueAt(t, cli, "k", types.JobStatusScheduled) // must not be a duplicate
}

// Ending or deleting a job never frees a key that another job holds by now.
func TestUniqueKey_HeldByAnotherJob_IsNotReleased(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(ctx context.Context, cli *Client, old *types.Job) error
	}{
		{"Cancel", func(ctx context.Context, cli *Client, old *types.Job) error { return cli.Cancel(ctx, old.ID) }},
		{"DeleteJob", func(ctx context.Context, cli *Client, old *types.Job) error {
			return cli.Store().DeleteJob(ctx, old.ID)
		}},
		{"unique key changed on the job", func(ctx context.Context, cli *Client, old *types.Job) error {
			other := "k2"
			old.UniqueKey = &other
			_, err := cli.Store().SaveJob(ctx, old)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli := newTestClient(t)
			ctx := context.Background()
			old := enqueueAt(t, cli, "k", types.JobStatusDead)
			// What an operator does to a key left behind by a dead job.
			if err := cli.Store().DeleteUniqueKey(ctx, "k"); err != nil {
				t.Fatalf("delete key: %v", err)
			}
			current := enqueueAt(t, cli, "k", types.JobStatusScheduled)

			if err := tc.call(ctx, cli, old); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if holder := keyHolder(t, cli, "k"); holder != current.ID {
				t.Fatalf("key holder = %q, want the current job %s", holder, current.ID)
			}
		})
	}
}

// A completed job keeps its key: releasing it on completion would change what
// UniqueKey means to callers.
func TestUniqueKey_CompletedJob_KeepsKey(t *testing.T) {
	cli := newTestClient(t)
	job := enqueueAt(t, cli, "k", types.JobStatusCompleted)

	runAt := time.Now().Add(time.Hour)
	key := "k"
	_, err := cli.EnqueueScheduled(context.Background(), &EnqueueRequest{
		TaskType:  "test.client",
		Payload:   json.RawMessage(`{}`),
		Schedule:  types.Schedule{Type: types.ScheduleOneTime, RunAt: &runAt},
		UniqueKey: &key,
	})
	if !isDuplicate(err) {
		t.Fatalf("enqueue err = %v, want duplicate", err)
	}
	if holder := keyHolder(t, cli, "k"); holder != job.ID {
		t.Fatalf("key holder = %q, want %s", holder, job.ID)
	}
}

// Retrying a dead or cancelled job while another job holds its key must leave
// the job as it is and dispatch nothing: two jobs would live under one key.
func TestRetry_UniqueKeyHeldByAnotherJob_Refused(t *testing.T) {
	for _, status := range []types.JobStatus{types.JobStatusDead, types.JobStatusCancelled} {
		t.Run(string(status), func(t *testing.T) {
			cli := newTestClient(t)
			ctx := context.Background()
			old := enqueueAt(t, cli, "k", status)
			if err := cli.Store().DeleteUniqueKey(ctx, "k"); err != nil {
				t.Fatalf("delete key: %v", err)
			}
			current := enqueueAt(t, cli, "k", types.JobStatusScheduled)

			if err := cli.Retry(ctx, old.ID); !isDuplicate(err) {
				t.Fatalf("retry err = %v, want duplicate", err)
			}
			if got := jobStatus(t, cli, old.ID); got != status {
				t.Fatalf("old job status = %s, want %s", got, status)
			}
			if n := workCount(t, cli); n != 0 {
				t.Fatalf("%d messages dispatched, want 0", n)
			}
			if holder := keyHolder(t, cli, "k"); holder != current.ID {
				t.Fatalf("key holder = %q, want the current job %s", holder, current.ID)
			}
		})
	}
}

// A retried job takes its free key back, so nothing can be registered next to it.
func TestRetry_FreeUniqueKey_IsClaimedAgain(t *testing.T) {
	cli := newTestClient(t)
	ctx := context.Background()
	job := enqueueAt(t, cli, "k", types.JobStatusScheduled)
	if err := cli.Cancel(ctx, job.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if err := cli.Store().DeleteUniqueKey(ctx, "k"); err != nil {
		t.Fatalf("delete key: %v", err)
	}

	if err := cli.Retry(ctx, job.ID); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if holder := keyHolder(t, cli, "k"); holder != job.ID {
		t.Fatalf("key holder = %q, want the retried job %s", holder, job.ID)
	}
	if n := workCount(t, cli); n != 1 {
		t.Fatalf("%d messages dispatched, want 1", n)
	}
}
