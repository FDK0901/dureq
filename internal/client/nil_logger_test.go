package client

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/FDK0901/dureq/pkg/types"
	"github.com/redis/rueidis"
	"github.com/rs/xid"
)

// newTestClient returns a client-only Client (no server, so its store has no
// logger) on the standalone Redis at REDIS_URL under a unique key prefix.
// Skips if REDIS_URL is unset or Redis is unreachable.
func newTestClient(t *testing.T) *Client {
	t.Helper()

	addr := strings.TrimPrefix(os.Getenv("REDIS_URL"), "redis://")
	if addr == "" {
		t.Skip("REDIS_URL not set")
	}
	password := os.Getenv("REDIS_PASSWORD")
	if password == "" {
		password = "your-password"
	}
	rdb, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, Password: password})
	if err != nil {
		t.Skipf("redis unavailable: %v", err)
	}
	t.Cleanup(rdb.Close)

	cli, err := New(WithRedisClient(rdb), WithKeyPrefix("test_"+xid.New().String()))
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	return cli
}

// enqueueAt registers a one-time job an hour from now and moves it to status.
func enqueueAt(t *testing.T, cli *Client, uniqueKey string, status types.JobStatus) *types.Job {
	t.Helper()
	ctx := context.Background()

	runAt := time.Now().Add(time.Hour)
	job, err := cli.EnqueueScheduled(ctx, &EnqueueRequest{
		TaskType:  "test.client",
		Payload:   json.RawMessage(`{}`),
		Schedule:  types.Schedule{Type: types.ScheduleOneTime, RunAt: &runAt},
		UniqueKey: &uniqueKey,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	job.Status = status
	if _, err := cli.Store().SaveJob(ctx, job); err != nil {
		t.Fatalf("save job as %s: %v", status, err)
	}
	return job
}

func noPanic(t *testing.T, what string, fn func() error) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("%s panicked: %v", what, r)
		}
	}()
	if err := fn(); err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

// A transition that UpdateJob only warns about must not crash a process whose
// store was created without a logger.
func TestClientOnlyStore_InvalidTransition_DoesNotPanic(t *testing.T) {
	cli := newTestClient(t)
	ctx := context.Background()
	job := enqueueAt(t, cli, "k", types.JobStatusScheduled)

	noPanic(t, "UpdateJob scheduled → dead", func() error {
		_, rev, err := cli.Store().GetJob(ctx, job.ID)
		if err != nil {
			return err
		}
		job.Status = types.JobStatusDead
		_, err = cli.Store().UpdateJob(ctx, job, rev)
		return err
	})
}

// The client calls that make such a transition on their own.
func TestClientOnly_OperationsOnEndedJobs_DoNotPanic(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status types.JobStatus
		call   func(ctx context.Context, cli *Client, job *types.Job) error
		want   types.JobStatus
	}{
		{"Cancel completed", types.JobStatusCompleted, func(ctx context.Context, cli *Client, job *types.Job) error {
			return cli.Cancel(ctx, job.ID)
		}, types.JobStatusCancelled},
		{"Cancel dead", types.JobStatusDead, func(ctx context.Context, cli *Client, job *types.Job) error {
			return cli.Cancel(ctx, job.ID)
		}, types.JobStatusCancelled},
		{"CancelByUniqueKey completed", types.JobStatusCompleted, func(ctx context.Context, cli *Client, job *types.Job) error {
			return cli.CancelByUniqueKey(ctx, *job.UniqueKey)
		}, types.JobStatusCancelled},
		{"Retry cancelled", types.JobStatusCancelled, func(ctx context.Context, cli *Client, job *types.Job) error {
			return cli.Retry(ctx, job.ID)
		}, types.JobStatusPending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli := newTestClient(t)
			ctx := context.Background()
			job := enqueueAt(t, cli, "k", tc.status)

			noPanic(t, tc.name, func() error { return tc.call(ctx, cli, job) })

			got, err := cli.GetJob(ctx, job.ID)
			if err != nil {
				t.Fatalf("get job: %v", err)
			}
			if got.Status != tc.want {
				t.Fatalf("status = %s, want %s", got.Status, tc.want)
			}
		})
	}
}
