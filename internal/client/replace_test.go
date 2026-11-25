package client

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/FDK0901/dureq/pkg/types"
	"github.com/redis/rueidis"
)

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
