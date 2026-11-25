// Package integration — a worker does as much as it has room for.
package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/client"
	"github.com/FDK0901/dureq/internal/server"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/rs/xid"
)

// startTiered starts a server with tiers, the default ones if nil, and returns a client to it.
func startTiered(t *testing.T, concurrency int, tiers []store.TierConfig, handlers ...types.HandlerDefinition) (string, *client.Client) {
	t.Helper()
	flushDB(t)
	nodeID := "tiered-" + xid.New().String()
	opts := []server.Option{
		server.WithNodeID(nodeID), server.WithMaxConcurrency(concurrency), server.WithKeyPrefix(keyPrefix),
		server.WithSchedulerTickInterval(500 * time.Millisecond), server.WithShutdownTimeout(3 * time.Second),
	}
	copts := []client.Option{client.WithKeyPrefix(keyPrefix)}
	if isStandalone() {
		opts = append(opts, server.WithRedisURL(os.Getenv("REDIS_URL")), server.WithRedisPassword(redisPW), server.WithRedisDB(redisDB()))
		copts = append(copts, client.WithRedisURL(os.Getenv("REDIS_URL")), client.WithRedisPassword(redisPW), client.WithRedisDB(redisDB()))
	} else {
		opts = append(opts, server.WithRedisCluster(redisAddrs()), server.WithRedisPassword(redisPW))
		copts = append(copts, client.WithClusterAddrs(redisAddrs()), client.WithRedisPassword(redisPW))
	}
	if tiers != nil {
		opts = append(opts, server.WithPriorityTiers(tiers))
		copts = append(copts, client.WithPriorityTiers(tiers))
	}
	srv, err := server.New(opts...)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	for _, h := range handlers {
		srv.RegisterHandler(h)
	}
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { srv.Stop() })
	cli, err := client.New(copts...)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(cli.Close)
	return nodeID, cli
}

func counter(taskType types.TaskType, n *atomic.Int64) types.HandlerDefinition {
	return types.HandlerDefinition{TaskType: taskType, Handler: func(context.Context, json.RawMessage) error {
		n.Add(1)
		return nil
	}}
}

// enqueueMany enqueues jobs of a priority for immediate execution.
func enqueueMany(t *testing.T, cli *client.Client, taskType types.TaskType, priority types.Priority, n int) {
	t.Helper()
	var wg sync.WaitGroup
	for g := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := g; i < n; i += 10 {
				req := &client.EnqueueRequest{TaskType: taskType, Payload: json.RawMessage(`{}`), Priority: &priority, Schedule: types.Schedule{Type: types.ScheduleImmediate}}
				if _, err := cli.EnqueueScheduled(context.Background(), req); err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func waitCount(t *testing.T, timeout time.Duration, n *atomic.Int64, want int64) {
	t.Helper()
	waitFor(t, timeout, fmt.Sprintf("%d jobs", want), func() bool { return n.Load() >= want })
}

// A worker takes new jobs as soon as it has room: far more of them in a second than run at
// once.
func TestWorker_TakesJobsAsSoonAsThereIsRoom(t *testing.T) {
	var n atomic.Int64
	_, cli := startTiered(t, 3, nil, counter("quick", &n))
	enqueueMany(t, cli, "quick", types.PriorityNormal, 300)
	waitCount(t, 8*time.Second, &n, 300)
}

// Tiers without jobs do not hold up the others.
func TestWorker_EmptyTiersDoNotHoldUpOthers(t *testing.T) {
	var n atomic.Int64
	tiers := []store.TierConfig{{Name: "high", Weight: 100}, {Name: "normal", Weight: 50}, {Name: "low", Weight: 10}}
	_, cli := startTiered(t, 10, tiers, counter("quick", &n))
	enqueueMany(t, cli, "quick", 1, 500)
	waitCount(t, 8*time.Second, &n, 500)
}

// A rate limit below the batch size lets jobs through, at the rate, and other tiers are not
// affected. Looking for jobs that are not there does not use it up.
func TestWorker_RateLimit(t *testing.T) {
	var limited, other atomic.Int64
	tiers := []store.TierConfig{{Name: "limited", Weight: 100, FetchBatch: 10, RateLimit: 5}, {Name: "other", Weight: 10}}
	_, cli := startTiered(t, 20, tiers, counter("limited", &limited), counter("other", &other))
	enqueueMany(t, cli, "other", 1, 300)
	waitCount(t, 8*time.Second, &other, 300)

	start := time.Now()
	enqueueMany(t, cli, "limited", 10, 15)
	waitCount(t, 8*time.Second, &limited, 15)
	// Five at once, and the rest at five a second.
	if took := time.Since(start); took < 1500*time.Millisecond {
		t.Errorf("15 jobs at 5 a second took only %v", took)
	}
}

// Many retries that are due at once do not wait for each other.
func TestRetries_ManyAtOnce(t *testing.T) {
	var n atomic.Int64
	_, cli := startTiered(t, 50, nil, types.HandlerDefinition{
		TaskType:    "again",
		RetryPolicy: &types.RetryPolicy{MaxAttempts: 3, InitialDelay: 100 * time.Millisecond, MaxDelay: 100 * time.Millisecond, Multiplier: 1},
		Handler: func(ctx context.Context, _ json.RawMessage) error {
			if types.GetAttempt(ctx) == 0 {
				return errors.New("again")
			}
			n.Add(1)
			return nil
		},
	})
	enqueueMany(t, cli, "again", types.PriorityNormal, 1000)
	waitCount(t, 7*time.Second, &n, 1000)
}

// A busy worker notices that a queue is paused, and that its node is draining.
func TestWorker_PauseAndDrainWhileBusy(t *testing.T) {
	var n atomic.Int64
	nodeID, cli := startTiered(t, 2, nil, types.HandlerDefinition{TaskType: "busy", Handler: func(context.Context, json.RawMessage) error {
		time.Sleep(5 * time.Millisecond)
		n.Add(1)
		return nil
	}})
	ctx := context.Background()
	st := cli.Store()
	stops := func(what string) {
		t.Helper()
		time.Sleep(300 * time.Millisecond)
		before := n.Load()
		time.Sleep(700 * time.Millisecond)
		if after := n.Load(); after != before {
			t.Errorf("%d jobs were processed after %s", after-before, what)
		}
	}
	goesOn := func(what string) {
		t.Helper()
		before := n.Load()
		waitFor(t, 5*time.Second, "jobs after "+what, func() bool { return n.Load() > before })
	}
	enqueueMany(t, cli, "busy", types.PriorityNormal, 3000)
	goesOn("start")
	if err := st.PauseQueue(ctx, "normal"); err != nil {
		t.Fatal(err)
	}
	stops("the queue was paused")
	if err := st.UnpauseQueue(ctx, "normal"); err != nil {
		t.Fatal(err)
	}
	goesOn("the queue was resumed")
	if err := st.SetNodeDrain(ctx, nodeID, true); err != nil {
		t.Fatal(err)
	}
	stops("the node was drained")
	if err := st.SetNodeDrain(ctx, nodeID, false); err != nil {
		t.Fatal(err)
	}
	goesOn("the node was undrained")
}

// A worker is notified of jobs again after it has lost the connection for that. The Redis
// client sees to it.
func TestWorker_IsNotifiedAfterReconnect(t *testing.T) {
	started := make(chan time.Time, 1)
	_, cli := startTiered(t, 10, nil, types.HandlerDefinition{TaskType: "quick", Handler: func(context.Context, json.RawMessage) error {
		started <- time.Now()
		return nil
	}})
	ctx := context.Background()
	time.Sleep(500 * time.Millisecond)
	for _, node := range cli.Store().Client().Nodes() {
		if err := node.Do(ctx, node.B().ClientKill().TypePubsub().Build()).Error(); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(time.Second)

	slow := 0
	for range 10 {
		at := time.Now()
		if _, err := cli.Enqueue(ctx, "quick", nil); err != nil {
			t.Fatal(err)
		}
		if took := (<-started).Sub(at); took > 100*time.Millisecond {
			slow++
		}
		time.Sleep(130 * time.Millisecond)
	}
	if slow > 2 {
		t.Errorf("%d of 10 jobs had to wait until the worker looked for them", slow)
	}
}
