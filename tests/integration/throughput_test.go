// Package integration — a worker does as much as it has room for.
package integration

import (
	"context"
	"encoding/json"
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
