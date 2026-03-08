package client

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/redis/rueidis"
	"github.com/rs/xid"
)

// getTestRedis creates a Redis client for testing.
// Skips if REDIS_URL is not set or Redis is unreachable.
func getTestRedis(t *testing.T) rueidis.Client {
	t.Helper()

	addr := os.Getenv("REDIS_URL")
	if addr == "" {
		addr = "localhost:6379"
	}
	password := os.Getenv("REDIS_PASSWORD")
	selectDB := 15
	if dbStr := os.Getenv("REDIS_DB"); dbStr != "" {
		fmt.Sscanf(dbStr, "%d", &selectDB)
	}

	rdb, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{addr},
		Password:    password,
		SelectDB:    selectDB,
	})
	if err != nil {
		t.Skipf("redis unavailable: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Do(ctx, rdb.B().Ping().Build()).Error(); err != nil {
		rdb.Close()
		t.Skipf("redis ping failed: %v", err)
	}

	t.Cleanup(func() { rdb.Close() })
	return rdb
}

// TestRaceCondition_OldPattern demonstrates the race condition in the old
// enqueue-first-then-subscribe pattern. When a result is published between
// the GetResult poll and the Subscribe call, the client misses it and times out.
func TestRaceCondition_OldPattern(t *testing.T) {
	rdb := getTestRedis(t)
	prefix := fmt.Sprintf("test_%s", xid.New().String())

	const iterations = 50
	var timeouts atomic.Int32

	for i := 0; i < iterations; i++ {
		jobID := xid.New().String()
		channel := store.ResultNotifyChannel(prefix, jobID)
		result := types.WorkResult{JobID: jobID, Success: true}
		resultData, _ := json.Marshal(result)

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)

		// OLD PATTERN (buggy): poll first, then subscribe.
		// Simulate: GetResult returns nil (job not done yet).
		// Then result arrives BEFORE subscribe starts → missed!

		// Publish result immediately (simulates an ultra-fast worker).
		rdb.Do(ctx, rdb.B().Publish().Channel(channel).Message(string(resultData)).Build())

		// Now subscribe (too late — the message was already published).
		resultCh := make(chan *types.WorkResult, 1)
		go func() {
			_ = rdb.Receive(ctx, rdb.B().Subscribe().Channel(channel).Build(),
				func(msg rueidis.PubSubMessage) {
					var r types.WorkResult
					if json.Unmarshal([]byte(msg.Message), &r) == nil && r.JobID == jobID {
						select {
						case resultCh <- &r:
						default:
						}
					}
				})
		}()

		select {
		case <-ctx.Done():
			timeouts.Add(1)
		case <-resultCh:
			// Got it (unlikely in old pattern)
		}
		cancel()
	}

	t.Logf("Old pattern: %d/%d iterations timed out (expected: most or all)", timeouts.Load(), iterations)
	if timeouts.Load() == 0 {
		t.Error("expected at least some timeouts with old pattern, got none")
	}
}

// TestRaceCondition_NewPattern verifies that the subscribe-before-publish
// pattern never misses a result.
func TestRaceCondition_NewPattern(t *testing.T) {
	rdb := getTestRedis(t)
	prefix := fmt.Sprintf("test_%s", xid.New().String())

	const iterations = 50
	var timeouts atomic.Int32

	for i := 0; i < iterations; i++ {
		jobID := xid.New().String()
		channel := store.ResultNotifyChannel(prefix, jobID)
		result := types.WorkResult{JobID: jobID, Success: true}
		resultData, _ := json.Marshal(result)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

		// NEW PATTERN (fixed): subscribe FIRST, then publish.
		resultCh := make(chan *types.WorkResult, 1)
		subReady := make(chan struct{}, 1)

		go func() {
			_ = rdb.Receive(ctx, rdb.B().Subscribe().Channel(channel).Build(),
				func(msg rueidis.PubSubMessage) {
					// First callback = subscription confirmed.
					select {
					case subReady <- struct{}{}:
					default:
					}

					var r types.WorkResult
					if json.Unmarshal([]byte(msg.Message), &r) == nil && r.JobID == jobID {
						select {
						case resultCh <- &r:
						default:
						}
					}
				})
			// Unblock subReady on error/cancel too.
			select {
			case subReady <- struct{}{}:
			default:
			}
		}()

		// Wait a tiny bit for subscription to be active, then publish.
		// In real code, the enqueue + worker dispatch takes >0 time,
		// so the subscription is almost always ready.
		time.Sleep(5 * time.Millisecond)

		rdb.Do(ctx, rdb.B().Publish().Channel(channel).Message(string(resultData)).Build())

		select {
		case <-ctx.Done():
			timeouts.Add(1)
		case <-resultCh:
			// Got it
		}
		cancel()
	}

	t.Logf("New pattern: %d/%d iterations timed out (expected: 0)", timeouts.Load(), iterations)
	if timeouts.Load() > 0 {
		t.Errorf("new pattern should never timeout, but got %d timeouts", timeouts.Load())
	}
}

// TestRaceCondition_NewPatternWithPollFallback tests the complete fixed pattern:
// subscribe + enqueue + poll fallback. Even if the result arrives before the
// subscription goroutine starts processing, the poll fallback catches it.
func TestRaceCondition_NewPatternWithPollFallback(t *testing.T) {
	rdb := getTestRedis(t)
	prefix := fmt.Sprintf("test_%s", xid.New().String())

	st, err := store.NewRedisStore(rdb, store.RedisStoreConfig{
		KeyPrefix: prefix,
		Tiers:     []store.TierConfig{{Name: "normal", Weight: 1, FetchBatch: 10}},
	}, nil)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}

	const iterations = 100
	var (
		gotViaPubSub atomic.Int32
		gotViaPoll   atomic.Int32
		wg           sync.WaitGroup
	)

	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			jobID := xid.New().String()
			channel := store.ResultNotifyChannel(prefix, jobID)
			result := types.WorkResult{JobID: jobID, Success: true}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			// 1. Start subscription.
			resultOut := make(chan *types.WorkResult, 1)
			go func() {
				_ = rdb.Receive(ctx, rdb.B().Subscribe().Channel(channel).Build(),
					func(msg rueidis.PubSubMessage) {
						var r types.WorkResult
						if json.Unmarshal([]byte(msg.Message), &r) == nil && r.JobID == jobID {
							select {
							case resultOut <- &r:
							default:
							}
						}
					})
			}()

			// 2. Simulate enqueue + instant worker: publish result via store.
			if err := st.PublishResult(ctx, result); err != nil {
				t.Errorf("publish result: %v", err)
				return
			}

			// 3. Poll fallback.
			if r, err := st.GetResult(ctx, jobID); err == nil && r != nil {
				gotViaPoll.Add(1)
				return
			}

			// 4. Wait for pub/sub.
			select {
			case <-resultOut:
				gotViaPubSub.Add(1)
			case <-ctx.Done():
				t.Errorf("job %s timed out", jobID)
			}
		}()
	}

	wg.Wait()

	t.Logf("Results: %d via poll fallback, %d via pub/sub (total %d/%d)",
		gotViaPoll.Load(), gotViaPubSub.Load(),
		gotViaPoll.Load()+gotViaPubSub.Load(), iterations)

	total := gotViaPoll.Load() + gotViaPubSub.Load()
	if total != int32(iterations) {
		t.Errorf("expected %d results, got %d", iterations, total)
	}
}
