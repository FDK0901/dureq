// Package failure provides integration tests that verify dureq's recovery
// from realistic failure scenarios: leader failover, worker crash, lock
// expiry, retry behavior, and schedule catchup.
//
// Default: Redis Cluster on ports 7001-7006 (make test-up)
// Standalone: REDIS_URL=redis://localhost:6381 REDIS_DB=15 go test ...
//
// Run: go test -v -count=1 -timeout=300s ./tests/failure/
package failure

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/client"
	"github.com/FDK0901/dureq/internal/server"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/redis/rueidis"
)

const (
	redisPW   = "your-password"
	keyPrefix = "ftest"
)

var defaultClusterAddrs = []string{
	"localhost:7001", "localhost:7002", "localhost:7003",
	"localhost:7004", "localhost:7005", "localhost:7006",
}

func isStandalone() bool { return os.Getenv("REDIS_URL") != "" }

func redisAddrs() []string {
	if env := os.Getenv("REDIS_CLUSTER_ADDRS"); env != "" {
		return strings.Split(env, ",")
	}
	return defaultClusterAddrs
}

func redisDB() int {
	if s := os.Getenv("REDIS_DB"); s != "" {
		n, _ := strconv.Atoi(s)
		return n
	}
	return 0
}

func flushDB(t *testing.T) {
	t.Helper()
	var opt rueidis.ClientOption
	if isStandalone() {
		addr := os.Getenv("REDIS_URL")
		addr = strings.TrimPrefix(addr, "redis://")
		opt = rueidis.ClientOption{InitAddress: []string{addr}, Password: redisPW, SelectDB: redisDB()}
	} else {
		opt = rueidis.ClientOption{InitAddress: redisAddrs(), Password: redisPW}
	}
	rdb, err := rueidis.NewClient(opt)
	if err != nil {
		t.Fatalf("connect redis for flush: %v", err)
	}
	defer rdb.Close()
	ctx := context.Background()
	if isStandalone() {
		if err := rdb.Do(ctx, rdb.B().Flushdb().Build()).Error(); err != nil {
			t.Fatalf("flushdb: %v", err)
		}
	} else {
		for addr, node := range rdb.Nodes() {
			if err := node.Do(ctx, node.B().Flushall().Build()).Error(); err != nil {
				t.Logf("flushall on %s: %v (may be replica)", addr, err)
			}
		}
	}
}

func newServer(t *testing.T, nodeID string, concurrency int, extraOpts ...server.Option) *server.Server {
	t.Helper()
	var opts []server.Option
	if isStandalone() {
		opts = []server.Option{
			server.WithRedisURL(os.Getenv("REDIS_URL")),
			server.WithRedisPassword(redisPW),
			server.WithRedisDB(redisDB()),
		}
	} else {
		opts = []server.Option{
			server.WithRedisCluster(redisAddrs()),
			server.WithRedisPassword(redisPW),
		}
	}
	opts = append(opts,
		server.WithNodeID(nodeID),
		server.WithMaxConcurrency(concurrency),
		server.WithKeyPrefix(keyPrefix),
		server.WithSchedulerTickInterval(500*time.Millisecond),
		server.WithShutdownTimeout(3*time.Second),
	)
	opts = append(opts, extraOpts...)

	srv, err := server.New(opts...)
	if err != nil {
		t.Fatalf("create server %s: %v", nodeID, err)
	}
	return srv
}

func newClient(t *testing.T) *client.Client {
	t.Helper()
	var opts []client.Option
	if isStandalone() {
		opts = []client.Option{
			client.WithRedisURL(os.Getenv("REDIS_URL")),
			client.WithRedisPassword(redisPW),
			client.WithRedisDB(redisDB()),
		}
	} else {
		opts = []client.Option{
			client.WithClusterAddrs(redisAddrs()),
			client.WithRedisPassword(redisPW),
		}
	}
	opts = append(opts, client.WithKeyPrefix(keyPrefix))
	cli, err := client.New(opts...)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	return cli
}

func waitFor(t *testing.T, timeout time.Duration, desc string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", desc)
}

func waitJobStatus(t *testing.T, cli *client.Client, jobID string, status types.JobStatus, timeout time.Duration) *types.Job {
	t.Helper()
	var job *types.Job
	waitFor(t, timeout, fmt.Sprintf("job %s → %s", jobID, status), func() bool {
		j, err := cli.GetJob(context.Background(), jobID)
		if err != nil {
			return false
		}
		job = j
		return j.Status == status
	})
	return job
}

func waitJobTerminal(t *testing.T, cli *client.Client, jobID string, timeout time.Duration) *types.Job {
	t.Helper()
	var job *types.Job
	waitFor(t, timeout, fmt.Sprintf("job %s terminal", jobID), func() bool {
		j, err := cli.GetJob(context.Background(), jobID)
		if err != nil {
			return false
		}
		job = j
		return j.Status.IsTerminal()
	})
	return job
}

// ============================================================
// F1: Leader dies before dispatch — new leader picks up schedule
// ============================================================

func TestF1_LeaderDiesBeforeDispatch(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var executed atomic.Int32

	// Start leader A — register handler but we'll kill it before it dispatches.
	srvA := newServer(t, "leader-a", 10, server.WithElectionTTL(3*time.Second))
	srvA.RegisterHandler(types.HandlerDefinition{
		TaskType: "f1.job",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			executed.Add(1)
			return nil
		},
	})
	if err := srvA.Start(ctx); err != nil {
		t.Fatalf("start A: %v", err)
	}

	cli := newClient(t)
	defer cli.Close()

	// Wait for leader election.
	time.Sleep(2 * time.Second)

	// Stop leader A before enqueuing — simulates leader dying before dispatch.
	srvA.Stop()

	// Enqueue a job while no leader is running.
	job, err := cli.Enqueue(ctx, "f1.job", map[string]string{"test": "f1"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Start leader B — should become leader and dispatch the pending job.
	srvB := newServer(t, "leader-b", 10, server.WithElectionTTL(3*time.Second))
	srvB.RegisterHandler(types.HandlerDefinition{
		TaskType: "f1.job",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			executed.Add(1)
			return nil
		},
	})
	if err := srvB.Start(ctx); err != nil {
		t.Fatalf("start B: %v", err)
	}
	defer srvB.Stop()

	// Job should complete on leader B.
	final := waitJobTerminal(t, cli, job.ID, 15*time.Second)
	if final.Status != types.JobStatusCompleted {
		t.Errorf("expected completed, got %s", final.Status)
	}
	if executed.Load() < 1 {
		t.Errorf("expected at least 1 execution, got %d", executed.Load())
	}
}

// ============================================================
// F2: Leader failover with two nodes — job still executes
// ============================================================

func TestF2_LeaderFailoverJobStillExecutes(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var executed atomic.Int32

	handler := func(ctx context.Context, payload json.RawMessage) error {
		executed.Add(1)
		return nil
	}

	// Start two servers. One becomes leader, the other is standby.
	srvA := newServer(t, "failover-a", 10, server.WithElectionTTL(3*time.Second))
	srvA.RegisterHandler(types.HandlerDefinition{TaskType: "f2.job", Handler: handler})
	if err := srvA.Start(ctx); err != nil {
		t.Fatalf("start A: %v", err)
	}

	srvB := newServer(t, "failover-b", 10, server.WithElectionTTL(3*time.Second))
	srvB.RegisterHandler(types.HandlerDefinition{TaskType: "f2.job", Handler: handler})
	if err := srvB.Start(ctx); err != nil {
		t.Fatalf("start B: %v", err)
	}
	defer srvB.Stop()

	cli := newClient(t)
	defer cli.Close()

	time.Sleep(2 * time.Second)

	// Enqueue a job.
	job, err := cli.Enqueue(ctx, "f2.job", map[string]string{"test": "f2"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Kill leader A mid-flight. B takes over.
	srvA.Stop()

	// Job should still complete (either A processed it or B picks it up).
	final := waitJobTerminal(t, cli, job.ID, 15*time.Second)
	if final.Status != types.JobStatusCompleted {
		t.Errorf("expected completed, got %s", final.Status)
	}
}

// ============================================================
// F3: Worker dies during handler — job retries on another worker
// ============================================================

func TestF3_WorkerDiesDuringHandler(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var executionsA, executionsB atomic.Int32

	// Worker A: will crash (context cancel) after starting the handler.
	ctxA, cancelA := context.WithCancel(ctx)
	srvA := newServer(t, "crash-worker-a", 10, server.WithElectionTTL(3*time.Second))
	srvA.RegisterHandler(types.HandlerDefinition{
		TaskType: "f3.job",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			executionsA.Add(1)
			// Simulate slow work — we'll kill this worker before it completes.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(30 * time.Second):
				return nil
			}
		},
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  3,
			InitialDelay: 1 * time.Second,
			MaxDelay:     5 * time.Second,
			Multiplier:   1.5,
		},
	})
	if err := srvA.Start(ctxA); err != nil {
		t.Fatalf("start A: %v", err)
	}

	cli := newClient(t)
	defer cli.Close()

	time.Sleep(2 * time.Second)

	// Enqueue a job.
	job, err := cli.Enqueue(ctx, "f3.job", map[string]string{"test": "f3"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Wait for worker A to pick it up.
	time.Sleep(2 * time.Second)

	// Kill worker A (simulates crash).
	cancelA()
	srvA.Stop()

	// Start worker B to recover the job.
	srvB := newServer(t, "crash-worker-b", 10, server.WithElectionTTL(3*time.Second))
	srvB.RegisterHandler(types.HandlerDefinition{
		TaskType: "f3.job",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			executionsB.Add(1)
			return nil // succeed immediately
		},
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  3,
			InitialDelay: 1 * time.Second,
			MaxDelay:     5 * time.Second,
			Multiplier:   1.5,
		},
	})
	if err := srvB.Start(ctx); err != nil {
		t.Fatalf("start B: %v", err)
	}
	defer srvB.Stop()

	// Job should eventually complete on worker B (via retry or orphan detection).
	final := waitJobTerminal(t, cli, job.ID, 90*time.Second)
	if final.Status != types.JobStatusCompleted {
		t.Errorf("expected completed, got %s", final.Status)
	}
	if executionsB.Load() < 1 {
		t.Errorf("expected worker B to execute at least once")
	}
}

// ============================================================
// F4: Handler returns retryable error — proper backoff retry
// ============================================================

func TestF4_RetryableErrorBackoff(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attempts atomic.Int32
	var timestamps []time.Time
	var mu sync.Mutex

	srv := newServer(t, "retry-node", 10)
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "f4.job",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			n := attempts.Add(1)
			mu.Lock()
			timestamps = append(timestamps, time.Now())
			mu.Unlock()
			if n < 3 {
				return fmt.Errorf("transient error attempt %d", n)
			}
			return nil
		},
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  5,
			InitialDelay: 1 * time.Second,
			MaxDelay:     10 * time.Second,
			Multiplier:   2.0,
			Jitter:       0.0, // no jitter for predictable timing
		},
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()

	time.Sleep(2 * time.Second)

	job, err := cli.Enqueue(ctx, "f4.job", map[string]string{"test": "f4"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	final := waitJobTerminal(t, cli, job.ID, 30*time.Second)
	if final.Status != types.JobStatusCompleted {
		t.Errorf("expected completed after retries, got %s", final.Status)
	}
	if attempts.Load() != 3 {
		t.Errorf("expected 3 attempts (2 fail + 1 success), got %d", attempts.Load())
	}

	// Verify backoff: second attempt should be ~1s after first, third ~2s after second.
	mu.Lock()
	defer mu.Unlock()
	if len(timestamps) >= 2 {
		gap1 := timestamps[1].Sub(timestamps[0])
		if gap1 < 800*time.Millisecond {
			t.Errorf("first retry gap too short: %v (expected ~1s)", gap1)
		}
	}
}

// ============================================================
// F5: Max retries exhausted — job moves to dead
// ============================================================

func TestF5_MaxRetriesExhausted_Dead(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attempts atomic.Int32

	srv := newServer(t, "dead-node", 10)
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "f5.job",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			attempts.Add(1)
			return fmt.Errorf("permanent failure")
		},
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  3,
			InitialDelay: 500 * time.Millisecond,
			MaxDelay:     2 * time.Second,
			Multiplier:   1.5,
		},
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()

	time.Sleep(2 * time.Second)

	job, err := cli.Enqueue(ctx, "f5.job", map[string]string{"test": "f5"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	final := waitJobTerminal(t, cli, job.ID, 30*time.Second)
	if final.Status != types.JobStatusDead {
		t.Errorf("expected dead after max retries, got %s", final.Status)
	}
	if attempts.Load() != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts.Load())
	}
}

// ============================================================
// F6: Non-retryable error — job goes directly to dead
// ============================================================

func TestF6_NonRetryableError_ImmediateDead(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attempts atomic.Int32

	srv := newServer(t, "nonretry-node", 10)
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "f6.job",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			attempts.Add(1)
			return &types.NonRetryableError{Err: fmt.Errorf("invalid input")}
		},
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  5,
			InitialDelay: 1 * time.Second,
			MaxDelay:     10 * time.Second,
			Multiplier:   2.0,
		},
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()

	time.Sleep(2 * time.Second)

	job, err := cli.Enqueue(ctx, "f6.job", map[string]string{"test": "f6"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	final := waitJobTerminal(t, cli, job.ID, 15*time.Second)
	if final.Status != types.JobStatusDead {
		t.Errorf("expected dead (non-retryable), got %s", final.Status)
	}
	// Should only execute once — no retries for non-retryable errors.
	if attempts.Load() != 1 {
		t.Errorf("expected 1 attempt (no retry), got %d", attempts.Load())
	}
}

// ============================================================
// F7: PauseError — job enters paused state, resumes after duration
// ============================================================

func TestF7_PauseError_PausesAndResumes(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attempts atomic.Int32

	srv := newServer(t, "pause-node", 10)
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "f7.job",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			n := attempts.Add(1)
			if n == 1 {
				return &types.PauseError{
					Reason:     "external dependency down",
					RetryAfter: 2 * time.Second,
				}
			}
			return nil
		},
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  5,
			InitialDelay: 1 * time.Second,
			MaxDelay:     10 * time.Second,
			Multiplier:   2.0,
		},
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()

	time.Sleep(2 * time.Second)

	job, err := cli.Enqueue(ctx, "f7.job", map[string]string{"test": "f7"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Should hit paused state first.
	waitJobStatus(t, cli, job.ID, types.JobStatusPaused, 15*time.Second)

	// Should resume and complete after RetryAfter duration.
	final := waitJobTerminal(t, cli, job.ID, 30*time.Second)
	if final.Status != types.JobStatusCompleted {
		t.Errorf("expected completed after pause+resume, got %s", final.Status)
	}
	if attempts.Load() != 2 {
		t.Errorf("expected 2 attempts (1 pause + 1 success), got %d", attempts.Load())
	}
}

// ============================================================
// F8: UniqueKey prevents duplicate enqueue
// ============================================================

func TestF8_UniqueKey_PreventsDuplicate(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var executed atomic.Int32

	srv := newServer(t, "unique-node", 10)
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "f8.job",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			executed.Add(1)
			time.Sleep(3 * time.Second) // hold the job active
			return nil
		},
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()

	time.Sleep(2 * time.Second)

	uniqueKey := "f8-unique-order-123"
	payload, _ := json.Marshal(map[string]string{"order": "123"})

	// First enqueue should succeed.
	job1, err := cli.EnqueueScheduled(ctx, &client.EnqueueRequest{
		TaskType:  "f8.job",
		Payload:   payload,
		Schedule:  types.Schedule{Type: types.ScheduleImmediate},
		UniqueKey: &uniqueKey,
	})
	if err != nil {
		t.Fatalf("first enqueue: %v", err)
	}

	// Second enqueue with same UniqueKey should fail.
	_, err = cli.EnqueueScheduled(ctx, &client.EnqueueRequest{
		TaskType:  "f8.job",
		Payload:   payload,
		Schedule:  types.Schedule{Type: types.ScheduleImmediate},
		UniqueKey: &uniqueKey,
	})
	if err == nil {
		t.Errorf("expected duplicate enqueue to be rejected")
	}

	// Original job should still complete.
	final := waitJobTerminal(t, cli, job1.ID, 15*time.Second)
	if final.Status != types.JobStatusCompleted {
		t.Errorf("expected completed, got %s", final.Status)
	}
	if executed.Load() != 1 {
		t.Errorf("expected exactly 1 execution, got %d", executed.Load())
	}
}

// ============================================================
// F9: RequestID idempotent enqueue — same ID returns cached job
// ============================================================

func TestF9_RequestID_IdempotentEnqueue(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var executed atomic.Int32

	srv := newServer(t, "reqid-node", 10)
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "f9.job",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			executed.Add(1)
			return nil
		},
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()

	time.Sleep(2 * time.Second)

	reqID := "f9-request-abc-123"
	payload, _ := json.Marshal(map[string]string{"data": "value"})

	// First enqueue with RequestID.
	job1, err := cli.EnqueueScheduled(ctx, &client.EnqueueRequest{
		TaskType:  "f9.job",
		Payload:   payload,
		Schedule:  types.Schedule{Type: types.ScheduleImmediate},
		RequestID: &reqID,
	})
	if err != nil {
		t.Fatalf("first enqueue: %v", err)
	}

	// Second enqueue with same RequestID — should return cached job.
	job2, err := cli.EnqueueScheduled(ctx, &client.EnqueueRequest{
		TaskType:  "f9.job",
		Payload:   payload,
		Schedule:  types.Schedule{Type: types.ScheduleImmediate},
		RequestID: &reqID,
	})
	if err != nil {
		t.Fatalf("second enqueue: %v", err)
	}

	// Both should return the same job ID.
	if job1.ID != job2.ID {
		t.Errorf("expected same job ID from idempotent enqueue: %s != %s", job1.ID, job2.ID)
	}

	// Should execute only once.
	final := waitJobTerminal(t, cli, job1.ID, 15*time.Second)
	if final.Status != types.JobStatusCompleted {
		t.Errorf("expected completed, got %s", final.Status)
	}

	// Wait a bit more and check execution count.
	time.Sleep(2 * time.Second)
	if executed.Load() != 1 {
		t.Errorf("expected exactly 1 execution, got %d", executed.Load())
	}
}

// ============================================================
// F10: Cancellation during execution — handler receives context cancel
// ============================================================

func TestF10_CancellationDuringExecution(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var handlerStarted atomic.Bool
	var handlerCancelled atomic.Bool

	srv := newServer(t, "cancel-node", 10)
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "f10.job",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			handlerStarted.Store(true)
			// Wait for cancellation.
			<-ctx.Done()
			handlerCancelled.Store(true)
			return ctx.Err()
		},
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()

	time.Sleep(2 * time.Second)

	job, err := cli.Enqueue(ctx, "f10.job", map[string]string{"test": "f10"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Wait for handler to start.
	waitFor(t, 10*time.Second, "handler started", func() bool {
		return handlerStarted.Load()
	})

	// Cancel the job.
	if err := cli.Cancel(ctx, job.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	// Wait for the job to reach cancelled state.
	final := waitJobTerminal(t, cli, job.ID, 15*time.Second)
	if final.Status != types.JobStatusCancelled {
		t.Errorf("expected cancelled, got %s", final.Status)
	}

	// Handler should have received context cancellation.
	time.Sleep(1 * time.Second)
	if !handlerCancelled.Load() {
		t.Errorf("expected handler to receive context cancellation")
	}
}
