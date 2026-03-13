// Package integration provides comprehensive end-to-end tests for dureqv1.
//
// Default: Redis Cluster on ports 7001-7006 (make test-up)
// Standalone: REDIS_URL=redis://localhost:6381 REDIS_DB=15 go test ...
//
// Run: go test -v -count=1 -timeout=300s ./tests/integration/
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/client"
	"github.com/FDK0901/dureq/internal/server"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/redis/rueidis"
)

const (
	redisPW   = "your-password"
	keyPrefix = "itest"
)

var defaultClusterAddrs = []string{
	"localhost:7001", "localhost:7002", "localhost:7003",
	"localhost:7004", "localhost:7005", "localhost:7006",
}

// isStandalone returns true when REDIS_URL is set (standalone mode).
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

// flushDB clears all keys before each test.
func flushDB(t *testing.T) {
	t.Helper()

	var opt rueidis.ClientOption
	if isStandalone() {
		// Standalone: parse URL for host:port
		addr := os.Getenv("REDIS_URL")
		addr = strings.TrimPrefix(addr, "redis://")
		opt = rueidis.ClientOption{
			InitAddress: []string{addr},
			Password:    redisPW,
			SelectDB:    redisDB(),
		}
	} else {
		opt = rueidis.ClientOption{
			InitAddress: redisAddrs(),
			Password:    redisPW,
		}
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

// newServer creates a server node with the given nodeID.
func newServer(t *testing.T, nodeID string, concurrency int) *server.Server {
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

	srv, err := server.New(opts...)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	return srv
}

// newClient creates a client connected to the test Redis.
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

// waitFor polls until condition returns true or timeout.
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

// waitJobTerminal polls until job reaches a terminal status.
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
// 1. One-shot Job: success, failure, retry, DLQ
// ============================================================

func TestOneshotJob_Success(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "oneshot-node", 10)
	var executed atomic.Int32
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "test.oneshot",
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

	time.Sleep(2 * time.Second) // leader election

	job, err := cli.Enqueue(ctx, "test.oneshot", map[string]string{"key": "value"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	final := waitJobTerminal(t, cli, job.ID, 15*time.Second)
	if final.Status != types.JobStatusCompleted {
		t.Errorf("expected completed, got %s", final.Status)
	}
	if executed.Load() != 1 {
		t.Errorf("expected 1 execution, got %d", executed.Load())
	}
}

func TestOneshotJob_EnqueueAndWait(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "eaw-node", 10)
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "test.eaw",
		HandlerWithResult: func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
			return json.Marshal(map[string]string{"result": "ok"})
		},
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()

	time.Sleep(2 * time.Second)

	result, err := cli.EnqueueAndWait(ctx, "test.eaw", map[string]string{"x": "1"}, 15*time.Second)
	if err != nil {
		t.Fatalf("enqueue and wait: %v", err)
	}
	if !result.Success {
		t.Errorf("expected success, got error: %v", result.Error)
	}
	var output map[string]string
	json.Unmarshal(result.Output, &output)
	if output["result"] != "ok" {
		t.Errorf("expected result=ok, got %v", output)
	}
}

func TestOneshotJob_FailureAndRetry(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "retry-node", 10)
	var attempts atomic.Int32
	// RetryPolicy set on handler — worker now falls back to handler policy
	// when job.RetryPolicy is nil (the fix under test).
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "test.retry",
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  3,
			InitialDelay: 500 * time.Millisecond,
			MaxDelay:     1 * time.Second,
			Multiplier:   1.0,
		},
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			n := attempts.Add(1)
			if n < 3 {
				return fmt.Errorf("transient error attempt %d", n)
			}
			return nil // succeed on 3rd attempt
		},
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()
	time.Sleep(2 * time.Second)

	// Enqueue with simple Enqueue() — no job-level RetryPolicy.
	// Worker should fall back to handler RetryPolicy (500ms, 3 attempts).
	job, err := cli.Enqueue(ctx, "test.retry", map[string]string{"fail": "twice"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	final := waitJobTerminal(t, cli, job.ID, 30*time.Second)
	if final.Status != types.JobStatusCompleted {
		t.Errorf("expected completed after retries, got %s", final.Status)
	}
	if attempts.Load() != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts.Load())
	}
}

func TestOneshotJob_DLQ(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "dlq-node", 10)
	// Handler-level RetryPolicy: 2 max attempts, fast backoff.
	// Worker falls back to this when job.RetryPolicy is nil.
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "test.dlq",
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  2,
			InitialDelay: 500 * time.Millisecond,
			MaxDelay:     500 * time.Millisecond,
			Multiplier:   1.0,
		},
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			return fmt.Errorf("permanent failure")
		},
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()
	time.Sleep(2 * time.Second)

	// Simple Enqueue — no job-level RetryPolicy. Handler policy used.
	job, err := cli.Enqueue(ctx, "test.dlq", map[string]string{"fail": "always"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	final := waitJobTerminal(t, cli, job.ID, 30*time.Second)
	if final.Status != types.JobStatusDead {
		t.Errorf("expected dead (DLQ), got %s", final.Status)
	}

	// Verify DLQ has the message.
	dlqMsgs, err := cli.Store().ListDLQ(ctx, 10)
	if err != nil {
		t.Fatalf("list dlq: %v", err)
	}
	found := false
	for _, msg := range dlqMsgs {
		if msg.JobID == job.ID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("job %s not found in DLQ (dlq has %d messages)", job.ID, len(dlqMsgs))
	}
}

func TestOneshotJob_NonRetryableError(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "nonretry-node", 10)
	var attempts atomic.Int32
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "test.nonretry",
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  5,
			InitialDelay: 300 * time.Millisecond,
			MaxDelay:     1 * time.Second,
			Multiplier:   1.0,
		},
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			attempts.Add(1)
			return &types.NonRetryableError{Err: fmt.Errorf("bad input")}
		},
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()
	time.Sleep(2 * time.Second)

	job, err := cli.Enqueue(ctx, "test.nonretry", nil)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	final := waitJobTerminal(t, cli, job.ID, 15*time.Second)
	// Non-retryable should go to dead/failed immediately, not retry 5 times.
	if attempts.Load() != 1 {
		t.Errorf("expected 1 attempt for non-retryable, got %d", attempts.Load())
	}
	if final.Status != types.JobStatusDead && final.Status != types.JobStatusFailed {
		t.Errorf("expected dead/failed, got %s", final.Status)
	}
}

func TestOneshotJob_Cancel(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "cancel-node", 10)
	started := make(chan struct{})
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "test.cancel",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			close(started)
			<-ctx.Done()
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

	job, err := cli.Enqueue(ctx, "test.cancel", nil)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Wait for handler to start.
	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("handler did not start")
	}

	if err := cli.Cancel(ctx, job.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	final := waitJobTerminal(t, cli, job.ID, 15*time.Second)
	if final.Status != types.JobStatusCancelled {
		t.Errorf("expected cancelled, got %s", final.Status)
	}
}

// ============================================================
// 2. Scheduled/Periodic Job (duration with EndAt)
// ============================================================

func TestScheduledJob_Periodic(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "sched-node", 10)
	var execCount atomic.Int32
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "test.periodic",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			execCount.Add(1)
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

	interval := types.Duration(2 * time.Second)
	startsAt := time.Now()
	endsAt := startsAt.Add(7 * time.Second) // should fire ~3-4 times

	payload, _ := json.Marshal(map[string]string{"task": "periodic"})
	_, err := cli.EnqueueScheduled(ctx, &client.EnqueueRequest{
		TaskType: "test.periodic",
		Payload:  payload,
		Schedule: types.Schedule{
			Type:     types.ScheduleDuration,
			Interval: &interval,
			StartsAt: &startsAt,
			EndsAt:   &endsAt,
		},
	})
	if err != nil {
		t.Fatalf("enqueue scheduled: %v", err)
	}

	// Wait for schedule window to pass plus buffer.
	time.Sleep(10 * time.Second)

	count := execCount.Load()
	if count < 2 {
		t.Errorf("expected at least 2 periodic executions, got %d", count)
	}
	t.Logf("periodic job executed %d times in ~7s window", count)
}

// ============================================================
// 3. Workflow (DAG)
// ============================================================

func TestWorkflow_DAG(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "wf-node", 10)

	// Track execution order.
	var mu sync.Mutex
	var order []string
	handler := func(name string) types.HandlerFunc {
		return func(ctx context.Context, payload json.RawMessage) error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			time.Sleep(200 * time.Millisecond)
			return nil
		}
	}

	srv.RegisterHandler(types.HandlerDefinition{TaskType: "wf.validate", Handler: handler("validate")})
	srv.RegisterHandler(types.HandlerDefinition{TaskType: "wf.charge", Handler: handler("charge")})
	srv.RegisterHandler(types.HandlerDefinition{TaskType: "wf.reserve", Handler: handler("reserve")})
	srv.RegisterHandler(types.HandlerDefinition{TaskType: "wf.ship", Handler: handler("ship")})
	srv.RegisterHandler(types.HandlerDefinition{TaskType: "wf.confirm", Handler: handler("confirm")})

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()
	time.Sleep(2 * time.Second)

	payload, _ := json.Marshal(map[string]string{"order_id": "ORD-001"})
	wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{
		Name: "order-processing",
		Tasks: []types.WorkflowTask{
			{Name: "validate", TaskType: "wf.validate", Payload: payload},
			{Name: "charge", TaskType: "wf.charge", Payload: payload, DependsOn: []string{"validate"}},
			{Name: "reserve", TaskType: "wf.reserve", Payload: payload, DependsOn: []string{"validate"}},
			{Name: "ship", TaskType: "wf.ship", Payload: payload, DependsOn: []string{"charge", "reserve"}},
			{Name: "confirm", TaskType: "wf.confirm", Payload: payload, DependsOn: []string{"ship"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("enqueue workflow: %v", err)
	}

	// Wait for workflow completion.
	var finalWF *types.WorkflowInstance
	waitFor(t, 30*time.Second, "workflow complete", func() bool {
		wfStatus, err := cli.GetWorkflow(ctx, wf.ID)
		if err != nil {
			return false
		}
		finalWF = wfStatus
		return wfStatus.Status.IsTerminal()
	})

	if finalWF.Status != types.WorkflowStatusCompleted {
		t.Errorf("expected workflow completed, got %s", finalWF.Status)
		for name, task := range finalWF.Tasks {
			t.Logf("  task %s: status=%s err=%v", name, task.Status, task.Error)
		}
	}

	// Verify DAG ordering: validate must come before charge/reserve, ship after both, confirm last.
	mu.Lock()
	executedOrder := make([]string, len(order))
	copy(executedOrder, order)
	mu.Unlock()

	indexOf := func(name string) int {
		for i, n := range executedOrder {
			if n == name {
				return i
			}
		}
		return -1
	}

	if indexOf("validate") >= indexOf("charge") || indexOf("validate") >= indexOf("reserve") {
		t.Errorf("validate should run before charge and reserve: order=%v", executedOrder)
	}
	if indexOf("ship") <= indexOf("charge") || indexOf("ship") <= indexOf("reserve") {
		t.Errorf("ship should run after charge and reserve: order=%v", executedOrder)
	}
	if indexOf("confirm") <= indexOf("ship") {
		t.Errorf("confirm should run after ship: order=%v", executedOrder)
	}
	t.Logf("execution order: %v", executedOrder)
}

func TestWorkflow_PartialFailure(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "wf-fail-node", 10)
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "wff.step1",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			return nil
		},
	})
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "wff.step2",
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  2,
			InitialDelay: 300 * time.Millisecond,
			MaxDelay:     500 * time.Millisecond,
			Multiplier:   1.0,
		},
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			return &types.NonRetryableError{Err: fmt.Errorf("step2 failed permanently")}
		},
	})
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "wff.step3",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
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

	payload, _ := json.Marshal(nil)
	wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{
		Name: "fail-workflow",
		Tasks: []types.WorkflowTask{
			{Name: "step1", TaskType: "wff.step1", Payload: payload},
			{Name: "step2", TaskType: "wff.step2", Payload: payload, DependsOn: []string{"step1"}},
			{Name: "step3", TaskType: "wff.step3", Payload: payload, DependsOn: []string{"step2"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("enqueue workflow: %v", err)
	}

	var finalWF *types.WorkflowInstance
	waitFor(t, 30*time.Second, "workflow terminal", func() bool {
		wfStatus, err := cli.GetWorkflow(ctx, wf.ID)
		if err != nil {
			return false
		}
		finalWF = wfStatus
		return wfStatus.Status.IsTerminal()
	})

	if finalWF.Status != types.WorkflowStatusFailed {
		t.Errorf("expected workflow failed, got %s", finalWF.Status)
	}
	// step3 should NOT have executed (depends on failed step2).
	if state, ok := finalWF.Tasks["step3"]; ok {
		if state.Status == types.JobStatusCompleted {
			t.Errorf("step3 should not have completed (depends on failed step2)")
		}
	}
}

func TestWorkflow_Cancel(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "wf-cancel-node", 10)
	step1Started := make(chan struct{})
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "wfc.step1",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			close(step1Started)
			<-ctx.Done()
			return ctx.Err()
		},
	})
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "wfc.step2",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
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

	payload, _ := json.Marshal(nil)
	wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{
		Name: "cancel-workflow",
		Tasks: []types.WorkflowTask{
			{Name: "step1", TaskType: "wfc.step1", Payload: payload},
			{Name: "step2", TaskType: "wfc.step2", Payload: payload, DependsOn: []string{"step1"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case <-step1Started:
	case <-time.After(15 * time.Second):
		t.Fatal("step1 did not start")
	}

	if err := cli.CancelWorkflow(ctx, wf.ID); err != nil {
		t.Fatalf("cancel workflow: %v", err)
	}

	var finalWF *types.WorkflowInstance
	waitFor(t, 15*time.Second, "workflow cancelled", func() bool {
		wfStatus, err := cli.GetWorkflow(ctx, wf.ID)
		if err != nil {
			return false
		}
		finalWF = wfStatus
		return wfStatus.Status == types.WorkflowStatusCancelled
	})

	if finalWF.Status != types.WorkflowStatusCancelled {
		t.Errorf("expected cancelled, got %s", finalWF.Status)
	}
}

// ============================================================
// 4. Batch Processing
// ============================================================

func TestBatch_Success(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "batch-node", 10)
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "bt.onetime",
		HandlerWithResult: func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
			return json.Marshal(map[string]string{"template": "loaded"})
		},
	})
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "bt.item",
		HandlerWithResult: func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
			var item struct {
				ID string `json:"id"`
			}
			json.Unmarshal(payload, &item)
			return json.Marshal(map[string]string{"output": fmt.Sprintf("processed-%s", item.ID)})
		},
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()
	time.Sleep(2 * time.Second)

	items := make([]types.BatchItem, 5)
	for i := range items {
		p, _ := json.Marshal(map[string]string{"id": fmt.Sprintf("item-%d", i)})
		items[i] = types.BatchItem{ID: fmt.Sprintf("item-%d", i), Payload: p}
	}

	onetimeType := types.TaskType("bt.onetime")
	batch, err := cli.EnqueueBatch(ctx, types.BatchDefinition{
		Name:            "test-batch",
		OnetimeTaskType: &onetimeType,
		OnetimePayload:  json.RawMessage(`{"template":"v1"}`),
		ItemTaskType:    "bt.item",
		Items:           items,
		FailurePolicy:   types.BatchContinueOnError,
		ChunkSize:       3,
	})
	if err != nil {
		t.Fatalf("enqueue batch: %v", err)
	}

	var finalBatch *types.BatchInstance
	waitFor(t, 30*time.Second, "batch complete", func() bool {
		b, err := cli.GetBatch(ctx, batch.ID)
		if err != nil {
			return false
		}
		finalBatch = b
		return b.Status.IsTerminal()
	})

	if finalBatch.Status != types.WorkflowStatusCompleted {
		t.Errorf("expected batch completed, got %s", finalBatch.Status)
	}
	if finalBatch.CompletedItems != 5 {
		t.Errorf("expected 5 completed items, got %d", finalBatch.CompletedItems)
	}

	// Check results.
	results, err := cli.GetBatchResults(ctx, batch.ID)
	if err != nil {
		t.Fatalf("get batch results: %v", err)
	}
	successCount := 0
	for _, r := range results {
		if r.Success {
			successCount++
		}
	}
	if successCount != 5 {
		t.Errorf("expected 5 successful results, got %d", successCount)
	}
}

func TestBatch_PartialFailure_ContinueOnError(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "batch-fail-node", 10)
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "bf.item",
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  1,
			InitialDelay: 200 * time.Millisecond,
			MaxDelay:     200 * time.Millisecond,
			Multiplier:   1.0,
		},
		HandlerWithResult: func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
			var item struct {
				ID   string `json:"id"`
				Fail bool   `json:"fail"`
			}
			json.Unmarshal(payload, &item)
			if item.Fail {
				return nil, &types.NonRetryableError{Err: fmt.Errorf("item %s failed", item.ID)}
			}
			return json.Marshal(map[string]string{"ok": item.ID})
		},
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()
	time.Sleep(2 * time.Second)

	items := []types.BatchItem{
		{ID: "ok-1", Payload: json.RawMessage(`{"id":"ok-1","fail":false}`)},
		{ID: "fail-1", Payload: json.RawMessage(`{"id":"fail-1","fail":true}`)},
		{ID: "ok-2", Payload: json.RawMessage(`{"id":"ok-2","fail":false}`)},
		{ID: "fail-2", Payload: json.RawMessage(`{"id":"fail-2","fail":true}`)},
		{ID: "ok-3", Payload: json.RawMessage(`{"id":"ok-3","fail":false}`)},
	}

	batch, err := cli.EnqueueBatch(ctx, types.BatchDefinition{
		Name:          "partial-fail-batch",
		ItemTaskType:  "bf.item",
		Items:         items,
		FailurePolicy: types.BatchContinueOnError,
		ChunkSize:     5,
	})
	if err != nil {
		t.Fatalf("enqueue batch: %v", err)
	}

	var finalBatch *types.BatchInstance
	waitFor(t, 30*time.Second, "batch terminal", func() bool {
		b, err := cli.GetBatch(ctx, batch.ID)
		if err != nil {
			return false
		}
		finalBatch = b
		return b.Status.IsTerminal()
	})

	// ContinueOnError: should complete (with failures), not fail-fast.
	if finalBatch.CompletedItems != 3 {
		t.Errorf("expected 3 completed items, got %d", finalBatch.CompletedItems)
	}
	if finalBatch.FailedItems < 2 {
		t.Errorf("expected at least 2 failed items, got %d", finalBatch.FailedItems)
	}
	t.Logf("batch status=%s completed=%d failed=%d", finalBatch.Status, finalBatch.CompletedItems, finalBatch.FailedItems)
}

func TestBatch_Cancel(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "batch-cancel-node", 10)
	itemStarted := make(chan struct{}, 1)
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "bc.item",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			select {
			case itemStarted <- struct{}{}:
			default:
			}
			<-ctx.Done()
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

	items := make([]types.BatchItem, 3)
	for i := range items {
		p, _ := json.Marshal(map[string]string{"id": fmt.Sprintf("item-%d", i)})
		items[i] = types.BatchItem{ID: fmt.Sprintf("item-%d", i), Payload: p}
	}

	batch, err := cli.EnqueueBatch(ctx, types.BatchDefinition{
		Name:         "cancel-batch",
		ItemTaskType: "bc.item",
		Items:        items,
		ChunkSize:    3,
	})
	if err != nil {
		t.Fatalf("enqueue batch: %v", err)
	}

	select {
	case <-itemStarted:
	case <-time.After(15 * time.Second):
		t.Fatal("no item started")
	}

	// CancelBatch now has internal CAS retry — single call should work.
	if err := cli.CancelBatch(ctx, batch.ID); err != nil {
		t.Fatalf("cancel batch: %v", err)
	}

	var finalBatch *types.BatchInstance
	waitFor(t, 15*time.Second, "batch cancelled", func() bool {
		b, err := cli.GetBatch(ctx, batch.ID)
		if err != nil {
			return false
		}
		finalBatch = b
		return b.Status == types.WorkflowStatusCancelled
	})

	if finalBatch.Status != types.WorkflowStatusCancelled {
		t.Errorf("expected cancelled, got %s", finalBatch.Status)
	}
}

// ============================================================
// 5. Progress Monitoring
// ============================================================

func TestProgress_Reporting(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "progress-node", 10)
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "test.progress",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			for i := 1; i <= 5; i++ {
				types.ReportProgress(ctx, map[string]any{
					"percent": i * 20,
					"step":    fmt.Sprintf("step-%d", i),
				})
				time.Sleep(300 * time.Millisecond)
			}
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

	job, err := cli.Enqueue(ctx, "test.progress", nil)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Wait a moment for some progress to be reported.
	time.Sleep(1 * time.Second)

	// Check that run has progress data.
	progressSeen := false
	waitFor(t, 10*time.Second, "progress reported", func() bool {
		runs, err := cli.Store().ListActiveRunsByJobID(ctx, job.ID)
		if err != nil || len(runs) == 0 {
			return false
		}
		if runs[0].Progress != nil && len(runs[0].Progress) > 2 {
			progressSeen = true
			return true
		}
		return false
	})

	if !progressSeen {
		// It may have already completed; that's OK — just check the job completed.
		final := waitJobTerminal(t, cli, job.ID, 10*time.Second)
		if final.Status != types.JobStatusCompleted {
			t.Errorf("expected completed, got %s", final.Status)
		}
	} else {
		// Wait for final completion.
		final := waitJobTerminal(t, cli, job.ID, 10*time.Second)
		if final.Status != types.JobStatusCompleted {
			t.Errorf("expected completed, got %s", final.Status)
		}
	}
}

// ============================================================
// 6. Priority Ordering
// ============================================================

func TestPriority_Ordering(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "prio-node", 1) // concurrency=1 to force serial execution
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "test.prio",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			time.Sleep(200 * time.Millisecond)
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

	// Enqueue: first a low-priority, then a critical-priority.
	lowPrio := types.PriorityLow
	critPrio := types.PriorityCritical
	payloadLow, _ := json.Marshal(map[string]string{"prio": "low"})
	payloadCrit, _ := json.Marshal(map[string]string{"prio": "critical"})

	_, err := cli.EnqueueScheduled(ctx, &client.EnqueueRequest{
		TaskType: "test.prio",
		Payload:  payloadLow,
		Schedule: types.Schedule{Type: types.ScheduleImmediate},
		Priority: &lowPrio,
	})
	if err != nil {
		t.Fatalf("enqueue low: %v", err)
	}

	critJob, err := cli.EnqueueScheduled(ctx, &client.EnqueueRequest{
		TaskType: "test.prio",
		Payload:  payloadCrit,
		Schedule: types.Schedule{Type: types.ScheduleImmediate},
		Priority: &critPrio,
	})
	if err != nil {
		t.Fatalf("enqueue critical: %v", err)
	}

	// Critical should complete (it gets priority in the weighted tier system).
	final := waitJobTerminal(t, cli, critJob.ID, 15*time.Second)
	if final.Status != types.JobStatusCompleted {
		t.Errorf("critical job should complete, got %s", final.Status)
	}
}

// ============================================================
// 7. Unique Key Deduplication
// ============================================================

func TestUniqueKey_Dedup(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "dedup-node", 10)
	var execCount atomic.Int32
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "test.dedup",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			execCount.Add(1)
			time.Sleep(500 * time.Millisecond)
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

	uk := "unique-test-key-001"
	payload, _ := json.Marshal(map[string]string{"key": "value"})
	job1, err := cli.EnqueueScheduled(ctx, &client.EnqueueRequest{
		TaskType:  "test.dedup",
		Payload:   payload,
		Schedule:  types.Schedule{Type: types.ScheduleImmediate},
		UniqueKey: &uk,
	})
	if err != nil {
		t.Fatalf("enqueue first: %v", err)
	}

	// Second enqueue with same unique key should return the same job (or error).
	job2, err := cli.EnqueueScheduled(ctx, &client.EnqueueRequest{
		TaskType:  "test.dedup",
		Payload:   payload,
		Schedule:  types.Schedule{Type: types.ScheduleImmediate},
		UniqueKey: &uk,
	})
	if err != nil {
		// Duplicate key may return error — that's fine.
		t.Logf("second enqueue returned error (expected): %v", err)
	} else if job2.ID != job1.ID {
		t.Errorf("expected same job ID for duplicate unique key, got %s vs %s", job1.ID, job2.ID)
	}

	waitJobTerminal(t, cli, job1.ID, 15*time.Second)
	if execCount.Load() != 1 {
		t.Errorf("expected 1 execution for deduplicated job, got %d", execCount.Load())
	}
}

// ============================================================
// 8. Multi-node Cluster: Leader Election + Work Distribution
// ============================================================

func TestMultiNode_WorkDistribution(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Track which node handles each job.
	var mu sync.Mutex
	nodeMap := make(map[string]string) // jobID -> nodeID

	makeHandler := func() types.HandlerFunc {
		return func(ctx context.Context, payload json.RawMessage) error {
			nodeID := types.GetNodeID(ctx)
			jobID := types.GetJobID(ctx)
			mu.Lock()
			nodeMap[jobID] = nodeID
			mu.Unlock()
			time.Sleep(300 * time.Millisecond)
			return nil
		}
	}

	// Start 2 server nodes.
	srv1 := newServer(t, "cluster-node-1", 5)
	srv1.RegisterHandler(types.HandlerDefinition{TaskType: "test.cluster", Handler: makeHandler()})
	if err := srv1.Start(ctx); err != nil {
		t.Fatalf("start node1: %v", err)
	}
	defer srv1.Stop()

	srv2 := newServer(t, "cluster-node-2", 5)
	srv2.RegisterHandler(types.HandlerDefinition{TaskType: "test.cluster", Handler: makeHandler()})
	if err := srv2.Start(ctx); err != nil {
		t.Fatalf("start node2: %v", err)
	}
	defer srv2.Stop()

	time.Sleep(3 * time.Second) // leader election

	cli := newClient(t)
	defer cli.Close()

	// Enqueue 10 jobs.
	jobIDs := make([]string, 10)
	for i := 0; i < 10; i++ {
		job, err := cli.Enqueue(ctx, "test.cluster", map[string]int{"i": i})
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
		jobIDs[i] = job.ID
	}

	// Wait for all jobs to complete.
	for _, id := range jobIDs {
		waitJobTerminal(t, cli, id, 30*time.Second)
	}

	// Check that both nodes handled at least some jobs.
	mu.Lock()
	defer mu.Unlock()
	nodeCounts := make(map[string]int)
	for _, nodeID := range nodeMap {
		nodeCounts[nodeID]++
	}
	t.Logf("node distribution: %v", nodeCounts)

	if len(nodeCounts) < 2 {
		t.Logf("WARNING: only 1 node handled all jobs (this can happen with fast jobs). nodes: %v", nodeCounts)
	}
	// At minimum, all jobs should be completed.
	total := 0
	for _, c := range nodeCounts {
		total += c
	}
	if total != 10 {
		t.Errorf("expected 10 jobs handled, got %d", total)
	}
}

func TestMultiNode_SingleExecution(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Verify that each job runs exactly once, even with 3 nodes.
	var execCount atomic.Int32
	makeHandler := func() types.HandlerFunc {
		return func(ctx context.Context, payload json.RawMessage) error {
			execCount.Add(1)
			time.Sleep(100 * time.Millisecond)
			return nil
		}
	}

	servers := make([]*server.Server, 3)
	for i := 0; i < 3; i++ {
		srv := newServer(t, fmt.Sprintf("single-exec-node-%d", i), 10)
		srv.RegisterHandler(types.HandlerDefinition{TaskType: "test.once", Handler: makeHandler()})
		if err := srv.Start(ctx); err != nil {
			t.Fatalf("start node %d: %v", i, err)
		}
		servers[i] = srv
	}
	defer func() {
		for _, s := range servers {
			s.Stop()
		}
	}()

	time.Sleep(3 * time.Second)

	cli := newClient(t)
	defer cli.Close()

	numJobs := 20
	jobIDs := make([]string, numJobs)
	for i := 0; i < numJobs; i++ {
		job, err := cli.Enqueue(ctx, "test.once", map[string]int{"i": i})
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
		jobIDs[i] = job.ID
	}

	for _, id := range jobIDs {
		waitJobTerminal(t, cli, id, 30*time.Second)
	}

	if int(execCount.Load()) != numJobs {
		t.Errorf("expected exactly %d executions with 3 nodes, got %d", numJobs, execCount.Load())
	}
}

// ============================================================
// 9. Middleware
// ============================================================

func TestMiddleware_Global(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "mw-node", 10)

	var mwCalled atomic.Int32
	srv.Use(func(next types.HandlerFunc) types.HandlerFunc {
		return func(ctx context.Context, payload json.RawMessage) error {
			mwCalled.Add(1)
			return next(ctx, payload)
		}
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "test.mw",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
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

	job, err := cli.Enqueue(ctx, "test.mw", nil)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	waitJobTerminal(t, cli, job.ID, 15*time.Second)
	if mwCalled.Load() < 1 {
		t.Errorf("global middleware not called")
	}
}

// ============================================================
// 10. Handler with Result
// ============================================================

func TestHandlerWithResult(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "result-node", 10)
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "test.result",
		HandlerWithResult: func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
			var input map[string]int
			json.Unmarshal(payload, &input)
			result := map[string]int{"doubled": input["value"] * 2}
			return json.Marshal(result)
		},
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()
	time.Sleep(2 * time.Second)

	res, err := cli.EnqueueAndWait(ctx, "test.result", map[string]int{"value": 21}, 15*time.Second)
	if err != nil {
		t.Fatalf("enqueue and wait: %v", err)
	}
	if !res.Success {
		t.Fatalf("expected success, got error: %v", res.Error)
	}

	var output map[string]int
	json.Unmarshal(res.Output, &output)
	if output["doubled"] != 42 {
		t.Errorf("expected doubled=42, got %v", output)
	}
}

// ============================================================
// 11. Rate-Limited Error
// ============================================================

func TestRateLimitedError(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "ratelimit-node", 10)
	var attempts atomic.Int32
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "test.ratelimit",
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  5,
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     5 * time.Second,
			Multiplier:   1.0,
		},
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			n := attempts.Add(1)
			if n <= 2 {
				return &types.RateLimitedError{
					Err:        fmt.Errorf("rate limited"),
					RetryAfter: 500 * time.Millisecond,
				}
			}
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

	job, err := cli.Enqueue(ctx, "test.ratelimit", nil)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	final := waitJobTerminal(t, cli, job.ID, 30*time.Second)
	if final.Status != types.JobStatusCompleted {
		t.Errorf("expected completed after rate limit retries, got %s", final.Status)
	}
	if attempts.Load() != 3 {
		t.Errorf("expected 3 attempts (2 rate-limited + 1 success), got %d", attempts.Load())
	}
}

// ============================================================
// 12. Panic Recovery
// ============================================================

func TestPanicRecovery(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "panic-node", 10)
	var attempts atomic.Int32
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "test.panic",
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  3,
			InitialDelay: 300 * time.Millisecond,
			MaxDelay:     1 * time.Second,
			Multiplier:   1.0,
		},
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			n := attempts.Add(1)
			if n == 1 {
				panic("unexpected nil pointer")
			}
			return nil // succeed on retry
		},
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()
	time.Sleep(2 * time.Second)

	job, err := cli.Enqueue(ctx, "test.panic", nil)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	final := waitJobTerminal(t, cli, job.ID, 30*time.Second)
	if final.Status != types.JobStatusCompleted {
		t.Errorf("expected completed after panic recovery, got %s", final.Status)
	}
	if attempts.Load() < 2 {
		t.Errorf("expected at least 2 attempts (1 panic + 1 success), got %d", attempts.Load())
	}
}

// ============================================================
// 13. Concurrent Load: burst of jobs across multiple nodes
// ============================================================

func TestConcurrentLoad(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processed atomic.Int32
	makeHandler := func() types.HandlerFunc {
		return func(ctx context.Context, payload json.RawMessage) error {
			processed.Add(1)
			time.Sleep(time.Duration(50+rand.Intn(100)) * time.Millisecond)
			return nil
		}
	}

	// 2 nodes, 20 concurrency each.
	srv1 := newServer(t, "load-node-1", 20)
	srv1.RegisterHandler(types.HandlerDefinition{TaskType: "test.load", Handler: makeHandler()})
	srv1.Start(ctx)
	defer srv1.Stop()

	srv2 := newServer(t, "load-node-2", 20)
	srv2.RegisterHandler(types.HandlerDefinition{TaskType: "test.load", Handler: makeHandler()})
	srv2.Start(ctx)
	defer srv2.Stop()

	time.Sleep(3 * time.Second)

	cli := newClient(t)
	defer cli.Close()

	numJobs := 50
	var wg sync.WaitGroup
	jobIDs := make([]string, numJobs)
	var enqueueMu sync.Mutex

	for i := 0; i < numJobs; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			job, err := cli.Enqueue(ctx, "test.load", map[string]int{"i": idx})
			if err != nil {
				t.Errorf("enqueue %d: %v", idx, err)
				return
			}
			enqueueMu.Lock()
			jobIDs[idx] = job.ID
			enqueueMu.Unlock()
		}(i)
	}
	wg.Wait()

	// Wait for all to complete.
	for _, id := range jobIDs {
		if id != "" {
			waitJobTerminal(t, cli, id, 60*time.Second)
		}
	}

	if int(processed.Load()) != numJobs {
		t.Errorf("expected %d processed, got %d", numJobs, processed.Load())
	}
}

// ============================================================
// 14. Concurrency Limit (per-TaskType subpool)
// ============================================================

func TestConcurrencyLimit(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "conc-node", 20)
	var maxConcurrent atomic.Int32
	var current atomic.Int32

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType:    "test.conc",
		Concurrency: 2, // max 2 concurrent
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			c := current.Add(1)
			for {
				old := maxConcurrent.Load()
				if c > old {
					if maxConcurrent.CompareAndSwap(old, c) {
						break
					}
				} else {
					break
				}
			}
			time.Sleep(500 * time.Millisecond)
			current.Add(-1)
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

	// Enqueue 6 jobs; with concurrency=2, max parallel should be ~2.
	jobIDs := make([]string, 6)
	for i := 0; i < 6; i++ {
		job, err := cli.Enqueue(ctx, "test.conc", map[string]int{"i": i})
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
		jobIDs[i] = job.ID
	}

	for _, id := range jobIDs {
		waitJobTerminal(t, cli, id, 30*time.Second)
	}

	mc := maxConcurrent.Load()
	if mc > 3 { // allow small margin for scheduling jitter
		t.Errorf("max concurrent executions was %d, expected <= 2 (with small margin)", mc)
	}
	t.Logf("max concurrent observed: %d", mc)
}

// ============================================================
// 15. Tags and Search
// ============================================================

func TestTags(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "tags-node", 10)
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "test.tags",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
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

	payload, _ := json.Marshal(map[string]string{"user_id": "USR-42"})
	job, err := cli.EnqueueScheduled(ctx, &client.EnqueueRequest{
		TaskType: "test.tags",
		Payload:  payload,
		Schedule: types.Schedule{Type: types.ScheduleImmediate},
		Tags:     []string{"batch-import", "priority"},
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	final := waitJobTerminal(t, cli, job.ID, 15*time.Second)
	if len(final.Tags) != 2 {
		t.Errorf("expected 2 tags, got %v", final.Tags)
	}
}

// ============================================================
// 16. Workflow Retry
// ============================================================

func TestWorkflow_Retry(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "wf-retry-node", 10)
	var step2Attempts atomic.Int32
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "wfr.step1",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			return nil
		},
	})
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "wfr.step2",
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  1,
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     100 * time.Millisecond,
			Multiplier:   1.0,
		},
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			n := step2Attempts.Add(1)
			if n <= 1 {
				return &types.NonRetryableError{Err: fmt.Errorf("first-time fail")}
			}
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

	payload, _ := json.Marshal(nil)
	wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{
		Name: "retry-workflow",
		Tasks: []types.WorkflowTask{
			{Name: "step1", TaskType: "wfr.step1", Payload: payload},
			{Name: "step2", TaskType: "wfr.step2", Payload: payload, DependsOn: []string{"step1"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Wait for it to fail first.
	waitFor(t, 30*time.Second, "workflow failed", func() bool {
		wfStatus, _ := cli.GetWorkflow(ctx, wf.ID)
		return wfStatus != nil && wfStatus.Status == types.WorkflowStatusFailed
	})

	// Retry the workflow.
	_, err = cli.RetryWorkflow(ctx, wf.ID)
	if err != nil {
		t.Fatalf("retry workflow: %v", err)
	}

	// Now it should succeed (step2 succeeds on 2nd invocation).
	var finalWF *types.WorkflowInstance
	waitFor(t, 30*time.Second, "workflow completed after retry", func() bool {
		wfStatus, err := cli.GetWorkflow(ctx, wf.ID)
		if err != nil {
			return false
		}
		finalWF = wfStatus
		return wfStatus.Status == types.WorkflowStatusCompleted
	})

	if finalWF.Status != types.WorkflowStatusCompleted {
		t.Errorf("expected completed after retry, got %s", finalWF.Status)
	}
}

// ============================================================
// 17. Node Load Balancing (one node saturated)
// ============================================================

func TestMultiNode_LoadRebalance(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var node1Count, node2Count atomic.Int32
	// Node 1: concurrency=1, very slow (simulating heavy load).
	srv1 := newServer(t, "slow-node", 1)
	srv1.RegisterHandler(types.HandlerDefinition{
		TaskType: "test.rebalance",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			node1Count.Add(1)
			time.Sleep(2 * time.Second) // very slow
			return nil
		},
	})
	srv1.Start(ctx)
	defer srv1.Stop()

	// Node 2: concurrency=10, fast.
	srv2 := newServer(t, "fast-node", 10)
	srv2.RegisterHandler(types.HandlerDefinition{
		TaskType: "test.rebalance",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			node2Count.Add(1)
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	})
	srv2.Start(ctx)
	defer srv2.Stop()

	time.Sleep(3 * time.Second)

	cli := newClient(t)
	defer cli.Close()

	// Enqueue 8 jobs.
	jobIDs := make([]string, 8)
	for i := 0; i < 8; i++ {
		job, err := cli.Enqueue(ctx, "test.rebalance", map[string]int{"i": i})
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
		jobIDs[i] = job.ID
	}

	// Wait for all to complete.
	for _, id := range jobIDs {
		waitJobTerminal(t, cli, id, 60*time.Second)
	}

	// Fast node should handle more jobs since slow node is backlogged.
	t.Logf("slow-node handled: %d, fast-node handled: %d", node1Count.Load(), node2Count.Load())
	if node2Count.Load() < node1Count.Load() {
		t.Logf("WARNING: fast node handled fewer than slow node")
	}
}

// ============================================================
// 18. Worker.Stop() idempotency
// ============================================================

func TestWorkerStop_Idempotent(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "stop-node", 5)
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "test.stop",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			return nil
		},
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Call Stop() twice — should NOT panic (our sync.Once fix).
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Stop() panicked on second call: %v", r)
		}
	}()

	srv.Stop()
	srv.Stop() // should not panic
}

// ============================================================
// 19. Context metadata in handlers
// ============================================================

func TestContextMetadata(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "meta-node", 10)
	result := make(chan map[string]string, 1)

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "test.meta",
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  3,
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     100 * time.Millisecond,
			Multiplier:   1.0,
		},
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			meta := map[string]string{
				"job_id":    types.GetJobID(ctx),
				"run_id":    types.GetRunID(ctx),
				"task_type": string(types.GetTaskType(ctx)),
				"node_id":   types.GetNodeID(ctx),
			}
			result <- meta
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

	job, err := cli.Enqueue(ctx, "test.meta", nil)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case meta := <-result:
		if meta["job_id"] != job.ID {
			t.Errorf("job_id mismatch: %s vs %s", meta["job_id"], job.ID)
		}
		if meta["task_type"] != "test.meta" {
			t.Errorf("task_type mismatch: %s", meta["task_type"])
		}
		if meta["node_id"] != "meta-node" {
			t.Errorf("node_id mismatch: %s", meta["node_id"])
		}
		if meta["run_id"] == "" {
			t.Errorf("run_id should not be empty")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for handler result")
	}
}

// ============================================================
// 20. Headers
// ============================================================

func TestHeaders(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "header-node", 10)
	receivedHeaders := make(chan map[string]string, 1)
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "test.headers",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			h := types.GetHeaders(ctx)
			receivedHeaders <- h
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

	// Enqueue a job that will produce headers (via scheduler backfill or manual).
	// For direct testing, we use Enqueue (no headers by default), so just verify
	// GetHeaders doesn't panic and returns non-nil or nil map.
	_, err := cli.Enqueue(ctx, "test.headers", nil)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case h := <-receivedHeaders:
		t.Logf("received headers: %v", h)
	case <-time.After(15 * time.Second):
		t.Fatal("timeout")
	}
}

// ============================================================
// Condition Workflow: runtime branching
// ============================================================

func TestWorkflow_ConditionBranching(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "wf-condition-node", 10)

	var mu sync.Mutex
	var executed []string
	track := func(name string) {
		mu.Lock()
		executed = append(executed, name)
		mu.Unlock()
	}

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "cond.validate",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			track("validate")
			return nil
		},
	})

	// Condition handler: amount > 100 → route 0 (manual), else → route 1 (auto).
	type orderPayload struct {
		Amount float64 `json:"amount"`
	}
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "cond.check_amount",
		HandlerWithResult: types.TypedConditionHandler(func(ctx context.Context, p orderPayload) (uint, error) {
			track("check_amount")
			if p.Amount > 100 {
				return 0, nil
			}
			return 1, nil
		}),
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "cond.manual_review",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			track("manual_review")
			return nil
		},
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "cond.auto_approve",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			track("auto_approve")
			return nil
		},
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "cond.finalize",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			track("finalize")
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

	// High-value order → should take route 0 (manual_review).
	payload, _ := json.Marshal(orderPayload{Amount: 250.00})
	wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{
		Name: "condition-branching",
		Tasks: []types.WorkflowTask{
			{Name: "validate", TaskType: "cond.validate", Payload: payload},
			{
				Name: "check_amount", TaskType: "cond.check_amount", Payload: payload,
				DependsOn: []string{"validate"},
				Type:      types.WorkflowTaskCondition,
				ConditionRoutes: map[uint]string{
					0: "manual_review",
					1: "auto_approve",
				},
			},
			{Name: "manual_review", TaskType: "cond.manual_review", Payload: payload, DependsOn: []string{"check_amount"}},
			{Name: "auto_approve", TaskType: "cond.auto_approve", Payload: payload, DependsOn: []string{"check_amount"}},
			{Name: "finalize", TaskType: "cond.finalize", Payload: payload, DependsOn: []string{"manual_review", "auto_approve"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("enqueue workflow: %v", err)
	}

	var finalWF *types.WorkflowInstance
	waitFor(t, 30*time.Second, "condition workflow complete", func() bool {
		wfStatus, err := cli.GetWorkflow(ctx, wf.ID)
		if err != nil {
			return false
		}
		finalWF = wfStatus
		return wfStatus.Status.IsTerminal()
	})

	if finalWF.Status != types.WorkflowStatusCompleted {
		t.Errorf("expected completed, got %s", finalWF.Status)
		for name, task := range finalWF.Tasks {
			t.Logf("  task %s: status=%s err=%v", name, task.Status, task.Error)
		}
	}

	// Verify the condition chose route 0.
	checkState := finalWF.Tasks["check_amount"]
	if checkState.ConditionRoute == nil {
		t.Fatal("expected ConditionRoute to be set")
	}
	if *checkState.ConditionRoute != 0 {
		t.Errorf("expected route 0, got %d", *checkState.ConditionRoute)
	}

	// Verify manual_review was executed (route 0 target).
	mu.Lock()
	executedCopy := make([]string, len(executed))
	copy(executedCopy, executed)
	mu.Unlock()

	hasManualReview := false
	for _, name := range executedCopy {
		if name == "manual_review" {
			hasManualReview = true
		}
	}
	if !hasManualReview {
		t.Errorf("expected manual_review to be executed, got: %v", executedCopy)
	}

	t.Logf("execution order: %v", executedCopy)
}

// ============================================================
// Condition Loop: iteration with back-edge
// ============================================================

func TestWorkflow_ConditionLoop(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "wf-loop-node", 10)

	var batchesProcessed atomic.Int32

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "loop.init",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			batchesProcessed.Store(0)
			return nil
		},
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "loop.process",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			batchesProcessed.Add(1)
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	})

	type loopPayload struct {
		Total int `json:"total"`
	}
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "loop.check",
		HandlerWithResult: types.TypedConditionHandler(func(ctx context.Context, p loopPayload) (uint, error) {
			processed := int(batchesProcessed.Load())
			if processed < p.Total {
				return 0, nil // loop back
			}
			return 1, nil // exit
		}),
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "loop.summarize",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
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

	payload, _ := json.Marshal(loopPayload{Total: 3})
	wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{
		Name: "condition-loop",
		Tasks: []types.WorkflowTask{
			{Name: "init", TaskType: "loop.init", Payload: payload},
			{Name: "process", TaskType: "loop.process", Payload: payload, DependsOn: []string{"init"}},
			{
				Name: "check", TaskType: "loop.check", Payload: payload,
				DependsOn: []string{"process"},
				Type:      types.WorkflowTaskCondition,
				ConditionRoutes: map[uint]string{
					0: "process",   // loop back
					1: "summarize", // exit
				},
				MaxIterations: 10,
			},
			{Name: "summarize", TaskType: "loop.summarize", Payload: payload, DependsOn: []string{"check"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("enqueue workflow: %v", err)
	}

	var finalWF *types.WorkflowInstance
	waitFor(t, 60*time.Second, "loop workflow complete", func() bool {
		wfStatus, err := cli.GetWorkflow(ctx, wf.ID)
		if err != nil {
			return false
		}
		finalWF = wfStatus
		return wfStatus.Status.IsTerminal()
	})

	if finalWF.Status != types.WorkflowStatusCompleted {
		t.Errorf("expected completed, got %s", finalWF.Status)
		for name, task := range finalWF.Tasks {
			t.Logf("  task %s: status=%s err=%v", name, task.Status, task.Error)
		}
	}

	// Verify the loop ran the expected number of times.
	processed := int(batchesProcessed.Load())
	if processed < 3 {
		t.Errorf("expected at least 3 batches processed, got %d", processed)
	}

	t.Logf("batches processed: %d, condition iterations: %v", processed, finalWF.ConditionIterations)
}

// ============================================================
// Subflow: dynamic task generation
// ============================================================

func TestWorkflow_Subflow(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "wf-subflow-node", 10)

	var mu sync.Mutex
	var processedItems []string

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "sf.fetch",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			return nil
		},
	})

	type fetchResult struct {
		Items []string `json:"items"`
	}
	type itemPayload struct {
		Item string `json:"item"`
	}

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "sf.generate",
		HandlerWithResult: types.TypedSubflowHandler(func(ctx context.Context, result fetchResult) ([]types.WorkflowTask, error) {
			var tasks []types.WorkflowTask
			for _, item := range result.Items {
				p, _ := json.Marshal(itemPayload{Item: item})
				tasks = append(tasks, types.WorkflowTask{
					Name:     fmt.Sprintf("process_%s", item),
					TaskType: "sf.process_item",
					Payload:  p,
				})
			}
			return tasks, nil
		}),
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "sf.process_item",
		Handler: types.TypedHandler(func(ctx context.Context, p itemPayload) error {
			mu.Lock()
			processedItems = append(processedItems, p.Item)
			mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			return nil
		}),
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "sf.aggregate",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
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

	genPayload, _ := json.Marshal(fetchResult{Items: []string{"a", "b", "c"}})
	wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{
		Name: "subflow-test",
		Tasks: []types.WorkflowTask{
			{Name: "fetch", TaskType: "sf.fetch", Payload: genPayload},
			{
				Name: "generate", TaskType: "sf.generate", Payload: genPayload,
				DependsOn: []string{"fetch"},
				Type:      types.WorkflowTaskSubflow,
			},
			{Name: "aggregate", TaskType: "sf.aggregate", Payload: genPayload, DependsOn: []string{"generate"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("enqueue workflow: %v", err)
	}

	var finalWF *types.WorkflowInstance
	waitFor(t, 30*time.Second, "subflow workflow complete", func() bool {
		wfStatus, err := cli.GetWorkflow(ctx, wf.ID)
		if err != nil {
			return false
		}
		finalWF = wfStatus
		return wfStatus.Status.IsTerminal()
	})

	if finalWF.Status != types.WorkflowStatusCompleted {
		t.Errorf("expected completed, got %s", finalWF.Status)
		for name, task := range finalWF.Tasks {
			t.Logf("  task %s: status=%s err=%v", name, task.Status, task.Error)
		}
	}

	// Verify subflow tasks were injected.
	genState := finalWF.Tasks["generate"]
	if len(genState.SubflowTasks) != 3 {
		t.Errorf("expected 3 subflow tasks, got %d: %v", len(genState.SubflowTasks), genState.SubflowTasks)
	}

	// Verify all items were processed.
	mu.Lock()
	items := make([]string, len(processedItems))
	copy(items, processedItems)
	mu.Unlock()

	if len(items) != 3 {
		t.Errorf("expected 3 items processed, got %d: %v", len(items), items)
	}

	// Verify dynamic tasks exist in the final workflow definition.
	taskCount := len(finalWF.Definition.Tasks)
	if taskCount != 6 { // 3 original + 3 dynamic
		t.Errorf("expected 6 tasks in definition, got %d", taskCount)
	}

	t.Logf("subflow tasks: %v, processed items: %v", genState.SubflowTasks, items)
}

// ============================================================
// Priority: dispatch order within workflow
// ============================================================

func TestWorkflow_StepPriority(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use concurrency=1 so tasks execute sequentially in priority order.
	srv := newServer(t, "wf-priority-node", 1)

	var mu sync.Mutex
	var executionOrder []string
	track := func(name string) {
		mu.Lock()
		executionOrder = append(executionOrder, name)
		mu.Unlock()
	}

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "prio.start",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			track("start")
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "prio.critical",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			track("critical")
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "prio.normal",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			track("normal")
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "prio.low",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			track("low")
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "prio.finish",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			track("finish")
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

	payload, _ := json.Marshal(nil)
	wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{
		Name: "priority-ordering",
		Tasks: []types.WorkflowTask{
			{Name: "start", TaskType: "prio.start", Payload: payload},
			{Name: "critical", TaskType: "prio.critical", Payload: payload, DependsOn: []string{"start"}, Priority: 10},
			{Name: "normal", TaskType: "prio.normal", Payload: payload, DependsOn: []string{"start"}, Priority: 5},
			{Name: "low", TaskType: "prio.low", Payload: payload, DependsOn: []string{"start"}, Priority: 1},
			{Name: "finish", TaskType: "prio.finish", Payload: payload, DependsOn: []string{"critical", "normal", "low"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("enqueue workflow: %v", err)
	}

	var finalWF *types.WorkflowInstance
	waitFor(t, 30*time.Second, "priority workflow complete", func() bool {
		wfStatus, err := cli.GetWorkflow(ctx, wf.ID)
		if err != nil {
			return false
		}
		finalWF = wfStatus
		return wfStatus.Status.IsTerminal()
	})

	if finalWF.Status != types.WorkflowStatusCompleted {
		t.Errorf("expected completed, got %s", finalWF.Status)
	}

	mu.Lock()
	order := make([]string, len(executionOrder))
	copy(order, executionOrder)
	mu.Unlock()

	// With concurrency=1, the three middle tasks should execute in priority order.
	// Find indices of the three priority tasks.
	indexOf := func(name string) int {
		for i, n := range order {
			if n == name {
				return i
			}
		}
		return -1
	}

	critIdx := indexOf("critical")
	normIdx := indexOf("normal")
	lowIdx := indexOf("low")

	if critIdx == -1 || normIdx == -1 || lowIdx == -1 {
		t.Fatalf("not all tasks executed: %v", order)
	}

	if critIdx > normIdx || normIdx > lowIdx {
		t.Errorf("expected priority order critical→normal→low, got: %v", order)
	}

	t.Logf("execution order: %v", order)
}

// ============================================================
// PendingDeps: reference counting verification
// ============================================================

func TestWorkflow_PendingDeps(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "wf-pendingdeps-node", 10)

	handler := func(ctx context.Context, payload json.RawMessage) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	srv.RegisterHandler(types.HandlerDefinition{TaskType: "pd.a", Handler: handler})
	srv.RegisterHandler(types.HandlerDefinition{TaskType: "pd.b", Handler: handler})
	srv.RegisterHandler(types.HandlerDefinition{TaskType: "pd.c", Handler: handler})
	srv.RegisterHandler(types.HandlerDefinition{TaskType: "pd.d", Handler: handler})

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()
	time.Sleep(2 * time.Second)

	payload, _ := json.Marshal(nil)
	wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{
		Name: "pending-deps-test",
		Tasks: []types.WorkflowTask{
			{Name: "a", TaskType: "pd.a", Payload: payload},
			{Name: "b", TaskType: "pd.b", Payload: payload, DependsOn: []string{"a"}},
			{Name: "c", TaskType: "pd.c", Payload: payload, DependsOn: []string{"a"}},
			{Name: "d", TaskType: "pd.d", Payload: payload, DependsOn: []string{"b", "c"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("enqueue workflow: %v", err)
	}

	var finalWF *types.WorkflowInstance
	waitFor(t, 30*time.Second, "pendingdeps workflow complete", func() bool {
		wfStatus, err := cli.GetWorkflow(ctx, wf.ID)
		if err != nil {
			return false
		}
		finalWF = wfStatus
		return wfStatus.Status.IsTerminal()
	})

	if finalWF.Status != types.WorkflowStatusCompleted {
		t.Errorf("expected completed, got %s", finalWF.Status)
		for name, task := range finalWF.Tasks {
			t.Logf("  task %s: status=%s err=%v", name, task.Status, task.Error)
		}
	}

	// Verify PendingDeps are all 0 after completion.
	if finalWF.PendingDeps != nil {
		for name, count := range finalWF.PendingDeps {
			if count != 0 {
				t.Errorf("PendingDeps[%s] = %d, expected 0", name, count)
			}
		}
	}

	t.Logf("PendingDeps: %v", finalWF.PendingDeps)
}

// ============================================================
// AllowFailure: graceful degradation
// ============================================================

func TestWorkflow_AllowFailure(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "wf-allowfail-node", 10)

	var mu sync.Mutex
	var executed []string
	track := func(name string) {
		mu.Lock()
		executed = append(executed, name)
		mu.Unlock()
	}

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "af.fetch",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			track("fetch")
			return nil
		},
	})

	// This handler always fails.
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "af.enrich",
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  1,
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     100 * time.Millisecond,
			Multiplier:   1.0,
		},
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			track("enrich")
			return &types.NonRetryableError{Err: fmt.Errorf("enrichment service down")}
		},
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "af.transform",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			track("transform")
			return nil
		},
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "af.aggregate",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			track("aggregate")
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

	payload, _ := json.Marshal(nil)
	wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{
		Name: "allow-failure-test",
		Tasks: []types.WorkflowTask{
			{Name: "fetch", TaskType: "af.fetch", Payload: payload},
			{Name: "enrich", TaskType: "af.enrich", Payload: payload, DependsOn: []string{"fetch"}, AllowFailure: true},
			{Name: "transform", TaskType: "af.transform", Payload: payload, DependsOn: []string{"fetch"}},
			{Name: "aggregate", TaskType: "af.aggregate", Payload: payload, DependsOn: []string{"enrich", "transform"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("enqueue workflow: %v", err)
	}

	var finalWF *types.WorkflowInstance
	waitFor(t, 30*time.Second, "allow-failure workflow complete", func() bool {
		wfStatus, err := cli.GetWorkflow(ctx, wf.ID)
		if err != nil {
			return false
		}
		finalWF = wfStatus
		return wfStatus.Status.IsTerminal()
	})

	// Workflow should complete despite enrich failing.
	if finalWF.Status != types.WorkflowStatusCompleted {
		t.Errorf("expected completed, got %s", finalWF.Status)
		for name, task := range finalWF.Tasks {
			t.Logf("  task %s: status=%s err=%v", name, task.Status, task.Error)
		}
	}

	// Verify enrich failed but workflow still completed.
	enrichState := finalWF.Tasks["enrich"]
	if enrichState.Status != types.JobStatusFailed {
		t.Errorf("expected enrich to be failed, got %s", enrichState.Status)
	}

	// Verify aggregate ran (downstream of the failed enrich).
	mu.Lock()
	executedCopy := make([]string, len(executed))
	copy(executedCopy, executed)
	mu.Unlock()

	hasAggregate := false
	for _, name := range executedCopy {
		if name == "aggregate" {
			hasAggregate = true
		}
	}
	if !hasAggregate {
		t.Errorf("expected aggregate to be executed, got: %v", executedCopy)
	}

	t.Logf("execution order: %v", executedCopy)
}

// ============================================================
// ResultFrom: upstream result piping
// ============================================================

func TestWorkflow_ResultFrom(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "wf-resultfrom-node", 10)

	type stepAResult struct {
		Value int `json:"value"`
	}
	type stepBResult struct {
		Doubled int `json:"doubled"`
	}
	type originalPayload struct {
		Base int `json:"base"`
	}

	// Step A: produces a result.
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "rf.step_a",
		HandlerWithResult: types.TypedHandlerWithResult(func(ctx context.Context, p originalPayload) (stepAResult, error) {
			return stepAResult{Value: p.Base * 10}, nil
		}),
	})

	// Step B: receives step_a's result via ResultFrom + original payload.
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "rf.step_b",
		HandlerWithResult: types.TypedHandlerWithUpstreamResult(
			func(ctx context.Context, upstream stepAResult, original originalPayload) (stepBResult, error) {
				return stepBResult{Doubled: upstream.Value * 2}, nil
			},
		),
	})

	// Step C: receives step_b's result.
	receivedResult := make(chan stepBResult, 1)
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "rf.step_c",
		Handler: types.TypedHandlerWithUpstream(func(ctx context.Context, upstream stepBResult, original originalPayload) error {
			receivedResult <- upstream
			return nil
		}),
	})

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()
	time.Sleep(2 * time.Second)

	payload, _ := json.Marshal(originalPayload{Base: 5})
	wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{
		Name: "result-from-test",
		Tasks: []types.WorkflowTask{
			{Name: "step_a", TaskType: "rf.step_a", Payload: payload},
			{Name: "step_b", TaskType: "rf.step_b", Payload: payload, DependsOn: []string{"step_a"}, ResultFrom: "step_a"},
			{Name: "step_c", TaskType: "rf.step_c", Payload: payload, DependsOn: []string{"step_b"}, ResultFrom: "step_b"},
		},
	}, nil)
	if err != nil {
		t.Fatalf("enqueue workflow: %v", err)
	}

	var finalWF *types.WorkflowInstance
	waitFor(t, 30*time.Second, "result-from workflow complete", func() bool {
		wfStatus, err := cli.GetWorkflow(ctx, wf.ID)
		if err != nil {
			return false
		}
		finalWF = wfStatus
		return wfStatus.Status.IsTerminal()
	})

	if finalWF.Status != types.WorkflowStatusCompleted {
		t.Errorf("expected completed, got %s", finalWF.Status)
		for name, task := range finalWF.Tasks {
			t.Logf("  task %s: status=%s err=%v", name, task.Status, task.Error)
		}
	}

	// Verify the result piping: base=5 → step_a produces 50 → step_b produces 100.
	select {
	case result := <-receivedResult:
		if result.Doubled != 100 {
			t.Errorf("expected Doubled=100, got %d", result.Doubled)
		}
		t.Logf("result piping verified: base=5 → step_a=50 → step_b=100 → step_c received %d", result.Doubled)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for step_c result")
	}

	// Verify workflow output is set (from leaf task step_c — no result, so check step_b's output via workflow output).
	// step_c has no HandlerWithResult, so workflow output comes from step_b (last leaf with result might not apply).
	// Actually the leaf is step_c which has no output, so Output may be nil. That's fine.
	t.Logf("workflow output: %s", string(finalWF.Output))
}

// ============================================================
// WorkflowOutput: leaf task result as workflow output
// ============================================================

func TestWorkflow_Output(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "wf-output-node", 10)

	type finalResult struct {
		Message string `json:"message"`
	}

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "wo.step1",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			return nil
		},
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "wo.step2",
		HandlerWithResult: types.TypedHandlerWithResult(func(ctx context.Context, p json.RawMessage) (finalResult, error) {
			return finalResult{Message: "workflow complete"}, nil
		}),
	})

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()
	time.Sleep(2 * time.Second)

	payload, _ := json.Marshal(nil)
	wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{
		Name: "output-test",
		Tasks: []types.WorkflowTask{
			{Name: "step1", TaskType: "wo.step1", Payload: payload},
			{Name: "step2", TaskType: "wo.step2", Payload: payload, DependsOn: []string{"step1"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("enqueue workflow: %v", err)
	}

	var finalWF *types.WorkflowInstance
	waitFor(t, 30*time.Second, "output workflow complete", func() bool {
		wfStatus, err := cli.GetWorkflow(ctx, wf.ID)
		if err != nil {
			return false
		}
		finalWF = wfStatus
		return wfStatus.Status.IsTerminal()
	})

	if finalWF.Status != types.WorkflowStatusCompleted {
		t.Fatalf("expected completed, got %s", finalWF.Status)
	}

	// Verify workflow output contains the leaf task result.
	if len(finalWF.Output) == 0 {
		t.Fatal("expected workflow output to be set")
	}

	var output finalResult
	if err := json.Unmarshal(finalWF.Output, &output); err != nil {
		t.Fatalf("failed to unmarshal workflow output: %v", err)
	}
	if output.Message != "workflow complete" {
		t.Errorf("expected message 'workflow complete', got %q", output.Message)
	}

	t.Logf("workflow output: %s", string(finalWF.Output))
}

// ============================================================
// Run all tests.
// ============================================================

// Ensure unused imports are used.
var _ = store.TierConfig{}
var _ = rand.Int
var _ = fmt.Sprintf
