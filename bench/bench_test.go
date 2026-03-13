// Package bench provides performance benchmarks for dureq.
//
// These benchmarks require a running Redis instance.
// Standalone: REDIS_URL=redis://localhost:6381 REDIS_DB=15 go test -bench=. -benchmem ./bench/
// Cluster:    go test -bench=. -benchmem ./bench/
package bench

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
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/redis/rueidis"
)

const (
	redisPW   = "your-password"
	keyPrefix = "bench"
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

func flushDB(b *testing.B) {
	b.Helper()

	var opt rueidis.ClientOption
	if isStandalone() {
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
		b.Fatalf("connect redis for flush: %v", err)
	}
	defer rdb.Close()

	ctx := context.Background()
	if isStandalone() {
		rdb.Do(ctx, rdb.B().Flushdb().Build())
	} else {
		for _, node := range rdb.Nodes() {
			node.Do(ctx, node.B().Flushall().Build())
		}
	}
}

func newServer(b *testing.B, nodeID string, concurrency int) *server.Server {
	b.Helper()

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
		b.Fatalf("create server: %v", err)
	}
	return srv
}

func newClient(b *testing.B) *client.Client {
	b.Helper()

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
		b.Fatalf("create client: %v", err)
	}
	return cli
}

func newRedisStore(b *testing.B) *store.RedisStore {
	b.Helper()

	var opt rueidis.ClientOption
	if isStandalone() {
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
		b.Fatalf("connect redis: %v", err)
	}
	b.Cleanup(func() { rdb.Close() })

	s, err := store.NewRedisStore(rdb, store.RedisStoreConfig{KeyPrefix: keyPrefix}, nil)
	if err != nil {
		b.Fatalf("create store: %v", err)
	}
	return s
}

// --- Benchmarks ---

// BenchmarkEnqueue measures raw enqueue throughput (client → Redis).
func BenchmarkEnqueue(b *testing.B) {
	flushDB(b)
	cli := newClient(b)
	defer cli.Close()
	ctx := context.Background()

	payload := json.RawMessage(`{"key":"value","number":42}`)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := cli.Enqueue(ctx, "bench.task", payload)
			if err != nil {
				b.Fatalf("enqueue: %v", err)
			}
		}
	})
}

// BenchmarkEnqueueSerial measures single-goroutine enqueue latency.
func BenchmarkEnqueueSerial(b *testing.B) {
	flushDB(b)
	cli := newClient(b)
	defer cli.Close()
	ctx := context.Background()

	payload := json.RawMessage(`{"key":"value"}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := cli.Enqueue(ctx, "bench.task", payload)
		if err != nil {
			b.Fatalf("enqueue: %v", err)
		}
	}
}

// BenchmarkDispatchLatency measures end-to-end latency from enqueue to handler start.
func BenchmarkDispatchLatency(b *testing.B) {
	flushDB(b)

	var latencies []time.Duration
	var mu sync.Mutex
	var processed int64

	srv := newServer(b, "bench-node-1", 200)
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "bench.latency",
		Handler: types.HandlerFunc(func(ctx context.Context, payload json.RawMessage) error {
			// Decode the enqueue timestamp from payload to measure latency.
			var p struct{ T int64 `json:"t"` }
			json.Unmarshal(payload, &p)
			lat := time.Since(time.Unix(0, p.T))
			mu.Lock()
			latencies = append(latencies, lat)
			mu.Unlock()
			atomic.AddInt64(&processed, 1)
			return nil
		}),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		b.Fatalf("start server: %v", err)
	}
	defer srv.Stop()

	cli := newClient(b)
	defer cli.Close()

	// Warm up.
	time.Sleep(500 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload := json.RawMessage(fmt.Sprintf(`{"t":%d}`, time.Now().UnixNano()))
		if _, err := cli.Enqueue(ctx, "bench.latency", payload); err != nil {
			b.Fatalf("enqueue: %v", err)
		}
	}

	// Wait for all to be processed.
	deadline := time.Now().Add(30 * time.Second)
	for atomic.LoadInt64(&processed) < int64(b.N) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	b.StopTimer()

	mu.Lock()
	defer mu.Unlock()
	if len(latencies) > 0 {
		var total time.Duration
		for _, l := range latencies {
			total += l
		}
		avg := total / time.Duration(len(latencies))
		b.ReportMetric(float64(avg.Microseconds()), "avg_latency_us")

		// Report p50, p95, p99 if enough samples.
		if len(latencies) >= 10 {
			sorted := make([]time.Duration, len(latencies))
			copy(sorted, latencies)
			sortDurations(sorted)
			b.ReportMetric(float64(sorted[len(sorted)*50/100].Microseconds()), "p50_us")
			b.ReportMetric(float64(sorted[len(sorted)*95/100].Microseconds()), "p95_us")
			b.ReportMetric(float64(sorted[len(sorted)*99/100].Microseconds()), "p99_us")
		}
	}
}

// BenchmarkThroughput measures how many jobs/sec a single node can process.
func BenchmarkThroughput(b *testing.B) {
	for _, workers := range []int{10, 50, 100} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			flushDB(b)

			var processed int64
			srv := newServer(b, "bench-throughput", workers)
			srv.RegisterHandler(types.HandlerDefinition{
				TaskType: "bench.noop",
				Handler: types.HandlerFunc(func(ctx context.Context, payload json.RawMessage) error {
					atomic.AddInt64(&processed, 1)
					return nil
				}),
			})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := srv.Start(ctx); err != nil {
				b.Fatalf("start: %v", err)
			}
			defer srv.Stop()

			cli := newClient(b)
			defer cli.Close()

			// Pre-enqueue all jobs.
			payload := json.RawMessage(`{}`)
			for i := 0; i < b.N; i++ {
				if _, err := cli.Enqueue(ctx, "bench.noop", payload); err != nil {
					b.Fatalf("enqueue: %v", err)
				}
			}

			b.ResetTimer()
			start := time.Now()

			// Wait for all processed.
			deadline := time.Now().Add(60 * time.Second)
			for atomic.LoadInt64(&processed) < int64(b.N) && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			elapsed := time.Since(start)
			b.StopTimer()

			count := atomic.LoadInt64(&processed)
			if count > 0 {
				b.ReportMetric(float64(count)/elapsed.Seconds(), "jobs/sec")
			}
		})
	}
}

// BenchmarkWorkflowDAG measures throughput of a simple 5-task linear workflow.
func BenchmarkWorkflowDAG(b *testing.B) {
	flushDB(b)

	var completed int64
	srv := newServer(b, "bench-wf", 100)

	for i := 1; i <= 5; i++ {
		taskName := fmt.Sprintf("step%d", i)
		srv.RegisterHandler(types.HandlerDefinition{
			TaskType: types.TaskType(taskName),
			Handler: types.HandlerFunc(func(ctx context.Context, payload json.RawMessage) error {
				return nil
			}),
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		b.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(b)
	defer cli.Close()

	// Build a linear 5-step workflow definition.
	def := types.WorkflowDefinition{
		Name: "bench-linear",
		Tasks: []types.WorkflowTask{
			{Name: "step1", TaskType: "step1"},
			{Name: "step2", TaskType: "step2", DependsOn: []string{"step1"}},
			{Name: "step3", TaskType: "step3", DependsOn: []string{"step2"}},
			{Name: "step4", TaskType: "step4", DependsOn: []string{"step3"}},
			{Name: "step5", TaskType: "step5", DependsOn: []string{"step4"}},
		},
	}

	time.Sleep(500 * time.Millisecond) // let leader election settle

	b.ResetTimer()
	start := time.Now()

	for i := 0; i < b.N; i++ {
		wf, err := cli.EnqueueWorkflow(ctx, def, nil)
		if err != nil {
			b.Fatalf("enqueue workflow: %v", err)
		}
		// Poll for completion.
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			w, err := cli.GetWorkflow(ctx, wf.ID)
			if err == nil && w.Status.IsTerminal() {
				atomic.AddInt64(&completed, 1)
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	elapsed := time.Since(start)
	b.StopTimer()

	count := atomic.LoadInt64(&completed)
	if count > 0 {
		b.ReportMetric(float64(count)/elapsed.Seconds(), "workflows/sec")
	}
}

// BenchmarkStoreAlloc measures memory allocations for core store operations.
func BenchmarkStoreAlloc(b *testing.B) {
	flushDB(b)
	s := newRedisStore(b)
	ctx := context.Background()

	if err := s.EnsureStreams(ctx); err != nil {
		b.Fatalf("ensure streams: %v", err)
	}

	b.Run("SaveJob", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			job := &types.Job{
				ID:       fmt.Sprintf("bench-job-%d", i),
				TaskType: "bench.task",
				Status:   types.JobStatusPending,
				Payload:  json.RawMessage(`{"x":1}`),
			}
			if _, err := s.SaveJob(ctx, job); err != nil {
				b.Fatalf("save job: %v", err)
			}
		}
	})

	b.Run("GetJob", func(b *testing.B) {
		// Pre-create a job to read.
		job := &types.Job{
			ID:       "bench-get-target",
			TaskType: "bench.task",
			Status:   types.JobStatusPending,
			Payload:  json.RawMessage(`{"x":1}`),
		}
		s.SaveJob(ctx, job)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := s.GetJob(ctx, "bench-get-target")
			if err != nil {
				b.Fatalf("get job: %v", err)
			}
		}
	})
}

// --- Helpers ---

func sortDurations(ds []time.Duration) {
	// Simple insertion sort — good enough for benchmark sample sizes.
	for i := 1; i < len(ds); i++ {
		key := ds[i]
		j := i - 1
		for j >= 0 && ds[j] > key {
			ds[j+1] = ds[j]
			j--
		}
		ds[j+1] = key
	}
}
