package bench

// How fast a backlog is worked off. The benchmarks in bench_test.go enqueue one job after the
// other and have it executed in between, so that a node is never busy; these have all of a
// node's slots in use. bench_test.go has been the same since v0.2.0, and what is used here has
// been there since, so both can be copied into an older release to compare:
//
//	REDIS_URL=redis://localhost:6381 REDIS_DB=15 go test -run '^$' -bench Backlog ./bench/

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/client"
	"github.com/FDK0901/dureq/internal/server"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
)

const (
	backlogJobs    = 2000
	backlogTimeout = 30 * time.Second
)

// backlog is a server whose handlers wait until the gate is opened, so that jobs pile up.
type backlog struct {
	cli  *client.Client
	done atomic.Int64
	want atomic.Int64
	all  chan struct{} // is sent to when as many are done as wanted

	mu   sync.Mutex
	gate chan struct{}
}

// newBacklog starts a server with the default tier, or others.
func newBacklog(b *testing.B, workers int, tiers []store.TierConfig, register func(bl *backlog, srv *server.Server)) *backlog {
	b.Helper()
	flushDB(b)
	bl := &backlog{all: make(chan struct{}, 1), gate: make(chan struct{})}

	opts := []server.Option{
		server.WithNodeID("bench-backlog"), server.WithMaxConcurrency(workers), server.WithKeyPrefix(keyPrefix),
		server.WithSchedulerTickInterval(50 * time.Millisecond), server.WithShutdownTimeout(time.Second),
		server.WithLogger(chainedslog.NewSlog(slog.New(slog.DiscardHandler))),
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
		b.Fatalf("create server: %v", err)
	}
	register(bl, srv)
	if err := srv.Start(context.Background()); err != nil {
		b.Fatalf("start: %v", err)
	}
	b.Cleanup(func() {
		bl.open() // handlers that still wait
		srv.Stop()
	})
	if bl.cli, err = client.New(copts...); err != nil {
		b.Fatalf("create client: %v", err)
	}
	b.Cleanup(bl.cli.Close)
	time.Sleep(1500 * time.Millisecond) // until there is a leader
	return bl
}

// wait returns when the gate is open.
func (bl *backlog) wait() {
	bl.mu.Lock()
	gate := bl.gate
	bl.mu.Unlock()
	<-gate
}

func (bl *backlog) open() {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	select {
	case <-bl.gate:
	default:
		close(bl.gate)
	}
}

func (bl *backlog) shut() {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	select {
	case <-bl.gate:
		bl.gate = make(chan struct{})
	default:
	}
}

// count counts an execution.
func (bl *backlog) count() {
	if bl.done.Add(1) == bl.want.Load() {
		bl.all <- struct{}{}
	}
}

// handler waits for the gate, works, and counts.
func (bl *backlog) handler(taskType types.TaskType, takes time.Duration) types.HandlerDefinition {
	return types.HandlerDefinition{TaskType: taskType, Handler: func(context.Context, json.RawMessage) error {
		bl.wait()
		if takes > 0 {
			time.Sleep(takes)
		}
		bl.count()
		return nil
	}}
}

// enqueue enqueues a job with a priority from 1 to 10.
func (bl *backlog) enqueue(taskType types.TaskType, priority types.Priority) error {
	_, err := bl.cli.EnqueueScheduled(context.Background(), &client.EnqueueRequest{
		TaskType: taskType, Payload: json.RawMessage(`{}`), Priority: &priority, Schedule: types.Schedule{Type: types.ScheduleImmediate},
	})
	return err
}

// run has fill build up a backlog, opens the gate, and measures until want executions have been
// counted. It is called for each b.N, and reports executions a second.
func (bl *backlog) run(b *testing.B, want int, fill func(i int) error, n int, after func()) {
	b.Helper()
	var executions int64
	var elapsed time.Duration
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		bl.shut()
		bl.done.Store(0)
		bl.want.Store(int64(want))
		inParallel(b, n, fill)

		b.StartTimer()
		start := time.Now()
		bl.open()
		finished := true
		select {
		case <-bl.all:
		case <-time.After(backlogTimeout):
			finished = false
		}
		elapsed += time.Since(start)
		b.StopTimer()
		executions += min(bl.done.Load(), int64(want))
		if !finished {
			b.Logf("only %d of %d executions within %v", bl.done.Load(), want, backlogTimeout)
			break // what is left would count for the next one
		}
		if after != nil {
			after()
		}
	}
	b.ReportMetric(float64(executions)/elapsed.Seconds(), "jobs/sec")
}

func inParallel(b *testing.B, n int, f func(i int) error) {
	b.Helper()
	const workers = 20
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := w; i < n; i += workers {
				if err := f(i); err != nil {
					b.Errorf("enqueue: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if b.Failed() {
		b.FailNow()
	}
}

// BenchmarkBacklog measures jobs that do nothing, and jobs that take a while.
func BenchmarkBacklog(b *testing.B) {
	for _, bc := range []struct {
		name    string
		workers int
		takes   time.Duration
	}{
		{"workers=10", 10, 0},
		{"workers=100", 100, 0},
		{"workers=100,takes=10ms", 100, 10 * time.Millisecond},
	} {
		b.Run(bc.name, func(b *testing.B) {
			bl := newBacklog(b, bc.workers, nil, func(bl *backlog, srv *server.Server) {
				srv.RegisterHandler(bl.handler("bench.job", bc.takes))
			})
			bl.run(b, backlogJobs, func(int) error { return bl.enqueue("bench.job", types.PriorityNormal) }, backlogJobs, nil)
		})
	}
}

// BenchmarkBacklogTiers measures jobs of the lowest of three tiers, the others of which are
// empty.
func BenchmarkBacklogTiers(b *testing.B) {
	tiers := []store.TierConfig{{Name: "high", Weight: 100}, {Name: "normal", Weight: 50}, {Name: "low", Weight: 10}}
	bl := newBacklog(b, 100, tiers, func(bl *backlog, srv *server.Server) {
		srv.RegisterHandler(bl.handler("bench.job", 0))
	})
	bl.run(b, backlogJobs, func(int) error { return bl.enqueue("bench.job", 1) }, backlogJobs, nil)
}

// BenchmarkBacklogFullHandler measures jobs among which there are others, of a handler that has
// reached its limit and is not done with any until the end.
func BenchmarkBacklogFullHandler(b *testing.B) {
	const full = 100
	var release atomic.Pointer[chan struct{}]
	var stuck atomic.Int64
	hold := func() { ch := make(chan struct{}); release.Store(&ch) }
	hold()
	bl := newBacklog(b, 100, nil, func(bl *backlog, srv *server.Server) {
		srv.RegisterHandler(bl.handler("bench.job", 0))
		srv.RegisterHandler(types.HandlerDefinition{TaskType: "bench.full", Concurrency: 2, Handler: func(context.Context, json.RawMessage) error {
			<-*release.Load()
			stuck.Add(1)
			return nil
		}})
	})
	b.Cleanup(func() {
		select {
		case <-*release.Load():
		default:
			close(*release.Load())
		}
	})
	const every = backlogJobs / full
	fill := func(i int) error {
		if i%(every+1) == 0 {
			return bl.enqueue("bench.full", types.PriorityNormal)
		}
		return bl.enqueue("bench.job", types.PriorityNormal)
	}
	total := backlogJobs + full
	stuckJobs := (total + every) / (every + 1)
	bl.run(b, total-stuckJobs, fill, total, func() {
		close(*release.Load())
		for deadline := time.Now().Add(backlogTimeout); stuck.Load() < int64(stuckJobs) && time.Now().Before(deadline); {
			time.Sleep(10 * time.Millisecond)
		}
		stuck.Store(0)
		hold()
	})
}

// BenchmarkBacklogRetries measures jobs that fail once and succeed when they are retried after
// 100 ms.
func BenchmarkBacklogRetries(b *testing.B) {
	bl := newBacklog(b, 100, nil, func(bl *backlog, srv *server.Server) {
		srv.RegisterHandler(types.HandlerDefinition{
			TaskType:    "bench.retry",
			RetryPolicy: &types.RetryPolicy{MaxAttempts: 3, InitialDelay: 100 * time.Millisecond, MaxDelay: 100 * time.Millisecond, Multiplier: 1},
			Handler: func(ctx context.Context, _ json.RawMessage) error {
				bl.wait()
				if types.GetAttempt(ctx) == 0 {
					return errors.New("again")
				}
				bl.count()
				return nil
			},
		})
	})
	bl.run(b, backlogJobs, func(int) error { return bl.enqueue("bench.retry", types.PriorityNormal) }, backlogJobs, nil)
}

// BenchmarkBacklogScheduled measures one-time jobs that are due at once.
func BenchmarkBacklogScheduled(b *testing.B) {
	bl := newBacklog(b, 100, nil, func(bl *backlog, srv *server.Server) {
		srv.RegisterHandler(types.HandlerDefinition{TaskType: "bench.scheduled", Handler: func(context.Context, json.RawMessage) error {
			bl.count()
			return nil
		}})
	})
	var executions int64
	var elapsed time.Duration
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		bl.done.Store(0)
		bl.want.Store(backlogJobs)
		at := time.Now().Add(4 * time.Second)
		inParallel(b, backlogJobs, func(int) error {
			_, err := bl.cli.EnqueueScheduled(context.Background(), &client.EnqueueRequest{
				TaskType: "bench.scheduled", Payload: json.RawMessage(`{}`), Schedule: types.Schedule{Type: types.ScheduleOneTime, RunAt: &at},
			})
			return err
		})
		if time.Until(at) < 0 {
			b.Fatal("the jobs were due before all had been enqueued")
		}
		time.Sleep(time.Until(at))
		b.StartTimer()
		select {
		case <-bl.all:
		case <-time.After(backlogTimeout):
			b.Logf("only %d of %d executions within %v", bl.done.Load(), backlogJobs, backlogTimeout)
		}
		b.StopTimer()
		elapsed += time.Since(at)
		executions += bl.done.Load()
	}
	b.ReportMetric(float64(executions)/elapsed.Seconds(), "jobs/sec")
}

// BenchmarkBacklogWorkflows measures the tasks of workflows of two tasks, one after the other.
func BenchmarkBacklogWorkflows(b *testing.B) {
	const workflows = 500
	bl := newBacklog(b, 100, nil, func(bl *backlog, srv *server.Server) {
		srv.RegisterHandler(bl.handler("bench.task", 0))
	})
	def := types.WorkflowDefinition{Name: "bench", Tasks: []types.WorkflowTask{
		{Name: "a", TaskType: "bench.task"}, {Name: "b", TaskType: "bench.task", DependsOn: []string{"a"}},
	}}
	bl.run(b, 2*workflows, func(int) error {
		_, err := bl.cli.EnqueueWorkflow(context.Background(), def, nil)
		return err
	}, workflows, nil)
}
