# dureq

A distributed, Redis-native job scheduling and workflow orchestration system built in Go.

**dureq** is short for **du**rable **r**edis **e**xecution **q**ueue.

99.9% vibe-coded.

Inspired by [Asynq](https://github.com/hibiken/asynq), [gocron](https://github.com/go-co-op/gocron), and [river](https://github.com/riverqueue/river).

## Why v1? — Rollback from the Actor Model

This repository is the **v1 (current)** implementation of dureq.

A v2 rewrite was attempted using the [Hollywood](https://github.com/anthdm/hollywood) actor framework, moving from v1's pull-based Redis Streams model to a push-based actor model with cluster-wide singleton actors (Scheduler, Dispatcher, Orchestrator) and per-node worker actors (WorkerSupervisor, WorkerActor, EventBridge, Notifier, Heartbeat, ClusterGuard).

The v2 actor model was rolled back to v1 for the following reasons:

### 1. Debugging was a nightmare

Actor boundaries destroy stack traces. When a handler panics or returns an error, the error is caught in `recover()`, converted to a string, and passed through 4-5 actor hops (WorkerActor -> WorkerSupervisor -> EventBridge -> Orchestrator) — each hop flattening the error to a plain string. By the time you see the error in Redis or logs, all type information, wrapped error context, and stack traces are gone. You're left with `"render failed for user user-006: GPU out of memory"` and no idea where in the actor chain things went wrong.

In v1, errors propagate through normal Go function calls. You get real stack traces, `errors.Is()` / `errors.As()` works, and you can set breakpoints anywhere in the call chain.

### 2. Delayed and lagging job dispatch

v2's dispatch path requires **7 actor hops** for a simple job:

```
NotifierActor -> DispatcherActor -> WorkerSupervisorActor -> WorkerActor (spawn)
-> handler goroutine -> WorkerActor -> WorkerSupervisor -> EventBridgeActor
```

Each hop involves actor mailbox queuing, message serialization (protobuf for cross-node), and scheduling overhead. The Dispatcher singleton becomes a bottleneck — every job in the entire cluster must route through a single actor for node selection. Under load, the Dispatcher's mailbox backs up and dispatch latency spikes unpredictably.

v1's path is direct: client writes to Redis Stream -> worker polls stream -> executes. No intermediate actors, no singleton bottleneck, no message serialization between components on the same node.

### 3. Memory allocation explosion

v2 copies the job payload **at minimum 2x** on the dispatch path:

- `Job -> DispatchJobMsg` (protobuf allocation + payload byte copy)
- `DispatchJobMsg -> ExecuteJobMsg` (another protobuf allocation + payload copy)

Every job completion also allocates: `ExecutionDoneMsg -> WorkerDoneMsg -> DomainEventMsg` — each carrying the full `JobRun` struct and error strings through the actor chain. For high-throughput workloads, this creates significant GC pressure.

v1 passes job data by pointer within the same process. The only serialization boundary is Redis itself.

### 4. Operational complexity without proportional benefit

v2 introduced Hollywood cluster membership, protobuf code generation, singleton election via ClusterGuard, capacity reporting via WorkerStatusMsg (2s interval), and a complex actor wiring phase during startup. This added ~15 source files, protobuf definitions, and a `make genproto` build step — all to solve a problem (push-based dispatch) that v1 solves adequately with a simple Redis Pub/Sub notification that wakes the fetch loop.

The theoretical latency advantage of push-based dispatch (4-12ms vs polling) did not justify the complexity cost in practice, especially since v1's Pub/Sub wake mechanism already eliminates most polling latency.

## Architecture

```
Client (Go)
  |
  |  enqueue via Redis
  v
+-----------------------------------------------------------+
|                         Redis                              |
|  Jobs . Schedules . Streams . Locks . Events . Results     |
+-----------------------------------------------------------+
  |                         ^
  v  Pub/Sub notify         |  persist results
+-----------------------------------------------------------+
|  Node                                                      |
|                                                            |
|  +-------------+  +-------------+  +---------+             |
|  |  Scheduler  |  | Dispatcher  |  | Worker  |             |
|  |  (leader)   |  | (direct)    |  | Pool    |             |
|  +------+------+  +------+------+  +----+----+             |
|         |                |              |                   |
|         v                v              v                   |
|  Sorted Set scan   XADD to Stream   XREADGROUP              |
|  -> dispatch due   + Pub/Sub wake   -> execute handler      |
|                                     -> ack / retry          |
|                                                            |
|  +---------------+  +----------------+                     |
|  | Orchestrator  |  | Monitor API    |                     |
|  | (leader)      |  | HTTP/gRPC/WS   |                     |
|  +---------------+  +----------------+                     |
+-----------------------------------------------------------+
         ^
         |  HTTP / WebSocket
+--------+--------+      +------------------+
|    durequi      |      |    dureqctl       |
| (Web Dashboard) |      | (Terminal UI)     |
+-----------------+      +------------------+
```

- **Leader-elected coordinator**: only the leader runs Scheduler + Orchestrator
- **All nodes are workers**: every node polls Redis Streams and executes handlers
- **Direct Redis data flow**: no actors, no RPC between components on the same node
- **Single dependency**: Redis only (Standalone, Sentinel, or Cluster — including multi-shard clusters)

## Features

### Core
- **Flexible Scheduling** — Immediate, one-time, interval, cron, daily, weekly, monthly with timezone support
- **Workflow Orchestration** — DAG-based task dependencies with automatic state advancement and child workflows
- **Batch Processing** — Optional onetime preprocessing, chunked parallel item execution, failure policies
- **Group Aggregation** — Collect individual tasks into groups, automatically flush and aggregate into a single job based on grace period, max delay, or max size
- **Mux Handler** — Pattern-based task type routing (`order.*`), global and per-handler middleware
- **Priority Queues** — User-defined tiers with weighted fair queuing
- **Task Queue Rate Limiting** — Distributed token bucket rate limiter per queue (Redis Lua-backed)

### Reliability
- **Retry Policies** — Exponential backoff with jitter, error classification (retryable, non-retryable, rate-limited)
- **Overlap Policies** — Control concurrent runs for recurring jobs (allow all, skip, buffer one, buffer all, replace)
- **Catchup / Backfill** — Recover missed executions within a configurable window
- **Schedule Jitter** — Random offset on scheduled execution times to prevent thundering herd
- **Exactly-Once Execution** — Per-run distributed locks prevent duplicate handler invocation
- **Unique Keys** — Deduplication via unique keys with lookup and manual deletion
- **Worker Versioning** — BuildID-style safe deployments; version-mismatched work is re-enqueued for matching workers
- **ScheduleToStart Timeout** — Fail jobs that wait too long in the queue before starting
- **Workflow Signals** — Send asynchronous external data to running workflows

### Operations
- **Leader Election** — Automatic failover with configurable TTL and epoch-based fencing tokens
- **Dynamic Configuration** — Runtime-updateable settings (concurrency, timeouts, retry) via Redis, per-handler overrides
- **Archival / Retention** — Automatic cleanup of completed/dead/cancelled jobs after configurable retention period
- **Node Drain** — Graceful worker drain: stop accepting new work while finishing in-flight tasks
- **Audit Trail** — Per-job state transition history stored as capped Redis Streams
- **Progress Reporting** — In-flight progress updates from handlers via `types.ReportProgress(ctx, data)`
- **Distributed Locking** — Redis-based locks with automatic extension

### Observability
- **Web Dashboard** — React-based UI (durequi) with real-time WebSocket updates
- **Terminal UI** — Interactive TUI (dureqctl) for cluster monitoring
- **Monitoring APIs** — HTTP REST, gRPC, and WebSocket for full cluster observability
- **Payload Search** — JSONPath-based search across jobs, workflows, and batches
- **Multi-Tenancy** — Key prefix isolation per tenant

## Getting Started

### Prerequisites

- Go 1.25+
- Redis 7+

### Quick Example

**Server:**

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "time"

    "github.com/FDK0901/dureq/pkg/dureq"
    "github.com/FDK0901/dureq/pkg/types"
)

func main() {
    srv, _ := dureq.NewServer(
        dureq.WithRedisURL("redis://localhost:6379"),
        dureq.WithNodeID("node-1"),
        dureq.WithMaxConcurrency(10),
    )

    srv.RegisterHandler(types.HandlerDefinition{
        TaskType:    "email.send",
        Concurrency: 5,
        Timeout:     30 * time.Second,
        Handler: func(ctx context.Context, payload json.RawMessage) error {
            fmt.Printf("Sending email: %s\n", payload)
            return nil
        },
    })

    ctx := context.Background()
    srv.Start(ctx)
    // ... signal handling, srv.Stop()
}
```

**Client:**

```go
package main

import (
    "context"
    "time"

    "github.com/FDK0901/dureq/pkg/dureq"
    "github.com/FDK0901/dureq/pkg/types"
)

func main() {
    cli, _ := dureq.NewClient(dureq.WithClientRedisURL("redis://localhost:6379"))
    defer cli.Close()

    ctx := context.Background()

    // Immediate job
    cli.Enqueue(ctx, "email.send", map[string]string{
        "to":      "user@example.com",
        "subject": "Hello",
    })

    // Recurring job (every 30 seconds, bounded)
    start := time.Now().Add(time.Minute)
    end := start.Add(24 * time.Hour)
    interval := types.Duration(30 * time.Second)
    cli.EnqueueScheduled(ctx, &dureq.EnqueueRequest{
        TaskType: "report.generate",
        Payload:  []byte(`{}`),
        Schedule: types.Schedule{
            Type:     types.ScheduleDuration,
            Interval: &interval,
            StartsAt: &start,
            EndsAt:   &end,
        },
    })
}
```

## Scheduling

| Type        | Description                              | Example                       |
| ----------- | ---------------------------------------- | ----------------------------- |
| `IMMEDIATE` | Execute as soon as a worker is available | Fire-and-forget tasks         |
| `ONE_TIME`  | Execute once at a specific time          | Deferred notifications        |
| `DURATION`  | Repeat at fixed intervals                | Polling every 30s             |
| `CRON`      | Cron expression                          | `0 */6 * * *` (every 6 hours) |
| `DAILY`     | Specific time(s) each day                | Reports at 09:00 and 18:00    |
| `WEEKLY`    | Specific day(s) and time(s)              | Monday and Friday at 10:00    |
| `MONTHLY`   | Specific day(s) of month                 | 1st and 15th at midnight      |

### Overlap Policies

For recurring jobs, control what happens when a new execution is due while a previous one is still running:

| Policy       | Behavior                                     |
| ------------ | -------------------------------------------- |
| `ALLOW_ALL`  | Dispatch regardless of active runs (default) |
| `SKIP`       | Skip if any run is still active              |
| `BUFFER_ONE` | Queue at most one pending dispatch           |
| `BUFFER_ALL` | Queue all pending dispatches                 |
| `REPLACE`    | Cancel active run and start new one          |

## Workflows

Define DAG-based workflows where tasks execute in dependency order:

```go
cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{
    Name: "order-processing",
    Tasks: []types.WorkflowTask{
        {Name: "validate", TaskType: "order.validate", Payload: payload},
        {Name: "charge",   TaskType: "order.charge",   DependsOn: []string{"validate"}},
        {Name: "reserve",  TaskType: "order.reserve",  DependsOn: []string{"validate"}},
        {Name: "ship",     TaskType: "order.ship",     DependsOn: []string{"charge", "reserve"}},
        {Name: "notify",   TaskType: "order.notify",   DependsOn: []string{"ship"}},
    },
})
```

The orchestrator validates the DAG (no cycles, all dependencies exist), dispatches root tasks first, and advances downstream tasks as dependencies complete. On retry, only failed tasks and their dependents are re-executed — completed tasks are preserved.

## Batches

Process collections of items with optional shared preprocessing:

```go
onetimeType := types.TaskType("image.download_template")

cli.EnqueueBatch(ctx, types.BatchDefinition{
    Name:            "birthday-cards",
    OnetimeTaskType: &onetimeType,
    OnetimePayload:  templatePayload,
    ItemTaskType:    "image.overlay_text",
    Items:           items,
    ChunkSize:       100,
    FailurePolicy:   types.BatchContinueOnError,
})
```

- **Onetime preprocessing**: shared setup task runs once before items begin
- **Chunked dispatch**: configurable chunk size provides backpressure
- **Failure policies**: `continue_on_error` (collect all results) or `fail_fast` (stop on first failure)
- **Per-item results**: individual success/failure stored and queryable

## Group Aggregation

Collect individual events into groups and automatically aggregate them into a single job:

```go
// Server: define aggregator + register handler
aggregator := types.GroupAggregatorFunc(
    func(group string, payloads []json.RawMessage) (json.RawMessage, error) {
        // Merge individual payloads into one
        return json.Marshal(merged)
    },
)

srv, _ := dureq.NewServer(
    dureq.WithRedisURL("redis://localhost:6379"),
    dureq.WithGroupAggregation(types.GroupConfig{
        Aggregator:  aggregator,
        GracePeriod: 3 * time.Second,  // flush 3s after last item added
        MaxDelay:    15 * time.Second, // force flush after 15s regardless
        MaxSize:     10,               // flush immediately when 10 items
    }),
)

// Client: enqueue individual items into a group
cl.EnqueueGroup(ctx, types.EnqueueGroupOption{
    Group:    "user-alice",
    TaskType: "analytics.process",
    Payload:  eventPayload,
})
```

Groups are flushed when **any** trigger fires: grace period elapsed (no new items), max delay reached, or max size hit. The aggregator merges all buffered payloads into a single job payload dispatched to the registered handler.

The flush uses an atomic Lua script, so multiple nodes can safely run the aggregation processor concurrently — only one node will claim each group's messages.

## Error Handling & Retries

```go
// Default: 3 attempts, 5s initial delay, 5m max, 2x multiplier, 10% jitter
types.DefaultRetryPolicy()

// Custom
&types.RetryPolicy{
    MaxAttempts:  5,
    InitialDelay: 10 * time.Second,
    MaxDelay:     10 * time.Minute,
    Multiplier:   3.0,
    Jitter:       0.2,
}
```

Error classification controls retry behavior:

```go
return fmt.Errorf("transient failure: %w", err)              // retryable (default)
return &types.NonRetryableError{Err: err}                    // permanent, skip retries
return &types.RateLimitedError{Err: err, RetryAfter: 30*time.Second} // retry after delay
```

## Handler Context

Inside a handler, access job metadata via context:

```go
func handler(ctx context.Context, payload json.RawMessage) error {
    jobID    := types.GetJobID(ctx)
    attempt  := types.GetAttempt(ctx)
    taskType := types.GetTaskType(ctx)
    headers  := types.GetHeaders(ctx)

    types.ReportProgress(ctx, json.RawMessage(`{"percent": 50}`))

    return nil
}
```

## Middleware

```go
// Global — applies to ALL handlers
srv.Use(loggingMiddleware, metricsMiddleware)

// Per-handler — applied after global middleware
srv.RegisterHandler(types.HandlerDefinition{
    TaskType: "order.*",
    Handler:  orderHandler,
    Middlewares: []types.MiddlewareFunc{timingMiddleware},
})
```

Pattern-based routing: register `"order.*"` to match `order.validate`, `order.charge`, `order.ship`, etc. Use `types.GetTaskType(ctx)` inside the handler to dispatch.

## Monitoring API

### Endpoints

**Jobs:**

| Method   | Path                     | Description                       |
| -------- | ------------------------ | --------------------------------- |
| `GET`    | `/api/jobs`              | List jobs (paginated, filterable) |
| `GET`    | `/api/jobs/{id}`         | Get job details                   |
| `DELETE` | `/api/jobs/{id}`         | Delete a job                      |
| `POST`   | `/api/jobs/{id}/cancel`  | Cancel a running job              |
| `POST`   | `/api/jobs/{id}/retry`   | Retry a failed job                |
| `PUT`    | `/api/jobs/{id}/payload` | Update job payload                |
| `GET`    | `/api/jobs/{id}/events`  | Job event history                 |
| `GET`    | `/api/jobs/{id}/runs`    | Job run history                   |

**Workflows:**

| Method | Path                         | Description              |
| ------ | ---------------------------- | ------------------------ |
| `GET`  | `/api/workflows`             | List workflows           |
| `GET`  | `/api/workflows/{id}`        | Get workflow details     |
| `POST` | `/api/workflows/{id}/cancel` | Cancel workflow          |
| `POST` | `/api/workflows/{id}/retry`  | Retry from failure point |

**Batches:**

| Method | Path                                 | Description            |
| ------ | ------------------------------------ | ---------------------- |
| `GET`  | `/api/batches`                       | List batches           |
| `GET`  | `/api/batches/{id}`                  | Get batch details      |
| `GET`  | `/api/batches/{id}/results`          | List item results      |
| `GET`  | `/api/batches/{id}/results/{itemId}` | Get single item result |
| `POST` | `/api/batches/{id}/cancel`           | Cancel batch           |
| `POST` | `/api/batches/{id}/retry`            | Retry batch            |

**Bulk Operations:**

| Method | Path                         | Description               |
| ------ | ---------------------------- | ------------------------- |
| `POST` | `/api/jobs/bulk/cancel`      | Cancel multiple jobs      |
| `POST` | `/api/jobs/bulk/retry`       | Retry multiple jobs       |
| `POST` | `/api/jobs/bulk/delete`      | Delete multiple jobs      |
| `POST` | `/api/workflows/bulk/cancel` | Cancel multiple workflows |
| `POST` | `/api/workflows/bulk/retry`  | Retry multiple workflows  |
| `POST` | `/api/workflows/bulk/delete` | Delete multiple workflows |
| `POST` | `/api/batches/bulk/cancel`   | Cancel multiple batches   |
| `POST` | `/api/batches/bulk/retry`    | Retry multiple batches    |
| `POST` | `/api/batches/bulk/delete`   | Delete multiple batches   |

**Audit Trail:**

| Method | Path                        | Description                       |
| ------ | --------------------------- | --------------------------------- |
| `GET`  | `/api/jobs/{id}/audit`      | Job state transition history      |
| `POST` | `/api/audit/counts`         | Audit entry counts (batch lookup) |
| `GET`  | `/api/workflows/{id}/audit` | Aggregated workflow audit trail   |

**Node Drain:**

| Method   | Path                    | Description           |
| -------- | ----------------------- | --------------------- |
| `POST`   | `/api/nodes/{id}/drain` | Start draining a node |
| `DELETE` | `/api/nodes/{id}/drain` | Stop draining a node  |
| `GET`    | `/api/nodes/{id}/drain` | Check drain status    |

**Other:**

| Method   | Path                        | Description                        |
| -------- | --------------------------- | ---------------------------------- |
| `GET`    | `/api/stats`                | Cluster stats                      |
| `GET`    | `/api/stats/daily`          | Daily aggregated statistics        |
| `GET`    | `/api/nodes`                | Worker nodes                       |
| `GET`    | `/api/schedules`            | Active schedules                   |
| `GET`    | `/api/queues`               | Queue tiers (size, weight, paused) |
| `POST`   | `/api/queues/{tier}/pause`  | Pause a queue                      |
| `POST`   | `/api/queues/{tier}/resume` | Resume a queue                     |
| `GET`    | `/api/dlq`                  | Dead letter queue                  |
| `GET`    | `/api/history/runs`         | Historical runs (filterable)       |
| `GET`    | `/api/history/events`       | Historical events                  |
| `GET`    | `/api/search/jobs`          | Search by payload JSONPath         |
| `GET`    | `/api/search/workflows`     | Search workflows by payload        |
| `GET`    | `/api/search/batches`       | Search batches by payload          |
| `GET`    | `/api/unique-keys/{key}`    | Check unique key                   |
| `DELETE` | `/api/unique-keys/{key}`    | Delete unique key                  |
| `GET`    | `/api/redis/info`           | Redis server info                  |
| `GET`    | `/api/sync-retries`         | Pending sync retries               |
| `GET`    | `/api/health`               | Health check                       |

**WebSocket:**

| Path      | Description                                        |
| --------- | -------------------------------------------------- |
| `/api/ws` | Real-time event stream (all domain events as JSON) |

## Event Types

| Category     | Events                                                                                                                     |
| ------------ | -------------------------------------------------------------------------------------------------------------------------- |
| **Job**      | `enqueued`, `scheduled`, `dispatched`, `started`, `completed`, `failed`, `retrying`, `dead`, `cancelled`                   |
| **Schedule** | `created`, `removed`                                                                                                       |
| **Workflow** | `started`, `completed`, `failed`, `cancelled`, `task.dispatched`, `task.completed`, `task.failed`, `timed_out`, `retrying` |
| **Batch**    | `started`, `completed`, `failed`, `cancelled`, `item.completed`, `item.failed`, `progress`, `timed_out`, `retrying`        |
| **Node**     | `joined`, `left`, `crash_detected`                                                                                         |
| **Leader**   | `elected`, `lost`                                                                                                          |

## Configuration

### Server Options

| Option                            | Default        | Description                  |
| --------------------------------- | -------------- | ---------------------------- |
| `dureq.WithRedisURL(url)`         | -              | Redis connection URL         |
| `dureq.WithRedisDB(db)`           | `0`            | Redis database number        |
| `dureq.WithRedisPassword(pw)`     | -              | Redis password               |
| `dureq.WithRedisPoolSize(n)`      | -              | Connection pool size         |
| `dureq.WithRedisSentinel(...)`    | -              | Redis Sentinel failover      |
| `dureq.WithRedisCluster(addrs)`   | -              | Redis Cluster mode (multi-shard supported) |
| `dureq.WithNodeID(id)`            | auto-generated | Unique node identifier       |
| `dureq.WithMaxConcurrency(n)`     | `100`          | Maximum concurrent workers   |
| `dureq.WithLockTTL(d)`            | `30s`          | Distributed lock TTL         |
| `dureq.WithKeyPrefix(prefix)`     | -              | Redis key prefix             |
| `dureq.WithPriorityTiers(tiers)`  | -              | Custom priority tier mapping |
| `dureq.WithGroupAggregation(cfg)` | disabled       | Enable group aggregation     |
| `dureq.WithRetentionPeriod(d)`    | `7d`           | Retention for completed jobs |
| `dureq.WithLogger(logger)`        | -              | Custom logger                |

### Client Options

| Option                              | Default | Description                  |
| ----------------------------------- | ------- | ---------------------------- |
| `dureq.WithClientRedisURL(url)`     | -       | Redis connection URL         |
| `dureq.WithClientRedisDB(db)`       | `0`     | Redis database number        |
| `dureq.WithClientRedisPassword(pw)` | -       | Redis password               |
| `dureq.WithClientKeyPrefix(prefix)` | -       | Key namespace                |
| `dureq.WithClientTiers(tiers)`      | -       | Custom priority tier mapping |
| `dureq.WithClientRedisStore(store)` | -       | Use existing store           |

## Dynamic Configuration

Settings can be updated at runtime without restarting nodes. Overrides are stored in Redis and polled every 10 seconds.

**Global overrides:**

```bash
# Via Redis directly
redis-cli HSET dureq:config:global MaxConcurrency 200
redis-cli HSET dureq:config:global SchedulerTickInterval 5s
```

**Per-handler overrides:**

```bash
redis-cli HSET dureq:config:handler:email.send Concurrency 20
redis-cli HSET dureq:config:handler:email.send Timeout 60s
redis-cli HSET dureq:config:handler:email.send MaxAttempts 5
```

These overrides take precedence over values set at server startup. Delete a field to revert to the startup default.

## Project Structure

```
dureq/
+-- cmd/
|   +-- dureqd/             # Server daemon (HTTP/gRPC APIs)
|   +-- dureqctl/            # Interactive terminal UI (Bubble Tea)
+-- internal/
|   +-- server/              # Server setup, handler registry, options
|   +-- worker/              # Redis Streams consumer, goroutine pool, retry
|   +-- dispatcher/          # Job -> Stream dispatch, priority tier mapping
|   +-- scheduler/           # Leader-only schedule scanner, orphan detection
|   +-- workflow/            # DAG orchestrator, batch state machine
|   +-- aggregation/         # Group aggregation processor (leader-independent)
|   +-- store/               # Redis persistence (Lua scripts, streams, pagination)
|   +-- client/              # Go client SDK
|   +-- monitor/             # HTTP REST, gRPC, WebSocket APIs
|   +-- election/            # Redis-based leader election with epoch fencing
|   +-- lock/                # Distributed locking with auto-extension
|   +-- dynconfig/           # Dynamic configuration (runtime-updateable via Redis)
|   +-- ratelimit/           # Distributed token bucket rate limiter
|   +-- archival/            # Retention-based cleanup of old jobs
|   +-- cache/               # In-memory schedule and job cache
|   +-- lifecycle/           # Graceful shutdown coordination
+-- pkg/
|   +-- types/               # Public domain types (Job, Schedule, Workflow, Batch)
+-- examples/                # Runnable demos
+-- tests/
|   +-- integration/         # Integration tests (requires Redis)
+-- proto/
|   +-- dureq/monitor/v1/   # gRPC service definitions
+-- gen/                     # Generated protobuf code
```

## Examples

| Example                     | Description                                                  |
| --------------------------- | ------------------------------------------------------------ |
| `onetimeat/`                | One-time scheduled execution                                 |
| `onetimeat_with_mux/`       | One-time with pattern-based routing                          |
| `festival/`                 | Recurring scheduled jobs (duration, daily, weekly)           |
| `festival_with_mux/`        | Recurring with mux routing and middleware                    |
| `festival_mux_just_server/` | Server-only mux example                                      |
| `workflow/`                 | DAG workflow with task dependencies                          |
| `workflow_with_mux/`        | Workflow with mux handler                                    |
| `child_workflow/`           | Parent-child workflow with cancellation propagation          |
| `signals/`                  | Workflow signals (async external input to running workflows) |
| `batch/`                    | Batch processing with onetime preprocessing                  |
| `batch_with_mux/`           | Batch with pattern-based routing                             |
| `aggregation/`              | Group aggregation (collect events, flush as single batch)    |
| `heartbeat_progress/`       | In-flight progress reporting                                 |
| `overlap_policy/`           | Overlap policy demonstration (including REPLACE)             |
| `retention/`                | Archival and retention management                            |

```bash
cd examples/workflow_with_mux
go run main.go
```

## Dependencies

| Dependency                                                | Purpose                       |
| --------------------------------------------------------- | ----------------------------- |
| [rueidis](https://github.com/redis/rueidis)               | High-performance Redis client |
| [ants](https://github.com/panjf2000/ants)                 | Goroutine pool                |
| [robfig/cron](https://github.com/robfig/cron)             | Cron expression parsing       |
| [sonic](https://github.com/bytedance/sonic)               | Fast JSON serialization       |
| [xid](https://github.com/rs/xid)                          | Globally unique ID generation |
| [go-chainedlog](https://github.com/FDK0901/go-chainedlog) | Structured logging            |
| [websocket](https://github.com/coder/websocket)           | WebSocket server              |
| [bubbletea](https://github.com/charmbracelet/bubbletea)   | Terminal UI framework         |
| [koanf](https://github.com/knadh/koanf)                   | Configuration management      |
| [grpc-go](https://google.golang.org/grpc)                 | gRPC server                   |
| [vtprotobuf](https://github.com/planetscale/vtprotobuf)   | Optimized protobuf marshaling |
