# dureq

A Redis-native execution engine for Go: jobs, schedules, workflows, batches, and operational controls for application backends.

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
|  | (leader)      |  | (dureqd)       |                     |
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
- **Nothing that is yet to be done is trimmed**: messages leave a work stream once they have been acknowledged, and events leave the orchestrator's stream once it has processed them. Jobs, the run history and the dead letter queue are kept until you clean them up
- **Core runtime dependency**: Redis only (Standalone, Sentinel, or Cluster). Optional dashboards, APIs, and application-specific handlers run outside the core execution path.

## Features

### Core
- **Flexible Scheduling** — Immediate, one-time, interval, cron, daily, weekly, monthly with timezone support
- **Workflow Orchestration** — DAG-based task dependencies with automatic state advancement, child workflows, condition nodes and loops, dynamic subflows, result piping, lifecycle hooks, and suspend/resume
- **Batch Processing** — Optional onetime preprocessing, chunked parallel item execution, failure policies
- **Group Aggregation** — Collect individual tasks into groups, automatically flush and aggregate into a single job based on grace period, max delay, or max size
- **Mux Handler** — Pattern-based task type routing (`order.*`), global and per-handler middleware
- **Priority Queues** — User-defined tiers, looked at in their order
- **Concurrency Limits** — Per node (`MaxConcurrency`), per handler and node (`Concurrency`), and per key across the cluster (`ConcurrencyKeys`). A handler that has reached its limit does not hold up the others ([details](docs/concurrency.md))
- **Task Queue Rate Limiting** — Distributed token bucket rate limiter per queue (Redis Lua-backed)

### Reliability
- **Retry Policies** — Exponential backoff with jitter, error classification (retryable, non-retryable, rate-limited), and control flow errors (skip, repeat, pause)
- **Overlap Policies** — Control concurrent runs for recurring jobs (allow all, skip, buffer one, buffer all, replace)
- **Catchup / Backfill** — Recover missed executions within a configurable window
- **Schedule Jitter** — Random offset on scheduled execution times to prevent thundering herd
- **Operation Ledger (L2 Exactly-Once)** — Stable OperationKey per logical operation + Redis CAS ledger ensures each operation completes at most once internally. Crash recovery via `RepairCompletedOperation`. External side effects require idempotency keys, `sideeffect.Step`, or transactional completion ([details](docs/guarantees.md))
- **Atomic Dispatch** — Lua script performs dedup check + XADD + dedup mark in a single atomic operation; eliminates crash window between dedup and stream write
- **Transactional Completion** — `pkg/integration/postgres` adapter commits business write + dureq completion marker in the same SQL transaction for exactly-once business outcomes
- **Durable Cancellation** — Persisted cancel flag (`SET EX 24h`) + Pub/Sub fast path + 10s polling fallback; survives Pub/Sub disconnections
- **Unique Keys** — Deduplication via unique keys with lookup and manual deletion; released when the job goes dead or is cancelled, kept when it completes. `EnqueueOrReplace` cancels the job that holds the key and puts a new one in its place
- **Dead Letter Queue** — Keeps every job that has used up its attempts, unless you limit it
- **Durable Workflow Events** — What the orchestrator has to see has a stream of its own without a limit, and is tried again until it has been processed
- **Worker Versioning** — BuildID-style safe deployments; version-mismatched work is re-enqueued for matching workers
- **ScheduleToStart Timeout** — Fail jobs that wait too long in the queue before starting
- **Workflow Signals** — Send asynchronous external data to running workflows. `ReadSignals` + `AckSignals` provides at-least-once delivery; see [guarantees](docs/guarantees.md) and [signals](docs/signals.md) docs

### Operations
- **Leader Election** — Automatic failover with configurable TTL and epoch-based fencing tokens
- **Dynamic Configuration** — Scheduler interval, and timeout and attempts per handler, changed at runtime via Redis
- **Archival / Retention** — Off by default. Opt in to have completed/failed/dead/cancelled jobs, and finished runs, removed after a while, or remove runs by hand
- **Pause / Resume** — Queues, jobs, and workflows; a job can pause itself, and a recurring one after so many errors in a row
- **Node Drain** — Graceful worker drain: stop accepting new work while finishing in-flight tasks
- **Audit Trail** — Per-job state transition history stored as capped Redis Streams
- **Progress Reporting** — In-flight progress updates from handlers via `types.ReportProgress(ctx, data)`
- **Distributed Locking** — Redis-based locks with automatic extension

### Observability
- **Web Dashboard** — React-based UI (durequi, a repository of its own) with real-time WebSocket updates
- **Terminal UI** — Interactive TUI (dureqctl) for cluster monitoring
- **Monitoring APIs** — HTTP REST, gRPC, and WebSocket for full cluster observability
- **Payload Search** — JSONPath-based search across jobs, workflows, and batches; `GetJobsByParamJSONPath` returns all matching jobs, optionally only live ones (needs RedisJSON)
- **Run and Event History** — Finished runs by job and status, and about the last 50,000 events
- **Multi-Tenancy** — Key prefix isolation per tenant

### Execution Guarantees

dureq provides **at-least-once delivery with L2 exactly-once internal completion** via an operation ledger.

| Aspect | Guarantee | Mechanism |
|--------|-----------|-----------|
| Enqueue | At-most-once per UniqueKey | `SET NX` dedup guard |
| Dispatch | At-most-once per RunID | Atomic Lua script (dedup + XADD + mark) in single Redis slot |
| Schedule firing | At-most-once per FiringID | FiringID dedup checked and set in the atomic dispatch script (1h TTL) + deterministic firing identity |
| Handler start | Duplicate-suppressed | Operation ledger claim + per-run lock |
| Internal completion | Exactly-once per OperationKey | Two-phase: ledger CAS commit (Phase 1) + idempotent pipeline (Phase 2) |
| Business outcome | Exactly-once (opt-in) | `pkg/integration/postgres.CompleteTx` — same SQL TX as business write |
| Cancellation | Durable | Persisted flag (`SET EX 24h`) + Pub/Sub + 10s polling |
| Workflow progress | At-least-once event, counted once | Events are kept until the orchestrator has processed them; each is applied once by CAS |
| Workflow signals | At-least-once | `ReadSignals` + `AckSignals`; deprecated `ConsumeSignals` provides at-most-once |
| Durability | Opt-in | `WAIT` / `WAITAOF` after critical writes (configurable) |

For full details including failure scenarios, see [docs/guarantees.md](docs/guarantees.md) and [docs/faq.md](docs/faq.md).

## Getting Started

### Prerequisites

- Go 1.26.11+ (the go.mod minimum; older toolchains have known security vulnerabilities)
- Redis 7+. Payload search needs RedisJSON, which Redis 8 comes with, or valkey-json, which valkey-bundle comes with; everything else works without

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
    // In production: wire OS signal handling, call srv.Stop(), and expose /healthz and /readyz when using dureqd-style deployment.
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
}, nil) // the input of the workflow, if it has one
```

The orchestrator validates the DAG (no cycles, all dependencies exist), dispatches root tasks first, and advances downstream tasks as dependencies complete. On retry, only failed tasks and their dependents are re-executed — completed tasks are preserved. Each retry has the `ExecutionTimeout` to itself.

A task can be a child workflow (`ChildWorkflowDef`). If the child fails, times out or is cancelled, so does the task, unless that has `AllowFailure`. A suspended workflow starts nothing new until it is resumed. See [docs/workflow-model.md](docs/workflow-model.md).

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

See [docs/batch-model.md](docs/batch-model.md).

## Priority Tiers

All jobs share one queue unless you define tiers. List them from the most to the least important: workers look at them in that order, and the priority of a job, from 10 to 1, decides which one it goes to. `Weight` is shown by the monitoring API and does not change the order.

```go
tiers := []dureq.TierConfig{
    {Name: "high", Weight: 100},
    {Name: "normal", Weight: 50},
    {Name: "bulk", Weight: 10, RateLimit: 200}, // at most 200 jobs a second, across all nodes
}
srv, _ := dureq.NewServer(dureq.WithRedisURL("redis://localhost:6379"), dureq.WithPriorityTiers(tiers))
cli, _ := dureq.NewClient(dureq.WithClientRedisURL("redis://localhost:6379"), dureq.WithClientTiers(tiers))
```

Use the same tiers for all servers and clients. See [docs/concurrency.md](docs/concurrency.md).

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

For recurring jobs `MaxAttempts` applies to each firing on its own: successful runs do not count, and every firing starts at attempt 0. A firing that uses up its attempts ends the whole job (`dead`), including its schedule. With `Schedule.KeepOnFailure` it goes to the DLQ instead, and the job waits for its next firing. `Job.RunCount` counts the executions over the job's whole life.

A run that is lost with its worker counts as an attempt of a recurring job's firing.

Error classification controls retry behavior:

```go
return fmt.Errorf("transient failure: %w", err)              // retryable (default)
return &types.NonRetryableError{Err: err}                    // permanent, skip retries
return &types.RateLimitedError{Err: err, RetryAfter: 30*time.Second} // retry after delay
```

A job that has used up its attempts goes `dead` and into the dead letter queue. Retries start again by hand, with `Client.Retry` or the monitoring API.

## Handler Context

Inside a handler, access job metadata via context:

```go
func handler(ctx context.Context, payload json.RawMessage) error {
    jobID        := types.GetJobID(ctx)
    runID        := types.GetRunID(ctx)
    operationKey := types.GetOperationKey(ctx)
    attempt      := types.GetAttempt(ctx)
    taskType     := types.GetTaskType(ctx)
    headers      := types.GetHeaders(ctx)

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

[dureqd](docs/dureqd.md) serves it, over HTTP on `:8080` and gRPC on `:9090` by default. In a server of your own, build endpoints on `dureq.NewAPIService`, which has the same calls.

> [!IMPORTANT]
> The API has no authentication. Put it behind something that has, and do not let it be reached without it. Web pages of other origins are refused, unless they are in [`allowedOrigins`](docs/dureqd.md#who-may-use-the-api); that is no protection from anything but browsers, and a page that rebinds its own name to the API's address (DNS rebinding) gets past it. So list the hosts that the API is reached by in [`allowedHosts`](docs/dureqd.md#who-may-use-the-api).

A request that takes longer than 30 seconds, as when Redis does not answer, is answered 503 with `Retry-After: 5`, the same as a WebSocket that comes while the hub is stalled; `requestTimeoutMs` of dureqd sets another limit. The longest that are meant to be, a name filter or a payload search through 100,000, take a few seconds. A server of your own gets the same with `APIService.Timed`, which gives a call that long (`SetRequestTimeout`) and returns an `ApiError` with 503 when it is up.

What jobs carry may be personal data: payloads, results, progress and the errors of handlers. Answers that may hold any of it, such as jobs, workflows, batches, runs, events, the DLQ and searches, come with `Cache-Control: private, no-store`, so that a browser does not keep them. The others (stats, nodes, queues, schedules, Redis info, sync retries, signal stats, health) have a weak `ETag`, and are answered 304 if they have not changed.

### Endpoints

**Jobs:**

| Method   | Path                     | Description                       |
| -------- | ------------------------ | --------------------------------- |
| `GET`    | `/api/jobs`              | List jobs (paginated, filterable) |
| `GET`    | `/api/jobs/{id}`         | Get job details                   |
| `DELETE` | `/api/jobs/{id}`         | Delete a job                      |
| `POST`   | `/api/jobs/{id}/cancel`  | Cancel a running job              |
| `POST`   | `/api/jobs/{id}/retry`   | Retry a failed job                |
| `POST`   | `/api/jobs/{id}/resume`  | Resume a paused job               |
| `PUT`    | `/api/jobs/{id}/payload` | Update job payload                |
| `GET`    | `/api/jobs/{id}/events`  | Job event history                 |
| `GET`    | `/api/jobs/{id}/runs`    | Job run history                   |
| `GET`    | `/api/jobs/{id}/progress` | Progress of the job's active runs |
| `POST`   | `/api/jobs/by-ids`       | Several jobs (`{"ids": [...]}`, up to 100) |
| `POST`   | `/api/runs/by-jobs`      | The last runs of several jobs (`{"job_ids": [...], "limit": 20}`) |

**Workflows:**

| Method | Path                         | Description              |
| ------ | ---------------------------- | ------------------------ |
| `GET`  | `/api/workflows`             | List workflows (`?view=summary` without definitions, inputs and outputs) |
| `GET`  | `/api/workflows/{id}`        | Get workflow details     |
| `POST` | `/api/workflows/{id}/cancel` | Cancel workflow          |
| `POST` | `/api/workflows/{id}/retry`  | Retry from failure point |
| `POST` | `/api/workflows/{id}/suspend` | Suspend workflow        |
| `POST` | `/api/workflows/{id}/resume` | Resume a suspended workflow |
| `GET`  | `/api/workflows/{id}/tasks/{task}/result` | Result of a task; 404 if it is not there, not yet or not any more |
| `GET`  | `/api/workflows/{id}/signals/stats` | Signals of a workflow |
| `POST` | `/api/workflows/validate`    | Check a definition       |

**Batches:**

| Method | Path                                 | Description            |
| ------ | ------------------------------------ | ---------------------- |
| `GET`  | `/api/batches`                       | List batches (`?view=summary` without their items) |
| `GET`  | `/api/batches/{id}`                  | Get batch details      |
| `GET`  | `/api/batches/{id}/results`          | List item results      |
| `GET`  | `/api/batches/{id}/results/{itemId}` | Get single item result |
| `POST` | `/api/batches/{id}/cancel`           | Cancel batch           |
| `POST` | `/api/batches/{id}/retry`            | Retry batch            |

The lists of workflows and batches take `?name=`. There is no index of names, so that a name is looked for among the newest (or, with `sort=oldest`, the oldest) 100,000 of them at most; `dureq.WithMaxNameScan` (and `dureq.WithClientMaxNameScan` for the store of a client, `maxNameScan` in dureqd) sets another limit. Each of them is read, items and all, which takes about half a second for 10,000 batches of 50 items. If some were not looked at, the answer says `"total_is_lower_bound": true`: more may match. With `?total=false` it stops looking once the page is full, and says `"has_more"` instead of a total; that is true, too, if it stopped at `MaxNameScan`.

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
| `GET`    | `/api/nodes`                | Worker nodes, and whether they are `draining` |
| `GET`    | `/api/nodes/{id}`           | A worker node, and whether it is `draining` |
| `GET`    | `/api/schedules`            | Active schedules                   |
| `GET`    | `/api/schedules/{jobId}`    | The schedule of a job              |
| `GET`    | `/api/queues`               | Queue tiers (weight, paused, and how many jobs are `waiting`, `in_flight` and `delayed`) |
| `POST`   | `/api/queues/{tier}/pause`  | Pause a queue                      |
| `POST`   | `/api/queues/{tier}/resume` | Resume a queue                     |
| `GET`    | `/api/dlq`                  | Dead letter queue                  |
| `GET`    | `/api/history/runs`         | Historical runs (filterable)       |
| `DELETE` | `/api/history/runs`         | Remove runs (`?older_than=720h`)   |
| `GET`    | `/api/history/events`       | Historical events (`?job_id=` of one job; `?total=false` does not count them all) |
| `GET`    | `/api/runs`                 | Active runs (`?job_ids=a,b` and `?limit=`) |
| `GET`    | `/api/runs/{id}`            | A run, active or finished          |
| `GET`    | `/api/runs/{id}/progress`   | Progress of a run                  |
| `POST`   | `/api/runs/{id}/requeue`    | Queue a stuck run again            |
| `GET`    | `/api/search/jobs`          | Newest job by payload JSONPath     |
| `GET`    | `/api/search/jobs/all`      | All jobs by payload JSONPath       |
| `GET`    | `/api/search/workflows`     | Search workflows by payload        |
| `GET`    | `/api/search/batches`       | Search batches by payload          |
| `GET`    | `/api/unique-keys/{key}`    | Check unique key                   |
| `DELETE` | `/api/unique-keys/{key}`    | Delete unique key                  |
| `GET`    | `/api/locks/{key}`          | Check a lock                       |
| `DELETE` | `/api/locks/{key}`          | Release a lock                     |
| `GET`    | `/api/concurrency/{key}`    | Slots in use of a concurrency key  |
| `GET`    | `/api/groups`               | Groups being aggregated            |
| `GET`    | `/api/redis/info`           | Redis server info                  |
| `GET`    | `/api/sync-retries`         | Pending sync retries               |
| `GET`    | `/api/health`               | Health check                       |

The searches by payload need RedisJSON; without it, they answer 501.

**WebSocket:**

| Path      | Description                                        |
| --------- | -------------------------------------------------- |
| `/api/ws` | Real-time event stream (all domain events as JSON) |

With `/api/ws?v=2`, a client also gets `{"type":"ws.heartbeat"}` at once and every 15 seconds, so that it knows a dead connection from a quiet one, and `{"type":"ws.resync"}` if it has missed events, because it was too slow or Redis was away for longer than the event stream reaches back. Then it has to read again what it shows.

Both say when the server wrote them, in `server_time_ms`: milliseconds since 1970 by the clock of the server, such as `{"type":"ws.heartbeat","server_time_ms":1795695303123}`. Behind a slow connection, events and heartbeats come late while the connection is open; a client can tell by how much: how much later it got a heartbeat than it was written (`Date.now() - server_time_ms`), less the least of that on the connection, which is about the difference of the clocks. Events have their own `timestamp`, when they happened by the clock of the node that published them.

A client is told so once it has caught up with the events that are queued for it. One that has not caught up 5 seconds after it missed some, because events keep coming faster than it reads, is told all the same, and what is queued for it is dropped: it is to read everything again anyway. If it stays that slow, it is told again every 5 seconds or so. A frame that the connection does not take within 10 seconds ends it, as then nothing more can be told: the client is to connect again and read everything again.

A heartbeat says that events come as they happen. If Redis has not answered the reader of the events for longer than a read takes (10 seconds), as when a connection stays open and nothing comes back, the reader gives the connection up and goes on where it was on another one. Until it hears from Redis again, a client with `?v=2` gets no heartbeat: one that is in is closed with 1013 (try again later) when its next heartbeat is due, and one that comes is answered 503. A WebSocket of your own on `dureq.NewAPIService` can do the same: its hub is a `dureq.HubHealth`, whose `Stalled()` says so, also once the service has been shut down.

An event comes after what it stands for: whoever reads the job on `job.completed`, or its runs on `job.failed`, finds the new state. So it is with a run whose node is gone: `node.crash_detected`, and then `job.failed`, `job.dead` or `job.auto_recovered`, come once the run is in the history and no longer active. `job.failed` and `job.dead` carry the job's state then; `job.auto_recovered` does not, for by then the job may be running again. If the job cannot be saved or dispatched, or the leader goes away in the middle, the run stays as it is, and the next check, 30 seconds later, does it again.

## Event Types

| Category     | Events |
| ------------ | ------ |
| **Job**      | `dispatched`, `started`, `completed`, `failed`, `retrying`, `dead`, `cancelled`, `paused`, `resumed`, `updated`, `deleted`, `auto_recovered`, `schedule_to_start_timeout` |
| **Schedule** | `created`, `removed` |
| **Workflow** | `started`, `completed`, `failed`, `cancelled`, `timed_out`, `retrying`, `suspended`, `resumed`, `signal.received`, `task.dispatched`, `task.completed`, `task.failed`, `hook.dispatched`, `hook.completed`, `hook.failed` |
| **Batch**    | `started`, `completed`, `failed`, `cancelled`, `timed_out`, `retrying`, `progress`, `item.completed`, `item.failed`, `onetime.completed`, `onetime.failed` |
| **Node**     | `crash_detected` |

`job.completed`, `job.failed`, `job.paused` and `job.updated` carry the new `status`, `run_count` and `next_run_at` of the job, and the first three the `duration` of the run.

## Configuration

### Server Options

| Option                                | Default        | Description |
| ------------------------------------- | -------------- | ----------- |
| `dureq.WithRedisURL(url)`             | -              | Redis connection URL |
| `dureq.WithRedisUsername(name)`       | -              | Redis ACL username |
| `dureq.WithRedisPassword(pw)`         | -              | Redis password |
| `dureq.WithRedisDB(db)`               | `0`            | Redis database number |
| `dureq.WithRedisPoolSize(n)`          | `10`           | Connections for blocking commands, 10 at least |
| `dureq.WithRedisSentinel(...)`        | -              | Redis Sentinel failover |
| `dureq.WithRedisCluster(addrs)`       | -              | Redis Cluster mode (multi-shard supported) |
| `dureq.WithNodeID(id)`                | auto-generated | Unique node identifier |
| `dureq.WithMaxConcurrency(n)`         | `100`          | Jobs that run at once on this node |
| `dureq.WithPriorityTiers(tiers)`      | one, `normal`  | Priority tiers |
| `dureq.WithMode(mode)`                | `ModeFull`     | Which parts run on this node, such as `ModeQueue \| ModeScheduler` |
| `dureq.WithKeyPrefix(prefix)`         | -              | Redis key prefix |
| `dureq.WithSchedulerTickInterval(d)`  | `1s`           | How often the leader looks for due schedules |
| `dureq.WithHeartbeatInterval(d)`      | `5s`           | How often a node reports its health |
| `dureq.WithElectionTTL(d)`            | `10s`          | Leader election TTL |
| `dureq.WithLockTTL(d)`                | `30s`          | Distributed lock TTL |
| `dureq.WithShutdownTimeout(d)`        | `30s`          | How long jobs have to finish on shutdown before they are put back |
| `dureq.WithGroupAggregation(cfg)`     | disabled       | Enable group aggregation |
| `dureq.WithRetentionPeriod(d)`        | off            | Retention for ended jobs |
| `dureq.WithRunHistoryRetention(d)`    | off            | Retention for finished runs |
| `dureq.WithLogger(logger)`            | slog           | Custom logger |

### Client Options

| Option                                | Default        | Description |
| ------------------------------------- | -------------- | ----------- |
| `dureq.WithClientRedisURL(url)`       | -              | Redis connection URL |
| `dureq.WithClientRedisUsername(name)` | -              | Redis ACL username |
| `dureq.WithClientRedisPassword(pw)`   | -              | Redis password |
| `dureq.WithClientRedisDB(db)`         | `0`            | Redis database number |
| `dureq.WithClientRedisPoolSize(n)`    | `10`           | Connections for blocking commands, 10 at least |
| `dureq.WithClientClusterAddrs(addrs)` | -              | Redis Cluster mode |
| `dureq.WithClientKeyPrefix(prefix)`   | -              | Key namespace, the same as the servers' |
| `dureq.WithClientTiers(tiers)`        | one, `normal`  | Priority tiers, the same as the servers' |
| `dureq.WithClientRedisStore(store)`   | -              | Use existing store |

## Dynamic Configuration

These settings can be changed at runtime without restarting nodes. They are stored in Redis and looked up every 10 seconds.

```bash
# How often the leader looks for due schedules
redis-cli HSET dureq:config:global scheduler_tick_interval 5s

# Per handler
redis-cli HSET dureq:config:handler:email.send timeout 60s
redis-cli HSET dureq:config:handler:email.send max_attempts 5
```

They take precedence over what was set at startup. Delete a field to go back to that. With a key prefix, the keys start with `<prefix>_dureq:`.

## Project Structure

```
dureq/
+-- cmd/
|   +-- dureqd/             # Server daemon (HTTP/gRPC APIs)
|   +-- dureqctl/            # Interactive terminal UI (Bubble Tea)
+-- internal/
|   +-- server/              # Server setup, handler registry, options
|   +-- worker/              # Redis Streams consumer, goroutine pools, retry
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
|   +-- archival/            # Retention-based cleanup of old jobs and runs
|   +-- cache/               # In-memory schedule and job cache
|   +-- lifecycle/           # Job state machine
|   +-- periodic/            # Keeps recurring jobs in line with a list of them
|   +-- api/                 # HTTP client of the monitoring API (for dureqctl)
+-- pkg/
|   +-- dureq/               # What applications import: servers, clients, options
|   +-- types/               # Public domain types (Job, Schedule, Workflow, Batch)
|   +-- sideeffect/          # At-most-once side effect helpers (Step, ExternalCall)
|   +-- integration/
|       +-- postgres/        # Transactional completion adapter (CompleteTx, CheckpointTx)
+-- examples/                # Runnable demos
+-- tests/
|   +-- integration/         # Integration tests (requires Redis)
|   +-- failure/             # Tests of failures such as node crashes
+-- bench/                   # Benchmarks (requires Redis)
+-- configs/                 # Example configuration of dureqd
+-- docs/                    # Guarantees, models, operations
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
| `condition_workflow/`       | Branching on what a handler returns                          |
| `signals/`                  | Workflow signals (async external input to running workflows) |
| `batch/`                    | Batch processing with onetime preprocessing                  |
| `batch_with_mux/`           | Batch with pattern-based routing                             |
| `aggregation/`              | Group aggregation (collect events, flush as single batch)    |
| `heartbeat_progress/`       | In-flight progress reporting                                 |
| `overlap_policy/`           | Overlap policy demonstration (including REPLACE)             |
| `retention/`                | Archival and retention management                            |
| `condition_loop/`           | Workflow condition nodes with loop-back (while-loop pattern) |
| `subflow_workflow/`         | Tasks generated while the workflow runs                      |
| `result_piping/`            | The output of a task as the input of the next (`ResultFrom`) |
| `priority_workflow/`        | Ready tasks in order of their priority                       |
| `allow_failure/`            | A workflow that goes on when a task fails                    |
| `lifecycle_hooks/`          | Workflow hooks, and purging personal data                    |
| `pause_resume/`             | Jobs that pause, and resume by themselves or by hand         |
| `typed_handler/`            | Type-safe handlers, and control flow errors                  |
| `idempotent_handler/`       | Handlers that may run more than once                         |
| `pg_completion/`            | Transactional completion with PostgreSQL (exactly-once outcome) |

```bash
cd examples/workflow_with_mux
go run main.go
```

## Benchmarks

```bash
docker run -d --rm -p 6381:6379 redis:8 redis-server --requirepass your-password
REDIS_URL=redis://localhost:6381 REDIS_DB=15 go test -run '^$' -bench . ./bench/
```

`BenchmarkBacklog*` measure how fast a node works off what has piled up. The others measure single operations. One node on an 18-core machine, with Redis 8 in a container next to it (medians):

| Benchmark | v0.2.5 | v0.3.1 |
| --------- | ------ | ------ |
| Jobs that do nothing, `MaxConcurrency` 10 | 7.2/s | 3,830/s |
| Jobs that do nothing, `MaxConcurrency` 100 | 1,122/s | 13,022/s |
| Jobs that take 10 ms, `MaxConcurrency` 100 | 63/s | 6,039/s |
| Jobs of one of three tiers, the others empty | 24/s | 8,801/s |
| Jobs among others whose handler is full | 1.6/s | 13,208/s |
| Jobs that fail once | 99/s | 2,902/s |
| One-time jobs that are due at once | 323/s | 5,130/s |
| Tasks of workflows | 303/s | 5,652/s |

## Dependencies

| Dependency                                                | Purpose                       |
| --------------------------------------------------------- | ----------------------------- |
| [rueidis](https://github.com/redis/rueidis)               | High-performance Redis client |
| [ants](https://github.com/panjf2000/ants)                 | Goroutine pools               |
| [robfig/cron](https://github.com/robfig/cron)             | Cron expression parsing       |
| [sonic](https://github.com/bytedance/sonic)               | Fast JSON serialization       |
| [xid](https://github.com/rs/xid)                          | Globally unique ID generation |
| [go-chainedlog](https://github.com/FDK0901/go-chainedlog) | Structured logging            |
| [websocket](https://github.com/coder/websocket)           | WebSocket server              |
| [bubbletea](https://github.com/charmbracelet/bubbletea)   | Terminal UI framework         |
| [koanf](https://github.com/knadh/koanf)                   | Configuration management      |
| [grpc-go](https://google.golang.org/grpc)                 | gRPC server                   |
| [vtprotobuf](https://github.com/planetscale/vtprotobuf)   | Optimized protobuf marshaling |
| [pq](https://github.com/lib/pq)                           | PostgreSQL driver (tests and examples) |
