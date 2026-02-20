# dureq

A distributed, actor-based job scheduling and workflow orchestration system built in Go.

dureq is short for durable redis execution queue.

dureq is a full rewrite of the original pull-based model, now using a **push-based actor model** powered by [Hollywood](https://github.com/anthdm/hollywood) for inter-node communication. It supports one-time and recurring job scheduling, DAG-based workflows, batch processing, automatic retries, distributed locking, leader election, and real-time event streaming — all backed by Redis.
Heavily inspired by [Asynq](https://github.com/hibiken/asynq), [gocron](https://github.com/go-co-op/gocron), and [river](https://github.com/riverqueue/river).

## Features

- **Flexible Scheduling** — Immediate, one-time, interval-based, cron, daily, weekly, and monthly schedules
- **Workflow Orchestration** — DAG-based task dependencies with automatic state advancement
- **Batch Processing** — Concurrent item execution with optional preprocessing, chunking, and failure policies
- **Actor Model** — Cluster-wide singleton actors (scheduler, dispatcher, orchestrator) with per-node workers
- **Monitoring APIs** — HTTP REST and gRPC APIs for full cluster observability and management
- **Web Dashboard** — React-based UI ([durequi](https://github.com/FDK0901/durequi)) for visual monitoring
- **Terminal UI** — Interactive TUI (`dureqctl`) for real-time cluster monitoring
- **Distributed Locking** — Redis-based locks with auto-extension for safe concurrent execution
- **Leader Election** — Automatic singleton failover across nodes
- **Priority Queues** — Four priority levels (Low, Normal, High, Critical) with configurable tiers
- **Retry Policies** — Exponential backoff with jitter, error classification (retryable, non-retryable, rate-limited)
- **Overlap Policies** — Control concurrent runs for recurring jobs (allow all, skip, buffer one, buffer all)
- **Catchup / Backfill** — Recover missed executions within a configurable window
- **Middleware** — Composable handler middleware chains
- **Progress Reporting** — In-flight progress updates from workers
- **WebSocket Live Updates** — Real-time event push to web dashboard via WebSocket, with automatic polling fallback
- **Event Streaming** — Real-time lifecycle events via Redis Pub/Sub and Streams
- **Multi-Tenancy** — Key prefix isolation per tenant
- **Client SDKs** — Go

## Architecture

```
                    ┌─────────────────────┐
                    │     duraqui (UI)    │
                    │ React Web Dashboard │
                    └─────────┬───────────┘
                              │
               HTTP REST ─────┤───── gRPC
                              │
Client (Go)          │
  │                 ┌─────────▼───────────┐
  │  enqueue        │      dureqd         │
  │  via Redis      │  (server daemon)    │
  │                 │                     │
  │                 │  :8080 HTTP API     │
  │                 │  :9090 gRPC API     │
  │                 └─────────┬───────────┘
  │                           │
  ▼                           ▼
┌──────────────────────────────────────────────────────┐
│                      Redis                           │
│  Jobs · Schedules · Streams · Locks · Events         │
└──────────────────────────────────────────────────────┘
  │                         ▲
  ▼  Pub/Sub notify         │  persist results
┌──────────────────────────────────────────────────────┐
│  Node (Hollywood Actor Engine)                       │
│                                                      │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐   │
│  │  Notifier   │→ │  Dispatcher  │→ │  Worker    │   │
│  │  (per-node) │  │  (singleton) │  │  Supervisor│   │
│  └─────────────┘  └──────────────┘  │  (per-node)│   │
│                          ▲          └─────┬──────┘   │
│  ┌─────────────┐         │                │          │
│  │  Scheduler  │─────────┘          ┌─────▼──────┐   │
│  │  (singleton)│                    │  Worker(s) │   │
│  └─────────────┘                    │  (per-job) │   │
│                                     └─────┬──────┘   │
│  ┌──────────────┐  ┌──────────────┐       │          │
│  │ Orchestrator │← │ EventBridge  │←──────┘          │
│  │  (singleton) │  │  (per-node)  │                  │
│  └──────────────┘  └──────────────┘                  │
│                                                      │
│  ┌───────────────┐  ┌───────────────┐                │
│  │   Heartbeat   │  │ ClusterGuard  │                │
│  │  (per-node)   │  │  (per-node)   │                │
│  └───────────────┘  └───────────────┘                │
└──────────────────────────────────────────────────────┘
         ▲
         │  HTTP client
┌────────┴────────┐
│    dureqctl     │
│  (terminal UI)  │
└─────────────────┘
```

**Singleton actors** run on the elected leader node only:
- **SchedulerActor** — scans due schedules every second, dispatches jobs, detects orphaned runs, backfills missed executions
- **DispatcherActor** — routes jobs to nodes based on capacity, task-type affinity, and priority
- **OrchestratorActor** — advances workflow DAG and batch state machines on job completion/failure

**Per-node actors**:
- **WorkerSupervisorActor** — manages a pool of WorkerActors, enforces concurrency limits
- **WorkerActor** — executes a single job handler, manages heartbeats and progress
- **EventBridgeActor** — persists results to Redis, forwards domain events to OrchestratorActor
- **NotifierActor** — subscribes to Redis Pub/Sub for new job notifications
- **HeartbeatActor** — maintains node liveness
- **ClusterGuardActor** — monitors singleton health, triggers re-election if needed

## Getting Started

### Prerequisites

- Go 1.25+
- Redis 7+
- Protocol Buffers compiler (for regenerating proto files)

### Installation

```bash
go get github.com/FDK0901/dureq
```

### Quick Example

**Server:**

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"

    "github.com/FDK0901/dureq/internal/server"
    "github.com/FDK0901/dureq/pkg/types"
)

func main() {
    srv, err := server.New(
        server.WithRedisURL("redis://localhost:6379"),
        server.WithRedisDB(15),
        server.WithNodeID("node-1"),
        server.WithMaxConcurrency(10),
    )
    if err != nil {
        log.Fatal(err)
    }

    srv.RegisterHandler(types.HandlerDefinition{
        TaskType:    "email.send",
        Concurrency: 5,
        Handler: func(ctx context.Context, payload json.RawMessage) error {
            fmt.Printf("Sending email: %s\n", payload)
            return nil
        },
        RetryPolicy: types.DefaultRetryPolicy(),
    })

    ctx := context.Background()
    if err := srv.Start(ctx); err != nil {
        log.Fatal(err)
    }
}
```

**Client (Go):**

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/FDK0901/dureq/client"
    "github.com/FDK0901/dureq/pkg/types"
)

func main() {
    cli, err := client.New(client.WithRedisURL("redis://localhost:6379"))
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()

    // Immediate job
    job, _ := cli.EnqueueImmediate(ctx, &client.EnqueueRequest{
        TaskType: "email.send",
        Payload:  []byte(`{"to":"user@example.com","subject":"Hello"}`),
    })
    fmt.Println("Enqueued:", job.ID)

    // Recurring job (every 30 seconds)
    start := time.Now().Add(time.Minute)
    end := start.Add(24 * time.Hour)
    cli.EnqueueScheduled(ctx, &client.EnqueueRequest{
        TaskType: "report.generate",
        Payload:  []byte(`{}`),
        Schedule: types.Schedule{
            Type:     types.ScheduleDuration,
            Interval: &types.Duration{Duration: 30 * time.Second},
            StartsAt: &start,
            EndsAt:   &end,
        },
    })
}
```

## dureqd — Server Daemon

`dureqd` is the main server process that runs the full dureq cluster node. It starts the actor engine, exposes HTTP and gRPC monitoring APIs, and handles graceful shutdown.

```bash
go run ./cmd/dureqd
```

### Configuration

`dureqd` reads configuration from a YAML file. Set the path via `DUREQ_CONFIG_PATH` environment variable (default: `cmd/dureqd/config.yaml`).

```yaml
dureqd:
  nodeId: ""              # Auto-generated if empty
  apiAddress: ":8080"     # HTTP monitoring API
  grpcAddress: ":9090"    # gRPC monitoring API
  concurrency: 100        # Max concurrent workers
  prefix: ""              # Redis key prefix (multi-tenancy)

redis:
  url: "redis://localhost:6379"
  password: ""
  db: 0
  poolSize: 100
```

### What dureqd provides

| Port    | Protocol  | Purpose                                                       |
| ------- | --------- | ------------------------------------------------------------- |
| `:8080` | HTTP      | REST API for monitoring, management, and the web dashboard    |
| `:8080` | WebSocket | Real-time event push at `/api/ws` (Redis Pub/Sub → WebSocket) |
| `:9090` | gRPC      | gRPC API with the same capabilities (with reflection enabled) |

## dureqctl — Terminal UI

`dureqctl` is an interactive terminal UI for monitoring and managing dureq clusters in real-time. Built with [Bubble Tea](https://github.com/charmbracelet/bubbletea).

```bash
go run ./cmd/dureqctl
```

### Flags

| Flag       | Default                 | Description               |
| ---------- | ----------------------- | ------------------------- |
| `-api`     | `http://localhost:8080` | dureqd monitoring API URL |
| `-refresh` | `5s`                    | Data refresh interval     |

### Pages

| Key | Page      | Description                                                            |
| --- | --------- | ---------------------------------------------------------------------- |
| `1` | Dashboard | Cluster overview — active nodes, schedules, runs, job counts by status |
| `2` | Jobs      | Job list with status filtering, cancel, retry, and detail view         |
| `3` | Workflows | Workflow instances with task progress, cancel, retry, and detail view  |
| `4` | Batches   | Batch instances with progress bar, cancel, retry, and detail view      |
| `5` | Runs      | Historical job run history with status filtering                       |
| `6` | Nodes     | Active worker nodes with pool stats and heartbeat                      |
| `7` | Schedules | Active recurring schedules with next run times                         |
| `8` | Queues    | Priority queue tiers with size, weight, and pause/resume controls      |
| `9` | DLQ       | Dead letter queue messages                                             |

### Keyboard Controls

| Key             | Action                                                    |
| --------------- | --------------------------------------------------------- |
| `j/k` or arrows | Navigate list                                             |
| `enter`         | View detail (Jobs, Workflows, Batches)                    |
| `c`             | Cancel selected item (Jobs, Workflows, Batches)           |
| `r`             | Retry selected item (Jobs, Workflows, Batches) or refresh |
| `f`             | Cycle status filter (Jobs, Workflows, Batches, Runs)      |
| `p`             | Pause selected queue (Queues)                             |
| `u`             | Resume selected queue (Queues)                            |
| `tab`           | Next page                                                 |
| `1`-`9`         | Jump to page                                              |
| `esc`           | Back from detail view                                     |
| `q` / `ctrl+c`  | Quit                                                      |

## durequi — Web Dashboard

A React-based web dashboard for visual monitoring and management of dureq clusters. Connects to the `dureqd` HTTP and WebSocket APIs.

- **Real-time updates** via WebSocket (`/api/ws`) — events push to the browser and invalidate React Query caches, so data refreshes instantly without polling
- **Automatic fallback** — if WebSocket disconnects, the dashboard seamlessly falls back to configurable polling (default 5s)
- **Connection indicator** — sidebar shows live connection status (green = Live, yellow = Connecting, gray = Polling)
- **Configurable** — WebSocket can be toggled on/off from the Settings page

Repository: [github.com/FDK0901/durequi](https://github.com/FDK0901/durequi)

## Monitoring API

Both HTTP and gRPC APIs expose the same set of operations. The HTTP API serves the web dashboard and CLI, while the gRPC API provides type-safe access with protobuf definitions.

### Endpoints

**Jobs:**

| Method   | Path                         | Description                                                      |
| -------- | ---------------------------- | ---------------------------------------------------------------- |
| `GET`    | `/api/jobs`                  | List jobs (paginated, filterable by status/task_type/tag/search) |
| `GET`    | `/api/jobs/{jobID}`          | Get job details                                                  |
| `DELETE` | `/api/jobs/{jobID}`          | Delete a job                                                     |
| `POST`   | `/api/jobs/{jobID}/cancel`   | Cancel a running job                                             |
| `POST`   | `/api/jobs/{jobID}/retry`    | Retry a failed job                                               |
| `PUT`    | `/api/jobs/{jobID}/payload`  | Update job payload (pending/scheduled only)                      |
| `GET`    | `/api/jobs/{jobID}/events`   | List job event history                                           |
| `GET`    | `/api/jobs/{jobID}/runs`     | List job run history                                             |
| `GET`    | `/api/jobs/{jobID}/progress` | Get progress for all active runs                                 |

**Schedules:**

| Method | Path                     | Description           |
| ------ | ------------------------ | --------------------- |
| `GET`  | `/api/schedules`         | List active schedules |
| `GET`  | `/api/schedules/{jobID}` | Get schedule details  |

**Nodes:**

| Method | Path                  | Description                  |
| ------ | --------------------- | ---------------------------- |
| `GET`  | `/api/nodes`          | List registered worker nodes |
| `GET`  | `/api/nodes/{nodeID}` | Get node details             |

**Runs:**

| Method | Path                         | Description      |
| ------ | ---------------------------- | ---------------- |
| `GET`  | `/api/runs`                  | List active runs |
| `GET`  | `/api/runs/{runID}`          | Get run details  |
| `GET`  | `/api/runs/{runID}/progress` | Get run progress |

**History:**

| Method | Path                  | Description               |
| ------ | --------------------- | ------------------------- |
| `GET`  | `/api/history/runs`   | Paginated historical runs |
| `GET`  | `/api/history/events` | Paginated job events      |

**Workflows:**

| Method | Path                                 | Description                   |
| ------ | ------------------------------------ | ----------------------------- |
| `GET`  | `/api/workflows`                     | List workflow instances       |
| `GET`  | `/api/workflows/{workflowID}`        | Get workflow details          |
| `POST` | `/api/workflows/{workflowID}/cancel` | Cancel workflow and all tasks |
| `POST` | `/api/workflows/{workflowID}/retry`  | Retry entire workflow         |

**Batches:**

| Method | Path                                      | Description                                |
| ------ | ----------------------------------------- | ------------------------------------------ |
| `GET`  | `/api/batches`                            | List batch instances                       |
| `GET`  | `/api/batches/{batchID}`                  | Get batch details                          |
| `GET`  | `/api/batches/{batchID}/results`          | List all item results                      |
| `GET`  | `/api/batches/{batchID}/results/{itemID}` | Get single item result                     |
| `POST` | `/api/batches/{batchID}/cancel`           | Cancel batch                               |
| `POST` | `/api/batches/{batchID}/retry`            | Retry batch (optional `retry_failed_only`) |

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

Bulk request body: `{"ids": ["id1", "id2"]}` or `{"status": "failed"}`

**Queues & Groups:**

| Method | Path                            | Description                                   |
| ------ | ------------------------------- | --------------------------------------------- |
| `GET`  | `/api/queues`                   | List queue tiers (name, weight, paused, size) |
| `POST` | `/api/queues/{tierName}/pause`  | Pause a queue                                 |
| `POST` | `/api/queues/{tierName}/resume` | Resume a queue                                |
| `GET`  | `/api/groups`                   | List active aggregation groups                |

**Statistics & Health:**

| Method | Path               | Description                                        |
| ------ | ------------------ | -------------------------------------------------- |
| `GET`  | `/api/stats`       | Cluster stats (job counts, nodes, runs, schedules) |
| `GET`  | `/api/stats/daily` | Daily aggregated statistics                        |
| `GET`  | `/api/redis/info`  | Redis server info                                  |
| `GET`  | `/api/health`      | Health check                                       |

**WebSocket (Real-time Events):**

| Path      | Protocol  | Description                                                                   |
| --------- | --------- | ----------------------------------------------------------------------------- |
| `/api/ws` | WebSocket | Real-time event stream — all domain events pushed as JSON `JobEvent` messages |

The WebSocket endpoint subscribes to the Redis Pub/Sub `{prefix}:events` channel and fans out every event to all connected clients. Events include all 29+ types (job, workflow, batch, node, schedule, leader). The durequi dashboard uses this to invalidate React Query caches for instant UI updates, falling back to polling when disconnected.

**Pagination** — query params: `limit` (default 10, max 100), `offset`, `sort` (`newest` or `oldest`)

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

## Workflows

Define DAG-based workflows where tasks execute in dependency order:

```go
cli.CreateWorkflow(ctx, &types.WorkflowDefinition{
    Name: "user-onboarding",
    Tasks: []types.WorkflowTask{
        {Name: "create-account", TaskType: "account.create", Payload: payload},
        {Name: "send-welcome",   TaskType: "email.welcome",  DependsOn: []string{"create-account"}},
        {Name: "setup-profile",  TaskType: "profile.setup",  DependsOn: []string{"create-account"}},
        {Name: "notify-admin",   TaskType: "admin.notify",   DependsOn: []string{"send-welcome", "setup-profile"}},
    },
})
```

The orchestrator automatically validates the DAG (no cycles, all dependencies exist), dispatches root tasks first, and advances downstream tasks as dependencies complete.

## Batches

Process collections of items concurrently with optional preprocessing:

```go
cli.CreateBatch(ctx, &types.BatchDefinition{
    Name:            "process-images",
    OnetimeTaskType: types.TaskTypePtr("image.prepare"),  // optional preprocessing
    ItemTaskType:    "image.resize",
    Items: []types.BatchItem{
        {ID: "img-1", Payload: json.RawMessage(`{"url":"..."}`)},
        {ID: "img-2", Payload: json.RawMessage(`{"url":"..."}`)},
    },
    ChunkSize:     100,
    FailurePolicy: types.BatchFailureContinueOnError,
})
```

Failure policies:
- `CONTINUE_ON_ERROR` — continue processing remaining items even if some fail
- `FAIL_ON_ERROR` — stop the entire batch on first failure

## Error Handling & Retries

```go
// Default retry policy: 3 attempts, 5s initial delay, 5m max, 2x multiplier, 10% jitter
types.DefaultRetryPolicy()

// Custom retry policy
&types.RetryPolicy{
    MaxAttempts:  5,
    InitialDelay: 10 * time.Second,
    MaxDelay:     10 * time.Minute,
    Multiplier:   3.0,
    Jitter:       0.2,
}
```

Classify errors to control retry behavior:

```go
// Retryable (default) — transient failures, will retry
return &types.RetryableError{Err: err}

// Non-retryable — permanent failures, skip remaining retries
return &types.NonRetryableError{Err: err}

// Rate-limited — retry after a specific duration
return &types.RateLimitedError{Err: err, RetryAfter: 30 * time.Second}
```

## Handler Context

Inside a handler, access job metadata via context helpers:

```go
func handler(ctx context.Context, payload json.RawMessage) error {
    jobID    := types.GetJobID(ctx)
    runID    := types.GetRunID(ctx)
    attempt  := types.GetAttempt(ctx)
    maxRetry := types.GetMaxRetry(ctx)
    taskType := types.GetTaskType(ctx)
    priority := types.GetPriority(ctx)
    nodeID   := types.GetNodeID(ctx)
    headers  := types.GetHeaders(ctx)

    // Report progress
    types.ReportProgress(ctx, map[string]any{"percent": 50})

    return nil
}
```

## Middleware

```go
func loggingMiddleware(next types.HandlerFunc) types.HandlerFunc {
    return func(ctx context.Context, payload json.RawMessage) error {
        log.Printf("Starting job %s", types.GetJobID(ctx))
        err := next(ctx, payload)
        log.Printf("Finished job %s (err=%v)", types.GetJobID(ctx), err)
        return err
    }
}

srv.RegisterHandler(types.HandlerDefinition{
    TaskType:    "email.send",
    Handler:     emailHandler,
    Middlewares: []types.MiddlewareFunc{loggingMiddleware},
})
```

## Configuration

### Server Options

| Option                      | Default        | Description                        |
| --------------------------- | -------------- | ---------------------------------- |
| `WithRedisURL(url)`         | —              | Redis connection URL               |
| `WithRedisDB(db)`           | `0`            | Redis database number              |
| `WithRedisPassword(pw)`     | —              | Redis password                     |
| `WithRedisPoolSize(n)`      | —              | Connection pool size               |
| `WithNodeID(id)`            | auto-generated | Unique node identifier             |
| `WithListenAddr(addr)`      | —              | Address for remote actor RPC       |
| `WithClusterRegion(region)` | —              | Region for multi-region deployment |
| `WithMaxConcurrency(n)`     | `100`          | Maximum concurrent workers         |
| `WithShutdownTimeout(d)`    | `30s`          | Graceful shutdown timeout          |
| `WithLockTTL(d)`            | `30s`          | Distributed lock TTL               |
| `WithLockAutoExtend(d)`     | `LockTTL/3`    | Lock auto-extension interval       |
| `WithKeyPrefix(prefix)`     | —              | Redis key prefix (multi-tenancy)   |
| `WithLogger(logger)`        | —              | Custom logger                      |

### Client Options

| Option                     | Default | Description                        |
| -------------------------- | ------- | ---------------------------------- |
| `WithRedisURL(url)`        | —       | Redis connection URL               |
| `WithRedisDB(db)`          | `0`     | Redis database number              |
| `WithRedisPassword(pw)`    | —       | Redis password                     |
| `WithRedisPoolSize(n)`     | —       | Connection pool size               |
| `WithKeyPrefix(prefix)`    | —       | Key namespace for multi-tenancy    |
| `WithPriorityTiers(tiers)` | —       | Custom priority tier mapping       |
| `WithClusterAddrs(addrs)`  | —       | Redis Cluster addresses            |
| `WithStore(store)`         | —       | Use an existing store (in-process) |

## Event Types

dureq emits lifecycle events via Redis Streams for monitoring and integration:

| Category     | Events                                                                                                                     |
| ------------ | -------------------------------------------------------------------------------------------------------------------------- |
| **Job**      | `enqueued`, `scheduled`, `dispatched`, `started`, `completed`, `failed`, `retrying`, `dead`, `cancelled`                   |
| **Schedule** | `created`, `removed`                                                                                                       |
| **Workflow** | `started`, `completed`, `failed`, `cancelled`, `task.dispatched`, `task.completed`, `task.failed`, `timed_out`, `retrying` |
| **Batch**    | `started`, `completed`, `failed`, `cancelled`, `item.completed`, `item.failed`, `progress`, `timed_out`, `retrying`        |
| **Node**     | `joined`, `left`, `crash_detected`                                                                                         |
| **Leader**   | `elected`, `lost`                                                                                                          |
| **Recovery** | `job.auto_recovered`, `schedule_to_start_timeout`                                                                          |

## Project Structure

```
dureq/
├── cmd/
│   ├── dureqd/             # Server daemon (actor engine + HTTP/gRPC APIs)
│   │   ├── main.go
│   │   ├── config.yaml     # Default configuration
│   │   └── config/         # YAML config loading (Koanf)
│   └── dureqctl/           # Interactive terminal UI (Bubble Tea)
│       ├── main.go
│       └── teamodel/       # TUI model, views, and key handling
├── client/                 # Go client SDK
├── pkg/types/              # Public domain types (Job, Schedule, Workflow, Batch, etc.)
├── internal/
│   ├── server/             # v2 actor-based server entry point
│   ├── v1server/           # v1 pull-based server (legacy)
│   ├── actors/             # All actor implementations
│   ├── monitor/            # HTTP REST + gRPC monitoring APIs
│   ├── api/                # HTTP client for dureqctl
│   ├── store/              # Redis persistence layer (Lua scripts, streams, pagination)
│   ├── messages/           # Internal message types
│   ├── scheduler/          # Standalone scheduler component
│   ├── dispatcher/         # Standalone dispatcher component
│   ├── worker/             # Standalone worker component
│   ├── workflow/           # DAG validation & topological sort
│   ├── lock/               # Distributed locking
│   ├── cache/              # In-memory schedule cache
│   ├── election/           # Leader election
│   └── provider/           # Service providers (Redis client factory)
├── proto/
│   └── dureq/
│       ├── messages.proto          # Inter-actor message definitions
│       └── monitor/v1/             # gRPC monitoring service definitions
│           ├── service.proto       # 37 RPC methods
│           ├── job.proto
│           ├── workflow.proto
│           ├── batch.proto
│           ├── bulk.proto
│           ├── node.proto
│           ├── queue.proto
│           ├── stats.proto
│           └── common.proto
├── gen/dureq/              # Generated protobuf code
├── examples/               # Runnable examples
├── benchmarks/             # Performance benchmarks
├── Makefile                # Proto generation
└── go.mod
```

## Examples

The [examples/](examples/) directory contains runnable demos:

| Example               | Description                     |
| --------------------- | ------------------------------- |
| `festival/`           | Recurring scheduled jobs        |
| `batch/`              | Batch item processing           |
| `batch_with_mux/`     | Batch with HTTP server          |
| `workflow/`           | DAG workflow execution          |
| `workflow_with_mux/`  | Workflow with HTTP server       |
| `onetimeat/`          | One-time scheduled execution    |
| `heartbeat_progress/` | Progress reporting from workers |
| `overlap_policy/`     | Overlap policy demonstration    |

```bash
cd examples/festival
go run main.go
```

## Building

```bash
# Build all packages
go build ./...

# Run the server daemon
go run ./cmd/dureqd

# Run the terminal UI
go run ./cmd/dureqctl -api http://localhost:8080

# Run tests
go test ./...

# Run benchmarks
cd benchmarks && go test -bench=. ./...

# Regenerate protobuf code
make genproto
```

## Dependencies

| Dependency                                                | Purpose                           |
| --------------------------------------------------------- | --------------------------------- |
| [hollywood](https://github.com/anthdm/hollywood)          | Actor framework with clustering   |
| [rueidis](https://github.com/redis/rueidis)               | High-performance Redis client     |
| [robfig/cron](https://github.com/robfig/cron)             | Cron expression parsing           |
| [vtprotobuf](https://github.com/planetscale/vtprotobuf)   | Optimized protobuf marshaling     |
| [sonic](https://github.com/bytedance/sonic)               | Fast JSON serialization           |
| [ants](https://github.com/panjf2000/ants)                 | Goroutine pool                    |
| [xid](https://github.com/rs/xid)                          | Globally unique ID generation     |
| [go-chainedlog](https://github.com/FDK0901/go-chainedlog) | Structured logging                |
| [bubbletea](https://github.com/charmbracelet/bubbletea)   | Terminal UI framework (dureqctl)  |
| [koanf](https://github.com/knadh/koanf)                   | Configuration management (dureqd) |
| [grpc-go](https://google.golang.org/grpc)                 | gRPC server and reflection        |
| [websocket](https://nhooyr.io/websocket)                  | WebSocket server (real-time push) |

