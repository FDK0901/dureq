# Quickstart

This guide walks through installing dureq, starting a server, enqueuing jobs, and setting up scheduled tasks.

## Install

```bash
go get github.com/FDK0901/dureq
```

Requires Go 1.22+ and Redis 7+.

## Server

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "os"
    "os/signal"

    "github.com/FDK0901/dureq/internal/server"
    "github.com/FDK0901/dureq/pkg/types"
)

func main() {
    srv, err := server.New(
        server.WithRedisURL("redis://localhost:6379"),
        server.WithMaxConcurrency(50),
    )
    if err != nil {
        log.Fatal(err)
    }

    // Register a handler for the "send-email" task type.
    srv.RegisterHandler(types.HandlerDefinition{
        TaskType: "send-email",
        Handler: func(ctx context.Context, payload json.RawMessage) error {
            var p struct{ To, Subject string }
            json.Unmarshal(payload, &p)
            // ... send the email ...
            return nil
        },
        RetryPolicy: types.DefaultRetryPolicy(),
    })

    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
    defer stop()

    if err := srv.Start(ctx); err != nil {
        log.Fatal(err)
    }

    <-ctx.Done()
    srv.Stop()
}
```

### Mode Selection

By default the server runs all subsystems (`ModeFull`). In production you may want to split roles:

```go
// Worker-only node (no scheduler, no orchestrator, no HTTP API).
server.New(server.WithRedisURL(url), server.WithMode(server.ModeQueue))

// Scheduler + orchestrator without processing jobs.
server.New(server.WithRedisURL(url), server.WithMode(server.ModeScheduler|server.ModeWorkflow))
```

## Client

The client connects directly to Redis. No running server is required for enqueue.

```go
c, err := client.New(client.WithRedisURL("redis://localhost:6379"))
if err != nil {
    log.Fatal(err)
}
defer c.Close()

job, err := c.Enqueue(ctx, "send-email", map[string]string{
    "to":      "alice@example.com",
    "subject": "Hello",
})
fmt.Println("enqueued:", job.ID)
```

## Scheduled Jobs

Use `EnqueueScheduled` with a `Schedule` to run jobs on a timer.

```go
cron := "*/5 * * * *" // every 5 minutes
job, err := c.EnqueueScheduled(ctx, &client.EnqueueRequest{
    TaskType: "cleanup",
    Payload:  json.RawMessage(`{}`),
    Schedule: types.Schedule{
        Type:     types.ScheduleCron,
        CronExpr: &cron,
    },
})
```

Other schedule types:

| Type | Field | Example |
|------|-------|---------|
| `ONE_TIME` | `RunAt` | Run once at a future time |
| `DURATION` | `Interval` | Repeat every 30s |
| `CRON` | `CronExpr` | Standard cron expression |
| `DAILY` | `RegularInterval` + `AtTimes` | Every N days at specific times |
| `WEEKLY` | `IncludedDays` + `AtTimes` | Specific weekdays |
| `MONTHLY` | `IncludedDays` + `AtTimes` | Specific month days |

## Overlap Policy

For recurring schedules, set `OverlapPolicy` to control what happens when a new firing occurs while the previous run is still active:

- `ALLOW_ALL` -- dispatch regardless (default)
- `SKIP` -- skip if any run is active
- `BUFFER_ONE` -- buffer at most one pending dispatch
- `REPLACE` -- cancel active run, start new

## Handler Context

Inside a handler, use context accessors to read execution metadata:

```go
func handler(ctx context.Context, payload json.RawMessage) error {
    jobID   := types.GetJobID(ctx)
    runID   := types.GetRunID(ctx)
    attempt := types.GetAttempt(ctx)
    headers := types.GetHeaders(ctx)
    // ...
    return nil
}
```

## Next Steps

- [Workflow Model](workflow-model.md) -- DAG-based task orchestration
- [Batch Model](batch-model.md) -- parallel item processing
- [Concurrency](concurrency.md) -- rate limiting and priority
- [Execution Guarantees](guarantees.md) -- delivery semantics
- [Operations Playbook](operations-playbook.md) -- monitoring and repair
