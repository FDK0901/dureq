# Concurrency Control

dureq provides per-key concurrency limiting, priority tiers, and queue
pause/resume to control how jobs are dispatched and executed.

## Concurrency Keys

A `ConcurrencyKey` limits how many jobs sharing the same key can run
simultaneously. Attach one or more keys to a job at enqueue time.

```go
job, err := client.EnqueueScheduled(ctx, &client.EnqueueRequest{
    TaskType: "call-stripe",
    Payload:  payload,
    Schedule: types.Schedule{Type: types.ScheduleImmediate},
    ConcurrencyKeys: []types.ConcurrencyKey{
        {Key: "stripe-api", MaxConcurrency: 5},
        {Key: "tenant:acme", MaxConcurrency: 1},
    },
})
```

A job must satisfy **all** its concurrency keys before execution begins.

### How It Works

Each concurrency key is backed by a Redis sorted set used as a semaphore:

1. Before executing a job, the worker attempts to acquire a slot in each key's
   sorted set (`ZADD NX`, scored by timestamp).
2. If any key is at capacity, the job is re-enqueued to the work stream with
   a short delay. No work is lost.
3. On completion (success or failure), the worker releases the slot (`ZREM`).

### Orphan Cleanup

If a worker crashes while holding a concurrency slot, the scheduler's periodic
orphan detection removes stale entries from the sorted set. Entries older than
the heartbeat timeout (default 30s) are considered orphaned and released.

### Multiple Keys

A job can hold multiple concurrency keys simultaneously. This enables
hierarchical rate limiting:

```go
ConcurrencyKeys: []types.ConcurrencyKey{
    {Key: "global-api",      MaxConcurrency: 100},
    {Key: "tenant:acme",     MaxConcurrency: 10},
    {Key: "tenant:acme:api", MaxConcurrency: 2},
}
```

All keys must have available capacity for the job to proceed.

## Priority Tiers

By default, all jobs share a single Redis Stream. Use `WithPriorityTiers` to
split work across multiple streams with weighted consumption.

```go
srv, _ := server.New(
    server.WithRedisURL(url),
    server.WithPriorityTiers([]store.TierConfig{
        {Name: "critical", Weight: 10},
        {Name: "normal",   Weight: 5},
        {Name: "bulk",     Weight: 1},
    }),
)
```

Workers read from higher-weight tiers more frequently. A job's `Priority` value
(1--10) is mapped to a tier automatically:

| Priority | Tier |
|----------|------|
| 8--10 | First configured tier (highest weight) |
| 3--7 | Middle tier |
| 1--2 | Last configured tier (lowest weight) |

The mapping scales proportionally when more than three tiers are configured.

Set priority on enqueue via the `Priority` field. Built-in constants:
`PriorityLow` (1), `PriorityNormal` (5), `PriorityHigh` (8), `PriorityCritical` (10).

## Queue Pause / Resume

Pause a tier to stop all dispatch from that stream. In-flight jobs continue
to completion but no new jobs are picked up.

```
POST /api/queues/{tierName}/pause
POST /api/queues/{tierName}/resume
```

Or programmatically: `store.PauseQueue(ctx, "bulk")` / `store.ResumeQueue(ctx, "bulk")`.
Use during deployments to halt a priority level without affecting other tiers.

## Node-Level Concurrency

Each handler registration can set a per-node concurrency limit:

```go
srv.RegisterHandler(types.HandlerDefinition{
    TaskType:    "heavy-task",
    Handler:     handler,
    Concurrency: 5, // max 5 concurrent on this node (0 = pool default)
})
```

The overall pool size is set via `WithMaxConcurrency` (default: 100).

## Re-enqueue on Blocked

When a job cannot acquire its concurrency slots, it is not dropped. The worker
re-publishes the message to the work stream with a brief delay. The message
re-enters the normal dispatch flow and will be retried on the next read cycle.

This means concurrency blocking is transparent to the job -- it simply waits
longer before execution begins.
