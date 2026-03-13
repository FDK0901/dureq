# Frequently Asked Questions

## Why Not Exactly-Once?

**Q: dureq claims "duplicate-suppressed execution." Why not exactly-once?**

True exactly-once requires that a handler's side effects (API calls, DB writes, emails)
happen exactly once, even across retries and crashes. This is impossible to guarantee
at the infrastructure level without cooperation from the handler.

dureq guarantees that **at most one instance of your handler runs per RunID at any given
moment** (via per-run Redis lock). But if the lock expires (Redis slow, network partition)
or the worker crashes after committing a side effect but before marking the job complete,
the side effect can happen more than once.

**What dureq provides:**
- At-least-once delivery (XAUTOCLAIM reclaims crashed messages)
- Duplicate-suppressed handler start (per-run lock)
- UniqueKey / RequestID for enqueue-level dedup
- `pkg/sideeffect.Step()` — at-most-once side effect execution with cached results

**What you must provide:**
- Use `sideeffect.Step()` or `sideeffect.ExternalCall()` for external API calls, OR
- Idempotent handlers using stable business keys for external API calls, OR
- Transactional completion: commit DB writes and job completion in the same transaction

## When Can I Get Exactly-Once Outcomes?

When all three conditions are met:

1. External API calls use idempotency keys based on stable business identifiers
2. External APIs accept idempotency keys (or are naturally idempotent)
3. Long-running handlers periodically checkpoint state to tolerate restarts

## What Happens During Leader Failover?

The scheduler runs only on the elected leader. During failover:

1. Leader A stops (crash or graceful shutdown)
2. Leader election detects TTL expiry (default: 10s)
3. Leader B wins election, starts scheduler
4. **Gap**: Schedules due during the failover window are processed on B's first tick

**Risk**: If A dispatched a job but crashed before advancing the schedule,
B will re-dispatch the same schedule entry with a new RunID.
Both RunIDs execute (different locks), so the job fires twice.

**Mitigation**: Use `UniqueKey` on the job, or make the handler idempotent.

## What Happens When a Worker Crashes?

1. Worker stops heartbeating
2. After 5 minutes, XAUTOCLAIM reclaims its pending messages
3. Another worker picks up the message
4. Per-run lock prevents re-execution if original worker is somehow still running
5. Orphan detection (every 30s on leader) marks orphaned runs as failed and triggers retry

## Are Workflow Signals Reliable?

Workflow signals are stored in a durable Redis Stream (not Pub/Sub), so they
survive orchestrator restarts. However, `ConsumeSignals()` reads signals and
then deletes them (XDEL) in a non-atomic sequence. If the consumer crashes
**after** XDEL but **before** processing the signal, that signal is lost.

The effective guarantee is **at-most-once delivery**. Make signal handlers
idempotent where possible, and design workflows to tolerate a missed signal
(e.g., timeout-based fallback).

**Note**: The orchestrator does NOT consume signals — user code calls
`ConsumeSignals()` from within signal handlers.

## Are Cancellation Signals Reliable?

No. Cancellation uses Redis Pub/Sub (fire-and-forget). If the target worker is offline
when the PUBLISH arrives, the cancellation is permanently lost. The job continues until
completion or timeout. There is currently no cancellation flag polling mechanism —
cancellation relies solely on the Pub/Sub delivery.

## How Does dureq Compare to River?

| Feature | dureq | River |
|---------|-------|-------|
| Backend | Redis | PostgreSQL |
| Delivery | At-least-once + lock dedup | Exactly-once via SQL transaction |
| Completion | Pipelined Redis commands | Single SQL transaction |
| Workflows | Built-in DAG orchestration | Not built-in |
| Cancellation | Best-effort Pub/Sub | Reliable (polled from DB) |

River achieves true exactly-once because job state and completion are in the same
PostgreSQL transaction. dureq trades this for Redis's speed and built-in workflow
orchestration.

## How Does dureq Compare to Temporal?

| Feature | dureq | Temporal |
|---------|-------|----------|
| Execution model | Static DAG + checkpoints | Deterministic replay |
| State storage | Redis hash (JSON blob) | Event sourcing (history) |
| Side-effect safety | `Step()` API (opt-in) | Automatic (replay skips) |
| Deployment | Single binary + Redis | Server cluster + DB |
| Complexity | Low | High |

Temporal's deterministic replay automatically makes all side effects exactly-once.
dureq requires explicit `Step()` wrapping but is simpler to deploy and operate.
