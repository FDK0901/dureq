# Execution Guarantees

This document defines the exact delivery and execution semantics of dureq.
Every claim is backed by the specific Redis primitives and code paths that implement it.

## Summary Table

| Aspect | Guarantee | Mechanism | Limitation |
|--------|-----------|-----------|------------|
| **Enqueue** | At-most-once per UniqueKey | `SET NX` on unique key | Without UniqueKey, every call creates a new job |
| **Request ID** | At-most-once (5-min window) | `SET EX` dedup cache | TTL expiry allows duplicates after 5 minutes |
| **Dispatch** | At-most-once per FiringID | FiringID `SET NX EX 1h` + RunID dedup + `XADD` | FiringID TTL expiry (1h) allows duplicate after long failover |
| **Handler start** | Duplicate-suppressed per RunID | Per-run lock `SET NX EX 30s` + auto-extend | Lock TTL expiry creates a race window (see below) |
| **Completion** | Pipelined, not atomic | `DoMulti` 7-step pipeline | Partial failure recovered by orphan detection |
| **Schedule** | At-most-once per tick (single leader) | Leader-only scheduler | Leader failover gap may re-fire the same schedule |
| **Cancellation** | Durable + fast path | Persisted flag (SET EX) + Pub/Sub | Pub/Sub delivers instantly; flag polled every 10s as fallback |
| **Workflow signal** | API-dependent | Durable Redis Stream | `ReadSignals`+`AckSignals` = at-least-once; deprecated `ConsumeSignals` = at-most-once |
| **Crash recovery** | At-least-once | XAUTOCLAIM after 5-min idle | Per-run lock prevents duplicate execution |

## What "Duplicate-Suppressed Execution" Means

dureq provides **at-least-once delivery with duplicate-suppressed handler start**.
This is NOT the same as exactly-once outcome:

- **At-least-once delivery**: A job message will be delivered to a worker at least once.
  If a worker crashes, XAUTOCLAIM reclaims the message for another worker.
- **Duplicate-suppressed handler start**: A per-run distributed lock (`SET NX EX 30s`)
  ensures that at most one worker executes the handler for a given RunID at any time.
  If XAUTOCLAIM delivers the message to a second worker while the first is still running,
  the second worker fails to acquire the lock and skips execution.
- **External side effects are NOT covered**: If your handler calls an external API, writes
  to a database, or sends an email, dureq does not guarantee that these operations happen
  exactly once. The handler may be cancelled mid-execution (lock lost) and retried. You must
  make external side effects idempotent or use the `pkg/sideeffect` package.

## Failure Scenarios

### 1. Enqueue Durability

```
Client                  Redis
  |--- SET NX unique ----->|  (dedup guard)
  |--- HSET job hash ----->|  (job data)
  |--- XADD work stream ->|  (dispatch)
  |--- PUBLISH notify ---->|  (wake workers)
```

Once `XADD` succeeds, the job is durable. If the client crashes between `SET NX` and `XADD`,
the unique key is claimed but no job exists. The unique key must be manually deleted.

### 2. Leader Dies Before Dispatch

```
Leader A                Redis               Leader B
  |--- ListDueSchedules --->|
  |    (reads schedule)     |
  |--- [CRASH] ------------|
  |                         |<--- ListDueSchedules --- (new tick)
  |                         |    (same schedule)
  |                         |----- Dispatch ---------> worker
```

The schedule entry remains "due" until the new leader processes it.
No data loss occurs. Job executes normally.

### 3. Leader Dies After Dispatch, Before Schedule Advance

```
Leader A                Redis               Leader B
  |--- Dispatch (RunID=R1)->|
  |    SET NX firing:{FID}  |  (FiringID dedup acquired)
  |    XADD to stream      |
  |--- [CRASH] ------------|  (advanceSchedule not called)
  |                         |<--- ListDueSchedules --- (new tick)
  |                         |    (same schedule, still due)
  |                         |----- Dispatch (RunID=R2) --->
  |                         |    SET NX firing:{FID} → ALREADY EXISTS
  |                         |    (dispatch skipped — same FiringID)
```

**FiringID** is a deterministic identifier: `"{jobID}:{firingTime.UnixMilli()}"`.
When two leaders attempt to dispatch the same schedule firing, the `SET NX` on
the FiringID key ensures only the first dispatch succeeds. The second leader
sees the key already exists and skips the dispatch.

The FiringID dedup key has a 1-hour TTL for automatic cleanup.

Mitigation: Use `UniqueKey` or handler-level idempotency.

### 4. Lock Extension Failure

```
Worker A                Redis               Worker B
  |--- Lock run:R1 ------->|  (acquired, TTL=30s)
  |    (handler running)   |
  |    [auto-extend fails] |  (TTL expires at 30s)
  |                        |<--- Lock run:R1 --- (acquired by B)
  |<-- SetOnLost callback  |
  |    (context cancelled) |
  |    [handler stops]     |    (B starts handler)
```

Between TTL expiry and SetOnLost callback, there is a **brief window** where both
Worker A's handler (not yet cancelled) and Worker B's handler may run concurrently.
This is the race window that prevents true exactly-once.

Mitigation: Make handler idempotent; use `pkg/sideeffect.Step()` or business-key-based
idempotency keys for external calls.

### 5. Worker Dies After Side Effect, Before Completion

```
Worker                  External API        Redis
  |--- Lock run:R1 ------->|               |
  |--- API call (payment)->|               |
  |<-- API success --------|               |
  |--- [CRASH] ------------|               |
  |                                        |<--- XAUTOCLAIM (5-min idle)
New Worker                                 |
  |--- Lock run:R1 ------->|  (acquired)  |
  |--- API call (payment)->|               |  <-- DUPLICATE!
```

Without idempotency keys, the external API call executes twice.
Use a business key (e.g., OrderID) as an idempotency key when calling external APIs.

### 6. Cancellation Signal Delivery

```
Client                  Redis                      Worker
  |--- SET cancel_flag:R1 ->|  (durable flag, 24h TTL)
  |--- PUBLISH cancel:R1 -->|
  |                         |--- [Worker offline] ---X  (Pub/Sub lost)
  |                         |
  |                         |<--- [Worker reconnects]
  |                         |<--- EXISTS cancel_flag:R1 --- (polling, every 10s)
  |                         |     → found! cancel handler
```

Cancellation uses a dual-write pattern:
- **Pub/Sub** (fast path): Delivers cancellation instantly when the worker is online.
- **Persisted cancel flag** (durable fallback): `SET cancel_flag:{runID} 1 EX 86400` survives
  worker disconnects. Workers poll the flag every 10 seconds during handler execution.
  Workers also check the flag before starting a handler.

The cancel flag is automatically cleaned up after the run completes (or after 24h TTL).

### 7. Workflow Signal Delivery

```
Client                  Redis (Stream)      User Code (ReadSignals + AckSignals)
  |--- XADD signal ------->|
  |                        |<--- XRANGE ---|  (reads all pending)
  |                        |               [process signals]
  |                        |<--- XDEL ----|  (ack after processing)
```

Workflow signals are stored in a durable Redis Stream, so they survive
process restarts. Using `ReadSignals()` + `AckSignals()`, signals are only
deleted after successful processing. If the consumer crashes before acking,
the signals remain in the stream and are re-delivered on the next read.
The effective guarantee is **at-least-once delivery** — signal handlers
must be idempotent.

> The deprecated `ConsumeSignals()` reads and deletes in one call, giving
> only at-most-once delivery. Prefer `ReadSignals` + `AckSignals`.

Note: The orchestrator does NOT consume signals — user code calls
`ReadSignals()` from within signal handlers.

> **API summary**: `ReadSignals` + `AckSignals` provides at-least-once delivery.
> The deprecated `ConsumeSignals` provides at-most-once delivery. Choose based on
> your idempotency capability. See [signals.md](signals.md) for details.

### 8. Overlap Policy + Retry Interaction

When overlap=REPLACE and a retry is in-flight:

1. Schedule fires, sees active run R1 (which is a retry attempt)
2. REPLACE policy cancels R1 via Pub/Sub
3. New run R2 is dispatched
4. If R1's cancellation signal is lost, both R1 and R2 may run

Per-run locks prevent concurrent execution of the **same** RunID,
but R1 and R2 are different RunIDs and can run concurrently.

## When Can You Achieve Exactly-Once?

Exactly-once **outcome** (not just execution) is possible when:

1. **All side effects are idempotent**: External APIs accept idempotency keys
   (use a stable business key, not RunID); database writes use upsert/ON CONFLICT;
   email sends are deduplicated.
2. **Idempotency keys survive failover**: Use business identifiers (OrderID, etc.)
   that remain stable across retries and leader failover re-dispatches.
3. **Transactional completion**: If your side effect is a database write, commit
   the write and the job completion marker in the same transaction.
   Use `pkg/integration/postgres.CompleteTx()` to insert a completion marker
   inside the same SQL transaction as your business write.

dureq provides the **infrastructure** (per-run locks, UniqueKey, RequestID,
`pkg/sideeffect`, `pkg/integration/postgres`), but the exactly-once outcome
guarantee depends on handler implementation.

## Visibility Timeout Model

dureq does not use an explicit visibility timeout. Instead:

- **XREADGROUP** with `">"` reads only new messages (not pending).
- A message stays "pending" (invisible to other consumers) until `XACK`.
- If a consumer crashes without ACKing, **XAUTOCLAIM** reclaims idle messages
  after 5 minutes (configurable).
- The per-run lock prevents duplicate execution of reclaimed messages.

This is equivalent to a 5-minute visibility timeout with lock-based dedup.
