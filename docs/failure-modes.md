# Known Failure Modes

This document catalogs every known failure mode in dureq, how the system
recovers, and what users must do to protect against data loss.

## Leader Failures

### F1: Leader dies before dispatch

**Trigger**: Leader reads due schedule, crashes before calling `Dispatch()`.

**Recovery**: New leader reads the same schedule entry on next tick (1s default)
and dispatches normally. No data loss.

**User action**: None required.

### F2: Leader dies after dispatch, before schedule advance

**Trigger**: Leader calls `Dispatch()` (message in stream), crashes before
`advanceSchedule()`.

**Recovery**: New leader re-reads the schedule entry (still "due"), dispatches
again with a **new RunID**. Two RunIDs now exist for the same firing.

**Impact**: Job may execute twice. Per-run locks prevent concurrent execution
of the same RunID, but R1 and R2 are different RunIDs.

**User action**: Use `UniqueKey` or make handlers idempotent.

### F3: Leader election flap

**Trigger**: Network instability causes rapid leader alternation.

**Recovery**: Leader election uses TTL-based leases. A stale leader's lease
expires and a new leader is elected. The epoch counter is tracked internally
for leader identity but is not currently used as a fencing token on store
operations. During rapid alternation, duplicate dispatches are possible.

**User action**: Tune `LeaderTTL` (default 10s) for your network conditions.

## Worker Failures

### F4: Worker dies during handler execution

**Trigger**: Worker process crashes (OOM, SIGKILL) while handler is running.

**Recovery**:
1. Worker stops heartbeating
2. XAUTOCLAIM reclaims pending message after 5 minutes
3. Another worker acquires the per-run lock and executes
4. Orphan detection (leader, every 30s) marks the old run as failed

**Impact**: Handler side effects from the original execution are NOT rolled back.

**User action**: Use `sideeffect.Step()` or make handlers idempotent using stable
business keys for external API calls.

### F5: Worker dies after side effect, before completion

**Trigger**: Handler calls external API successfully, then worker crashes
before `CompleteRun`.

**Recovery**: Same as F4 — message reclaimed, handler re-executes.

**Impact**: External API called twice without idempotency protection.

**User action**: Use stable business keys as idempotency keys when calling
external APIs, so retried calls are de-duplicated by the external service.

### F6: Worker isolated by network partition

**Trigger**: Worker loses Redis connectivity but process stays alive.

**Recovery**:
1. Lock auto-extend fails → `SetOnLost` cancels handler context
2. Handler should check `ctx.Done()` and exit
3. XAUTOCLAIM reclaims message to another worker after 5 minutes

**Risk window**: Between lock expiry and `SetOnLost` callback, the handler
may still be running while another worker starts. Brief concurrent execution
is possible.

**User action**: Handlers must respect context cancellation promptly.

## Lock Failures

### F7: Lock TTL expires before auto-extend

**Trigger**: Redis is slow (high latency), auto-extend (every 10s) misses
the 30s TTL window.

**Recovery**: `SetOnLost` callback fires, cancelling the handler's context.
Another worker can acquire the lock.

**Risk window**: Between TTL expiry and context cancellation, the handler
continues executing with a lost lock. Side effects in this window are
unprotected.

**User action**: Increase `LockTTL` for long-running handlers. Use
idempotency keys for critical external operations.

### F8: Lock acquired by two workers simultaneously

**Trigger**: Theoretically impossible with `SET NX`, but possible under
Redis failover (master dies, replica promoted without lock key).

**Recovery**: Both workers execute. Second completion attempt has stale
revision (CAS conflict) and fails.

**User action**: Use Redis Cluster with replicas for high availability.
Accept that Redis failover may cause duplicate execution.

## Completion Failures

### F9: Partial pipeline failure in CompleteRun

**Trigger**: One of the 7 `DoMulti` commands fails (e.g., network error
after SaveRun but before AckMessage).

**Recovery**: Message remains pending (not ACKed). XAUTOCLAIM reclaims it.
Per-run lock prevents re-execution. Orphan detection eventually cleans up.

**Impact**: Run state may be inconsistent briefly (saved as complete in
history but message still pending). Self-healing within 5 minutes.

**User action**: None required.

### F10: SyncRetrier buffer full

**Trigger**: Redis outage causes >256 completion operations to queue up.

**Recovery**: Operations beyond buffer capacity are dropped and logged.
Dropped ACKs cause message redelivery (at-least-once). Dropped state
updates are caught by orphan detection.

**User action**: Monitor `sync_retrier` metrics. The 256 buffer is a
deliberate OOM prevention mechanism.

## Schedule Failures

### F11: Catchup window exceeded

**Trigger**: Leader is down longer than `CatchupWindow` (default 1 hour).

**Recovery**: Firings older than the catchup window are skipped.
`MaxBackfillPerTick` limits how many missed firings are replayed per tick.

**User action**: Tune `CatchupWindow` for your SLA requirements.

### F12: Overlap policy conflict with retry

**Trigger**: Job with `OverlapReplace` policy has an in-flight retry
when schedule fires again.

**Recovery**: Active run is cancelled via Pub/Sub, new run dispatched.
If cancellation signal is lost, both runs may execute.

**User action**: Make handlers idempotent when using `OverlapReplace`.

## Signal & Cancellation Failures

### F13: Cancellation signal lost

**Trigger**: Pub/Sub PUBLISH sent while target worker is offline.

**Recovery**: None — cancellation is permanently lost. Job continues
until completion or timeout.

**User action**: Implement timeout-based cancellation as a fallback.
Check cancellation flags periodically in long-running handlers.

### F14: Workflow signal during cancellation

**Trigger**: Signal sent to a workflow that is being cancelled.

**Recovery**: Signal is durably stored in Redis Stream but ignored by
the orchestrator after workflow reaches terminal state.

**User action**: None required. Signals to terminal workflows are safe.

### F15: Workflow signal loss on consumer crash

**Trigger**: Using the deprecated `ConsumeSignals()`, signals are read
(XRANGE) and deleted (XDEL) in one call. A crash before processing loses
the signal permanently.

**Recovery with ReadSignals + AckSignals**: Use `ReadSignals()` to read
without deleting, process signals, then call `AckSignals()` to delete
only successfully processed entries. On crash, unacked signals remain
in the stream and are re-delivered.

**User action**: Migrate from `ConsumeSignals()` to `ReadSignals()` +
`AckSignals()`. Signal handlers must be idempotent since re-delivery
is possible.

## Configuration Failures

### F16: Dynamic config change during in-flight runs

**Trigger**: Rate limit or concurrency config changes while runs are active.

**Recovery**: In-flight runs complete with their original config.
New fetches use the updated config. No interruption.

**User action**: None required.

### F17: Worker version mismatch

**Trigger**: Deploying new handler version while old workers process messages.

**Recovery**: Worker detects version mismatch, re-enqueues message
(requeue-before-ACK), and ACKs the original. A matching worker picks
it up.

**User action**: Deploy new workers before removing old ones (rolling deploy).

## Recovery Time Summary

| Failure | Detection Time | Recovery Time | Data Risk |
|---------|---------------|---------------|-----------|
| Leader crash | TTL expiry (10s) | Next tick (1s) | Schedule may double-fire |
| Worker crash | XAUTOCLAIM (5m) | Immediate after claim | Side effects may duplicate |
| Lock expiry | Auto-extend check (10s) | Immediate | Brief concurrent execution |
| Pipeline partial fail | XAUTOCLAIM (5m) | Self-healing | Temporary inconsistency |
| Cancellation lost | Never detected | Never recovered | Job runs to completion |
| Network partition | Lock TTL (30s) | After reconnection | Side effects during window |
