# Operations Playbook

This guide covers health checks, deployment modes, monitoring, and repair
procedures for running dureq in production.

## Health Checks

| Endpoint | Purpose |
|----------|---------|
| `GET /healthz` | Liveness probe (K8s `livenessProbe`) |
| `GET /readyz` | Readiness probe -- Redis connected, worker accepting jobs |
| `GET /api/health` | Detailed health with subsystem state |

## Mode Flags

Use `WithMode` to control which subsystems run on a given node.

| Mode | Bitmask | Subsystems |
|------|---------|------------|
| `ModeQueue` | `1` | Worker pool + dispatcher |
| `ModeScheduler` | `2` | Schedule scanner + orphan detection + archival (leader-only) |
| `ModeWorkflow` | `4` | Workflow orchestrator + batch orchestrator (leader-only) |
| `ModeMonitor` | `8` | Nothing in a server of your own. A `dureqd` in this mode alone runs no jobs and serves the monitoring API, which it does in every mode |
| `ModeFull` | `15` | All of the above |

### Recommended Topology

For small deployments, use `ModeFull` on every node. For larger clusters:

```
Worker nodes:    ModeQueue
Leader node:     ModeScheduler | ModeWorkflow
Monitor node:    ModeMonitor (dureqd)
```

Leader election is automatic. Any node with `ModeScheduler` or `ModeWorkflow`
participates in the election. Only the elected leader runs the scheduler and
orchestrator.

## Leader Election

- Uses a Redis key with TTL (`SET NX EX`) as the election lock.
- Default TTL: 10s (`WithElectionTTL`).
- The leader renews the lock every TTL/3.
- If the leader crashes, another node wins the election within one TTL period.
- On leadership change, the new leader starts the scheduler and orchestrator;
  the demoted node stops them.

No fencing token is used. The scheduler and orchestrator are idempotent
across brief dual-leader overlap.

## Graceful Shutdown

When `Server.Stop()` is called:

1. The worker stops fetching new messages from the work stream.
2. In-flight tasks are given `ShutdownTimeout` (default: 30s) to complete.
3. Tasks that do not complete within the timeout are cancelled via context
   and their messages are re-enqueued to the work stream.
4. The node deregisters its heartbeat.
5. If this node was the leader, it releases the election lock.

Set `WithShutdownTimeout` to tune the grace period.

## Node Drain

Drain mode stops a node from accepting new work while allowing in-flight tasks
to complete. Unlike shutdown, the process stays alive.

```
POST /api/nodes/{nodeID}/drain     -- start drain
DELETE /api/nodes/{nodeID}/drain   -- stop drain
GET /api/nodes/{nodeID}/drain      -- check status
```

Or via SDK: `client.DrainNode(ctx, nodeID, true)` / `client.DrainNode(ctx, nodeID, false)`.
Use before rolling deployments.

## Monitoring API Endpoints

`dureqd` serves the monitoring API, in every mode. A server of your own does not; build endpoints on
`dureq.NewAPIService`, which has the same calls. It has no authentication: see
[Who may use the API](dureqd.md#who-may-use-the-api). Key route groups:

### Jobs

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/jobs` | List jobs (paginated, filterable) |
| `GET` | `/api/jobs/{id}` | Get job details |
| `POST` | `/api/jobs/{id}/cancel` | Cancel a job |
| `POST` | `/api/jobs/{id}/retry` | Retry a failed/dead job |
| `POST` | `/api/jobs/{id}/resume` | Resume a paused job |
| `PUT` | `/api/jobs/{id}/payload` | Update job payload |
| `DELETE` | `/api/jobs/{id}` | Delete a job |

### Workflows

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/workflows` | List workflows |
| `GET` | `/api/workflows/{id}` | Get workflow details |
| `POST` | `/api/workflows/{id}/cancel` | Cancel workflow |
| `POST` | `/api/workflows/{id}/retry` | Retry from failure point |
| `POST` | `/api/workflows/{id}/suspend` | Suspend workflow |
| `POST` | `/api/workflows/{id}/resume` | Resume suspended workflow |

### Batches

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/batches` | List batches |
| `GET` | `/api/batches/{id}` | Get batch details |
| `GET` | `/api/batches/{id}/results` | List item results |
| `POST` | `/api/batches/{id}/cancel` | Cancel batch |
| `POST` | `/api/batches/{id}/retry` | Retry batch |

### Cluster

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/stats` | Cluster-wide statistics |
| `GET` | `/api/stats/daily` | Daily aggregated statistics |
| `GET` | `/api/nodes` | Worker nodes and pool stats |
| `GET` | `/api/schedules` | Active schedules |
| `GET` | `/api/queues` | Queue tiers: weight, paused, `waiting` (no worker has it yet), `in_flight`, `delayed` (due later, such as retries). `size` is the length of the stream, which includes what has been processed but not trimmed yet |
| `GET` | `/api/dlq` | Dead letter queue entries |
| `GET` | `/api/redis/info` | Redis server info |
| `GET` | `/api/ws` | WebSocket real-time event stream (`?v=2` with heartbeats, and a message if events were missed) |

### Bulk and Search

Bulk operations: `POST /api/{jobs,workflows,batches}/bulk/{cancel,retry,delete}`

Search by payload JSONPath: `GET /api/search/{jobs,workflows,batches}?path=field&value=val` returns the newest match.

`GET /api/search/jobs/all?path=field&value=val` returns all jobs that match, newest first, as
`{"data": [...], "has_more": false}`. Narrow it down with `task_type`, `live=true` (leaves out completed, dead
and cancelled jobs), `status` (repeatable) and `limit` (10 by default, 100 at most).

`GET /api/jobs?search=text` finds jobs by a part of their ID or task type. It reads the jobs one by one, up to
100,000 of them, so narrow it down with `status`, `task_type` or `tag` if there are many.

`GET /api/history/events` reads the event stream, which keeps about the last `EventStreamMaxLen` events (50,000).
With `?job_id=` it goes through all of them to count those of the job; `&total=false` stops at the page, and says
`has_more` instead.

## Repair Operations

### Requeue a Stuck Run

If a run is stuck (worker crashed, lock lost, message not ACKed):

```
POST /api/runs/{runID}/requeue
```

This re-publishes the work message to the stream.

### Force Unlock

If a distributed lock is stuck (e.g., holder crashed without releasing):

```
GET /api/locks/{key}        -- inspect lock state
DELETE /api/locks/{key}     -- force release
```

### Inspect Concurrency and Unique Keys

```
GET /api/concurrency/{key}         -- sorted set members holding slots
GET /api/unique-keys/{key}         -- check if key exists
DELETE /api/unique-keys/{key}      -- remove orphaned key
```

`CancelByUniqueKey` of the client frees an orphaned key as well. If a job does hold the key,
it cancels that job.

A dead or cancelled job has released its key already. Retrying it takes the key back; if
another job holds the key by then, the retry is refused (409, `ErrDuplicateJob` from the
client) and the job stays as it is. Jobs that went dead before v0.2.5 still hold their key:
cancel them with the client (`Cancel(id)`; the API refuses ended jobs) or delete the key.

## Dynamic Configuration

dureq supports runtime configuration changes without restart. The dynamic
config manager polls Redis every 10s for updates.

### Global Settings

Set via Redis hash at `{prefix}:config:global`:

| Field | Type | Description |
|-------|------|-------------|
| `max_concurrency` | int | Override worker pool size |
| `scheduler_tick_interval` | duration | Override scheduler scan interval |

### Per-Handler Overrides

Set via Redis hash at `{prefix}:config:handler:{taskType}`:

| Field | Type | Description |
|-------|------|-------------|
| `concurrency` | int | Override per-node concurrency for this handler |
| `timeout` | duration | Override execution timeout |
| `max_attempts` | int | Override max retry attempts |

Example: `redis-cli HSET dureq:config:global max_concurrency 20`

Changes take effect within the next poll interval (default 10s).

## Audit Trail

State transitions are recorded per job and workflow:
`GET /api/jobs/{id}/audit`, `GET /api/workflows/{id}/audit`, `POST /api/audit/counts`.

## Retention and Archival

Archival is off by default: jobs are kept until they are deleted. With
`WithRetentionPeriod(d)`, completed/failed/dead/cancelled jobs are deleted once they have
not been updated for `d` (a negative `d` means 7 days, 0 means off). Runs on the leader node.

The run history is separate from that and is kept, too, until you clean it up:

- by hand: `client.CleanupRunHistory(ctx, time.Now().Add(-30*24*time.Hour))` or
  `DELETE /api/history/runs?older_than=720h`, which answers `{"removed": 1234}`;
- or regularly: with `WithRunHistoryRetention(30*24*time.Hour)` the leader removes runs once they finished longer ago
  than that (dureqd: `runHistoryRetentionHours`). It looks every hour, and when a node becomes the leader. Off by
  default.

Either removes finished runs only. Jobs, their audit trails and their active runs are left alone.

Nothing that is yet to be done is ever trimmed:

- Work streams: the leader removes messages that have been processed, and only those.
- Events that the orchestrator has to see (`events:orchestrator`): each is removed once it has been processed.
- The dead letter queue keeps every entry. It is limited only if `DLQStreamMaxLen` is set.
- The event stream is limited to about `EventStreamMaxLen` events, as it is a history only.
