# State Machines

This document defines the valid state transitions for all dureq entities.
Transition validation is enforced at the store layer — invalid transitions
are logged as warnings via `UpdateJob`.

## Job States

```
                    ┌──────────────────────────────────────────────────┐
                    │                                                  │
                    ▼                                                  │
 ┌─────────┐   ┌───────────┐   ┌─────────┐   ┌───────────┐   ┌──────┴────┐
 │ pending  │──▶│ scheduled │──▶│ running │──▶│ completed │──▶│ scheduled │
 └────┬─────┘   └─────┬─────┘   └────┬────┘   └───────────┘   └───────────┘
      │               │              │                          (recurring)
      │               │              ├──▶ failed ──▶ retrying ──▶ running
      │               │              │       │          │
      │               │              │       ├──▶ paused ◀──┘
      │               │              │       │      │
      │               │              │       ▼      ▼
      │               │              ├──▶  dead  ◀──┘
      │               │              │
      └───────────────┴──────────────┴──▶ cancelled
```

### Transition Table

| From | Allowed Targets |
|------|----------------|
| `pending` | `scheduled`, `running`, `cancelled` |
| `scheduled` | `running`, `completed`, `pending`, `cancelled` |
| `running` | `completed`, `failed`, `retrying`, `paused`, `dead`, `scheduled`, `cancelled` |
| `retrying` | `running`, `paused`, `dead`, `cancelled` |
| `paused` | `retrying`, `running`, `dead`, `cancelled` |
| `failed` | `retrying`, `paused`, `dead`, `scheduled`, `pending`, `cancelled` |
| `completed` | `scheduled` (recurring), `pending` (manual retry) |
| `dead` | `retrying`, `pending` (manual retry) |
| `cancelled` | *(terminal — no transitions out)* |

### Terminal States
- `completed` — job succeeded (may transition to `scheduled` for recurring, `pending` via manual retry)
- `dead` — all retries exhausted (may transition to `retrying` or `pending` via manual retry)
- `cancelled` — permanently cancelled

### Notes
- `failed` is NOT terminal — it indicates a single attempt failed but retries remain
- `paused` can be entered from `running` (via `PauseError`) or `failed` (auto-pause on consecutive errors)
- `dead` → `retrying` is only allowed via explicit `RetryJob` / `BulkRetryJobs` API calls

## Run States

```
 ┌─────────┐   ┌─────────┐   ┌───────────┐
 │ claimed │──▶│ running │──▶│ succeeded │
 └────┬────┘   └────┬────┘   └───────────┘
      │              │
      ├──▶ failed    ├──▶ failed
      │              ├──▶ timed_out
      └──▶ cancelled └──▶ cancelled
```

| From | Allowed Targets |
|------|----------------|
| `claimed` | `running`, `failed`, `cancelled` |
| `running` | `succeeded`, `failed`, `timed_out`, `cancelled` |
| `succeeded` | *(terminal)* |
| `failed` | *(terminal)* |
| `timed_out` | *(terminal)* |
| `cancelled` | *(terminal)* |

### Notes
- A Run represents a single execution attempt of a Job
- `claimed` means the message was read from the stream but handler hasn't started
- All terminal run states are truly final — no transitions out

## Workflow States

```
 ┌─────────┐   ┌─────────┐   ┌───────────┐
 │ pending │──▶│ running │──▶│ completed │
 └────┬────┘   └────┬────┘   └───────────┘
      │              │
      │              ├──▶ failed ──▶ running (retry)
      │              │
      │              ├──▶ suspended ──▶ running (resume)
      │              │         │
      │              │         └──▶ cancelled
      │              │
      └──────────────┴──▶ cancelled
```

| From | Allowed Targets |
|------|----------------|
| `pending` | `running`, `cancelled` |
| `running` | `completed`, `failed`, `cancelled`, `suspended` |
| `suspended` | `running`, `cancelled` |
| `failed` | `running` (retry) |
| `completed` | *(terminal)* |
| `cancelled` | *(terminal)* |

### Notes
- `failed` → `running` is via `RetryWorkflow` — resets failed tasks, preserves completed tasks
- `suspended` → `running` is via `ResumeWorkflow`
- Task-level states use `JobStatus` (each workflow task is backed by a Job)

## Error Classification and Retry Behavior

| Error Class | Retry? | Next State | Mechanism |
|-------------|--------|------------|-----------|
| `Retryable` | Yes | `retrying` → `running` | Exponential backoff with jitter |
| `NonRetryable` | No | `dead` | Immediate terminal |
| `RateLimited` | Yes | `retrying` → `running` | Custom delay from `RetryAfter` |
| `Skip` | No | *(unchanged)* | Job stays in current state |
| `Pause` | No | `paused` | Manual or auto-resume after duration |
| `Repeat` | Yes | `running` | Immediate re-enqueue (checkpoint) |

## Code References

- Transition tables: `pkg/types/status.go`
- Validation functions: `ValidJobTransition()`, `ValidRunTransition()`, `ValidWorkflowTransition()`
- Enforcement: `internal/store/redis_store.go` `UpdateJob()` (post-CAS validation)
- Error classification: `pkg/types/errors.go` `ClassifyControlFlow()`
