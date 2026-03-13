# Workflow Model

dureq workflows are static DAGs of tasks executed by the workflow orchestrator.
The orchestrator runs on the elected leader node and advances workflow state as
individual task jobs complete.

## Defining a Workflow

A `WorkflowDefinition` declares a name, a list of tasks, and optional policies.

```go
def := types.WorkflowDefinition{
    Name: "order-pipeline",
    Tasks: []types.WorkflowTask{
        {Name: "validate",  TaskType: "validate-order"},
        {Name: "charge",    TaskType: "charge-payment", DependsOn: []string{"validate"}},
        {Name: "ship",      TaskType: "ship-order",     DependsOn: []string{"charge"}},
        {Name: "notify",    TaskType: "send-receipt",    DependsOn: []string{"ship"}},
    },
}

wf, err := client.EnqueueWorkflow(ctx, def, orderInput)
```

Tasks with no `DependsOn` are root tasks and are dispatched immediately.
Downstream tasks are dispatched when all their dependencies complete.

## Task Types

### Static (default)

A regular task dispatched as a job. The handler signature is the standard
`HandlerFunc` or `HandlerFuncWithResult`.

### Condition

A condition node evaluates a predicate and branches to one of several successors.
The handler returns a `uint` route index. The orchestrator looks up the successor
task via `ConditionRoutes`.

```go
{
    Name:     "check-fraud",
    TaskType: "fraud-check",
    Type:     types.WorkflowTaskCondition,
    ConditionRoutes: map[uint]string{
        0: "approve",
        1: "manual-review",
        2: "reject",
    },
    MaxIterations: 5,
}
```

`MaxIterations` prevents infinite loops (default system limit: 100).

### Subflow

A subflow node's handler returns `[]WorkflowTask` at runtime. The orchestrator
injects these tasks into the running workflow instance, allowing dynamic
fan-out patterns.

```go
{Name: "expand-items", TaskType: "generate-subtasks", Type: types.WorkflowTaskSubflow}
```

### Child Workflow

Set `ChildWorkflowDef` on a task to spawn an independent child workflow.
The parent task completes when the child workflow reaches a terminal state.

## Result Piping

Use `ResultFrom` to feed one task's output as the next task's payload.

```go
{Name: "enrich", TaskType: "enrich-data", DependsOn: []string{"validate"}, ResultFrom: "validate"}
```

The `validate` task must use `HandlerFuncWithResult` and return JSON output.
The `enrich` task receives that output as its payload (the static `Payload` field is ignored).

## Preconditions

Tasks can define `Preconditions` that gate dispatch based on upstream output.
If any precondition fails, the task is skipped. Each precondition specifies
a `Type` ("upstream_output"), a `Task`, a JSONPath `Path`, and an `Expected` value.

## AllowFailure

When `AllowFailure: true`, the task's failure does not block downstream tasks
or cause the workflow to fail. Dependencies are considered satisfied.

## Retry

### Task-level retry

Individual task jobs use the standard `RetryPolicy` on the handler definition
or the job's own retry policy.

### Workflow-level retry

Set `RetryPolicy` on the `WorkflowDefinition` to retry the entire workflow
when it fails. On retry, only failed/dead/cancelled tasks are reset. Completed
tasks are preserved so work resumes from the failure point.

```go
client.RetryWorkflow(ctx, workflowID)
```

### ResultReusePolicy

Controls whether a task's cached result is reused on workflow retry:

| Policy | Behavior |
|--------|----------|
| `always` (default) | Reuse cached result if available |
| `never` | Force re-execution |
| `on_success` | Reuse only if the previous run succeeded |

## Continue-as-New

Long-running workflows accumulate state. To prevent unbounded growth, a task
handler can return a `ContinueAsNewError`:

```go
return &types.ContinueAsNewError{Input: newInputJSON}
```

The orchestrator:

1. Marks the current workflow as `continued`
2. Creates a new workflow instance with the same definition and the provided input
3. Links the two via `ContinuedFrom` / `ContinuedTo` fields

## State Compaction

When a workflow instance accumulates 50+ completed tasks, the orchestrator
archives finished task states to a separate Redis hash
(`prefix:workflow:{id}:archive`). The `ArchivedTaskCount` field on the instance
tracks how many tasks have been moved. This keeps the primary workflow state
small for large DAGs.

## Signals

Workflows can receive external signals while running. See [Signals](signals.md)
for details.

- `client.SignalWorkflow(ctx, wfID, "approval", payload)`
- Handler reads signals via `store.ConsumeSignals(ctx, wfID)`

## Hooks

Lifecycle hooks fire automatically at workflow lifecycle events:

| Hook | When |
|------|------|
| `OnInit` | Before root tasks are dispatched |
| `OnSuccess` | Workflow completes successfully |
| `OnFailure` | Workflow fails (all retries exhausted) |
| `OnExit` | Always, after completion or failure |

Each hook is a `WorkflowHookDef` with a `TaskType` and optional `Payload`.
The hook job runs as a regular task.

## Workflow Status Lifecycle

```
pending -> running -> completed | failed | cancelled | continued | suspended
```

Terminal states: `completed`, `failed`, `cancelled`, `continued`.

## Execution Timeout and Priority

Set `ExecutionTimeout` on the definition to enforce a deadline for the entire
workflow. Set `DefaultPriority` to control dispatch priority of all tasks
(individual tasks can override via their own `Priority` field).
