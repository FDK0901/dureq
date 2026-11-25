# Batch Model

Batches process a collection of items using the same handler, with built-in
progress tracking and partial failure handling.

## Defining a Batch

```go
def := types.BatchDefinition{
    Name:         "import-users",
    ItemTaskType: "import-user",
    Items: []types.BatchItem{
        {ID: "u1", Payload: json.RawMessage(`{"email":"a@example.com"}`)},
        {ID: "u2", Payload: json.RawMessage(`{"email":"b@example.com"}`)},
    },
    FailurePolicy: types.BatchContinueOnError,
    ChunkSize:     50,
}

batch, err := client.EnqueueBatch(ctx, def)
```

Each item is dispatched as an independent job of type `ItemTaskType`.

## Onetime Preprocessing

An optional `OnetimeTaskType` runs once before any items are dispatched.
Use this for shared setup (e.g., downloading a file, validating credentials).

```go
taskType := types.TaskType("prepare-import")
def.OnetimeTaskType = &taskType
def.OnetimePayload  = json.RawMessage(`{"source":"s3://bucket/file.csv"}`)
```

The onetime job must complete successfully before item dispatch begins.

## Chunked Dispatch

Items are dispatched in chunks of `ChunkSize` (default: 100). When running items
in a chunk complete, the next chunk is dispatched. This prevents flooding the
work stream with thousands of messages at once.

The batch records which jobs it starts before it starts them, so that an item is dispatched
once, however fast the first ones finish. If the process that does so stops in between, the
orchestrator starts what is missing as soon as another item of the batch finishes.
`EnqueueBatch` and `RetryBatch` return an error, and the batch is `failed`, if nothing could
be dispatched.

## Failure Policy

| Policy | Behavior |
|--------|----------|
| `continue_on_error` (default) | Process all items; report failures at the end |
| `fail_fast` | Stop dispatching new items on first failure |

## Item Retry

Set `ItemRetryPolicy` on the definition to retry individual item failures.
This uses the standard `RetryPolicy` struct (backoff, max attempts, jitter).

## Tracking Progress

`BatchInstance` tracks `TotalItems`, `CompletedItems`, `FailedItems`,
`RunningItems`, and `PendingItems`. Poll with `client.GetBatch(ctx, batchID)`.

For real-time updates, subscribe via `client.SubscribeBatchProgress(ctx, batchID)`.

## Item Results

```go
results, _ := client.GetBatchResults(ctx, batchID)
```

Each `BatchItemResult` contains `BatchID`, `ItemID`, `Success`, `Output`, and `Error`.

## Retry a Batch

```go
// Retry only failed items.
batch, err := client.RetryBatch(ctx, batchID, true)

// Retry all items.
batch, err := client.RetryBatch(ctx, batchID, false)
```

On retry, the batch increments its `Attempt` counter. If `retryFailedOnly` is true,
completed items are preserved, and only failed and cancelled items and those that were never
dispatched are. An item that was still under way when the batch ended is not started again:
what became of it counts. Otherwise completed items run again, too.

## Cancellation

```go
err := client.CancelBatch(ctx, batchID)
```

All non-terminal item jobs and the onetime job (if running) are cancelled.

## Execution Timeout

Set `ExecutionTimeout` on the definition to enforce a deadline for the entire batch.
