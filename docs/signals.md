# Workflow Signals

Signals are asynchronous messages sent to running workflows from external code.
They are stored in a per-workflow Redis Stream and consumed by user code inside
handlers.

## Sending a Signal

```go
err := client.SignalWorkflow(ctx, workflowID, "approval", map[string]any{
    "approved_by": "alice",
    "comment":     "LGTM",
})
```

The signal is appended to the stream `prefix:workflow:{id}:signals`.

### Terminal Guard

Sending a signal to a workflow that has already completed, failed, or been
cancelled returns `ErrWorkflowTerminal`. The client checks the workflow status
before writing to the stream.

## Consuming Signals

Signal consumption happens in user handler code, not in the orchestrator.
Use the crash-safe `ReadSignals` + `AckSignals` pattern:

```go
signals, err := store.ReadSignals(ctx, workflowID)
for _, sig := range signals {
    // Process the signal. If we crash here, the signal remains in the
    // stream and will be re-delivered on the next call to ReadSignals.
    fmt.Printf("signal: %s, payload: %s\n", sig.Name, sig.Payload)
}
// Acknowledge only after successful processing.
ids := make([]string, len(signals))
for i, sig := range signals {
    ids[i] = sig.StreamID
}
store.AckSignals(ctx, workflowID, ids)
```

`ReadSignals` reads all pending entries from the signal stream **without
deleting them**. `AckSignals` deletes the entries after the caller has
successfully processed them. This gives **at-least-once delivery**.

> **Deprecated**: `ConsumeSignals` (read + delete in one call) is retained for
> backward compatibility but is lossy — a crash between read and processing
> permanently loses the signal.

### Delivery Semantics

Signals provide **at-least-once delivery** when using `ReadSignals` +
`AckSignals`. Signal handlers must be idempotent since a crash before
`AckSignals` will cause re-delivery. Use `DedupeKey` on the
sender side.

For scenarios requiring stronger guarantees, combine signals with an external
state check (e.g., poll a database flag) as a fallback.

## Deduplication

Use `WithDedupeKey` to prevent duplicate signals:

```go
err := client.SignalWorkflow(ctx, workflowID, "approval", payload,
    types.WithDedupeKey("approval:order-123"),
)
```

The dedup key is stored with `SET NX` and a TTL. If the key already exists,
`ErrSignalDuplicate` is returned and the signal is not appended.

This is useful when the signal sender may retry (e.g., webhook retries) and
you want to guarantee the signal is stored at most once.

## Signal Timeout

Set `SignalTimeout` on the `WorkflowDefinition` to limit how long a signal
handler task can run. If not set, signal handlers inherit the workflow's
`ExecutionTimeout`.

## Use Cases

- **Human-in-the-loop approval**: A workflow task waits for an external approval
  signal before proceeding. The approval service sends a signal when the human
  acts.

- **External event injection**: Push data into a running workflow from a webhook
  or message queue consumer.

- **Cancellation with reason**: Send a "cancel" signal with a reason payload,
  consumed by a cleanup task before the workflow is formally cancelled.

## Error Reference

| Error | Cause |
|-------|-------|
| `ErrWorkflowTerminal` | Workflow is completed, failed, or cancelled |
| `ErrSignalDuplicate` | DedupeKey already exists (signal already sent) |
| `ErrWorkflowNotFound` | No workflow with the given ID |
