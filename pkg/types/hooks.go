package types

import "context"

// HookFunc is a callback invoked when a lifecycle event occurs.
// The context carries the same metadata available in handler contexts
// (job ID, run ID, etc. via GetJobID, GetRunID, etc.).
type HookFunc func(ctx context.Context, event JobEvent)

// Hooks holds registered lifecycle callbacks.
// All hooks are optional and invoked asynchronously (non-blocking).
type Hooks struct {
	OnJobCompleted []HookFunc
	OnJobFailed    []HookFunc
	OnJobDead      []HookFunc
	OnJobPaused    []HookFunc
	OnJobResumed   []HookFunc
	OnJobCancelled []HookFunc

	OnWorkflowCompleted []HookFunc
	OnWorkflowFailed    []HookFunc

	OnBatchCompleted []HookFunc
	OnBatchFailed    []HookFunc
}

// Fire invokes all hooks registered for the given event type.
// Hooks are called asynchronously in separate goroutines.
func (h *Hooks) Fire(ctx context.Context, event JobEvent) {
	if h == nil {
		return
	}

	var fns []HookFunc
	switch event.Type {
	case EventJobCompleted:
		fns = h.OnJobCompleted
	case EventJobFailed:
		fns = h.OnJobFailed
	case EventJobDead:
		fns = h.OnJobDead
	case EventJobPaused:
		fns = h.OnJobPaused
	case EventJobResumed:
		fns = h.OnJobResumed
	case EventJobCancelled:
		fns = h.OnJobCancelled
	case EventWorkflowCompleted:
		fns = h.OnWorkflowCompleted
	case EventWorkflowFailed:
		fns = h.OnWorkflowFailed
	case EventBatchCompleted:
		fns = h.OnBatchCompleted
	case EventBatchFailed:
		fns = h.OnBatchFailed
	}

	for _, fn := range fns {
		go fn(ctx, event)
	}
}
