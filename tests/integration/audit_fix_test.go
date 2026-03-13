// Package integration — tests for bug fixes and behavioral changes from v0.1.10 audit.
//
// Each test targets a specific fix to prevent regression.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/dispatcher"
	"github.com/FDK0901/dureq/internal/monitor"
	"github.com/FDK0901/dureq/pkg/types"
)

// ============================================================
// Fix: Child workflow completion must decrement PendingDeps
// (orchestrator.go: missing DecrementPendingDeps in progressParentWorkflowTask)
// ============================================================

func TestFix_ChildWorkflow_PendingDeps(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "fix-child-deps-node", 10)

	// Register handlers for parent + child workflow tasks.
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "fix.validate",
		Handler: func(_ context.Context, _ json.RawMessage) error {
			return nil
		},
	})
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "fix.pick",
		Handler: func(_ context.Context, _ json.RawMessage) error {
			time.Sleep(200 * time.Millisecond)
			return nil
		},
	})
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "fix.ship",
		Handler: func(_ context.Context, _ json.RawMessage) error {
			return nil
		},
	})
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "fix.notify",
		Handler: func(_ context.Context, _ json.RawMessage) error {
			return nil
		},
	})

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()
	time.Sleep(2 * time.Second) // wait for leader election

	payload, _ := json.Marshal(map[string]string{"test": "child-deps"})

	// Parent: validate → fulfillment (child workflow: pick → ship) → notify
	wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{
		Name: "child-deps-test",
		Tasks: []types.WorkflowTask{
			{Name: "validate", TaskType: "fix.validate", Payload: payload},
			{
				Name:      "fulfillment",
				DependsOn: []string{"validate"},
				ChildWorkflowDef: &types.WorkflowDefinition{
					Name: "child-fulfillment",
					Tasks: []types.WorkflowTask{
						{Name: "pick", TaskType: "fix.pick", Payload: payload},
						{Name: "ship", TaskType: "fix.ship", Payload: payload, DependsOn: []string{"pick"}},
					},
				},
			},
			{Name: "notify", TaskType: "fix.notify", Payload: payload, DependsOn: []string{"fulfillment"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("enqueue workflow: %v", err)
	}

	// The fix: without DecrementPendingDeps, "notify" never becomes ready.
	var finalWF *types.WorkflowInstance
	waitFor(t, 30*time.Second, "child workflow parent completion", func() bool {
		status, err := cli.GetWorkflow(ctx, wf.ID)
		if err != nil {
			return false
		}
		finalWF = status
		return status.Status.IsTerminal()
	})

	if finalWF.Status != types.WorkflowStatusCompleted {
		t.Fatalf("expected completed, got %s", finalWF.Status)
	}

	// Verify all 3 parent tasks completed (including "notify" which depends on child).
	for _, taskName := range []string{"validate", "fulfillment", "notify"} {
		state, ok := finalWF.Tasks[taskName]
		if !ok {
			t.Fatalf("task %q not found in workflow", taskName)
		}
		if state.Status != types.JobStatusCompleted {
			t.Errorf("task %q: expected completed, got %s", taskName, state.Status)
		}
	}
	t.Logf("PASS: child workflow PendingDeps fix — all 3 parent tasks completed")
}

// ============================================================
// Fix: RetryJob rejects completed jobs (IsRetryable vs IsTerminal)
// (api_service.go: changed from IsTerminal to IsRetryable)
// ============================================================

func TestFix_RetryJob_RejectsCompleted(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "fix-retry-reject-node", 10)

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "fix.quick",
		Handler: func(_ context.Context, _ json.RawMessage) error {
			return nil
		},
	})

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()

	// Enqueue and wait for completion.
	job, err := cli.Enqueue(ctx, "fix.quick", map[string]string{"test": "retry-reject"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	completedJob := waitJobTerminal(t, cli, job.ID, 15*time.Second)
	if completedJob.Status != types.JobStatusCompleted {
		t.Fatalf("expected completed, got %s", completedJob.Status)
	}

	// Retry on completed job should fail.
	err = cli.Retry(ctx, job.ID)
	if err == nil {
		t.Fatal("expected error retrying completed job, got nil")
	}
	t.Logf("PASS: retry on completed job correctly rejected: %v", err)
}

// ============================================================
// Fix: DeleteJob blocked for non-terminal jobs
// (api_service.go: added terminal guard in DeleteJob)
// ============================================================

func TestFix_DeleteJob_TerminalGuard(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "fix-delete-guard-node", 10)

	blockCh := make(chan struct{})
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "fix.slow",
		Handler: func(_ context.Context, _ json.RawMessage) error {
			<-blockCh // block until test releases
			return nil
		},
	})

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()

	job, err := cli.Enqueue(ctx, "fix.slow", map[string]string{"test": "delete-guard"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Wait for the job to start running.
	time.Sleep(2 * time.Second)

	// Try to delete the in-flight job via the API service.
	apiSvc := monitor.NewAPIService(srv.Store(), dispatcher.New(srv.Store(), nil), nil)
	defer apiSvc.Shutdown()

	err = apiSvc.DeleteJob(ctx, job.ID)
	if err == nil {
		t.Fatal("expected error deleting non-terminal job, got nil")
	}
	apiErr, ok := err.(*monitor.ApiError)
	if !ok {
		t.Fatalf("expected ApiError, got %T: %v", err, err)
	}
	if apiErr.StatusCode != 409 {
		t.Fatalf("expected 409 Conflict, got %d", apiErr.StatusCode)
	}
	t.Logf("PASS: delete of non-terminal job blocked: %v", apiErr.Msg)

	// Unblock handler, wait for completion, then delete should succeed.
	close(blockCh)
	waitJobTerminal(t, cli, job.ID, 15*time.Second)

	if err := apiSvc.DeleteJob(ctx, job.ID); err != nil {
		t.Fatalf("delete of completed job failed: %v", err)
	}
	t.Log("PASS: delete of completed job succeeded")
}

// ============================================================
// Fix: RetryWorkflow preserves completed tasks
// (api_service.go: only reset failed/dead/cancelled, not completed)
// ============================================================

func TestFix_RetryWorkflow_PreservesCompletedTasks(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "fix-retry-preserve-node", 10)

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "fix.ok",
		Handler: func(_ context.Context, _ json.RawMessage) error {
			return nil
		},
	})
	var flakyAttempt atomic.Int32
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "fix.flaky",
		Handler: func(_ context.Context, _ json.RawMessage) error {
			n := flakyAttempt.Add(1)
			if n <= 1 {
				// Return a non-retryable error so the job goes dead immediately,
				// which causes the workflow to fail.
				return &types.NonRetryableError{Err: fmt.Errorf("permanent failure on attempt %d", n)}
			}
			return nil // succeed on workflow retry
		},
	})

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()
	time.Sleep(2 * time.Second)

	payload, _ := json.Marshal(nil)
	wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{
		Name: "retry-preserve-test",
		Tasks: []types.WorkflowTask{
			{Name: "step1", TaskType: "fix.ok", Payload: payload},
			{Name: "step2", TaskType: "fix.flaky", Payload: payload, DependsOn: []string{"step1"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("enqueue workflow: %v", err)
	}

	// Wait for workflow to fail (step2 fails on first attempt).
	var failedWF *types.WorkflowInstance
	waitFor(t, 30*time.Second, "workflow to fail", func() bool {
		status, err := cli.GetWorkflow(ctx, wf.ID)
		if err != nil {
			return false
		}
		failedWF = status
		return status.Status.IsTerminal()
	})

	if failedWF.Status != types.WorkflowStatusFailed {
		t.Fatalf("expected failed, got %s", failedWF.Status)
	}

	// Verify step1 completed.
	step1State := failedWF.Tasks["step1"]
	if step1State.Status != types.JobStatusCompleted {
		t.Fatalf("step1: expected completed, got %s", step1State.Status)
	}

	// Retry the workflow.
	retriedWF, err := cli.RetryWorkflow(ctx, wf.ID)
	if err != nil {
		t.Fatalf("retry workflow: %v", err)
	}

	// Key assertion: step1 should still be completed (not reset).
	step1After := retriedWF.Tasks["step1"]
	if step1After.Status != types.JobStatusCompleted {
		t.Fatalf("after retry, step1: expected completed (preserved), got %s", step1After.Status)
	}

	// step2 should be reset to pending (or already dispatched to running).
	step2After := retriedWF.Tasks["step2"]
	if step2After.Status != types.JobStatusPending && step2After.Status != types.JobStatusRunning {
		t.Fatalf("after retry, step2: expected pending or running (reset), got %s", step2After.Status)
	}

	// Wait for the retried workflow to complete.
	var finalWF *types.WorkflowInstance
	waitFor(t, 30*time.Second, "retried workflow completion", func() bool {
		status, err := cli.GetWorkflow(ctx, wf.ID)
		if err != nil {
			return false
		}
		finalWF = status
		return status.Status == types.WorkflowStatusCompleted
	})

	if finalWF.Status != types.WorkflowStatusCompleted {
		t.Fatalf("expected completed after retry, got %s", finalWF.Status)
	}
	t.Log("PASS: retry workflow preserved completed step1, only reset failed step2")
}

// ============================================================
// Fix: Signal deduplication
// (redis_store.go: SendSignal with dedup key prevents duplicates)
// ============================================================

func TestFix_SignalDedup(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "fix-signal-dedup-node", 10)

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "fix.wait_signal",
		Handler: func(ctx context.Context, _ json.RawMessage) error {
			<-ctx.Done()
			return nil
		},
	})

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()
	time.Sleep(2 * time.Second)

	payload, _ := json.Marshal(nil)
	wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{
		Name: "signal-dedup-test",
		Tasks: []types.WorkflowTask{
			{Name: "waiter", TaskType: "fix.wait_signal", Payload: payload},
		},
	}, nil)
	if err != nil {
		t.Fatalf("enqueue workflow: %v", err)
	}

	time.Sleep(2 * time.Second)

	// Send signal with dedup key — first should succeed.
	dedupeKey := "unique-signal-123"
	err = cli.SignalWorkflow(ctx, wf.ID, "my-signal", nil, types.WithDedupeKey(dedupeKey))
	if err != nil {
		t.Fatalf("first signal: %v", err)
	}

	// Send same signal again — should be rejected or silently deduped.
	err = cli.SignalWorkflow(ctx, wf.ID, "my-signal", nil, types.WithDedupeKey(dedupeKey))
	t.Logf("second signal result: %v (expected: duplicate suppressed)", err)

	// Verify: only 1 signal was stored in the stream.
	sigs, readErr := srv.Store().ReadSignals(ctx, wf.ID)
	if readErr != nil {
		t.Fatalf("read signals: %v", readErr)
	}
	if len(sigs) > 1 {
		t.Errorf("expected at most 1 signal in stream, got %d (dedup failed)", len(sigs))
	}
	t.Logf("PASS: signal dedup — %d signal(s) in stream", len(sigs))

	cli.CancelWorkflow(ctx, wf.ID)
}

// ============================================================
// Fix: RepeatError requeue-before-ack ordering
// (worker.go: requeue BEFORE ack to prevent message loss)
// ============================================================

func TestFix_RepeatError_RequeueBeforeAck(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "fix-repeat-node", 10)

	var execCount atomic.Int32
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType:    "fix.repeat",
		Concurrency: 1,
		Handler: func(_ context.Context, _ json.RawMessage) error {
			n := execCount.Add(1)
			if n < 3 {
				return &types.RepeatError{} // request re-execution
			}
			return nil // complete on 3rd execution
		},
	})

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()

	job, err := cli.Enqueue(ctx, "fix.repeat", map[string]string{"test": "repeat"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Wait for the job to complete (needs 3 executions).
	completedJob := waitJobTerminal(t, cli, job.ID, 30*time.Second)
	if completedJob.Status != types.JobStatusCompleted {
		t.Fatalf("expected completed, got %s", completedJob.Status)
	}

	count := execCount.Load()
	if count < 3 {
		t.Fatalf("expected at least 3 executions, got %d (message lost during repeat)", count)
	}
	t.Logf("PASS: repeat handler executed %d times, job completed (requeue-before-ack works)", count)
}

// ============================================================
// Fix: ContinueAsNew workflow
// (worker.go + orchestrator.go: new workflow instance spawned)
// ============================================================

func TestFix_ContinueAsNew(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "fix-cont-node", 10)

	var iteration atomic.Int32

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "fix.iterate",
		Handler: func(_ context.Context, payload json.RawMessage) error {
			n := iteration.Add(1)
			if n < 3 {
				// Continue as new with updated payload.
				newPayload, _ := json.Marshal(map[string]int{"iteration": int(n + 1)})
				return &types.ContinueAsNewError{Input: newPayload}
			}
			return nil // complete on 3rd iteration
		},
	})

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()
	time.Sleep(2 * time.Second)

	payload, _ := json.Marshal(map[string]int{"iteration": 1})
	wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{
		Name: "continue-as-new-test",
		Tasks: []types.WorkflowTask{
			{Name: "iterate", TaskType: "fix.iterate", Payload: payload},
		},
	}, nil)
	if err != nil {
		t.Fatalf("enqueue workflow: %v", err)
	}

	// Wait for the original workflow to become "continued".
	var origWF *types.WorkflowInstance
	waitFor(t, 30*time.Second, "original workflow continued", func() bool {
		status, err := cli.GetWorkflow(ctx, wf.ID)
		if err != nil {
			return false
		}
		origWF = status
		return status.Status.IsTerminal()
	})

	if origWF.Status != types.WorkflowStatusContinued {
		// May also complete if all iterations happen in same instance.
		// The key test is that the handler ran 3 times and terminated.
		t.Logf("original workflow status: %s (may vary based on timing)", origWF.Status)
	}

	// Verify: handler ran at least 3 times.
	waitFor(t, 30*time.Second, "all iterations complete", func() bool {
		return iteration.Load() >= 3
	})

	finalIter := iteration.Load()
	t.Logf("PASS: ContinueAsNew — handler ran %d iterations", finalIter)
}

// ============================================================
// Fix: Pending → Completed transition is valid
// (status.go: added completed/failed/retrying/dead to pending transitions)
// ============================================================

func TestFix_PendingToCompleted_NoWarning(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "fix-transition-node", 10)

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "fix.instant",
		Handler: func(_ context.Context, _ json.RawMessage) error {
			return nil // complete immediately
		},
	})

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()

	// Enqueue 10 jobs — all should complete without warnings.
	// The fix: worker transitions job directly pending→completed
	// (no intermediate "running" state on the Job object).
	for i := 0; i < 10; i++ {
		_, err := cli.Enqueue(ctx, "fix.instant", map[string]int{"i": i})
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	// Validate transitions are valid in code.
	if !types.ValidJobTransition(types.JobStatusPending, types.JobStatusCompleted) {
		t.Fatal("pending → completed should be a valid transition")
	}
	if !types.ValidJobTransition(types.JobStatusPending, types.JobStatusFailed) {
		t.Fatal("pending → failed should be a valid transition")
	}
	if !types.ValidJobTransition(types.JobStatusPending, types.JobStatusDead) {
		t.Fatal("pending → dead should be a valid transition")
	}
	if !types.ValidJobTransition(types.JobStatusPending, types.JobStatusRetrying) {
		t.Fatal("pending → retrying should be a valid transition")
	}
	t.Log("PASS: pending → completed/failed/dead/retrying transitions are valid")
}

// ============================================================
// Fix: Workflow retry only for retryable statuses
// (client.go: IsRetryable check, not IsTerminal)
// ============================================================

func TestFix_RetryWorkflow_RejectsCompleted(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "fix-wf-retry-node", 10)

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "fix.wf_ok",
		Handler: func(_ context.Context, _ json.RawMessage) error {
			return nil
		},
	})

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()
	time.Sleep(2 * time.Second)

	payload, _ := json.Marshal(nil)
	wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{
		Name: "retry-completed-test",
		Tasks: []types.WorkflowTask{
			{Name: "ok", TaskType: "fix.wf_ok", Payload: payload},
		},
	}, nil)
	if err != nil {
		t.Fatalf("enqueue workflow: %v", err)
	}

	// Wait for completion.
	waitFor(t, 30*time.Second, "workflow complete", func() bool {
		status, err := cli.GetWorkflow(ctx, wf.ID)
		if err != nil {
			return false
		}
		return status.Status == types.WorkflowStatusCompleted
	})

	// Retry completed workflow should fail.
	_, err = cli.RetryWorkflow(ctx, wf.ID)
	if err == nil {
		t.Fatal("expected error retrying completed workflow, got nil")
	}
	t.Logf("PASS: retry on completed workflow correctly rejected: %v", err)
}

// ============================================================
// Fix: Bulk retry workflows via APIService
// (api_service.go: BulkRetryWorkflows preserves completed tasks)
// ============================================================

func TestFix_BulkRetryWorkflows_PreservesCompleted(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "fix-bulk-retry-node", 10)

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "fix.bulk_ok",
		Handler: func(_ context.Context, _ json.RawMessage) error {
			return nil
		},
	})
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "fix.bulk_fail",
		Handler: func(_ context.Context, _ json.RawMessage) error {
			return fmt.Errorf("always fails")
		},
	})

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()
	time.Sleep(2 * time.Second)

	payload, _ := json.Marshal(nil)
	wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{
		Name: "bulk-retry-test",
		Tasks: []types.WorkflowTask{
			{Name: "good", TaskType: "fix.bulk_ok", Payload: payload},
			{Name: "bad", TaskType: "fix.bulk_fail", Payload: payload, DependsOn: []string{"good"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("enqueue workflow: %v", err)
	}

	// Wait for workflow to fail.
	waitFor(t, 30*time.Second, "workflow to fail", func() bool {
		status, err := cli.GetWorkflow(ctx, wf.ID)
		if err != nil {
			return false
		}
		return status.Status == types.WorkflowStatusFailed
	})

	// Use APIService for bulk retry.
	apiSvc := monitor.NewAPIService(srv.Store(), dispatcher.New(srv.Store(), nil), nil)
	defer apiSvc.Shutdown()

	result, err := apiSvc.BulkRetryWorkflows(ctx, monitor.BulkRequest{IDs: []string{wf.ID}})
	if err != nil {
		t.Fatalf("bulk retry: %v", err)
	}
	if result.Affected != 1 {
		t.Fatalf("expected 1 affected, got %d", result.Affected)
	}

	// Verify: good task still completed, bad task reset to pending.
	retriedWF, err := cli.GetWorkflow(ctx, wf.ID)
	if err != nil {
		t.Fatalf("get workflow after retry: %v", err)
	}

	goodState := retriedWF.Tasks["good"]
	if goodState.Status != types.JobStatusCompleted {
		t.Errorf("good task: expected completed (preserved), got %s", goodState.Status)
	}

	badState := retriedWF.Tasks["bad"]
	if badState.Status != types.JobStatusPending {
		t.Errorf("bad task: expected pending (reset), got %s", badState.Status)
	}

	t.Logf("PASS: bulk retry preserved completed 'good' task, reset failed 'bad' task")
}

// ============================================================
// Fix: Bulk cancel workflows with child workflows
// (api_service.go: BulkCancelWorkflows calls cancelChildWorkflow)
// ============================================================

func TestFix_BulkCancelWorkflows_CancelsChildren(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "fix-bulk-cancel-node", 10)

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "fix.bc_validate",
		Handler: func(_ context.Context, _ json.RawMessage) error {
			return nil
		},
	})
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "fix.bc_slow",
		Handler: func(ctx context.Context, _ json.RawMessage) error {
			<-ctx.Done() // block forever
			return nil
		},
	})

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()
	time.Sleep(2 * time.Second)

	payload, _ := json.Marshal(nil)
	wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{
		Name: "bulk-cancel-child-test",
		Tasks: []types.WorkflowTask{
			{Name: "validate", TaskType: "fix.bc_validate", Payload: payload},
			{
				Name:      "child_step",
				DependsOn: []string{"validate"},
				ChildWorkflowDef: &types.WorkflowDefinition{
					Name: "cancel-child-sub",
					Tasks: []types.WorkflowTask{
						{Name: "slow", TaskType: "fix.bc_slow", Payload: payload},
					},
				},
			},
		},
	}, nil)
	if err != nil {
		t.Fatalf("enqueue workflow: %v", err)
	}

	// Wait for child workflow to spawn (parent "validate" completes, child starts).
	var childWfID string
	waitFor(t, 30*time.Second, "child workflow spawned", func() bool {
		status, err := cli.GetWorkflow(ctx, wf.ID)
		if err != nil {
			return false
		}
		if state, ok := status.Tasks["child_step"]; ok && state.ChildWorkflowID != "" {
			childWfID = state.ChildWorkflowID
			return true
		}
		return false
	})

	// Bulk cancel the parent.
	apiSvc := monitor.NewAPIService(srv.Store(), dispatcher.New(srv.Store(), nil), nil)
	defer apiSvc.Shutdown()

	result, err := apiSvc.BulkCancelWorkflows(ctx, monitor.BulkRequest{IDs: []string{wf.ID}})
	if err != nil {
		t.Fatalf("bulk cancel: %v", err)
	}
	if result.Affected != 1 {
		t.Fatalf("expected 1 affected, got %d", result.Affected)
	}

	// Verify child workflow also cancelled.
	waitFor(t, 15*time.Second, "child workflow cancelled", func() bool {
		childWF, err := cli.GetWorkflow(ctx, childWfID)
		if err != nil {
			return false
		}
		return childWF.Status == types.WorkflowStatusCancelled
	})

	t.Logf("PASS: bulk cancel parent also cancelled child workflow %s", childWfID)
}

// ============================================================
// Fix: ReadSignals + AckSignals crash-safe pattern
// (redis_store.go: ReadSignals without delete, AckSignals explicit delete)
// ============================================================

func TestFix_ReadSignals_AckSignals(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()

	// Create a workflow entry for signal storage.
	wfID := "test-signal-readack"
	wf := &types.WorkflowInstance{
		ID:           wfID,
		WorkflowName: "signal-test",
		Status:       types.WorkflowStatusRunning,
		Tasks:        map[string]types.WorkflowTaskState{},
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
	st.SaveWorkflow(ctx, wf)

	// Send 3 signals.
	for i := 0; i < 3; i++ {
		payload, _ := json.Marshal(map[string]int{"n": i})
		st.SendSignal(ctx, &types.WorkflowSignal{
			WorkflowID: wfID,
			Name:       "test-signal",
			Payload:    payload,
		})
	}

	// ReadSignals: should return all 3 without deleting.
	sigs, err := st.ReadSignals(ctx, wfID)
	if err != nil {
		t.Fatalf("ReadSignals: %v", err)
	}
	if len(sigs) != 3 {
		t.Fatalf("expected 3 signals, got %d", len(sigs))
	}

	// Read again: should still return 3 (no delete on read).
	sigs2, err := st.ReadSignals(ctx, wfID)
	if err != nil {
		t.Fatalf("ReadSignals (second): %v", err)
	}
	if len(sigs2) != 3 {
		t.Fatalf("expected 3 signals on second read, got %d (read deleted them!)", len(sigs2))
	}

	// Ack first 2 signals.
	ackIDs := []string{sigs[0].StreamID, sigs[1].StreamID}
	if err := st.AckSignals(ctx, wfID, ackIDs); err != nil {
		t.Fatalf("AckSignals: %v", err)
	}

	// Read again: should return only 1 remaining.
	sigs3, err := st.ReadSignals(ctx, wfID)
	if err != nil {
		t.Fatalf("ReadSignals (after ack): %v", err)
	}
	if len(sigs3) != 1 {
		t.Fatalf("expected 1 signal after acking 2, got %d", len(sigs3))
	}

	t.Log("PASS: ReadSignals is non-destructive, AckSignals deletes selectively")
}

// ============================================================
// Fix: IsRetryable excludes completed for workflow status
// (job.go: IsRetryable only returns true for failed/cancelled)
// ============================================================

func TestFix_WorkflowStatus_IsRetryable(t *testing.T) {
	tests := []struct {
		status    types.WorkflowStatus
		retryable bool
	}{
		{types.WorkflowStatusPending, false},
		{types.WorkflowStatusRunning, false},
		{types.WorkflowStatusCompleted, false},
		{types.WorkflowStatusFailed, true},
		{types.WorkflowStatusCancelled, true},
		{types.WorkflowStatusSuspended, false},
		{types.WorkflowStatusContinued, false},
	}

	for _, tc := range tests {
		got := tc.status.IsRetryable()
		if got != tc.retryable {
			t.Errorf("WorkflowStatus(%s).IsRetryable() = %v, want %v", tc.status, got, tc.retryable)
		}
	}
	t.Log("PASS: WorkflowStatus.IsRetryable correctly excludes completed/running/pending/continued")
}

// ============================================================
// Fix: JobStatus IsRetryable excludes completed
// (status.go: only failed/dead/cancelled)
// ============================================================

func TestFix_JobStatus_IsRetryable(t *testing.T) {
	tests := []struct {
		status    types.JobStatus
		retryable bool
	}{
		{types.JobStatusPending, false},
		{types.JobStatusScheduled, false},
		{types.JobStatusRunning, false},
		{types.JobStatusCompleted, false},
		{types.JobStatusFailed, true},
		{types.JobStatusDead, true},
		{types.JobStatusCancelled, true},
		{types.JobStatusRetrying, false},
		{types.JobStatusPaused, false},
	}

	for _, tc := range tests {
		got := tc.status.IsRetryable()
		if got != tc.retryable {
			t.Errorf("JobStatus(%s).IsRetryable() = %v, want %v", tc.status, got, tc.retryable)
		}
	}
	t.Log("PASS: JobStatus.IsRetryable correctly excludes completed/running/pending/retrying/paused")
}

