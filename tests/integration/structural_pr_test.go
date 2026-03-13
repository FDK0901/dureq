// Package integration — tests for structural PRs 1-3.
//
// PR1: Signal stats endpoint
// PR2: Durable cancellation (cancel flag persisted + polling)
// PR3: FiringID schedule firing dedup
package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/dispatcher"
	"github.com/FDK0901/dureq/internal/monitor"
	"github.com/FDK0901/dureq/pkg/types"
)

// ============================================================
// PR1: Signal stats endpoint
// ============================================================

func TestPR1_SignalStats(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()

	// Create a workflow for signal testing.
	wfID := "test-signal-stats"
	wf := &types.WorkflowInstance{
		ID:           wfID,
		WorkflowName: "signal-stats-test",
		Status:       types.WorkflowStatusRunning,
		Tasks:        map[string]types.WorkflowTaskState{},
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
	st.SaveWorkflow(ctx, wf)

	// No signals yet — stats should be zero.
	stats, err := st.GetSignalStats(ctx, wfID)
	if err != nil {
		t.Fatalf("GetSignalStats (empty): %v", err)
	}
	if stats.PendingCount != 0 {
		t.Fatalf("expected 0 pending, got %d", stats.PendingCount)
	}

	// Send 3 signals.
	for i := 0; i < 3; i++ {
		payload, _ := json.Marshal(map[string]int{"n": i})
		st.SendSignal(ctx, &types.WorkflowSignal{
			WorkflowID: wfID,
			Name:       "test",
			Payload:    payload,
		})
	}

	// Stats should show 3 pending.
	stats, err = st.GetSignalStats(ctx, wfID)
	if err != nil {
		t.Fatalf("GetSignalStats: %v", err)
	}
	if stats.PendingCount != 3 {
		t.Fatalf("expected 3 pending, got %d", stats.PendingCount)
	}
	if stats.OldestUnackedMs < 0 {
		t.Fatalf("oldest unacked age should be >= 0, got %d", stats.OldestUnackedMs)
	}

	// Ack 2 signals.
	sigs, _ := st.ReadSignals(ctx, wfID)
	st.AckSignals(ctx, wfID, []string{sigs[0].StreamID, sigs[1].StreamID})

	// Stats should show 1 pending.
	stats, err = st.GetSignalStats(ctx, wfID)
	if err != nil {
		t.Fatalf("GetSignalStats (after ack): %v", err)
	}
	if stats.PendingCount != 1 {
		t.Fatalf("expected 1 pending after acking 2, got %d", stats.PendingCount)
	}

	t.Logf("PASS: signal stats — pending count tracks correctly, oldest age = %dms", stats.OldestUnackedMs)
}

// PR1: Signal stats via monitor API endpoint.
func TestPR1_SignalStats_MonitorAPI(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	srv := newServer(t, "pr1-signal-api-node", 10)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	st := srv.Store()
	apiSvc := monitor.NewAPIService(st, dispatcher.New(st, nil), nil)
	defer apiSvc.Shutdown()

	// Create workflow + send signals.
	wfID := "test-signal-api"
	wf := &types.WorkflowInstance{
		ID:           wfID,
		WorkflowName: "signal-api-test",
		Status:       types.WorkflowStatusRunning,
		Tasks:        map[string]types.WorkflowTaskState{},
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
	st.SaveWorkflow(ctx, wf)

	for i := 0; i < 2; i++ {
		payload, _ := json.Marshal(map[string]int{"n": i})
		st.SendSignal(ctx, &types.WorkflowSignal{
			WorkflowID: wfID,
			Name:       "test",
			Payload:    payload,
		})
	}

	stats, err := apiSvc.GetWorkflowSignalStats(ctx, wfID)
	if err != nil {
		t.Fatalf("GetWorkflowSignalStats: %v", err)
	}
	if stats.PendingCount != 2 {
		t.Fatalf("expected 2 pending via API, got %d", stats.PendingCount)
	}

	t.Log("PASS: signal stats endpoint returns correct data via APIService")
}

// ============================================================
// PR2: Durable cancellation — cancel flag survives Pub/Sub loss
// ============================================================

func TestPR2_DurableCancel_FlagBeforeHandlerStart(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "pr2-cancel-flag-node", 10)

	handlerStarted := make(chan struct{})
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "pr2.slow",
		Handler: func(ctx context.Context, _ json.RawMessage) error {
			close(handlerStarted)
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

	// Enqueue job and wait for it to start.
	job, err := cli.Enqueue(ctx, "pr2.slow", map[string]string{"test": "cancel-flag"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case <-handlerStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("handler did not start within 10s")
	}

	// Cancel the job — this sets the durable cancel flag.
	if err := cli.Cancel(ctx, job.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	// Verify: the cancel flag was set.
	runs, _ := srv.Store().ListActiveRunsByJobID(ctx, job.ID)
	if len(runs) > 0 {
		cancelled, err := srv.Store().IsCancelRequested(ctx, runs[0].ID)
		if err != nil {
			t.Fatalf("IsCancelRequested: %v", err)
		}
		if !cancelled {
			t.Fatal("expected cancel flag to be set after Cancel()")
		}
		t.Logf("PASS: cancel flag set for run %s", runs[0].ID)
	}

	// Wait for the job to reach terminal state (handler context cancelled via polling or Pub/Sub).
	completedJob := waitJobTerminal(t, cli, job.ID, 30*time.Second)
	t.Logf("PASS: job reached terminal state %s after durable cancel", completedJob.Status)
}

func TestPR2_DurableCancel_StoreOps(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	srv := newServer(t, "pr2-cancel-ops-node", 10)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	st := srv.Store()

	// Initially no cancel flag.
	cancelled, err := st.IsCancelRequested(ctx, "nonexistent-run")
	if err != nil {
		t.Fatalf("IsCancelRequested: %v", err)
	}
	if cancelled {
		t.Fatal("expected no cancel flag for nonexistent run")
	}

	// Set cancel flag.
	if err := st.RequestCancel(ctx, "test-run-1"); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}

	// Verify flag is set.
	cancelled, err = st.IsCancelRequested(ctx, "test-run-1")
	if err != nil {
		t.Fatalf("IsCancelRequested: %v", err)
	}
	if !cancelled {
		t.Fatal("expected cancel flag to be set")
	}

	// Clear flag.
	st.ClearCancelFlag(ctx, "test-run-1")

	// Verify flag is cleared.
	cancelled, _ = st.IsCancelRequested(ctx, "test-run-1")
	if cancelled {
		t.Fatal("expected cancel flag to be cleared")
	}

	t.Log("PASS: RequestCancel/IsCancelRequested/ClearCancelFlag lifecycle works")
}

// ============================================================
// PR3: FiringID dedup — same firing dispatched once
// ============================================================

func TestPR3_FiringID_Dedup(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	srv := newServer(t, "pr3-firing-node", 10)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	st := srv.Store()

	// Simulate two dispatches with the same FiringID (leader failover scenario).
	msg1 := &types.WorkMessage{
		RunID:    "run-1",
		JobID:    "job-1",
		TaskType: "pr3.task",
		Attempt:  1,
		FiringID: "job-1:1700000000000",
	}

	msg2 := &types.WorkMessage{
		RunID:    "run-2",
		JobID:    "job-1",
		TaskType: "pr3.task",
		Attempt:  1,
		FiringID: "job-1:1700000000000", // same FiringID
	}

	// First dispatch should succeed.
	id1, err := st.DispatchWork(ctx, "default", msg1)
	if err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	if id1 == "" {
		t.Fatal("first dispatch returned empty stream ID (dedup collision)")
	}

	// Second dispatch with same FiringID should be silently skipped.
	id2, err := st.DispatchWork(ctx, "default", msg2)
	if err != nil {
		t.Fatalf("second dispatch: %v", err)
	}
	if id2 != "" {
		t.Fatalf("expected second dispatch to be skipped (FiringID dedup), got stream ID %s", id2)
	}

	// Third dispatch with a different FiringID should succeed.
	msg3 := &types.WorkMessage{
		RunID:    "run-3",
		JobID:    "job-1",
		TaskType: "pr3.task",
		Attempt:  1,
		FiringID: "job-1:1700000001000", // different firing time
	}
	id3, err := st.DispatchWork(ctx, "default", msg3)
	if err != nil {
		t.Fatalf("third dispatch: %v", err)
	}
	if id3 == "" {
		t.Fatal("third dispatch with different FiringID should succeed")
	}

	t.Log("PASS: FiringID dedup prevents duplicate dispatch of same schedule firing")
}

func TestPR3_FiringID_NoFiringID_SkipsDedup(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	srv := newServer(t, "pr3-nofiring-node", 10)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	st := srv.Store()

	// Dispatches without FiringID should not be deduped (ad-hoc jobs).
	msg1 := &types.WorkMessage{
		RunID:    "adhoc-run-1",
		JobID:    "job-2",
		TaskType: "pr3.task",
		Attempt:  1,
		// No FiringID
	}
	msg2 := &types.WorkMessage{
		RunID:    "adhoc-run-2",
		JobID:    "job-2",
		TaskType: "pr3.task",
		Attempt:  1,
		// No FiringID
	}

	id1, err := st.DispatchWork(ctx, "default", msg1)
	if err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	id2, err := st.DispatchWork(ctx, "default", msg2)
	if err != nil {
		t.Fatalf("second dispatch: %v", err)
	}

	if id1 == "" || id2 == "" {
		t.Fatal("both ad-hoc dispatches should succeed (no FiringID dedup)")
	}

	t.Log("PASS: dispatches without FiringID are not deduped")
}
