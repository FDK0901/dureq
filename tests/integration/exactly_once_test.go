// Package integration — tests for exactly-once improvements.
//
// Phase 1: OperationKey derivation and propagation
// Phase 2: Atomic dispatch (Lua script dedup + XADD)
// Phase 3: Operation ledger claim check
// Phase 4: Two-phase completion (ledger commit + pipeline)
// Phase 5: Durability config
// Phase 6: Operation-scoped effect ledger (L3)
package integration

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/dispatcher"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
)

// xrangeAll reads all entries from a stream using XRANGE - +.
func xrangeAll(t *testing.T, st *store.RedisStore, streamKey string) []map[string]string {
	t.Helper()
	ctx := context.Background()
	rdb := st.Client()
	entries, err := rdb.Do(ctx, rdb.B().Xrange().Key(streamKey).Start("-").End("+").Build()).AsXRange()
	if err != nil {
		t.Fatalf("XRANGE %s: %v", streamKey, err)
	}
	var result []map[string]string
	for _, e := range entries {
		result = append(result, e.FieldValues)
	}
	return result
}

// ============================================================
// Phase 1: OperationKey derivation and propagation
// ============================================================

func TestPhase1_OperationKey_ImmediateJob(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()
	disp := dispatcher.New(st, nil)

	job := &types.Job{
		ID:       "job-opkey-immediate",
		TaskType: "test-task",
		Payload:  json.RawMessage(`{"x":1}`),
		Schedule: types.Schedule{Type: types.ScheduleImmediate},
		Status:   types.JobStatusPending,
	}

	if err := disp.Dispatch(ctx, job, 0); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	entries := xrangeAll(t, st, store.WorkStreamKey(st.Prefix(), "normal"))
	if len(entries) == 0 {
		t.Fatal("expected at least 1 stream entry")
	}
	opKey := entries[0]["operation_key"]
	if opKey != job.ID {
		t.Fatalf("expected operation_key=%q, got %q", job.ID, opKey)
	}
}

func TestPhase1_OperationKey_ScheduledJob(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()
	disp := dispatcher.New(st, nil)

	firingID := "sched-job:1234567890"
	job := &types.Job{
		ID:       "sched-job",
		TaskType: "test-task",
		Payload:  json.RawMessage(`{}`),
		Schedule: types.Schedule{Type: types.ScheduleImmediate},
		Status:   types.JobStatusPending,
		Headers:  map[string]string{"x-dureq-firing-id": firingID},
	}

	if err := disp.Dispatch(ctx, job, 0); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	entries := xrangeAll(t, st, store.WorkStreamKey(st.Prefix(), "normal"))
	if len(entries) == 0 {
		t.Fatal("expected at least 1 stream entry")
	}
	opKey := entries[0]["operation_key"]
	if opKey != firingID {
		t.Fatalf("expected operation_key=%q (FiringID), got %q", firingID, opKey)
	}
}

func TestPhase1_OperationKey_WorkflowTask(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()
	disp := dispatcher.New(st, nil)

	wfID := "wf-123"
	taskName := "step-a"
	job := &types.Job{
		ID:       "wf-task-job",
		TaskType: "test-task",
		Payload:  json.RawMessage(`{}`),
		Schedule: types.Schedule{Type: types.ScheduleImmediate},
		Status:   types.JobStatusPending,
		Headers:  map[string]string{"x-dureq-operation-key": wfID + ":" + taskName},
	}

	if err := disp.Dispatch(ctx, job, 0); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	entries := xrangeAll(t, st, store.WorkStreamKey(st.Prefix(), "normal"))
	if len(entries) == 0 {
		t.Fatal("expected at least 1 stream entry")
	}
	opKey := entries[0]["operation_key"]
	expected := wfID + ":" + taskName
	if opKey != expected {
		t.Fatalf("expected operation_key=%q, got %q", expected, opKey)
	}
}

// ============================================================
// Phase 2: Atomic dispatch dedup
// ============================================================

func TestPhase2_AtomicDispatch_DuplicateRunID(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()

	wm := &types.WorkMessage{
		RunID:        "run-dedup-test",
		JobID:        "job-dedup-test",
		TaskType:     "test-task",
		Payload:      json.RawMessage(`{}`),
		Attempt:      0,
		Deadline:     time.Now().Add(time.Hour),
		DispatchedAt: time.Now(),
		OperationKey: "job-dedup-test",
	}

	// First dispatch should succeed.
	msgID1, err := st.DispatchWork(ctx, "normal", wm)
	if err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	if msgID1 == "" {
		t.Fatal("first dispatch returned empty msgID")
	}

	// Second dispatch with same RunID should be idempotent (DUP).
	msgID2, err := st.DispatchWork(ctx, "normal", wm)
	if err != nil {
		t.Fatalf("second dispatch: %v", err)
	}
	if msgID2 != "" {
		t.Fatalf("expected empty msgID (DUP), got %q", msgID2)
	}

	// Verify only 1 message in stream.
	entries := xrangeAll(t, st, store.WorkStreamKey(st.Prefix(), "normal"))
	if len(entries) != 1 {
		t.Fatalf("expected 1 stream entry, got %d", len(entries))
	}
}

func TestPhase2_AtomicDispatch_DifferentRunIDs(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()

	for i, runID := range []string{"run-a", "run-b", "run-c"} {
		wm := &types.WorkMessage{
			RunID:        runID,
			JobID:        "job-multi",
			TaskType:     "test-task",
			Payload:      json.RawMessage(`{}`),
			Attempt:      i,
			Deadline:     time.Now().Add(time.Hour),
			DispatchedAt: time.Now(),
			OperationKey: "job-multi",
		}
		st.DispatchWork(ctx, "normal", wm)
	}

	// All 3 should be in stream (different RunIDs).
	entries := xrangeAll(t, st, store.WorkStreamKey(st.Prefix(), "normal"))
	if len(entries) != 3 {
		t.Fatalf("expected 3 stream entries, got %d", len(entries))
	}
}

// ============================================================
// Phase 3: Operation ledger claim check
// ============================================================

func TestPhase3_OperationLedger_ClaimAndComplete(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()
	opKey := "test-op-claim"

	// Initial check: should return "ok" and set status=claimed.
	result, err := st.CheckAndClaimOperation(ctx, opKey, "run-1")
	if err != nil {
		t.Fatalf("CheckAndClaimOperation: %v", err)
	}
	if result != "ok" {
		t.Fatalf("expected 'ok', got %q", result)
	}

	// Second claim for same opKey: should still return "ok" (already claimed, not done).
	result, err = st.CheckAndClaimOperation(ctx, opKey, "run-2")
	if err != nil {
		t.Fatalf("CheckAndClaimOperation (second): %v", err)
	}
	if result != "ok" {
		t.Fatalf("expected 'ok', got %q", result)
	}

	// Complete the operation.
	isNew, err := st.CompleteOperation(ctx, opKey, "run-1", `{"status":"done"}`)
	if err != nil {
		t.Fatalf("CompleteOperation: %v", err)
	}
	if !isNew {
		t.Fatal("expected isNew=true for first completion")
	}

	// After completion: claim should return "DONE:..."
	result, err = st.CheckAndClaimOperation(ctx, opKey, "run-3")
	if err != nil {
		t.Fatalf("CheckAndClaimOperation (after complete): %v", err)
	}
	if !strings.HasPrefix(result, "DONE:") {
		t.Fatalf("expected DONE prefix, got %q", result)
	}
	if !strings.Contains(result, `"status":"done"`) {
		t.Fatalf("expected result JSON in DONE response, got %q", result)
	}
}

func TestPhase3_OperationLedger_EmptyOpKey_SkipsCheck(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()

	// Empty opKey should always return "ok" (backward compatible).
	result, err := st.CheckAndClaimOperation(ctx, "", "run-1")
	if err != nil {
		t.Fatalf("CheckAndClaimOperation (empty): %v", err)
	}
	if result != "ok" {
		t.Fatalf("expected 'ok' for empty opKey, got %q", result)
	}
}

// ============================================================
// Phase 4: Atomic completion (two-phase)
// ============================================================

func TestPhase4_CompleteOperation_Idempotent(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()
	opKey := "test-op-complete-idem"

	// First completion.
	isNew, err := st.CompleteOperation(ctx, opKey, "run-1", `{"x":1}`)
	if err != nil {
		t.Fatalf("CompleteOperation (first): %v", err)
	}
	if !isNew {
		t.Fatal("expected isNew=true")
	}

	// Second completion with same opKey: should be idempotent.
	isNew, err = st.CompleteOperation(ctx, opKey, "run-2", `{"x":2}`)
	if err != nil {
		t.Fatalf("CompleteOperation (second): %v", err)
	}
	if isNew {
		t.Fatal("expected isNew=false for duplicate completion")
	}
}

func TestPhase4_CompleteOperation_EmptyOpKey_Passthrough(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()

	// Empty opKey should always return true (backward compatible).
	isNew, err := st.CompleteOperation(ctx, "", "run-1", `{}`)
	if err != nil {
		t.Fatalf("CompleteOperation (empty): %v", err)
	}
	if !isNew {
		t.Fatal("expected isNew=true for empty opKey passthrough")
	}
}

// ============================================================
// Phase 5: Durability config
// ============================================================

func TestPhase5_DurabilityConfig_Defaults(t *testing.T) {
	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()
	cfg := st.Config()

	if cfg.OperationLedgerTTL != 24*time.Hour {
		t.Fatalf("expected OperationLedgerTTL=24h, got %v", cfg.OperationLedgerTTL)
	}
	if cfg.DurabilityTimeout != 500*time.Millisecond {
		t.Fatalf("expected DurabilityTimeout=500ms, got %v", cfg.DurabilityTimeout)
	}
	if cfg.DurabilityLevel != "" {
		t.Fatalf("expected empty DurabilityLevel, got %q", cfg.DurabilityLevel)
	}
}

// ============================================================
// Phase 6: Operation-scoped effect ledger (L3)
// ============================================================

func TestPhase6_OperationEffect_ClaimAndComplete(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()
	opKey := "test-op-effect"
	stepKey := "send-email"

	// Claim: should return newly claimed.
	result, done, err := st.ClaimOperationEffect(ctx, opKey, stepKey, 3600)
	if err != nil {
		t.Fatalf("ClaimOperationEffect: %v", err)
	}
	if done {
		t.Fatal("expected done=false for fresh claim")
	}
	if result != "" {
		t.Fatalf("expected empty result for fresh claim, got %q", result)
	}

	// Second claim: should return ErrEffectPending (not done, not newly claimed).
	_, done, err = st.ClaimOperationEffect(ctx, opKey, stepKey, 3600)
	if !errors.Is(err, types.ErrEffectPending) {
		t.Fatalf("expected ErrEffectPending, got err=%v done=%v", err, done)
	}
	if done {
		t.Fatal("expected done=false for pending")
	}

	// Complete the effect (CAS).
	isNew, err := st.CompleteOperationEffect(ctx, opKey, stepKey, `email-sent-123`)
	if err != nil {
		t.Fatalf("CompleteOperationEffect: %v", err)
	}
	if !isNew {
		t.Fatal("expected isNew=true for first completion")
	}

	// Stale complete: should be rejected (CAS).
	isNew, err = st.CompleteOperationEffect(ctx, opKey, stepKey, `stale-result`)
	if err != nil {
		t.Fatalf("CompleteOperationEffect (stale): %v", err)
	}
	if isNew {
		t.Fatal("expected isNew=false for stale completer")
	}

	// Third claim: should return cached result.
	result, done, err = st.ClaimOperationEffect(ctx, opKey, stepKey, 3600)
	if err != nil {
		t.Fatalf("ClaimOperationEffect (after complete): %v", err)
	}
	if !done {
		t.Fatal("expected done=true after completion")
	}
	if result != "email-sent-123" {
		t.Fatalf("expected cached result 'email-sent-123', got %q", result)
	}
}

func TestPhase6_OperationEffect_DifferentSteps(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()
	opKey := "test-op-multi-step"

	// Claim two different steps.
	_, done1, err1 := st.ClaimOperationEffect(ctx, opKey, "step-a", 3600)
	_, done2, err2 := st.ClaimOperationEffect(ctx, opKey, "step-b", 3600)

	if done1 || done2 || err1 != nil || err2 != nil {
		t.Fatalf("expected both steps freshly claimed, got done1=%v err1=%v done2=%v err2=%v", done1, err1, done2, err2)
	}

	// Complete step-a.
	st.CompleteOperationEffect(ctx, opKey, "step-a", "result-a")

	// step-a should be done, step-b should be pending (ErrEffectPending).
	resultA, doneA, errA := st.ClaimOperationEffect(ctx, opKey, "step-a", 3600)
	_, _, errB := st.ClaimOperationEffect(ctx, opKey, "step-b", 3600)

	if !doneA || errA != nil {
		t.Fatalf("expected step-a done, got done=%v err=%v", doneA, errA)
	}
	if resultA != "result-a" {
		t.Fatalf("expected 'result-a', got %q", resultA)
	}
	if !errors.Is(errB, types.ErrEffectPending) {
		t.Fatalf("expected step-b ErrEffectPending, got %v", errB)
	}
}
