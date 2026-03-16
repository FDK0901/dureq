// Package integration — tests for exactly-once audit fixes.
//
// These tests cover the risk scenarios identified across 8 audit passes:
//   - Fix A: CompleteRun replay doesn't double-count stats
//   - Fix B: Crash-after-commit repair stores result + ACKs + emits event
//   - Fix H: Event XADD always runs (even on replay) for orchestrator progression
//   - Fix J: Repair event uses proper JobEvent payload (not WorkResult)
//   - Fix K/L: Orchestrator idempotency + identity guards
//   - Fix D: Client dispatchJob propagates OperationKey
//   - Fix G: Ledger TTL refresh during heartbeat
//   - PG: Transactional completion uses OperationKey as dedup key
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/FDK0901/dureq/internal/dispatcher"
	"github.com/FDK0901/dureq/internal/store"
	pgcomp "github.com/FDK0901/dureq/pkg/integration/postgres"
	"github.com/FDK0901/dureq/pkg/types"
)

// ============================================================
// Fix A: CompleteRun replay skips stats but keeps events
// ============================================================

func TestFixA_CompleteRun_Replay_StatsNotDoubled(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()
	opKey := "fix-a-replay-stats"

	// First completion (isNew=true) — stats should be incremented.
	isNew, err := st.CompleteOperation(ctx, opKey, "run-1", `{"ok":true}`)
	if err != nil {
		t.Fatalf("CompleteOperation: %v", err)
	}
	if !isNew {
		t.Fatal("expected isNew=true")
	}

	// Read daily stats before CompleteRun.
	rdb := st.Client()
	date := time.Now().Format("2006-01-02")
	statsKey := fmt.Sprintf("%s:stats:daily:%s", st.Prefix(), date)

	// Simulate first CompleteRun: build a minimal CompletionBatch.
	run := &types.JobRun{
		ID:        "run-1",
		JobID:     "job-fix-a",
		Status:    types.RunStatusSucceeded,
		StartedAt: time.Now(),
	}
	batch := &store.CompletionBatch{
		Run: run,
		Event: types.JobEvent{
			Type:      types.EventJobCompleted,
			JobID:     "job-fix-a",
			RunID:     "run-1",
			Timestamp: time.Now(),
		},
		Result: types.WorkResult{
			RunID:   "run-1",
			JobID:   "job-fix-a",
			Success: true,
		},
		DailyStatField: "processed",
		OperationKey:   opKey,
	}
	if err := st.CompleteRun(ctx, batch); err != nil {
		t.Fatalf("CompleteRun (first): %v", err)
	}

	// Read stats after first completion.
	count1, _ := rdb.Do(ctx, rdb.B().Hget().Key(statsKey).Field("processed").Build()).ToInt64()

	// Second CompleteRun (replay, isNew=false) — stats should NOT increment again.
	if err := st.CompleteRun(ctx, batch); err != nil {
		t.Fatalf("CompleteRun (replay): %v", err)
	}

	count2, _ := rdb.Do(ctx, rdb.B().Hget().Key(statsKey).Field("processed").Build()).ToInt64()

	if count2 != count1 {
		t.Fatalf("stats double-counted: after first=%d, after replay=%d", count1, count2)
	}
	t.Logf("PASS: stats not doubled (count=%d after replay)", count2)
}

// ============================================================
// Fix B/J: Crash-after-commit repair stores result + emits JobEvent
// ============================================================

func TestFixBJ_RepairCompletedOperation_StoresResult(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()

	// Simulate: operation ledger is DONE but result was never stored
	// (crash between Phase 1 and Phase 2).
	opKey := "fix-bj-repair"
	st.CompleteOperation(ctx, opKey, "run-repair", `{"success":true,"job_id":"job-repair"}`)

	// Ensure streams exist for XACK.
	st.EnsureStreams(ctx)

	// Call repair path.
	st.RepairCompletedOperation(ctx, "job-repair", "", "", `{"success":true,"job_id":"job-repair"}`)

	// Verify result was stored.
	result, err := st.GetResult(ctx, "job-repair")
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if result == nil {
		t.Fatal("result not stored by repair")
	}
	if !result.Success {
		t.Fatal("expected result.Success=true")
	}

	// Verify event was published to the events stream (Fix J: proper JobEvent payload).
	rdb := st.Client()
	entries, err := rdb.Do(ctx, rdb.B().Xrange().Key(store.EventsStreamKey(st.Prefix())).Start("-").End("+").Build()).AsXRange()
	if err != nil {
		t.Fatalf("XRANGE events: %v", err)
	}

	foundCompletionEvent := false
	for _, e := range entries {
		if e.FieldValues["type"] == string(types.EventJobCompleted) && e.FieldValues["job_id"] == "job-repair" {
			// Verify the data field is a proper JobEvent JSON (not WorkResult).
			var event types.JobEvent
			if json.Unmarshal([]byte(e.FieldValues["data"]), &event) == nil && event.Type == types.EventJobCompleted {
				foundCompletionEvent = true
			}
		}
	}
	if !foundCompletionEvent {
		t.Fatal("repair did not emit proper EventJobCompleted to events stream")
	}
	t.Log("PASS: repair stored result + emitted proper JobEvent to events stream")
}

// ============================================================
// Fix H: Event XADD always runs on replay (for orchestrator)
// ============================================================

func TestFixH_CompleteRun_Replay_EventSkippedOnReplay(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()
	st.EnsureStreams(ctx)

	opKey := "fix-h-replay-event"
	run := &types.JobRun{
		ID:        "run-h",
		JobID:     "job-fix-h",
		Status:    types.RunStatusSucceeded,
		StartedAt: time.Now(),
	}
	batch := &store.CompletionBatch{
		Run: run,
		Event: types.JobEvent{
			Type:      types.EventJobCompleted,
			JobID:     "job-fix-h",
			RunID:     "run-h",
			Timestamp: time.Now(),
		},
		Result: types.WorkResult{
			RunID:   "run-h",
			JobID:   "job-fix-h",
			Success: true,
		},
		DailyStatField: "processed",
		OperationKey:   opKey,
	}

	// First CompleteRun — events fire (firstCompletion=true).
	st.CompleteRun(ctx, batch)

	rdb := st.Client()
	count1, _ := rdb.Do(ctx, rdb.B().Xlen().Key(store.EventsStreamKey(st.Prefix())).Build()).ToInt64()
	if count1 == 0 {
		t.Fatal("first CompleteRun did not emit event")
	}

	// Replay CompleteRun (isNew=false) — events should NOT fire again.
	// Crash recovery events are handled by RepairCompletedOperation, not CompleteRun.
	st.CompleteRun(ctx, batch)

	count2, _ := rdb.Do(ctx, rdb.B().Xlen().Key(store.EventsStreamKey(st.Prefix())).Build()).ToInt64()
	if count2 != count1 {
		t.Fatalf("replay emitted duplicate event: count before=%d, after=%d", count1, count2)
	}
	t.Logf("PASS: replay skipped event emission (count=%d, no duplicate)", count2)
}

// ============================================================
// Fix D: Client dispatchJob propagates OperationKey
// ============================================================

func TestFixD_ClientDispatchJob_SetsOperationKey(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()

	// Enqueue a job via the client SDK.
	job, err := cli.Enqueue(ctx, "test-client-opkey", json.RawMessage(`{"x":1}`))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Read the stream and verify OperationKey is set.
	entries := xrangeAll(t, st, store.WorkStreamKey(st.Prefix(), "normal"))
	if len(entries) == 0 {
		t.Fatal("expected at least 1 stream entry")
	}

	// Find the entry matching our job.
	var found bool
	for _, e := range entries {
		if e["job_id"] == job.ID {
			opKey := e["operation_key"]
			if opKey == "" {
				t.Fatalf("client-dispatched job has empty operation_key")
			}
			if opKey != job.ID {
				t.Fatalf("expected operation_key=%q (jobID), got %q", job.ID, opKey)
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("job %s not found in stream", job.ID)
	}
	t.Logf("PASS: client-dispatched job has operation_key=%s", job.ID)
}

// ============================================================
// Fix G: Ledger TTL refresh method works
// ============================================================

func TestFixG_LedgerTTL_RefreshExtendsTTL(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()
	opKey := "fix-g-ttl-refresh"

	// Claim the operation (sets TTL).
	st.CheckAndClaimOperation(ctx, opKey, "run-g")

	// Read initial TTL.
	rdb := st.Client()
	ledgerKey := store.OperationLedgerKey(st.Prefix(), opKey)
	ttl1, _ := rdb.Do(ctx, rdb.B().Ttl().Key(ledgerKey).Build()).ToInt64()
	if ttl1 <= 0 {
		t.Fatalf("expected positive TTL, got %d", ttl1)
	}

	// Simulate time passing by reducing TTL manually.
	rdb.Do(ctx, rdb.B().Expire().Key(ledgerKey).Seconds(60).Build())

	// Refresh via RefreshOperationLedger.
	st.RefreshOperationLedger(ctx, opKey)

	// TTL should be back to full OperationLedgerTTL (~86400).
	ttl2, _ := rdb.Do(ctx, rdb.B().Ttl().Key(ledgerKey).Build()).ToInt64()
	if ttl2 <= 60 {
		t.Fatalf("TTL not refreshed: expected >60, got %d", ttl2)
	}
	t.Logf("PASS: ledger TTL refreshed from 60s to %ds", ttl2)
}

// ============================================================
// Fix K: Operation ledger claim → complete idempotency
// ============================================================

func TestFixK_LedgerClaim_Complete_Replay_Idempotent(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()
	opKey := "fix-k-idem"

	// Claim.
	result, _ := st.CheckAndClaimOperation(ctx, opKey, "run-k")
	if result != "ok" {
		t.Fatalf("expected ok, got %q", result)
	}

	// Complete.
	isNew, _ := st.CompleteOperation(ctx, opKey, "run-k", `{"done":true}`)
	if !isNew {
		t.Fatal("expected isNew=true")
	}

	// Replay complete — should be idempotent.
	isNew, _ = st.CompleteOperation(ctx, opKey, "run-k-retry", `{"done":true}`)
	if isNew {
		t.Fatal("expected isNew=false on replay")
	}

	// Claim should see DONE.
	result, _ = st.CheckAndClaimOperation(ctx, opKey, "run-k-2")
	if result[:5] != "DONE:" {
		t.Fatalf("expected DONE:, got %q", result)
	}
	t.Log("PASS: ledger claim → complete → replay → DONE lifecycle")
}

// ============================================================
// Fix L/M: Orchestrator identity guards (stale event rejection)
// ============================================================

func TestFixL_Orchestrator_StaleEvent_WorkflowTask(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()

	// Create a workflow with a task that has JobID=A.
	wfID := "wf-stale-test"
	wf := &types.WorkflowInstance{
		ID:           wfID,
		WorkflowName: "stale-test",
		Status:       types.WorkflowStatusRunning,
		Tasks: map[string]types.WorkflowTaskState{
			"step-a": {
				Name:   "step-a",
				JobID:  "job-B",
				Status: types.JobStatusRunning,
			},
		},
		Definition: types.WorkflowDefinition{
			Name: "stale-test",
			Tasks: []types.WorkflowTask{
				{Name: "step-a", TaskType: "handler-a"},
			},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	st.SaveWorkflow(ctx, wf)

	// Create job-A (old incarnation) and job-B (current incarnation).
	jobA := &types.Job{ID: "job-A", TaskType: "handler-a", Status: types.JobStatusCompleted, WorkflowID: &wfID}
	jobATask := "step-a"
	jobA.WorkflowTask = &jobATask
	st.SaveJob(ctx, jobA)

	jobB := &types.Job{ID: "job-B", TaskType: "handler-a", Status: types.JobStatusRunning, WorkflowID: &wfID}
	jobB.WorkflowTask = &jobATask
	st.SaveJob(ctx, jobB)

	// Simulate a stale EventJobCompleted for job-A arriving.
	// The identity guard should reject it because step-a.JobID == "job-B" != "job-A".
	srv := newServer(t, "stale-event-node", 10)
	defer srv.Stop()
	srv.Start(ctx)

	// Publish stale event.
	st.PublishEvent(ctx, types.JobEvent{
		Type:      types.EventJobCompleted,
		JobID:     "job-A", // stale — current incarnation is job-B
		Timestamp: time.Now(),
	})

	// Wait a bit for orchestrator to process.
	time.Sleep(500 * time.Millisecond)

	// Verify step-a is still Running (not completed by stale event).
	wfAfter, _, _ := st.GetWorkflow(ctx, wfID)
	if wfAfter.Tasks["step-a"].Status != types.JobStatusRunning {
		t.Fatalf("stale event corrupted task state: expected Running, got %s", wfAfter.Tasks["step-a"].Status)
	}
	t.Log("PASS: stale completion event for old incarnation was rejected")
}

// ============================================================
// PG: Transactional completion uses OperationKey
// ============================================================

func TestPG_CompleteTx_UsesOperationKey(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	cleanupTables(t, db)

	ctx := context.Background()
	if err := pgcomp.EnsureTable(ctx, db); err != nil {
		t.Fatalf("EnsureTable: %v", err)
	}

	// Context with both RunID and OperationKey.
	ctx = types.WithRunID(ctx, "run-pg-opkey")
	ctx = types.WithJobID(ctx, "job-pg-opkey")
	ctx = types.WithOperationKey(ctx, "op-pg-stable-key")

	// First completion in TX.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := pgcomp.CompleteTx(ctx, tx); err != nil {
		t.Fatalf("CompleteTx: %v", err)
	}
	tx.Commit()

	// Verify the completion marker used OperationKey (not RunID) as run_id.
	var storedRunID string
	db.QueryRowContext(ctx, "SELECT run_id FROM dureq_completions WHERE step = ''").Scan(&storedRunID)
	if storedRunID != "op-pg-stable-key" {
		t.Fatalf("expected run_id='op-pg-stable-key' (OperationKey), got %q", storedRunID)
	}

	// Second attempt with a DIFFERENT RunID but SAME OperationKey — should be ErrAlreadyCompleted.
	ctx2 := types.WithRunID(context.Background(), "run-pg-opkey-retry")
	ctx2 = types.WithJobID(ctx2, "job-pg-opkey")
	ctx2 = types.WithOperationKey(ctx2, "op-pg-stable-key")

	tx2, _ := db.BeginTx(ctx2, nil)
	err = pgcomp.CompleteTx(ctx2, tx2)
	tx2.Rollback()

	if err != pgcomp.ErrAlreadyCompleted {
		t.Fatalf("expected ErrAlreadyCompleted, got %v", err)
	}
	t.Log("PASS: PG completion uses OperationKey, dedup works across different RunIDs")
}

// ============================================================
// Atomic Dispatch: FiringID + OperationKey interaction
// ============================================================

func TestAtomicDispatch_FiringID_BlocksDuplicate(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()
	disp := dispatcher.New(st, nil)

	firingID := "sched-dedup-test:1000"
	job := &types.Job{
		ID:       "sched-dedup-job",
		TaskType: "test-task",
		Payload:  json.RawMessage(`{}`),
		Schedule: types.Schedule{Type: types.ScheduleImmediate},
		Status:   types.JobStatusPending,
		Headers:  map[string]string{"x-dureq-firing-id": firingID},
	}

	// First dispatch succeeds.
	if err := disp.Dispatch(ctx, job, 0); err != nil {
		t.Fatalf("dispatch 1: %v", err)
	}

	// Second dispatch with same FiringID (simulating leader failover) — should be idempotent.
	if err := disp.Dispatch(ctx, job, 0); err != nil {
		t.Fatalf("dispatch 2: %v", err)
	}

	// Verify only 1 message on stream.
	entries := xrangeAll(t, st, store.WorkStreamKey(st.Prefix(), "normal"))
	if len(entries) != 1 {
		t.Fatalf("expected 1 stream entry (FiringID dedup), got %d", len(entries))
	}

	// Verify OperationKey == FiringID.
	if entries[0]["operation_key"] != firingID {
		t.Fatalf("expected operation_key=%q, got %q", firingID, entries[0]["operation_key"])
	}
	t.Logf("PASS: FiringID dedup blocked duplicate dispatch, OperationKey=%s", firingID)
}

// ============================================================
// Effect Ledger: CAS protection
// ============================================================

func TestEffectLedger_CAS_RejectsStaleCompleter(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()
	opKey := "effect-cas-test"
	step := "send-email"

	// Worker 1 claims.
	_, done, err := st.ClaimOperationEffect(ctx, opKey, step, 3600)
	if err != nil || done {
		t.Fatalf("claim: err=%v done=%v", err, done)
	}

	// Worker 1 completes.
	isNew, err := st.CompleteOperationEffect(ctx, opKey, step, "email-sent-100")
	if err != nil || !isNew {
		t.Fatalf("complete 1: err=%v isNew=%v", err, isNew)
	}

	// Stale worker 2 tries to complete — CAS rejects.
	isNew, err = st.CompleteOperationEffect(ctx, opKey, step, "stale-email-200")
	if err != nil {
		t.Fatalf("complete 2: %v", err)
	}
	if isNew {
		t.Fatal("stale completer should have been rejected by CAS")
	}

	// Verify original result is preserved.
	result, done, err := st.ClaimOperationEffect(ctx, opKey, step, 3600)
	if err != nil || !done || result != "email-sent-100" {
		t.Fatalf("expected cached result 'email-sent-100', got result=%q done=%v err=%v", result, done, err)
	}
	t.Log("PASS: CAS rejected stale completer, original result preserved")
}

// ============================================================
// Batch item stable OperationKey
// ============================================================

func TestBatchItem_StableOperationKey(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()

	// Dispatch a work message with batch-item operation key pattern.
	batchID := "batch-123"
	itemID := "item-A"
	wm := &types.WorkMessage{
		RunID:        "run-batch-item",
		JobID:        "job-batch-item",
		TaskType:     "test-task",
		Payload:      json.RawMessage(`{}`),
		Attempt:      0,
		Deadline:     time.Now().Add(time.Hour),
		DispatchedAt: time.Now(),
		Headers:      map[string]string{"x-dureq-operation-key": fmt.Sprintf("%s:item:%s", batchID, itemID)},
		OperationKey: fmt.Sprintf("%s:item:%s", batchID, itemID),
	}

	msgID, err := st.DispatchWork(ctx, "normal", wm)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if msgID == "" {
		t.Fatal("expected non-empty msgID")
	}

	// Verify OperationKey in stream.
	entries := xrangeAll(t, st, store.WorkStreamKey(st.Prefix(), "normal"))
	expected := fmt.Sprintf("%s:item:%s", batchID, itemID)
	if entries[0]["operation_key"] != expected {
		t.Fatalf("expected operation_key=%q, got %q", expected, entries[0]["operation_key"])
	}
	t.Logf("PASS: batch item OperationKey=%s", expected)
}

// ============================================================
// WAIT/WAITAOF durability check
// ============================================================

func TestWaitDurability_DefaultNone_NoError(t *testing.T) {
	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()
	cfg := st.Config()

	// Default DurabilityLevel is "" (none) — should not error.
	if cfg.DurabilityLevel != "" {
		t.Fatalf("expected empty DurabilityLevel, got %q", cfg.DurabilityLevel)
	}
	// Config defaults.
	if cfg.OperationLedgerTTL != 24*time.Hour {
		t.Fatalf("expected 24h, got %v", cfg.OperationLedgerTTL)
	}
	if cfg.DurabilityTimeout != 500*time.Millisecond {
		t.Fatalf("expected 500ms, got %v", cfg.DurabilityTimeout)
	}
	t.Log("PASS: durability config defaults verified")
}

// ============================================================
// Fix N: Stale FAILED event rejected by identity guard
// ============================================================

func TestFixN_Orchestrator_StaleFailedEvent_WorkflowTask(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()

	// Create a workflow where step-a is running with JobID=job-B (current incarnation).
	// job-A is the old incarnation (already dead/completed from a previous attempt).
	wfID := "wf-stale-fail-test"
	wf := &types.WorkflowInstance{
		ID:           wfID,
		WorkflowName: "stale-fail-test",
		Status:       types.WorkflowStatusRunning,
		Tasks: map[string]types.WorkflowTaskState{
			"step-a": {
				Name:   "step-a",
				JobID:  "job-B-fail",
				Status: types.JobStatusRunning,
			},
		},
		Definition: types.WorkflowDefinition{
			Name: "stale-fail-test",
			Tasks: []types.WorkflowTask{
				{Name: "step-a", TaskType: "handler-a"},
			},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	st.SaveWorkflow(ctx, wf)

	// job-A is old incarnation — already dead.
	jobA := &types.Job{ID: "job-A-fail", TaskType: "handler-a", Status: types.JobStatusDead, WorkflowID: &wfID}
	taskName := "step-a"
	jobA.WorkflowTask = &taskName
	st.SaveJob(ctx, jobA)

	// job-B is current incarnation — running.
	jobB := &types.Job{ID: "job-B-fail", TaskType: "handler-a", Status: types.JobStatusRunning, WorkflowID: &wfID}
	jobB.WorkflowTask = &taskName
	st.SaveJob(ctx, jobB)

	srv := newServer(t, "stale-fail-node", 10)
	defer srv.Stop()
	srv.Start(ctx)

	// Publish stale EventJobFailed for job-A (old incarnation).
	errStr := "old incarnation failure"
	st.PublishEvent(ctx, types.JobEvent{
		Type:      types.EventJobFailed,
		JobID:     "job-A-fail",
		Error:     &errStr,
		Timestamp: time.Now(),
	})

	time.Sleep(500 * time.Millisecond)

	// Verify step-a is still Running (stale failed event rejected by identity guard).
	wfAfter, _, _ := st.GetWorkflow(ctx, wfID)
	if wfAfter.Tasks["step-a"].Status != types.JobStatusRunning {
		t.Fatalf("stale failed event corrupted task: expected Running, got %s", wfAfter.Tasks["step-a"].Status)
	}
	// Verify workflow is still running (not failed).
	if wfAfter.Status != types.WorkflowStatusRunning {
		t.Fatalf("stale failed event corrupted workflow: expected Running, got %s", wfAfter.Status)
	}
	t.Log("PASS: stale failed event for old incarnation was rejected")
}

// ============================================================
// Fix N: EventWorkflowTaskCompleted emitted after guard
// ============================================================

func TestFixN_WorkflowTaskCompleted_EmittedAfterGuard(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()
	st.EnsureStreams(ctx)

	// Create workflow where step-a is running with JobID=job-current.
	wfID := "wf-preguard-test"
	wf := &types.WorkflowInstance{
		ID:           wfID,
		WorkflowName: "preguard-test",
		Status:       types.WorkflowStatusRunning,
		Tasks: map[string]types.WorkflowTaskState{
			"step-a": {
				Name:   "step-a",
				JobID:  "job-current",
				Status: types.JobStatusRunning,
			},
		},
		Definition: types.WorkflowDefinition{
			Name: "preguard-test",
			Tasks: []types.WorkflowTask{
				{Name: "step-a", TaskType: "handler-a"},
			},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	st.SaveWorkflow(ctx, wf)

	// Create the stale job (old incarnation).
	jobStale := &types.Job{ID: "job-stale-preguard", TaskType: "handler-a", Status: types.JobStatusCompleted, WorkflowID: &wfID}
	taskName := "step-a"
	jobStale.WorkflowTask = &taskName
	st.SaveJob(ctx, jobStale)

	srv := newServer(t, "preguard-node", 10)
	defer srv.Stop()
	srv.Start(ctx)

	// Count events before stale event.
	rdb := st.Client()
	countBefore, _ := rdb.Do(ctx, rdb.B().Xlen().Key(store.EventsStreamKey(st.Prefix())).Build()).ToInt64()

	// Publish stale completion event (job-stale-preguard, not job-current).
	st.PublishEvent(ctx, types.JobEvent{
		Type:      types.EventJobCompleted,
		JobID:     "job-stale-preguard",
		Timestamp: time.Now(),
	})

	time.Sleep(500 * time.Millisecond)

	// Count EventWorkflowTaskCompleted events in the stream.
	entries, _ := rdb.Do(ctx, rdb.B().Xrange().Key(store.EventsStreamKey(st.Prefix())).Start("-").End("+").Build()).AsXRange()
	taskCompletedCount := 0
	for _, e := range entries {
		if e.FieldValues["type"] == string(types.EventWorkflowTaskCompleted) {
			taskCompletedCount++
		}
	}

	// The stale event should have been rejected by the identity guard BEFORE
	// emitting EventWorkflowTaskCompleted. So no new task-completed events.
	if taskCompletedCount > 0 {
		t.Fatalf("stale event emitted EventWorkflowTaskCompleted before guard: count=%d", taskCompletedCount)
	}
	t.Logf("PASS: no EventWorkflowTaskCompleted emitted for stale event (events total: before=%d after=%d)", countBefore, len(entries))
}

// ============================================================
// Batch isBatchDone re-check on duplicate event
// ============================================================

func TestBatch_DuplicateEvent_StillFinalizes(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()

	st := cli.Store()

	// Create a batch with 2 items, both already completed.
	batchID := "batch-dup-finalize"
	batch := &types.BatchInstance{
		ID:         batchID,
		Name:       "dup-finalize",
		Status:     types.WorkflowStatusRunning,
		TotalItems: 2,
		CompletedItems: 2,
		FailedItems:    0,
		RunningItems:   0,
		PendingItems:   0,
		ItemStates: map[string]types.BatchItemState{
			"item-1": {ItemID: "item-1", JobID: "job-item-1", Status: types.JobStatusCompleted},
			"item-2": {ItemID: "item-2", JobID: "job-item-2", Status: types.JobStatusCompleted},
		},
		Definition: types.BatchDefinition{
			Name:         "dup-finalize",
			ItemTaskType: "bf.test",
			Items: []types.BatchItem{
				{ID: "item-1"},
				{ID: "item-2"},
			},
			FailurePolicy: types.BatchContinueOnError,
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	st.SaveBatch(ctx, batch)

	// Create a job for item-1 so GetJob works.
	batchIDPtr := &batchID
	itemIDPtr := func(s string) *string { return &s }
	rolePtr := func(s string) *string { return &s }
	job := &types.Job{
		ID:        "job-item-1",
		TaskType:  "bf.test",
		Status:    types.JobStatusCompleted,
		BatchID:   batchIDPtr,
		BatchItem: itemIDPtr("item-1"),
		BatchRole: rolePtr("item"),
	}
	st.SaveJob(ctx, job)

	srv := newServer(t, "dup-finalize-node", 10)
	defer srv.Stop()
	srv.Start(ctx)

	// Publish a duplicate completion event for item-1 (already completed).
	// The identity guard should skip mutation but re-check isBatchDone → finalize.
	st.PublishEvent(ctx, types.JobEvent{
		Type:      types.EventJobCompleted,
		JobID:     "job-item-1",
		Timestamp: time.Now(),
	})

	// Wait for orchestrator to process.
	var finalBatch *types.BatchInstance
	waitFor(t, 5*time.Second, "batch finalized", func() bool {
		b, err := cli.GetBatch(ctx, batchID)
		if err != nil {
			return false
		}
		finalBatch = b
		return b.Status.IsTerminal()
	})

	if finalBatch.Status != types.WorkflowStatusCompleted {
		t.Fatalf("expected batch completed, got %s", finalBatch.Status)
	}
	t.Log("PASS: duplicate event triggered isBatchDone re-check and finalized batch")
}
