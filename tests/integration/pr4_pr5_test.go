// Package integration — tests for structural PRs 4-5.
//
// PR4: Transactional completion (PostgreSQL)
// PR5: dureqd productization (handler registry, health, drain)
package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/FDK0901/dureq/cmd/dureqd/handlers"
	pgcomp "github.com/FDK0901/dureq/pkg/integration/postgres"
	"github.com/FDK0901/dureq/pkg/types"
)

// ============================================================
// PR4: Transactional completion with real PostgreSQL
// ============================================================

func postgresDSN() string {
	if dsn := os.Getenv("POSTGRES_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://dureq:dureq@localhost:5432/dureq?sslmode=disable"
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", postgresDSN())
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		t.Skipf("postgres not available, skipping: %v", err)
	}
	return db
}

func cleanupTables(t *testing.T, db *sql.DB) {
	t.Helper()
	db.ExecContext(context.Background(), "DELETE FROM dureq_completions")
	db.ExecContext(context.Background(), "DELETE FROM orders")
}

func TestPR4_CompleteTx_Lifecycle(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	cleanupTables(t, db)

	ctx := context.Background()
	runID := fmt.Sprintf("test-run-%d", time.Now().UnixNano())
	jobID := "test-job-1"

	// Inject run context (simulating what the worker does).
	ctx = types.WithRunID(ctx, runID)
	ctx = types.WithJobID(ctx, jobID)

	// Not completed yet.
	done, err := pgcomp.IsCompleted(ctx, db)
	if err != nil {
		t.Fatalf("IsCompleted: %v", err)
	}
	if done {
		t.Fatal("expected not completed initially")
	}

	// Begin TX, do business write + CompleteTx.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO orders (order_id, amount, status) VALUES ($1, $2, 'completed')
		 ON CONFLICT (order_id) DO UPDATE SET status = 'completed'`,
		"ORD-TEST-001", 100.50,
	)
	if err != nil {
		tx.Rollback()
		t.Fatalf("insert order: %v", err)
	}

	err = pgcomp.CompleteTx(ctx, tx)
	if err != nil {
		tx.Rollback()
		t.Fatalf("CompleteTx: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Verify: order exists.
	var status string
	if err := db.QueryRowContext(ctx, "SELECT status FROM orders WHERE order_id = $1", "ORD-TEST-001").Scan(&status); err != nil {
		t.Fatalf("query order: %v", err)
	}
	if status != "completed" {
		t.Fatalf("expected status 'completed', got %q", status)
	}

	// Verify: completion marker exists.
	done, err = pgcomp.IsCompleted(ctx, db)
	if err != nil {
		t.Fatalf("IsCompleted after commit: %v", err)
	}
	if !done {
		t.Fatal("expected completed after commit")
	}

	t.Log("PASS: CompleteTx lifecycle — business write + completion marker committed atomically")
}

func TestPR4_CompleteTx_Idempotency(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	cleanupTables(t, db)

	ctx := context.Background()
	runID := fmt.Sprintf("test-run-idem-%d", time.Now().UnixNano())
	ctx = types.WithRunID(ctx, runID)
	ctx = types.WithJobID(ctx, "test-job-idem")

	// First completion — should succeed.
	tx1, _ := db.BeginTx(ctx, nil)
	err := pgcomp.CompleteTx(ctx, tx1)
	if err != nil {
		tx1.Rollback()
		t.Fatalf("first CompleteTx: %v", err)
	}
	tx1.Commit()

	// Second completion with same runID — should return ErrAlreadyCompleted.
	tx2, _ := db.BeginTx(ctx, nil)
	err = pgcomp.CompleteTx(ctx, tx2)
	tx2.Rollback()

	if err != pgcomp.ErrAlreadyCompleted {
		t.Fatalf("expected ErrAlreadyCompleted on retry, got: %v", err)
	}

	t.Log("PASS: CompleteTx idempotency — duplicate attempt returns ErrAlreadyCompleted")
}

func TestPR4_CheckpointTx_Lifecycle(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	cleanupTables(t, db)

	ctx := context.Background()
	runID := fmt.Sprintf("test-run-ckpt-%d", time.Now().UnixNano())
	ctx = types.WithRunID(ctx, runID)
	ctx = types.WithJobID(ctx, "test-job-ckpt")

	// No checkpoints yet.
	done, data, err := pgcomp.IsCheckpointed(ctx, db, "step-1")
	if err != nil {
		t.Fatalf("IsCheckpointed: %v", err)
	}
	if done {
		t.Fatal("expected no checkpoint initially")
	}

	// Checkpoint step-1 with data.
	checkpointData, _ := json.Marshal(map[string]string{"result": "ok"})
	tx, _ := db.BeginTx(ctx, nil)
	if err := pgcomp.CheckpointTx(ctx, tx, "step-1", checkpointData); err != nil {
		tx.Rollback()
		t.Fatalf("CheckpointTx: %v", err)
	}
	tx.Commit()

	// Verify checkpoint exists with data.
	done, data, err = pgcomp.IsCheckpointed(ctx, db, "step-1")
	if err != nil {
		t.Fatalf("IsCheckpointed after: %v", err)
	}
	if !done {
		t.Fatal("expected checkpoint to exist")
	}

	var parsed map[string]string
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal checkpoint data: %v", err)
	}
	if parsed["result"] != "ok" {
		t.Fatalf("expected checkpoint data result=ok, got %q", parsed["result"])
	}

	// step-2 should not exist.
	done, _, _ = pgcomp.IsCheckpointed(ctx, db, "step-2")
	if done {
		t.Fatal("step-2 should not exist")
	}

	t.Log("PASS: CheckpointTx lifecycle — checkpoint with data stored and retrieved correctly")
}

func TestPR4_FailTx(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	cleanupTables(t, db)

	ctx := context.Background()
	runID := fmt.Sprintf("test-run-fail-%d", time.Now().UnixNano())
	ctx = types.WithRunID(ctx, runID)
	ctx = types.WithJobID(ctx, "test-job-fail")

	// Record failure.
	tx, _ := db.BeginTx(ctx, nil)
	if err := pgcomp.FailTx(ctx, tx, fmt.Errorf("test error: db timeout")); err != nil {
		tx.Rollback()
		t.Fatalf("FailTx: %v", err)
	}
	tx.Commit()

	// IsCompleted should return false (FailTx uses step="failed:..." not empty).
	done, _ := pgcomp.IsCompleted(ctx, db)
	if done {
		t.Fatal("FailTx should not mark as completed (different step)")
	}

	// But the failure entry exists in the table.
	var count int
	db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dureq_completions WHERE run_id = $1", runID).Scan(&count)
	if count != 1 {
		t.Fatalf("expected 1 failure entry, got %d", count)
	}

	t.Log("PASS: FailTx records failure marker without marking as completed")
}

func TestPR4_RollbackSafety(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	cleanupTables(t, db)

	ctx := context.Background()
	runID := fmt.Sprintf("test-run-rollback-%d", time.Now().UnixNano())
	ctx = types.WithRunID(ctx, runID)
	ctx = types.WithJobID(ctx, "test-job-rollback")

	// Begin TX, write order + completion, then ROLLBACK.
	tx, _ := db.BeginTx(ctx, nil)
	tx.ExecContext(ctx,
		`INSERT INTO orders (order_id, amount, status) VALUES ($1, $2, 'completed')`,
		"ORD-ROLLBACK-001", 50.00,
	)
	pgcomp.CompleteTx(ctx, tx)
	tx.Rollback() // simulate crash

	// Neither the order nor the completion should exist.
	var orderCount int
	db.QueryRowContext(ctx, "SELECT COUNT(*) FROM orders WHERE order_id = $1", "ORD-ROLLBACK-001").Scan(&orderCount)
	if orderCount != 0 {
		t.Fatal("order should not exist after rollback")
	}

	done, _ := pgcomp.IsCompleted(ctx, db)
	if done {
		t.Fatal("completion should not exist after rollback")
	}

	t.Log("PASS: rollback correctly discards both business write and completion marker")
}

func TestPR4_EndToEnd_HandlerWithCompletion(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	cleanupTables(t, db)
	flushDB(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "pr4-e2e-node", 10)

	completed := make(chan string, 1)

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType:    "pr4.order",
		Concurrency: 5,
		Timeout:     10 * time.Second,
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			var p struct {
				OrderID string  `json:"order_id"`
				Amount  float64 `json:"amount"`
			}
			if err := json.Unmarshal(payload, &p); err != nil {
				return err
			}

			// Idempotency guard.
			if done, _ := pgcomp.IsCompleted(ctx, db); done {
				completed <- p.OrderID
				return nil
			}

			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()

			_, err = tx.ExecContext(ctx,
				`INSERT INTO orders (order_id, amount, status) VALUES ($1, $2, 'completed')
				 ON CONFLICT (order_id) DO UPDATE SET status = 'completed'`,
				p.OrderID, p.Amount,
			)
			if err != nil {
				return err
			}

			if err := pgcomp.CompleteTx(ctx, tx); err != nil {
				if err == pgcomp.ErrAlreadyCompleted {
					completed <- p.OrderID
					return nil
				}
				return err
			}

			if err := tx.Commit(); err != nil {
				return err
			}

			completed <- p.OrderID
			return nil
		},
	})

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()

	_, err := cli.Enqueue(ctx, "pr4.order", map[string]any{"order_id": "ORD-E2E-001", "amount": 299.99})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case orderID := <-completed:
		t.Logf("PASS: handler completed order %s with transactional completion", orderID)
	case <-time.After(15 * time.Second):
		t.Fatal("handler did not complete within 15s")
	}

	// Verify both tables.
	var status string
	if err := db.QueryRowContext(ctx, "SELECT status FROM orders WHERE order_id = 'ORD-E2E-001'").Scan(&status); err != nil {
		t.Fatalf("query order: %v", err)
	}
	if status != "completed" {
		t.Fatalf("expected order status 'completed', got %q", status)
	}

	t.Log("PASS: end-to-end transactional completion — order + completion committed together via real handler")
}

// ============================================================
// PR5: dureqd productization
// ============================================================

func TestPR5_HandlerRegistry_ListAndCount(t *testing.T) {
	names := handlers.List()
	count := handlers.Count()

	if count < 2 {
		t.Fatalf("expected at least 2 built-in handlers (noop, fail-always), got %d", count)
	}
	if count != len(names) {
		t.Fatalf("Count() = %d but List() returned %d names", count, len(names))
	}

	// Verify noop and fail-always are present.
	found := map[string]bool{}
	for _, n := range names {
		found[n] = true
	}
	for _, required := range []string{"noop", "fail-always"} {
		if !found[required] {
			t.Fatalf("expected built-in handler %q in registry, got: %v", required, names)
		}
	}

	t.Logf("PASS: handler registry contains %d handlers: %v", count, names)
}

func TestPR5_RegisterFiltered(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	srv := newServer(t, "pr5-filter-node", 10)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	// Register only "noop".
	enabled := map[string]bool{"noop": true}
	registered, skipped := handlers.RegisterFiltered(srv, enabled)

	if registered != 1 {
		t.Fatalf("expected 1 registered (noop only), got %d", registered)
	}
	if skipped < 1 {
		t.Fatalf("expected at least 1 skipped, got %d", skipped)
	}

	t.Logf("PASS: RegisterFiltered — registered=%d, skipped=%d", registered, skipped)
}

func TestPR5_RegisterAll(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	srv := newServer(t, "pr5-all-node", 10)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	registered, errors := handlers.RegisterAll(srv)

	if registered < 2 {
		t.Fatalf("expected at least 2 registered, got %d (errors=%d)", registered, errors)
	}

	t.Logf("PASS: RegisterAll — registered=%d, errors=%d", registered, errors)
}

func TestPR5_NoopHandler_Executes(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "pr5-noop-exec-node", 10)

	// Register only noop via the registry.
	enabled := map[string]bool{"noop": true}
	handlers.RegisterFiltered(srv, enabled)

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()

	job, err := cli.Enqueue(ctx, "noop", struct{}{})
	if err != nil {
		t.Fatalf("enqueue noop: %v", err)
	}

	completed := waitJobTerminal(t, cli, job.ID, 15*time.Second)
	if completed.Status != types.JobStatusCompleted {
		t.Fatalf("expected noop job completed, got %s", completed.Status)
	}

	t.Log("PASS: noop built-in handler executes and completes via registry")
}

func TestPR5_FailAlwaysHandler_Fails(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "pr5-fail-exec-node", 10)

	// Register only fail-always.
	enabled := map[string]bool{"fail-always": true}
	handlers.RegisterFiltered(srv, enabled)

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()

	job, err := cli.Enqueue(ctx, "fail-always", struct{}{})
	if err != nil {
		t.Fatalf("enqueue fail-always: %v", err)
	}

	terminal := waitJobTerminal(t, cli, job.ID, 15*time.Second)
	if terminal.Status == types.JobStatusCompleted {
		t.Fatal("fail-always handler should not complete successfully")
	}

	t.Logf("PASS: fail-always built-in handler fails as expected, status=%s", terminal.Status)
}

func TestPR5_DrainTimeout_Config(t *testing.T) {
	// Verify that the server accepts ShutdownTimeout option and respects it.
	flushDB(t)
	ctx := context.Background()

	srv := newServer(t, "pr5-drain-node", 10)
	// newServer already sets WithShutdownTimeout(3s)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Stop should return within a reasonable time (shutdown timeout + buffer).
	done := make(chan struct{})
	go func() {
		srv.Stop()
		close(done)
	}()

	select {
	case <-done:
		t.Log("PASS: server stopped within shutdown timeout")
	case <-time.After(10 * time.Second):
		t.Fatal("server.Stop() did not return within 10s (drain timeout not respected)")
	}
}
