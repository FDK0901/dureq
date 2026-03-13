// Package postgres provides transactional completion adapters for dureq handlers
// whose business state lives in PostgreSQL. By committing a business write and the
// dureq completion marker in the same SQL transaction, handlers achieve exactly-once
// outcome semantics — even under retries and crashes.
//
// Usage:
//
//	func handleOrder(ctx context.Context, order OrderPayload) error {
//	    tx, _ := db.BeginTx(ctx, nil)
//	    defer tx.Rollback()
//
//	    // Business write.
//	    tx.ExecContext(ctx, "INSERT INTO orders (...) VALUES (...)", ...)
//
//	    // Mark this run as completed — same TX.
//	    if err := postgres.CompleteTx(ctx, tx); err != nil {
//	        return err // already completed on a previous attempt
//	    }
//	    return tx.Commit()
//	}
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/FDK0901/dureq/pkg/types"
)

// ErrAlreadyCompleted is returned when a run has already been marked as completed.
// The handler should treat this as a successful no-op (the previous attempt committed).
var ErrAlreadyCompleted = errors.New("dureq: run already completed")

// EnsureTable creates the dureq_completions table if it does not exist.
// Call this once during application startup or in a migration.
func EnsureTable(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS dureq_completions (
			run_id       TEXT NOT NULL,
			job_id       TEXT NOT NULL,
			step         TEXT NOT NULL DEFAULT '',
			data         JSONB,
			completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (run_id, step)
		)
	`)
	return err
}

// CompleteTx inserts a completion marker for the current run inside the given
// transaction. If the run was already completed (duplicate), ErrAlreadyCompleted
// is returned and the caller should skip further work (or commit as no-op).
//
// The run ID and job ID are extracted from the context (set by the dureq worker).
func CompleteTx(ctx context.Context, tx *sql.Tx) error {
	return completeTx(ctx, tx, "", nil)
}

// FailTx records a failure marker for the current run. This is useful when
// the handler wants to record that it attempted but failed, preventing
// re-execution of side effects on retry.
func FailTx(ctx context.Context, tx *sql.Tx, err error) error {
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	return completeTx(ctx, tx, "failed:"+errStr, nil)
}

// CheckpointTx records an intermediate checkpoint within a long-running handler.
// The step name distinguishes different checkpoints within the same run.
// On retry, the handler can call IsCheckpointed to skip already-completed steps.
func CheckpointTx(ctx context.Context, tx *sql.Tx, step string, data []byte) error {
	runID := types.GetRunID(ctx)
	jobID := types.GetJobID(ctx)
	if runID == "" {
		return fmt.Errorf("dureq: run_id not found in context")
	}

	_, err := tx.ExecContext(ctx, `
		INSERT INTO dureq_completions (run_id, job_id, step, data, completed_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (run_id, step) DO NOTHING
	`, runID, jobID, step, data, time.Now())
	return err
}

// IsCompleted checks whether the current run has already been completed.
// Use this at the start of a handler to skip re-execution entirely.
func IsCompleted(ctx context.Context, db *sql.DB) (bool, error) {
	runID := types.GetRunID(ctx)
	if runID == "" {
		return false, fmt.Errorf("dureq: run_id not found in context")
	}

	var exists bool
	err := db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM dureq_completions WHERE run_id = $1 AND step = '')`,
		runID,
	).Scan(&exists)
	return exists, err
}

// IsCheckpointed checks whether a specific checkpoint step has been recorded.
func IsCheckpointed(ctx context.Context, db *sql.DB, step string) (bool, []byte, error) {
	runID := types.GetRunID(ctx)
	if runID == "" {
		return false, nil, fmt.Errorf("dureq: run_id not found in context")
	}

	var data []byte
	err := db.QueryRowContext(ctx,
		`SELECT data FROM dureq_completions WHERE run_id = $1 AND step = $2`,
		runID, step,
	).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, err
	}
	return true, data, nil
}

func completeTx(ctx context.Context, tx *sql.Tx, step string, data []byte) error {
	runID := types.GetRunID(ctx)
	jobID := types.GetJobID(ctx)
	if runID == "" {
		return fmt.Errorf("dureq: run_id not found in context")
	}

	result, err := tx.ExecContext(ctx, `
		INSERT INTO dureq_completions (run_id, job_id, step, data, completed_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (run_id, step) DO NOTHING
	`, runID, jobID, step, data, time.Now())
	if err != nil {
		return fmt.Errorf("dureq: insert completion: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("dureq: rows affected: %w", err)
	}
	if rows == 0 {
		return ErrAlreadyCompleted
	}
	return nil
}
