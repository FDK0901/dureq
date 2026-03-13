// Package main demonstrates transactional completion — committing a business
// database write and the dureq completion marker in the same SQL transaction.
//
// This is the strongest exactly-once pattern when your business state and
// job result live in the same PostgreSQL database.
//
// Requires: PostgreSQL running with the dureq_completions table created.
// See pkg/integration/postgres.EnsureTable().
//
// Usage:
//
//	export POSTGRES_DSN="postgres://user:pass@localhost/mydb?sslmode=disable"
//	go run ./examples/pg_completion
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver

	"github.com/FDK0901/go-chainedlog/impl/chainedzerolog"
	"github.com/rs/zerolog"

	"github.com/FDK0901/dureq/examples/shared"
	"github.com/FDK0901/dureq/pkg/dureq"
	pgcomp "github.com/FDK0901/dureq/pkg/integration/postgres"
	"github.com/FDK0901/dureq/pkg/types"
)

type OrderPayload struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
}

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	zl := chainedzerolog.NewZerologBase()
	logger := chainedzerolog.NewZerolog(zl)

	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		fmt.Println("Set POSTGRES_DSN to run this example")
		fmt.Println("  export POSTGRES_DSN=\"postgres://user:pass@localhost/mydb?sslmode=disable\"")
		os.Exit(1)
	}

	// Connect to PostgreSQL.
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to connect to postgres")
	}
	defer db.Close()

	// Ensure the completions table exists.
	if err := pgcomp.EnsureTable(context.Background(), db); err != nil {
		logger.Fatal().Err(err).Msg("failed to create dureq_completions table")
	}

	// Ensure our business table exists.
	db.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS orders (
			order_id TEXT PRIMARY KEY,
			amount   NUMERIC NOT NULL,
			status   TEXT NOT NULL DEFAULT 'pending'
		)
	`)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Server ---

	srv, err := dureq.NewServer(
		append(shared.ServerOptions(),
			dureq.WithNodeID("pg-completion-example"),
			dureq.WithMaxConcurrency(10),
			dureq.WithLogger(logger),
		)...,
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create server")
	}

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType:    "order.complete",
		Concurrency: 5,
		Timeout:     30 * time.Second,
		RetryPolicy: &types.RetryPolicy{MaxAttempts: 3, InitialDelay: time.Second},
		Handler: types.TypedHandler(func(ctx context.Context, p OrderPayload) error {
			// Check if already completed (idempotency guard).
			if done, _ := pgcomp.IsCompleted(ctx, db); done {
				fmt.Printf("[order.complete] order=%s already completed, skipping\n", p.OrderID)
				return nil
			}

			// Begin transaction.
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return fmt.Errorf("begin tx: %w", err)
			}
			defer tx.Rollback()

			// Business write: update order status.
			_, err = tx.ExecContext(ctx,
				`INSERT INTO orders (order_id, amount, status) VALUES ($1, $2, 'completed')
				 ON CONFLICT (order_id) DO UPDATE SET status = 'completed'`,
				p.OrderID, p.Amount,
			)
			if err != nil {
				return fmt.Errorf("insert order: %w", err)
			}

			// Completion marker: same transaction.
			if err := pgcomp.CompleteTx(ctx, tx); err != nil {
				if err == pgcomp.ErrAlreadyCompleted {
					fmt.Printf("[order.complete] order=%s duplicate detected via completion table\n", p.OrderID)
					return nil // safe to skip
				}
				return fmt.Errorf("complete tx: %w", err)
			}

			// Commit both writes atomically.
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("commit: %w", err)
			}

			fmt.Printf("[order.complete] order=%s amount=%.2f — committed in same transaction\n",
				p.OrderID, p.Amount)
			return nil
		}),
	})

	if err := srv.Start(ctx); err != nil {
		logger.Fatal().Err(err).Msg("failed to start server")
	}

	// --- Client ---

	cli, err := dureq.NewClient(shared.ClientOptions()...)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create client")
	}
	defer cli.Close()

	// Enqueue an order.
	job, err := cli.Enqueue(ctx, "order.complete", OrderPayload{
		OrderID: "ORD-TX-001",
		Amount:  299.99,
	})
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to enqueue")
	}
	fmt.Printf("Enqueued: job=%s\n", job.ID)
	fmt.Println("Waiting for handler to execute... (Ctrl+C to exit)")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println()
	logger.Info().Msg("shutting down")
	srv.Stop()
}
