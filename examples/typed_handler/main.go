// Package main demonstrates the generic type-safe handler wrapper and
// rich control flow errors (Skip, Repeat, Pause).
//
// Scenario: An order processing pipeline where:
//   - TypedHandler[T] eliminates manual JSON unmarshal boilerplate
//   - TypedHandlerWithResult[T,R] returns typed output
//   - SkipError skips orders below a minimum threshold
//   - RepeatError re-enqueues for incremental batch processing
//   - PauseError pauses the job when an external dependency is down
package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/FDK0901/go-chainedlog/impl/chainedzerolog"

	"github.com/FDK0901/dureq/pkg/dureq"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/rs/zerolog"
)

// --- Typed payloads ---

type OrderPayload struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
	Items   int     `json:"items"`
}

type OrderResult struct {
	OrderID   string `json:"order_id"`
	Status    string `json:"status"`
	Reference string `json:"reference"`
}

type BatchPayload struct {
	BatchID   string `json:"batch_id"`
	Processed int    `json:"processed"`
	Total     int    `json:"total"`
}

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	zl := chainedzerolog.NewZerologBase()
	logger := chainedzerolog.NewZerolog(zl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Server Setup ---

	srv, err := dureq.NewServer(
		dureq.WithRedisURL("redis://localhost:6381"),
		dureq.WithRedisDB(15),
		dureq.WithRedisPassword("your-password"),
		dureq.WithNodeID("typed-handler-node-1"),
		dureq.WithMaxConcurrency(10),
		dureq.WithLogger(logger),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	// 1. TypedHandlerWithResult: compile-time type safety, no json.Unmarshal needed.
	//    Returns a typed result that is automatically marshalled.
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType:    "order.process",
		Concurrency: 5,
		Timeout:     30 * time.Second,
		HandlerWithResult: types.TypedHandlerWithResult(func(ctx context.Context, order OrderPayload) (OrderResult, error) {
			jobID := types.GetJobID(ctx)
			logger.Info().String("job_id", jobID).String("order_id", order.OrderID).Float64("amount", order.Amount).Msg("processing order")

			// SkipError: skip orders below minimum threshold.
			if order.Amount < 1.0 {
				return OrderResult{}, &types.SkipError{Reason: fmt.Sprintf("order %s below minimum amount ($%.2f)", order.OrderID, order.Amount)}
			}

			// PauseError: simulate external payment gateway being down.
			if rand.IntN(10) == 0 {
				return OrderResult{}, &types.PauseError{
					Reason:     "payment gateway unavailable",
					RetryAfter: 30 * time.Second,
				}
			}

			return OrderResult{
				OrderID:   order.OrderID,
				Status:    "confirmed",
				Reference: fmt.Sprintf("REF-%s-%d", order.OrderID, time.Now().UnixMilli()),
			}, nil
		}),
	})

	// 2. TypedHandler: simple handler with no result, uses RepeatError for incremental processing.
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType:    "batch.process_chunk",
		Concurrency: 3,
		Timeout:     2 * time.Minute,
		Handler: types.TypedHandler(func(ctx context.Context, batch BatchPayload) error {
			logger.Info().String("batch_id", batch.BatchID).Int("processed", batch.Processed).Int("total", batch.Total).Msg("processing batch chunk")

			// Simulate processing a chunk of 100 items.
			chunkSize := 100
			newProcessed := batch.Processed + chunkSize
			if newProcessed > batch.Total {
				newProcessed = batch.Total
			}

			time.Sleep(1 * time.Second) // simulate work

			if newProcessed < batch.Total {
				// RepeatError: save progress and re-enqueue for next chunk.
				return &types.RepeatError{
					Delay: 500 * time.Millisecond,
				}
			}

			logger.Info().String("batch_id", batch.BatchID).Msg("batch complete")
			return nil
		}),
	})

	if err := srv.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to start server")
		os.Exit(1)
	}

	// --- Client Side ---

	cli, err := dureq.NewClient(
		dureq.WithClientRedisURL("redis://localhost:6381"),
		dureq.WithClientRedisPassword("your-password"),
		dureq.WithClientRedisDB(15),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create client")
		os.Exit(1)
	}
	defer cli.Close()

	// Enqueue typed order jobs.
	orders := []OrderPayload{
		{OrderID: "ORD-001", Amount: 49.99, Items: 3},
		{OrderID: "ORD-002", Amount: 0.50, Items: 1},  // will be skipped (below minimum)
		{OrderID: "ORD-003", Amount: 199.99, Items: 7},
	}
	for _, order := range orders {
		job, err := cli.Enqueue(ctx, "order.process", order)
		if err != nil {
			logger.Error().String("order_id", order.OrderID).Err(err).Msg("failed to enqueue order")
			continue
		}
		logger.Info().String("job_id", job.ID).String("order_id", order.OrderID).Msg("enqueued order")
	}

	// Enqueue a batch processing job using RepeatError.
	batchJob, err := cli.Enqueue(ctx, "batch.process_chunk", BatchPayload{
		BatchID:   "BATCH-001",
		Processed: 0,
		Total:     500,
	})
	if err != nil {
		logger.Error().Err(err).Msg("failed to enqueue batch")
	} else {
		logger.Info().String("job_id", batchJob.ID).Msg("enqueued batch job (will repeat until all chunks processed)")
	}

	// --- Wait for shutdown ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println()
	logger.Info().Msg("shutting down")
	srv.Stop()
}
