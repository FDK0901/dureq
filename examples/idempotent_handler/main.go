// Package main demonstrates idempotent handler patterns for dureq.
//
// dureq provides duplicate-suppressed execution (per-run lock), but external
// side effects (API calls, DB writes, emails) can still execute more than once
// if a worker crashes after the side effect but before completion.
//
// This example shows three idempotency strategies:
//
//  1. UniqueKey — prevent duplicate enqueue at the source
//  2. RequestID — idempotent enqueue (same request returns cached job)
//  3. Handler-level idempotency — use a business key inside the handler
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/FDK0901/go-chainedlog/impl/chainedzerolog"

	"github.com/FDK0901/dureq/examples/shared"
	"github.com/FDK0901/dureq/pkg/dureq"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/rs/zerolog"
)

// --- Payloads ---

type PaymentPayload struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
}

type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// --- Simulated external services ---

// paymentGateway simulates a payment API that accepts an idempotency key.
// In production, this would be Stripe, PayPal, etc.
var paymentGateway = &idempotentAPI{processed: make(map[string]bool)}

type idempotentAPI struct {
	mu        sync.Mutex
	processed map[string]bool
}

func (api *idempotentAPI) Charge(idempotencyKey string, amount float64) error {
	api.mu.Lock()
	defer api.mu.Unlock()

	if api.processed[idempotencyKey] {
		fmt.Printf("  [payment-gateway] Duplicate charge ignored (key=%s)\n", idempotencyKey)
		return nil
	}
	api.processed[idempotencyKey] = true
	fmt.Printf("  [payment-gateway] Charged $%.2f (key=%s)\n", amount, idempotencyKey)
	return nil
}

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	zl := chainedzerolog.NewZerologBase()
	logger := chainedzerolog.NewZerolog(zl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Server ---

	srv, err := dureq.NewServer(
		append(shared.ServerOptions(),
			dureq.WithNodeID("idempotent-example-1"),
			dureq.WithMaxConcurrency(10),
			dureq.WithLogger(logger),
		)...,
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	// Strategy 3: Handler-level idempotency using a business key (OrderID).
	//
	// IMPORTANT: Do NOT use RunID as the idempotency key. During leader
	// failover, the same schedule firing can produce two different RunIDs,
	// so RunID-based dedup would NOT prevent duplicate charges.
	//
	// Instead, use a stable business identifier (OrderID) that is the same
	// across all retry attempts and failover re-dispatches.
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType:    "payment.charge",
		Concurrency: 5,
		Timeout:     30 * time.Second,
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  3,
			InitialDelay: 2 * time.Second,
			MaxDelay:     30 * time.Second,
			Multiplier:   2.0,
			Jitter:       0.1,
		},
		Handler: types.TypedHandler(func(ctx context.Context, p PaymentPayload) error {
			jobID := types.GetJobID(ctx)

			fmt.Printf("[payment.charge] job=%s order=%s amount=%.2f\n",
				jobID, p.OrderID, p.Amount)

			// Use OrderID (business key) as the idempotency key.
			// This survives: retries (same job), leader failover (new RunID
			// but same OrderID), and XAUTOCLAIM recovery.
			idempotencyKey := fmt.Sprintf("payment:charge:%s", p.OrderID)
			if err := paymentGateway.Charge(idempotencyKey, p.Amount); err != nil {
				return fmt.Errorf("charge failed: %w", err)
			}

			fmt.Printf("[payment.charge] completed order=%s\n", p.OrderID)
			return nil
		}),
	})

	// Simple email handler — idempotency is at the enqueue level (UniqueKey).
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType:    "email.send",
		Concurrency: 3,
		Timeout:     15 * time.Second,
		Handler: types.TypedHandler(func(ctx context.Context, p EmailPayload) error {
			jobID := types.GetJobID(ctx)
			fmt.Printf("[email.send] job=%s to=%s subject=%q\n", jobID, p.To, p.Subject)
			// In production: call SendGrid/SES/etc.
			return nil
		}),
	})

	if err := srv.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to start server")
		os.Exit(1)
	}

	// --- Client ---

	cli, err := dureq.NewClient(shared.ClientOptions()...)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create client")
		os.Exit(1)
	}
	defer cli.Close()

	// ─── Strategy 1: UniqueKey ───
	// Prevents duplicate jobs at the enqueue level.
	// If you enqueue the same UniqueKey twice, the second call is rejected.
	fmt.Println("\n=== Strategy 1: UniqueKey (enqueue-level dedup) ===")
	uniqueKey := "order:ORD-100:welcome-email"
	emailPayload, _ := json.Marshal(EmailPayload{
		To:      "user@example.com",
		Subject: "Welcome!",
		Body:    "Thanks for your order ORD-100.",
	})

	job1, err := cli.EnqueueScheduled(ctx, &dureq.EnqueueRequest{
		TaskType:  "email.send",
		Payload:   emailPayload,
		Schedule:  types.Schedule{Type: types.ScheduleImmediate},
		UniqueKey: &uniqueKey,
	})
	if err != nil {
		fmt.Printf("  First enqueue failed: %v\n", err)
	} else {
		fmt.Printf("  First enqueue: job=%s (created)\n", job1.ID)
	}

	// Second enqueue with the same UniqueKey — should be rejected.
	job2, err := cli.EnqueueScheduled(ctx, &dureq.EnqueueRequest{
		TaskType:  "email.send",
		Payload:   emailPayload,
		Schedule:  types.Schedule{Type: types.ScheduleImmediate},
		UniqueKey: &uniqueKey,
	})
	if err != nil {
		fmt.Printf("  Second enqueue rejected (expected): %v\n", err)
	} else {
		fmt.Printf("  Second enqueue: job=%s (duplicate — should not happen)\n", job2.ID)
	}

	// ─── Strategy 2: RequestID ───
	// Idempotent enqueue — same RequestID returns the cached job within 5min TTL.
	fmt.Println("\n=== Strategy 2: RequestID (idempotent enqueue) ===")
	reqID := "req-payment-ORD-200-v1"
	paymentPayload, _ := json.Marshal(PaymentPayload{
		OrderID: "ORD-200",
		Amount:  99.99,
	})

	job3, err := cli.EnqueueScheduled(ctx, &dureq.EnqueueRequest{
		TaskType:  "payment.charge",
		Payload:   paymentPayload,
		Schedule:  types.Schedule{Type: types.ScheduleImmediate},
		RequestID: &reqID,
	})
	if err != nil {
		fmt.Printf("  First enqueue failed: %v\n", err)
	} else {
		fmt.Printf("  First enqueue: job=%s\n", job3.ID)
	}

	// Same RequestID — returns cached response, no duplicate job created.
	job4, err := cli.EnqueueScheduled(ctx, &dureq.EnqueueRequest{
		TaskType:  "payment.charge",
		Payload:   paymentPayload,
		Schedule:  types.Schedule{Type: types.ScheduleImmediate},
		RequestID: &reqID,
	})
	if err != nil {
		fmt.Printf("  Second enqueue failed: %v\n", err)
	} else {
		fmt.Printf("  Second enqueue: job=%s (same ID = cached response)\n", job4.ID)
	}

	// ─── Strategy 3: Handler-level idempotency ───
	// The handler uses OrderID (business key) as the external API's
	// idempotency key. This works across retries AND leader failover
	// (where a new RunID is assigned to the same logical firing).
	fmt.Println("\n=== Strategy 3: Handler-level idempotency (business key as dedup key) ===")
	job5, err := cli.Enqueue(ctx, "payment.charge", PaymentPayload{
		OrderID: "ORD-300",
		Amount:  149.99,
	})
	if err != nil {
		fmt.Printf("  Enqueue failed: %v\n", err)
	} else {
		fmt.Printf("  Enqueued: job=%s (handler uses OrderID for external API dedup)\n", job5.ID)
	}

	fmt.Println("\nWaiting for handlers to execute... (Ctrl+C to exit)")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println()
	logger.Info().Msg("shutting down")
	srv.Stop()
}
