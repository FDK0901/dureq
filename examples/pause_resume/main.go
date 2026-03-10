// Package main demonstrates the Pause/Resume and Auto-Retry mechanism.
//
// Scenario: A webhook delivery service where:
//   - PauseAfterErrCount=3: auto-pauses after 3 consecutive failures
//   - PauseRetryDelay=30s: auto-resumes after 30 seconds
//   - Manual resume via client.ResumeJob() for operator intervention
//   - ConsecutiveErrors resets to 0 on success
//
// This prevents a failing endpoint from consuming retry budget endlessly,
// giving operators time to investigate while allowing automatic recovery.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/FDK0901/go-chainedlog/impl/chainedzerolog"

	"github.com/FDK0901/dureq/pkg/dureq"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/rs/zerolog"
)

type WebhookPayload struct {
	URL     string `json:"url"`
	EventID string `json:"event_id"`
	Body    string `json:"body"`
}

// Simulate an unreliable external endpoint.
var failCount atomic.Int32

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
		dureq.WithNodeID("pause-resume-node-1"),
		dureq.WithMaxConcurrency(10),
		dureq.WithLogger(logger),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	// Register webhook delivery handler with auto-pause policy.
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType:    "webhook.deliver",
		Concurrency: 5,
		Timeout:     10 * time.Second,
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  10,
			InitialDelay: 2 * time.Second,
			MaxDelay:     30 * time.Second,
			Multiplier:   2.0,
			Jitter:       0.1,
			// Auto-pause after 3 consecutive errors instead of going to dead.
			PauseAfterErrCount: 3,
			// Auto-resume after 30 seconds (scheduler picks it up).
			PauseRetryDelay: 30 * time.Second,
		},
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			var p WebhookPayload
			if err := json.Unmarshal(payload, &p); err != nil {
				return &types.NonRetryableError{Err: fmt.Errorf("invalid payload: %w", err)}
			}

			attempt := types.GetAttempt(ctx)
			logger.Info().String("url", p.URL).String("event_id", p.EventID).Int("attempt", attempt).Msg("delivering webhook")

			// Simulate: first 4 calls fail, then succeed.
			count := failCount.Add(1)
			if count <= 4 {
				return fmt.Errorf("connection refused: %s (attempt %d)", p.URL, count)
			}

			// After resume, delivery succeeds.
			logger.Info().String("url", p.URL).String("event_id", p.EventID).Msg("webhook delivered successfully")
			return nil
		},
	})

	// Register a handler that uses explicit PauseError from handler code.
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType:    "payment.charge",
		Concurrency: 3,
		Timeout:     15 * time.Second,
		RetryPolicy: types.DefaultRetryPolicy(),
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			logger.Info().Msg("attempting payment charge")

			// Simulate: payment gateway is down, handler decides to pause.
			if rand.IntN(2) == 0 {
				return &types.PauseError{
					Reason:     "payment gateway returned 503 Service Unavailable",
					RetryAfter: 1 * time.Minute, // auto-resume after 1 minute
				}
			}

			logger.Info().Msg("payment charged successfully")
			return nil
		},
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

	// Enqueue a webhook delivery that will fail and get auto-paused.
	payload, _ := json.Marshal(WebhookPayload{
		URL:     "https://example.com/webhook",
		EventID: "evt-001",
		Body:    `{"type":"order.created","order_id":"ORD-123"}`,
	})

	webhookJob, err := cli.Enqueue(ctx, "webhook.deliver", payload)
	if err != nil {
		logger.Error().Err(err).Msg("failed to enqueue webhook job")
		os.Exit(1)
	}
	logger.Info().String("job_id", webhookJob.ID).Msg("enqueued webhook job (will auto-pause after 3 failures, then auto-resume in 30s)")

	// Demonstrate manual resume: wait a few seconds and check status.
	go func() {
		time.Sleep(15 * time.Second)

		job, err := cli.GetJob(ctx, webhookJob.ID)
		if err != nil {
			logger.Error().Err(err).Msg("failed to get job status")
			return
		}

		logger.Info().
			String("job_id", job.ID).
			String("status", string(job.Status)).
			Int("consecutive_errors", job.ConsecutiveErrors).
			Msg("job status check")

		if job.Status == types.JobStatusPaused {
			logger.Info().String("job_id", job.ID).Msg("job is paused — issuing manual resume")
			if err := cli.ResumeJob(ctx, job.ID); err != nil {
				logger.Error().Err(err).Msg("failed to resume job")
			} else {
				logger.Info().String("job_id", job.ID).Msg("job resumed manually")
			}
		}
	}()

	// Enqueue a payment that may trigger PauseError.
	paymentJob, err := cli.Enqueue(ctx, "payment.charge", map[string]any{
		"customer_id": "cust-456",
		"amount":      99.99,
	})
	if err != nil {
		logger.Error().Err(err).Msg("failed to enqueue payment job")
	} else {
		logger.Info().String("job_id", paymentJob.ID).Msg("enqueued payment job (may pause on gateway failure)")
	}

	// --- Wait for shutdown ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println()
	logger.Info().Msg("shutting down")
	srv.Stop()
}
