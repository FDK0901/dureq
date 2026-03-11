// Package main demonstrates lifecycle hooks and GDPR data purge.
//
// Scenario: An email sending service where:
//   - Lifecycle hooks notify external systems on job completion/failure/pause
//   - OnJobCompleted hook sends a Slack notification (simulated)
//   - OnJobDead hook creates an incident ticket (simulated)
//   - OnJobPaused hook alerts the on-call engineer (simulated)
//   - GDPR purge removes personal data after job completion
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/FDK0901/go-chainedlog/impl/chainedzerolog"

	"github.com/FDK0901/dureq/examples/shared"
	"github.com/FDK0901/dureq/pkg/dureq"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/rs/zerolog"
)

// --- Typed payloads (using TypedHandler generics) ---

type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

type EmailResult struct {
	MessageID  string `json:"message_id"`
	DeliveredAt string `json:"delivered_at"`
}

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	zl := chainedzerolog.NewZerologBase()
	logger := chainedzerolog.NewZerolog(zl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Server Setup ---

	srv, err := dureq.NewServer(
		append(shared.ServerOptions(),
			dureq.WithNodeID("hooks-node-1"),
			dureq.WithMaxConcurrency(10),
			dureq.WithLogger(logger),
		)...,
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	// --- Register Lifecycle Hooks (before Start) ---

	// OnJobCompleted: fire-and-forget notification.
	srv.OnJobCompleted(func(ctx context.Context, event types.JobEvent) {
		logger.Info().
			String("job_id", event.JobID).
			String("task_type", string(event.TaskType)).
			Msg("[HOOK] job completed — sending Slack notification")
		// In production: slackClient.PostMessage(channel, msg)
	})

	// OnJobFailed: log for monitoring.
	srv.OnJobFailed(func(ctx context.Context, event types.JobEvent) {
		errStr := ""
		if event.Error != nil {
			errStr = *event.Error
		}
		logger.Warn().
			String("job_id", event.JobID).
			Int("attempt", event.Attempt).
			String("error", errStr).
			Msg("[HOOK] job failed — incrementing failure metric")
		// In production: metricsClient.Increment("job.failure", tags)
	})

	// OnJobDead: create an incident when retries exhausted.
	srv.OnJobDead(func(ctx context.Context, event types.JobEvent) {
		logger.Error().
			String("job_id", event.JobID).
			String("task_type", string(event.TaskType)).
			Msg("[HOOK] job dead — creating PagerDuty incident")
		// In production: pagerduty.CreateIncident(...)
	})

	// OnJobPaused: alert the on-call engineer.
	srv.OnJobPaused(func(ctx context.Context, event types.JobEvent) {
		logger.Warn().
			String("job_id", event.JobID).
			String("task_type", string(event.TaskType)).
			Msg("[HOOK] job paused — alerting on-call engineer")
		// In production: oncall.Alert(...)
	})

	// OnJobResumed: clear the alert.
	srv.OnJobResumed(func(ctx context.Context, event types.JobEvent) {
		logger.Info().
			String("job_id", event.JobID).
			Msg("[HOOK] job resumed — clearing pause alert")
	})

	// --- Register Handlers (using TypedHandler generics) ---

	// Email handler: uses TypedHandlerWithResult for compile-time type safety.
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType:    "email.send",
		Concurrency: 5,
		Timeout:     30 * time.Second,
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:        3,
			InitialDelay:       2 * time.Second,
			MaxDelay:           10 * time.Second,
			Multiplier:         2.0,
			PauseAfterErrCount: 2, // pause after 2 consecutive errors
			PauseRetryDelay:    20 * time.Second,
		},
		HandlerWithResult: types.TypedHandlerWithResult(func(ctx context.Context, email EmailPayload) (EmailResult, error) {
			logger.Info().String("to", email.To).String("subject", email.Subject).Msg("sending email")

			// Simulate email sending.
			time.Sleep(500 * time.Millisecond)

			return EmailResult{
				MessageID:   fmt.Sprintf("msg-%d", time.Now().UnixNano()),
				DeliveredAt: time.Now().Format(time.RFC3339),
			}, nil
		}),
	})

	// Failing handler to demonstrate dead + hooks.
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType:    "email.send_broken",
		Concurrency: 2,
		Timeout:     5 * time.Second,
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  2,
			InitialDelay: 1 * time.Second,
			Multiplier:   1.0,
		},
		Handler: types.TypedHandler(func(ctx context.Context, email EmailPayload) error {
			return fmt.Errorf("SMTP connection refused: mail server is down")
		}),
	})

	if err := srv.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to start server")
		os.Exit(1)
	}

	// --- Client Side ---

	cli, err := dureq.NewClient(shared.ClientOptions()...)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create client")
		os.Exit(1)
	}
	defer cli.Close()

	// 1. Enqueue a successful email — triggers OnJobCompleted hook.
	successJob, err := cli.Enqueue(ctx, "email.send", EmailPayload{
		To:      "user@example.com",
		Subject: "Your order has shipped!",
		Body:    "Tracking number: ABC123",
	})
	if err != nil {
		logger.Error().Err(err).Msg("failed to enqueue email")
	} else {
		logger.Info().String("job_id", successJob.ID).Msg("enqueued email (will complete → OnJobCompleted hook fires)")
	}

	// 2. Enqueue a failing email — triggers OnJobFailed then OnJobDead hooks.
	failJob, err := cli.Enqueue(ctx, "email.send_broken", EmailPayload{
		To:      "admin@example.com",
		Subject: "Alert: System failure",
		Body:    "...",
	})
	if err != nil {
		logger.Error().Err(err).Msg("failed to enqueue failing email")
	} else {
		logger.Info().String("job_id", failJob.ID).Msg("enqueued failing email (will die → OnJobDead hook fires)")
	}

	// 3. GDPR data purge: wait for the successful job to complete, then purge personal data.
	go func() {
		time.Sleep(5 * time.Second)
		if successJob == nil {
			return
		}

		job, err := cli.GetJob(ctx, successJob.ID)
		if err != nil {
			logger.Error().Err(err).Msg("failed to get job for purge")
			return
		}

		if job.Status.IsTerminal() {
			logger.Info().String("job_id", job.ID).Msg("GDPR: purging personal data from completed job")
			if err := cli.Store().PurgeJobData(ctx, job.ID); err != nil {
				logger.Error().Err(err).Msg("GDPR: purge failed")
			} else {
				logger.Info().String("job_id", job.ID).Msg("GDPR: personal data purged (payload and headers cleared)")
			}

			// Verify: payload is nil after purge.
			purgedJob, _ := cli.GetJob(ctx, job.ID)
			if purgedJob != nil {
				logger.Info().
					String("job_id", purgedJob.ID).
					String("status", string(purgedJob.Status)).
					Bool("payload_nil", purgedJob.Payload == nil).
					Bool("headers_nil", purgedJob.Headers == nil).
					Msg("GDPR: verified job metadata preserved, data removed")
			}
		}
	}()

	// --- Wait for shutdown ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println()
	logger.Info().Msg("shutting down")
	srv.Stop()
}
