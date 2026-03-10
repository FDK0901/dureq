// Package main demonstrates dureq workflow signals — sending asynchronous
// external signals to a running workflow instance.
//
// Scenario: An approval-based order processing flow:
//
//	wait_for_approval ──── process_order
//
// After enqueuing the workflow, an "approve" signal is sent externally.
// The orchestrator picks up the signal and advances the workflow.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/FDK0901/go-chainedlog/impl/chainedzerolog"

	"github.com/FDK0901/dureq/pkg/dureq"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/rs/zerolog"
)

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	zl := chainedzerolog.NewZerologBase()
	logger := chainedzerolog.NewZerolog(zl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Server Side ---

	srv, err := dureq.NewServer(
		dureq.WithRedisURL("redis://localhost:6381"),
		dureq.WithRedisDB(15),
		dureq.WithRedisPassword("your-password"),
		dureq.WithNodeID("signals-demo-node-1"),
		dureq.WithMaxConcurrency(10),
		dureq.WithLogger(logger),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	// Register task handlers.
	handlers := []types.HandlerDefinition{
		{
			TaskType: "approval.wait",
			Timeout:  60 * time.Second,
			Handler: func(_ context.Context, payload json.RawMessage) error {
				logger.Info().String("payload", string(payload)).Msg("waiting for approval signal")
				// In a real scenario this task would block or poll for a signal.
				// Here it completes quickly; the signal is processed by the orchestrator.
				time.Sleep(time.Duration(200+rand.Intn(300)) * time.Millisecond)
				return nil
			},
		},
		{
			TaskType: "order.process",
			Timeout:  15 * time.Second,
			Handler: func(_ context.Context, payload json.RawMessage) error {
				logger.Info().String("payload", string(payload)).Msg("processing approved order")
				time.Sleep(time.Duration(500+rand.Intn(500)) * time.Millisecond)
				return nil
			},
		},
	}

	for _, h := range handlers {
		if err := srv.RegisterHandler(h); err != nil {
			logger.Error().String("task_type", string(h.TaskType)).Err(err).Msg("failed to register handler")
			os.Exit(1)
		}
	}

	if err := srv.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to start server")
		os.Exit(1)
	}

	// Give the server a moment to elect a leader.
	time.Sleep(3 * time.Second)

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

	// Define workflow with two tasks.
	orderPayload, _ := json.Marshal(map[string]string{
		"order_id": "ORD-2026-099",
		"customer": "carol",
		"amount":   "149.99",
	})

	wfDef := types.WorkflowDefinition{
		Name: "approval-flow",
		Tasks: []types.WorkflowTask{
			{
				Name:     "wait_for_approval",
				TaskType: "approval.wait",
				Payload:  orderPayload,
			},
			{
				Name:      "process_order",
				TaskType:  "order.process",
				Payload:   orderPayload,
				DependsOn: []string{"wait_for_approval"},
			},
		},
	}

	// Enqueue the workflow.
	wf, err := cli.EnqueueWorkflow(ctx, wfDef, map[string]string{"source": "web", "requires_approval": "true"})
	if err != nil {
		logger.Error().Err(err).Msg("failed to enqueue workflow")
		os.Exit(1)
	}

	logger.Info().String("workflow_id", wf.ID).String("name", wf.WorkflowName).String("status", string(wf.Status)).Int("tasks", len(wf.Tasks)).Msg("workflow enqueued")

	// Simulate an external approval arriving after a short delay.
	go func() {
		time.Sleep(2 * time.Second)
		logger.Info().String("workflow_id", wf.ID).Msg("sending 'approve' signal to workflow")

		err := cli.SignalWorkflow(ctx, wf.ID, "approve", map[string]string{
			"approved_by": "admin",
		})
		if err != nil {
			logger.Error().Err(err).Msg("failed to send signal")
			return
		}
		logger.Info().String("workflow_id", wf.ID).Msg("signal sent successfully")
	}()

	// Poll workflow status until terminal.
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				status, err := cli.GetWorkflow(ctx, wf.ID)
				if err != nil {
					logger.Error().Err(err).Msg("failed to get workflow status")
					continue
				}

				completed := 0
				for _, t := range status.Tasks {
					if t.Status == types.JobStatusCompleted {
						completed++
					}
				}

				logger.Info().String("workflow_id", wf.ID).String("status", string(status.Status)).Int("tasks_completed", completed).Int("total_tasks", len(status.Tasks)).Msg("workflow progress")

				if status.Status == types.WorkflowStatusCompleted {
					logger.Info().String("workflow_id", wf.ID).Msg("workflow completed successfully!")
					return
				}
				if status.Status == types.WorkflowStatusFailed {
					logger.Error().String("workflow_id", wf.ID).Msg("workflow failed")
					return
				}
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
