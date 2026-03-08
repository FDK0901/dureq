// Package main demonstrates dureq workflows — a DAG of tasks executed
// with dependency ordering, orchestrated by the leader node.
//
// Scenario: An order processing pipeline with this task graph:
//
//	validate_order ──┬── charge_payment
//	                 └── reserve_inventory
//	                         │
//	charge_payment ──────┐   │
//	                     ▼   ▼
//	                  ship_order
//	                     │
//	                     ▼
//	                send_confirmation
//
// The orchestrator dispatches root tasks (validate_order), then
// advances through the DAG as each task completes.
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
		dureq.WithRedisURL("redis://localhost:6379"),
		dureq.WithRedisDB(15),
		dureq.WithRedisPassword("your-password"),
		dureq.WithNodeID("workflow-demo-node-1"),
		dureq.WithMaxConcurrency(10),
		dureq.WithLogger(logger),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	// Register all task type handlers used in the workflow.
	handlers := []types.HandlerDefinition{
		{
			TaskType: "order.validate",
			Timeout:  10 * time.Second,
			Handler: func(_ context.Context, payload json.RawMessage) error {
				logger.Info().String("payload", string(payload)).Msg("validating order")
				time.Sleep(time.Duration(500+rand.Intn(500)) * time.Millisecond)
				return nil
			},
		},
		{
			TaskType: "order.charge_payment",
			Timeout:  15 * time.Second,
			Handler: func(_ context.Context, payload json.RawMessage) error {
				logger.Info().String("payload", string(payload)).Msg("charging payment")
				time.Sleep(time.Duration(800+rand.Intn(700)) * time.Millisecond)
				return nil
			},
		},
		{
			TaskType: "order.reserve_inventory",
			Timeout:  10 * time.Second,
			Handler: func(_ context.Context, payload json.RawMessage) error {
				logger.Info().String("payload", string(payload)).Msg("reserving inventory")
				time.Sleep(time.Duration(300+rand.Intn(400)) * time.Millisecond)
				return nil
			},
		},
		{
			TaskType: "order.ship",
			Timeout:  20 * time.Second,
			Handler: func(_ context.Context, payload json.RawMessage) error {
				logger.Info().String("payload", string(payload)).Msg("shipping order")
				time.Sleep(time.Duration(1000+rand.Intn(500)) * time.Millisecond)
				return nil
			},
		},
		{
			TaskType: "order.send_confirmation",
			Timeout:  10 * time.Second,
			Handler: func(_ context.Context, payload json.RawMessage) error {
				logger.Info().String("payload", string(payload)).Msg("sending confirmation email")
				time.Sleep(time.Duration(200+rand.Intn(300)) * time.Millisecond)
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
		dureq.WithClientRedisURL("redis://localhost:6379"),
		dureq.WithClientRedisPassword("your-password"),
		dureq.WithClientRedisDB(15),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create client")
		os.Exit(1)
	}
	defer cli.Close()

	// Define the order processing workflow DAG.
	orderPayload, _ := json.Marshal(map[string]string{"order_id": "ORD-2026-001", "customer": "alice"})

	wfDef := types.WorkflowDefinition{
		Name: "order-processing",
		Tasks: []types.WorkflowTask{
			{
				Name:     "validate_order",
				TaskType: "order.validate",
				Payload:  orderPayload,
			},
			{
				Name:      "charge_payment",
				TaskType:  "order.charge_payment",
				Payload:   orderPayload,
				DependsOn: []string{"validate_order"},
			},
			{
				Name:      "reserve_inventory",
				TaskType:  "order.reserve_inventory",
				Payload:   orderPayload,
				DependsOn: []string{"validate_order"},
			},
			{
				Name:      "ship_order",
				TaskType:  "order.ship",
				Payload:   orderPayload,
				DependsOn: []string{"charge_payment", "reserve_inventory"},
			},
			{
				Name:      "send_confirmation",
				TaskType:  "order.send_confirmation",
				Payload:   orderPayload,
				DependsOn: []string{"ship_order"},
			},
		},
	}

	// Enqueue the workflow.
	wf, err := cli.EnqueueWorkflow(ctx, wfDef, map[string]string{"source": "api", "priority": "high"})
	if err != nil {
		logger.Error().Err(err).Msg("failed to enqueue workflow")
		os.Exit(1)
	}

	logger.Info().String("workflow_id", wf.ID).String("name", wf.WorkflowName).String("status", string(wf.Status)).Int("tasks", len(wf.Tasks)).Msg("workflow enqueued")

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
