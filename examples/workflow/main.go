// Package main demonstrates dureq workflows — a DAG of tasks executed
// with dependency ordering, orchestrated by the actor cluster.
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
// The OrchestratorActor dispatches root tasks (validate_order), then
// advances through the DAG as each task completes.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	client "github.com/FDK0901/dureq/clients/go"
	"github.com/FDK0901/dureq/internal/server"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/bytedance/sonic"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Server Side ---

	srv, err := server.New(
		server.WithRedisURL("redis://localhost:6379"),
		server.WithRedisDB(15),
		server.WithRedisPassword("your-password"),
		server.WithNodeID("workflow-demo-node-1"),
		server.WithMaxConcurrency(10),
	)
	if err != nil {
		slog.Error("failed to create server", "err", err)
		os.Exit(1)
	}

	// Register all task type handlers used in the workflow.
	handlers := []types.HandlerDefinition{
		{
			TaskType: "order.validate",
			Timeout:  10 * time.Second,
			Handler: func(_ context.Context, payload json.RawMessage) error {
				slog.Info("validating order", "payload", string(payload))
				time.Sleep(time.Duration(500+rand.Intn(500)) * time.Millisecond)
				return nil
			},
		},
		{
			TaskType: "order.charge_payment",
			Timeout:  15 * time.Second,
			Handler: func(_ context.Context, payload json.RawMessage) error {
				slog.Info("charging payment", "payload", string(payload))
				time.Sleep(time.Duration(800+rand.Intn(700)) * time.Millisecond)
				return nil
			},
		},
		{
			TaskType: "order.reserve_inventory",
			Timeout:  10 * time.Second,
			Handler: func(_ context.Context, payload json.RawMessage) error {
				slog.Info("reserving inventory", "payload", string(payload))
				time.Sleep(time.Duration(300+rand.Intn(400)) * time.Millisecond)
				return nil
			},
		},
		{
			TaskType: "order.ship",
			Timeout:  20 * time.Second,
			Handler: func(_ context.Context, payload json.RawMessage) error {
				slog.Info("shipping order", "payload", string(payload))
				time.Sleep(time.Duration(1000+rand.Intn(500)) * time.Millisecond)
				return nil
			},
		},
		{
			TaskType: "order.send_confirmation",
			Timeout:  10 * time.Second,
			Handler: func(_ context.Context, payload json.RawMessage) error {
				slog.Info("sending confirmation email", "payload", string(payload))
				time.Sleep(time.Duration(200+rand.Intn(300)) * time.Millisecond)
				return nil
			},
		},
	}

	for _, h := range handlers {
		if err := srv.RegisterHandler(h); err != nil {
			slog.Error("failed to register handler", "task_type", string(h.TaskType), "err", err)
			os.Exit(1)
		}
	}

	if err := srv.Start(ctx); err != nil {
		slog.Error("failed to start server", "err", err)
		os.Exit(1)
	}

	// Wait for cluster singleton activation.
	time.Sleep(2 * time.Second)

	// --- Client Side ---

	cli, err := client.New(
		client.WithRedisURL("redis://localhost:6379"),
		client.WithRedisPassword("your-password"),
		client.WithRedisDB(15),
	)
	if err != nil {
		slog.Error("failed to create client", "err", err)
		os.Exit(1)
	}
	defer cli.Close()

	// Define the order processing workflow DAG.
	orderPayload, _ := sonic.ConfigFastest.Marshal(map[string]string{"order_id": "ORD-2026-001", "customer": "alice"})

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
		slog.Error("failed to enqueue workflow", "err", err)
		os.Exit(1)
	}

	slog.Info("workflow enqueued",
		"workflow_id", wf.ID,
		"name", wf.WorkflowName,
		"status", string(wf.Status),
		"tasks", len(wf.Tasks),
	)

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
					slog.Error("failed to get workflow status", "err", err)
					continue
				}

				completed := 0
				for _, t := range status.Tasks {
					if t.Status == types.JobStatusCompleted {
						completed++
					}
				}

				slog.Info("workflow progress",
					"workflow_id", wf.ID,
					"status", string(status.Status),
					"tasks_completed", completed,
					"total_tasks", len(status.Tasks),
				)

				if status.Status == types.WorkflowStatusCompleted {
					slog.Info("workflow completed successfully!", "workflow_id", wf.ID)
					return
				}
				if status.Status == types.WorkflowStatusFailed {
					slog.Error("workflow failed", "workflow_id", wf.ID)
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
	slog.Info("shutting down")
	srv.Stop()
}
