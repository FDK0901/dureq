// Package main demonstrates dureq child workflows — a workflow task that
// spawns an entire sub-workflow instead of dispatching a single job.
//
// Scenario: An order pipeline with a child workflow for fulfillment:
//
//	validate ──── fulfillment (child workflow) ──── notify
//	                 │
//	                 ├── pick
//	                 ├── pack
//	                 └── ship
//
// The parent "fulfillment" task completes only when the child workflow
// (pick → pack → ship) finishes.
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

	"github.com/FDK0901/dureq/examples/shared"
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
		append(shared.ServerOptions(),
			dureq.WithNodeID("child-workflow-demo-node-1"),
			dureq.WithMaxConcurrency(10),
			dureq.WithLogger(logger),
		)...,
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	// Register handlers for all task types used in parent and child workflows.
	handlers := []types.HandlerDefinition{
		{
			TaskType: "pipeline.validate",
			Timeout:  10 * time.Second,
			Handler: func(_ context.Context, payload json.RawMessage) error {
				logger.Info().String("payload", string(payload)).Msg("validating order")
				time.Sleep(time.Duration(300+rand.Intn(300)) * time.Millisecond)
				return nil
			},
		},
		{
			TaskType: "fulfillment.pick",
			Timeout:  10 * time.Second,
			Handler: func(_ context.Context, payload json.RawMessage) error {
				logger.Info().String("payload", string(payload)).Msg("picking items from warehouse")
				time.Sleep(time.Duration(400+rand.Intn(400)) * time.Millisecond)
				return nil
			},
		},
		{
			TaskType: "fulfillment.pack",
			Timeout:  10 * time.Second,
			Handler: func(_ context.Context, payload json.RawMessage) error {
				logger.Info().String("payload", string(payload)).Msg("packing items")
				time.Sleep(time.Duration(300+rand.Intn(300)) * time.Millisecond)
				return nil
			},
		},
		{
			TaskType: "fulfillment.ship",
			Timeout:  15 * time.Second,
			Handler: func(_ context.Context, payload json.RawMessage) error {
				logger.Info().String("payload", string(payload)).Msg("shipping package")
				time.Sleep(time.Duration(500+rand.Intn(500)) * time.Millisecond)
				return nil
			},
		},
		{
			TaskType: "pipeline.notify",
			Timeout:  10 * time.Second,
			Handler: func(_ context.Context, payload json.RawMessage) error {
				logger.Info().String("payload", string(payload)).Msg("sending notification to customer")
				time.Sleep(time.Duration(200+rand.Intn(200)) * time.Millisecond)
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

	cli, err := dureq.NewClient(shared.ClientOptions()...)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create client")
		os.Exit(1)
	}
	defer cli.Close()

	// Define the parent workflow with a child workflow for fulfillment.
	orderPayload, _ := json.Marshal(map[string]string{
		"order_id": "ORD-2026-042",
		"customer": "bob",
	})

	wfDef := types.WorkflowDefinition{
		Name: "order-pipeline",
		Tasks: []types.WorkflowTask{
			{
				Name:     "validate",
				TaskType: "pipeline.validate",
				Payload:  orderPayload,
			},
			{
				Name:      "fulfillment",
				DependsOn: []string{"validate"},
				// Instead of a TaskType, this task spawns a child workflow.
				ChildWorkflowDef: &types.WorkflowDefinition{
					Name: "fulfillment-subprocess",
					Tasks: []types.WorkflowTask{
						{
							Name:     "pick",
							TaskType: "fulfillment.pick",
							Payload:  orderPayload,
						},
						{
							Name:      "pack",
							TaskType:  "fulfillment.pack",
							Payload:   orderPayload,
							DependsOn: []string{"pick"},
						},
						{
							Name:      "ship",
							TaskType:  "fulfillment.ship",
							Payload:   orderPayload,
							DependsOn: []string{"pack"},
						},
					},
				},
			},
			{
				Name:      "notify",
				TaskType:  "pipeline.notify",
				Payload:   orderPayload,
				DependsOn: []string{"fulfillment"},
			},
		},
	}

	// Enqueue the parent workflow.
	wf, err := cli.EnqueueWorkflow(ctx, wfDef, map[string]string{"source": "api", "pipeline": "order"})
	if err != nil {
		logger.Error().Err(err).Msg("failed to enqueue workflow")
		os.Exit(1)
	}

	logger.Info().String("workflow_id", wf.ID).String("name", wf.WorkflowName).String("status", string(wf.Status)).Int("tasks", len(wf.Tasks)).Msg("parent workflow enqueued")

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
					// Log child workflow IDs when present.
					if t.ChildWorkflowID != "" {
						logger.Info().String("task", t.Name).String("child_workflow_id", t.ChildWorkflowID).String("status", string(t.Status)).Msg("child workflow task")
					}
				}

				logger.Info().String("workflow_id", wf.ID).String("status", string(status.Status)).Int("tasks_completed", completed).Int("total_tasks", len(status.Tasks)).Msg("workflow progress")

				if status.Status == types.WorkflowStatusCompleted {
					logger.Info().String("workflow_id", wf.ID).Msg("parent workflow completed successfully!")
					return
				}
				if status.Status == types.WorkflowStatusFailed {
					logger.Error().String("workflow_id", wf.ID).Msg("parent workflow failed")
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
