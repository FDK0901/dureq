// Package main demonstrates dureq workflows using the mux handler pattern —
// pattern-based routing, global middleware, per-handler middleware, and
// context utilities for metadata access.
//
// Compared to the plain workflow example, this version:
//   - Registers a single "order.*" pattern handler that dispatches by task type
//   - Adds global logging middleware via srv.Use()
//   - Adds per-handler timing middleware
//   - Uses context utilities (GetJobID, GetTaskType, GetAttempt, etc.)
//
// Scenario: Same order processing DAG as the plain workflow example.
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

	client "github.com/FDK0901/dureq/clients/go"
	"github.com/FDK0901/dureq/internal/server"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedzerolog"
	"github.com/bytedance/sonic"
	"github.com/rs/zerolog"
)

// loggingMiddleware is a global middleware that logs every handler invocation
// with job metadata extracted from the context.
func loggingMiddleware(logger gochainedlog.Logger) types.MiddlewareFunc {
	return func(next types.HandlerFunc) types.HandlerFunc {
		return func(ctx context.Context, payload json.RawMessage) error {
			jobID := types.GetJobID(ctx)
			taskType := types.GetTaskType(ctx)
			attempt := types.GetAttempt(ctx)
			nodeID := types.GetNodeID(ctx)

			logger.Info().String("job_id", jobID).String("task_type", string(taskType)).Int("attempt", attempt).String("node_id", nodeID).Msg("[middleware:logging] handler started")

			err := next(ctx, payload)

			if err != nil {
				logger.Error().String("job_id", jobID).String("task_type", string(taskType)).Err(err).Msg("[middleware:logging] handler failed")
			} else {
				logger.Info().String("job_id", jobID).String("task_type", string(taskType)).Msg("[middleware:logging] handler succeeded")
			}
			return err
		}
	}
}

// timingMiddleware is a per-handler middleware that measures execution duration.
func timingMiddleware(logger gochainedlog.Logger) types.MiddlewareFunc {
	return func(next types.HandlerFunc) types.HandlerFunc {
		return func(ctx context.Context, payload json.RawMessage) error {
			start := time.Now()
			err := next(ctx, payload)
			elapsed := time.Since(start)

			taskType := types.GetTaskType(ctx)
			logger.Info().String("task_type", string(taskType)).Duration("elapsed_ms", elapsed).Msg("[middleware:timing] execution time")
			return err
		}
	}
}

// orderHandler is a single handler registered for the "order.*" pattern.
// It dispatches internally based on the actual task type from the context.
func orderHandler(logger gochainedlog.Logger) types.HandlerFunc {
	return func(ctx context.Context, payload json.RawMessage) error {
		taskType := types.GetTaskType(ctx)
		jobID := types.GetJobID(ctx)
		maxRetry := types.GetMaxRetry(ctx)
		headers := types.GetHeaders(ctx)

		logger.Info().String("task_type", string(taskType)).String("job_id", jobID).Int("max_retry", int(maxRetry)).Any("headers", headers).String("payload", string(payload)).Msg("order handler dispatching")

		switch taskType {
		case "order.validate":
			logger.Info().Msg("validating order")
			time.Sleep(time.Duration(500+rand.Intn(500)) * time.Millisecond)
			return nil

		case "order.charge_payment":
			logger.Info().Msg("charging payment")
			time.Sleep(time.Duration(800+rand.Intn(700)) * time.Millisecond)
			return nil

		case "order.reserve_inventory":
			logger.Info().Msg("reserving inventory")
			time.Sleep(time.Duration(300+rand.Intn(400)) * time.Millisecond)
			return nil

		case "order.ship":
			logger.Info().Msg("shipping order")
			time.Sleep(time.Duration(1000+rand.Intn(500)) * time.Millisecond)
			return nil

		case "order.send_confirmation":
			logger.Info().Msg("sending confirmation email")
			time.Sleep(time.Duration(200+rand.Intn(300)) * time.Millisecond)
			return nil

		default:
			return fmt.Errorf("unknown order task type: %s", taskType)
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	zl := chainedzerolog.NewZerologBase()
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	logger := chainedzerolog.NewZerolog(zl)

	// --- Server Side ---

	srv, err := server.New(
		server.WithRedisURL("redis://localhost:6379"),
		server.WithRedisDB(15),
		server.WithRedisPassword("your-password"),
		server.WithNodeID("workflow-mux-node-1"),
		server.WithMaxConcurrency(10),
		server.WithLogger(logger),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	// Register global middleware — applies to ALL handlers.
	srv.Use(loggingMiddleware(logger))

	// Register a single pattern handler for all "order.*" task types.
	err = srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "order.*", // pattern matching — longest prefix wins
		Timeout:  20 * time.Second,
		Handler:  orderHandler(logger),
		// Per-handler middleware — applied after global middleware.
		Middlewares: []types.MiddlewareFunc{
			timingMiddleware(logger),
		},
	})
	if err != nil {
		logger.Error().Err(err).Msg("failed to register order handler")
		os.Exit(1)
	}

	if err := srv.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to start server")
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
		logger.Error().Err(err).Msg("failed to create client")
		os.Exit(1)
	}
	defer cli.Close()

	// Define the order processing workflow DAG.
	orderPayload, _ := sonic.ConfigFastest.Marshal(map[string]string{"order_id": "ORD-2026-001", "customer": "alice"})

	wfDef := types.WorkflowDefinition{
		Name: "order-processing-mux",
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
