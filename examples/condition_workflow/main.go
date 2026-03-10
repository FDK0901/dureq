// Package main demonstrates condition nodes in dureq workflows — runtime
// branching based on handler return values.
//
// Scenario: Order amount verification with conditional routing:
//
//	validate → check_amount(condition) ─┬─ route 0 → manual_review ─┬─ finalize
//	                                    └─ route 1 → auto_approve  ─┘
//
// Orders above $100 go through manual review (route 0), while smaller
// orders are auto-approved (route 1). The condition handler returns
// a route index that the orchestrator uses to pick the next branch.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/FDK0901/go-chainedlog/impl/chainedzerolog"

	"github.com/FDK0901/dureq/pkg/dureq"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/rs/zerolog"
)

type OrderPayload struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
}

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
		dureq.WithNodeID("condition-workflow-node-1"),
		dureq.WithMaxConcurrency(10),
		dureq.WithLogger(logger),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	// Register handlers.
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "order.validate",
		Timeout:  10 * time.Second,
		Handler: func(_ context.Context, payload json.RawMessage) error {
			logger.Info().String("payload", string(payload)).Msg("validating order")
			time.Sleep(500 * time.Millisecond)
			return nil
		},
	})

	// Condition handler: returns route index based on order amount.
	// Route 0 = high value (manual review), Route 1 = low value (auto approve).
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "order.check_amount",
		Timeout:  10 * time.Second,
		HandlerWithResult: types.TypedConditionHandler(func(_ context.Context, order OrderPayload) (uint, error) {
			logger.Info().String("order_id", order.OrderID).Float64("amount", order.Amount).Msg("checking order amount")
			if order.Amount > 100.0 {
				logger.Info().Msg("high-value order → route 0 (manual review)")
				return 0, nil
			}
			logger.Info().Msg("low-value order → route 1 (auto approve)")
			return 1, nil
		}),
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "order.manual_review",
		Timeout:  15 * time.Second,
		Handler: func(_ context.Context, payload json.RawMessage) error {
			logger.Info().String("payload", string(payload)).Msg("manual review in progress")
			time.Sleep(1 * time.Second)
			return nil
		},
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "order.auto_approve",
		Timeout:  10 * time.Second,
		Handler: func(_ context.Context, payload json.RawMessage) error {
			logger.Info().String("payload", string(payload)).Msg("auto-approving order")
			time.Sleep(300 * time.Millisecond)
			return nil
		},
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "order.finalize",
		Timeout:  10 * time.Second,
		Handler: func(_ context.Context, payload json.RawMessage) error {
			logger.Info().String("payload", string(payload)).Msg("finalizing order")
			time.Sleep(300 * time.Millisecond)
			return nil
		},
	})

	if err := srv.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to start server")
		os.Exit(1)
	}

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

	// Enqueue a high-value order (will go through manual review).
	orderPayload, _ := json.Marshal(OrderPayload{OrderID: "ORD-HIGH-001", Amount: 250.00})

	wfDef := types.WorkflowDefinition{
		Name: "order-condition-routing",
		Tasks: []types.WorkflowTask{
			{
				Name:     "validate",
				TaskType: "order.validate",
				Payload:  orderPayload,
			},
			{
				Name:      "check_amount",
				TaskType:  "order.check_amount",
				Payload:   orderPayload,
				DependsOn: []string{"validate"},
				Type:      types.WorkflowTaskCondition,
				ConditionRoutes: map[uint]string{
					0: "manual_review",
					1: "auto_approve",
				},
			},
			{
				Name:      "manual_review",
				TaskType:  "order.manual_review",
				Payload:   orderPayload,
				DependsOn: []string{"check_amount"},
			},
			{
				Name:      "auto_approve",
				TaskType:  "order.auto_approve",
				Payload:   orderPayload,
				DependsOn: []string{"check_amount"},
			},
			{
				Name:      "finalize",
				TaskType:  "order.finalize",
				Payload:   orderPayload,
				DependsOn: []string{"manual_review", "auto_approve"},
			},
		},
	}

	wf, err := cli.EnqueueWorkflow(ctx, wfDef, nil)
	if err != nil {
		logger.Error().Err(err).Msg("failed to enqueue workflow")
		os.Exit(1)
	}

	logger.Info().String("workflow_id", wf.ID).Float64("amount", 250.00).Msg("enqueued high-value order workflow")

	// Poll workflow status.
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
					continue
				}
				completed := 0
				for _, t := range status.Tasks {
					if t.Status == types.JobStatusCompleted {
						completed++
					}
				}
				logger.Info().String("status", string(status.Status)).Int("completed", completed).Int("total", len(status.Tasks)).Msg("workflow progress")

				if status.Status == types.WorkflowStatusCompleted {
					// Show which route was taken.
					if route := status.Tasks["check_amount"].ConditionRoute; route != nil {
						logger.Info().Int("route", int(*route)).Msg("condition chose route")
					}
					logger.Info().Msg("workflow completed!")
					return
				}
				if status.Status == types.WorkflowStatusFailed {
					logger.Error().Msg("workflow failed")
					return
				}
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println()
	logger.Info().Msg("shutting down")
	srv.Stop()
}
