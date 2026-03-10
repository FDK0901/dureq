// Package main demonstrates ResultFrom (result piping) in dureq workflows —
// automatically passing one task's output as the next task's input.
//
// Scenario: A multi-step order pipeline where each step's output feeds
// the next step via ResultFrom.
//
//	validate_order → calculate_total → apply_discount → finalize
//
// Each task receives the upstream result + its own original payload
// via TypedHandlerWithUpstream[U, T].
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

	"github.com/FDK0901/dureq/examples/shared"
	"github.com/FDK0901/dureq/pkg/dureq"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/rs/zerolog"
)

type OrderPayload struct {
	OrderID string  `json:"order_id"`
	Items   []Item  `json:"items"`
	Coupon  string  `json:"coupon,omitempty"`
}

type Item struct {
	Name  string  `json:"name"`
	Price float64 `json:"price"`
	Qty   int     `json:"qty"`
}

type ValidationResult struct {
	Valid       bool    `json:"valid"`
	TotalItems  int     `json:"total_items"`
}

type TotalResult struct {
	Subtotal float64 `json:"subtotal"`
}

type DiscountResult struct {
	FinalTotal float64 `json:"final_total"`
	Discount   float64 `json:"discount"`
}

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	zl := chainedzerolog.NewZerologBase()
	logger := chainedzerolog.NewZerolog(zl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Server Side ---

	srv, err := dureq.NewServer(
		append(shared.ServerOptions(),
			dureq.WithNodeID("result-piping-node-1"),
			dureq.WithMaxConcurrency(10),
			dureq.WithLogger(logger),
		)...,
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	// Step 1: Validate order and return validation result.
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "rp.validate",
		Timeout:  10 * time.Second,
		HandlerWithResult: types.TypedHandlerWithResult(func(_ context.Context, order OrderPayload) (ValidationResult, error) {
			totalItems := 0
			for _, item := range order.Items {
				totalItems += item.Qty
			}
			logger.Info().String("order_id", order.OrderID).Int("total_items", totalItems).Msg("order validated")
			return ValidationResult{Valid: true, TotalItems: totalItems}, nil
		}),
	})

	// Step 2: Calculate total using upstream validation result + original order payload.
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "rp.calculate_total",
		Timeout:  10 * time.Second,
		HandlerWithResult: types.TypedHandlerWithUpstreamResult(
			func(_ context.Context, upstream ValidationResult, order OrderPayload) (TotalResult, error) {
				if !upstream.Valid {
					return TotalResult{}, fmt.Errorf("order invalid")
				}
				subtotal := 0.0
				for _, item := range order.Items {
					subtotal += item.Price * float64(item.Qty)
				}
				logger.Info().String("order_id", order.OrderID).Float64("subtotal", subtotal).Int("validated_items", upstream.TotalItems).Msg("total calculated")
				return TotalResult{Subtotal: subtotal}, nil
			},
		),
	})

	// Step 3: Apply discount using upstream total result.
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "rp.apply_discount",
		Timeout:  10 * time.Second,
		HandlerWithResult: types.TypedHandlerWithUpstreamResult(
			func(_ context.Context, upstream TotalResult, order OrderPayload) (DiscountResult, error) {
				discount := 0.0
				if order.Coupon == "SAVE10" {
					discount = upstream.Subtotal * 0.10
				}
				finalTotal := upstream.Subtotal - discount
				logger.Info().Float64("subtotal", upstream.Subtotal).Float64("discount", discount).Float64("final", finalTotal).Msg("discount applied")
				return DiscountResult{FinalTotal: finalTotal, Discount: discount}, nil
			},
		),
	})

	// Step 4: Finalize using upstream discount result.
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "rp.finalize",
		Timeout:  10 * time.Second,
		Handler: types.TypedHandlerWithUpstream(func(_ context.Context, upstream DiscountResult, order OrderPayload) error {
			logger.Info().String("order_id", order.OrderID).Float64("final_total", upstream.FinalTotal).Float64("discount", upstream.Discount).Msg("order finalized!")
			return nil
		}),
	})

	if err := srv.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to start server")
		os.Exit(1)
	}

	time.Sleep(3 * time.Second)

	// --- Client Side ---

	cli, err := dureq.NewClient(shared.ClientOptions()...)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create client")
		os.Exit(1)
	}
	defer cli.Close()

	order := OrderPayload{
		OrderID: "ORD-001",
		Items: []Item{
			{Name: "Widget", Price: 29.99, Qty: 3},
			{Name: "Gadget", Price: 49.99, Qty: 1},
		},
		Coupon: "SAVE10",
	}
	payload, _ := json.Marshal(order)

	wfDef := types.WorkflowDefinition{
		Name: "result-piping-demo",
		Tasks: []types.WorkflowTask{
			{
				Name:     "validate",
				TaskType: "rp.validate",
				Payload:  payload,
			},
			{
				Name:       "calculate_total",
				TaskType:   "rp.calculate_total",
				Payload:    payload,
				DependsOn:  []string{"validate"},
				ResultFrom: "validate", // receives ValidationResult from validate
			},
			{
				Name:       "apply_discount",
				TaskType:   "rp.apply_discount",
				Payload:    payload,
				DependsOn:  []string{"calculate_total"},
				ResultFrom: "calculate_total", // receives TotalResult from calculate_total
			},
			{
				Name:       "finalize",
				TaskType:   "rp.finalize",
				Payload:    payload,
				DependsOn:  []string{"apply_discount"},
				ResultFrom: "apply_discount", // receives DiscountResult from apply_discount
			},
		},
	}

	wf, err := cli.EnqueueWorkflow(ctx, wfDef, nil)
	if err != nil {
		logger.Error().Err(err).Msg("failed to enqueue workflow")
		os.Exit(1)
	}

	logger.Info().String("workflow_id", wf.ID).Msg("enqueued result-piping workflow")

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
					if len(status.Output) > 0 {
						logger.Info().String("workflow_output", string(status.Output)).Msg("workflow output (from leaf task)")
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
