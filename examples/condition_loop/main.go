// Package main demonstrates condition-based looping in dureq workflows.
//
// Scenario: Iterative data processing with a condition node controlling
// the loop. The workflow processes batches of items until all are done.
//
//	init → process_batch → check_done(condition) ─┬─ route 0 → process_batch (loop)
//	                                               └─ route 1 → summarize
//
// The check_done condition handler inspects progress and loops back
// to process_batch (route 0) until processing is complete, then exits
// to summarize (route 1). MaxIterations prevents infinite loops.
package main

import (
	"context"
	"encoding/json"
	"fmt"
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

type LoopPayload struct {
	Total int `json:"total"`
}

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	zl := chainedzerolog.NewZerologBase()
	logger := chainedzerolog.NewZerolog(zl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Track how many batches have been processed.
	var batchesProcessed atomic.Int32

	// --- Server Side ---

	srv, err := dureq.NewServer(
		dureq.WithRedisURL("redis://localhost:6381"),
		dureq.WithRedisDB(15),
		dureq.WithRedisPassword("your-password"),
		dureq.WithNodeID("condition-loop-node-1"),
		dureq.WithMaxConcurrency(10),
		dureq.WithLogger(logger),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "loop.init",
		Timeout:  10 * time.Second,
		Handler: func(_ context.Context, payload json.RawMessage) error {
			logger.Info().Msg("initializing batch processing")
			batchesProcessed.Store(0)
			return nil
		},
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "loop.process_batch",
		Timeout:  10 * time.Second,
		Handler: func(_ context.Context, payload json.RawMessage) error {
			n := batchesProcessed.Add(1)
			logger.Info().Int("batch", int(n)).Msg("processing batch")
			time.Sleep(300 * time.Millisecond)
			return nil
		},
	})

	// Condition handler: check if all batches are done.
	// Route 0 = loop back (more work), Route 1 = exit (done).
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "loop.check_done",
		Timeout:  10 * time.Second,
		HandlerWithResult: types.TypedConditionHandler(func(_ context.Context, p LoopPayload) (uint, error) {
			processed := int(batchesProcessed.Load())
			logger.Info().Int("processed", processed).Int("total", p.Total).Msg("checking if done")
			if processed < p.Total {
				return 0, nil // route 0: loop back to process_batch
			}
			return 1, nil // route 1: exit to summarize
		}),
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "loop.summarize",
		Timeout:  10 * time.Second,
		Handler: func(_ context.Context, payload json.RawMessage) error {
			logger.Info().Int("total_batches", int(batchesProcessed.Load())).Msg("summarizing results")
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

	// Process 3 batches total, looping through condition node.
	loopPayload, _ := json.Marshal(LoopPayload{Total: 3})

	wfDef := types.WorkflowDefinition{
		Name: "iterative-processing",
		Tasks: []types.WorkflowTask{
			{
				Name:     "init",
				TaskType: "loop.init",
				Payload:  loopPayload,
			},
			{
				Name:      "process_batch",
				TaskType:  "loop.process_batch",
				Payload:   loopPayload,
				DependsOn: []string{"init"},
			},
			{
				Name:      "check_done",
				TaskType:  "loop.check_done",
				Payload:   loopPayload,
				DependsOn: []string{"process_batch"},
				Type:      types.WorkflowTaskCondition,
				ConditionRoutes: map[uint]string{
					0: "process_batch", // loop back
					1: "summarize",     // exit loop
				},
				MaxIterations: 10, // safety limit
			},
			{
				Name:      "summarize",
				TaskType:  "loop.summarize",
				Payload:   loopPayload,
				DependsOn: []string{"check_done"},
			},
		},
	}

	wf, err := cli.EnqueueWorkflow(ctx, wfDef, nil)
	if err != nil {
		logger.Error().Err(err).Msg("failed to enqueue workflow")
		os.Exit(1)
	}

	logger.Info().String("workflow_id", wf.ID).Msg("enqueued iterative workflow (will loop 3 times)")

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
				logger.Info().String("status", string(status.Status)).Int("batches_processed", int(batchesProcessed.Load())).Msg("workflow progress")

				if status.Status == types.WorkflowStatusCompleted {
					if iters, ok := status.ConditionIterations["check_done"]; ok {
						logger.Info().Int("iterations", iters).Msg("condition loop completed")
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
