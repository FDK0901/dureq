package handlers

import (
	"context"
	"fmt"
	"time"

	"github.com/FDK0901/dureq/pkg/types"
)

func init() {
	// noop: completes immediately without side effects. Useful for testing
	// scheduling, dispatch, and completion pipelines.
	Register(types.HandlerDefinition{
		TaskType:    "noop",
		Concurrency: 10,
		Timeout:     5 * time.Second,
		Handler: types.TypedHandler(func(ctx context.Context, _ struct{}) error {
			return nil
		}),
	})

	// fail-always: always returns an error. Useful for testing retry policies,
	// DLQ behavior, and error handling pipelines.
	Register(types.HandlerDefinition{
		TaskType:    "fail-always",
		Concurrency: 5,
		Timeout:     5 * time.Second,
		RetryPolicy: &types.RetryPolicy{MaxAttempts: 1, InitialDelay: time.Second},
		Handler: types.TypedHandler(func(ctx context.Context, _ struct{}) error {
			return fmt.Errorf("fail-always: intentional failure for testing")
		}),
	})
}
