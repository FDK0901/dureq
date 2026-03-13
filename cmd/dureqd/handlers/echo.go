package handlers

import (
	"context"
	"fmt"
	"time"

	"github.com/FDK0901/dureq/pkg/types"
)

// EchoPayload is the input for the built-in echo handler.
type EchoPayload struct {
	Message string `json:"message"`
}

func init() {
	Register(types.HandlerDefinition{
		TaskType:    "echo",
		Concurrency: 5,
		Timeout:     10 * time.Second,
		Handler: types.TypedHandler(func(ctx context.Context, p EchoPayload) error {
			jobID := types.GetJobID(ctx)
			fmt.Printf("[echo] job=%s message=%q\n", jobID, p.Message)
			return nil
		}),
	})
}
