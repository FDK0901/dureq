package handlers

import (
	"context"
	"fmt"
	"time"

	"github.com/FDK0901/dureq/pkg/types"
)

// SleepPayload is the input for the built-in sleep handler.
// Useful for testing, demos, and simulating work duration.
type SleepPayload struct {
	DurationMs int    `json:"duration_ms"`
	Message    string `json:"message,omitempty"`
}

func init() {
	Register(types.HandlerDefinition{
		TaskType:    "sleep",
		Concurrency: 10,
		Timeout:     5 * time.Minute,
		Handler: types.TypedHandler(func(ctx context.Context, p SleepPayload) error {
			d := time.Duration(p.DurationMs) * time.Millisecond
			if d <= 0 {
				d = time.Second
			}
			if p.Message != "" {
				fmt.Printf("[sleep] %s (waiting %s)\n", p.Message, d)
			}
			select {
			case <-time.After(d):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}),
	})
}
