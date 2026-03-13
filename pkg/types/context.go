package types

import (
	"context"
	"encoding/json"
	"fmt"
)

// Context keys for job metadata accessible within handler execution.
type contextKey int

const (
	ctxKeyJobID contextKey = iota
	ctxKeyRunID
	ctxKeyAttempt
	ctxKeyMaxRetry
	ctxKeyTaskType
	ctxKeyPriority
	ctxKeyNodeID
	ctxKeyHeaders
	ctxKeyProgressReporter
	ctxKeySideEffectStore
	ctxKeySideEffectTTL
)

// WithJobID stores the job ID in the context.
func WithJobID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyJobID, id)
}

// GetJobID extracts the job ID from the context.
func GetJobID(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyJobID).(string)
	return v
}

// WithRunID stores the run ID in the context.
func WithRunID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyRunID, id)
}

// GetRunID extracts the run ID from the context.
func GetRunID(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyRunID).(string)
	return v
}

// WithAttempt stores the attempt number in the context.
func WithAttempt(ctx context.Context, attempt int) context.Context {
	return context.WithValue(ctx, ctxKeyAttempt, attempt)
}

// GetAttempt extracts the attempt number from the context.
func GetAttempt(ctx context.Context) int {
	v, _ := ctx.Value(ctxKeyAttempt).(int)
	return v
}

// WithMaxRetry stores the max retry count in the context.
func WithMaxRetry(ctx context.Context, max int) context.Context {
	return context.WithValue(ctx, ctxKeyMaxRetry, max)
}

// GetMaxRetry extracts the max retry count from the context.
func GetMaxRetry(ctx context.Context) int {
	v, _ := ctx.Value(ctxKeyMaxRetry).(int)
	return v
}

// WithTaskType stores the task type in the context.
func WithTaskType(ctx context.Context, tt TaskType) context.Context {
	return context.WithValue(ctx, ctxKeyTaskType, tt)
}

// GetTaskType extracts the task type from the context.
func GetTaskType(ctx context.Context) TaskType {
	v, _ := ctx.Value(ctxKeyTaskType).(TaskType)
	return v
}

// WithPriority stores the priority in the context.
func WithPriority(ctx context.Context, p Priority) context.Context {
	return context.WithValue(ctx, ctxKeyPriority, p)
}

// GetPriority extracts the priority from the context.
func GetPriority(ctx context.Context) Priority {
	v, _ := ctx.Value(ctxKeyPriority).(Priority)
	return v
}

// WithNodeID stores the node ID in the context.
func WithNodeID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyNodeID, id)
}

// GetNodeID extracts the node ID from the context.
func GetNodeID(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyNodeID).(string)
	return v
}

// WithHeaders stores the headers map in the context.
func WithHeaders(ctx context.Context, headers map[string]string) context.Context {
	return context.WithValue(ctx, ctxKeyHeaders, headers)
}

// GetHeaders extracts the headers map from the context.
func GetHeaders(ctx context.Context) map[string]string {
	v, _ := ctx.Value(ctxKeyHeaders).(map[string]string)
	return v
}

// ProgressReporterFunc is the callback stored in context for reporting progress.
type ProgressReporterFunc func(ctx context.Context, data json.RawMessage) error

// WithProgressReporter stores the progress reporter function in the context.
func WithProgressReporter(ctx context.Context, fn ProgressReporterFunc) context.Context {
	return context.WithValue(ctx, ctxKeyProgressReporter, fn)
}

// SideEffectStore is a minimal interface for side-effect step persistence.
// Implemented by internal/store.RedisStore and injected via context by the worker.
type SideEffectStore interface {
	// ClaimSideEffect atomically claims a side-effect step for the given run.
	// Returns (result, true) if already done, ("", false) if newly claimed.
	ClaimSideEffect(ctx context.Context, runID, stepKey string, ttlSeconds int) (string, bool, error)
	// CompleteSideEffect marks a step as done and stores the result.
	CompleteSideEffect(ctx context.Context, runID, stepKey string, result string) error
}

// WithSideEffectStore stores the SideEffectStore in the context.
func WithSideEffectStore(ctx context.Context, s SideEffectStore) context.Context {
	return context.WithValue(ctx, ctxKeySideEffectStore, s)
}

// GetSideEffectStore extracts the SideEffectStore from the context.
func GetSideEffectStore(ctx context.Context) SideEffectStore {
	v, _ := ctx.Value(ctxKeySideEffectStore).(SideEffectStore)
	return v
}

// WithSideEffectTTL stores the side-effect step cache TTL (in seconds) in the context.
func WithSideEffectTTL(ctx context.Context, seconds int) context.Context {
	return context.WithValue(ctx, ctxKeySideEffectTTL, seconds)
}

// GetSideEffectTTL extracts the side-effect TTL from the context.
// Returns 0 if not set (caller should use a default).
func GetSideEffectTTL(ctx context.Context) int {
	v, _ := ctx.Value(ctxKeySideEffectTTL).(int)
	return v
}

// ReportProgress reports user-defined progress data for the current run.
// The data is stored alongside the run's heartbeat and is accessible via
// the monitoring API and client SDK.
func ReportProgress(ctx context.Context, data any) error {
	fn, ok := ctx.Value(ctxKeyProgressReporter).(ProgressReporterFunc)
	if !ok || fn == nil {
		return ErrNoProgressReporter
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal progress: %w", err)
	}
	return fn(ctx, jsonData)
}
