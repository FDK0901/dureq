package types

import (
	"context"
	"encoding/json"
	"time"
)

// GroupAggregator defines how a set of grouped tasks should be combined
// into a single aggregated task for processing.
type GroupAggregator interface {
	// Aggregate combines multiple task payloads into a single payload.
	// The group name and list of payloads are provided.
	Aggregate(group string, payloads []json.RawMessage) (json.RawMessage, error)
}

// GroupAggregatorFunc is a convenience adapter for GroupAggregator.
type GroupAggregatorFunc func(group string, payloads []json.RawMessage) (json.RawMessage, error)

func (f GroupAggregatorFunc) Aggregate(group string, payloads []json.RawMessage) (json.RawMessage, error) {
	return f(group, payloads)
}

// GroupConfig configures task aggregation for a server.
type GroupConfig struct {
	// Aggregator combines grouped tasks into one before processing.
	Aggregator GroupAggregator

	// GracePeriod is how long to wait for additional tasks after the
	// first task arrives in a group. Default: 5s.
	GracePeriod time.Duration

	// MaxDelay is the maximum time a group can wait before being
	// flushed, regardless of GracePeriod. Default: 30s.
	MaxDelay time.Duration

	// MaxSize is the maximum number of tasks in a group before
	// it is flushed immediately. Default: 100.
	MaxSize int
}

// GroupMessage is stored in the group set pending aggregation.
type GroupMessage struct {
	JobID    string          `json:"job_id"`
	TaskType TaskType        `json:"task_type"`
	Payload  json.RawMessage `json:"payload"`
	AddedAt  time.Time       `json:"added_at"`
}

// AggregateResult is the outcome of flushing a group.
type AggregateResult struct {
	Group           string          `json:"group"`
	Count           int             `json:"count"`
	AggregatedPayload json.RawMessage `json:"aggregated_payload"`
}

// EnqueueGroupOption configures a grouped task enqueue.
type EnqueueGroupOption struct {
	Group    string // the group name (required)
	TaskType TaskType
	Payload  json.RawMessage
}

// GroupHandler processes an aggregated group of tasks.
type GroupHandler func(ctx context.Context, group string, payload json.RawMessage) error
