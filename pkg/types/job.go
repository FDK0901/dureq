package types

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"
)

// Priority represents the execution priority of a job (1-10).
type Priority int

const (
	PriorityLow      Priority = 1
	PriorityNormal   Priority = 5
	PriorityHigh     Priority = 8
	PriorityCritical Priority = 10
)

// PriorityTier maps a numeric priority to a Redis Stream tier.
type PriorityTier string

const (
	TierHigh   PriorityTier = "high"
	TierNormal PriorityTier = "normal"
	TierLow    PriorityTier = "low"
)

// TierForPriority returns the subject tier for a given priority value.
func TierForPriority(p Priority) PriorityTier {
	switch {
	case p >= 8:
		return TierHigh
	case p >= 3:
		return TierNormal
	default:
		return TierLow
	}
}

// TaskType identifies a registered handler. This is the "how" —
// what function to call when a job of this type fires.
type TaskType string

func (t TaskType) String() string { return string(t) }

// HandlerFunc is the function signature server handlers must implement.
type HandlerFunc func(ctx context.Context, payload json.RawMessage) error

// HandlerFuncWithResult is an extended handler that returns output data.
type HandlerFuncWithResult func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error)

// HandlerDefinition describes a handler registered on the server.
type HandlerDefinition struct {
	TaskType          TaskType
	Handler           HandlerFunc           // original handler (no result)
	HandlerWithResult HandlerFuncWithResult // handler that returns result data (optional)
	Concurrency       int                   // max concurrent per node (0 = pool default)
	Timeout           time.Duration         // per-execution timeout (0 = no timeout)
	RetryPolicy       *RetryPolicy          // default retry (overridable per-job)
	Middlewares       []MiddlewareFunc      // per-handler middleware (applied after global)
}

// RetryPolicy describes retry behavior.
type RetryPolicy struct {
	MaxAttempts  int           `json:"max_attempts"`
	InitialDelay time.Duration `json:"initial_delay"`
	MaxDelay     time.Duration `json:"max_delay"`
	Multiplier   float64       `json:"multiplier"`
	Jitter       float64       `json:"jitter"` // 0.0 - 1.0
}

// DefaultRetryPolicy returns a sensible default retry policy.
func DefaultRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		MaxAttempts:  3,
		InitialDelay: 5 * time.Second,
		MaxDelay:     5 * time.Minute,
		Multiplier:   2.0,
		Jitter:       0.1,
	}
}

// CalculateBackoff computes the retry delay for the given attempt using
// exponential backoff with optional jitter.
func CalculateBackoff(attempt int, policy *RetryPolicy) time.Duration {
	delay := float64(policy.InitialDelay) * math.Pow(policy.Multiplier, float64(attempt))

	if time.Duration(delay) > policy.MaxDelay {
		delay = float64(policy.MaxDelay)
	}

	if policy.Jitter > 0 {
		jitterRange := delay * policy.Jitter
		delay = delay - jitterRange + (rand.Float64() * 2 * jitterRange)
	}

	return time.Duration(delay)
}

// ScheduleType defines how a job is scheduled.
type ScheduleType string

const (
	ScheduleImmediate ScheduleType = "IMMEDIATE"
	ScheduleOneTime   ScheduleType = "ONE_TIME"
	ScheduleDuration  ScheduleType = "DURATION"
	ScheduleCron      ScheduleType = "CRON"
	ScheduleDaily     ScheduleType = "DAILY"
	ScheduleWeekly    ScheduleType = "WEEKLY"
	ScheduleMonthly   ScheduleType = "MONTHLY"
)

// OverlapPolicy controls behavior when a scheduled job fires while
// a previous execution is still running.
type OverlapPolicy string

const (
	// OverlapAllowAll dispatches regardless of active runs (default).
	OverlapAllowAll OverlapPolicy = "ALLOW_ALL"
	// OverlapSkip skips the dispatch if any run for this job is active.
	OverlapSkip OverlapPolicy = "SKIP"
	// OverlapBufferOne buffers at most one pending dispatch while a run is active.
	OverlapBufferOne OverlapPolicy = "BUFFER_ONE"
	// OverlapBufferAll buffers all pending dispatches while a run is active.
	OverlapBufferAll OverlapPolicy = "BUFFER_ALL"
)

// Schedule defines "when" a job should run.
type Schedule struct {
	Type ScheduleType `json:"type"`

	// ONE_TIME: run once at this time
	RunAt *time.Time `json:"run_at,omitempty"`

	// DURATION: repeat at fixed interval
	Interval *Duration `json:"interval,omitempty"`

	// CRON: cron expression
	CronExpr *string `json:"cron_expr,omitempty"`

	// DAILY/WEEKLY/MONTHLY
	RegularInterval *uint    `json:"regular_interval,omitempty"` // every N days/weeks/months
	AtTimes         []AtTime `json:"at_times,omitempty"`         // times of day
	IncludedDays    []int    `json:"included_days,omitempty"`    // weekdays (0-6) or month days (1-31)

	// Common boundaries
	StartsAt *time.Time `json:"starts_at,omitempty"`
	EndsAt   *time.Time `json:"ends_at,omitempty"`
	Timezone string     `json:"timezone,omitempty"`

	// OverlapPolicy controls what happens when a new occurrence fires
	// while a previous run is still active. Only for recurring schedules.
	OverlapPolicy OverlapPolicy `json:"overlap_policy,omitempty"`

	// CatchupWindow defines the maximum age of a missed firing that should
	// be backfilled. Zero means no backfill (only the latest due firing).
	CatchupWindow *Duration `json:"catchup_window,omitempty"`

	// MaxBackfillPerTick limits how many missed firings are enqueued in a
	// single scheduler tick to prevent flooding. Default: 10 if CatchupWindow is set.
	MaxBackfillPerTick *int `json:"max_backfill_per_tick,omitempty"`
}

// AtTime represents a time of day as hour, minute, second.
type AtTime struct {
	Hour   uint `json:"hour"`
	Minute uint `json:"minute"`
	Second uint `json:"second"`
}

// Duration is a JSON-friendly time.Duration.
type Duration time.Duration

func (d Duration) Std() time.Duration { return time.Duration(d) }

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

// Validate checks the schedule for consistency.
func (s *Schedule) Validate() error {
	if s.Type == "" {
		return errors.New("schedule type is required")
	}

	now := time.Now()

	if s.EndsAt != nil && s.EndsAt.Before(now) {
		return errors.New("ends_at must be in the future")
	}

	switch s.Type {
	case ScheduleImmediate:
		// No extra validation needed.

	case ScheduleOneTime:
		if s.RunAt == nil {
			return errors.New("run_at is required for ONE_TIME schedule")
		}
		if s.RunAt.Before(now) {
			return errors.New("run_at must be in the future")
		}

	case ScheduleDuration:
		if s.Interval == nil {
			return errors.New("interval is required for DURATION schedule")
		}
		if s.Interval.Std() <= 0 {
			return errors.New("interval must be positive")
		}

	case ScheduleCron:
		if s.CronExpr == nil || *s.CronExpr == "" {
			return errors.New("cron_expr is required for CRON schedule")
		}

	case ScheduleDaily:
		if err := s.validateRegular(); err != nil {
			return fmt.Errorf("daily schedule: %w", err)
		}

	case ScheduleWeekly:
		if err := s.validateRegular(); err != nil {
			return fmt.Errorf("weekly schedule: %w", err)
		}
		if len(s.IncludedDays) == 0 {
			return errors.New("included_days is required for WEEKLY schedule")
		}
		for _, day := range s.IncludedDays {
			if day < 0 || day > 6 {
				return fmt.Errorf("weekly included_days must be 0 (Sun) - 6 (Sat), got %d", day)
			}
		}

	case ScheduleMonthly:
		if err := s.validateRegular(); err != nil {
			return fmt.Errorf("monthly schedule: %w", err)
		}
		if len(s.IncludedDays) == 0 {
			return errors.New("included_days is required for MONTHLY schedule")
		}
		for _, day := range s.IncludedDays {
			if day < 1 || day > 31 {
				return fmt.Errorf("monthly included_days must be 1-31, got %d", day)
			}
		}

	default:
		return fmt.Errorf("unknown schedule type: %s", s.Type)
	}

	// Validate overlap policy.
	if s.OverlapPolicy != "" {
		switch s.OverlapPolicy {
		case OverlapAllowAll, OverlapSkip, OverlapBufferOne, OverlapBufferAll:
			// valid
		default:
			return fmt.Errorf("unknown overlap_policy: %s", s.OverlapPolicy)
		}
		if s.Type == ScheduleImmediate || s.Type == ScheduleOneTime {
			return errors.New("overlap_policy is only applicable to recurring schedules")
		}
	}

	// Validate catchup window.
	if s.CatchupWindow != nil {
		if s.CatchupWindow.Std() < 0 {
			return errors.New("catchup_window must be non-negative")
		}
		if s.Type == ScheduleImmediate || s.Type == ScheduleOneTime {
			return errors.New("catchup_window is only valid for recurring schedules")
		}
	}
	if s.MaxBackfillPerTick != nil && *s.MaxBackfillPerTick < 1 {
		return errors.New("max_backfill_per_tick must be >= 1")
	}

	return nil
}

func (s *Schedule) validateRegular() error {
	if s.RegularInterval == nil {
		return errors.New("regular_interval is required")
	}
	if *s.RegularInterval == 0 {
		return errors.New("regular_interval must be > 0")
	}
	if len(s.AtTimes) == 0 {
		return errors.New("at_times is required")
	}
	for _, at := range s.AtTimes {
		if at.Hour > 23 {
			return fmt.Errorf("hour must be 0-23, got %d", at.Hour)
		}
		if at.Minute > 59 {
			return fmt.Errorf("minute must be 0-59, got %d", at.Minute)
		}
		if at.Second > 59 {
			return fmt.Errorf("second must be 0-59, got %d", at.Second)
		}
	}
	return nil
}

// Job is the persisted entity representing a registered job with its schedule.
type Job struct {
	ID          string          `json:"id"`
	TaskType    TaskType        `json:"task_type"`
	Payload     json.RawMessage `json:"payload"`
	Schedule    Schedule        `json:"schedule"`
	RetryPolicy *RetryPolicy    `json:"retry_policy,omitempty"`

	// State
	Status      JobStatus  `json:"status"`
	Attempt     int        `json:"attempt"`
	LastError   *string    `json:"last_error,omitempty"`
	LastRunAt   *time.Time `json:"last_run_at,omitempty"`
	NextRunAt   *time.Time `json:"next_run_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`

	// Metadata
	Tags      []string          `json:"tags,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"` // key-value metadata
	UniqueKey *string           `json:"unique_key,omitempty"`
	DLQAfter  *int              `json:"dlq_after,omitempty"` // move to DLQ after N failures

	// Workflow association (set when this job is part of a workflow).
	WorkflowID   *string `json:"workflow_id,omitempty"`
	WorkflowTask *string `json:"workflow_task,omitempty"`

	// Batch association (set when this job is part of a batch).
	BatchID   *string `json:"batch_id,omitempty"`
	BatchItem *string `json:"batch_item,omitempty"` // item ID within batch
	BatchRole *string `json:"batch_role,omitempty"` // "onetime" or "item"

	// Priority and queue policies.
	Priority               *Priority `json:"priority,omitempty"`
	ScheduleToStartTimeout *Duration `json:"schedule_to_start_timeout,omitempty"`

	// HeartbeatTimeout overrides the default 30s stale age for orphan detection.
	// If set, a run is considered orphaned if no heartbeat is received within
	// this duration.
	HeartbeatTimeout *Duration `json:"heartbeat_timeout,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// JobRun represents an individual execution of a job.
type JobRun struct {
	ID              string          `json:"id"`
	JobID           string          `json:"job_id"`
	NodeID          string          `json:"node_id"`
	Status          RunStatus       `json:"status"`
	Attempt         int             `json:"attempt"`
	Error           *string         `json:"error,omitempty"`
	StartedAt       time.Time       `json:"started_at"`
	FinishedAt      *time.Time      `json:"finished_at,omitempty"`
	Duration        time.Duration   `json:"duration,omitempty"`
	LastHeartbeatAt *time.Time      `json:"last_heartbeat_at,omitempty"`
	Progress        json.RawMessage `json:"progress,omitempty"` // user-reported progress data
}

// ScheduleEntry is the KV entry the leader's scheduler scans for due jobs.
type ScheduleEntry struct {
	JobID           string     `json:"job_id"`
	NextRunAt       time.Time  `json:"next_run_at"`
	Schedule        Schedule   `json:"schedule"`
	LastProcessedAt *time.Time `json:"last_processed_at,omitempty"` // last successful processing time
}

// WorkMessage is the message published to DUREQ_WORK stream.
type WorkMessage struct {
	RunID        string            `json:"run_id"`
	JobID        string            `json:"job_id"`
	TaskType     TaskType          `json:"task_type"`
	Payload      json.RawMessage   `json:"payload"`
	Attempt      int               `json:"attempt"`
	Deadline     time.Time         `json:"deadline"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	Headers      map[string]string `json:"headers,omitempty"`
	Priority     Priority          `json:"priority,omitempty"`
	DispatchedAt time.Time         `json:"dispatched_at,omitempty"`
}

// WorkResult is the outcome of executing a work message.
type WorkResult struct {
	RunID   string          `json:"run_id"`
	JobID   string          `json:"job_id"`
	Success bool            `json:"success"`
	Error   *string         `json:"error,omitempty"`
	Output  json.RawMessage `json:"output,omitempty"` // handler output data
}

// NodeInfo is registered by each node in the dureq_nodes KV bucket.
type NodeInfo struct {
	NodeID        string     `json:"node_id"`
	Address       string     `json:"address,omitempty"`
	TaskTypes     []string   `json:"task_types"`
	StartedAt     time.Time  `json:"started_at"`
	LastHeartbeat time.Time  `json:"last_heartbeat"`
	PoolStats     *PoolStats `json:"pool_stats,omitempty"`
	ActiveRunIDs  []string   `json:"active_run_ids,omitempty"`
}

// PoolStats captures worker pool metrics for a node.
type PoolStats struct {
	RunningWorkers int    `json:"running_workers"`
	IdleWorkers    int    `json:"idle_workers"`
	MaxConcurrency int    `json:"max_concurrency"`
	TotalSubmitted uint64 `json:"total_submitted"`
	TotalCompleted uint64 `json:"total_completed"`
	TotalFailed    uint64 `json:"total_failed"`
	QueueLength    int    `json:"queue_length"`
}

// --- Workflow Types ---

// WorkflowStatus represents the lifecycle state of a workflow instance.
type WorkflowStatus string

const (
	WorkflowStatusPending   WorkflowStatus = "pending"
	WorkflowStatusRunning   WorkflowStatus = "running"
	WorkflowStatusCompleted WorkflowStatus = "completed"
	WorkflowStatusFailed    WorkflowStatus = "failed"
	WorkflowStatusCancelled WorkflowStatus = "cancelled"
)

func (s WorkflowStatus) IsTerminal() bool {
	switch s {
	case WorkflowStatusCompleted, WorkflowStatusFailed, WorkflowStatusCancelled:
		return true
	}
	return false
}

func (s WorkflowStatus) String() string { return string(s) }

// WorkflowDefinition describes a DAG of tasks to execute.
type WorkflowDefinition struct {
	Name             string         `json:"name"`
	Tasks            []WorkflowTask `json:"tasks"`
	ExecutionTimeout *Duration      `json:"execution_timeout,omitempty"`
	RetryPolicy      *RetryPolicy   `json:"retry_policy,omitempty"`
	DefaultPriority  *Priority      `json:"default_priority,omitempty"`
}

// WorkflowTask is one step in a workflow DAG.
type WorkflowTask struct {
	Name      string          `json:"name"`
	TaskType  TaskType        `json:"task_type"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	DependsOn []string        `json:"depends_on,omitempty"`
}

// WorkflowInstance is a running instance of a workflow definition.
type WorkflowInstance struct {
	ID           string                       `json:"id"`
	WorkflowName string                       `json:"workflow_name"`
	Status       WorkflowStatus               `json:"status"`
	Tasks        map[string]WorkflowTaskState `json:"tasks"`
	Definition   WorkflowDefinition           `json:"definition"`
	Input        json.RawMessage              `json:"input,omitempty"`
	Deadline     *time.Time                   `json:"deadline,omitempty"`
	Attempt      int                          `json:"attempt,omitempty"`
	MaxAttempts  int                          `json:"max_attempts,omitempty"`
	CreatedAt    time.Time                    `json:"created_at"`
	UpdatedAt    time.Time                    `json:"updated_at"`
	CompletedAt  *time.Time                   `json:"completed_at,omitempty"`
}

// WorkflowTaskState tracks the state of one task within a workflow instance.
type WorkflowTaskState struct {
	Name       string     `json:"name"`
	JobID      string     `json:"job_id,omitempty"`
	Status     JobStatus  `json:"status"`
	Error      *string    `json:"error,omitempty"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// --- Batch Processing Types ---

// BatchFailurePolicy controls behavior when a batch item fails.
type BatchFailurePolicy string

const (
	// BatchFailFast stops the batch immediately on first item failure.
	BatchFailFast BatchFailurePolicy = "fail_fast"
	// BatchContinueOnError continues processing remaining items.
	BatchContinueOnError BatchFailurePolicy = "continue_on_error"
)

// BatchDefinition describes a batch processing job to submit.
type BatchDefinition struct {
	Name string `json:"name"`

	// OnetimeTaskType is the handler for shared preprocessing (runs once before items).
	OnetimeTaskType *TaskType       `json:"onetime_task_type,omitempty"`
	OnetimePayload  json.RawMessage `json:"onetime_payload,omitempty"`

	// ItemTaskType is the handler that processes each batch item.
	ItemTaskType TaskType    `json:"item_task_type"`
	Items        []BatchItem `json:"items"`

	// FailurePolicy controls partial failure behavior. Default: continue_on_error.
	FailurePolicy BatchFailurePolicy `json:"failure_policy,omitempty"`

	// ChunkSize controls how many items are dispatched concurrently. Default: 100.
	ChunkSize int `json:"chunk_size,omitempty"`

	// ItemRetryPolicy for individual item failures.
	ItemRetryPolicy *RetryPolicy `json:"item_retry_policy,omitempty"`

	// Execution timeout for the entire batch.
	ExecutionTimeout *Duration `json:"execution_timeout,omitempty"`

	// RetryPolicy for retrying the entire batch on failure.
	RetryPolicy *RetryPolicy `json:"retry_policy,omitempty"`

	// DefaultPriority for all jobs created by this batch.
	DefaultPriority *Priority `json:"default_priority,omitempty"`
}

// BatchItem is a single item within a batch.
type BatchItem struct {
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
}

// BatchInstance is the runtime state of a batch being processed.
type BatchInstance struct {
	ID         string          `json:"id"`
	Name       string          `json:"name"`
	Status     WorkflowStatus  `json:"status"`
	Definition BatchDefinition `json:"definition"`

	// Onetime preprocessing state.
	OnetimeState *BatchOnetimeState `json:"onetime_state,omitempty"`

	// Per-item tracking.
	ItemStates map[string]BatchItemState `json:"item_states"`

	// Progress counters.
	TotalItems     int `json:"total_items"`
	CompletedItems int `json:"completed_items"`
	FailedItems    int `json:"failed_items"`
	RunningItems   int `json:"running_items"`
	PendingItems   int `json:"pending_items"`

	// Chunk tracking — index into Definition.Items for the next chunk.
	NextChunkIndex int `json:"next_chunk_index"`

	// Timeout and retry tracking.
	Deadline    *time.Time `json:"deadline,omitempty"`
	Attempt     int        `json:"attempt,omitempty"`
	MaxAttempts int        `json:"max_attempts,omitempty"`

	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

// BatchOnetimeState tracks the shared preprocessing step.
type BatchOnetimeState struct {
	JobID      string          `json:"job_id,omitempty"`
	Status     JobStatus       `json:"status"`
	ResultData json.RawMessage `json:"result_data,omitempty"`
	Error      *string         `json:"error,omitempty"`
	StartedAt  *time.Time      `json:"started_at,omitempty"`
	FinishedAt *time.Time      `json:"finished_at,omitempty"`
}

// BatchItemState tracks an individual item within a batch.
type BatchItemState struct {
	ItemID     string     `json:"item_id"`
	JobID      string     `json:"job_id,omitempty"`
	Status     JobStatus  `json:"status"`
	Error      *string    `json:"error,omitempty"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// BatchItemResult stores the output of a single batch item execution.
type BatchItemResult struct {
	BatchID string          `json:"batch_id"`
	ItemID  string          `json:"item_id"`
	Success bool            `json:"success"`
	Output  json.RawMessage `json:"output,omitempty"`
	Error   *string         `json:"error,omitempty"`
}

// BatchProgress is published as an event for progress tracking.
type BatchProgress struct {
	BatchID string `json:"batch_id"`
	Done    int    `json:"done"`
	Total   int    `json:"total"`
	Failed  int    `json:"failed"`
	Step    string `json:"step,omitempty"`
}
