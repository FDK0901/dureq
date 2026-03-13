package types

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ConcurrencyKey defines a concurrency constraint for job execution.
// Jobs sharing the same Key are limited to MaxConcurrency simultaneous runs.
type ConcurrencyKey struct {
	Key            string `json:"key"`             // e.g., "tenant:acme", "resource:stripe-api"
	MaxConcurrency int    `json:"max_concurrency"` // e.g., 1 for serialized, 5 for bounded
}

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
	// Version identifies the handler build for safe deployments.
	// When set, the worker will skip messages dispatched with a different version
	// and re-enqueue them for a matching worker to pick up.
	Version string
}

// RetryPolicy describes retry behavior.
type RetryPolicy struct {
	MaxAttempts  int           `json:"max_attempts"`
	InitialDelay time.Duration `json:"initial_delay"`
	MaxDelay     time.Duration `json:"max_delay"`
	Multiplier   float64       `json:"multiplier"`
	Jitter       float64       `json:"jitter"` // 0.0 - 1.0
	// NonRetryableErrors is a list of error message substrings that should not be retried.
	// If a task fails with an error containing any of these strings, it is immediately
	// moved to dead status without retrying.
	NonRetryableErrors []string `json:"non_retryable_errors,omitempty"`
	// PauseAfterErrCount pauses the job instead of moving to dead when
	// N consecutive errors are reached. 0 = disabled (go directly to dead).
	// When set, the job transitions to "paused" instead of "dead" after
	// this many consecutive failures, giving operators a chance to investigate.
	PauseAfterErrCount int `json:"pause_after_err_count,omitempty"`
	// PauseRetryDelay is the duration after which a paused job is automatically
	// resumed. 0 = manual resume only.
	PauseRetryDelay time.Duration `json:"pause_retry_delay,omitempty"`
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
	// OverlapReplace cancels the current active run and starts a new one.
	OverlapReplace OverlapPolicy = "REPLACE"
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

	// Jitter adds a random offset to the computed next run time.
	// This prevents thundering herd when many jobs share the same schedule.
	// E.g., Jitter of 30s means the firing time is offset by rand(0, 30s).
	Jitter *Duration `json:"jitter,omitempty"`
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
		case OverlapAllowAll, OverlapSkip, OverlapBufferOne, OverlapBufferAll, OverlapReplace:
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

	// Validate jitter.
	if s.Jitter != nil {
		if s.Jitter.Std() < 0 {
			return errors.New("jitter must be non-negative")
		}
		if s.Type == ScheduleImmediate || s.Type == ScheduleOneTime {
			return errors.New("jitter is only valid for recurring schedules")
		}
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
	Status            JobStatus  `json:"status"`
	Attempt           int        `json:"attempt"`
	ConsecutiveErrors int        `json:"consecutive_errors,omitempty"`
	LastError         *string    `json:"last_error,omitempty"`
	LastRunAt         *time.Time `json:"last_run_at,omitempty"`
	NextRunAt         *time.Time `json:"next_run_at,omitempty"`
	CompletedAt       *time.Time `json:"completed_at,omitempty"`
	PausedAt          *time.Time `json:"paused_at,omitempty"`
	ResumeAt          *time.Time `json:"resume_at,omitempty"` // auto-resume time (nil = manual only)

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

	// ConcurrencyKeys limits simultaneous execution across jobs sharing the same key.
	// For example, ConcurrencyKey{Key: "tenant:acme", MaxConcurrency: 1} ensures
	// only one job for tenant "acme" runs at a time.
	ConcurrencyKeys []ConcurrencyKey `json:"concurrency_keys,omitempty"`

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
	// Version is the handler version this message was dispatched with.
	// Workers with a mismatched version will skip and re-enqueue the message.
	Version string `json:"version,omitempty"`

	// ConcurrencyKeys carried from the Job at dispatch time.
	ConcurrencyKeys []ConcurrencyKey `json:"concurrency_keys,omitempty"`
}

// WorkResult is the outcome of executing a work message.
type WorkResult struct {
	RunID              string          `json:"run_id"`
	JobID              string          `json:"job_id"`
	Success            bool            `json:"success"`
	Error              *string         `json:"error,omitempty"`
	Output             json.RawMessage `json:"output,omitempty"`               // handler output data
	ContinueAsNewInput json.RawMessage `json:"continue_as_new_input,omitempty"` // set when handler returns ContinueAsNewError
}

// NodeInfo is registered by each node in the dureq_nodes KV bucket.
type NodeInfo struct {
	NodeID        string            `json:"node_id"`
	Address       string            `json:"address,omitempty"`
	TaskTypes     []string          `json:"task_types"`
	StartedAt     time.Time         `json:"started_at"`
	LastHeartbeat time.Time         `json:"last_heartbeat"`
	PoolStats     *PoolStats        `json:"pool_stats,omitempty"`
	ActiveRunIDs  []string          `json:"active_run_ids,omitempty"`
	// HandlerVersions maps task type → handler version for safe deployment routing.
	HandlerVersions map[string]string `json:"handler_versions,omitempty"`
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
	WorkflowStatusSuspended WorkflowStatus = "suspended"
	WorkflowStatusContinued WorkflowStatus = "continued" // replaced by continue-as-new
)

func (s WorkflowStatus) IsTerminal() bool {
	switch s {
	case WorkflowStatusCompleted, WorkflowStatusFailed, WorkflowStatusCancelled, WorkflowStatusContinued:
		return true
	}
	return false
}

// IsRetryable returns true if the workflow/batch is in a state that allows retry.
// Excludes Completed and Continued — only failed/cancelled should be retried.
func (s WorkflowStatus) IsRetryable() bool {
	switch s {
	case WorkflowStatusFailed, WorkflowStatusCancelled:
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
	Hooks            *WorkflowHooks `json:"hooks,omitempty"`

	// SignalTimeout limits how long a signal handler can run.
	// If not set, signal handlers inherit the workflow's ExecutionTimeout.
	SignalTimeout *Duration `json:"signal_timeout,omitempty"`
}

// WorkflowHooks defines lifecycle hooks that are automatically dispatched
// at specific workflow lifecycle events.
type WorkflowHooks struct {
	OnInit    *WorkflowHookDef `json:"on_init,omitempty"`
	OnSuccess *WorkflowHookDef `json:"on_success,omitempty"`
	OnFailure *WorkflowHookDef `json:"on_failure,omitempty"`
	OnExit    *WorkflowHookDef `json:"on_exit,omitempty"`
}

// WorkflowHookDef defines a single lifecycle hook job.
type WorkflowHookDef struct {
	TaskType TaskType        `json:"task_type"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Timeout  Duration        `json:"timeout,omitempty"`
}

// Precondition defines a condition that must be true before a task is dispatched.
type Precondition struct {
	Type     string `json:"type"`          // "upstream_output"
	Task     string `json:"task,omitempty"` // upstream task name
	Path     string `json:"path"`          // JSONPath in upstream output
	Expected string `json:"expected"`      // expected value (string comparison)
}

// WorkflowTaskType distinguishes static tasks, condition nodes, and subflow generators.
type WorkflowTaskType string

const (
	// WorkflowTaskStatic is a regular task dispatched as a job (default).
	WorkflowTaskStatic WorkflowTaskType = ""
	// WorkflowTaskCondition is a condition node whose handler returns a route index.
	// The orchestrator evaluates the condition and branches to the corresponding successor.
	WorkflowTaskCondition WorkflowTaskType = "condition"
	// WorkflowTaskSubflow is a dynamic subflow node whose handler returns []WorkflowTask
	// at runtime. The orchestrator injects these tasks into the workflow instance.
	WorkflowTaskSubflow WorkflowTaskType = "subflow"
)

// ResultReusePolicy controls whether a workflow task's cached result
// is reused on workflow retry.
type ResultReusePolicy string

const (
	// ResultReuseAlways reuses the cached result if available (default).
	ResultReuseAlways ResultReusePolicy = "always"
	// ResultReuseNever forces re-execution even if a cached result exists.
	ResultReuseNever ResultReusePolicy = "never"
	// ResultReuseOnSuccess reuses only if the previous run succeeded.
	ResultReuseOnSuccess ResultReusePolicy = "on_success"
)

// WorkflowTask is one step in a workflow DAG.
// A task is either a regular task (TaskType set), a child workflow
// (ChildWorkflowDef set), a condition node, or a dynamic subflow generator.
type WorkflowTask struct {
	Name      string          `json:"name"`
	TaskType  TaskType        `json:"task_type"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	DependsOn []string        `json:"depends_on,omitempty"`

	// Priority controls dispatch order among concurrently ready tasks.
	// Higher values are dispatched first. Default is 0.
	Priority int `json:"priority,omitempty"`

	// AllowFailure makes this task's failure non-fatal for the workflow.
	// When true, the task's failure is logged but does not block downstream
	// tasks or cause the workflow to fail. Downstream dependencies are
	// considered "satisfied" even if this task fails.
	AllowFailure bool `json:"allow_failure,omitempty"`

	// ResultFrom specifies which upstream task's output should be injected
	// as this task's payload when dispatching. The referenced task must be
	// in DependsOn. If set, the original Payload field is ignored and
	// replaced with the upstream task's result at dispatch time.
	ResultFrom string `json:"result_from,omitempty"`

	// Type distinguishes static tasks, condition nodes, and subflow generators.
	// Empty string (default) means a regular static task.
	Type WorkflowTaskType `json:"type,omitempty"`

	// ConditionRoutes maps condition handler return values (0, 1, 2, ...)
	// to successor task names. Only used when Type == WorkflowTaskCondition.
	// The condition handler returns a uint; the orchestrator looks up the
	// corresponding task name and marks only that branch as "satisfied".
	ConditionRoutes map[uint]string `json:"condition_routes,omitempty"`

	// MaxIterations limits how many times a condition node can be re-evaluated
	// (to prevent infinite loops). Default 0 means use system default (100).
	MaxIterations int `json:"max_iterations,omitempty"`

	// ChildWorkflowDef, when set, makes this task spawn a child workflow
	// instead of dispatching a regular job. The child runs independently
	// and the parent task completes when the child workflow completes.
	ChildWorkflowDef *WorkflowDefinition `json:"child_workflow_def,omitempty"`

	// Preconditions define conditions that must be true before this task is dispatched.
	// If any precondition fails, the task is skipped.
	Preconditions []Precondition `json:"preconditions,omitempty"`

	// ResultReuse controls whether this task's cached result is reused on
	// workflow retry. Default ("" or "always") reuses cached results.
	// "never" forces re-execution. "on_success" reuses only successful results.
	ResultReuse ResultReusePolicy `json:"result_reuse,omitempty"`
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

	// PendingDeps tracks the number of unresolved dependencies per task.
	// Initialized at workflow start, decremented on dependency completion.
	// A task with PendingDeps[name] == 0 and status == pending is ready.
	PendingDeps map[string]int `json:"pending_deps,omitempty"`

	// ConditionIterations tracks how many times each condition node has been
	// evaluated, to enforce MaxIterations limits.
	ConditionIterations map[string]int `json:"condition_iterations,omitempty"`

	// ParentWorkflowID links this workflow to a parent workflow that spawned it
	// as a child workflow task.
	ParentWorkflowID *string `json:"parent_workflow_id,omitempty"`
	// ParentTaskName is the task name in the parent workflow that this child represents.
	ParentTaskName *string `json:"parent_task_name,omitempty"`

	// HookStates tracks the state of lifecycle hooks dispatched for this workflow.
	HookStates map[string]WorkflowTaskState `json:"hook_states,omitempty"`

	// Output is the result of the final (leaf) task, copied here when the
	// workflow completes so callers can retrieve the workflow-level result.
	Output json.RawMessage `json:"output,omitempty"`

	// ArchivedTaskCount is the number of completed tasks that have been moved
	// to the archive hash (prefix:workflow:{id}:archive) to reduce state size.
	ArchivedTaskCount int `json:"archived_task_count,omitempty"`

	// ContinuedFrom links to the predecessor workflow when this instance was
	// created via continue-as-new.
	ContinuedFrom *string `json:"continued_from,omitempty"`
	// ContinuedTo links to the successor workflow after continue-as-new.
	ContinuedTo *string `json:"continued_to,omitempty"`

	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
}

// WorkflowTaskState tracks the state of one task within a workflow instance.
type WorkflowTaskState struct {
	Name       string     `json:"name"`
	JobID      string     `json:"job_id,omitempty"`
	Status     JobStatus  `json:"status"`
	Error      *string    `json:"error,omitempty"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	// ChildWorkflowID is set when this task spawned a child workflow.
	ChildWorkflowID string `json:"child_workflow_id,omitempty"`
	// ConditionRoute is set when a condition node completes, indicating
	// which route index was selected by the handler.
	ConditionRoute *uint `json:"condition_route,omitempty"`
	// SubflowTasks lists the task names that were dynamically injected
	// by a subflow generator node.
	SubflowTasks []string `json:"subflow_tasks,omitempty"`
	// Skipped indicates the task was skipped due to a failed precondition.
	Skipped    bool   `json:"skipped,omitempty"`
	SkipReason string `json:"skip_reason,omitempty"`
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

// --- Workflow Signal Types ---

// WorkflowSignal is an external signal sent to a running workflow.
type WorkflowSignal struct {
	Name       string          `json:"name"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	SentAt     time.Time       `json:"sent_at"`
	WorkflowID string          `json:"workflow_id"`

	// DedupeKey, if non-empty, prevents duplicate signals with the same key
	// from being stored. The dedup check uses a SET NX with TTL.
	DedupeKey string `json:"dedupe_key,omitempty"`

	// StreamID is the Redis stream entry ID. Populated by ReadSignals on read,
	// used by AckSignals to delete processed entries. Not persisted in the
	// stream payload itself.
	StreamID string `json:"-"`
}

// SignalOption is a functional option for configuring signal delivery.
type SignalOption func(*WorkflowSignal)

// WithDedupeKey sets a dedup key on the signal to prevent duplicate delivery.
func WithDedupeKey(key string) SignalOption {
	return func(s *WorkflowSignal) {
		s.DedupeKey = key
	}
}

// BatchProgress is published as an event for progress tracking.
type BatchProgress struct {
	BatchID string `json:"batch_id"`
	Done    int    `json:"done"`
	Total   int    `json:"total"`
	Failed  int    `json:"failed"`
	Step    string `json:"step,omitempty"`
}
