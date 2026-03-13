package types

// JobStatus represents the lifecycle state of a job definition.
type JobStatus string

const (
	JobStatusPending   JobStatus = "pending"
	JobStatusScheduled JobStatus = "scheduled"
	JobStatusRunning   JobStatus = "running"
	JobStatusCompleted JobStatus = "completed"
	JobStatusFailed    JobStatus = "failed"
	JobStatusRetrying  JobStatus = "retrying"
	JobStatusPaused    JobStatus = "paused"
	JobStatusDead      JobStatus = "dead"
	JobStatusCancelled JobStatus = "cancelled"
)

func (s JobStatus) IsTerminal() bool {
	switch s {
	case JobStatusCompleted, JobStatusDead, JobStatusCancelled:
		return true
	}
	return false
}

// IsRetryable returns true if the job is in a state that allows retry.
// Unlike IsTerminal, this excludes Completed — only failed/dead/cancelled jobs
// should be retried to prevent accidental re-execution of successful work.
func (s JobStatus) IsRetryable() bool {
	switch s {
	case JobStatusFailed, JobStatusDead, JobStatusCancelled:
		return true
	}
	return false
}

// IsPaused returns true if the job is in the paused state.
func (s JobStatus) IsPaused() bool {
	return s == JobStatusPaused
}

// --- State Transition Graph ---

// jobTransitions defines the set of valid state transitions for a job.
//
// Runtime paths that use these transitions:
//   - scheduler.dispatchFiring: completed/scheduled/failed → pending
//   - worker.handleSuccess: pending/running → scheduled (recurring), pending/running → completed
//   - worker.handleFailure: pending/running → failed/retrying/dead (worker skips job-level running state)
//   - client.Retry: dead/completed → pending (manual retry)
//   - scheduler.markEndOfSchedule: scheduled → completed
var jobTransitions = map[JobStatus][]JobStatus{
	JobStatusPending:   {JobStatusScheduled, JobStatusRunning, JobStatusCompleted, JobStatusFailed, JobStatusRetrying, JobStatusDead, JobStatusCancelled},
	JobStatusScheduled: {JobStatusRunning, JobStatusCompleted, JobStatusPending, JobStatusCancelled},
	JobStatusRunning:   {JobStatusCompleted, JobStatusFailed, JobStatusRetrying, JobStatusPaused, JobStatusDead, JobStatusScheduled, JobStatusCancelled},
	JobStatusRetrying:  {JobStatusRunning, JobStatusPaused, JobStatusDead, JobStatusCancelled},
	JobStatusPaused:    {JobStatusRetrying, JobStatusRunning, JobStatusDead, JobStatusCancelled},
	JobStatusFailed:    {JobStatusRetrying, JobStatusPaused, JobStatusDead, JobStatusScheduled, JobStatusPending, JobStatusCancelled},
	JobStatusCompleted: {JobStatusScheduled, JobStatusPending}, // recurring: scheduled, manual retry: pending
	JobStatusDead:      {JobStatusRetrying, JobStatusPending},  // manual retry from dead
	JobStatusCancelled: {},
}

// runTransitions defines the set of valid state transitions for a run.
var runTransitions = map[RunStatus][]RunStatus{
	RunStatusClaimed:   {RunStatusRunning, RunStatusFailed, RunStatusCancelled},
	RunStatusRunning:   {RunStatusSucceeded, RunStatusFailed, RunStatusTimedOut, RunStatusCancelled},
	RunStatusSucceeded: {},
	RunStatusFailed:    {},
	RunStatusTimedOut:  {},
	RunStatusCancelled: {},
}

// ValidJobTransition returns true if transitioning from → to is allowed.
func ValidJobTransition(from, to JobStatus) bool {
	targets, ok := jobTransitions[from]
	if !ok {
		return false
	}
	for _, t := range targets {
		if t == to {
			return true
		}
	}
	return false
}

// ValidRunTransition returns true if transitioning from → to is allowed.
func ValidRunTransition(from, to RunStatus) bool {
	targets, ok := runTransitions[from]
	if !ok {
		return false
	}
	for _, t := range targets {
		if t == to {
			return true
		}
	}
	return false
}

func (s JobStatus) String() string { return string(s) }

// RunStatus represents the state of an individual job execution.
type RunStatus string

const (
	RunStatusClaimed   RunStatus = "claimed"
	RunStatusRunning   RunStatus = "running"
	RunStatusSucceeded RunStatus = "succeeded"
	RunStatusFailed    RunStatus = "failed"
	RunStatusTimedOut  RunStatus = "timed_out"
	RunStatusCancelled RunStatus = "cancelled"
)

func (s RunStatus) IsTerminal() bool {
	switch s {
	case RunStatusSucceeded, RunStatusFailed, RunStatusTimedOut, RunStatusCancelled:
		return true
	}
	return false
}

func (s RunStatus) String() string { return string(s) }

// --- Workflow State Transition Graph ---

var workflowTransitions = map[WorkflowStatus][]WorkflowStatus{
	WorkflowStatusPending:   {WorkflowStatusRunning, WorkflowStatusCancelled},
	WorkflowStatusRunning:   {WorkflowStatusCompleted, WorkflowStatusFailed, WorkflowStatusCancelled, WorkflowStatusSuspended, WorkflowStatusContinued},
	WorkflowStatusSuspended: {WorkflowStatusRunning, WorkflowStatusCancelled},
	WorkflowStatusCompleted: {},
	WorkflowStatusFailed:    {WorkflowStatusRunning}, // retry
	WorkflowStatusCancelled: {},
	WorkflowStatusContinued: {},                       // terminal: replaced by new instance
}

// ValidWorkflowTransition returns true if transitioning from → to is allowed.
func ValidWorkflowTransition(from, to WorkflowStatus) bool {
	targets, ok := workflowTransitions[from]
	if !ok {
		return false
	}
	for _, t := range targets {
		if t == to {
			return true
		}
	}
	return false
}
