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

// IsPaused returns true if the job is in the paused state.
func (s JobStatus) IsPaused() bool {
	return s == JobStatusPaused
}

// --- State Transition Graph ---

// jobTransitions defines the set of valid state transitions for a job.
var jobTransitions = map[JobStatus][]JobStatus{
	JobStatusPending:   {JobStatusScheduled, JobStatusRunning, JobStatusCancelled},
	JobStatusScheduled: {JobStatusRunning, JobStatusCancelled},
	JobStatusRunning:   {JobStatusCompleted, JobStatusFailed, JobStatusRetrying, JobStatusPaused, JobStatusDead, JobStatusCancelled},
	JobStatusRetrying:  {JobStatusRunning, JobStatusPaused, JobStatusDead, JobStatusCancelled},
	JobStatusPaused:    {JobStatusRetrying, JobStatusRunning, JobStatusDead, JobStatusCancelled},
	JobStatusFailed:    {JobStatusRetrying, JobStatusPaused, JobStatusDead, JobStatusScheduled, JobStatusCancelled},
	JobStatusCompleted: {JobStatusScheduled}, // recurring jobs go back to scheduled
	JobStatusDead:      {JobStatusRetrying},  // manual retry from dead
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
