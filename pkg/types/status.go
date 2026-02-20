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
