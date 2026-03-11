package types

import "time"

// EventType identifies the kind of event published on the DUREQ_EVENTS stream.
type EventType string

const (
	EventJobEnqueued   EventType = "job.enqueued"
	EventJobScheduled  EventType = "job.scheduled"
	EventJobDispatched EventType = "job.dispatched"
	EventJobStarted    EventType = "job.started"
	EventJobCompleted  EventType = "job.completed"
	EventJobFailed     EventType = "job.failed"
	EventJobRetrying   EventType = "job.retrying"
	EventJobDead       EventType = "job.dead"
	EventJobCancelled  EventType = "job.cancelled"
	EventJobPaused     EventType = "job.paused"
	EventJobResumed    EventType = "job.resumed"

	EventNodeJoined    EventType = "node.joined"
	EventNodeLeft      EventType = "node.left"
	EventLeaderElected EventType = "leader.elected"
	EventLeaderLost    EventType = "leader.lost"

	EventScheduleCreated EventType = "schedule.created"
	EventScheduleRemoved EventType = "schedule.removed"

	EventWorkflowStarted        EventType = "workflow.started"
	EventWorkflowCompleted      EventType = "workflow.completed"
	EventWorkflowFailed         EventType = "workflow.failed"
	EventWorkflowCancelled      EventType = "workflow.cancelled"
	EventWorkflowTaskDispatched EventType = "workflow.task.dispatched"
	EventWorkflowTaskCompleted  EventType = "workflow.task.completed"
	EventWorkflowTaskFailed     EventType = "workflow.task.failed"

	EventBatchStarted          EventType = "batch.started"
	EventBatchCompleted        EventType = "batch.completed"
	EventBatchFailed           EventType = "batch.failed"
	EventBatchCancelled        EventType = "batch.cancelled"
	EventBatchOnetimeCompleted EventType = "batch.onetime.completed"
	EventBatchOnetimeFailed    EventType = "batch.onetime.failed"
	EventBatchItemCompleted    EventType = "batch.item.completed"
	EventBatchItemFailed       EventType = "batch.item.failed"
	EventBatchProgress         EventType = "batch.progress"

	EventWorkflowTimedOut          EventType = "workflow.timed_out"
	EventWorkflowRetrying          EventType = "workflow.retrying"
	EventBatchTimedOut             EventType = "batch.timed_out"
	EventBatchRetrying             EventType = "batch.retrying"
	EventJobScheduleToStartTimeout EventType = "job.schedule_to_start_timeout"
	EventNodeCrashDetected         EventType = "node.crash_detected"
	EventJobAutoRecovered          EventType = "job.auto_recovered"

	EventWorkflowSignalReceived    EventType = "workflow.signal.received"
	EventWorkflowTaskPanicked      EventType = "workflow.task.panicked"

	EventWorkflowSuspended       EventType = "workflow.suspended"
	EventWorkflowResumed         EventType = "workflow.resumed"

	EventWorkflowHookDispatched  EventType = "workflow.hook.dispatched"
	EventWorkflowHookCompleted   EventType = "workflow.hook.completed"
	EventWorkflowHookFailed      EventType = "workflow.hook.failed"
)

// JobEvent is the envelope published to the DUREQ_EVENTS stream.
type JobEvent struct {
	Type           EventType      `json:"type"`
	JobID          string         `json:"job_id,omitempty"`
	RunID          string         `json:"run_id,omitempty"`
	NodeID         string         `json:"node_id,omitempty"`
	TaskType       TaskType       `json:"task_type,omitempty"`
	Error          *string        `json:"error,omitempty"`
	Attempt        int            `json:"attempt,omitempty"`
	Timestamp      time.Time      `json:"timestamp"`
	BatchProgress  *BatchProgress `json:"batch_progress,omitempty"`
	AffectedRunIDs []string       `json:"affected_run_ids,omitempty"`
	// WorkflowID is set for task-level workflow events so they can be indexed
	// under the parent workflow rather than the task's job ID.
	WorkflowID string `json:"workflow_id,omitempty"`
}
