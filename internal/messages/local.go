package messages

import (
	"encoding/json"
	"time"

	"github.com/FDK0901/dureq/pkg/types"
	"github.com/anthdm/hollywood/actor"
)

// TickMsg is a periodic tick for the SchedulerActor.
type TickMsg struct{}

// OrphanCheckMsg triggers orphan run detection.
type OrphanCheckMsg struct{}

// TimeoutCheckMsg triggers workflow/batch timeout checks.
type TimeoutCheckMsg struct{}

// StaleCheckMsg triggers stale node capacity checks.
type StaleCheckMsg struct{}

// HeartbeatMsg triggers a periodic heartbeat.
type HeartbeatMsg struct{}

// CapacityTickMsg triggers a capacity report.
type CapacityTickMsg struct{}

// DrainMsg is a shutdown drain signal.
type DrainMsg struct{}

// WorkerDoneMsg is the completion report from a child worker actor.
type WorkerDoneMsg struct {
	RunID      string          `json:"run_id"`
	JobID      string          `json:"job_id"`
	TaskType   string          `json:"task_type"`
	Success    bool            `json:"success"`
	Error      string          `json:"error,omitempty"`
	Output     json.RawMessage `json:"output,omitempty"`
	Attempt    int             `json:"attempt"`
	StartedAt  time.Time       `json:"started_at"`
	FinishedAt time.Time       `json:"finished_at"`
	WorkflowID string          `json:"workflow_id,omitempty"`
	BatchID    string          `json:"batch_id,omitempty"`
	NodeID     string          `json:"node_id,omitempty"`

	// Run carries the complete terminal run record for batched completion.
	// Local-only; not serialized across node boundaries.
	Run *types.JobRun `json:"-"`
}

// ExecutionDoneMsg is the result from a handler goroutine.
type ExecutionDoneMsg struct {
	Success bool            `json:"success"`
	Error   string          `json:"error,omitempty"`
	Output  json.RawMessage `json:"output,omitempty"`
}

// NewJobFromRedisMsg is delivered from the Redis Pub/Sub subscriber goroutine.
type NewJobFromRedisMsg struct {
	JobID    string `json:"job_id"`
	TaskType string `json:"task_type"`
	Priority int    `json:"priority"`
}

// WireDispatcherPIDMsg delivers the DispatcherActor PID to local actors
// after singleton activation.
type WireDispatcherPIDMsg struct {
	PID *actor.PID
}

// WireEventBridgePIDMsg delivers the EventBridgeActor PID to the
// WorkerSupervisorActor after local actors are spawned.
type WireEventBridgePIDMsg struct {
	PID *actor.PID
}

// WireOrchestratorPIDMsg delivers the OrchestratorActor PID to the
// EventBridgeActor after singleton activation.
type WireOrchestratorPIDMsg struct {
	PID *actor.PID
}
