// Package lifecycle implements a job status state machine with validated
// transitions and optional hooks.
package lifecycle

import (
	"fmt"

	"github.com/FDK0901/dureq/pkg/types"
)

// transitions defines the set of valid state transitions for a job.
// The key is the current status; the value is the set of statuses it can move to.
var transitions = map[types.JobStatus]map[types.JobStatus]bool{
	types.JobStatusPending: {
		types.JobStatusRunning:   true,
		types.JobStatusCancelled: true,
		types.JobStatusScheduled: true,
	},
	types.JobStatusScheduled: {
		types.JobStatusPending:   true, // scheduler fires it
		types.JobStatusCancelled: true,
		types.JobStatusCompleted: true, // schedule expired
	},
	types.JobStatusRunning: {
		types.JobStatusCompleted: true,
		types.JobStatusFailed:    true,
		types.JobStatusRetrying:  true,
		types.JobStatusCancelled: true,
	},
	types.JobStatusFailed: {
		types.JobStatusPending:   true, // manual retry
		types.JobStatusDead:      true,
		types.JobStatusRetrying:  true,
		types.JobStatusCancelled: true,
	},
	types.JobStatusRetrying: {
		types.JobStatusPending:   true, // re-dispatched
		types.JobStatusRunning:   true,
		types.JobStatusDead:      true,
		types.JobStatusCancelled: true,
	},
	types.JobStatusDead: {
		types.JobStatusPending: true, // manual retry from dead
	},
	types.JobStatusCompleted: {
		// Terminal — no outgoing transitions except re-schedule.
		types.JobStatusScheduled: true, // recurring job re-scheduled
	},
	types.JobStatusCancelled: {
		// Terminal — no outgoing transitions.
	},
}

// CanTransition returns true if the transition from → to is valid.
func CanTransition(from, to types.JobStatus) bool {
	allowed, ok := transitions[from]
	if !ok {
		return false
	}
	return allowed[to]
}

// Validate returns an error if the transition from → to is not allowed.
func Validate(from, to types.JobStatus) error {
	if !CanTransition(from, to) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
	}
	return nil
}

// ErrInvalidTransition is returned when a status transition is not allowed.
var ErrInvalidTransition = fmt.Errorf("invalid status transition")

// Hook is called when a job transitions between states.
type Hook func(jobID string, from, to types.JobStatus)

// Machine enforces valid transitions and fires hooks.
type Machine struct {
	hooks []Hook
}

// New creates a new lifecycle state machine.
func New() *Machine {
	return &Machine{}
}

// OnTransition registers a hook that fires on every valid transition.
func (m *Machine) OnTransition(h Hook) {
	m.hooks = append(m.hooks, h)
}

// Transition validates the state change and fires hooks if valid.
// Returns ErrInvalidTransition if the transition is not allowed.
func (m *Machine) Transition(job *types.Job, to types.JobStatus) error {
	from := job.Status
	if err := Validate(from, to); err != nil {
		return err
	}

	job.Status = to

	for _, h := range m.hooks {
		h(job.ID, from, to)
	}

	return nil
}

// AllowedTransitions returns the set of statuses a job can move to from its current state.
func AllowedTransitions(from types.JobStatus) []types.JobStatus {
	allowed, ok := transitions[from]
	if !ok {
		return nil
	}
	out := make([]types.JobStatus, 0, len(allowed))
	for to := range allowed {
		out = append(out, to)
	}
	return out
}
