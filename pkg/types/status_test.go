package types

import "testing"

func TestValidJobTransition_AllowedPaths(t *testing.T) {
	allowed := []struct {
		from, to JobStatus
	}{
		{JobStatusPending, JobStatusScheduled},
		{JobStatusPending, JobStatusRunning},
		{JobStatusPending, JobStatusCancelled},
		{JobStatusScheduled, JobStatusRunning},
		{JobStatusScheduled, JobStatusCancelled},
		{JobStatusRunning, JobStatusCompleted},
		{JobStatusRunning, JobStatusFailed},
		{JobStatusRunning, JobStatusRetrying},
		{JobStatusRunning, JobStatusPaused},
		{JobStatusRunning, JobStatusDead},
		{JobStatusRunning, JobStatusCancelled},
		{JobStatusRetrying, JobStatusRunning},
		{JobStatusRetrying, JobStatusPaused},
		{JobStatusRetrying, JobStatusDead},
		{JobStatusRetrying, JobStatusCancelled},
		{JobStatusPaused, JobStatusRetrying},
		{JobStatusPaused, JobStatusRunning},
		{JobStatusPaused, JobStatusDead},
		{JobStatusPaused, JobStatusCancelled},
		{JobStatusFailed, JobStatusRetrying},
		{JobStatusFailed, JobStatusPaused},
		{JobStatusFailed, JobStatusDead},
		{JobStatusFailed, JobStatusScheduled},
		{JobStatusFailed, JobStatusCancelled},
		{JobStatusFailed, JobStatusPending},
		{JobStatusCompleted, JobStatusScheduled},
		{JobStatusCompleted, JobStatusPending},
		{JobStatusDead, JobStatusRetrying},
		{JobStatusDead, JobStatusPending},
		{JobStatusScheduled, JobStatusCompleted},
		{JobStatusScheduled, JobStatusPending},
		{JobStatusRunning, JobStatusScheduled},
	}

	for _, tc := range allowed {
		if !ValidJobTransition(tc.from, tc.to) {
			t.Errorf("expected %s → %s to be allowed", tc.from, tc.to)
		}
	}
}

func TestValidJobTransition_BlockedPaths(t *testing.T) {
	blocked := []struct {
		from, to JobStatus
	}{
		{JobStatusCompleted, JobStatusRunning},
		{JobStatusCompleted, JobStatusFailed},
		{JobStatusCancelled, JobStatusRunning},
		{JobStatusCancelled, JobStatusPending},
		{JobStatusDead, JobStatusRunning},
		{JobStatusDead, JobStatusCompleted},
	}

	for _, tc := range blocked {
		if ValidJobTransition(tc.from, tc.to) {
			t.Errorf("expected %s → %s to be blocked", tc.from, tc.to)
		}
	}
}

func TestValidRunTransition_AllowedPaths(t *testing.T) {
	allowed := []struct {
		from, to RunStatus
	}{
		{RunStatusClaimed, RunStatusRunning},
		{RunStatusClaimed, RunStatusFailed},
		{RunStatusClaimed, RunStatusCancelled},
		{RunStatusRunning, RunStatusSucceeded},
		{RunStatusRunning, RunStatusFailed},
		{RunStatusRunning, RunStatusTimedOut},
		{RunStatusRunning, RunStatusCancelled},
	}

	for _, tc := range allowed {
		if !ValidRunTransition(tc.from, tc.to) {
			t.Errorf("expected %s → %s to be allowed", tc.from, tc.to)
		}
	}
}

func TestValidRunTransition_TerminalBlocked(t *testing.T) {
	terminals := []RunStatus{RunStatusSucceeded, RunStatusFailed, RunStatusTimedOut, RunStatusCancelled}
	targets := []RunStatus{RunStatusClaimed, RunStatusRunning, RunStatusSucceeded, RunStatusFailed}

	for _, from := range terminals {
		for _, to := range targets {
			if ValidRunTransition(from, to) {
				t.Errorf("expected terminal %s → %s to be blocked", from, to)
			}
		}
	}
}

func TestValidWorkflowTransition_AllowedPaths(t *testing.T) {
	allowed := []struct {
		from, to WorkflowStatus
	}{
		{WorkflowStatusPending, WorkflowStatusRunning},
		{WorkflowStatusPending, WorkflowStatusCancelled},
		{WorkflowStatusRunning, WorkflowStatusCompleted},
		{WorkflowStatusRunning, WorkflowStatusFailed},
		{WorkflowStatusRunning, WorkflowStatusCancelled},
		{WorkflowStatusRunning, WorkflowStatusSuspended},
		{WorkflowStatusSuspended, WorkflowStatusRunning},
		{WorkflowStatusSuspended, WorkflowStatusCancelled},
		{WorkflowStatusFailed, WorkflowStatusRunning},
	}

	for _, tc := range allowed {
		if !ValidWorkflowTransition(tc.from, tc.to) {
			t.Errorf("expected %s → %s to be allowed", tc.from, tc.to)
		}
	}
}

func TestValidWorkflowTransition_TerminalBlocked(t *testing.T) {
	blocked := []struct {
		from, to WorkflowStatus
	}{
		{WorkflowStatusCompleted, WorkflowStatusRunning},
		{WorkflowStatusCompleted, WorkflowStatusFailed},
		{WorkflowStatusCancelled, WorkflowStatusRunning},
		{WorkflowStatusCancelled, WorkflowStatusPending},
	}

	for _, tc := range blocked {
		if ValidWorkflowTransition(tc.from, tc.to) {
			t.Errorf("expected %s → %s to be blocked", tc.from, tc.to)
		}
	}
}

func TestJobStatus_IsTerminal(t *testing.T) {
	terminal := []JobStatus{JobStatusCompleted, JobStatusDead, JobStatusCancelled}
	nonTerminal := []JobStatus{JobStatusPending, JobStatusScheduled, JobStatusRunning, JobStatusFailed, JobStatusRetrying, JobStatusPaused}

	for _, s := range terminal {
		if !s.IsTerminal() {
			t.Errorf("expected %s to be terminal", s)
		}
	}
	for _, s := range nonTerminal {
		if s.IsTerminal() {
			t.Errorf("expected %s to be non-terminal", s)
		}
	}
}
