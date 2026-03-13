package types

import (
	"encoding/json"
	"errors"
	"time"
)

// Sentinel errors.
var (
	ErrJobNotFound      = errors.New("dureq: job not found")
	ErrScheduleNotFound = errors.New("dureq: schedule not found")
	ErrRunNotFound      = errors.New("dureq: run not found")
	ErrDuplicateJob     = errors.New("dureq: duplicate unique key")
	ErrHandlerNotFound  = errors.New("dureq: handler not registered for task type")
	ErrNotLeader        = errors.New("dureq: not the leader node")
	ErrLockFailed       = errors.New("dureq: failed to acquire lock")
	ErrLockNotHeld      = errors.New("dureq: lock not held")
	ErrServerStopped    = errors.New("dureq: server stopped")
	ErrInvalidSchedule  = errors.New("dureq: invalid schedule")
	ErrInvalidJobStatus = errors.New("dureq: invalid job status transition")
	ErrRedisNotConnected = errors.New("dureq: Redis not connected")
	ErrWorkflowNotFound = errors.New("dureq: workflow not found")
	ErrCyclicDependency = errors.New("dureq: workflow has cyclic dependencies")
	ErrBatchNotFound          = errors.New("dureq: batch not found")
	ErrExecutionTimedOut      = errors.New("dureq: execution timed out")
	ErrScheduleToStartTimeout = errors.New("dureq: schedule-to-start timeout exceeded")
	ErrNoProgressReporter     = errors.New("dureq: no progress reporter in context (called outside handler?)")
	ErrWorkflowTerminal       = errors.New("dureq: workflow is in terminal state, cannot receive signals")
	ErrSignalDuplicate        = errors.New("dureq: duplicate signal (dedup key already exists)")
)

// ErrorClassification categorizes errors for retry decisions.
type ErrorClassification int

const (
	ErrorClassRetryable    ErrorClassification = iota // transient, should retry
	ErrorClassNonRetryable                            // permanent, do not retry
	ErrorClassRateLimited                             // retry after specific delay
)

// RetryableError wraps an error as retryable.
type RetryableError struct{ Err error }

func (e *RetryableError) Error() string { return e.Err.Error() }
func (e *RetryableError) Unwrap() error { return e.Err }

// NonRetryableError wraps an error as non-retryable.
type NonRetryableError struct{ Err error }

func (e *NonRetryableError) Error() string { return e.Err.Error() }
func (e *NonRetryableError) Unwrap() error { return e.Err }

// RateLimitedError wraps an error with a retry-after duration.
type RateLimitedError struct {
	Err        error
	RetryAfter time.Duration
}

func (e *RateLimitedError) Error() string { return e.Err.Error() }
func (e *RateLimitedError) Unwrap() error { return e.Err }

// ClassifyError determines the retry classification of an error.
func ClassifyError(err error) ErrorClassification {
	if err == nil {
		return ErrorClassRetryable
	}

	var rateLimited *RateLimitedError
	if errors.As(err, &rateLimited) {
		return ErrorClassRateLimited
	}

	var nonRetryable *NonRetryableError
	if errors.As(err, &nonRetryable) {
		return ErrorClassNonRetryable
	}

	var retryable *RetryableError
	if errors.As(err, &retryable) {
		return ErrorClassRetryable
	}

	// Default: treat unknown errors as retryable
	return ErrorClassRetryable
}

// GetErrorClassString extracts the error string from an ErrorClassification.
func GetErrorClassString(errClass ErrorClassification) string {
	switch errClass {
	case ErrorClassRetryable:
		return "retryable"
	case ErrorClassNonRetryable:
		return "non-retryable"
	case ErrorClassRateLimited:
		return "rate-limited"
	case ErrorClassSkip:
		return "skip"
	case ErrorClassRepeat:
		return "repeat"
	case ErrorClassPause:
		return "pause"
	case ErrorClassContinueAsNew:
		return "continue-as-new"
	}
	return "unknown"
}

// GetRetryAfter extracts the RetryAfter duration from a RateLimitedError.
func GetRetryAfter(err error) time.Duration {
	var rateLimited *RateLimitedError
	if errors.As(err, &rateLimited) {
		return rateLimited.RetryAfter
	}
	return 0
}

// --- Rich handler control flow errors ---

// SkipError signals that the handler wants to skip this execution
// without changing the job status. Useful when the handler determines
// that preconditions are not met and the job should simply be ignored
// for this run.
type SkipError struct {
	Reason string
}

func (e *SkipError) Error() string {
	if e.Reason != "" {
		return "skip: " + e.Reason
	}
	return "skip"
}

// RepeatError signals that the handler wants to save current progress
// and immediately re-enqueue the job for another execution. This is
// useful for long-running tasks that want to checkpoint periodically.
type RepeatError struct {
	Err   error
	Delay time.Duration // optional delay before re-execution
}

func (e *RepeatError) Error() string {
	if e.Err != nil {
		return "repeat: " + e.Err.Error()
	}
	return "repeat"
}

func (e *RepeatError) Unwrap() error { return e.Err }

// PauseError signals that the handler wants to pause the job.
// The job transitions to paused status and can be resumed manually
// or auto-resumed after RetryAfter duration.
type PauseError struct {
	Reason     string
	RetryAfter time.Duration // 0 = manual resume only
}

func (e *PauseError) Error() string {
	if e.Reason != "" {
		return "pause: " + e.Reason
	}
	return "pause"
}

// ClassifyControlFlow returns the extended classification including
// control flow errors. Returns ErrorClassRetryable for standard errors.
func ClassifyControlFlow(err error) ErrorClassification {
	if err == nil {
		return ErrorClassRetryable
	}

	var skipErr *SkipError
	if errors.As(err, &skipErr) {
		return ErrorClassSkip
	}

	var repeatErr *RepeatError
	if errors.As(err, &repeatErr) {
		return ErrorClassRepeat
	}

	var pauseErr *PauseError
	if errors.As(err, &pauseErr) {
		return ErrorClassPause
	}

	var contErr *ContinueAsNewError
	if errors.As(err, &contErr) {
		return ErrorClassContinueAsNew
	}

	return ClassifyError(err)
}

const (
	ErrorClassSkip          ErrorClassification = iota + 10 // skip execution, no state change
	ErrorClassRepeat                                         // save and re-enqueue immediately
	ErrorClassPause                                          // pause the job
	ErrorClassContinueAsNew                                  // complete and spawn new workflow instance
)

// ContinueAsNewError signals that a workflow should complete with status
// "continued" and a new instance should be created with the provided input.
// This is used for long-running workflows that need to reset their state
// periodically to prevent unbounded growth.
type ContinueAsNewError struct {
	Input json.RawMessage
}

func (e *ContinueAsNewError) Error() string {
	return "continue-as-new"
}
