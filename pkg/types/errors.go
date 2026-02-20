package types

import (
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
