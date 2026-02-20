package types

import (
	"errors"
	"fmt"
)

// PanicError wraps a recovered panic value as an error.
type PanicError struct {
	Value      interface{}
	Stacktrace string
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("panic: %v", e.Value)
}

// IsPanicError checks whether err (or any wrapped error) is a PanicError.
func IsPanicError(err error) bool {
	var pe *PanicError
	return errors.As(err, &pe)
}

// GetPanicError extracts the PanicError from an error chain, if present.
func GetPanicError(err error) *PanicError {
	var pe *PanicError
	if errors.As(err, &pe) {
		return pe
	}
	return nil
}
