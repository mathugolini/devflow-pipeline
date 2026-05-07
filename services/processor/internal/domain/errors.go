package domain

import "fmt"

// ValidationError signals a permanent rejection of an event.
// The worker layer must NOT retry these — the message stays in the
// queue and is redriven to the DLQ by SQS after maxReceiveCount.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation failed on %q: %s", e.Field, e.Reason)
}

func newValidationError(field, reason string) *ValidationError {
	return &ValidationError{Field: field, Reason: reason}
}

// IsValidation reports whether err is a ValidationError.
func IsValidation(err error) bool {
	_, ok := err.(*ValidationError)
	return ok
}
