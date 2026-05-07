package domain

import (
	"errors"
	"fmt"
)

// UnsupportedSchemaError signals a permanent rejection: the message's
// schema_version is not one this aggregator knows how to handle.
// Workers must not delete it — SQS DLQs after maxReceiveCount.
type UnsupportedSchemaError struct {
	Got  int
	Want int
}

func (e *UnsupportedSchemaError) Error() string {
	return fmt.Sprintf("unsupported schema_version %d (want %d)", e.Got, e.Want)
}

// IsUnsupportedSchema reports whether err is an UnsupportedSchemaError.
func IsUnsupportedSchema(err error) bool {
	var u *UnsupportedSchemaError
	return errors.As(err, &u)
}

// AlreadyProcessedError is an internal signal from the repository to
// the use case meaning "this event id was already persisted; the
// transaction was a no-op". It is not returned upward — the use case
// turns it into a successful (deletable) outcome and just logs.
type AlreadyProcessedError struct {
	EventID string
}

func (e *AlreadyProcessedError) Error() string {
	return fmt.Sprintf("event %s already processed", e.EventID)
}

func IsAlreadyProcessed(err error) bool {
	var a *AlreadyProcessedError
	return errors.As(err, &a)
}
