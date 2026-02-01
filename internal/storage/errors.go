package storage

import "errors"

// Typed errors for storage operations.
// These errors abstract database-specific errors and provide
// meaningful error types for the service layer.

var (
	// ErrNotFound indicates the requested resource does not exist.
	ErrNotFound = errors.New("resource not found")

	// ErrConflict indicates a resource with the same identifier already exists.
	ErrConflict = errors.New("resource already exists")

	// ErrJobTerminal indicates an attempt to modify a job in a terminal state.
	// Terminal states (completed, failed, cancelled) are immutable.
	ErrJobTerminal = errors.New("job is in terminal state and cannot be modified")

	// ErrInvalidInput indicates invalid input data or constraint violation.
	ErrInvalidInput = errors.New("invalid input or constraint violation")
)
