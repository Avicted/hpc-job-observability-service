package storage

import (
	"context"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/domain"
)

// JobStore defines the interface for job persistence operations.
type JobStore interface {
	// CreateJob creates a new job. Returns ErrConflict if job ID already exists.
	CreateJob(ctx context.Context, job *domain.Job) error

	// GetJob retrieves a job by ID. Returns ErrNotFound if job doesn't exist.
	GetJob(ctx context.Context, id string) (*domain.Job, error)

	// UpdateJob updates an existing job. Returns ErrNotFound if job doesn't exist.
	// Returns ErrJobTerminal if attempting to modify a job in a terminal state.
	UpdateJob(ctx context.Context, job *domain.Job) error

	// UpsertJob creates or updates a job. Returns ErrJobTerminal if attempting
	// to modify a job in a terminal state.
	UpsertJob(ctx context.Context, job *domain.Job) error

	// DeleteJob deletes a job by ID. Returns ErrNotFound if job doesn't exist.
	DeleteJob(ctx context.Context, id string) error

	// ListJobs retrieves jobs matching the filter criteria.
	// Returns jobs and total count (before pagination).
	ListJobs(ctx context.Context, filter domain.JobFilter) ([]*domain.Job, int, error)

	// GetAllJobs retrieves all jobs without pagination.
	GetAllJobs(ctx context.Context) ([]*domain.Job, error)
}

// MetricStore defines the interface for metric persistence operations.
type MetricStore interface {
	// RecordMetrics records a single metric sample for a job.
	RecordMetrics(ctx context.Context, sample *domain.MetricSample) error

	// RecordMetricsBatch efficiently records multiple metric samples.
	RecordMetricsBatch(ctx context.Context, samples []*domain.MetricSample) error

	// GetJobMetrics retrieves metrics for a job with optional time filtering.
	// Returns metrics and total count (before pagination).
	GetJobMetrics(ctx context.Context, jobID string, filter domain.MetricsFilter) ([]*domain.MetricSample, int, error)

	// GetLatestMetrics retrieves the most recent metric sample for a job.
	// Returns ErrNotFound if no metrics exist for the job.
	GetLatestMetrics(ctx context.Context, jobID string) (*domain.MetricSample, error)

	// DeleteMetricsBefore deletes all metrics older than the cutoff time.
	DeleteMetricsBefore(ctx context.Context, cutoff time.Time) error
}

// AuditStore defines the interface for audit event persistence.
type AuditStore interface {
	// RecordAuditEvent records an audit event for a job change.
	// This is typically called within the same transaction as the job mutation.
	RecordAuditEvent(ctx context.Context, event *domain.JobAuditEvent) error
}

// Tx represents a transaction-scoped store with access to all sub-stores.
// All operations within the same Tx share the same database transaction.
type Tx interface {
	Jobs() JobStore
	Metrics() MetricStore
	Audit() AuditStore
}

// Store is the root storage interface providing access to all persistence operations.
type Store interface {
	JobStore
	MetricStore
	AuditStore

	// WithTx executes fn within a database transaction.
	// If fn returns an error, the transaction is rolled back.
	// If fn returns nil, the transaction is committed.
	// The Tx passed to fn provides transaction-scoped access to all stores.
	WithTx(ctx context.Context, fn func(Tx) error) error

	// Migrate runs database schema migrations.
	Migrate(ctx context.Context) error

	// SeedDemoData populates the database with demo data for development/testing.
	SeedDemoData(ctx context.Context) error

	// Close releases all resources associated with the store.
	Close() error
}
