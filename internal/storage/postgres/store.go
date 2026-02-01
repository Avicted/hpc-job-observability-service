package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/domain"
	"github.com/Avicted/hpc-job-observability-service/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// DBTX is an interface satisfied by both *pgxpool.Pool and pgx.Tx.
// This allows storage methods to work transparently with both pooled connections
// and transactions without exposing pgx types outside the storage layer.
type DBTX interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// Store implements the storage.Store interface using PostgreSQL with pgx.
type Store struct {
	pool *pgxpool.Pool

	// Prometheus metrics for observability
	opDuration *prometheus.HistogramVec
	opErrors   *prometheus.CounterVec
}

// Config contains configuration for the PostgreSQL store.
type Config struct {
	// DSN is the PostgreSQL connection string (e.g., "postgres://user:pass@host:port/dbname")
	DSN string

	// MaxConns is the maximum number of concurrent connections in the pool.
	// Default: 25 (matching previous database/sql configuration)
	MaxConns int32

	// MinConns is the minimum number of idle connections in the pool.
	// Default: 5 (matching previous database/sql configuration)
	MinConns int32

	// MaxConnLifetime is the maximum duration a connection can be reused.
	// Default: 5 minutes (matching previous database/sql configuration)
	MaxConnLifetime time.Duration

	// MaxConnIdleTime is the maximum duration a connection can be idle.
	// Default: 30 seconds
	MaxConnIdleTime time.Duration
}

// DefaultConfig returns a Config with sensible defaults matching the previous
// database/sql configuration.
func DefaultConfig(dsn string) *Config {
	return &Config{
		DSN:             dsn,
		MaxConns:        25,
		MinConns:        5,
		MaxConnLifetime: 5 * time.Minute,
		MaxConnIdleTime: 30 * time.Second,
	}
}

// NewStore creates a new PostgreSQL store with pgxpool.
// Prometheus metrics must be registered externally using GetMetrics().
func NewStore(ctx context.Context, cfg *Config) (*Store, error) {
	if cfg == nil {
		return nil, errors.New("config is required")
	}

	// Parse DSN and configure pool
	poolConfig, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DSN: %w", err)
	}

	// Apply explicit pool configuration
	poolConfig.MaxConns = cfg.MaxConns
	poolConfig.MinConns = cfg.MinConns
	poolConfig.MaxConnLifetime = cfg.MaxConnLifetime
	poolConfig.MaxConnIdleTime = cfg.MaxConnIdleTime

	// Create pool
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Create Prometheus metrics (registered externally)
	opDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "storage_operation_duration_seconds",
			Help:    "Duration of storage operations in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation", "success"},
	)

	opErrors := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "storage_operation_errors_total",
			Help: "Total count of storage operation errors",
		},
		[]string{"operation", "error_type"},
	)

	return &Store{
		pool:       pool,
		opDuration: opDuration,
		opErrors:   opErrors,
	}, nil
}

// GetMetrics returns Prometheus collectors to be registered in the main application.
// This must be called and registered before using the store.
func (s *Store) GetMetrics() []prometheus.Collector {
	return []prometheus.Collector{s.opDuration, s.opErrors}
}

// Close releases all resources associated with the store.
func (s *Store) Close() error {
	s.pool.Close()
	return nil
}

// observe records metrics for a storage operation.
// Use with defer pattern:
//
//	defer s.observe(operation, time.Now(), &err)
func (s *Store) observe(operation string, start time.Time, err *error) {
	duration := time.Since(start).Seconds()
	success := "true"
	errorType := "none"

	if err != nil && *err != nil {
		success = "false"
		errorType = classifyError(*err)
		s.opErrors.WithLabelValues(operation, errorType).Inc()
	}

	s.opDuration.WithLabelValues(operation, success).Observe(duration)
}

// classifyError maps storage errors to metric labels.
func classifyError(err error) string {
	switch {
	case errors.Is(err, storage.ErrNotFound):
		return "not_found"
	case errors.Is(err, storage.ErrConflict):
		return "conflict"
	case errors.Is(err, storage.ErrJobTerminal):
		return "job_terminal"
	case errors.Is(err, storage.ErrInvalidInput):
		return "invalid_input"
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "context_deadline"
	default:
		return "unknown"
	}
}

// tx is a transaction-scoped store that implements storage.Tx.
type tx struct {
	pgxTx pgx.Tx
	store *Store
}

// Jobs returns a transaction-scoped JobStore.
func (t *tx) Jobs() storage.JobStore {
	return &jobStore{db: t.pgxTx, store: t.store}
}

// Metrics returns a transaction-scoped MetricStore.
func (t *tx) Metrics() storage.MetricStore {
	return &metricStore{db: t.pgxTx, store: t.store}
}

// Audit returns a transaction-scoped AuditStore.
func (t *tx) Audit() storage.AuditStore {
	return &auditStore{db: t.pgxTx, store: t.store}
}

// WithTx executes fn within a database transaction.
// If fn returns an error, the transaction is rolled back.
// If fn returns nil, the transaction is committed.
func (s *Store) WithTx(ctx context.Context, fn func(storage.Tx) error) (err error) {
	defer s.observe("with_tx", time.Now(), &err)

	pgxTx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Ensure rollback on panic or error
	defer func() {
		if p := recover(); p != nil {
			_ = pgxTx.Rollback(ctx)
			panic(p) // Re-throw panic after rollback
		} else if err != nil {
			_ = pgxTx.Rollback(ctx)
		}
	}()

	// Execute function with transaction-scoped store
	txStore := &tx{pgxTx: pgxTx, store: s}
	if err = fn(txStore); err != nil {
		return err
	}

	// Commit transaction
	if err = pgxTx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Jobs returns the root-level JobStore using pooled connections.
func (s *Store) Jobs() storage.JobStore {
	return &jobStore{db: s.pool, store: s}
}

// Metrics returns the root-level MetricStore using pooled connections.
func (s *Store) Metrics() storage.MetricStore {
	return &metricStore{db: s.pool, store: s}
}

// Audit returns the root-level AuditStore using pooled connections.
func (s *Store) Audit() storage.AuditStore {
	return &auditStore{db: s.pool, store: s}
}

// Implement storage.Store interface methods that delegate to sub-stores.
// These allow calling s.CreateJob() instead of s.Jobs().CreateJob().

func (s *Store) CreateJob(ctx context.Context, job *domain.Job) error {
	return s.Jobs().CreateJob(ctx, job)
}

func (s *Store) GetJob(ctx context.Context, id string) (*domain.Job, error) {
	return s.Jobs().GetJob(ctx, id)
}

func (s *Store) UpdateJob(ctx context.Context, job *domain.Job) error {
	return s.Jobs().UpdateJob(ctx, job)
}

func (s *Store) UpsertJob(ctx context.Context, job *domain.Job) error {
	return s.Jobs().UpsertJob(ctx, job)
}

func (s *Store) DeleteJob(ctx context.Context, id string) error {
	return s.Jobs().DeleteJob(ctx, id)
}

func (s *Store) ListJobs(ctx context.Context, filter domain.JobFilter) ([]*domain.Job, int, error) {
	return s.Jobs().ListJobs(ctx, filter)
}

func (s *Store) GetAllJobs(ctx context.Context) ([]*domain.Job, error) {
	return s.Jobs().GetAllJobs(ctx)
}

func (s *Store) RecordMetrics(ctx context.Context, sample *domain.MetricSample) error {
	return s.Metrics().RecordMetrics(ctx, sample)
}

func (s *Store) RecordMetricsBatch(ctx context.Context, samples []*domain.MetricSample) error {
	return s.Metrics().RecordMetricsBatch(ctx, samples)
}

func (s *Store) GetJobMetrics(ctx context.Context, jobID string, filter domain.MetricsFilter) ([]*domain.MetricSample, int, error) {
	return s.Metrics().GetJobMetrics(ctx, jobID, filter)
}

func (s *Store) GetLatestMetrics(ctx context.Context, jobID string) (*domain.MetricSample, error) {
	return s.Metrics().GetLatestMetrics(ctx, jobID)
}

func (s *Store) DeleteMetricsBefore(ctx context.Context, cutoff time.Time) error {
	return s.Metrics().DeleteMetricsBefore(ctx, cutoff)
}

func (s *Store) RecordAuditEvent(ctx context.Context, event *domain.JobAuditEvent) error {
	return s.Audit().RecordAuditEvent(ctx, event)
}

// Migrate runs database schema migrations.
func (s *Store) Migrate(ctx context.Context) error {
	schemas := []string{
		schemaJobs,
		schemaJobsIndexes,
		schemaMetrics,
		schemaMetricsIndexes,
		schemaAudit,
		schemaAuditIndexes,
	}

	for _, schema := range schemas {
		if _, err := s.pool.Exec(ctx, schema); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
	}

	return nil
}
