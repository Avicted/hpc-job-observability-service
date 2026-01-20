// Package storage provides PostgreSQL-specific storage implementation.
package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// PostgresStorage implements Storage interface using PostgreSQL.
type PostgresStorage struct {
	baseStorage
}

// NewPostgresStorage creates a new PostgreSQL storage instance.
func NewPostgresStorage(dsn string) (*PostgresStorage, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open PostgreSQL database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping PostgreSQL database: %w", err)
	}

	return &PostgresStorage{baseStorage: baseStorage{db: db}}, nil
}

// Migrate creates the database schema.
func (s *PostgresStorage) Migrate() error {
	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		id TEXT PRIMARY KEY,
		user_name TEXT NOT NULL,
		nodes TEXT NOT NULL,
		state TEXT NOT NULL DEFAULT 'pending',
		start_time TIMESTAMPTZ NOT NULL,
		end_time TIMESTAMPTZ,
		runtime_seconds DOUBLE PRECISION DEFAULT 0,
		cpu_usage DOUBLE PRECISION DEFAULT 0,
		memory_usage_mb BIGINT DEFAULT 0,
		gpu_usage DOUBLE PRECISION,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state);
	CREATE INDEX IF NOT EXISTS idx_jobs_user ON jobs(user_name);
	CREATE INDEX IF NOT EXISTS idx_jobs_start_time ON jobs(start_time);

	CREATE TABLE IF NOT EXISTS metric_samples (
		id BIGSERIAL PRIMARY KEY,
		job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
		timestamp TIMESTAMPTZ NOT NULL,
		cpu_usage DOUBLE PRECISION NOT NULL,
		memory_usage_mb BIGINT NOT NULL,
		gpu_usage DOUBLE PRECISION
	);

	CREATE INDEX IF NOT EXISTS idx_metrics_job_id ON metric_samples(job_id);
	CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metric_samples(timestamp);
	CREATE INDEX IF NOT EXISTS idx_metrics_job_timestamp ON metric_samples(job_id, timestamp);
	`

	_, err := s.db.Exec(schema)
	return err
}

// CreateJob inserts a new job into the database.
func (s *PostgresStorage) CreateJob(ctx context.Context, job *Job) error {
	// Check if job already exists
	var exists bool
	err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM jobs WHERE id = $1)", job.ID).Scan(&exists)
	if err != nil {
		return err
	}
	if exists {
		return ErrJobAlreadyExists
	}

	now := time.Now()
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	job.UpdatedAt = now
	if job.StartTime.IsZero() {
		job.StartTime = now
	}
	if job.State == "" {
		job.State = JobStatePending
	}

	nodesStr := strings.Join(job.Nodes, ",")
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO jobs (id, user_name, nodes, state, start_time, end_time, runtime_seconds, 
		                  cpu_usage, memory_usage_mb, gpu_usage, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`, job.ID, job.User, nodesStr, job.State, job.StartTime, job.EndTime, job.RuntimeSeconds,
		job.CPUUsage, job.MemoryUsageMB, job.GPUUsage, job.CreatedAt, job.UpdatedAt)
	return err
}

// GetJob retrieves a job by ID.
func (s *PostgresStorage) GetJob(ctx context.Context, id string) (*Job, error) {
	job := &Job{}
	var nodesStr string
	var endTime sql.NullTime
	var gpuUsage sql.NullFloat64

	err := s.db.QueryRowContext(ctx, `
		SELECT id, user_name, nodes, state, start_time, end_time, runtime_seconds,
		       cpu_usage, memory_usage_mb, gpu_usage, created_at, updated_at
		FROM jobs WHERE id = $1
	`, id).Scan(&job.ID, &job.User, &nodesStr, &job.State, &job.StartTime, &endTime,
		&job.RuntimeSeconds, &job.CPUUsage, &job.MemoryUsageMB, &gpuUsage,
		&job.CreatedAt, &job.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, ErrJobNotFound
	}
	if err != nil {
		return nil, err
	}

	job.Nodes = strings.Split(nodesStr, ",")
	if endTime.Valid {
		job.EndTime = &endTime.Time
	}
	if gpuUsage.Valid {
		job.GPUUsage = &gpuUsage.Float64
	}

	return job, nil
}

// UpdateJob updates an existing job.
func (s *PostgresStorage) UpdateJob(ctx context.Context, job *Job) error {
	job.UpdatedAt = time.Now()

	// Calculate runtime if job is completing
	if job.State == JobStateCompleted || job.State == JobStateFailed || job.State == JobStateCancelled {
		if job.EndTime == nil {
			now := time.Now()
			job.EndTime = &now
		}
		job.RuntimeSeconds = job.EndTime.Sub(job.StartTime).Seconds()
	}

	nodesStr := strings.Join(job.Nodes, ",")
	result, err := s.db.ExecContext(ctx, `
		UPDATE jobs SET user_name = $1, nodes = $2, state = $3, start_time = $4, end_time = $5,
		                runtime_seconds = $6, cpu_usage = $7, memory_usage_mb = $8, gpu_usage = $9,
		                updated_at = $10
		WHERE id = $11
	`, job.User, nodesStr, job.State, job.StartTime, job.EndTime, job.RuntimeSeconds,
		job.CPUUsage, job.MemoryUsageMB, job.GPUUsage, job.UpdatedAt, job.ID)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrJobNotFound
	}

	return nil
}

// DeleteJob removes a job and its metrics from the database.
func (s *PostgresStorage) DeleteJob(ctx context.Context, id string) error {
	result, err := s.db.ExecContext(ctx, "DELETE FROM jobs WHERE id = $1", id)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrJobNotFound
	}

	return nil
}

// ListJobs returns jobs matching the filter criteria.
func (s *PostgresStorage) ListJobs(ctx context.Context, filter JobFilter) ([]*Job, int, error) {
	// Build query with filters
	whereClause := []string{}
	args := []interface{}{}
	argIndex := 1

	if filter.State != nil {
		whereClause = append(whereClause, fmt.Sprintf("state = $%d", argIndex))
		args = append(args, *filter.State)
		argIndex++
	}
	if filter.User != nil {
		whereClause = append(whereClause, fmt.Sprintf("user_name = $%d", argIndex))
		args = append(args, *filter.User)
		argIndex++
	}
	if filter.Node != nil {
		whereClause = append(whereClause, fmt.Sprintf("nodes LIKE $%d", argIndex))
		args = append(args, "%"+*filter.Node+"%")
		argIndex++
	}

	where := ""
	if len(whereClause) > 0 {
		where = "WHERE " + strings.Join(whereClause, " AND ")
	}

	// Count total matching jobs
	var total int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM jobs %s", where)
	if err := s.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	// Apply pagination
	if filter.Limit <= 0 {
		filter.Limit = 100
	}
	if filter.Limit > 1000 {
		filter.Limit = 1000
	}

	query := fmt.Sprintf(`
		SELECT id, user_name, nodes, state, start_time, end_time, runtime_seconds,
		       cpu_usage, memory_usage_mb, gpu_usage, created_at, updated_at
		FROM jobs %s
		ORDER BY start_time DESC
		LIMIT $%d OFFSET $%d
	`, where, argIndex, argIndex+1)
	args = append(args, filter.Limit, filter.Offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	jobs := []*Job{}
	for rows.Next() {
		job := &Job{}
		var nodesStr string
		var endTime sql.NullTime
		var gpuUsage sql.NullFloat64

		if err := rows.Scan(&job.ID, &job.User, &nodesStr, &job.State, &job.StartTime, &endTime,
			&job.RuntimeSeconds, &job.CPUUsage, &job.MemoryUsageMB, &gpuUsage,
			&job.CreatedAt, &job.UpdatedAt); err != nil {
			return nil, 0, err
		}

		job.Nodes = strings.Split(nodesStr, ",")
		if endTime.Valid {
			job.EndTime = &endTime.Time
		}
		if gpuUsage.Valid {
			job.GPUUsage = &gpuUsage.Float64
		}
		jobs = append(jobs, job)
	}

	return jobs, total, nil
}

// GetAllJobs returns all jobs (used for metrics collection).
func (s *PostgresStorage) GetAllJobs(ctx context.Context) ([]*Job, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, user_name, nodes, state, start_time, end_time, runtime_seconds,
		       cpu_usage, memory_usage_mb, gpu_usage, created_at, updated_at
		FROM jobs
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := []*Job{}
	for rows.Next() {
		job := &Job{}
		var nodesStr string
		var endTime sql.NullTime
		var gpuUsage sql.NullFloat64

		if err := rows.Scan(&job.ID, &job.User, &nodesStr, &job.State, &job.StartTime, &endTime,
			&job.RuntimeSeconds, &job.CPUUsage, &job.MemoryUsageMB, &gpuUsage,
			&job.CreatedAt, &job.UpdatedAt); err != nil {
			return nil, err
		}

		job.Nodes = strings.Split(nodesStr, ",")
		if endTime.Valid {
			job.EndTime = &endTime.Time
		}
		if gpuUsage.Valid {
			job.GPUUsage = &gpuUsage.Float64
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

// RecordMetrics inserts a new metric sample.
func (s *PostgresStorage) RecordMetrics(ctx context.Context, sample *MetricSample) error {
	if sample.Timestamp.IsZero() {
		sample.Timestamp = time.Now()
	}

	err := s.db.QueryRowContext(ctx, `
		INSERT INTO metric_samples (job_id, timestamp, cpu_usage, memory_usage_mb, gpu_usage)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id
	`, sample.JobID, sample.Timestamp, sample.CPUUsage, sample.MemoryUsageMB, sample.GPUUsage).Scan(&sample.ID)

	return err
}

// GetJobMetrics retrieves metric samples for a job.
func (s *PostgresStorage) GetJobMetrics(ctx context.Context, jobID string, filter MetricsFilter) ([]*MetricSample, int, error) {
	// Check if job exists
	var exists bool
	if err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM jobs WHERE id = $1)", jobID).Scan(&exists); err != nil {
		return nil, 0, err
	}
	if !exists {
		return nil, 0, ErrJobNotFound
	}

	whereClause := []string{"job_id = $1"}
	args := []interface{}{jobID}
	argIndex := 2

	if filter.StartTime != nil {
		whereClause = append(whereClause, fmt.Sprintf("timestamp >= $%d", argIndex))
		args = append(args, *filter.StartTime)
		argIndex++
	}
	if filter.EndTime != nil {
		whereClause = append(whereClause, fmt.Sprintf("timestamp <= $%d", argIndex))
		args = append(args, *filter.EndTime)
		argIndex++
	}

	where := "WHERE " + strings.Join(whereClause, " AND ")

	// Count total
	var total int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM metric_samples %s", where)
	if err := s.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	// Apply limit
	if filter.Limit <= 0 {
		filter.Limit = 1000
	}
	if filter.Limit > 10000 {
		filter.Limit = 10000
	}

	query := fmt.Sprintf(`
		SELECT id, job_id, timestamp, cpu_usage, memory_usage_mb, gpu_usage
		FROM metric_samples %s
		ORDER BY timestamp DESC
		LIMIT $%d
	`, where, argIndex)
	args = append(args, filter.Limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	samples := []*MetricSample{}
	for rows.Next() {
		sample := &MetricSample{}
		var gpuUsage sql.NullFloat64

		if err := rows.Scan(&sample.ID, &sample.JobID, &sample.Timestamp, &sample.CPUUsage,
			&sample.MemoryUsageMB, &gpuUsage); err != nil {
			return nil, 0, err
		}

		if gpuUsage.Valid {
			sample.GPUUsage = &gpuUsage.Float64
		}
		samples = append(samples, sample)
	}

	return samples, total, nil
}

// GetLatestMetrics retrieves the most recent metric sample for a job.
func (s *PostgresStorage) GetLatestMetrics(ctx context.Context, jobID string) (*MetricSample, error) {
	sample := &MetricSample{}
	var gpuUsage sql.NullFloat64

	err := s.db.QueryRowContext(ctx, `
		SELECT id, job_id, timestamp, cpu_usage, memory_usage_mb, gpu_usage
		FROM metric_samples
		WHERE job_id = $1
		ORDER BY timestamp DESC
		LIMIT 1
	`, jobID).Scan(&sample.ID, &sample.JobID, &sample.Timestamp, &sample.CPUUsage,
		&sample.MemoryUsageMB, &gpuUsage)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if gpuUsage.Valid {
		sample.GPUUsage = &gpuUsage.Float64
	}

	return sample, nil
}

// DeleteMetricsBefore removes metric samples older than the cutoff time.
func (s *PostgresStorage) DeleteMetricsBefore(cutoff time.Time) error {
	_, err := s.db.Exec("DELETE FROM metric_samples WHERE timestamp < $1", cutoff)
	return err
}
