// Package storage provides SQLite-specific storage implementation.
package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// SQLiteStorage implements Storage interface using SQLite.
type SQLiteStorage struct {
	baseStorage
}

// NewSQLiteStorage creates a new SQLite storage instance.
func NewSQLiteStorage(dsn string) (*SQLiteStorage, error) {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}

	// Enable WAL mode and foreign keys
	if _, err := db.Exec("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to configure SQLite: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping SQLite database: %w", err)
	}

	return &SQLiteStorage{baseStorage: baseStorage{db: db}}, nil
}

// Migrate creates the database schema.
func (s *SQLiteStorage) Migrate() error {
	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		id TEXT PRIMARY KEY,
		user_name TEXT NOT NULL,
		nodes TEXT NOT NULL,
		state TEXT NOT NULL DEFAULT 'pending',
		start_time DATETIME NOT NULL,
		end_time DATETIME,
		runtime_seconds REAL DEFAULT 0,
		cpu_usage REAL DEFAULT 0,
		memory_usage_mb INTEGER DEFAULT 0,
		gpu_usage REAL,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state);
	CREATE INDEX IF NOT EXISTS idx_jobs_user ON jobs(user_name);
	CREATE INDEX IF NOT EXISTS idx_jobs_start_time ON jobs(start_time);

	CREATE TABLE IF NOT EXISTS metric_samples (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		job_id TEXT NOT NULL,
		timestamp DATETIME NOT NULL,
		cpu_usage REAL NOT NULL,
		memory_usage_mb INTEGER NOT NULL,
		gpu_usage REAL,
		FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
	);

	CREATE INDEX IF NOT EXISTS idx_metrics_job_id ON metric_samples(job_id);
	CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metric_samples(timestamp);
	CREATE INDEX IF NOT EXISTS idx_metrics_job_timestamp ON metric_samples(job_id, timestamp);
	`

	_, err := s.db.Exec(schema)
	return err
}

// CreateJob inserts a new job into the database.
func (s *SQLiteStorage) CreateJob(ctx context.Context, job *Job) error {
	// Check if job already exists
	var exists bool
	err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM jobs WHERE id = ?)", job.ID).Scan(&exists)
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
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, job.ID, job.User, nodesStr, job.State, job.StartTime, job.EndTime, job.RuntimeSeconds,
		job.CPUUsage, job.MemoryUsageMB, job.GPUUsage, job.CreatedAt, job.UpdatedAt)
	return err
}

// GetJob retrieves a job by ID.
func (s *SQLiteStorage) GetJob(ctx context.Context, id string) (*Job, error) {
	job := &Job{}
	var nodesStr string
	var endTime sql.NullTime
	var gpuUsage sql.NullFloat64

	err := s.db.QueryRowContext(ctx, `
		SELECT id, user_name, nodes, state, start_time, end_time, runtime_seconds,
		       cpu_usage, memory_usage_mb, gpu_usage, created_at, updated_at
		FROM jobs WHERE id = ?
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
func (s *SQLiteStorage) UpdateJob(ctx context.Context, job *Job) error {
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
		UPDATE jobs SET user_name = ?, nodes = ?, state = ?, start_time = ?, end_time = ?,
		                runtime_seconds = ?, cpu_usage = ?, memory_usage_mb = ?, gpu_usage = ?,
		                updated_at = ?
		WHERE id = ?
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

// UpsertJob creates a job if it doesn't exist, or updates it if it does.
func (s *SQLiteStorage) UpsertJob(ctx context.Context, job *Job) error {
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

	// Calculate runtime if job is completing
	if job.State == JobStateCompleted || job.State == JobStateFailed || job.State == JobStateCancelled {
		if job.EndTime != nil && !job.EndTime.IsZero() {
			job.RuntimeSeconds = job.EndTime.Sub(job.StartTime).Seconds()
		}
	}

	nodesStr := strings.Join(job.Nodes, ",")
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO jobs (id, user_name, nodes, state, start_time, end_time, runtime_seconds,
		                  cpu_usage, memory_usage_mb, gpu_usage, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET
			user_name = excluded.user_name,
			nodes = excluded.nodes,
			state = excluded.state,
			start_time = excluded.start_time,
			end_time = excluded.end_time,
			runtime_seconds = excluded.runtime_seconds,
			cpu_usage = excluded.cpu_usage,
			memory_usage_mb = excluded.memory_usage_mb,
			gpu_usage = excluded.gpu_usage,
			updated_at = excluded.updated_at
	`, job.ID, job.User, nodesStr, job.State, job.StartTime, job.EndTime, job.RuntimeSeconds,
		job.CPUUsage, job.MemoryUsageMB, job.GPUUsage, job.CreatedAt, job.UpdatedAt)
	return err
}

// DeleteJob removes a job and its metrics from the database.
func (s *SQLiteStorage) DeleteJob(ctx context.Context, id string) error {
	result, err := s.db.ExecContext(ctx, "DELETE FROM jobs WHERE id = ?", id)
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
func (s *SQLiteStorage) ListJobs(ctx context.Context, filter JobFilter) ([]*Job, int, error) {
	// Build query with filters
	whereClause := []string{}
	args := []interface{}{}

	if filter.State != nil {
		whereClause = append(whereClause, "state = ?")
		args = append(args, *filter.State)
	}
	if filter.User != nil {
		whereClause = append(whereClause, "user_name = ?")
		args = append(args, *filter.User)
	}
	if filter.Node != nil {
		whereClause = append(whereClause, "nodes LIKE ?")
		args = append(args, "%"+*filter.Node+"%")
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
		LIMIT ? OFFSET ?
	`, where)
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
func (s *SQLiteStorage) GetAllJobs(ctx context.Context) ([]*Job, error) {
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
func (s *SQLiteStorage) RecordMetrics(ctx context.Context, sample *MetricSample) error {
	if sample.Timestamp.IsZero() {
		sample.Timestamp = time.Now()
	}

	result, err := s.db.ExecContext(ctx, `
		INSERT INTO metric_samples (job_id, timestamp, cpu_usage, memory_usage_mb, gpu_usage)
		VALUES (?, ?, ?, ?, ?)
	`, sample.JobID, sample.Timestamp, sample.CPUUsage, sample.MemoryUsageMB, sample.GPUUsage)
	if err != nil {
		return err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	sample.ID = id

	return nil
}

// GetJobMetrics retrieves metric samples for a job.
func (s *SQLiteStorage) GetJobMetrics(ctx context.Context, jobID string, filter MetricsFilter) ([]*MetricSample, int, error) {
	// Check if job exists
	var exists bool
	if err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM jobs WHERE id = ?)", jobID).Scan(&exists); err != nil {
		return nil, 0, err
	}
	if !exists {
		return nil, 0, ErrJobNotFound
	}

	whereClause := []string{"job_id = ?"}
	args := []interface{}{jobID}

	if filter.StartTime != nil {
		whereClause = append(whereClause, "timestamp >= ?")
		args = append(args, *filter.StartTime)
	}
	if filter.EndTime != nil {
		whereClause = append(whereClause, "timestamp <= ?")
		args = append(args, *filter.EndTime)
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
		LIMIT ?
	`, where)
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
func (s *SQLiteStorage) GetLatestMetrics(ctx context.Context, jobID string) (*MetricSample, error) {
	sample := &MetricSample{}
	var gpuUsage sql.NullFloat64

	err := s.db.QueryRowContext(ctx, `
		SELECT id, job_id, timestamp, cpu_usage, memory_usage_mb, gpu_usage
		FROM metric_samples
		WHERE job_id = ?
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
func (s *SQLiteStorage) DeleteMetricsBefore(cutoff time.Time) error {
	_, err := s.db.Exec("DELETE FROM metric_samples WHERE timestamp < ?", cutoff)
	return err
}
