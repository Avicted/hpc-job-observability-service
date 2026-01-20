// Package storage provides database storage for jobs and metrics.
// It supports both SQLite (default) and PostgreSQL backends.
package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Common errors returned by storage operations.
var (
	ErrJobNotFound      = errors.New("job not found")
	ErrJobAlreadyExists = errors.New("job already exists")
	ErrInvalidJobState  = errors.New("invalid job state")
)

// JobState represents the current state of a job.
type JobState string

const (
	JobStatePending   JobState = "pending"
	JobStateRunning   JobState = "running"
	JobStateCompleted JobState = "completed"
	JobStateFailed    JobState = "failed"
	JobStateCancelled JobState = "cancelled"
)

// SchedulerType identifies the type of scheduler backend.
type SchedulerType string

const (
	SchedulerTypeMock  SchedulerType = "mock"
	SchedulerTypeSlurm SchedulerType = "slurm"
)

// SchedulerInfo contains metadata from the external scheduler.
type SchedulerInfo struct {
	Type          SchedulerType          `json:"type"`
	ExternalJobID string                 `json:"external_job_id,omitempty"`
	RawState      string                 `json:"raw_state,omitempty"`
	SubmitTime    *time.Time             `json:"submit_time,omitempty"`
	Partition     string                 `json:"partition,omitempty"`
	Account       string                 `json:"account,omitempty"`
	QoS           string                 `json:"qos,omitempty"`
	Priority      *int                   `json:"priority,omitempty"`
	ExitCode      *int                   `json:"exit_code,omitempty"`
	Extra         map[string]interface{} `json:"extra,omitempty"`
}

// Job represents a batch job with its metadata and current resource usage.
type Job struct {
	ID             string         `json:"id"`
	User           string         `json:"user"`
	Nodes          []string       `json:"nodes"`
	State          JobState       `json:"state"`
	StartTime      time.Time      `json:"start_time"`
	EndTime        *time.Time     `json:"end_time,omitempty"`
	RuntimeSeconds float64        `json:"runtime_seconds,omitempty"`
	CPUUsage       float64        `json:"cpu_usage"`
	MemoryUsageMB  int64          `json:"memory_usage_mb"`
	GPUUsage       *float64       `json:"gpu_usage,omitempty"`
	Scheduler      *SchedulerInfo `json:"scheduler,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

// MetricSample represents a single point-in-time resource usage measurement.
type MetricSample struct {
	ID            int64     `json:"id"`
	JobID         string    `json:"job_id"`
	Timestamp     time.Time `json:"timestamp"`
	CPUUsage      float64   `json:"cpu_usage"`
	MemoryUsageMB int64     `json:"memory_usage_mb"`
	GPUUsage      *float64  `json:"gpu_usage,omitempty"`
}

// JobFilter contains optional filters for listing jobs.
type JobFilter struct {
	State  *JobState
	User   *string
	Node   *string
	Limit  int
	Offset int
}

// MetricsFilter contains optional filters for listing metrics.
type MetricsFilter struct {
	StartTime *time.Time
	EndTime   *time.Time
	Limit     int
}

// Storage defines the interface for job and metrics persistence.
type Storage interface {
	// Job operations
	CreateJob(ctx context.Context, job *Job) error
	GetJob(ctx context.Context, id string) (*Job, error)
	UpdateJob(ctx context.Context, job *Job) error
	DeleteJob(ctx context.Context, id string) error
	ListJobs(ctx context.Context, filter JobFilter) ([]*Job, int, error)
	GetAllJobs(ctx context.Context) ([]*Job, error)

	// Metrics operations
	RecordMetrics(ctx context.Context, sample *MetricSample) error
	GetJobMetrics(ctx context.Context, jobID string, filter MetricsFilter) ([]*MetricSample, int, error)
	GetLatestMetrics(ctx context.Context, jobID string) (*MetricSample, error)
	DeleteMetricsBefore(cutoff time.Time) error

	// Lifecycle operations
	Migrate() error
	Close() error
	SeedDemoData() error
}

// New creates a new storage instance based on the database type.
func New(dbType, dsn string) (Storage, error) {
	switch strings.ToLower(dbType) {
	case "sqlite":
		return NewSQLiteStorage(dsn)
	case "postgres", "postgresql":
		return NewPostgresStorage(dsn)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbType)
	}
}

// baseStorage contains common SQL operations shared between SQLite and PostgreSQL.
type baseStorage struct {
	db *sql.DB
}

func (s *baseStorage) Close() error {
	return s.db.Close()
}

// SeedDemoData creates sample jobs and metrics for demonstration purposes.
func (s *baseStorage) SeedDemoData() error {
	ctx := context.Background()
	now := time.Now()

	// Demo jobs representing various HPC workloads
	demoJobs := []*Job{
		{
			ID:            "demo-job-001",
			User:          "alice",
			Nodes:         []string{"node-01", "node-02"},
			State:         JobStateRunning,
			StartTime:     now.Add(-2 * time.Hour),
			CPUUsage:      78.5,
			MemoryUsageMB: 8192,
			GPUUsage:      floatPtr(45.0),
		},
		{
			ID:            "demo-job-002",
			User:          "bob",
			Nodes:         []string{"node-03"},
			State:         JobStateRunning,
			StartTime:     now.Add(-30 * time.Minute),
			CPUUsage:      92.3,
			MemoryUsageMB: 16384,
			GPUUsage:      floatPtr(88.0),
		},
		{
			ID:             "demo-job-003",
			User:           "charlie",
			Nodes:          []string{"node-04", "node-05", "node-06"},
			State:          JobStateCompleted,
			StartTime:      now.Add(-5 * time.Hour),
			EndTime:        timePtr(now.Add(-1 * time.Hour)),
			RuntimeSeconds: 14400,
			CPUUsage:       0,
			MemoryUsageMB:  0,
		},
		{
			ID:             "demo-job-004",
			User:           "alice",
			Nodes:          []string{"node-07"},
			State:          JobStateFailed,
			StartTime:      now.Add(-3 * time.Hour),
			EndTime:        timePtr(now.Add(-2*time.Hour - 30*time.Minute)),
			RuntimeSeconds: 1800,
			CPUUsage:       0,
			MemoryUsageMB:  0,
		},
		{
			ID:            "demo-job-005",
			User:          "diana",
			Nodes:         []string{"node-08", "node-09"},
			State:         JobStatePending,
			StartTime:     now,
			CPUUsage:      0,
			MemoryUsageMB: 0,
		},
	}

	for _, job := range demoJobs {
		job.CreatedAt = job.StartTime
		job.UpdatedAt = now
		if err := s.createJobInternal(ctx, job); err != nil && !errors.Is(err, ErrJobAlreadyExists) {
			return fmt.Errorf("failed to seed job %s: %w", job.ID, err)
		}
	}

	// Generate historical metrics for running and completed jobs
	for _, job := range demoJobs {
		if job.State == JobStateRunning || job.State == JobStateCompleted {
			if err := s.seedJobMetrics(ctx, job, now); err != nil {
				return fmt.Errorf("failed to seed metrics for job %s: %w", job.ID, err)
			}
		}
	}

	return nil
}

func (s *baseStorage) createJobInternal(ctx context.Context, job *Job) error {
	// Check if job already exists
	var exists bool
	err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM jobs WHERE id = $1)", job.ID).Scan(&exists)
	if err != nil {
		return err
	}
	if exists {
		return ErrJobAlreadyExists
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

func (s *baseStorage) seedJobMetrics(ctx context.Context, job *Job, now time.Time) error {
	// Generate metrics samples every minute for the job's duration
	endTime := now
	if job.EndTime != nil {
		endTime = *job.EndTime
	}

	// Limit to last 2 hours of samples for demo
	startTime := job.StartTime
	if endTime.Sub(startTime) > 2*time.Hour {
		startTime = endTime.Add(-2 * time.Hour)
	}

	for t := startTime; t.Before(endTime); t = t.Add(1 * time.Minute) {
		// Simulate varying resource usage
		cpuVariation := (float64(t.Unix()%10) - 5) * 2
		memVariation := int64((t.Unix() % 5) * 100)

		sample := &MetricSample{
			JobID:         job.ID,
			Timestamp:     t,
			CPUUsage:      clamp(job.CPUUsage+cpuVariation, 0, 100),
			MemoryUsageMB: max(0, job.MemoryUsageMB+memVariation),
		}
		if job.GPUUsage != nil {
			gpuVariation := (float64(t.Unix()%8) - 4) * 3
			gpuUsage := clamp(*job.GPUUsage+gpuVariation, 0, 100)
			sample.GPUUsage = &gpuUsage
		}

		_, err := s.db.ExecContext(ctx, `
			INSERT INTO metric_samples (job_id, timestamp, cpu_usage, memory_usage_mb, gpu_usage)
			VALUES ($1, $2, $3, $4, $5)
		`, sample.JobID, sample.Timestamp, sample.CPUUsage, sample.MemoryUsageMB, sample.GPUUsage)
		if err != nil {
			return err
		}
	}

	return nil
}

func floatPtr(f float64) *float64 {
	return &f
}

func timePtr(t time.Time) *time.Time {
	return &t
}

func clamp(val, minVal, maxVal float64) float64 {
	if val < minVal {
		return minVal
	}
	if val > maxVal {
		return maxVal
	}
	return val
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
