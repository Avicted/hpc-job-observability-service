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
	UpsertJob(ctx context.Context, job *Job) error
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

	// User names for demo jobs
	users := []string{"Alice", "Bob", "Liam", "Sofia", "Noah", "Olivia", "Ethan", "Mia", "Lucas", "Ava",
		"Leo", "Isabella", "Jack", "Amelia", "Oliver", "Charlotte", "Henry", "Ella", "Benjamin", "Grace",
		"Daniel", "Lily", "Samuel", "Hannah", "Matthew", "Nora", "James", "Zoe", "William", "Chloe",
		"Michael", "Aria", "David", "Ruby", "Joseph", "Lucy", "Andrew", "Violet", "Thomas", "Emily",
		"Christopher", "Freya", "Joshua", "Clara", "Nathan", "Iris", "Ryan", "Stella", "Jonathan", "Eliza",
		"Aaron", "Naomi", "Caleb", "Maya", "Isaac", "Eva", "Sebastian", "Rose", "Nicholas", "Aurora",
		"Julian", "Hazel", "Owen", "Willow", "Dylan", "Penelope", "Carter", "Margot", "Eli", "June",
		"Finn", "Scarlett", "Adrian", "Thea", "Miles", "Florence", "Simon", "Beatrice", "Victor", "Helena",
		"Max", "Astrid", "Patrick", "Ingrid", "Rowan", "Sienna", "Theo", "Elin", "Hugo", "Linnea",
		"Marcus", "Josephine", "Paul", "Matilda", "Oscar", "Agnes", "Tobias", "Edith", "Emma", "Diana"}

	partitions := []string{"gpu", "compute", "batch", "debug"}
	states := []JobState{JobStateRunning, JobStateCompleted, JobStateFailed, JobStateCancelled, JobStatePending}

	jobCount := 100
	demoJobs := make([]*Job, 0, jobCount)

	for i := 1; i <= jobCount; i++ {
		user := users[i%len(users)]
		partition := partitions[i%len(partitions)]
		state := states[i%len(states)]

		jobID := fmt.Sprintf("demo-job-%03d", i)

		nodeCount := 1 + (i % 32)
		nodes := make([]string, 0, nodeCount)
		for n := 0; n < nodeCount; n++ {
			nodes = append(nodes, fmt.Sprintf("node-%02d", (i+n)%50+1))
		}

		submitTime := now.Add(-time.Duration(10+i) * time.Minute)
		startTime := now.Add(-time.Duration(5*i) * time.Minute)
		if startTime.Before(submitTime) {
			startTime = submitTime.Add(5 * time.Minute)
		}

		job := &Job{
			ID:            jobID,
			User:          user,
			Nodes:         nodes,
			State:         state,
			StartTime:     startTime,
			CPUUsage:      25 + float64(i%75),
			MemoryUsageMB: int64(1024 * (1 + i%16)),
		}

		// Add GPU usage for GPU partition jobs
		if partition == "gpu" {
			job.GPUUsage = floatPtr(30 + float64(i%70))
		}

		// Set end time and runtime for completed/failed/cancelled jobs
		switch state {
		case JobStateCompleted:
			end := startTime.Add(time.Duration(30+i) * time.Minute)
			job.EndTime = &end
			job.RuntimeSeconds = end.Sub(startTime).Seconds()
		case JobStateFailed:
			end := startTime.Add(time.Duration(10+i) * time.Minute)
			job.EndTime = &end
			job.RuntimeSeconds = end.Sub(startTime).Seconds()
		case JobStateCancelled:
			end := startTime.Add(time.Duration(5+i) * time.Minute)
			job.EndTime = &end
			job.RuntimeSeconds = end.Sub(startTime).Seconds()
		}

		demoJobs = append(demoJobs, job)
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
