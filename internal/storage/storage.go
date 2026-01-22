// Package storage provides database storage for jobs and metrics.
// It supports PostgreSQL as the primary backend.
package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Common errors returned by storage operations.
var (
	ErrJobNotFound       = errors.New("job not found")
	ErrJobAlreadyExists  = errors.New("job already exists")
	ErrInvalidJobState   = errors.New("invalid job state")
	ErrMissingAuditInfo  = errors.New("missing audit info")
	ErrJobTerminalFrozen = errors.New("job is in terminal state and cannot be updated")
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

// IsTerminalState returns true if the job state is a final state (completed, failed, or cancelled).
func (s JobState) IsTerminal() bool {
	return s == JobStateCompleted || s == JobStateFailed || s == JobStateCancelled
}

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
	Priority      *int64                 `json:"priority,omitempty"`
	ExitCode      *int                   `json:"exit_code,omitempty"`
	StateReason   string                 `json:"state_reason,omitempty"`
	TimeLimitMins *int                   `json:"time_limit_minutes,omitempty"`
	Extra         map[string]interface{} `json:"extra,omitempty"`
}

// JobAuditInfo contains metadata for auditing job changes.
type JobAuditInfo struct {
	ChangedBy     string `json:"changed_by"`
	Source        string `json:"source"`
	CorrelationID string `json:"correlation_id"`
}

// Job represents a batch job with its metadata and current resource usage.
type Job struct {
	ID             string         `json:"id"`
	User           string         `json:"user"`
	Nodes          []string       `json:"nodes"`
	NodeCount      int            `json:"node_count,omitempty"`
	State          JobState       `json:"state"`
	StartTime      time.Time      `json:"start_time"`
	EndTime        *time.Time     `json:"end_time,omitempty"`
	RuntimeSeconds float64        `json:"runtime_seconds,omitempty"`
	CPUUsage       float64        `json:"cpu_usage"`
	MemoryUsageMB  int64          `json:"memory_usage_mb"`
	GPUUsage       *float64       `json:"gpu_usage,omitempty"`
	RequestedCPUs  int64          `json:"requested_cpus,omitempty"`
	AllocatedCPUs  int64          `json:"allocated_cpus,omitempty"`
	RequestedMemMB int64          `json:"requested_memory_mb,omitempty"`
	AllocatedMemMB int64          `json:"allocated_memory_mb,omitempty"`
	RequestedGPUs  int64          `json:"requested_gpus,omitempty"`
	AllocatedGPUs  int64          `json:"allocated_gpus,omitempty"`
	ClusterName    string         `json:"cluster_name,omitempty"`
	SchedulerInst  string         `json:"scheduler_instance,omitempty"`
	IngestVersion  string         `json:"ingest_version,omitempty"`
	LastSampleAt   *time.Time     `json:"last_sample_at,omitempty"`
	SampleCount    int64          `json:"sample_count,omitempty"`
	AvgCPUUsage    float64        `json:"avg_cpu_usage,omitempty"`
	MaxCPUUsage    float64        `json:"max_cpu_usage,omitempty"`
	MaxMemUsageMB  int64          `json:"max_memory_usage_mb,omitempty"`
	AvgGPUUsage    float64        `json:"avg_gpu_usage,omitempty"`
	MaxGPUUsage    float64        `json:"max_gpu_usage,omitempty"`
	Scheduler      *SchedulerInfo `json:"scheduler,omitempty"`
	Audit          *JobAuditInfo  `json:"audit,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

// JobSnapshot represents a full snapshot of job data for audit logging.
type JobSnapshot struct {
	ID             string         `json:"id"`
	User           string         `json:"user"`
	Nodes          []string       `json:"nodes"`
	NodeCount      int            `json:"node_count,omitempty"`
	State          JobState       `json:"state"`
	StartTime      time.Time      `json:"start_time"`
	EndTime        *time.Time     `json:"end_time,omitempty"`
	RuntimeSeconds float64        `json:"runtime_seconds,omitempty"`
	CPUUsage       float64        `json:"cpu_usage"`
	MemoryUsageMB  int64          `json:"memory_usage_mb"`
	GPUUsage       *float64       `json:"gpu_usage,omitempty"`
	RequestedCPUs  int64          `json:"requested_cpus,omitempty"`
	AllocatedCPUs  int64          `json:"allocated_cpus,omitempty"`
	RequestedMemMB int64          `json:"requested_memory_mb,omitempty"`
	AllocatedMemMB int64          `json:"allocated_memory_mb,omitempty"`
	RequestedGPUs  int64          `json:"requested_gpus,omitempty"`
	AllocatedGPUs  int64          `json:"allocated_gpus,omitempty"`
	ClusterName    string         `json:"cluster_name,omitempty"`
	SchedulerInst  string         `json:"scheduler_instance,omitempty"`
	IngestVersion  string         `json:"ingest_version,omitempty"`
	LastSampleAt   *time.Time     `json:"last_sample_at,omitempty"`
	SampleCount    int64          `json:"sample_count,omitempty"`
	AvgCPUUsage    float64        `json:"avg_cpu_usage,omitempty"`
	MaxCPUUsage    float64        `json:"max_cpu_usage,omitempty"`
	MaxMemUsageMB  int64          `json:"max_memory_usage_mb,omitempty"`
	AvgGPUUsage    float64        `json:"avg_gpu_usage,omitempty"`
	MaxGPUUsage    float64        `json:"max_gpu_usage,omitempty"`
	Scheduler      *SchedulerInfo `json:"scheduler,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

// JobAuditEvent represents a persisted audit entry for a job change.
type JobAuditEvent struct {
	ID            int64        `json:"id"`
	JobID         string       `json:"job_id"`
	ChangeType    string       `json:"change_type"`
	ChangedAt     time.Time    `json:"changed_at"`
	ChangedBy     string       `json:"changed_by"`
	Source        string       `json:"source"`
	CorrelationID string       `json:"correlation_id"`
	Snapshot      *JobSnapshot `json:"snapshot"`
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

// auditInfoContextKey is the context key for audit info.
type auditInfoContextKey struct{}

// WithAuditInfo attaches audit info to a context.
func WithAuditInfo(ctx context.Context, info *JobAuditInfo) context.Context {
	return context.WithValue(ctx, auditInfoContextKey{}, info)
}

// GetAuditInfo retrieves audit info from a context.
func GetAuditInfo(ctx context.Context) (*JobAuditInfo, bool) {
	info, ok := ctx.Value(auditInfoContextKey{}).(*JobAuditInfo)
	return info, ok
}

// NewAuditInfo creates audit info with a new correlation ID.
func NewAuditInfo(changedBy, source string) *JobAuditInfo {
	return &JobAuditInfo{
		ChangedBy:     strings.TrimSpace(changedBy),
		Source:        strings.TrimSpace(source),
		CorrelationID: uuid.NewString(),
	}
}

// NewAuditInfoWithCorrelation creates audit info with an explicit correlation ID.
func NewAuditInfoWithCorrelation(changedBy, source, correlationID string) *JobAuditInfo {
	return &JobAuditInfo{
		ChangedBy:     strings.TrimSpace(changedBy),
		Source:        strings.TrimSpace(source),
		CorrelationID: strings.TrimSpace(correlationID),
	}
}

func validateAuditInfo(info *JobAuditInfo) error {
	if info == nil {
		return ErrMissingAuditInfo
	}
	if strings.TrimSpace(info.ChangedBy) == "" || strings.TrimSpace(info.Source) == "" || strings.TrimSpace(info.CorrelationID) == "" {
		return ErrMissingAuditInfo
	}
	return nil
}

func auditInfoFromJobOrContext(ctx context.Context, job *Job) (*JobAuditInfo, error) {
	if job != nil && job.Audit != nil {
		if err := validateAuditInfo(job.Audit); err != nil {
			return nil, err
		}
		return job.Audit, nil
	}
	if info, ok := GetAuditInfo(ctx); ok {
		if err := validateAuditInfo(info); err != nil {
			return nil, err
		}
		if job != nil {
			job.Audit = info
		}
		return info, nil
	}
	return nil, ErrMissingAuditInfo
}

func buildJobSnapshot(job *Job) *JobSnapshot {
	if job == nil {
		return nil
	}
	return &JobSnapshot{
		ID:             job.ID,
		User:           job.User,
		Nodes:          append([]string(nil), job.Nodes...),
		NodeCount:      job.NodeCount,
		State:          job.State,
		StartTime:      job.StartTime,
		EndTime:        job.EndTime,
		RuntimeSeconds: job.RuntimeSeconds,
		CPUUsage:       job.CPUUsage,
		MemoryUsageMB:  job.MemoryUsageMB,
		GPUUsage:       job.GPUUsage,
		RequestedCPUs:  job.RequestedCPUs,
		AllocatedCPUs:  job.AllocatedCPUs,
		RequestedMemMB: job.RequestedMemMB,
		AllocatedMemMB: job.AllocatedMemMB,
		RequestedGPUs:  job.RequestedGPUs,
		AllocatedGPUs:  job.AllocatedGPUs,
		ClusterName:    job.ClusterName,
		SchedulerInst:  job.SchedulerInst,
		IngestVersion:  job.IngestVersion,
		LastSampleAt:   job.LastSampleAt,
		SampleCount:    job.SampleCount,
		AvgCPUUsage:    job.AvgCPUUsage,
		MaxCPUUsage:    job.MaxCPUUsage,
		MaxMemUsageMB:  job.MaxMemUsageMB,
		AvgGPUUsage:    job.AvgGPUUsage,
		MaxGPUUsage:    job.MaxGPUUsage,
		Scheduler:      job.Scheduler,
		CreatedAt:      job.CreatedAt,
		UpdatedAt:      job.UpdatedAt,
	}
}

func marshalJobSnapshot(job *Job) ([]byte, error) {
	snapshot := buildJobSnapshot(job)
	if snapshot == nil {
		return nil, fmt.Errorf("job snapshot is nil")
	}
	return json.Marshal(snapshot)
}

// New creates a new storage instance based on the database type.
func New(dbType, dsn string) (Storage, error) {
	switch strings.ToLower(strings.TrimSpace(dbType)) {
	case "", "postgres", "postgresql":
		return NewPostgresStorage(dsn)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbType)
	}
}

// baseStorage contains common SQL operations shared between storage backends.
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

	if job.NodeCount == 0 && len(job.Nodes) > 0 {
		job.NodeCount = len(job.Nodes)
	}

	var externalJobID, schedulerType, rawState, partition, account, qos, stateReason string
	var submitTime *time.Time
	var priority *int64
	var exitCode, timeLimitMins *int
	if job.Scheduler != nil {
		externalJobID = job.Scheduler.ExternalJobID
		schedulerType = string(job.Scheduler.Type)
		rawState = job.Scheduler.RawState
		partition = job.Scheduler.Partition
		account = job.Scheduler.Account
		qos = job.Scheduler.QoS
		submitTime = job.Scheduler.SubmitTime
		priority = job.Scheduler.Priority
		exitCode = job.Scheduler.ExitCode
		stateReason = job.Scheduler.StateReason
		timeLimitMins = job.Scheduler.TimeLimitMins
	}

	nodesStr := strings.Join(job.Nodes, ",")
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO jobs (
			id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,
			cpu_usage, memory_usage_mb, gpu_usage,
			external_job_id, scheduler_type, raw_state, partition, account, qos, priority, submit_time, exit_code, state_reason, time_limit_minutes,
			requested_cpus, allocated_cpus, requested_memory_mb, allocated_memory_mb, requested_gpus, allocated_gpus,
			cluster_name, scheduler_instance, ingest_version,
			last_sample_at, sample_count, avg_cpu_usage, max_cpu_usage, max_memory_usage_mb, avg_gpu_usage, max_gpu_usage,
			created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40)
	`, job.ID, job.User, nodesStr, job.NodeCount, job.State, job.StartTime, job.EndTime, job.RuntimeSeconds,
		job.CPUUsage, job.MemoryUsageMB, job.GPUUsage,
		externalJobID, schedulerType, rawState, partition, account, qos, priority, submitTime, exitCode, stateReason, timeLimitMins,
		job.RequestedCPUs, job.AllocatedCPUs, job.RequestedMemMB, job.AllocatedMemMB, job.RequestedGPUs, job.AllocatedGPUs,
		job.ClusterName, job.SchedulerInst, job.IngestVersion,
		job.LastSampleAt, job.SampleCount, job.AvgCPUUsage, job.MaxCPUUsage, job.MaxMemUsageMB, job.AvgGPUUsage, job.MaxGPUUsage,
		job.CreatedAt, job.UpdatedAt)
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
