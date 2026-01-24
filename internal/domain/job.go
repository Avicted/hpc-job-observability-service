// Package domain contains the core business entities and domain logic for the
// HPC Job Observability Service. Domain entities are pure Go types with no
// dependencies on external packages (API, database, etc.).
//
// This is the innermost layer of the clean architecture - all other layers
// depend on domain, but domain depends on nothing else.
package domain

import (
	"errors"
	"time"
)

// Common errors returned by storage and service operations.
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

// IsTerminal returns true if the job state is a final state (completed, failed, or cancelled).
func (s JobState) IsTerminal() bool {
	return s == JobStateCompleted || s == JobStateFailed || s == JobStateCancelled
}

// IsValid returns true if the job state is a recognized state.
func (s JobState) IsValid() bool {
	switch s {
	case JobStatePending, JobStateRunning, JobStateCompleted, JobStateFailed, JobStateCancelled:
		return true
	default:
		return false
	}
}

// SchedulerType identifies the type of scheduler backend.
type SchedulerType string

const (
	SchedulerTypeMock  SchedulerType = "mock"
	SchedulerTypeSlurm SchedulerType = "slurm"
)

// GPUVendor identifies the GPU vendor for a job.
type GPUVendor string

const (
	GPUVendorNone   GPUVendor = "none"
	GPUVendorNvidia GPUVendor = "nvidia"
	GPUVendorAMD    GPUVendor = "amd"
	GPUVendorMixed  GPUVendor = "mixed" // For jobs spanning nodes with different GPU vendors
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

// AuditInfo contains metadata for auditing job changes.
type AuditInfo struct {
	ChangedBy     string `json:"changed_by"`
	Source        string `json:"source"`
	CorrelationID string `json:"correlation_id"`
}

// Job represents a batch job with its metadata and current resource usage.
// This is the core domain entity for the HPC Job Observability Service.
type Job struct {
	ID             string     `json:"id"`
	User           string     `json:"user"`
	Nodes          []string   `json:"nodes"`
	NodeCount      int        `json:"node_count,omitempty"`
	State          JobState   `json:"state"`
	StartTime      time.Time  `json:"start_time"`
	EndTime        *time.Time `json:"end_time,omitempty"`
	RuntimeSeconds float64    `json:"runtime_seconds,omitempty"`
	CPUUsage       float64    `json:"cpu_usage"`
	MemoryUsageMB  int64      `json:"memory_usage_mb"`
	GPUUsage       *float64   `json:"gpu_usage,omitempty"`
	RequestedCPUs  int64      `json:"requested_cpus,omitempty"`
	AllocatedCPUs  int64      `json:"allocated_cpus,omitempty"`
	RequestedMemMB int64      `json:"requested_memory_mb,omitempty"`
	AllocatedMemMB int64      `json:"allocated_memory_mb,omitempty"`
	RequestedGPUs  int64      `json:"requested_gpus,omitempty"`
	AllocatedGPUs  int64      `json:"allocated_gpus,omitempty"`
	ClusterName    string     `json:"cluster_name,omitempty"`
	SchedulerInst  string     `json:"scheduler_instance,omitempty"`
	IngestVersion  string     `json:"ingest_version,omitempty"`
	LastSampleAt   *time.Time `json:"last_sample_at,omitempty"`
	SampleCount    int64      `json:"sample_count,omitempty"`
	AvgCPUUsage    float64    `json:"avg_cpu_usage,omitempty"`
	MaxCPUUsage    float64    `json:"max_cpu_usage,omitempty"`
	MaxMemUsageMB  int64      `json:"max_memory_usage_mb,omitempty"`
	AvgGPUUsage    float64    `json:"avg_gpu_usage,omitempty"`
	MaxGPUUsage    float64    `json:"max_gpu_usage,omitempty"`
	// Cgroup and GPU fields for real resource tracking
	CgroupPath string         `json:"cgroup_path,omitempty"` // Path to job's cgroup
	GPUCount   int            `json:"gpu_count,omitempty"`   // Number of GPUs allocated
	GPUVendor  GPUVendor      `json:"gpu_vendor,omitempty"`  // GPU vendor
	GPUDevices []string       `json:"gpu_devices,omitempty"` // GPU device IDs
	Scheduler  *SchedulerInfo `json:"scheduler,omitempty"`
	Audit      *AuditInfo     `json:"audit,omitempty"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

// IsInTerminalState returns true if the job is in a terminal state.
func (j *Job) IsInTerminalState() bool {
	return j.State.IsTerminal()
}

// CalculateRuntime calculates the runtime in seconds from start to end time.
func (j *Job) CalculateRuntime() float64 {
	if j.EndTime == nil {
		return 0
	}
	return j.EndTime.Sub(j.StartTime).Seconds()
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
	CgroupPath     string         `json:"cgroup_path,omitempty"`
	GPUCount       int            `json:"gpu_count,omitempty"`
	GPUVendor      GPUVendor      `json:"gpu_vendor,omitempty"`
	GPUDevices     []string       `json:"gpu_devices,omitempty"`
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
