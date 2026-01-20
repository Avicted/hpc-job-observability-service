// Package scheduler provides an abstraction layer for job scheduling systems.
// It defines interfaces for integrating with various HPC workload managers
// such as SLURM, PBS, or mock/simulated job sources.
package scheduler

import (
	"context"
	"time"
)

// SchedulerType identifies the type of scheduler backend.
type SchedulerType string

const (
	// SchedulerTypeMock represents a mock/simulated scheduler for testing.
	SchedulerTypeMock SchedulerType = "mock"
	// SchedulerTypeSlurm represents the SLURM workload manager.
	SchedulerTypeSlurm SchedulerType = "slurm"
)

// JobState represents normalized job states across all schedulers.
type JobState string

const (
	JobStatePending   JobState = "pending"
	JobStateRunning   JobState = "running"
	JobStateCompleted JobState = "completed"
	JobStateFailed    JobState = "failed"
	JobStateCancelled JobState = "cancelled"
)

// SchedulerInfo contains metadata from the external scheduler.
// This allows preserving scheduler-specific information while normalizing
// the core job model for the API.
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

// Job represents a normalized job from any scheduler.
// The core fields are consistent across schedulers, while
// scheduler-specific metadata is stored in the Scheduler field.
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
}

// MetricSample represents a point-in-time resource usage measurement.
type MetricSample struct {
	JobID         string    `json:"job_id"`
	Timestamp     time.Time `json:"timestamp"`
	CPUUsage      float64   `json:"cpu_usage"`
	MemoryUsageMB int64     `json:"memory_usage_mb"`
	GPUUsage      *float64  `json:"gpu_usage,omitempty"`
}

// JobSource defines the interface for fetching jobs from a scheduler.
// Implementations can be created for different workload managers (SLURM, PBS, etc.)
// or for mock/simulated job sources for testing.
type JobSource interface {
	// Type returns the scheduler type identifier.
	Type() SchedulerType

	// ListJobs returns all jobs matching the given filter.
	// The filter may specify state, user, time range, etc.
	ListJobs(ctx context.Context, filter JobFilter) ([]*Job, error)

	// GetJob returns a specific job by its ID.
	// Returns nil and no error if the job is not found.
	GetJob(ctx context.Context, id string) (*Job, error)

	// GetJobMetrics returns recent metrics samples for a job.
	// Some schedulers may not support historical metrics.
	GetJobMetrics(ctx context.Context, jobID string) ([]*MetricSample, error)

	// SupportsMetrics returns true if this scheduler can provide metrics.
	SupportsMetrics() bool
}

// JobFilter contains optional filters for listing jobs.
type JobFilter struct {
	State     *JobState
	User      *string
	Partition *string
	StartTime *time.Time
	EndTime   *time.Time
	Limit     int
	Offset    int
}

// StateMapping provides utilities for mapping scheduler-specific states
// to the normalized JobState enum.
type StateMapping struct {
	mappings map[string]JobState
}

// NewSlurmStateMapping creates a state mapper for SLURM job states.
func NewSlurmStateMapping() *StateMapping {
	return &StateMapping{
		mappings: map[string]JobState{
			// Pending states
			"PENDING":       JobStatePending,
			"CONFIGURING":   JobStatePending,
			"RESV_DEL_HOLD": JobStatePending,
			"REQUEUE_FED":   JobStatePending,
			"REQUEUE_HOLD":  JobStatePending,
			"REQUEUED":      JobStatePending,
			"RESIZING":      JobStatePending,
			"SUSPENDED":     JobStatePending,
			"SPECIAL_EXIT":  JobStatePending,

			// Running states
			"RUNNING":    JobStateRunning,
			"COMPLETING": JobStateRunning,
			"SIGNALING":  JobStateRunning,
			"STAGE_OUT":  JobStateRunning,

			// Completed states
			"COMPLETED": JobStateCompleted,

			// Failed states
			"FAILED":        JobStateFailed,
			"BOOT_FAIL":     JobStateFailed,
			"DEADLINE":      JobStateFailed,
			"NODE_FAIL":     JobStateFailed,
			"OUT_OF_MEMORY": JobStateFailed,
			"TIMEOUT":       JobStateFailed,

			// Cancelled states
			"CANCELLED": JobStateCancelled,
			"PREEMPTED": JobStateCancelled,
			"REVOKED":   JobStateCancelled,
		},
	}
}

// MapState converts a raw scheduler state to the normalized JobState.
// If the state is not recognized, it returns the default state and false.
func (m *StateMapping) MapState(rawState string) (JobState, bool) {
	if state, ok := m.mappings[rawState]; ok {
		return state, true
	}
	return JobStatePending, false
}

// NormalizeState converts a raw scheduler state to JobState.
// Uses sensible defaults for unknown states.
func (m *StateMapping) NormalizeState(rawState string) JobState {
	if state, ok := m.MapState(rawState); ok {
		return state
	}
	// Default to pending for unknown states
	return JobStatePending
}
