// Package service provides domain services for the HPC Job Observability Service.
// Services encapsulate business logic, validation, and orchestration between
// storage and other dependencies. Handlers should delegate to services for
// all non-HTTP concerns.
package service

import (
	"context"
	"errors"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/api/types"
	"github.com/Avicted/hpc-job-observability-service/internal/mapper"
	"github.com/Avicted/hpc-job-observability-service/internal/metrics"
	"github.com/Avicted/hpc-job-observability-service/internal/storage"
)

// Service errors returned to handlers for HTTP status code mapping.
var (
	ErrJobNotFound       = errors.New("job not found")
	ErrJobAlreadyExists  = errors.New("job already exists")
	ErrValidation        = errors.New("validation error")
	ErrInvalidJobState   = errors.New("invalid job state")
	ErrInternalError     = errors.New("internal error")
	ErrJobTerminalFrozen = errors.New("job is in terminal state and cannot be updated")
)

// ValidationError wraps a validation error with a user-friendly message.
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string {
	return e.Message
}

// AuditContext contains audit information extracted from HTTP headers.
// This is used to track who made changes and for correlation across services.
type AuditContext struct {
	ChangedBy     string
	Source        string
	CorrelationID string
}

// ToStorageAuditInfo converts AuditContext to storage.JobAuditInfo.
func (a *AuditContext) ToStorageAuditInfo() *storage.JobAuditInfo {
	return storage.NewAuditInfoWithCorrelation(a.ChangedBy, a.Source, a.CorrelationID)
}

// JobService handles job-related business logic including CRUD operations,
// validation, and state management. It does not handle HTTP concerns.
type JobService struct {
	store    storage.Storage
	exporter *metrics.Exporter
	mapper   *mapper.Mapper
}

// NewJobService creates a new JobService with the given dependencies.
func NewJobService(store storage.Storage, exporter *metrics.Exporter, mapper *mapper.Mapper) *JobService {
	return &JobService{
		store:    store,
		exporter: exporter,
		mapper:   mapper,
	}
}

// CreateJobInput contains the input data for creating a job.
type CreateJobInput struct {
	ID        string
	User      string
	Nodes     []string
	Scheduler *types.SchedulerInfo
	Audit     *AuditContext
}

// CreateJob creates a new job with the given input.
// Returns the created job or an error.
//
// Validation:
//   - ID is required
//   - User is required
//   - At least one node is required
//
// Side effects:
//   - Increments Prometheus jobs total counter
func (s *JobService) CreateJob(ctx context.Context, input CreateJobInput) (*storage.Job, error) {
	// Validate required fields
	if input.ID == "" {
		return nil, &ValidationError{Message: "Job ID is required"}
	}
	if input.User == "" {
		return nil, &ValidationError{Message: "User is required"}
	}
	if len(input.Nodes) == 0 {
		return nil, &ValidationError{Message: "At least one node is required"}
	}

	job := &storage.Job{
		ID:            input.ID,
		User:          input.User,
		Nodes:         input.Nodes,
		State:         storage.JobStateRunning,
		StartTime:     time.Now(),
		Scheduler:     s.mapper.APISchedulerToStorage(input.Scheduler),
		Audit:         input.Audit.ToStorageAuditInfo(),
		ClusterName:   "default",
		SchedulerInst: "api",
		IngestVersion: "api-v1",
	}

	if err := s.store.CreateJob(ctx, job); err != nil {
		if err == storage.ErrJobAlreadyExists {
			return nil, ErrJobAlreadyExists
		}
		return nil, ErrInternalError
	}

	// Update Prometheus metrics
	s.exporter.IncrementJobsTotal()

	return job, nil
}

// GetJob retrieves a job by ID.
// Returns ErrJobNotFound if the job does not exist.
func (s *JobService) GetJob(ctx context.Context, id string) (*storage.Job, error) {
	job, err := s.store.GetJob(ctx, id)
	if err != nil {
		if err == storage.ErrJobNotFound {
			return nil, ErrJobNotFound
		}
		return nil, ErrInternalError
	}
	return job, nil
}

// ListJobsInput contains the filter parameters for listing jobs.
type ListJobsInput struct {
	State  *string // Job state as string (pending, running, completed, failed, cancelled)
	User   *string
	Node   *string
	Limit  *int
	Offset *int
}

// ListJobsOutput contains the result of listing jobs.
type ListJobsOutput struct {
	Jobs   []*storage.Job
	Total  int
	Limit  int
	Offset int
}

// ListJobs retrieves jobs matching the given filter.
func (s *JobService) ListJobs(ctx context.Context, input ListJobsInput) (*ListJobsOutput, error) {
	filter := storage.JobFilter{
		Limit:  100,
		Offset: 0,
	}

	if input.State != nil {
		state := storage.JobState(*input.State)
		filter.State = &state
	}
	if input.User != nil {
		filter.User = input.User
	}
	if input.Node != nil {
		filter.Node = input.Node
	}
	if input.Limit != nil {
		filter.Limit = *input.Limit
	}
	if input.Offset != nil {
		filter.Offset = *input.Offset
	}

	jobs, total, err := s.store.ListJobs(ctx, filter)
	if err != nil {
		return nil, ErrInternalError
	}

	return &ListJobsOutput{
		Jobs:   jobs,
		Total:  total,
		Limit:  filter.Limit,
		Offset: filter.Offset,
	}, nil
}

// UpdateJobInput contains the input data for updating a job.
type UpdateJobInput struct {
	State         *types.JobState
	CPUUsage      *float64
	MemoryUsageMB *int
	GPUUsage      *float64
	Audit         *AuditContext
}

// UpdateJob updates an existing job with the given input.
// Returns the updated job or an error.
//
// Validation:
//   - State must be a valid job state if provided
//
// Side effects:
//   - Updates Prometheus job metrics
func (s *JobService) UpdateJob(ctx context.Context, id string, input UpdateJobInput) (*storage.Job, error) {
	job, err := s.store.GetJob(ctx, id)
	if err != nil {
		if err == storage.ErrJobNotFound {
			return nil, ErrJobNotFound
		}
		return nil, ErrInternalError
	}

	// Apply updates
	if input.State != nil {
		state := s.mapper.APIJobStateToStorage(*input.State)
		if !s.mapper.IsValidJobState(state) {
			return nil, &ValidationError{Message: "Invalid job state"}
		}
		job.State = state
	}
	if input.CPUUsage != nil {
		job.CPUUsage = *input.CPUUsage
	}
	if input.MemoryUsageMB != nil {
		job.MemoryUsageMB = int64(*input.MemoryUsageMB)
	}
	if input.GPUUsage != nil {
		job.GPUUsage = input.GPUUsage
	}
	job.Audit = input.Audit.ToStorageAuditInfo()

	if err := s.store.UpdateJob(ctx, job); err != nil {
		return nil, ErrInternalError
	}

	// Update Prometheus metrics
	s.exporter.UpdateJobMetrics(job)

	return job, nil
}

// DeleteJob deletes a job by ID.
// Returns ErrJobNotFound if the job does not exist.
func (s *JobService) DeleteJob(ctx context.Context, id string, audit *AuditContext) error {
	ctx = storage.WithAuditInfo(ctx, audit.ToStorageAuditInfo())
	if err := s.store.DeleteJob(ctx, id); err != nil {
		if err == storage.ErrJobNotFound {
			return ErrJobNotFound
		}
		return ErrInternalError
	}
	return nil
}

// Mapper returns the mapper used by the service for handler convenience.
func (s *JobService) Mapper() *mapper.Mapper {
	return s.mapper
}
