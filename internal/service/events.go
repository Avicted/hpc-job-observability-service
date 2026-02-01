package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/api/types"
	"github.com/Avicted/hpc-job-observability-service/internal/domain"
	"github.com/Avicted/hpc-job-observability-service/internal/storage"
	"github.com/Avicted/hpc-job-observability-service/internal/utils/audit"
	"github.com/Avicted/hpc-job-observability-service/internal/utils/mapper"
	"github.com/Avicted/hpc-job-observability-service/internal/utils/metrics"
)

// EventService handles job lifecycle events from Slurm prolog/epilog scripts.
// It implements idempotent event processing for job start and finish events.
type EventService struct {
	store    storage.Store
	exporter *metrics.Exporter
	mapper   *mapper.Mapper
}

// NewEventService creates a new EventService with the given dependencies.
func NewEventService(store storage.Store, exporter *metrics.Exporter, mapper *mapper.Mapper) *EventService {
	return &EventService{
		store:    store,
		exporter: exporter,
		mapper:   mapper,
	}
}

// JobStartedInput contains the input data from a job started event.
type JobStartedInput struct {
	JobID              string
	User               string
	NodeList           []string
	Timestamp          time.Time
	CgroupPath         *string
	GPUVendor          *types.GPUVendor
	GPUDevices         *[]string
	GPUAllocation      *int
	CPUAllocation      *int
	MemoryAllocationMB *int
	Partition          *string
	Account            *string
}

// JobStartedOutput contains the result of processing a job started event.
type JobStartedOutput struct {
	JobID   string
	Status  types.JobEventResponseStatus
	Message string
}

// JobStarted processes a job started event from Slurm prolog scripts.
// This method is idempotent - duplicate events are handled safely.
//
// Validation:
//   - job_id is required
//   - user is required
//   - node_list is required (at least one node)
//
// Idempotency:
//   - If job already exists, returns Skipped status
//   - Handles race conditions where job is created between check and create
//
// Side effects:
//   - Creates a new job in storage
//   - Increments Prometheus jobs total counter
//
// Parameters:
//   - ctx: Context for cancellation and deadline propagation
//   - input: Job started event data
func (s *EventService) JobStarted(ctx context.Context, input JobStartedInput) (*JobStartedOutput, error) {
	// Validate required fields
	if input.JobID == "" {
		return nil, &ValidationError{Message: "job_id is required"}
	}
	if input.User == "" {
		return nil, &ValidationError{Message: "user is required"}
	}
	if len(input.NodeList) == 0 {
		return nil, &ValidationError{Message: "node_list is required"}
	}

	// Check if job already exists (idempotency)
	_, err := s.store.GetJob(ctx, input.JobID)
	if err == nil {
		// Job exists - return success but indicate it was a duplicate
		return &JobStartedOutput{
			JobID:   input.JobID,
			Status:  types.Skipped,
			Message: "Job already registered",
		}, nil
	} else if !errors.Is(err, storage.ErrNotFound) {
		// Log the unexpected error
		return nil, fmt.Errorf("failed to check if job exists: %w", err)
	}

	// Parse GPU vendor
	gpuVendor := domain.GPUVendorNone
	if input.GPUVendor != nil {
		gpuVendor = domain.GPUVendor(*input.GPUVendor)
	}

	// Create the job from the event
	job := &domain.Job{
		ID:        input.JobID,
		User:      input.User,
		Nodes:     input.NodeList,
		NodeCount: len(input.NodeList),
		State:     domain.JobStateRunning,
		StartTime: input.Timestamp,
		Audit:     audit.NewAuditInfo("slurm-prolog", "lifecycle-event"),

		// Cgroup and GPU info from the event
		CgroupPath: derefString(input.CgroupPath),
		GPUVendor:  gpuVendor,
		GPUDevices: derefStringSlice(input.GPUDevices),
	}

	// Set GPU count
	if input.GPUAllocation != nil {
		job.GPUCount = *input.GPUAllocation
		job.AllocatedGPUs = int64(*input.GPUAllocation)
		job.RequestedGPUs = int64(*input.GPUAllocation)
	}

	// Set CPU allocation
	if input.CPUAllocation != nil {
		job.AllocatedCPUs = int64(*input.CPUAllocation)
		job.RequestedCPUs = int64(*input.CPUAllocation)
	}

	// Set memory allocation
	if input.MemoryAllocationMB != nil {
		job.AllocatedMemMB = int64(*input.MemoryAllocationMB)
		job.RequestedMemMB = int64(*input.MemoryAllocationMB)
	}

	// Set scheduler info
	job.Scheduler = &domain.SchedulerInfo{
		Type:          domain.SchedulerTypeSlurm,
		ExternalJobID: input.JobID,
	}
	if input.Partition != nil {
		job.Scheduler.Partition = *input.Partition
	}
	if input.Account != nil {
		job.Scheduler.Account = *input.Account
	}

	// Set cluster metadata
	job.ClusterName = "default"
	job.SchedulerInst = "slurm"
	job.IngestVersion = "prolog-v1"

	if err := s.store.CreateJob(ctx, job); err != nil {
		if errors.Is(err, storage.ErrConflict) {
			// Race condition - job was created between our check and create
			return &JobStartedOutput{
				JobID:   input.JobID,
				Status:  types.Skipped,
				Message: "Job already registered (race condition)",
			}, nil
		}
		// Return the original error so we can see what went wrong
		return nil, fmt.Errorf("failed to create job: %w", err)
	}

	// Update Prometheus metrics
	s.exporter.IncrementJobsTotal()

	return &JobStartedOutput{
		JobID:   input.JobID,
		Status:  types.Created,
		Message: "Job registered successfully",
	}, nil
}

// JobFinishedInput contains the input data from a job finished event.
type JobFinishedInput struct {
	JobID      string
	FinalState types.JobState
	Timestamp  time.Time
	ExitCode   *int
	Signal     *int
}

// JobFinishedOutput contains the result of processing a job finished event.
type JobFinishedOutput struct {
	JobID   string
	Status  types.JobEventResponseStatus
	Message string
}

// JobFinished processes a job finished event from Slurm epilog scripts.
// This method is idempotent - duplicate events are handled safely.
//
// Validation:
//   - job_id is required
//   - final_state must be a valid state
//
// Idempotency:
//   - If job is already in terminal state, returns Skipped status
//
// Side effects:
//   - Updates job state, end time, and runtime
//   - Sets exit code in scheduler info if provided
//
// Parameters:
//   - ctx: Context for cancellation and deadline propagation
//   - input: Job finished event data
func (s *EventService) JobFinished(ctx context.Context, input JobFinishedInput) (*JobFinishedOutput, error) {
	// Validate required fields
	if input.JobID == "" {
		return nil, &ValidationError{Message: "job_id is required"}
	}

	// Get the existing job
	job, err := s.store.GetJob(ctx, input.JobID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, ErrJobNotFound
		}
		return nil, ErrInternalError
	}

	// Check if job is already in terminal state (idempotency)
	if job.State.IsTerminal() {
		return &JobFinishedOutput{
			JobID:   input.JobID,
			Status:  types.Skipped,
			Message: "Job already in terminal state",
		}, nil
	}

	// Map final state from the event
	finalState := s.mapper.APIJobStateToDomain(input.FinalState)
	if finalState == "" {
		return nil, &ValidationError{Message: "Invalid final_state"}
	}

	// Update job state
	job.State = finalState
	job.EndTime = &input.Timestamp
	job.RuntimeSeconds = input.Timestamp.Sub(job.StartTime).Seconds()
	job.Audit = audit.NewAuditInfo("slurm-epilog", "lifecycle-event")

	// Set exit code if provided
	if input.ExitCode != nil {
		if job.Scheduler == nil {
			job.Scheduler = &domain.SchedulerInfo{
				Type:          domain.SchedulerTypeSlurm,
				ExternalJobID: input.JobID,
			}
		}
		job.Scheduler.ExitCode = input.ExitCode
	}

	if err := s.store.UpdateJob(ctx, job); err != nil {
		return nil, ErrInternalError
	}

	return &JobFinishedOutput{
		JobID:   input.JobID,
		Status:  types.Updated,
		Message: "Job finished successfully",
	}, nil
}

// Helper functions

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func derefStringSlice(s *[]string) []string {
	if s == nil {
		return nil
	}
	return *s
}

// Mapper returns the mapper used by the service for handler convenience.
func (s *EventService) Mapper() *mapper.Mapper {
	return s.mapper
}
