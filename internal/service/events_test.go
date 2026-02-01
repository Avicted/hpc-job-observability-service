package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/api/types"
	"github.com/Avicted/hpc-job-observability-service/internal/domain"
	"github.com/Avicted/hpc-job-observability-service/internal/utils/mapper"
)

// --- EventService Tests ---

func TestEventService_JobStarted_Success(t *testing.T) {
	store := newMockStorage()
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewEventService(store, exporter, m)

	gpuVendor := types.Nvidia
	gpuDevices := []string{"GPU-0", "GPU-1"}
	gpuAlloc := 2
	cpuAlloc := 8
	memAlloc := 4096
	partition := "gpu"
	account := "research"
	cgroupPath := "/sys/fs/cgroup/system.slice/job-123"

	input := JobStartedInput{
		JobID:              "job-event-1",
		User:               "eventuser",
		NodeList:           []string{"node1", "node2"},
		Timestamp:          time.Now(),
		CgroupPath:         &cgroupPath,
		GPUVendor:          &gpuVendor,
		GPUDevices:         &gpuDevices,
		GPUAllocation:      &gpuAlloc,
		CPUAllocation:      &cpuAlloc,
		MemoryAllocationMB: &memAlloc,
		Partition:          &partition,
		Account:            &account,
	}

	output, err := svc.JobStarted(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.JobID != "job-event-1" {
		t.Errorf("JobID = %q, want %q", output.JobID, "job-event-1")
	}
	if output.Status != types.Created {
		t.Errorf("Status = %v, want %v", output.Status, types.Created)
	}
	if output.Message != "Job registered successfully" {
		t.Errorf("Message = %q, want %q", output.Message, "Job registered successfully")
	}

	// Verify job was created in storage
	job, err := store.GetJob(context.Background(), "job-event-1")
	if err != nil {
		t.Fatalf("job should exist in storage: %v", err)
	}

	if job.User != "eventuser" {
		t.Errorf("User = %q, want %q", job.User, "eventuser")
	}
	if job.State != domain.JobStateRunning {
		t.Errorf("State = %v, want %v", job.State, domain.JobStateRunning)
	}
	if job.NodeCount != 2 {
		t.Errorf("NodeCount = %d, want 2", job.NodeCount)
	}
	if job.CgroupPath != cgroupPath {
		t.Errorf("CgroupPath = %q, want %q", job.CgroupPath, cgroupPath)
	}
	if job.GPUVendor != domain.GPUVendorNvidia {
		t.Errorf("GPUVendor = %v, want %v", job.GPUVendor, domain.GPUVendorNvidia)
	}
	if job.GPUCount != 2 {
		t.Errorf("GPUCount = %d, want 2", job.GPUCount)
	}
	if job.AllocatedCPUs != 8 {
		t.Errorf("AllocatedCPUs = %d, want 8", job.AllocatedCPUs)
	}
	if job.AllocatedMemMB != 4096 {
		t.Errorf("AllocatedMemMB = %d, want 4096", job.AllocatedMemMB)
	}
	if job.Scheduler == nil {
		t.Fatal("Scheduler should not be nil")
	}
	if job.Scheduler.Type != domain.SchedulerTypeSlurm {
		t.Errorf("Scheduler.Type = %v, want slurm", job.Scheduler.Type)
	}
	if job.Scheduler.Partition != "gpu" {
		t.Errorf("Scheduler.Partition = %q, want gpu", job.Scheduler.Partition)
	}
}

func TestEventService_JobStarted_ValidationErrors(t *testing.T) {
	store := newMockStorage()
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewEventService(store, exporter, m)

	tests := []struct {
		name    string
		input   JobStartedInput
		wantErr string
	}{
		{
			name:    "empty JobID",
			input:   JobStartedInput{JobID: "", User: "user", NodeList: []string{"n1"}, Timestamp: time.Now()},
			wantErr: "job_id is required",
		},
		{
			name:    "empty User",
			input:   JobStartedInput{JobID: "job-1", User: "", NodeList: []string{"n1"}, Timestamp: time.Now()},
			wantErr: "user is required",
		},
		{
			name:    "empty NodeList",
			input:   JobStartedInput{JobID: "job-1", User: "user", NodeList: []string{}, Timestamp: time.Now()},
			wantErr: "node_list is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := svc.JobStarted(context.Background(), tt.input)
			if err == nil {
				t.Fatal("expected error")
			}

			var validErr *ValidationError
			if !errors.As(err, &validErr) {
				t.Fatalf("expected ValidationError, got %T", err)
			}

			if validErr.Message != tt.wantErr {
				t.Errorf("error message = %q, want %q", validErr.Message, tt.wantErr)
			}
		})
	}
}

func TestEventService_JobStarted_Idempotent(t *testing.T) {
	store := newMockStorage()
	// Pre-populate with existing job
	store.jobs["job-dup"] = &domain.Job{ID: "job-dup", User: "existinguser", State: domain.JobStateRunning}
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewEventService(store, exporter, m)

	input := JobStartedInput{
		JobID:     "job-dup",
		User:      "newuser",
		NodeList:  []string{"n1"},
		Timestamp: time.Now(),
	}

	output, err := svc.JobStarted(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Status != types.Skipped {
		t.Errorf("Status = %v, want %v", output.Status, types.Skipped)
	}
	if output.Message != "Job already registered" {
		t.Errorf("Message = %q, want %q", output.Message, "Job already registered")
	}

	// Verify original job wasn't modified
	job := store.jobs["job-dup"]
	if job.User != "existinguser" {
		t.Errorf("User = %q, want existinguser (original)", job.User)
	}
}

func TestEventService_JobStarted_MinimalInput(t *testing.T) {
	store := newMockStorage()
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewEventService(store, exporter, m)

	input := JobStartedInput{
		JobID:     "job-minimal",
		User:      "user",
		NodeList:  []string{"node1"},
		Timestamp: time.Now(),
	}

	output, err := svc.JobStarted(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Status != types.Created {
		t.Errorf("Status = %v, want %v", output.Status, types.Created)
	}

	job := store.jobs["job-minimal"]
	if job.GPUVendor != domain.GPUVendorNone {
		t.Errorf("GPUVendor = %v, want none (default)", job.GPUVendor)
	}
}

func TestEventService_JobFinished_Success(t *testing.T) {
	store := newMockStorage()
	startTime := time.Now().Add(-time.Hour)
	store.jobs["job-finish"] = &domain.Job{
		ID:        "job-finish",
		User:      "user",
		State:     domain.JobStateRunning,
		StartTime: startTime,
	}
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewEventService(store, exporter, m)

	exitCode := 0
	endTime := time.Now()

	input := JobFinishedInput{
		JobID:      "job-finish",
		FinalState: types.Completed,
		Timestamp:  endTime,
		ExitCode:   &exitCode,
	}

	output, err := svc.JobFinished(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.JobID != "job-finish" {
		t.Errorf("JobID = %q, want %q", output.JobID, "job-finish")
	}
	if output.Status != types.Updated {
		t.Errorf("Status = %v, want %v", output.Status, types.Updated)
	}
	if output.Message != "Job finished successfully" {
		t.Errorf("Message = %q, want %q", output.Message, "Job finished successfully")
	}

	// Verify job was updated
	job := store.jobs["job-finish"]
	if job.State != domain.JobStateCompleted {
		t.Errorf("State = %v, want %v", job.State, domain.JobStateCompleted)
	}
	if job.EndTime == nil {
		t.Error("EndTime should not be nil")
	}
	if job.RuntimeSeconds <= 0 {
		t.Errorf("RuntimeSeconds = %v, should be > 0", job.RuntimeSeconds)
	}
	if job.Scheduler == nil {
		t.Fatal("Scheduler should not be nil")
	}
	if job.Scheduler.ExitCode == nil || *job.Scheduler.ExitCode != 0 {
		t.Errorf("Scheduler.ExitCode = %v, want 0", job.Scheduler.ExitCode)
	}
}

func TestEventService_JobFinished_ValidationErrors(t *testing.T) {
	store := newMockStorage()
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewEventService(store, exporter, m)

	_, err := svc.JobFinished(context.Background(), JobFinishedInput{
		JobID:      "",
		FinalState: types.Completed,
		Timestamp:  time.Now(),
	})
	if err == nil {
		t.Fatal("expected error for empty job_id")
	}

	var validErr *ValidationError
	if !errors.As(err, &validErr) {
		t.Fatalf("expected ValidationError, got %T", err)
	}
	if validErr.Message != "job_id is required" {
		t.Errorf("error message = %q, want %q", validErr.Message, "job_id is required")
	}
}

func TestEventService_JobFinished_NotFound(t *testing.T) {
	store := newMockStorage()
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewEventService(store, exporter, m)

	_, err := svc.JobFinished(context.Background(), JobFinishedInput{
		JobID:      "nonexistent",
		FinalState: types.Completed,
		Timestamp:  time.Now(),
	})
	if !errors.Is(err, ErrJobNotFound) {
		t.Errorf("expected ErrJobNotFound, got %v", err)
	}
}

func TestEventService_JobFinished_Idempotent(t *testing.T) {
	store := newMockStorage()
	endTime := time.Now()
	store.jobs["job-terminal"] = &domain.Job{
		ID:      "job-terminal",
		User:    "user",
		State:   domain.JobStateCompleted, // Already terminal
		EndTime: &endTime,
	}
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewEventService(store, exporter, m)

	output, err := svc.JobFinished(context.Background(), JobFinishedInput{
		JobID:      "job-terminal",
		FinalState: types.Failed, // Try to change to different terminal state
		Timestamp:  time.Now(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Status != types.Skipped {
		t.Errorf("Status = %v, want %v", output.Status, types.Skipped)
	}
	if output.Message != "Job already in terminal state" {
		t.Errorf("Message = %q, want %q", output.Message, "Job already in terminal state")
	}

	// Verify job wasn't changed
	job := store.jobs["job-terminal"]
	if job.State != domain.JobStateCompleted {
		t.Errorf("State = %v, want completed (unchanged)", job.State)
	}
}

func TestEventService_JobFinished_InvalidState(t *testing.T) {
	store := newMockStorage()
	store.jobs["job-invalid-state"] = &domain.Job{
		ID:    "job-invalid-state",
		User:  "user",
		State: domain.JobStateRunning,
	}
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewEventService(store, exporter, m)

	_, err := svc.JobFinished(context.Background(), JobFinishedInput{
		JobID:      "job-invalid-state",
		FinalState: types.JobState("invalid"),
		Timestamp:  time.Now(),
	})
	if err == nil {
		t.Fatal("expected error for invalid state")
	}

	var validErr *ValidationError
	if !errors.As(err, &validErr) {
		t.Fatalf("expected ValidationError, got %T", err)
	}
}

func TestEventService_JobFinished_AllTerminalStates(t *testing.T) {
	store := newMockStorage()
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewEventService(store, exporter, m)

	terminalStates := []struct {
		apiState    types.JobState
		domainState domain.JobState
	}{
		{types.Completed, domain.JobStateCompleted},
		{types.Failed, domain.JobStateFailed},
		{types.Cancelled, domain.JobStateCancelled},
	}

	for _, tt := range terminalStates {
		t.Run(string(tt.apiState), func(t *testing.T) {
			jobID := "job-" + string(tt.apiState)
			store.jobs[jobID] = &domain.Job{
				ID:        jobID,
				User:      "user",
				State:     domain.JobStateRunning,
				StartTime: time.Now().Add(-time.Minute),
			}

			output, err := svc.JobFinished(context.Background(), JobFinishedInput{
				JobID:      jobID,
				FinalState: tt.apiState,
				Timestamp:  time.Now(),
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if output.Status != types.Updated {
				t.Errorf("Status = %v, want %v", output.Status, types.Updated)
			}

			job := store.jobs[jobID]
			if job.State != tt.domainState {
				t.Errorf("State = %v, want %v", job.State, tt.domainState)
			}
		})
	}
}

func TestEventService_JobFinished_WithExitCodeOnExistingScheduler(t *testing.T) {
	store := newMockStorage()
	store.jobs["job-sched"] = &domain.Job{
		ID:        "job-sched",
		User:      "user",
		State:     domain.JobStateRunning,
		StartTime: time.Now().Add(-time.Minute),
		Scheduler: &domain.SchedulerInfo{
			Type:          domain.SchedulerTypeSlurm,
			ExternalJobID: "slurm-123",
			Partition:     "compute",
		},
	}
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewEventService(store, exporter, m)

	exitCode := 1

	output, err := svc.JobFinished(context.Background(), JobFinishedInput{
		JobID:      "job-sched",
		FinalState: types.Failed,
		Timestamp:  time.Now(),
		ExitCode:   &exitCode,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Status != types.Updated {
		t.Errorf("Status = %v, want %v", output.Status, types.Updated)
	}

	// Verify existing scheduler info preserved and exit code added
	job := store.jobs["job-sched"]
	if job.Scheduler.ExternalJobID != "slurm-123" {
		t.Errorf("ExternalJobID = %q, want slurm-123 (preserved)", job.Scheduler.ExternalJobID)
	}
	if job.Scheduler.Partition != "compute" {
		t.Errorf("Partition = %q, want compute (preserved)", job.Scheduler.Partition)
	}
	if job.Scheduler.ExitCode == nil || *job.Scheduler.ExitCode != 1 {
		t.Errorf("ExitCode = %v, want 1", job.Scheduler.ExitCode)
	}
}

func TestEventService_Mapper(t *testing.T) {
	store := newMockStorage()
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewEventService(store, exporter, m)

	if svc.Mapper() != m {
		t.Error("expected Mapper() to return the injected mapper")
	}
}

func TestDerefString(t *testing.T) {
	val := "test"
	if got := derefString(&val); got != "test" {
		t.Errorf("derefString(&val) = %q, want test", got)
	}
	if got := derefString(nil); got != "" {
		t.Errorf("derefString(nil) = %q, want empty string", got)
	}
}

func TestDerefStringSlice(t *testing.T) {
	val := []string{"a", "b"}
	if got := derefStringSlice(&val); len(got) != 2 {
		t.Errorf("derefStringSlice(&val) len = %d, want 2", len(got))
	}
	if got := derefStringSlice(nil); got != nil {
		t.Errorf("derefStringSlice(nil) = %v, want nil", got)
	}
}

// TestEventService_JobStarted_GetJobInternalError tests the internal error path
// when GetJob fails with an error other than ErrJobNotFound.
func TestEventService_JobStarted_GetJobInternalError(t *testing.T) {
	store := newMockStorage()
	store.getErr = errors.New("database connection failed")
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewEventService(store, exporter, m)

	input := JobStartedInput{
		JobID:     "job-internal-error",
		User:      "user",
		NodeList:  []string{"node1"},
		Timestamp: time.Now(),
	}

	_, err := svc.JobStarted(context.Background(), input)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// Error should be wrapped, not ErrInternalError
	expectedMsg := "failed to check if job exists: database connection failed"
	if err.Error() != expectedMsg {
		t.Errorf("expected error message %q, got %q", expectedMsg, err.Error())
	}
}

// TestEventService_JobStarted_CreateJobInternalError tests the internal error path
// when CreateJob fails with an error other than ErrJobAlreadyExists.
func TestEventService_JobStarted_CreateJobInternalError(t *testing.T) {
	store := newMockStorage()
	store.createErr = errors.New("database write failed")
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewEventService(store, exporter, m)

	input := JobStartedInput{
		JobID:     "job-create-error",
		User:      "user",
		NodeList:  []string{"node1"},
		Timestamp: time.Now(),
	}

	_, err := svc.JobStarted(context.Background(), input)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// Error should be wrapped
	expectedMsg := "failed to create job: database write failed"
	if err.Error() != expectedMsg {
		t.Errorf("expected error message %q, got %q", expectedMsg, err.Error())
	}
}

// TestEventService_JobFinished_GetJobInternalError tests the internal error path
// when GetJob fails with an error other than ErrJobNotFound.
func TestEventService_JobFinished_GetJobInternalError(t *testing.T) {
	store := newMockStorage()
	store.getErr = errors.New("database connection failed")
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewEventService(store, exporter, m)

	input := JobFinishedInput{
		JobID:      "job-internal-error",
		FinalState: types.Completed,
		Timestamp:  time.Now(),
	}

	_, err := svc.JobFinished(context.Background(), input)
	if !errors.Is(err, ErrInternalError) {
		t.Errorf("expected ErrInternalError, got %v", err)
	}
}

// TestEventService_JobFinished_UpdateJobInternalError tests the internal error path
// when UpdateJob fails.
func TestEventService_JobFinished_UpdateJobInternalError(t *testing.T) {
	store := newMockStorage()
	// Add a running job first
	store.jobs["job-update-error"] = &domain.Job{
		ID:        "job-update-error",
		User:      "user",
		State:     domain.JobStateRunning,
		StartTime: time.Now().Add(-time.Hour),
	}
	store.updateErr = errors.New("database write failed")
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewEventService(store, exporter, m)

	input := JobFinishedInput{
		JobID:      "job-update-error",
		FinalState: types.Completed,
		Timestamp:  time.Now(),
	}

	_, err := svc.JobFinished(context.Background(), input)
	if !errors.Is(err, ErrInternalError) {
		t.Errorf("expected ErrInternalError, got %v", err)
	}
}
