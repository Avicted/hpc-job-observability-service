package scheduler

import (
	"context"
	"testing"
)

func TestSlurmStateMapping(t *testing.T) {
	mapper := NewSlurmStateMapping()

	tests := []struct {
		slurmState string
		expected   JobState
		found      bool
	}{
		// Pending states
		{"PENDING", JobStatePending, true},
		{"CONFIGURING", JobStatePending, true},
		{"SUSPENDED", JobStatePending, true},

		// Running states
		{"RUNNING", JobStateRunning, true},
		{"COMPLETING", JobStateRunning, true},

		// Completed states
		{"COMPLETED", JobStateCompleted, true},

		// Failed states
		{"FAILED", JobStateFailed, true},
		{"NODE_FAIL", JobStateFailed, true},
		{"OUT_OF_MEMORY", JobStateFailed, true},
		{"TIMEOUT", JobStateFailed, true},

		// Cancelled states
		{"CANCELLED", JobStateCancelled, true},
		{"PREEMPTED", JobStateCancelled, true},

		// Unknown state
		{"UNKNOWN_STATE", JobStatePending, false},
	}

	for _, tt := range tests {
		t.Run(tt.slurmState, func(t *testing.T) {
			got, found := mapper.MapState(tt.slurmState)
			if found != tt.found {
				t.Errorf("MapState(%q) found = %v, want %v", tt.slurmState, found, tt.found)
			}
			if got != tt.expected {
				t.Errorf("MapState(%q) = %v, want %v", tt.slurmState, got, tt.expected)
			}
		})
	}
}

func TestNormalizeState(t *testing.T) {
	mapper := NewSlurmStateMapping()

	// Known state
	if got := mapper.NormalizeState("RUNNING"); got != JobStateRunning {
		t.Errorf("NormalizeState(RUNNING) = %v, want %v", got, JobStateRunning)
	}

	// Unknown state defaults to pending
	if got := mapper.NormalizeState("UNKNOWN"); got != JobStatePending {
		t.Errorf("NormalizeState(UNKNOWN) = %v, want %v", got, JobStatePending)
	}
}

func TestMockJobSource(t *testing.T) {
	ctx := context.Background()
	source := NewMockJobSource()

	// Test Type
	if source.Type() != SchedulerTypeMock {
		t.Errorf("Type() = %v, want %v", source.Type(), SchedulerTypeMock)
	}

	// Test SupportsMetrics
	if !source.SupportsMetrics() {
		t.Error("SupportsMetrics() = false, want true")
	}

	// Add a job
	job := &Job{
		ID:            "test-job-001",
		User:          "testuser",
		Nodes:         []string{"node-01"},
		State:         JobStateRunning,
		CPUUsage:      50.0,
		MemoryUsageMB: 1024,
	}
	source.AddJob(job)

	// Test GetJob
	got, err := source.GetJob(ctx, "test-job-001")
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}
	if got == nil {
		t.Fatal("GetJob() returned nil")
	}
	if got.ID != job.ID {
		t.Errorf("GetJob().ID = %v, want %v", got.ID, job.ID)
	}
	if got.Scheduler == nil {
		t.Error("GetJob().Scheduler = nil, want non-nil")
	}
	if got.Scheduler != nil && got.Scheduler.Type != SchedulerTypeMock {
		t.Errorf("GetJob().Scheduler.Type = %v, want %v", got.Scheduler.Type, SchedulerTypeMock)
	}

	// Test GetJob with non-existent job
	got, err = source.GetJob(ctx, "non-existent")
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}
	if got != nil {
		t.Errorf("GetJob(non-existent) = %v, want nil", got)
	}

	// Test ListJobs
	jobs, err := source.ListJobs(ctx, JobFilter{})
	if err != nil {
		t.Fatalf("ListJobs() error = %v", err)
	}
	if len(jobs) != 1 {
		t.Errorf("ListJobs() returned %d jobs, want 1", len(jobs))
	}

	// Test ListJobs with state filter
	runningState := JobStateRunning
	jobs, err = source.ListJobs(ctx, JobFilter{State: &runningState})
	if err != nil {
		t.Fatalf("ListJobs() error = %v", err)
	}
	if len(jobs) != 1 {
		t.Errorf("ListJobs(running) returned %d jobs, want 1", len(jobs))
	}

	pendingState := JobStatePending
	jobs, err = source.ListJobs(ctx, JobFilter{State: &pendingState})
	if err != nil {
		t.Fatalf("ListJobs() error = %v", err)
	}
	if len(jobs) != 0 {
		t.Errorf("ListJobs(pending) returned %d jobs, want 0", len(jobs))
	}

	// Test AddMetrics and GetJobMetrics
	samples := []*MetricSample{
		{JobID: "test-job-001", CPUUsage: 55.0, MemoryUsageMB: 1100},
		{JobID: "test-job-001", CPUUsage: 60.0, MemoryUsageMB: 1200},
	}
	source.AddMetrics("test-job-001", samples)

	gotMetrics, err := source.GetJobMetrics(ctx, "test-job-001")
	if err != nil {
		t.Fatalf("GetJobMetrics() error = %v", err)
	}
	if len(gotMetrics) != 2 {
		t.Errorf("GetJobMetrics() returned %d samples, want 2", len(gotMetrics))
	}
}

func TestMockJobSourceGenerateDemoJobs(t *testing.T) {
	ctx := context.Background()
	source := NewMockJobSource()
	source.GenerateDemoJobs()

	jobs, err := source.ListJobs(ctx, JobFilter{})
	if err != nil {
		t.Fatalf("ListJobs() error = %v", err)
	}

	if len(jobs) < 3 {
		t.Errorf("GenerateDemoJobs() created %d jobs, want at least 3", len(jobs))
	}

	// Verify demo jobs have scheduler info
	for _, job := range jobs {
		if job.Scheduler == nil {
			t.Errorf("Demo job %s has nil Scheduler", job.ID)
			continue
		}
		if job.Scheduler.Type != SchedulerTypeMock {
			t.Errorf("Demo job %s has Scheduler.Type = %v, want %v", job.ID, job.Scheduler.Type, SchedulerTypeMock)
		}
	}
}
