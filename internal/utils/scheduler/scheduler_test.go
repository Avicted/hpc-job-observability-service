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

func TestMockJobSource_ListJobsWithUserFilter(t *testing.T) {
	ctx := context.Background()
	source := NewMockJobSource()

	// Add jobs with different users
	source.AddJob(&Job{ID: "job-1", User: "alice", State: JobStateRunning})
	source.AddJob(&Job{ID: "job-2", User: "bob", State: JobStateRunning})
	source.AddJob(&Job{ID: "job-3", User: "alice", State: JobStatePending})

	user := "alice"
	jobs, err := source.ListJobs(ctx, JobFilter{User: &user})
	if err != nil {
		t.Fatalf("ListJobs() error = %v", err)
	}
	if len(jobs) != 2 {
		t.Errorf("ListJobs(user=alice) returned %d jobs, want 2", len(jobs))
	}
	for _, job := range jobs {
		if job.User != "alice" {
			t.Errorf("Job %s has User = %s, want alice", job.ID, job.User)
		}
	}
}

func TestMockJobSource_GetJobMetrics_NotFound(t *testing.T) {
	ctx := context.Background()
	source := NewMockJobSource()

	metrics, err := source.GetJobMetrics(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("GetJobMetrics() error = %v", err)
	}
	if len(metrics) != 0 {
		t.Errorf("GetJobMetrics(nonexistent) returned %d samples, want 0", len(metrics))
	}
}

func TestJobState_AllStates(t *testing.T) {
	states := []JobState{
		JobStatePending,
		JobStateRunning,
		JobStateCompleted,
		JobStateFailed,
		JobStateCancelled,
	}

	expectedStrings := []string{
		"pending",
		"running",
		"completed",
		"failed",
		"cancelled",
	}

	for i, state := range states {
		if string(state) != expectedStrings[i] {
			t.Errorf("JobState %v = %s, want %s", state, string(state), expectedStrings[i])
		}
	}
}

func TestSchedulerType_AllTypes(t *testing.T) {
	types := []SchedulerType{
		SchedulerTypeMock,
		SchedulerTypeSlurm,
	}

	expectedStrings := []string{
		"mock",
		"slurm",
	}

	for i, stype := range types {
		if string(stype) != expectedStrings[i] {
			t.Errorf("SchedulerType %v = %s, want %s", stype, string(stype), expectedStrings[i])
		}
	}
}

func TestStateMapping_AllSlurmStates(t *testing.T) {
	mapper := NewSlurmStateMapping()

	// Complete list of SLURM states
	slurmStates := map[string]JobState{
		"PENDING":       JobStatePending,
		"CONFIGURING":   JobStatePending,
		"SUSPENDED":     JobStatePending,
		"REQUEUED":      JobStatePending,
		"RESV_DEL_HOLD": JobStatePending,
		"SPECIAL_EXIT":  JobStatePending,
		"RUNNING":       JobStateRunning,
		"COMPLETING":    JobStateRunning,
		"COMPLETED":     JobStateCompleted,
		"FAILED":        JobStateFailed,
		"NODE_FAIL":     JobStateFailed,
		"OUT_OF_MEMORY": JobStateFailed,
		"TIMEOUT":       JobStateFailed,
		"BOOT_FAIL":     JobStateFailed,
		"CANCELLED":     JobStateCancelled,
		"PREEMPTED":     JobStateCancelled,
		"REVOKED":       JobStateCancelled,
		"DEADLINE":      JobStateFailed,
	}

	for slurm, expected := range slurmStates {
		got, found := mapper.MapState(slurm)
		if !found {
			t.Errorf("MapState(%s) not found, expected mapping to %s", slurm, expected)
		}
		if got != expected {
			t.Errorf("MapState(%s) = %s, want %s", slurm, got, expected)
		}
	}
}

func TestMockJobSource_MultipleJobs(t *testing.T) {
	ctx := context.Background()
	source := NewMockJobSource()

	// Add various jobs
	source.AddJob(&Job{ID: "job-pending", User: "user1", State: JobStatePending, Nodes: []string{"node-01"}})
	source.AddJob(&Job{ID: "job-running", User: "user2", State: JobStateRunning, Nodes: []string{"node-02"}})
	source.AddJob(&Job{ID: "job-completed", User: "user1", State: JobStateCompleted, Nodes: []string{"node-01"}})
	source.AddJob(&Job{ID: "job-failed", User: "user3", State: JobStateFailed, Nodes: []string{"node-03"}})
	source.AddJob(&Job{ID: "job-cancelled", User: "user2", State: JobStateCancelled, Nodes: []string{"node-02"}})

	// Test listing all
	jobs, _ := source.ListJobs(ctx, JobFilter{})
	if len(jobs) != 5 {
		t.Errorf("ListJobs() returned %d jobs, want 5", len(jobs))
	}

	// Test each state filter
	stateCounts := map[JobState]int{
		JobStatePending:   1,
		JobStateRunning:   1,
		JobStateCompleted: 1,
		JobStateFailed:    1,
		JobStateCancelled: 1,
	}

	for state, expectedCount := range stateCounts {
		s := state
		jobs, _ := source.ListJobs(ctx, JobFilter{State: &s})
		if len(jobs) != expectedCount {
			t.Errorf("ListJobs(state=%s) returned %d jobs, want %d", state, len(jobs), expectedCount)
		}
	}
}

func TestMockJobSource_SchedulerInfoFields(t *testing.T) {
	ctx := context.Background()
	source := NewMockJobSource()

	priority := int64(100)
	exitCode := 0
	job := &Job{
		ID:    "detailed-job",
		User:  "testuser",
		State: JobStateRunning,
		Nodes: []string{"node-01"},
		Scheduler: &SchedulerInfo{
			Type:          SchedulerTypeMock,
			ExternalJobID: "ext-123",
			RawState:      "MOCK_RUNNING",
			Partition:     "gpu",
			Account:       "account1",
			QoS:           "high",
			Priority:      &priority,
			ExitCode:      &exitCode,
			Extra:         map[string]interface{}{"key": "value"},
		},
	}
	source.AddJob(job)

	retrieved, _ := source.GetJob(ctx, "detailed-job")
	if retrieved.Scheduler.ExternalJobID != "ext-123" {
		t.Errorf("ExternalJobID = %s, want ext-123", retrieved.Scheduler.ExternalJobID)
	}
	if retrieved.Scheduler.Partition != "gpu" {
		t.Errorf("Partition = %s, want gpu", retrieved.Scheduler.Partition)
	}
	if retrieved.Scheduler.Account != "account1" {
		t.Errorf("Account = %s, want account1", retrieved.Scheduler.Account)
	}
	if *retrieved.Scheduler.Priority != 100 {
		t.Errorf("Priority = %d, want 100", *retrieved.Scheduler.Priority)
	}
}

func TestJobFilter_CombinedFilters(t *testing.T) {
	ctx := context.Background()
	source := NewMockJobSource()

	// Add jobs with various combinations
	source.AddJob(&Job{ID: "job-1", User: "alice", State: JobStateRunning})
	source.AddJob(&Job{ID: "job-2", User: "alice", State: JobStatePending})
	source.AddJob(&Job{ID: "job-3", User: "bob", State: JobStateRunning})
	source.AddJob(&Job{ID: "job-4", User: "bob", State: JobStatePending})

	// Filter by user and state
	user := "alice"
	state := JobStateRunning
	jobs, err := source.ListJobs(ctx, JobFilter{User: &user, State: &state})
	if err != nil {
		t.Fatalf("ListJobs() error = %v", err)
	}
	if len(jobs) != 1 {
		t.Errorf("ListJobs(user=alice, state=running) returned %d jobs, want 1", len(jobs))
	}
	if len(jobs) == 1 && jobs[0].ID != "job-1" {
		t.Errorf("Expected job-1, got %s", jobs[0].ID)
	}
}
