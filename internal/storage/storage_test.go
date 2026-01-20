package storage

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestSQLiteStorage(t *testing.T) {
	// Create temporary database file
	tmpFile, err := os.CreateTemp("", "test-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	// Create storage
	store, err := NewSQLiteStorage("file:" + tmpFile.Name() + "?cache=shared&mode=rwc")
	if err != nil {
		t.Fatalf("Failed to create SQLite storage: %v", err)
	}
	defer store.Close()

	// Run migrations
	if err := store.Migrate(); err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	ctx := context.Background()

	// Test CreateJob
	t.Run("CreateJob", func(t *testing.T) {
		job := &Job{
			ID:    "test-job-001",
			User:  "testuser",
			Nodes: []string{"node-1", "node-2"},
			State: JobStateRunning,
		}
		if err := store.CreateJob(ctx, job); err != nil {
			t.Errorf("CreateJob failed: %v", err)
		}

		// Verify job was created with timestamps
		if job.CreatedAt.IsZero() {
			t.Error("CreatedAt should be set")
		}
		if job.UpdatedAt.IsZero() {
			t.Error("UpdatedAt should be set")
		}
	})

	// Test duplicate job creation
	t.Run("CreateJob_Duplicate", func(t *testing.T) {
		job := &Job{
			ID:    "test-job-001",
			User:  "testuser",
			Nodes: []string{"node-1"},
		}
		err := store.CreateJob(ctx, job)
		if err != ErrJobAlreadyExists {
			t.Errorf("Expected ErrJobAlreadyExists, got: %v", err)
		}
	})

	// Test GetJob
	t.Run("GetJob", func(t *testing.T) {
		job, err := store.GetJob(ctx, "test-job-001")
		if err != nil {
			t.Errorf("GetJob failed: %v", err)
		}
		if job.ID != "test-job-001" {
			t.Errorf("Expected ID 'test-job-001', got '%s'", job.ID)
		}
		if job.User != "testuser" {
			t.Errorf("Expected User 'testuser', got '%s'", job.User)
		}
		if len(job.Nodes) != 2 {
			t.Errorf("Expected 2 nodes, got %d", len(job.Nodes))
		}
		if job.State != JobStateRunning {
			t.Errorf("Expected state 'running', got '%s'", job.State)
		}
	})

	// Test GetJob not found
	t.Run("GetJob_NotFound", func(t *testing.T) {
		_, err := store.GetJob(ctx, "nonexistent")
		if err != ErrJobNotFound {
			t.Errorf("Expected ErrJobNotFound, got: %v", err)
		}
	})

	// Test UpdateJob
	t.Run("UpdateJob", func(t *testing.T) {
		job, _ := store.GetJob(ctx, "test-job-001")
		job.State = JobStateCompleted
		job.CPUUsage = 85.5
		job.MemoryUsageMB = 4096
		if err := store.UpdateJob(ctx, job); err != nil {
			t.Errorf("UpdateJob failed: %v", err)
		}

		// Verify update
		updated, _ := store.GetJob(ctx, "test-job-001")
		if updated.State != JobStateCompleted {
			t.Errorf("Expected state 'completed', got '%s'", updated.State)
		}
		if updated.CPUUsage != 85.5 {
			t.Errorf("Expected CPUUsage 85.5, got %f", updated.CPUUsage)
		}
		if updated.EndTime == nil {
			t.Error("EndTime should be set for completed job")
		}
		if updated.RuntimeSeconds <= 0 {
			t.Error("RuntimeSeconds should be calculated for completed job")
		}
	})

	// Test ListJobs
	t.Run("ListJobs", func(t *testing.T) {
		// Create another job
		store.CreateJob(ctx, &Job{
			ID:    "test-job-002",
			User:  "testuser",
			Nodes: []string{"node-3"},
			State: JobStateRunning,
		})

		jobs, total, err := store.ListJobs(ctx, JobFilter{Limit: 10})
		if err != nil {
			t.Errorf("ListJobs failed: %v", err)
		}
		if total != 2 {
			t.Errorf("Expected total 2, got %d", total)
		}
		if len(jobs) != 2 {
			t.Errorf("Expected 2 jobs, got %d", len(jobs))
		}
	})

	// Test ListJobs with filter
	t.Run("ListJobs_Filtered", func(t *testing.T) {
		state := JobStateRunning
		jobs, total, err := store.ListJobs(ctx, JobFilter{State: &state, Limit: 10})
		if err != nil {
			t.Errorf("ListJobs failed: %v", err)
		}
		if total != 1 {
			t.Errorf("Expected total 1, got %d", total)
		}
		if len(jobs) != 1 {
			t.Errorf("Expected 1 job, got %d", len(jobs))
		}
	})

	// Test RecordMetrics
	t.Run("RecordMetrics", func(t *testing.T) {
		sample := &MetricSample{
			JobID:         "test-job-002",
			Timestamp:     time.Now(),
			CPUUsage:      75.0,
			MemoryUsageMB: 2048,
		}
		if err := store.RecordMetrics(ctx, sample); err != nil {
			t.Errorf("RecordMetrics failed: %v", err)
		}
		if sample.ID == 0 {
			t.Error("Sample ID should be set after insert")
		}
	})

	// Test GetJobMetrics
	t.Run("GetJobMetrics", func(t *testing.T) {
		// Record a few more samples
		for i := 0; i < 5; i++ {
			store.RecordMetrics(ctx, &MetricSample{
				JobID:         "test-job-002",
				Timestamp:     time.Now().Add(time.Duration(i) * time.Minute),
				CPUUsage:      70.0 + float64(i),
				MemoryUsageMB: 2048,
			})
		}

		samples, total, err := store.GetJobMetrics(ctx, "test-job-002", MetricsFilter{Limit: 100})
		if err != nil {
			t.Errorf("GetJobMetrics failed: %v", err)
		}
		if total < 5 {
			t.Errorf("Expected at least 5 samples, got %d", total)
		}
		if len(samples) < 5 {
			t.Errorf("Expected at least 5 samples returned, got %d", len(samples))
		}
	})

	// Test GetJobMetrics for nonexistent job
	t.Run("GetJobMetrics_NotFound", func(t *testing.T) {
		_, _, err := store.GetJobMetrics(ctx, "nonexistent", MetricsFilter{Limit: 10})
		if err != ErrJobNotFound {
			t.Errorf("Expected ErrJobNotFound, got: %v", err)
		}
	})

	// Test GetLatestMetrics
	t.Run("GetLatestMetrics", func(t *testing.T) {
		sample, err := store.GetLatestMetrics(ctx, "test-job-002")
		if err != nil {
			t.Errorf("GetLatestMetrics failed: %v", err)
		}
		if sample == nil {
			t.Error("Expected a metric sample")
		}
	})

	// Test DeleteMetricsBefore
	t.Run("DeleteMetricsBefore", func(t *testing.T) {
		// All metrics should be recent, so deleting old ones should not affect count
		cutoff := time.Now().Add(-24 * time.Hour)
		if err := store.DeleteMetricsBefore(cutoff); err != nil {
			t.Errorf("DeleteMetricsBefore failed: %v", err)
		}

		// Metrics should still exist
		samples, _, _ := store.GetJobMetrics(ctx, "test-job-002", MetricsFilter{Limit: 100})
		if len(samples) == 0 {
			t.Error("Metrics should not have been deleted")
		}
	})

	// Test DeleteJob
	t.Run("DeleteJob", func(t *testing.T) {
		if err := store.DeleteJob(ctx, "test-job-002"); err != nil {
			t.Errorf("DeleteJob failed: %v", err)
		}

		// Verify deletion
		_, err := store.GetJob(ctx, "test-job-002")
		if err != ErrJobNotFound {
			t.Errorf("Expected ErrJobNotFound after deletion, got: %v", err)
		}
	})

	// Test DeleteJob not found
	t.Run("DeleteJob_NotFound", func(t *testing.T) {
		err := store.DeleteJob(ctx, "nonexistent")
		if err != ErrJobNotFound {
			t.Errorf("Expected ErrJobNotFound, got: %v", err)
		}
	})
}

func TestJobStateValidation(t *testing.T) {
	tests := []struct {
		state    JobState
		expected bool
	}{
		{JobStatePending, true},
		{JobStateRunning, true},
		{JobStateCompleted, true},
		{JobStateFailed, true},
		{JobStateCancelled, true},
		{JobState("invalid"), false},
	}

	validStates := map[JobState]bool{
		JobStatePending:   true,
		JobStateRunning:   true,
		JobStateCompleted: true,
		JobStateFailed:    true,
		JobStateCancelled: true,
	}

	for _, tc := range tests {
		t.Run(string(tc.state), func(t *testing.T) {
			_, valid := validStates[tc.state]
			if valid != tc.expected {
				t.Errorf("State %s: expected valid=%v, got %v", tc.state, tc.expected, valid)
			}
		})
	}
}
