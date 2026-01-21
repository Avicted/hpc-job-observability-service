package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
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
	store, err := NewSQLiteStorage("file:" + tmpFile.Name() + "?cache=shared&mode=rwc&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("Failed to create SQLite storage: %v", err)
	}
	defer store.Close()

	// Run migrations
	if err := store.Migrate(); err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	ctx := WithAuditInfo(context.Background(), NewAuditInfo("test", "storage-test"))

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
		_ = store.CreateJob(ctx, &Job{
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
			_ = store.RecordMetrics(ctx, &MetricSample{
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

func TestStressConcurrentJobs(t *testing.T) {
	if os.Getenv("STRESS_TEST") != "1" {
		t.Skip("Skipping stress test; set STRESS_TEST=1 to run")
	}

	// Create temporary database file
	tmpFile, err := os.CreateTemp("", "stress-test-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	store, err := NewSQLiteStorage("file:" + tmpFile.Name() + "?cache=shared&mode=rwc")
	if err != nil {
		t.Fatalf("Failed to create SQLite storage: %v", err)
	}
	defer store.Close()

	if err := store.Migrate(); err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	ctx := WithAuditInfo(context.Background(), NewAuditInfo("test", "storage-test"))
	jobCount := 5000
	workers := 100
	metricsSamplesPerJob := 3

	jobsCh := make(chan int)
	var createErrors int64
	var createFirstErr string
	var createOnce sync.Once
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobsCh {
				job := &Job{
					ID:            fmt.Sprintf("stress-job-%05d", i),
					User:          fmt.Sprintf("user-%02d", i%10),
					Nodes:         []string{fmt.Sprintf("node-%02d", i%50)},
					State:         JobStateRunning,
					StartTime:     time.Now(),
					CPUUsage:      50.0,
					MemoryUsageMB: 2048,
				}
				if err := retryIfBusy(func() error { return store.CreateJob(ctx, job) }); err != nil {
					createOnce.Do(func() { createFirstErr = err.Error() })
					atomic.AddInt64(&createErrors, 1)
				}
			}
		}()
	}

	for i := 0; i < jobCount; i++ {
		jobsCh <- i
	}
	close(jobsCh)
	wg.Wait()

	if createErrors != 0 {
		t.Fatalf("CreateJob errors: %d (first error: %s)", createErrors, createFirstErr)
	}

	jobs, err := store.GetAllJobs(ctx)
	if err != nil {
		t.Fatalf("GetAllJobs failed: %v", err)
	}
	if len(jobs) != jobCount {
		t.Fatalf("Expected %d jobs, got %d", jobCount, len(jobs))
	}

	var metricErrors int64
	metricsCh := make(chan int)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range metricsCh {
				jobID := fmt.Sprintf("stress-job-%05d", i)
				for s := 0; s < metricsSamplesPerJob; s++ {
					sample := &MetricSample{
						JobID:         jobID,
						Timestamp:     time.Now(),
						CPUUsage:      60.0,
						MemoryUsageMB: 3072,
					}
					if err := retryIfBusy(func() error { return store.RecordMetrics(ctx, sample) }); err != nil {
						atomic.AddInt64(&metricErrors, 1)
						break
					}
				}
			}
		}()
	}

	for i := 0; i < jobCount; i++ {
		metricsCh <- i
	}
	close(metricsCh)
	wg.Wait()

	if metricErrors != 0 {
		t.Fatalf("RecordMetrics errors: %d", metricErrors)
	}

	// Spot-check metrics for a single job
	samples, total, err := store.GetJobMetrics(ctx, "stress-job-00000", MetricsFilter{Limit: 100})
	if err != nil {
		t.Fatalf("GetJobMetrics failed: %v", err)
	}
	if total < metricsSamplesPerJob || len(samples) < metricsSamplesPerJob {
		t.Fatalf("Expected at least %d samples, got total=%d len=%d", metricsSamplesPerJob, total, len(samples))
	}
}

func retryIfBusy(fn func() error) error {
	const maxAttempts = 10
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}
		msg := err.Error()
		if !strings.Contains(msg, "database is locked") &&
			!strings.Contains(msg, "database is busy") &&
			!strings.Contains(msg, "database table is locked") {
			return err
		}
		time.Sleep(time.Duration(attempt+1) * 20 * time.Millisecond)
	}
	return err
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

func TestSQLiteStorage_AdditionalFilters(t *testing.T) {
	// Create temporary database file
	tmpFile, err := os.CreateTemp("", "test-filter-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	store, err := NewSQLiteStorage("file:" + tmpFile.Name() + "?cache=shared&mode=rwc&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("Failed to create SQLite storage: %v", err)
	}
	defer store.Close()

	if err := store.Migrate(); err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	ctx := WithAuditInfo(context.Background(), NewAuditInfo("test", "storage-test"))

	// Create test jobs for filtering
	for i := 0; i < 10; i++ {
		state := JobStateRunning
		if i%2 == 0 {
			state = JobStateCompleted
		}
		_ = store.CreateJob(ctx, &Job{
			ID:        fmt.Sprintf("filter-job-%02d", i),
			User:      fmt.Sprintf("user-%d", i%3),
			Nodes:     []string{fmt.Sprintf("node-%02d", i%5)},
			State:     state,
			StartTime: time.Now(),
		})
	}

	// Test ListJobs with user filter
	t.Run("ListJobs_UserFilter", func(t *testing.T) {
		user := "user-0"
		jobs, total, err := store.ListJobs(ctx, JobFilter{User: &user, Limit: 50})
		if err != nil {
			t.Errorf("ListJobs failed: %v", err)
		}
		for _, job := range jobs {
			if job.User != "user-0" {
				t.Errorf("Expected user 'user-0', got '%s'", job.User)
			}
		}
		if total < 1 {
			t.Error("Expected at least 1 job for user-0")
		}
	})

	// Test ListJobs with node filter
	t.Run("ListJobs_NodeFilter", func(t *testing.T) {
		node := "node-00"
		jobs, _, err := store.ListJobs(ctx, JobFilter{Node: &node, Limit: 50})
		if err != nil {
			t.Errorf("ListJobs failed: %v", err)
		}
		for _, job := range jobs {
			hasNode := false
			for _, n := range job.Nodes {
				if n == "node-00" {
					hasNode = true
					break
				}
			}
			if !hasNode {
				t.Errorf("Job %s does not have node 'node-00'", job.ID)
			}
		}
	})

	// Test ListJobs with pagination
	t.Run("ListJobs_Pagination", func(t *testing.T) {
		// First page
		jobs1, total, err := store.ListJobs(ctx, JobFilter{Limit: 3, Offset: 0})
		if err != nil {
			t.Errorf("ListJobs failed: %v", err)
		}
		if len(jobs1) != 3 {
			t.Errorf("Expected 3 jobs on first page, got %d", len(jobs1))
		}
		if total != 10 {
			t.Errorf("Expected total 10, got %d", total)
		}

		// Second page
		jobs2, _, err := store.ListJobs(ctx, JobFilter{Limit: 3, Offset: 3})
		if err != nil {
			t.Errorf("ListJobs failed: %v", err)
		}
		if len(jobs2) != 3 {
			t.Errorf("Expected 3 jobs on second page, got %d", len(jobs2))
		}

		// Verify no overlap
		for _, j1 := range jobs1 {
			for _, j2 := range jobs2 {
				if j1.ID == j2.ID {
					t.Errorf("Job %s appears on multiple pages", j1.ID)
				}
			}
		}
	})

	// Test GetAllJobs
	t.Run("GetAllJobs", func(t *testing.T) {
		jobs, err := store.GetAllJobs(ctx)
		if err != nil {
			t.Errorf("GetAllJobs failed: %v", err)
		}
		if len(jobs) != 10 {
			t.Errorf("Expected 10 jobs, got %d", len(jobs))
		}
	})
}

func TestSQLiteStorage_MetricsFilters(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-metrics-filter-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	store, err := NewSQLiteStorage("file:" + tmpFile.Name() + "?cache=shared&mode=rwc&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("Failed to create SQLite storage: %v", err)
	}
	defer store.Close()

	if err := store.Migrate(); err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	ctx := WithAuditInfo(context.Background(), NewAuditInfo("test", "storage-test"))

	// Create a job
	_ = store.CreateJob(ctx, &Job{
		ID:        "metrics-filter-job",
		User:      "testuser",
		Nodes:     []string{"node-1"},
		State:     JobStateRunning,
		StartTime: time.Now(),
	})

	// Record metrics at different timestamps
	baseTime := time.Now().Add(-1 * time.Hour)
	for i := 0; i < 20; i++ {
		_ = store.RecordMetrics(ctx, &MetricSample{
			JobID:         "metrics-filter-job",
			Timestamp:     baseTime.Add(time.Duration(i) * 5 * time.Minute),
			CPUUsage:      50.0 + float64(i),
			MemoryUsageMB: int64(1024 + i*100),
		})
	}

	// Test GetJobMetrics with time filter
	t.Run("GetJobMetrics_TimeFilter", func(t *testing.T) {
		startTime := baseTime.Add(30 * time.Minute)
		endTime := baseTime.Add(60 * time.Minute)
		samples, total, err := store.GetJobMetrics(ctx, "metrics-filter-job", MetricsFilter{
			StartTime: &startTime,
			EndTime:   &endTime,
			Limit:     50,
		})
		if err != nil {
			t.Errorf("GetJobMetrics failed: %v", err)
		}
		// Should have samples between 30min and 60min marks
		for _, sample := range samples {
			if sample.Timestamp.Before(startTime) || sample.Timestamp.After(endTime) {
				t.Errorf("Sample timestamp %v outside range [%v, %v]", sample.Timestamp, startTime, endTime)
			}
		}
		_ = total
	})

	// Test GetJobMetrics with limit
	t.Run("GetJobMetrics_Limit", func(t *testing.T) {
		samples, total, err := store.GetJobMetrics(ctx, "metrics-filter-job", MetricsFilter{Limit: 5})
		if err != nil {
			t.Errorf("GetJobMetrics failed: %v", err)
		}
		if len(samples) != 5 {
			t.Errorf("Expected 5 samples, got %d", len(samples))
		}
		if total != 20 {
			t.Errorf("Expected total 20, got %d", total)
		}
	})

	// Test GetLatestMetrics for job with no metrics
	t.Run("GetLatestMetrics_NoMetrics", func(t *testing.T) {
		_ = store.CreateJob(ctx, &Job{
			ID:        "empty-metrics-job",
			User:      "testuser",
			Nodes:     []string{"node-1"},
			State:     JobStateRunning,
			StartTime: time.Now(),
		})
		sample, err := store.GetLatestMetrics(ctx, "empty-metrics-job")
		if err != nil {
			t.Errorf("GetLatestMetrics failed: %v", err)
		}
		if sample != nil {
			t.Error("Expected nil sample for job with no metrics")
		}
	})

	// Test DeleteMetricsBefore
	t.Run("DeleteMetricsBefore_Old", func(t *testing.T) {
		// Delete metrics older than 50 minutes ago
		cutoff := baseTime.Add(50 * time.Minute)
		if err := store.DeleteMetricsBefore(cutoff); err != nil {
			t.Errorf("DeleteMetricsBefore failed: %v", err)
		}

		// Should have fewer metrics now
		samples, total, _ := store.GetJobMetrics(ctx, "metrics-filter-job", MetricsFilter{Limit: 100})
		if total >= 20 {
			t.Errorf("Expected fewer than 20 metrics after deletion, got %d", total)
		}
		_ = samples
	})
}

func TestSQLiteStorage_SchedulerInfo(t *testing.T) {
	// Skip this test since scheduler info persistence is not yet implemented in SQLite storage
	// The scheduler info is handled at the API level and stored separately
	t.Skip("Scheduler info persistence not yet implemented in SQLite storage layer")
}

func TestSQLiteStorage_GPUUsage(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-gpu-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	store, err := NewSQLiteStorage("file:" + tmpFile.Name() + "?cache=shared&mode=rwc&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("Failed to create SQLite storage: %v", err)
	}
	defer store.Close()

	if err := store.Migrate(); err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	ctx := WithAuditInfo(context.Background(), NewAuditInfo("test", "storage-test"))

	// Test job with GPU usage
	t.Run("Job_WithGPU", func(t *testing.T) {
		gpuUsage := 75.5
		job := &Job{
			ID:        "gpu-job",
			User:      "testuser",
			Nodes:     []string{"gpu-node-1"},
			State:     JobStateRunning,
			StartTime: time.Now(),
			GPUUsage:  &gpuUsage,
		}
		if err := store.CreateJob(ctx, job); err != nil {
			t.Errorf("CreateJob failed: %v", err)
		}

		retrieved, _ := store.GetJob(ctx, "gpu-job")
		if retrieved.GPUUsage == nil {
			t.Fatal("Expected GPU usage to be set")
		}
		if *retrieved.GPUUsage != 75.5 {
			t.Errorf("Expected GPU usage 75.5, got %f", *retrieved.GPUUsage)
		}
	})

	// Test metrics with GPU
	t.Run("Metrics_WithGPU", func(t *testing.T) {
		gpuUsage := 80.0
		sample := &MetricSample{
			JobID:         "gpu-job",
			Timestamp:     time.Now(),
			CPUUsage:      50.0,
			MemoryUsageMB: 1024,
			GPUUsage:      &gpuUsage,
		}
		if err := store.RecordMetrics(ctx, sample); err != nil {
			t.Errorf("RecordMetrics failed: %v", err)
		}

		samples, _, _ := store.GetJobMetrics(ctx, "gpu-job", MetricsFilter{Limit: 10})
		if len(samples) == 0 {
			t.Fatal("Expected at least one sample")
		}
		if samples[0].GPUUsage == nil {
			t.Fatal("Expected GPU usage in sample")
		}
		if *samples[0].GPUUsage != 80.0 {
			t.Errorf("Expected GPU usage 80.0, got %f", *samples[0].GPUUsage)
		}
	})
}

func TestSQLiteStorage_UpdateJobNonexistent(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-update-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	store, err := NewSQLiteStorage("file:" + tmpFile.Name() + "?cache=shared&mode=rwc&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("Failed to create SQLite storage: %v", err)
	}
	defer store.Close()

	if err := store.Migrate(); err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	ctx := WithAuditInfo(context.Background(), NewAuditInfo("test", "storage-test"))

	job := &Job{
		ID:    "nonexistent-job",
		User:  "testuser",
		Nodes: []string{"node-1"},
		State: JobStateRunning,
	}
	err = store.UpdateJob(ctx, job)
	if err != ErrJobNotFound {
		t.Errorf("Expected ErrJobNotFound, got: %v", err)
	}
}

func TestSQLiteStorage_SeedDemoData(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-demo-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	store, err := NewSQLiteStorage("file:" + tmpFile.Name() + "?cache=shared&mode=rwc&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("Failed to create SQLite storage: %v", err)
	}
	defer store.Close()

	if err := store.Migrate(); err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	ctx := WithAuditInfo(context.Background(), NewAuditInfo("test", "storage-test"))

	// Seed demo data
	if err := store.SeedDemoData(); err != nil {
		t.Errorf("SeedDemoData failed: %v", err)
	}

	// Verify jobs were created - expect 100 demo jobs
	jobs, err := store.GetAllJobs(ctx)
	if err != nil {
		t.Errorf("GetAllJobs failed: %v", err)
	}
	if len(jobs) != 100 {
		t.Errorf("Expected 100 demo jobs, got %d", len(jobs))
	}

	// Verify jobs have varied states
	stateCounts := make(map[JobState]int)
	for _, job := range jobs {
		stateCounts[job.State]++
	}
	// Should have jobs in multiple states
	if len(stateCounts) < 3 {
		t.Errorf("Expected jobs in multiple states, got %d unique states", len(stateCounts))
	}

	// Run again to exercise duplicate handling
	if err := store.SeedDemoData(); err != nil {
		t.Errorf("SeedDemoData second run failed: %v", err)
	}
}

func TestStorage_New_Unsupported(t *testing.T) {
	_, err := New("unknown", "")
	if err == nil {
		t.Fatal("expected error for unsupported db type")
	}
}

func TestStorage_New_SQLite(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-new-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	store, err := New("sqlite", "file:"+tmpFile.Name()+"?cache=shared&mode=rwc")
	if err != nil {
		t.Fatalf("New(sqlite) failed: %v", err)
	}
	defer store.Close()
}

func TestStorage_New_Postgres_Error(t *testing.T) {
	_, err := New("postgres", "postgres://invalid:invalid@localhost:1/db?sslmode=disable&connect_timeout=1")
	if err == nil {
		t.Fatal("expected error for postgres connection")
	}
}

func TestSQLiteStorage_CreateJob_Defaults(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-defaults-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	store, err := NewSQLiteStorage("file:" + tmpFile.Name() + "?cache=shared&mode=rwc&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("Failed to create SQLite storage: %v", err)
	}
	defer store.Close()

	if err := store.Migrate(); err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	ctx := WithAuditInfo(context.Background(), NewAuditInfo("test", "storage-test"))
	job := &Job{ID: "defaults-job", User: "testuser", Nodes: []string{"node-1"}}
	if err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}
	if job.State != JobStatePending {
		t.Errorf("Expected default state pending, got %s", job.State)
	}
	if job.StartTime.IsZero() {
		t.Error("Expected StartTime to be set")
	}
	if job.CreatedAt.IsZero() || job.UpdatedAt.IsZero() {
		t.Error("Expected CreatedAt and UpdatedAt to be set")
	}
}

func TestSQLiteStorage_RecordMetrics_DefaultTimestamp(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-metric-ts-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	store, err := NewSQLiteStorage("file:" + tmpFile.Name() + "?cache=shared&mode=rwc&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("Failed to create SQLite storage: %v", err)
	}
	defer store.Close()

	if err := store.Migrate(); err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	ctx := WithAuditInfo(context.Background(), NewAuditInfo("test", "storage-test"))
	if err := store.CreateJob(ctx, &Job{ID: "metric-ts-job", User: "u", Nodes: []string{"n"}, State: JobStateRunning, StartTime: time.Now()}); err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	sample := &MetricSample{JobID: "metric-ts-job", CPUUsage: 10, MemoryUsageMB: 128}
	if err := store.RecordMetrics(ctx, sample); err != nil {
		t.Fatalf("RecordMetrics failed: %v", err)
	}
	if sample.Timestamp.IsZero() {
		t.Fatal("Expected RecordMetrics to set timestamp")
	}
}

func TestSQLiteStorage_GetLatestMetrics_WithGPU(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-latest-gpu-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	store, err := NewSQLiteStorage("file:" + tmpFile.Name() + "?cache=shared&mode=rwc&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("Failed to create SQLite storage: %v", err)
	}
	defer store.Close()

	if err := store.Migrate(); err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	ctx := WithAuditInfo(context.Background(), NewAuditInfo("test", "storage-test"))
	if err := store.CreateJob(ctx, &Job{ID: "latest-gpu", User: "u", Nodes: []string{"n"}, State: JobStateRunning, StartTime: time.Now()}); err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}
	gpu := 55.0
	if err := store.RecordMetrics(ctx, &MetricSample{JobID: "latest-gpu", Timestamp: time.Now(), CPUUsage: 10, MemoryUsageMB: 128, GPUUsage: &gpu}); err != nil {
		t.Fatalf("RecordMetrics failed: %v", err)
	}

	sample, err := store.GetLatestMetrics(ctx, "latest-gpu")
	if err != nil {
		t.Fatalf("GetLatestMetrics failed: %v", err)
	}
	if sample == nil || sample.GPUUsage == nil {
		t.Fatal("Expected GPU usage in latest sample")
	}
}

func TestSQLiteStorage_GetJobMetrics_LimitCap(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-metric-limit-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	store, err := NewSQLiteStorage("file:" + tmpFile.Name() + "?cache=shared&mode=rwc&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("Failed to create SQLite storage: %v", err)
	}
	defer store.Close()

	if err := store.Migrate(); err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	ctx := WithAuditInfo(context.Background(), NewAuditInfo("test", "storage-test"))
	if err := store.CreateJob(ctx, &Job{ID: "metric-limit-job", User: "u", Nodes: []string{"n"}, State: JobStateRunning, StartTime: time.Now()}); err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	for i := 0; i < 5; i++ {
		_ = store.RecordMetrics(ctx, &MetricSample{JobID: "metric-limit-job", Timestamp: time.Now(), CPUUsage: 10, MemoryUsageMB: 128})
	}

	_, _, err = store.GetJobMetrics(ctx, "metric-limit-job", MetricsFilter{Limit: 20000})
	if err != nil {
		t.Fatalf("GetJobMetrics failed: %v", err)
	}
}

func TestStorage_HelperFunctions(t *testing.T) {
	if got := clamp(-1, 0, 10); got != 0 {
		t.Errorf("clamp below min = %v, want 0", got)
	}
	if got := clamp(11, 0, 10); got != 10 {
		t.Errorf("clamp above max = %v, want 10", got)
	}
	if got := clamp(5, 0, 10); got != 5 {
		t.Errorf("clamp in range = %v, want 5", got)
	}

	if got := max(3, 7); got != 7 {
		t.Errorf("max = %v, want 7", got)
	}
	if got := max(9, 2); got != 9 {
		t.Errorf("max = %v, want 9", got)
	}

	if floatPtr(1.5) == nil {
		t.Error("floatPtr returned nil")
	}
	if timePtr(time.Now()) == nil {
		t.Error("timePtr returned nil")
	}
}

func TestBaseStorage_CreateJobInternal_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	bs := &baseStorage{db: db}

	mock.ExpectQuery("SELECT EXISTS\\(SELECT 1 FROM jobs WHERE id = \\$1\\)").
		WithArgs("job-err").
		WillReturnError(errors.New("query failed"))

	err = bs.createJobInternal(context.Background(), &Job{ID: "job-err"})
	if err == nil || err.Error() != "query failed" {
		t.Fatalf("expected query error, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestBaseStorage_CreateJobInternal_InsertError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	bs := &baseStorage{db: db}
	job := &Job{ID: "job-err", User: "u", Nodes: []string{"n"}, State: JobStateRunning, StartTime: time.Now()}

	mock.ExpectQuery("SELECT EXISTS\\(SELECT 1 FROM jobs WHERE id = \\$1\\)").
		WithArgs("job-err").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	mock.ExpectExec("INSERT INTO jobs").
		WillReturnError(errors.New("insert failed"))

	err = bs.createJobInternal(context.Background(), job)
	if err == nil || err.Error() != "insert failed" {
		t.Fatalf("expected insert error, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestBaseStorage_SeedJobMetrics_ExecError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	bs := &baseStorage{db: db}

	now := time.Now()
	job := &Job{ID: "job-1", User: "u", Nodes: []string{"n"}, State: JobStateRunning, StartTime: now.Add(-time.Minute)}

	mock.ExpectExec("INSERT INTO metric_samples").
		WillReturnError(errors.New("metrics insert failed"))

	err = bs.seedJobMetrics(context.Background(), job, now)
	if err == nil || err.Error() != "metrics insert failed" {
		t.Fatalf("expected metrics error, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
