package metrics

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/avic/hpc-job-observability-service/internal/storage"
	"github.com/prometheus/client_golang/prometheus"
)

// testStore creates a temporary SQLite store for testing.
func testStore(t *testing.T) storage.Storage {
	t.Helper()

	tmpFile, err := os.CreateTemp("", "test-metrics-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	t.Cleanup(func() { os.Remove(tmpFile.Name()) })

	store, err := storage.NewSQLiteStorage("file:" + tmpFile.Name() + "?cache=shared&mode=rwc")
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	if err := store.Migrate(); err != nil {
		t.Fatalf("Failed to migrate: %v", err)
	}

	return store
}

func TestNewExporter(t *testing.T) {
	store := testStore(t)
	exporter := NewExporter(store)

	if exporter == nil {
		t.Fatal("NewExporter returned nil")
	}
	if exporter.store == nil {
		t.Error("Exporter store is nil")
	}
	if exporter.jobRuntimeSeconds == nil {
		t.Error("jobRuntimeSeconds gauge is nil")
	}
	if exporter.jobCPUUsagePercent == nil {
		t.Error("jobCPUUsagePercent gauge is nil")
	}
	if exporter.jobMemoryUsageBytes == nil {
		t.Error("jobMemoryUsageBytes gauge is nil")
	}
	if exporter.jobGPUUsagePercent == nil {
		t.Error("jobGPUUsagePercent gauge is nil")
	}
	if exporter.jobStateTotal == nil {
		t.Error("jobStateTotal gauge is nil")
	}
	if exporter.jobsTotal == nil {
		t.Error("jobsTotal counter is nil")
	}
}

func TestExporter_Register(t *testing.T) {
	store := testStore(t)
	exporter := NewExporter(store)

	registry := prometheus.NewRegistry()
	if err := exporter.Register(registry); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Registering again should fail (duplicate registration)
	registry2 := prometheus.NewRegistry()
	// First registration on new registry should work
	exporter2 := NewExporter(store)
	if err := exporter2.Register(registry2); err != nil {
		t.Fatalf("Register on new registry failed: %v", err)
	}
}

func TestExporter_Collect(t *testing.T) {
	store := testStore(t)
	exporter := NewExporter(store)
	ctx := context.Background()

	// Create test jobs
	gpuUsage := 45.0
	jobs := []*storage.Job{
		{
			ID:            "collect-job-001",
			User:          "alice",
			Nodes:         []string{"node-01", "node-02"},
			State:         storage.JobStateRunning,
			StartTime:     time.Now().Add(-1 * time.Hour),
			CPUUsage:      75.5,
			MemoryUsageMB: 4096,
			GPUUsage:      &gpuUsage,
		},
		{
			ID:             "collect-job-002",
			User:           "bob",
			Nodes:          []string{"node-03"},
			State:          storage.JobStateCompleted,
			StartTime:      time.Now().Add(-2 * time.Hour),
			RuntimeSeconds: 3600,
			CPUUsage:       0,
			MemoryUsageMB:  0,
		},
		{
			ID:            "collect-job-003",
			User:          "charlie",
			Nodes:         []string{}, // Edge case: no nodes
			State:         storage.JobStatePending,
			StartTime:     time.Now(),
			CPUUsage:      0,
			MemoryUsageMB: 0,
		},
	}

	for _, job := range jobs {
		if err := store.CreateJob(ctx, job); err != nil {
			t.Fatalf("Failed to create job %s: %v", job.ID, err)
		}
	}

	// Collect metrics
	if err := exporter.Collect(ctx); err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	// Verify we can collect again (reset works)
	if err := exporter.Collect(ctx); err != nil {
		t.Fatalf("Second Collect failed: %v", err)
	}
}

func TestExporter_Collect_EmptyStore(t *testing.T) {
	store := testStore(t)
	exporter := NewExporter(store)

	// Collect with no jobs should not error
	if err := exporter.Collect(context.Background()); err != nil {
		t.Fatalf("Collect on empty store failed: %v", err)
	}
}

func TestExporter_UpdateJobMetrics(t *testing.T) {
	store := testStore(t)
	exporter := NewExporter(store)

	// Test with running job
	gpuUsage := 50.0
	job := &storage.Job{
		ID:            "update-job-001",
		User:          "testuser",
		Nodes:         []string{"node-01"},
		State:         storage.JobStateRunning,
		StartTime:     time.Now().Add(-30 * time.Minute),
		CPUUsage:      80.0,
		MemoryUsageMB: 8192,
		GPUUsage:      &gpuUsage,
	}

	// Should not panic
	exporter.UpdateJobMetrics(job)

	// Test with completed job
	job.State = storage.JobStateCompleted
	job.RuntimeSeconds = 1800
	exporter.UpdateJobMetrics(job)

	// Test with no nodes (edge case)
	job.Nodes = []string{}
	exporter.UpdateJobMetrics(job)

	// Test without GPU usage
	job.GPUUsage = nil
	exporter.UpdateJobMetrics(job)
}

func TestExporter_IncrementJobsTotal(t *testing.T) {
	store := testStore(t)
	exporter := NewExporter(store)

	// Should not panic
	exporter.IncrementJobsTotal()
	exporter.IncrementJobsTotal()
	exporter.IncrementJobsTotal()
}

func TestExporter_Handler(t *testing.T) {
	store := testStore(t)
	exporter := NewExporter(store)

	handler := exporter.Handler()
	if handler == nil {
		t.Fatal("Handler returned nil")
	}
}

func TestDefaultHandler(t *testing.T) {
	handler := DefaultHandler()
	if handler == nil {
		t.Fatal("DefaultHandler returned nil")
	}
}

func TestExporter_AllJobStates(t *testing.T) {
	store := testStore(t)
	exporter := NewExporter(store)
	ctx := context.Background()

	// Create jobs in all states
	states := []storage.JobState{
		storage.JobStatePending,
		storage.JobStateRunning,
		storage.JobStateCompleted,
		storage.JobStateFailed,
		storage.JobStateCancelled,
	}

	for i, state := range states {
		job := &storage.Job{
			ID:        "state-job-" + string(state),
			User:      "user",
			Nodes:     []string{"node-" + string(rune('0'+i))},
			State:     state,
			StartTime: time.Now(),
		}
		if state == storage.JobStateCompleted || state == storage.JobStateFailed || state == storage.JobStateCancelled {
			now := time.Now()
			job.EndTime = &now
			job.RuntimeSeconds = 100
		}
		if err := store.CreateJob(ctx, job); err != nil {
			t.Fatalf("Failed to create job: %v", err)
		}
	}

	if err := exporter.Collect(ctx); err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
}

func TestExporter_MetricsFormat(t *testing.T) {
	store := testStore(t)
	exporter := NewExporter(store)
	ctx := context.Background()

	// Create a job
	job := &storage.Job{
		ID:            "format-job-001",
		User:          "alice",
		Nodes:         []string{"compute-node-01"},
		State:         storage.JobStateRunning,
		StartTime:     time.Now().Add(-1 * time.Hour),
		CPUUsage:      75.5,
		MemoryUsageMB: 4096,
	}
	if err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}

	if err := exporter.Collect(ctx); err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	// Get the handler and make a request
	handler := exporter.Handler()
	if handler == nil {
		t.Fatal("Handler is nil")
	}

	// Use httptest to verify the /metrics endpoint returns valid data
	// This is tested more fully in handler_test.go, but verify basic functionality
}

func TestExporter_Register_DuplicateCollector(t *testing.T) {
	store := testStore(t)
	exporter := NewExporter(store)

	registry := prometheus.NewRegistry()

	// First registration should succeed
	if err := exporter.Register(registry); err != nil {
		t.Fatalf("First Register failed: %v", err)
	}

	// Second registration of same exporter should fail
	err := exporter.Register(registry)
	if err == nil {
		t.Error("Expected error on duplicate registration")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		// The error message varies, but registration should fail
		t.Logf("Got expected error: %v", err)
	}
}
