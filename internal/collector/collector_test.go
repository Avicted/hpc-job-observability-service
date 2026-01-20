package collector

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/avic/hpc-job-observability-service/internal/metrics"
	"github.com/avic/hpc-job-observability-service/internal/storage"
)

// testStore creates a temporary SQLite store for testing.
func testStore(t *testing.T) storage.Storage {
	t.Helper()
	tmpFile, err := os.CreateTemp("", "test-collector-*.db")
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

func TestCollector(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)

	// Create a running job
	ctx := context.Background()
	job := &storage.Job{
		ID:            "collector-test-job",
		User:          "testuser",
		Nodes:         []string{"node-1"},
		State:         storage.JobStateRunning,
		StartTime:     time.Now(),
		CPUUsage:      75.0,
		MemoryUsageMB: 4096,
	}
	if err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}

	// Create collector with short interval for testing
	coll := NewWithInterval(store, exporter, 100*time.Millisecond)

	// Test CollectOnce
	t.Run("CollectOnce", func(t *testing.T) {
		coll.CollectOnce()

		// Check that metrics were recorded
		samples, total, err := store.GetJobMetrics(ctx, "collector-test-job", storage.MetricsFilter{Limit: 10})
		if err != nil {
			t.Errorf("Failed to get metrics: %v", err)
		}
		if total == 0 {
			t.Error("Expected at least one metric sample")
		}
		if len(samples) == 0 {
			t.Error("Expected at least one metric sample returned")
		}
	})

	// Test Start/Stop
	t.Run("StartStop", func(t *testing.T) {
		coll.Start()

		// Wait for a few collection cycles
		time.Sleep(350 * time.Millisecond)

		coll.Stop()

		// Check that multiple metrics were recorded
		samples, total, err := store.GetJobMetrics(ctx, "collector-test-job", storage.MetricsFilter{Limit: 100})
		if err != nil {
			t.Errorf("Failed to get metrics: %v", err)
		}
		if total < 3 {
			t.Errorf("Expected at least 3 metric samples, got %d", total)
		}
		if len(samples) < 3 {
			t.Errorf("Expected at least 3 samples returned, got %d", len(samples))
		}
	})
}

func TestSimulateUsage(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	coll := New(store, exporter)

	// Test CPU simulation stays in bounds
	t.Run("SimulateCPU", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			result := coll.simulateCPUUsage(50.0)
			if result < 0 || result > 100 {
				t.Errorf("CPU usage out of bounds: %f", result)
			}
		}

		// Test edge cases
		result := coll.simulateCPUUsage(0)
		if result < 0 {
			t.Errorf("CPU usage below 0: %f", result)
		}

		result = coll.simulateCPUUsage(100)
		if result > 100 {
			t.Errorf("CPU usage above 100: %f", result)
		}
	})

	// Test Memory simulation stays positive
	t.Run("SimulateMemory", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			result := coll.simulateMemoryUsage(4096)
			if result < 0 {
				t.Errorf("Memory usage negative: %d", result)
			}
		}
	})

	// Test GPU simulation stays in bounds
	t.Run("SimulateGPU", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			result := coll.simulateGPUUsage(50.0)
			if result < 0 || result > 100 {
				t.Errorf("GPU usage out of bounds: %f", result)
			}
		}
	})
}

type fakeStorage struct {
	jobs        []*storage.Job
	getAllErr   error
	recordErr   error
	updateErr   error
	getAllCalls int
	updateCalls int
	recorded    []*storage.MetricSample
}

func (f *fakeStorage) CreateJob(ctx context.Context, job *storage.Job) error { return nil }
func (f *fakeStorage) GetJob(ctx context.Context, id string) (*storage.Job, error) {
	return nil, storage.ErrJobNotFound
}
func (f *fakeStorage) UpdateJob(ctx context.Context, job *storage.Job) error {
	f.updateCalls++
	return f.updateErr
}
func (f *fakeStorage) DeleteJob(ctx context.Context, id string) error { return nil }
func (f *fakeStorage) ListJobs(ctx context.Context, filter storage.JobFilter) ([]*storage.Job, int, error) {
	return nil, 0, nil
}
func (f *fakeStorage) GetAllJobs(ctx context.Context) ([]*storage.Job, error) {
	f.getAllCalls++
	if f.getAllErr != nil {
		return nil, f.getAllErr
	}
	return f.jobs, nil
}
func (f *fakeStorage) RecordMetrics(ctx context.Context, sample *storage.MetricSample) error {
	f.recorded = append(f.recorded, sample)
	return f.recordErr
}
func (f *fakeStorage) GetJobMetrics(ctx context.Context, jobID string, filter storage.MetricsFilter) ([]*storage.MetricSample, int, error) {
	return nil, 0, nil
}
func (f *fakeStorage) GetLatestMetrics(ctx context.Context, jobID string) (*storage.MetricSample, error) {
	return nil, nil
}
func (f *fakeStorage) DeleteMetricsBefore(cutoff time.Time) error { return nil }
func (f *fakeStorage) Migrate() error                             { return nil }
func (f *fakeStorage) Close() error                               { return nil }
func (f *fakeStorage) SeedDemoData() error                        { return nil }

func TestCollector_CollectAll_GetAllJobsError(t *testing.T) {
	store := &fakeStorage{getAllErr: errors.New("db error")}
	exporter := metrics.NewExporter(store)
	coll := NewWithInterval(store, exporter, 10*time.Millisecond)

	coll.CollectOnce()

	if store.getAllCalls != 1 {
		t.Fatalf("expected 1 GetAllJobs call, got %d", store.getAllCalls)
	}
}

func TestCollector_CollectJobMetrics_RecordError(t *testing.T) {
	store := &fakeStorage{
		jobs: []*storage.Job{{
			ID:            "job-1",
			User:          "alice",
			Nodes:         []string{"node-1"},
			State:         storage.JobStateRunning,
			StartTime:     time.Now().Add(-time.Minute),
			CPUUsage:      50,
			MemoryUsageMB: 1024,
		}},
		recordErr: errors.New("record failed"),
	}
	exporter := metrics.NewExporter(store)
	coll := NewWithInterval(store, exporter, 10*time.Millisecond)

	coll.CollectOnce()

	if store.updateCalls != 0 {
		t.Fatalf("expected UpdateJob not to be called, got %d", store.updateCalls)
	}
}

func TestCollector_CollectJobMetrics_UpdateErrorAndGPU(t *testing.T) {
	gpu := 45.0
	store := &fakeStorage{
		jobs: []*storage.Job{{
			ID:            "job-gpu",
			User:          "alice",
			Nodes:         []string{"node-1"},
			State:         storage.JobStateRunning,
			StartTime:     time.Now().Add(-time.Minute),
			CPUUsage:      50,
			MemoryUsageMB: 1024,
			GPUUsage:      &gpu,
		}},
		updateErr: errors.New("update failed"),
	}
	exporter := metrics.NewExporter(store)
	coll := NewWithInterval(store, exporter, 10*time.Millisecond)

	coll.CollectOnce()

	if store.updateCalls != 1 {
		t.Fatalf("expected UpdateJob to be called once, got %d", store.updateCalls)
	}
	if len(store.recorded) == 0 || store.recorded[0].GPUUsage == nil {
		t.Fatal("expected GPUUsage to be recorded")
	}
}
