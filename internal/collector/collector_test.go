package collector

import (
	"context"
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
