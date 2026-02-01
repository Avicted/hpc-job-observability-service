package collector

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/domain"
	"github.com/Avicted/hpc-job-observability-service/internal/storage"
	"github.com/Avicted/hpc-job-observability-service/internal/utils/cgroup"
	"github.com/Avicted/hpc-job-observability-service/internal/utils/metrics"
)

// mockStorage implements storage.Storage for testing.
type mockStorage struct {
	mu                  sync.Mutex
	jobs                []*domain.Job
	metricsRecorded     []*domain.MetricSample
	getAllErr           error
	recordMetricsErr    error
	updateJobErr        error
	updateJobCalled     int
	recordMetricsCalled int
}

func (m *mockStorage) CreateJob(ctx context.Context, job *domain.Job) error {
	return nil
}

func (m *mockStorage) GetJob(ctx context.Context, id string) (*domain.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, job := range m.jobs {
		if job.ID == id {
			return job, nil
		}
	}
	return nil, storage.ErrNotFound
}

func (m *mockStorage) UpdateJob(ctx context.Context, job *domain.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updateJobCalled++
	return m.updateJobErr
}

func (m *mockStorage) UpsertJob(ctx context.Context, job *domain.Job) error {
	return nil
}

func (m *mockStorage) DeleteJob(ctx context.Context, id string) error {
	return nil
}

func (m *mockStorage) ListJobs(ctx context.Context, filter domain.JobFilter) ([]*domain.Job, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.jobs, len(m.jobs), nil
}

func (m *mockStorage) GetAllJobs(ctx context.Context) ([]*domain.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getAllErr != nil {
		return nil, m.getAllErr
	}
	return m.jobs, nil
}

func (m *mockStorage) RecordMetrics(ctx context.Context, sample *domain.MetricSample) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recordMetricsCalled++
	if m.recordMetricsErr != nil {
		return m.recordMetricsErr
	}
	m.metricsRecorded = append(m.metricsRecorded, sample)
	return nil
}

func (m *mockStorage) GetJobMetrics(ctx context.Context, jobID string, filter domain.MetricsFilter) ([]*domain.MetricSample, int, error) {
	return nil, 0, nil
}

func (m *mockStorage) GetLatestMetrics(ctx context.Context, jobID string) (*domain.MetricSample, error) {
	return nil, nil
}

func (m *mockStorage) RecordMetricsBatch(ctx context.Context, samples []*domain.MetricSample) error {
	return nil
}

func (m *mockStorage) DeleteMetricsBefore(ctx context.Context, cutoff time.Time) error {
	return nil
}

func (m *mockStorage) RecordAuditEvent(ctx context.Context, event *domain.JobAuditEvent) error {
	return nil
}

func (m *mockStorage) WithTx(ctx context.Context, fn func(storage.Tx) error) error {
	return fn(m)
}

func (m *mockStorage) Jobs() storage.JobStore {
	return m
}

func (m *mockStorage) Metrics() storage.MetricStore {
	return m
}

func (m *mockStorage) Audit() storage.AuditStore {
	return m
}

func (m *mockStorage) Migrate(ctx context.Context) error {
	return nil
}

func (m *mockStorage) Close() error {
	return nil
}

func TestNew(t *testing.T) {
	store := &mockStorage{}
	exporter := metrics.NewExporter(store)
	collector := New(store, exporter)

	if collector == nil {
		t.Fatal("expected non-nil collector")
	}
	if collector.exporter != exporter {
		t.Error("expected exporter to be set")
	}
	if collector.config.Interval != 30*time.Second {
		t.Errorf("expected interval 30s, got %v", collector.config.Interval)
	}
}

func TestNewWithInterval(t *testing.T) {
	store := &mockStorage{}
	exporter := metrics.NewExporter(store)
	interval := 5 * time.Second
	collector := NewWithInterval(store, exporter, interval)

	if collector == nil {
		t.Fatal("expected non-nil collector")
	}
	if collector.config.Interval != interval {
		t.Errorf("expected interval %v, got %v", interval, collector.config.Interval)
	}
}

func TestStartStop(t *testing.T) {
	store := &mockStorage{jobs: []*domain.Job{}}
	exporter := metrics.NewExporter(store)
	collector := NewWithInterval(store, exporter, 100*time.Millisecond)

	collector.Start()
	time.Sleep(50 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		collector.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() took too long")
	}
}

func TestCollectOnce_NoJobs(t *testing.T) {
	store := &mockStorage{jobs: []*domain.Job{}}
	exporter := metrics.NewExporter(store)
	collector := New(store, exporter)

	collector.CollectOnce()
}

func TestCollectOnce_WithRunningJob(t *testing.T) {
	gpuUsage := 75.0
	now := time.Now()
	jobs := []*domain.Job{
		{
			ID:            "job-1",
			User:          "alice",
			Nodes:         []string{"node-1"},
			State:         domain.JobStateRunning,
			StartTime:     now.Add(-30 * time.Minute),
			CPUUsage:      50.0,
			MemoryUsageMB: 2048,
			GPUUsage:      &gpuUsage,
		},
	}

	store := &mockStorage{jobs: jobs}
	exporter := metrics.NewExporter(store)
	collector := New(store, exporter)

	collector.CollectOnce()

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.recordMetricsCalled != 1 {
		t.Errorf("expected RecordMetrics to be called once, got %d", store.recordMetricsCalled)
	}
	if store.updateJobCalled != 1 {
		t.Errorf("expected UpdateJob to be called once, got %d", store.updateJobCalled)
	}
}

func TestCollectOnce_WithPendingJob(t *testing.T) {
	now := time.Now()
	jobs := []*domain.Job{
		{
			ID:        "job-1",
			User:      "alice",
			Nodes:     []string{},
			State:     domain.JobStatePending,
			StartTime: now,
		},
	}

	store := &mockStorage{jobs: jobs}
	exporter := metrics.NewExporter(store)
	collector := New(store, exporter)

	collector.CollectOnce()

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.recordMetricsCalled != 0 {
		t.Errorf("expected RecordMetrics not to be called for pending job, got %d", store.recordMetricsCalled)
	}
}

func TestCollectOnce_WithCompletedJob(t *testing.T) {
	now := time.Now()
	endTime := now.Add(-10 * time.Minute)
	jobs := []*domain.Job{
		{
			ID:             "job-1",
			User:           "alice",
			Nodes:          []string{"node-1"},
			State:          domain.JobStateCompleted,
			StartTime:      now.Add(-1 * time.Hour),
			EndTime:        &endTime,
			RuntimeSeconds: 3000,
		},
	}

	store := &mockStorage{jobs: jobs}
	exporter := metrics.NewExporter(store)
	collector := New(store, exporter)

	collector.CollectOnce()

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.recordMetricsCalled != 0 {
		t.Errorf("expected RecordMetrics not to be called for completed job, got %d", store.recordMetricsCalled)
	}
}

func TestCollectOnce_MultipleRunningJobs(t *testing.T) {
	now := time.Now()
	gpuUsage := 60.0
	jobs := []*domain.Job{
		{
			ID:            "job-1",
			User:          "alice",
			Nodes:         []string{"node-1"},
			State:         domain.JobStateRunning,
			StartTime:     now.Add(-30 * time.Minute),
			CPUUsage:      50.0,
			MemoryUsageMB: 2048,
			GPUUsage:      &gpuUsage,
		},
		{
			ID:            "job-2",
			User:          "bob",
			Nodes:         []string{"node-2"},
			State:         domain.JobStateRunning,
			StartTime:     now.Add(-15 * time.Minute),
			CPUUsage:      75.0,
			MemoryUsageMB: 4096,
		},
		{
			ID:        "job-3",
			User:      "charlie",
			Nodes:     []string{},
			State:     domain.JobStatePending,
			StartTime: now,
		},
	}

	store := &mockStorage{jobs: jobs}
	exporter := metrics.NewExporter(store)
	collector := New(store, exporter)

	collector.CollectOnce()

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.recordMetricsCalled != 2 {
		t.Errorf("expected RecordMetrics to be called twice, got %d", store.recordMetricsCalled)
	}
	if store.updateJobCalled != 2 {
		t.Errorf("expected UpdateJob to be called twice, got %d", store.updateJobCalled)
	}
}

func TestCollectOnce_GetAllJobsError(t *testing.T) {
	store := &mockStorage{
		jobs:      []*domain.Job{},
		getAllErr: errors.New("database error"),
	}
	exporter := metrics.NewExporter(store)
	collector := New(store, exporter)

	collector.CollectOnce()

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.recordMetricsCalled != 0 {
		t.Errorf("expected RecordMetrics not to be called on error, got %d", store.recordMetricsCalled)
	}
}

func TestCollectOnce_RecordMetricsError(t *testing.T) {
	now := time.Now()
	jobs := []*domain.Job{
		{
			ID:            "job-1",
			User:          "alice",
			Nodes:         []string{"node-1"},
			State:         domain.JobStateRunning,
			StartTime:     now.Add(-30 * time.Minute),
			CPUUsage:      50.0,
			MemoryUsageMB: 2048,
		},
	}

	store := &mockStorage{
		jobs:             jobs,
		recordMetricsErr: errors.New("metrics error"),
	}
	exporter := metrics.NewExporter(store)
	collector := New(store, exporter)

	collector.CollectOnce()

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.recordMetricsCalled != 1 {
		t.Errorf("expected RecordMetrics to be called once, got %d", store.recordMetricsCalled)
	}
	if store.updateJobCalled != 0 {
		t.Errorf("expected UpdateJob not to be called on RecordMetrics error, got %d", store.updateJobCalled)
	}
}

func TestCollectOnce_UpdateJobError(t *testing.T) {
	now := time.Now()
	jobs := []*domain.Job{
		{
			ID:            "job-1",
			User:          "alice",
			Nodes:         []string{"node-1"},
			State:         domain.JobStateRunning,
			StartTime:     now.Add(-30 * time.Minute),
			CPUUsage:      50.0,
			MemoryUsageMB: 2048,
		},
	}

	store := &mockStorage{
		jobs:         jobs,
		updateJobErr: errors.New("update error"),
	}
	exporter := metrics.NewExporter(store)
	collector := New(store, exporter)

	collector.CollectOnce()

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.recordMetricsCalled != 1 {
		t.Errorf("expected RecordMetrics to be called once, got %d", store.recordMetricsCalled)
	}
	if store.updateJobCalled != 1 {
		t.Errorf("expected UpdateJob to be called once, got %d", store.updateJobCalled)
	}
}

func TestCollectorLoop(t *testing.T) {
	now := time.Now()
	jobs := []*domain.Job{
		{
			ID:            "job-1",
			User:          "alice",
			Nodes:         []string{"node-1"},
			State:         domain.JobStateRunning,
			StartTime:     now.Add(-30 * time.Minute),
			CPUUsage:      50.0,
			MemoryUsageMB: 2048,
		},
	}

	store := &mockStorage{jobs: jobs}
	exporter := metrics.NewExporter(store)
	collector := NewWithInterval(store, exporter, 50*time.Millisecond)

	collector.Start()
	time.Sleep(150 * time.Millisecond)
	collector.Stop()

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.recordMetricsCalled < 2 {
		t.Errorf("expected RecordMetrics to be called at least twice, got %d", store.recordMetricsCalled)
	}
}

func TestSimulateCPUUsage(t *testing.T) {
	store := &mockStorage{}
	exporter := metrics.NewExporter(store)
	collector := New(store, exporter)

	for i := 0; i < 100; i++ {
		result := collector.simulateCPUUsage(50.0)
		if result < 0 || result > 100 {
			t.Errorf("CPU usage %f out of bounds", result)
		}

		result = collector.simulateCPUUsage(5.0)
		if result < 0 || result > 100 {
			t.Errorf("CPU usage %f out of bounds for low baseline", result)
		}

		result = collector.simulateCPUUsage(95.0)
		if result < 0 || result > 100 {
			t.Errorf("CPU usage %f out of bounds for high baseline", result)
		}

		result = collector.simulateCPUUsage(0.0)
		if result < 0 || result > 100 {
			t.Errorf("CPU usage %f out of bounds for zero baseline", result)
		}

		result = collector.simulateCPUUsage(100.0)
		if result < 0 || result > 100 {
			t.Errorf("CPU usage %f out of bounds for max baseline", result)
		}
	}
}

func TestSimulateMemoryUsage(t *testing.T) {
	store := &mockStorage{}
	exporter := metrics.NewExporter(store)
	collector := New(store, exporter)

	for i := 0; i < 100; i++ {
		result := collector.simulateMemoryUsage(4096)
		if result < 0 {
			t.Errorf("Memory usage %d is negative", result)
		}

		result = collector.simulateMemoryUsage(100)
		if result < 0 {
			t.Errorf("Memory usage %d is negative for low baseline", result)
		}

		result = collector.simulateMemoryUsage(0)
		if result < 0 {
			t.Errorf("Memory usage %d is negative for zero baseline", result)
		}

		result = collector.simulateMemoryUsage(256000)
		if result < 0 {
			t.Errorf("Memory usage %d is negative for high baseline", result)
		}
	}
}

func TestSimulateGPUUsage(t *testing.T) {
	store := &mockStorage{}
	exporter := metrics.NewExporter(store)
	collector := New(store, exporter)

	for i := 0; i < 100; i++ {
		result := collector.simulateGPUUsage(50.0)
		if result < 0 || result > 100 {
			t.Errorf("GPU usage %f out of bounds", result)
		}

		result = collector.simulateGPUUsage(10.0)
		if result < 0 || result > 100 {
			t.Errorf("GPU usage %f out of bounds for low baseline", result)
		}

		result = collector.simulateGPUUsage(90.0)
		if result < 0 || result > 100 {
			t.Errorf("GPU usage %f out of bounds for high baseline", result)
		}

		result = collector.simulateGPUUsage(0.0)
		if result < 0 || result > 100 {
			t.Errorf("GPU usage %f out of bounds for zero baseline", result)
		}

		result = collector.simulateGPUUsage(100.0)
		if result < 0 || result > 100 {
			t.Errorf("GPU usage %f out of bounds for max baseline", result)
		}
	}
}

func TestCollectOnce_JobWithoutGPU(t *testing.T) {
	now := time.Now()
	jobs := []*domain.Job{
		{
			ID:            "job-1",
			User:          "alice",
			Nodes:         []string{"node-1"},
			State:         domain.JobStateRunning,
			StartTime:     now.Add(-30 * time.Minute),
			CPUUsage:      50.0,
			MemoryUsageMB: 2048,
			GPUUsage:      nil,
		},
	}

	store := &mockStorage{jobs: jobs}
	exporter := metrics.NewExporter(store)
	collector := New(store, exporter)

	collector.CollectOnce()

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.recordMetricsCalled != 1 {
		t.Errorf("expected RecordMetrics to be called once, got %d", store.recordMetricsCalled)
	}

	if len(store.metricsRecorded) != 1 {
		t.Fatalf("expected 1 metric sample, got %d", len(store.metricsRecorded))
	}
	if store.metricsRecorded[0].GPUUsage != nil {
		t.Error("expected no GPU usage in recorded sample")
	}
}

func TestCollectOnce_AllJobStates(t *testing.T) {
	now := time.Now()
	endTime := now.Add(-10 * time.Minute)
	jobs := []*domain.Job{
		{ID: "job-1", User: "user", Nodes: []string{"node"}, State: domain.JobStatePending, StartTime: now},
		{ID: "job-2", User: "user", Nodes: []string{"node"}, State: domain.JobStateRunning, StartTime: now, CPUUsage: 50, MemoryUsageMB: 1024},
		{ID: "job-3", User: "user", Nodes: []string{"node"}, State: domain.JobStateCompleted, StartTime: now, EndTime: &endTime},
		{ID: "job-4", User: "user", Nodes: []string{"node"}, State: domain.JobStateFailed, StartTime: now, EndTime: &endTime},
		{ID: "job-5", User: "user", Nodes: []string{"node"}, State: domain.JobStateCancelled, StartTime: now, EndTime: &endTime},
	}

	store := &mockStorage{jobs: jobs}
	exporter := metrics.NewExporter(store)
	collector := New(store, exporter)

	collector.CollectOnce()

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.recordMetricsCalled != 1 {
		t.Errorf("expected RecordMetrics to be called once for running job, got %d", store.recordMetricsCalled)
	}
}

func TestMetricSampleFields(t *testing.T) {
	now := time.Now()
	gpuUsage := 75.0
	jobs := []*domain.Job{
		{
			ID:            "job-1",
			User:          "alice",
			Nodes:         []string{"node-1"},
			State:         domain.JobStateRunning,
			StartTime:     now.Add(-30 * time.Minute),
			CPUUsage:      50.0,
			MemoryUsageMB: 2048,
			GPUUsage:      &gpuUsage,
		},
	}

	store := &mockStorage{jobs: jobs}
	exporter := metrics.NewExporter(store)
	collector := New(store, exporter)

	collector.CollectOnce()

	store.mu.Lock()
	defer store.mu.Unlock()

	if len(store.metricsRecorded) != 1 {
		t.Fatalf("expected 1 metric sample, got %d", len(store.metricsRecorded))
	}

	sample := store.metricsRecorded[0]
	if sample.JobID != "job-1" {
		t.Errorf("expected JobID job-1, got %s", sample.JobID)
	}
	if sample.Timestamp.IsZero() {
		t.Error("expected non-zero timestamp")
	}
	if sample.CPUUsage < 0 || sample.CPUUsage > 100 {
		t.Errorf("CPU usage %f out of bounds", sample.CPUUsage)
	}
	if sample.MemoryUsageMB < 0 {
		t.Errorf("Memory usage %d is negative", sample.MemoryUsageMB)
	}
	if sample.GPUUsage == nil {
		t.Error("expected GPU usage to be set")
	}
}

func TestCleanupPrevStats(t *testing.T) {
	store := &mockStorage{}
	exporter := metrics.NewExporter(store)
	collector := New(store, exporter)

	// Initially, prevStats should be empty (or will be populated with nil check)
	// Add some stats first by simulating a collection with cgroup
	jobID := "test-job-123"

	// Manually add an entry to prevStats via the calculateCPUPercent path
	// We need to access the internal map, so we'll use the method directly
	collector.prevStatsMu.Lock()
	collector.prevStats[jobID] = &cgroup.Stats{
		CPUUsageUsec: 1000000,
		Timestamp:    time.Now(),
	}
	collector.prevStatsMu.Unlock()

	// Verify the entry exists
	collector.prevStatsMu.Lock()
	_, exists := collector.prevStats[jobID]
	collector.prevStatsMu.Unlock()
	if !exists {
		t.Fatal("expected prevStats entry to exist before cleanup")
	}

	// Cleanup
	collector.CleanupPrevStats(jobID)

	// Verify entry is removed
	collector.prevStatsMu.Lock()
	_, exists = collector.prevStats[jobID]
	collector.prevStatsMu.Unlock()
	if exists {
		t.Error("expected prevStats entry to be removed after cleanup")
	}

	// Cleanup non-existent key should not panic
	collector.CleanupPrevStats("non-existent-job")
}

func TestCalculateCPUPercent(t *testing.T) {
	store := &mockStorage{}
	exporter := metrics.NewExporter(store)
	collector := New(store, exporter)

	jobID := "test-job-cpu"

	// First call with no previous stats should store current and return 0
	now := time.Now()
	firstStats := &cgroup.Stats{
		CPUUsageUsec: 1000000, // 1 second
		Timestamp:    now,
	}

	result := collector.calculateCPUPercent(jobID, firstStats, 1)
	// First call returns 0 because there's no previous stats to compare
	if result != 0 {
		t.Logf("First call returned %f (expected 0 for no previous stats)", result)
	}

	// Second call with new stats should calculate delta
	secondStats := &cgroup.Stats{
		CPUUsageUsec: 2000000, // 2 seconds (1 second delta)
		Timestamp:    now.Add(time.Second),
	}

	result = collector.calculateCPUPercent(jobID, secondStats, 1)
	// With 1 CPU, 1 second of CPU time in 1 second = 100%
	if result != 100.0 {
		t.Logf("Second call returned %f%% (expected around 100%% for 1 CPU)", result)
	}

	// Third call with 4 CPUs
	thirdStats := &cgroup.Stats{
		CPUUsageUsec: 3000000, // 3 seconds total (1 second delta)
		Timestamp:    now.Add(2 * time.Second),
	}

	result = collector.calculateCPUPercent(jobID, thirdStats, 4)
	// With 4 CPUs, 1 second of CPU time in 1 second = 25%
	if result != 25.0 {
		t.Logf("Third call returned %f%% (expected around 25%% for 4 CPUs)", result)
	}

	// Cleanup
	collector.CleanupPrevStats(jobID)
}

func TestCollectGPUMetrics_NoDetector(t *testing.T) {
	store := &mockStorage{}
	exporter := metrics.NewExporter(store)
	collector := New(store, exporter) // gpuDetector is nil by default

	job := &domain.Job{
		ID:         "job-gpu-1",
		GPUDevices: []string{"0", "1"},
	}

	result := collector.collectGPUMetrics(context.Background(), job)
	if result != nil {
		t.Error("expected nil result when gpuDetector is nil")
	}
}

func TestCollectGPUMetrics_NoGPUDevices(t *testing.T) {
	store := &mockStorage{}
	exporter := metrics.NewExporter(store)
	collector := New(store, exporter)
	// Even if we had a detector, with no GPU devices it should return nil

	job := &domain.Job{
		ID:         "job-gpu-2",
		GPUDevices: []string{}, // Empty GPU devices
	}

	result := collector.collectGPUMetrics(context.Background(), job)
	if result != nil {
		t.Error("expected nil result when job has no GPU devices")
	}
}
