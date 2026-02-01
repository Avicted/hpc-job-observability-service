package metrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/domain"
	"github.com/Avicted/hpc-job-observability-service/internal/storage"
	"github.com/Avicted/hpc-job-observability-service/internal/utils/scheduler"
	"github.com/prometheus/client_golang/prometheus"
)

// mockStorage implements storage.Storage for testing.
type mockStorage struct {
	jobs      []*domain.Job
	getAllErr error
}

func (m *mockStorage) CreateJob(ctx context.Context, job *domain.Job) error {
	return nil
}

func (m *mockStorage) GetJob(ctx context.Context, id string) (*domain.Job, error) {
	for _, job := range m.jobs {
		if job.ID == id {
			return job, nil
		}
	}
	return nil, storage.ErrNotFound
}

func (m *mockStorage) UpdateJob(ctx context.Context, job *domain.Job) error {
	return nil
}

func (m *mockStorage) UpsertJob(ctx context.Context, job *domain.Job) error {
	return nil
}

func (m *mockStorage) DeleteJob(ctx context.Context, id string) error {
	return nil
}

func (m *mockStorage) ListJobs(ctx context.Context, filter domain.JobFilter) ([]*domain.Job, int, error) {
	return m.jobs, len(m.jobs), nil
}

func (m *mockStorage) GetAllJobs(ctx context.Context) ([]*domain.Job, error) {
	if m.getAllErr != nil {
		return nil, m.getAllErr
	}
	return m.jobs, nil
}

func (m *mockStorage) RecordMetrics(ctx context.Context, sample *domain.MetricSample) error {
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

// mockScheduler implements scheduler.JobSource for testing.
type mockScheduler struct {
	nodes    []*scheduler.Node
	nodesErr error
}

func (m *mockScheduler) Type() scheduler.SchedulerType {
	return scheduler.SchedulerTypeMock
}

func (m *mockScheduler) ListJobs(ctx context.Context, filter scheduler.JobFilter) ([]*scheduler.Job, error) {
	return nil, nil
}

func (m *mockScheduler) GetJob(ctx context.Context, id string) (*scheduler.Job, error) {
	return nil, nil
}

func (m *mockScheduler) GetJobMetrics(ctx context.Context, jobID string) ([]*scheduler.MetricSample, error) {
	return nil, nil
}

func (m *mockScheduler) SupportsMetrics() bool {
	return false
}

func (m *mockScheduler) ListNodes(ctx context.Context) ([]*scheduler.Node, error) {
	if m.nodesErr != nil {
		return nil, m.nodesErr
	}
	return m.nodes, nil
}

func TestNewExporter(t *testing.T) {
	store := &mockStorage{}
	exporter := NewExporter(store)

	if exporter == nil {
		t.Fatal("expected non-nil exporter")
	}
	if exporter.store != store {
		t.Error("expected store to be set")
	}
	if exporter.scheduler != nil {
		t.Error("expected scheduler to be nil")
	}
}

func TestNewExporterWithScheduler(t *testing.T) {
	store := &mockStorage{}
	sched := &mockScheduler{}
	exporter := NewExporterWithScheduler(store, sched)

	if exporter == nil {
		t.Fatal("expected non-nil exporter")
	}
	if exporter.scheduler != sched {
		t.Error("expected scheduler to be set")
	}
}

func TestRegister(t *testing.T) {
	store := &mockStorage{}
	exporter := NewExporter(store)

	registry := prometheus.NewRegistry()
	err := exporter.Register(registry)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Attempt to register to a new registry should succeed
	registry2 := prometheus.NewRegistry()
	exporter2 := NewExporter(store)
	err = exporter2.Register(registry2)
	if err != nil {
		t.Fatalf("expected no error registering to new registry, got %v", err)
	}
}

func TestRegister_DuplicateError(t *testing.T) {
	store := &mockStorage{}
	exporter := NewExporter(store)

	registry := prometheus.NewRegistry()
	err := exporter.Register(registry)
	if err != nil {
		t.Fatalf("expected no error on first register, got %v", err)
	}

	// Attempting to register the same exporter again should fail
	err = exporter.Register(registry)
	if err == nil {
		t.Error("expected error on duplicate registration")
	}
}

func TestCollect_EmptyJobs(t *testing.T) {
	store := &mockStorage{jobs: []*domain.Job{}}
	exporter := NewExporter(store)

	err := exporter.Collect(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestCollect_WithJobs(t *testing.T) {
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
		{
			ID:             "job-2",
			User:           "bob",
			Nodes:          []string{"node-2"},
			State:          domain.JobStateCompleted,
			StartTime:      now.Add(-2 * time.Hour),
			RuntimeSeconds: 3600,
			CPUUsage:       80.0,
			MemoryUsageMB:  4096,
		},
		{
			ID:            "job-3",
			User:          "alice",
			Nodes:         []string{"node-1", "node-2"},
			State:         domain.JobStatePending,
			StartTime:     now,
			CPUUsage:      0,
			MemoryUsageMB: 0,
		},
	}

	store := &mockStorage{jobs: jobs}
	exporter := NewExporter(store)

	err := exporter.Collect(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestCollect_WithScheduler(t *testing.T) {
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

	nodes := []*scheduler.Node{
		{
			Name:           "node-1",
			Hostname:       "node-1.cluster",
			State:          scheduler.NodeStateAllocated,
			CPUs:           64,
			RealMemoryMB:   256000,
			FreeMemoryMB:   128000,
			CPULoad:        32.0,
			AllocatedCPUs:  32,
			AllocatedMemMB: 128000,
			RunningJobs:    5,
		},
		{
			Name:           "node-2",
			Hostname:       "node-2.cluster",
			State:          scheduler.NodeStateIdle,
			CPUs:           64,
			RealMemoryMB:   256000,
			FreeMemoryMB:   256000,
			CPULoad:        0.5,
			AllocatedCPUs:  0,
			AllocatedMemMB: 0,
			RunningJobs:    0,
		},
		{
			Name:         "node-3",
			Hostname:     "node-3.cluster",
			State:        scheduler.NodeStateDown,
			CPUs:         64,
			RealMemoryMB: 256000,
			FreeMemoryMB: 0,
			CPULoad:      0,
		},
		{
			Name:         "node-4",
			Hostname:     "node-4.cluster",
			State:        scheduler.NodeStateDrained,
			CPUs:         64,
			RealMemoryMB: 256000,
			FreeMemoryMB: 0,
			CPULoad:      0,
		},
	}

	store := &mockStorage{jobs: jobs}
	sched := &mockScheduler{nodes: nodes}
	exporter := NewExporterWithScheduler(store, sched)

	err := exporter.Collect(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestCollect_StorageError(t *testing.T) {
	errTest := errors.New("database error")
	store := &mockStorage{getAllErr: errTest}
	exporter := NewExporter(store)

	err := exporter.Collect(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err != errTest {
		t.Errorf("expected %v, got %v", errTest, err)
	}
}

func TestCollect_AllJobStates(t *testing.T) {
	now := time.Now()
	endTime := now.Add(-10 * time.Minute)
	jobs := []*domain.Job{
		{ID: "job-1", User: "user", Nodes: []string{"node"}, State: domain.JobStatePending, StartTime: now},
		{ID: "job-2", User: "user", Nodes: []string{"node"}, State: domain.JobStateRunning, StartTime: now},
		{ID: "job-3", User: "user", Nodes: []string{"node"}, State: domain.JobStateCompleted, StartTime: now, EndTime: &endTime},
		{ID: "job-4", User: "user", Nodes: []string{"node"}, State: domain.JobStateFailed, StartTime: now, EndTime: &endTime},
		{ID: "job-5", User: "user", Nodes: []string{"node"}, State: domain.JobStateCancelled, StartTime: now, EndTime: &endTime},
	}

	store := &mockStorage{jobs: jobs}
	exporter := NewExporter(store)

	err := exporter.Collect(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestUpdateJobMetrics(t *testing.T) {
	store := &mockStorage{}
	exporter := NewExporter(store)

	gpuUsage := 85.5
	job := &domain.Job{
		ID:            "test-job",
		User:          "testuser",
		Nodes:         []string{"node-1"},
		State:         domain.JobStateRunning,
		StartTime:     time.Now().Add(-30 * time.Minute),
		CPUUsage:      75.0,
		MemoryUsageMB: 4096,
		GPUUsage:      &gpuUsage,
	}

	// Should not panic
	exporter.UpdateJobMetrics(job)
}

func TestUpdateJobMetrics_CompletedJob(t *testing.T) {
	store := &mockStorage{}
	exporter := NewExporter(store)

	job := &domain.Job{
		ID:             "test-job",
		User:           "testuser",
		Nodes:          []string{"node-1"},
		State:          domain.JobStateCompleted,
		StartTime:      time.Now().Add(-2 * time.Hour),
		RuntimeSeconds: 3600,
		CPUUsage:       75.0,
		MemoryUsageMB:  4096,
	}

	// Should not panic
	exporter.UpdateJobMetrics(job)
}

func TestUpdateJobMetrics_NoNodes(t *testing.T) {
	store := &mockStorage{}
	exporter := NewExporter(store)

	job := &domain.Job{
		ID:            "test-job",
		User:          "testuser",
		Nodes:         []string{},
		State:         domain.JobStateRunning,
		StartTime:     time.Now().Add(-30 * time.Minute),
		CPUUsage:      75.0,
		MemoryUsageMB: 4096,
	}

	// Should not panic
	exporter.UpdateJobMetrics(job)
}

func TestUpdateJobMetrics_NilGPU(t *testing.T) {
	store := &mockStorage{}
	exporter := NewExporter(store)

	job := &domain.Job{
		ID:            "test-job",
		User:          "testuser",
		Nodes:         []string{"node-1"},
		State:         domain.JobStateRunning,
		StartTime:     time.Now().Add(-30 * time.Minute),
		CPUUsage:      75.0,
		MemoryUsageMB: 4096,
		GPUUsage:      nil,
	}

	// Should not panic
	exporter.UpdateJobMetrics(job)
}

func TestIncrementJobsTotal(t *testing.T) {
	store := &mockStorage{}
	exporter := NewExporter(store)

	// Should not panic
	exporter.IncrementJobsTotal()
	exporter.IncrementJobsTotal()
}

func TestHandler(t *testing.T) {
	store := &mockStorage{jobs: []*domain.Job{
		{
			ID:            "job-1",
			User:          "alice",
			Nodes:         []string{"node-1"},
			State:         domain.JobStateRunning,
			StartTime:     time.Now().Add(-30 * time.Minute),
			CPUUsage:      50.0,
			MemoryUsageMB: 2048,
		},
	}}
	exporter := NewExporter(store)

	handler := exporter.Handler()
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestDefaultHandler(t *testing.T) {
	handler := DefaultHandler()
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestCollect_NodeAggregation(t *testing.T) {
	now := time.Now()
	gpuUsage1 := 60.0
	gpuUsage2 := 80.0
	jobs := []*domain.Job{
		{
			ID:            "job-1",
			User:          "alice",
			Nodes:         []string{"node-1"},
			State:         domain.JobStateRunning,
			StartTime:     now.Add(-30 * time.Minute),
			CPUUsage:      50.0,
			MemoryUsageMB: 2048,
			GPUUsage:      &gpuUsage1,
		},
		{
			ID:            "job-2",
			User:          "bob",
			Nodes:         []string{"node-1"},
			State:         domain.JobStateRunning,
			StartTime:     now.Add(-15 * time.Minute),
			CPUUsage:      70.0,
			MemoryUsageMB: 4096,
			GPUUsage:      &gpuUsage2,
		},
		{
			ID:            "job-3",
			User:          "charlie",
			Nodes:         []string{"node-1"},
			State:         domain.JobStateCompleted, // Completed jobs should not be in node aggregation
			StartTime:     now.Add(-2 * time.Hour),
			CPUUsage:      90.0,
			MemoryUsageMB: 8192,
		},
	}

	store := &mockStorage{jobs: jobs}
	exporter := NewExporter(store)

	err := exporter.Collect(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestCollect_MultiNodeJobs(t *testing.T) {
	now := time.Now()
	jobs := []*domain.Job{
		{
			ID:            "job-1",
			User:          "alice",
			Nodes:         []string{"node-1", "node-2", "node-3"},
			State:         domain.JobStateRunning,
			StartTime:     now.Add(-30 * time.Minute),
			CPUUsage:      50.0,
			MemoryUsageMB: 2048,
		},
	}

	store := &mockStorage{jobs: jobs}
	exporter := NewExporter(store)

	err := exporter.Collect(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestCollect_SchedulerNodeError(t *testing.T) {
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
	sched := &mockScheduler{nodesErr: errors.New("scheduler error")}
	exporter := NewExporterWithScheduler(store, sched)

	// Should not fail even if scheduler fails
	err := exporter.Collect(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestCollect_SchedulerNodeCPUUsage(t *testing.T) {
	// Test the case where scheduler provides node CPU load but no job-based data exists
	jobs := []*domain.Job{} // No jobs running on node-2

	nodes := []*scheduler.Node{
		{
			Name:         "node-2",
			Hostname:     "node-2.cluster",
			State:        scheduler.NodeStateIdle,
			CPUs:         64,
			RealMemoryMB: 256000,
			FreeMemoryMB: 256000,
			CPULoad:      32.0, // 50% when divided by 64 CPUs
		},
	}

	store := &mockStorage{jobs: jobs}
	sched := &mockScheduler{nodes: nodes}
	exporter := NewExporterWithScheduler(store, sched)

	err := exporter.Collect(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestCollect_SchedulerMemoryUsage(t *testing.T) {
	// Test the case where scheduler provides memory usage data
	jobs := []*domain.Job{}

	nodes := []*scheduler.Node{
		{
			Name:           "node-1",
			Hostname:       "node-1.cluster",
			State:          scheduler.NodeStateAllocated,
			CPUs:           64,
			RealMemoryMB:   256000,
			FreeMemoryMB:   128000, // Half used
			AllocatedMemMB: 128000,
		},
	}

	store := &mockStorage{jobs: jobs}
	sched := &mockScheduler{nodes: nodes}
	exporter := NewExporterWithScheduler(store, sched)

	err := exporter.Collect(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestCollect_HighCPULoad(t *testing.T) {
	// Test the case where CPU load exceeds 100%
	jobs := []*domain.Job{}

	nodes := []*scheduler.Node{
		{
			Name:         "node-1",
			Hostname:     "node-1.cluster",
			State:        scheduler.NodeStateAllocated,
			CPUs:         4,
			RealMemoryMB: 32000,
			FreeMemoryMB: 16000,
			CPULoad:      8.0, // 200% load, should be capped at 100
		},
	}

	store := &mockStorage{jobs: jobs}
	sched := &mockScheduler{nodes: nodes}
	exporter := NewExporterWithScheduler(store, sched)

	err := exporter.Collect(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestCollect_JobsWithNoNodes(t *testing.T) {
	now := time.Now()
	jobs := []*domain.Job{
		{
			ID:            "job-1",
			User:          "alice",
			Nodes:         []string{}, // No nodes assigned yet
			State:         domain.JobStatePending,
			StartTime:     now,
			CPUUsage:      0,
			MemoryUsageMB: 0,
		},
	}

	store := &mockStorage{jobs: jobs}
	exporter := NewExporter(store)

	err := exporter.Collect(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestCollect_RunningJobRuntime(t *testing.T) {
	now := time.Now()
	jobs := []*domain.Job{
		{
			ID:            "job-1",
			User:          "alice",
			Nodes:         []string{"node-1"},
			State:         domain.JobStateRunning,
			StartTime:     now.Add(-1 * time.Hour), // Started 1 hour ago
			CPUUsage:      50.0,
			MemoryUsageMB: 2048,
		},
	}

	store := &mockStorage{jobs: jobs}
	exporter := NewExporter(store)

	err := exporter.Collect(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestUpdateJobMetricsDomain_RunningJob(t *testing.T) {
	store := &mockStorage{}
	exporter := NewExporter(store)

	gpuUsage := 75.0
	job := &domain.Job{
		ID:             "job-domain-1",
		User:           "testuser",
		Nodes:          []string{"node-1", "node-2"},
		State:          domain.JobStateRunning,
		StartTime:      time.Now().Add(-30 * time.Minute),
		CPUUsage:       65.5,
		MemoryUsageMB:  4096,
		GPUUsage:       &gpuUsage,
		RuntimeSeconds: 0, // Running job uses time.Since
	}

	// Should not panic
	exporter.UpdateJobMetricsDomain(job)
}

func TestUpdateJobMetricsDomain_CompletedJob(t *testing.T) {
	store := &mockStorage{}
	exporter := NewExporter(store)

	endTime := time.Now()
	job := &domain.Job{
		ID:             "job-domain-2",
		User:           "testuser",
		Nodes:          []string{"node-1"},
		State:          domain.JobStateCompleted,
		StartTime:      time.Now().Add(-time.Hour),
		EndTime:        &endTime,
		CPUUsage:       85.0,
		MemoryUsageMB:  8192,
		GPUUsage:       nil,
		RuntimeSeconds: 3600,
	}

	// Should not panic
	exporter.UpdateJobMetricsDomain(job)
}

func TestUpdateJobMetricsDomain_NoNodes(t *testing.T) {
	store := &mockStorage{}
	exporter := NewExporter(store)

	job := &domain.Job{
		ID:            "job-domain-3",
		User:          "testuser",
		Nodes:         []string{}, // Empty nodes
		State:         domain.JobStatePending,
		StartTime:     time.Now(),
		CPUUsage:      0,
		MemoryUsageMB: 0,
	}

	// Should not panic with empty nodes
	exporter.UpdateJobMetricsDomain(job)
}

func TestUpdateGPUDeviceMetrics(t *testing.T) {
	store := &mockStorage{}
	exporter := NewExporter(store)

	metrics := []GPUDeviceMetric{
		{
			JobID:          "job-gpu-1",
			Node:           "node-1",
			Vendor:         "nvidia",
			DeviceID:       "0",
			Utilization:    85.5,
			MemoryUsedMB:   4096,
			PowerWatts:     250.0,
			TemperatureCel: 72.0,
		},
		{
			JobID:          "job-gpu-1",
			Node:           "node-1",
			Vendor:         "nvidia",
			DeviceID:       "1",
			Utilization:    90.0,
			MemoryUsedMB:   6144,
			PowerWatts:     280.0,
			TemperatureCel: 75.0,
		},
	}

	// Should not panic
	exporter.UpdateGPUDeviceMetrics(metrics)
}

func TestUpdateGPUDeviceMetrics_Empty(t *testing.T) {
	store := &mockStorage{}
	exporter := NewExporter(store)

	// Should not panic with empty slice
	exporter.UpdateGPUDeviceMetrics([]GPUDeviceMetric{})
}

func TestUpdateGPUDeviceMetrics_NoPowerOrTemp(t *testing.T) {
	store := &mockStorage{}
	exporter := NewExporter(store)

	metrics := []GPUDeviceMetric{
		{
			JobID:          "job-gpu-2",
			Node:           "node-1",
			Vendor:         "amd",
			DeviceID:       "0",
			Utilization:    50.0,
			MemoryUsedMB:   2048,
			PowerWatts:     0, // Not available
			TemperatureCel: 0, // Not available
		},
	}

	// Should not panic
	exporter.UpdateGPUDeviceMetrics(metrics)
}

func TestClearGPUDeviceMetrics(t *testing.T) {
	store := &mockStorage{}
	exporter := NewExporter(store)

	// First add some metrics
	metrics := []GPUDeviceMetric{
		{
			JobID:          "job-clear",
			Node:           "node-1",
			Vendor:         "nvidia",
			DeviceID:       "0",
			Utilization:    75.0,
			MemoryUsedMB:   4096,
			PowerWatts:     200.0,
			TemperatureCel: 65.0,
		},
	}
	exporter.UpdateGPUDeviceMetrics(metrics)

	// Then clear them
	exporter.ClearGPUDeviceMetrics("job-clear")
	// Should not panic
}

func TestClearGPUDeviceMetrics_NonExistent(t *testing.T) {
	store := &mockStorage{}
	exporter := NewExporter(store)

	// Should not panic even if job doesn't exist
	exporter.ClearGPUDeviceMetrics("nonexistent-job")
}
