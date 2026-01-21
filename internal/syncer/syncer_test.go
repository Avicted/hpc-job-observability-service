package syncer

import (
	"context"
	"testing"
	"time"

	"github.com/avic/hpc-job-observability-service/internal/scheduler"
	"github.com/avic/hpc-job-observability-service/internal/storage"
)

// mockJobSource implements scheduler.JobSource for testing.
type mockJobSource struct {
	jobs    []*scheduler.Job
	listErr error
}

func (m *mockJobSource) Type() scheduler.SchedulerType { return scheduler.SchedulerTypeMock }
func (m *mockJobSource) ListJobs(ctx context.Context, filter scheduler.JobFilter) ([]*scheduler.Job, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.jobs, nil
}
func (m *mockJobSource) GetJob(ctx context.Context, id string) (*scheduler.Job, error) {
	return nil, nil
}
func (m *mockJobSource) GetJobMetrics(ctx context.Context, jobID string) ([]*scheduler.MetricSample, error) {
	return nil, nil
}
func (m *mockJobSource) SupportsMetrics() bool { return false }
func (m *mockJobSource) ListNodes(ctx context.Context) ([]*scheduler.Node, error) {
	return nil, nil
}

// mockStorage implements storage.Storage for testing.
type mockStorage struct {
	upsertedJobs []*storage.Job
	upsertErr    error
}

func (m *mockStorage) CreateJob(ctx context.Context, job *storage.Job) error { return nil }
func (m *mockStorage) GetJob(ctx context.Context, id string) (*storage.Job, error) {
	return nil, storage.ErrJobNotFound
}
func (m *mockStorage) UpdateJob(ctx context.Context, job *storage.Job) error { return nil }
func (m *mockStorage) UpsertJob(ctx context.Context, job *storage.Job) error {
	if m.upsertErr != nil {
		return m.upsertErr
	}
	m.upsertedJobs = append(m.upsertedJobs, job)
	return nil
}
func (m *mockStorage) DeleteJob(ctx context.Context, id string) error { return nil }
func (m *mockStorage) ListJobs(ctx context.Context, filter storage.JobFilter) ([]*storage.Job, int, error) {
	return nil, 0, nil
}
func (m *mockStorage) GetAllJobs(ctx context.Context) ([]*storage.Job, error) { return nil, nil }
func (m *mockStorage) RecordMetrics(ctx context.Context, sample *storage.MetricSample) error {
	return nil
}
func (m *mockStorage) GetJobMetrics(ctx context.Context, jobID string, filter storage.MetricsFilter) ([]*storage.MetricSample, int, error) {
	return nil, 0, nil
}
func (m *mockStorage) GetLatestMetrics(ctx context.Context, jobID string) (*storage.MetricSample, error) {
	return nil, nil
}
func (m *mockStorage) DeleteMetricsBefore(cutoff time.Time) error { return nil }
func (m *mockStorage) Migrate() error                             { return nil }
func (m *mockStorage) Close() error                               { return nil }
func (m *mockStorage) SeedDemoData() error                        { return nil }

func TestSyncer_SyncNow(t *testing.T) {
	now := time.Now()
	jobs := []*scheduler.Job{
		{
			ID:            "job-1",
			User:          "alice",
			Nodes:         []string{"node-1"},
			State:         scheduler.JobStateRunning,
			StartTime:     now.Add(-10 * time.Minute),
			CPUUsage:      50.0,
			MemoryUsageMB: 2048,
		},
		{
			ID:            "job-2",
			User:          "bob",
			Nodes:         []string{"node-2", "node-3"},
			State:         scheduler.JobStateCompleted,
			StartTime:     now.Add(-1 * time.Hour),
			EndTime:       timePtr(now.Add(-30 * time.Minute)),
			CPUUsage:      75.0,
			MemoryUsageMB: 4096,
		},
	}

	source := &mockJobSource{jobs: jobs}
	store := &mockStorage{}

	config := Config{
		SyncInterval: 100 * time.Millisecond,
		InitialDelay: 0,
	}

	syncer := New(source, store, config)
	syncer.SyncNow()

	if len(store.upsertedJobs) != 2 {
		t.Errorf("expected 2 upserted jobs, got %d", len(store.upsertedJobs))
	}

	// Verify first job was converted correctly
	if store.upsertedJobs[0].ID != "job-1" {
		t.Errorf("expected job ID 'job-1', got '%s'", store.upsertedJobs[0].ID)
	}
	if store.upsertedJobs[0].User != "alice" {
		t.Errorf("expected user 'alice', got '%s'", store.upsertedJobs[0].User)
	}
	if store.upsertedJobs[0].State != storage.JobStateRunning {
		t.Errorf("expected state 'running', got '%s'", store.upsertedJobs[0].State)
	}
}

func TestSyncer_StartStop(t *testing.T) {
	source := &mockJobSource{jobs: []*scheduler.Job{}}
	store := &mockStorage{}

	config := Config{
		SyncInterval: 50 * time.Millisecond,
		InitialDelay: 10 * time.Millisecond,
	}

	syncer := New(source, store, config)

	syncer.Start()
	time.Sleep(150 * time.Millisecond) // Allow a few sync cycles
	syncer.Stop()

	// Should not panic or hang
}

func TestConvertToStorageJob(t *testing.T) {
	now := time.Now()
	endTime := now.Add(10 * time.Minute)
	gpuUsage := 45.5

	schedJob := &scheduler.Job{
		ID:             "test-123",
		User:           "testuser",
		Nodes:          []string{"node-a", "node-b"},
		State:          scheduler.JobStateCompleted,
		StartTime:      now,
		EndTime:        &endTime,
		RuntimeSeconds: 600.0,
		CPUUsage:       80.0,
		MemoryUsageMB:  8192,
		GPUUsage:       &gpuUsage,
		Scheduler: &scheduler.SchedulerInfo{
			Type:          scheduler.SchedulerTypeSlurm,
			ExternalJobID: "slurm-456",
			RawState:      "COMPLETED",
			Partition:     "gpu",
		},
	}

	storageJob := convertToStorageJob(schedJob)

	if storageJob.ID != "test-123" {
		t.Errorf("ID mismatch: got %s", storageJob.ID)
	}
	if storageJob.User != "testuser" {
		t.Errorf("User mismatch: got %s", storageJob.User)
	}
	if len(storageJob.Nodes) != 2 {
		t.Errorf("Nodes length mismatch: got %d", len(storageJob.Nodes))
	}
	if storageJob.State != storage.JobStateCompleted {
		t.Errorf("State mismatch: got %s", storageJob.State)
	}
	if storageJob.CPUUsage != 80.0 {
		t.Errorf("CPUUsage mismatch: got %f", storageJob.CPUUsage)
	}
	if storageJob.GPUUsage == nil || *storageJob.GPUUsage != 45.5 {
		t.Errorf("GPUUsage mismatch")
	}
	if storageJob.Scheduler == nil {
		t.Error("Scheduler info should not be nil")
	} else {
		if storageJob.Scheduler.Type != storage.SchedulerTypeSlurm {
			t.Errorf("Scheduler type mismatch: got %s", storageJob.Scheduler.Type)
		}
		if storageJob.Scheduler.Partition != "gpu" {
			t.Errorf("Partition mismatch: got %s", storageJob.Scheduler.Partition)
		}
	}
}

func timePtr(t time.Time) *time.Time {
	return &t
}
