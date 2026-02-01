package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/api/types"
	"github.com/Avicted/hpc-job-observability-service/internal/domain"
	"github.com/Avicted/hpc-job-observability-service/internal/storage"
	"github.com/Avicted/hpc-job-observability-service/internal/utils/audit"
	"github.com/Avicted/hpc-job-observability-service/internal/utils/mapper"
	"github.com/Avicted/hpc-job-observability-service/internal/utils/metrics"
)

// mockStorage implements storage.Store for testing services.
type mockStorage struct {
	jobs         map[string]*domain.Job
	createErr    error
	getErr       error
	updateErr    error
	deleteErr    error
	listErr      error
	metricsErr   error
	recordErr    error
	metricsStore []*domain.MetricSample
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		jobs: make(map[string]*domain.Job),
	}
}

func (m *mockStorage) CreateJob(ctx context.Context, job *domain.Job) error {
	if m.createErr != nil {
		return m.createErr
	}
	if _, exists := m.jobs[job.ID]; exists {
		return storage.ErrConflict
	}
	m.jobs[job.ID] = job
	return nil
}

func (m *mockStorage) GetJob(ctx context.Context, id string) (*domain.Job, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	job, exists := m.jobs[id]
	if !exists {
		return nil, storage.ErrNotFound
	}
	return job, nil
}

func (m *mockStorage) UpdateJob(ctx context.Context, job *domain.Job) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	m.jobs[job.ID] = job
	return nil
}

func (m *mockStorage) UpsertJob(ctx context.Context, job *domain.Job) error {
	m.jobs[job.ID] = job
	return nil
}

func (m *mockStorage) DeleteJob(ctx context.Context, id string) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	if _, exists := m.jobs[id]; !exists {
		return storage.ErrNotFound
	}
	delete(m.jobs, id)
	return nil
}

func (m *mockStorage) ListJobs(ctx context.Context, filter domain.JobFilter) ([]*domain.Job, int, error) {
	if m.listErr != nil {
		return nil, 0, m.listErr
	}
	var result []*domain.Job
	for _, job := range m.jobs {
		// Apply state filter
		if filter.State != nil && job.State != *filter.State {
			continue
		}
		// Apply user filter
		if filter.User != nil && job.User != *filter.User {
			continue
		}
		result = append(result, job)
	}
	return result, len(result), nil
}

func (m *mockStorage) GetAllJobs(ctx context.Context) ([]*domain.Job, error) {
	var result []*domain.Job
	for _, job := range m.jobs {
		result = append(result, job)
	}
	return result, nil
}

func (m *mockStorage) RecordMetrics(ctx context.Context, sample *domain.MetricSample) error {
	if m.recordErr != nil {
		return m.recordErr
	}
	m.metricsStore = append(m.metricsStore, sample)
	return nil
}

func (m *mockStorage) GetJobMetrics(ctx context.Context, jobID string, filter domain.MetricsFilter) ([]*domain.MetricSample, int, error) {
	if m.metricsErr != nil {
		return nil, 0, m.metricsErr
	}
	var result []*domain.MetricSample
	for _, s := range m.metricsStore {
		if s.JobID == jobID {
			result = append(result, s)
		}
	}
	return result, len(result), nil
}

func (m *mockStorage) GetLatestMetrics(ctx context.Context, jobID string) (*domain.MetricSample, error) {
	var latest *domain.MetricSample
	for _, s := range m.metricsStore {
		if s.JobID == jobID {
			if latest == nil || s.Timestamp.After(latest.Timestamp) {
				latest = s
			}
		}
	}
	return latest, nil
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

func newMockExporter() *metrics.Exporter {
	// Create a real exporter with the mock storage - it won't actually record metrics in tests
	return metrics.NewExporter(newMockStorage())
}

func testAuditContext() *audit.Context {
	return audit.NewContext("test-user", "test-service", "test-correlation-id")
}

// --- JobService Tests ---

func TestJobService_CreateJob_Success(t *testing.T) {
	store := newMockStorage()
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	input := CreateJobInput{
		ID:    "job-123",
		User:  "testuser",
		Nodes: []string{"node1", "node2"},
		Audit: testAuditContext(),
	}

	job, err := svc.CreateJob(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if job.ID != "job-123" {
		t.Errorf("ID = %q, want %q", job.ID, "job-123")
	}
	if job.User != "testuser" {
		t.Errorf("User = %q, want %q", job.User, "testuser")
	}
	if job.State != domain.JobStateRunning {
		t.Errorf("State = %v, want %v", job.State, domain.JobStateRunning)
	}
	if len(job.Nodes) != 2 {
		t.Errorf("Nodes len = %d, want 2", len(job.Nodes))
	}
}

func TestJobService_CreateJob_ValidationErrors(t *testing.T) {
	store := newMockStorage()
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	tests := []struct {
		name    string
		input   CreateJobInput
		wantErr string
	}{
		{
			name:    "empty ID",
			input:   CreateJobInput{ID: "", User: "user", Nodes: []string{"n1"}, Audit: testAuditContext()},
			wantErr: "Job ID is required",
		},
		{
			name:    "empty User",
			input:   CreateJobInput{ID: "job-1", User: "", Nodes: []string{"n1"}, Audit: testAuditContext()},
			wantErr: "User is required",
		},
		{
			name:    "empty Nodes",
			input:   CreateJobInput{ID: "job-1", User: "user", Nodes: []string{}, Audit: testAuditContext()},
			wantErr: "At least one node is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := svc.CreateJob(context.Background(), tt.input)
			if err == nil {
				t.Fatal("expected error")
			}

			var validErr *ValidationError
			if !errors.As(err, &validErr) {
				t.Fatalf("expected ValidationError, got %T", err)
			}

			if validErr.Message != tt.wantErr {
				t.Errorf("error message = %q, want %q", validErr.Message, tt.wantErr)
			}
		})
	}
}

func TestJobService_CreateJob_AlreadyExists(t *testing.T) {
	store := newMockStorage()
	store.jobs["job-dup"] = &domain.Job{ID: "job-dup"}
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	input := CreateJobInput{
		ID:    "job-dup",
		User:  "user",
		Nodes: []string{"n1"},
		Audit: testAuditContext(),
	}

	_, err := svc.CreateJob(context.Background(), input)
	if !errors.Is(err, ErrJobAlreadyExists) {
		t.Errorf("expected ErrJobAlreadyExists, got %v", err)
	}
}

func TestJobService_CreateJob_WithScheduler(t *testing.T) {
	store := newMockStorage()
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	schedType := types.Slurm
	partition := "compute"

	input := CreateJobInput{
		ID:    "job-sched",
		User:  "user",
		Nodes: []string{"n1"},
		Scheduler: &types.SchedulerInfo{
			Type:      &schedType,
			Partition: &partition,
		},
		Audit: testAuditContext(),
	}

	job, err := svc.CreateJob(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if job.Scheduler == nil {
		t.Fatal("Scheduler should not be nil")
	}
	if job.Scheduler.Type != domain.SchedulerTypeSlurm {
		t.Errorf("Scheduler.Type = %v, want slurm", job.Scheduler.Type)
	}
	if job.Scheduler.Partition != "compute" {
		t.Errorf("Scheduler.Partition = %q, want compute", job.Scheduler.Partition)
	}
}

func TestJobService_GetJob_Success(t *testing.T) {
	store := newMockStorage()
	store.jobs["job-get"] = &domain.Job{ID: "job-get", User: "user", State: domain.JobStateRunning}
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	job, err := svc.GetJob(context.Background(), "job-get")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if job.ID != "job-get" {
		t.Errorf("ID = %q, want %q", job.ID, "job-get")
	}
}

func TestJobService_GetJob_NotFound(t *testing.T) {
	store := newMockStorage()
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	_, err := svc.GetJob(context.Background(), "nonexistent")
	if !errors.Is(err, ErrJobNotFound) {
		t.Errorf("expected ErrJobNotFound, got %v", err)
	}
}

func TestJobService_ListJobs_Success(t *testing.T) {
	store := newMockStorage()
	store.jobs["job-1"] = &domain.Job{ID: "job-1", User: "user1", State: domain.JobStateRunning}
	store.jobs["job-2"] = &domain.Job{ID: "job-2", User: "user2", State: domain.JobStateCompleted}
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	output, err := svc.ListJobs(context.Background(), ListJobsInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Total != 2 {
		t.Errorf("Total = %d, want 2", output.Total)
	}
	if len(output.Jobs) != 2 {
		t.Errorf("Jobs len = %d, want 2", len(output.Jobs))
	}
}

func TestJobService_ListJobs_WithFilters(t *testing.T) {
	store := newMockStorage()
	store.jobs["job-1"] = &domain.Job{ID: "job-1", User: "user1", State: domain.JobStateRunning}
	store.jobs["job-2"] = &domain.Job{ID: "job-2", User: "user2", State: domain.JobStateCompleted}
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	// Filter by state
	state := "running"
	output, err := svc.ListJobs(context.Background(), ListJobsInput{State: &state})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Total != 1 {
		t.Errorf("Total = %d, want 1", output.Total)
	}
}

func TestJobService_ListJobs_WithPagination(t *testing.T) {
	store := newMockStorage()
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	limit := 10
	offset := 5
	output, err := svc.ListJobs(context.Background(), ListJobsInput{Limit: &limit, Offset: &offset})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Limit != 10 {
		t.Errorf("Limit = %d, want 10", output.Limit)
	}
	if output.Offset != 5 {
		t.Errorf("Offset = %d, want 5", output.Offset)
	}
}

func TestJobService_UpdateJob_Success(t *testing.T) {
	store := newMockStorage()
	store.jobs["job-update"] = &domain.Job{ID: "job-update", User: "user", State: domain.JobStateRunning}
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	cpuUsage := 75.5
	memUsage := 2048
	newState := types.Completed

	input := UpdateJobInput{
		State:         &newState,
		CPUUsage:      &cpuUsage,
		MemoryUsageMB: &memUsage,
		Audit:         testAuditContext(),
	}

	job, err := svc.UpdateJob(context.Background(), "job-update", input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if job.State != domain.JobStateCompleted {
		t.Errorf("State = %v, want %v", job.State, domain.JobStateCompleted)
	}
	if job.CPUUsage != 75.5 {
		t.Errorf("CPUUsage = %v, want 75.5", job.CPUUsage)
	}
	if job.MemoryUsageMB != 2048 {
		t.Errorf("MemoryUsageMB = %v, want 2048", job.MemoryUsageMB)
	}
}

func TestJobService_UpdateJob_NotFound(t *testing.T) {
	store := newMockStorage()
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	input := UpdateJobInput{Audit: testAuditContext()}
	_, err := svc.UpdateJob(context.Background(), "nonexistent", input)
	if !errors.Is(err, ErrJobNotFound) {
		t.Errorf("expected ErrJobNotFound, got %v", err)
	}
}

func TestJobService_UpdateJob_InvalidState(t *testing.T) {
	store := newMockStorage()
	store.jobs["job-invalid"] = &domain.Job{ID: "job-invalid", User: "user", State: domain.JobStateRunning}
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	invalidState := types.JobState("invalid")
	input := UpdateJobInput{
		State: &invalidState,
		Audit: testAuditContext(),
	}

	_, err := svc.UpdateJob(context.Background(), "job-invalid", input)
	if err == nil {
		t.Fatal("expected error for invalid state")
	}

	var validErr *ValidationError
	if !errors.As(err, &validErr) {
		t.Fatalf("expected ValidationError, got %T", err)
	}
}

func TestJobService_DeleteJob_Success(t *testing.T) {
	store := newMockStorage()
	store.jobs["job-delete"] = &domain.Job{ID: "job-delete", User: "user"}
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	err := svc.DeleteJob(context.Background(), "job-delete", testAuditContext())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, exists := store.jobs["job-delete"]; exists {
		t.Error("job should have been deleted")
	}
}

func TestJobService_DeleteJob_NotFound(t *testing.T) {
	store := newMockStorage()
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	err := svc.DeleteJob(context.Background(), "nonexistent", testAuditContext())
	if !errors.Is(err, ErrJobNotFound) {
		t.Errorf("expected ErrJobNotFound, got %v", err)
	}
}

func TestJobService_Mapper(t *testing.T) {
	store := newMockStorage()
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	if svc.Mapper() != m {
		t.Error("expected Mapper() to return the injected mapper")
	}
}

func TestValidationError_Error(t *testing.T) {
	err := &ValidationError{Message: "test error"}
	if err.Error() != "test error" {
		t.Errorf("Error() = %q, want %q", err.Error(), "test error")
	}
}

func TestJobService_GetJob_InternalError(t *testing.T) {
	store := newMockStorage()
	store.getErr = errors.New("database connection failed")
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	_, err := svc.GetJob(context.Background(), "any-job")
	if !errors.Is(err, ErrInternalError) {
		t.Errorf("expected ErrInternalError, got %v", err)
	}
}

func TestJobService_ListJobs_Error(t *testing.T) {
	store := newMockStorage()
	store.listErr = errors.New("database error")
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	_, err := svc.ListJobs(context.Background(), ListJobsInput{})
	if !errors.Is(err, ErrInternalError) {
		t.Errorf("expected ErrInternalError, got %v", err)
	}
}

func TestJobService_CreateJob_InternalError(t *testing.T) {
	store := newMockStorage()
	store.createErr = errors.New("database error")
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	input := CreateJobInput{
		ID:    "job-err",
		User:  "user",
		Nodes: []string{"node1"},
		Audit: testAuditContext(),
	}

	_, err := svc.CreateJob(context.Background(), input)
	if !errors.Is(err, ErrInternalError) {
		t.Errorf("expected ErrInternalError, got %v", err)
	}
}

func TestJobService_UpdateJob_StorageError(t *testing.T) {
	store := newMockStorage()
	store.jobs["job-upderr"] = &domain.Job{ID: "job-upderr", User: "user", State: domain.JobStateRunning}
	store.updateErr = errors.New("database error")
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	input := UpdateJobInput{Audit: testAuditContext()}
	_, err := svc.UpdateJob(context.Background(), "job-upderr", input)
	if !errors.Is(err, ErrInternalError) {
		t.Errorf("expected ErrInternalError, got %v", err)
	}
}

func TestJobService_UpdateJob_GetError(t *testing.T) {
	store := newMockStorage()
	store.getErr = errors.New("database error")
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	input := UpdateJobInput{Audit: testAuditContext()}
	_, err := svc.UpdateJob(context.Background(), "any-job", input)
	if !errors.Is(err, ErrInternalError) {
		t.Errorf("expected ErrInternalError, got %v", err)
	}
}

func TestJobService_DeleteJob_StorageError(t *testing.T) {
	store := newMockStorage()
	store.jobs["job-delerr"] = &domain.Job{ID: "job-delerr", User: "user"}
	store.deleteErr = errors.New("database error")
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	err := svc.DeleteJob(context.Background(), "job-delerr", testAuditContext())
	if !errors.Is(err, ErrInternalError) {
		t.Errorf("expected ErrInternalError, got %v", err)
	}
}

func TestJobService_UpdateJob_WithGPUUsage(t *testing.T) {
	store := newMockStorage()
	store.jobs["job-gpu"] = &domain.Job{ID: "job-gpu", User: "user", State: domain.JobStateRunning}
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	gpuUsage := 85.5
	input := UpdateJobInput{
		GPUUsage: &gpuUsage,
		Audit:    testAuditContext(),
	}

	job, err := svc.UpdateJob(context.Background(), "job-gpu", input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if job.GPUUsage == nil || *job.GPUUsage != 85.5 {
		t.Errorf("GPUUsage = %v, want 85.5", job.GPUUsage)
	}
}

func TestJobService_ListJobs_AllFilters(t *testing.T) {
	store := newMockStorage()
	store.jobs["job-1"] = &domain.Job{ID: "job-1", User: "alice", State: domain.JobStateRunning, Nodes: []string{"node1"}}
	store.jobs["job-2"] = &domain.Job{ID: "job-2", User: "bob", State: domain.JobStateCompleted, Nodes: []string{"node2"}}
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewJobService(store, exporter, m)

	user := "alice"
	state := "running"
	node := "node1"
	limit := 10
	offset := 0

	input := ListJobsInput{
		User:   &user,
		State:  &state,
		Node:   &node,
		Limit:  &limit,
		Offset: &offset,
	}

	output, err := svc.ListJobs(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Limit != 10 {
		t.Errorf("Limit = %d, want 10", output.Limit)
	}
	if output.Offset != 0 {
		t.Errorf("Offset = %d, want 0", output.Offset)
	}
}
