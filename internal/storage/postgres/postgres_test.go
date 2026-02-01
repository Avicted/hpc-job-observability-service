package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/domain"
	"github.com/Avicted/hpc-job-observability-service/internal/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// setupTestStore creates a test Postgres container and returns a configured store.
func setupTestStore(t *testing.T) (*Store, func()) {
	t.Helper()

	ctx := context.Background()

	// Use a shorter timeout for container creation to fail fast if resources are exhausted
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()

	// Start Postgres container
	req := testcontainers.ContainerRequest{
		Image:        "postgres:15-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "testuser",
			"POSTGRES_PASSWORD": "testpass",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(40 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("Failed to start container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("Failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("Failed to get container port: %v", err)
	}

	// Create connection pool (use fresh context for connection, not the timed one)
	connCtx := context.Background()
	dsn := fmt.Sprintf("postgres://testuser:testpass@%s:%s/testdb?sslmode=disable", host, port.Port())
	cfg := DefaultConfig(dsn)
	store, err := NewStore(connCtx, cfg)
	if err != nil {
		container.Terminate(context.Background())
		t.Fatalf("Failed to create store: %v", err)
	}

	// Register metrics
	reg := prometheus.NewRegistry()
	reg.MustRegister(store.GetMetrics()...)

	// Run migrations
	if err := store.Migrate(connCtx); err != nil {
		store.Close()
		container.Terminate(context.Background())
		t.Fatalf("Failed to run migrations: %v", err)
	}

	cleanup := func() {
		store.Close()
		// Use fresh context for cleanup to ensure it completes even if test context is cancelled
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := container.Terminate(cleanupCtx); err != nil {
			t.Logf("Warning: failed to terminate container: %v", err)
		}
	}

	return store, cleanup
}

// TestDefaultConfig validates default configuration values.
func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig("postgres://localhost/test")

	if cfg.MaxConns != 25 {
		t.Errorf("expected MaxConns=25, got %d", cfg.MaxConns)
	}
	if cfg.MinConns != 5 {
		t.Errorf("expected MinConns=5, got %d", cfg.MinConns)
	}
	if cfg.MaxConnLifetime != 5*time.Minute {
		t.Errorf("expected MaxConnLifetime=5m, got %v", cfg.MaxConnLifetime)
	}
	if cfg.MaxConnIdleTime != 30*time.Second {
		t.Errorf("expected MaxConnIdleTime=30s, got %v", cfg.MaxConnIdleTime)
	}
}

// TestCreateJob_Success validates successful job creation.
func TestCreateJob_Success(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	auditInfo := domain.NewAuditInfo("testuser", "test")
	ctx = domain.WithAuditInfo(ctx, auditInfo)

	now := time.Now().Truncate(time.Millisecond)
	job := &domain.Job{
		ID:            "job-1",
		User:          "alice",
		Nodes:         []string{"node1", "node2"},
		NodeCount:     2,
		State:         domain.JobStateRunning,
		StartTime:     now,
		CreatedAt:     now,
		UpdatedAt:     now,
		CPUUsage:      50.0,
		MemoryUsageMB: 2048,
		Audit:         auditInfo,
	}

	err := store.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	// Verify job was created
	retrieved, err := store.GetJob(ctx, "job-1")
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}

	if retrieved.ID != job.ID {
		t.Errorf("expected ID=%s, got %s", job.ID, retrieved.ID)
	}
	if retrieved.User != job.User {
		t.Errorf("expected User=%s, got %s", job.User, retrieved.User)
	}
	if retrieved.State != job.State {
		t.Errorf("expected State=%s, got %s", job.State, retrieved.State)
	}
	if len(retrieved.Nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(retrieved.Nodes))
	}
}

// TestCreateJob_Conflict validates unique constraint enforcement.
func TestCreateJob_Conflict(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	auditInfo := domain.NewAuditInfo("testuser", "test")
	ctx = domain.WithAuditInfo(ctx, auditInfo)

	job := &domain.Job{
		ID:        "job-1",
		User:      "alice",
		State:     domain.JobStateRunning,
		StartTime: time.Now(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Audit:     auditInfo,
	}

	// Create first time - should succeed
	err := store.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("First CreateJob failed: %v", err)
	}

	// Create second time - should fail with conflict
	err = store.CreateJob(ctx, job)
	if !errors.Is(err, storage.ErrConflict) {
		t.Errorf("expected ErrConflict, got %v", err)
	}
}

// TestGetJob_NotFound validates not found error handling.
func TestGetJob_NotFound(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	_, err := store.GetJob(ctx, "nonexistent")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// TestUpdateJob_Success validates successful job updates.
func TestUpdateJob_Success(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	auditInfo := domain.NewAuditInfo("testuser", "test")
	ctx = domain.WithAuditInfo(ctx, auditInfo)

	// Create initial job
	job := &domain.Job{
		ID:        "job-1",
		User:      "alice",
		State:     domain.JobStateRunning,
		StartTime: time.Now(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		CPUUsage:  50.0,
		Audit:     auditInfo,
	}

	err := store.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	// Update job
	job.CPUUsage = 75.0
	job.MemoryUsageMB = 4096
	err = store.UpdateJob(ctx, job)
	if err != nil {
		t.Fatalf("UpdateJob failed: %v", err)
	}

	// Verify update
	retrieved, err := store.GetJob(ctx, "job-1")
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}

	if retrieved.CPUUsage != 75.0 {
		t.Errorf("expected CPUUsage=75.0, got %f", retrieved.CPUUsage)
	}
	if retrieved.MemoryUsageMB != 4096 {
		t.Errorf("expected MemoryUsageMB=4096, got %d", retrieved.MemoryUsageMB)
	}
}

// TestUpdateJob_TerminalState validates terminal state protection.
func TestUpdateJob_TerminalState(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	auditInfo := domain.NewAuditInfo("testuser", "test")
	ctx = domain.WithAuditInfo(ctx, auditInfo)

	// Create completed job
	job := &domain.Job{
		ID:        "job-1",
		User:      "alice",
		State:     domain.JobStateCompleted,
		StartTime: time.Now(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Audit:     auditInfo,
	}

	err := store.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	// Try to update terminal job - should fail
	job.CPUUsage = 100.0
	err = store.UpdateJob(ctx, job)
	if !errors.Is(err, storage.ErrJobTerminal) {
		t.Errorf("expected ErrJobTerminal, got %v", err)
	}
}

// TestUpsertJob_Insert validates upsert creates new job.
func TestUpsertJob_Insert(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	auditInfo := domain.NewAuditInfo("testuser", "test")
	ctx = domain.WithAuditInfo(ctx, auditInfo)

	job := &domain.Job{
		ID:        "job-1",
		User:      "alice",
		State:     domain.JobStateRunning,
		StartTime: time.Now(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Audit:     auditInfo,
	}

	err := store.UpsertJob(ctx, job)
	if err != nil {
		t.Fatalf("UpsertJob failed: %v", err)
	}

	// Verify job exists
	retrieved, err := store.GetJob(ctx, "job-1")
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}
	if retrieved.ID != job.ID {
		t.Errorf("expected ID=%s, got %s", job.ID, retrieved.ID)
	}
}

// TestUpsertJob_Update validates upsert updates existing job.
func TestUpsertJob_Update(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	auditInfo := domain.NewAuditInfo("testuser", "test")
	ctx = domain.WithAuditInfo(ctx, auditInfo)

	// Create initial job
	job := &domain.Job{
		ID:        "job-1",
		User:      "alice",
		State:     domain.JobStateRunning,
		StartTime: time.Now(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		CPUUsage:  50.0,
		Audit:     auditInfo,
	}

	err := store.UpsertJob(ctx, job)
	if err != nil {
		t.Fatalf("First UpsertJob failed: %v", err)
	}

	// Upsert with different values
	job.CPUUsage = 80.0
	err = store.UpsertJob(ctx, job)
	if err != nil {
		t.Fatalf("Second UpsertJob failed: %v", err)
	}

	// Verify update
	retrieved, err := store.GetJob(ctx, "job-1")
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}
	if retrieved.CPUUsage != 80.0 {
		t.Errorf("expected CPUUsage=80.0, got %f", retrieved.CPUUsage)
	}
}

// TestDeleteJob_Success validates job deletion.
func TestDeleteJob_Success(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	auditInfo := domain.NewAuditInfo("testuser", "test")
	ctx = domain.WithAuditInfo(ctx, auditInfo)

	// Create job
	job := &domain.Job{
		ID:        "job-1",
		User:      "alice",
		State:     domain.JobStateRunning,
		StartTime: time.Now(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Audit:     auditInfo,
	}

	err := store.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	// Delete job
	err = store.DeleteJob(ctx, "job-1")
	if err != nil {
		t.Fatalf("DeleteJob failed: %v", err)
	}

	// Verify deletion
	_, err = store.GetJob(ctx, "job-1")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound after deletion, got %v", err)
	}
}

// TestDeleteJob_NotFound validates delete of nonexistent job.
func TestDeleteJob_NotFound(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	err := store.DeleteJob(ctx, "nonexistent")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// TestListJobs_Filtering validates job filtering.
func TestListJobs_Filtering(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	auditInfo := domain.NewAuditInfo("testuser", "test")
	ctx = domain.WithAuditInfo(ctx, auditInfo)

	// Create multiple jobs
	jobs := []*domain.Job{
		{
			ID:        "job-1",
			User:      "alice",
			State:     domain.JobStateRunning,
			StartTime: time.Now(),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			Audit:     auditInfo,
		},
		{
			ID:        "job-2",
			User:      "bob",
			State:     domain.JobStateCompleted,
			StartTime: time.Now(),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			Audit:     auditInfo,
		},
		{
			ID:        "job-3",
			User:      "alice",
			State:     domain.JobStateCompleted,
			StartTime: time.Now(),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			Audit:     auditInfo,
		},
	}

	for _, job := range jobs {
		if err := store.CreateJob(ctx, job); err != nil {
			t.Fatalf("CreateJob failed: %v", err)
		}
	}

	// Filter by user
	user := "alice"
	filter := domain.JobFilter{User: &user}
	results, count, err := store.ListJobs(ctx, filter)
	if err != nil {
		t.Fatalf("ListJobs failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected count=2 for alice, got %d", count)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results for alice, got %d", len(results))
	}

	// Filter by state
	state := domain.JobStateCompleted
	filter = domain.JobFilter{State: &state}
	results, count, err = store.ListJobs(ctx, filter)
	if err != nil {
		t.Fatalf("ListJobs failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected count=2 for completed, got %d", count)
	}
}

// TestListJobs_Pagination validates pagination.
func TestListJobs_Pagination(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	auditInfo := domain.NewAuditInfo("testuser", "test")
	ctx = domain.WithAuditInfo(ctx, auditInfo)

	// Create 5 jobs
	for i := 1; i <= 5; i++ {
		job := &domain.Job{
			ID:        fmt.Sprintf("job-%d", i),
			User:      "alice",
			State:     domain.JobStateRunning,
			StartTime: time.Now(),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			Audit:     auditInfo,
		}
		if err := store.CreateJob(ctx, job); err != nil {
			t.Fatalf("CreateJob failed: %v", err)
		}
	}

	// Get first page
	limit := 2
	offset := 0
	filter := domain.JobFilter{Limit: limit, Offset: offset}
	results, count, err := store.ListJobs(ctx, filter)
	if err != nil {
		t.Fatalf("ListJobs failed: %v", err)
	}
	if count != 5 {
		t.Errorf("expected total count=5, got %d", count)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results in page, got %d", len(results))
	}

	// Get second page
	offset = 2
	filter = domain.JobFilter{Limit: limit, Offset: offset}
	results, count, err = store.ListJobs(ctx, filter)
	if err != nil {
		t.Fatalf("ListJobs failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results in second page, got %d", len(results))
	}
}

// TestGetAllJobs validates retrieving all jobs.
func TestGetAllJobs(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	auditInfo := domain.NewAuditInfo("testuser", "test")
	ctx = domain.WithAuditInfo(ctx, auditInfo)

	// Create 3 jobs
	for i := 1; i <= 3; i++ {
		job := &domain.Job{
			ID:        fmt.Sprintf("job-%d", i),
			User:      "alice",
			State:     domain.JobStateRunning,
			StartTime: time.Now(),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			Audit:     auditInfo,
		}
		if err := store.CreateJob(ctx, job); err != nil {
			t.Fatalf("CreateJob failed: %v", err)
		}
	}

	jobs, err := store.GetAllJobs(ctx)
	if err != nil {
		t.Fatalf("GetAllJobs failed: %v", err)
	}
	if len(jobs) != 3 {
		t.Errorf("expected 3 jobs, got %d", len(jobs))
	}
}

// TestRecordMetrics_Success validates metric recording.
func TestRecordMetrics_Success(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	auditInfo := domain.NewAuditInfo("testuser", "test")
	ctx = domain.WithAuditInfo(ctx, auditInfo)

	// Create job first
	job := &domain.Job{
		ID:        "job-1",
		User:      "alice",
		State:     domain.JobStateRunning,
		StartTime: time.Now(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Audit:     auditInfo,
	}
	if err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	// Record metric
	sample := &domain.MetricSample{
		JobID:         "job-1",
		Timestamp:     time.Now(),
		CPUUsage:      65.5,
		MemoryUsageMB: 2048,
	}

	err := store.RecordMetrics(ctx, sample)
	if err != nil {
		t.Fatalf("RecordMetrics failed: %v", err)
	}

	// Retrieve latest metric
	latest, err := store.GetLatestMetrics(ctx, "job-1")
	if err != nil {
		t.Fatalf("GetLatestMetrics failed: %v", err)
	}

	if latest.CPUUsage != 65.5 {
		t.Errorf("expected CPUUsage=65.5, got %f", latest.CPUUsage)
	}
	if latest.MemoryUsageMB != 2048 {
		t.Errorf("expected MemoryUsageMB=2048, got %d", latest.MemoryUsageMB)
	}
}

// TestRecordMetricsBatch validates batch metric insertion.
func TestRecordMetricsBatch(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	auditInfo := domain.NewAuditInfo("testuser", "test")
	ctx = domain.WithAuditInfo(ctx, auditInfo)

	// Create job
	job := &domain.Job{
		ID:        "job-1",
		User:      "alice",
		State:     domain.JobStateRunning,
		StartTime: time.Now(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Audit:     auditInfo,
	}
	if err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	// Record batch of metrics
	now := time.Now()
	samples := []*domain.MetricSample{
		{
			JobID:         "job-1",
			Timestamp:     now.Add(-2 * time.Minute),
			CPUUsage:      50.0,
			MemoryUsageMB: 1024,
		},
		{
			JobID:         "job-1",
			Timestamp:     now.Add(-1 * time.Minute),
			CPUUsage:      60.0,
			MemoryUsageMB: 1536,
		},
		{
			JobID:         "job-1",
			Timestamp:     now,
			CPUUsage:      70.0,
			MemoryUsageMB: 2048,
		},
	}

	err := store.RecordMetricsBatch(ctx, samples)
	if err != nil {
		t.Fatalf("RecordMetricsBatch failed: %v", err)
	}

	// Verify all metrics were recorded
	metrics, count, err := store.GetJobMetrics(ctx, "job-1", domain.MetricsFilter{})
	if err != nil {
		t.Fatalf("GetJobMetrics failed: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 metrics, got %d", count)
	}
	if len(metrics) != 3 {
		t.Errorf("expected 3 metric samples, got %d", len(metrics))
	}
}

// TestGetJobMetrics_TimeFilter validates time-based filtering.
func TestGetJobMetrics_TimeFilter(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	auditInfo := domain.NewAuditInfo("testuser", "test")
	ctx = domain.WithAuditInfo(ctx, auditInfo)

	// Create job
	job := &domain.Job{
		ID:        "job-1",
		User:      "alice",
		State:     domain.JobStateRunning,
		StartTime: time.Now(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Audit:     auditInfo,
	}
	if err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	// Record metrics at different times
	now := time.Now()
	samples := []*domain.MetricSample{
		{JobID: "job-1", Timestamp: now.Add(-3 * time.Hour), CPUUsage: 30.0, MemoryUsageMB: 512},
		{JobID: "job-1", Timestamp: now.Add(-2 * time.Hour), CPUUsage: 50.0, MemoryUsageMB: 1024},
		{JobID: "job-1", Timestamp: now.Add(-1 * time.Hour), CPUUsage: 70.0, MemoryUsageMB: 1536},
	}

	for _, sample := range samples {
		if err := store.RecordMetrics(ctx, sample); err != nil {
			t.Fatalf("RecordMetrics failed: %v", err)
		}
	}

	// Filter by time range
	startTime := now.Add(-2*time.Hour - 30*time.Minute)
	endTime := now.Add(-30 * time.Minute)
	filter := domain.MetricsFilter{
		StartTime: &startTime,
		EndTime:   &endTime,
	}

	metrics, count, err := store.GetJobMetrics(ctx, "job-1", filter)
	if err != nil {
		t.Fatalf("GetJobMetrics failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 metrics in time range, got %d", count)
	}
	if len(metrics) != 2 {
		t.Errorf("expected 2 metric samples, got %d", len(metrics))
	}
}

// TestDeleteMetricsBefore validates metric cleanup.
func TestDeleteMetricsBefore(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	auditInfo := domain.NewAuditInfo("testuser", "test")
	ctx = domain.WithAuditInfo(ctx, auditInfo)

	// Create job
	job := &domain.Job{
		ID:        "job-1",
		User:      "alice",
		State:     domain.JobStateRunning,
		StartTime: time.Now(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Audit:     auditInfo,
	}
	if err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	// Record old and new metrics
	now := time.Now()
	samples := []*domain.MetricSample{
		{JobID: "job-1", Timestamp: now.Add(-48 * time.Hour), CPUUsage: 30.0, MemoryUsageMB: 512},
		{JobID: "job-1", Timestamp: now.Add(-1 * time.Hour), CPUUsage: 70.0, MemoryUsageMB: 1536},
	}

	for _, sample := range samples {
		if err := store.RecordMetrics(ctx, sample); err != nil {
			t.Fatalf("RecordMetrics failed: %v", err)
		}
	}

	// Delete old metrics
	cutoff := now.Add(-24 * time.Hour)
	err := store.DeleteMetricsBefore(ctx, cutoff)
	if err != nil {
		t.Fatalf("DeleteMetricsBefore failed: %v", err)
	}

	// Verify only recent metric remains
	metrics, count, err := store.GetJobMetrics(ctx, "job-1", domain.MetricsFilter{})
	if err != nil {
		t.Fatalf("GetJobMetrics failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 metric after cleanup, got %d", count)
	}
	if len(metrics) != 1 {
		t.Errorf("expected 1 metric sample, got %d", len(metrics))
	}
}

// TestRecordAuditEvent validates audit event recording.
func TestRecordAuditEvent(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	event := &domain.JobAuditEvent{
		JobID:         "job-1",
		ChangedAt:     time.Now(),
		ChangeType:    "created",
		ChangedBy:     "testuser",
		Source:        "test",
		CorrelationID: "test-123",
		Snapshot: &domain.JobSnapshot{
			ID:        "job-1",
			User:      "testuser",
			State:     domain.JobStatePending,
			StartTime: time.Now(),
		},
	}

	err := store.RecordAuditEvent(ctx, event)
	if err != nil {
		t.Fatalf("RecordAuditEvent failed: %v", err)
	}
}

// TestWithTx_CommitOnSuccess validates transaction commit behavior.
func TestWithTx_CommitOnSuccess(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	auditInfo := domain.NewAuditInfo("testuser", "test")

	err := store.WithTx(ctx, func(tx storage.Tx) error {
		job := &domain.Job{
			ID:        "job-1",
			User:      "alice",
			State:     domain.JobStateRunning,
			StartTime: time.Now(),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			Audit:     auditInfo,
		}

		txCtx := domain.WithAuditInfo(ctx, auditInfo)
		return tx.Jobs().CreateJob(txCtx, job)
	})

	if err != nil {
		t.Fatalf("WithTx failed: %v", err)
	}

	// Verify job was committed
	_, err = store.GetJob(ctx, "job-1")
	if err != nil {
		t.Errorf("expected job to be committed, got error: %v", err)
	}
}

// TestWithTx_RollbackOnError validates transaction rollback behavior.
func TestWithTx_RollbackOnError(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	auditInfo := domain.NewAuditInfo("testuser", "test")

	testErr := errors.New("test error")
	err := store.WithTx(ctx, func(tx storage.Tx) error {
		job := &domain.Job{
			ID:        "job-1",
			User:      "alice",
			State:     domain.JobStateRunning,
			StartTime: time.Now(),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			Audit:     auditInfo,
		}

		txCtx := domain.WithAuditInfo(ctx, auditInfo)
		if err := tx.Jobs().CreateJob(txCtx, job); err != nil {
			return err
		}

		// Force rollback
		return testErr
	})

	if !errors.Is(err, testErr) {
		t.Fatalf("expected test error, got %v", err)
	}

	// Verify job was rolled back
	_, err = store.GetJob(ctx, "job-1")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected job to be rolled back, got error: %v", err)
	}
}

// TestConcurrentJobUpdates validates concurrent access patterns.
func TestConcurrentJobUpdates(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	auditInfo := domain.NewAuditInfo("testuser", "test")
	ctx = domain.WithAuditInfo(ctx, auditInfo)

	// Create initial job
	job := &domain.Job{
		ID:        "job-1",
		User:      "alice",
		State:     domain.JobStateRunning,
		StartTime: time.Now(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		CPUUsage:  0.0,
		Audit:     auditInfo,
	}

	if err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	// Perform concurrent updates
	var wg sync.WaitGroup
	concurrency := 10

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(val float64) {
			defer wg.Done()

			updateJob := &domain.Job{
				ID:        "job-1",
				User:      "alice",
				State:     domain.JobStateRunning,
				StartTime: job.StartTime,
				CreatedAt: job.CreatedAt,
				UpdatedAt: time.Now(),
				CPUUsage:  val,
				Audit:     auditInfo,
			}

			_ = store.UpdateJob(ctx, updateJob)
		}(float64(i))
	}

	wg.Wait()

	// Verify job still exists and is consistent
	retrieved, err := store.GetJob(ctx, "job-1")
	if err != nil {
		t.Fatalf("GetJob failed after concurrent updates: %v", err)
	}
	if retrieved.ID != "job-1" {
		t.Errorf("job data corrupted after concurrent updates")
	}
}

// Unit tests for helpers (no database required)

func TestMapPgError_Nil(t *testing.T) {
	err := mapPgError(nil)
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestPrepareJobArgs(t *testing.T) {
	job := &domain.Job{
		ID:            "test-1",
		User:          "testuser",
		Nodes:         []string{"node1", "node2"},
		State:         domain.JobStateRunning,
		StartTime:     time.Now(),
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
		CPUUsage:      50.0,
		MemoryUsageMB: 1024,
	}

	args := prepareJobArgs(job)
	if len(args) != 44 {
		t.Errorf("expected 44 args, got %d", len(args))
	}
	if args[0] != "test-1" {
		t.Errorf("expected first arg to be job ID")
	}
}

func TestGetAuditInfo_FromJob(t *testing.T) {
	ctx := context.Background()
	job := &domain.Job{
		Audit: &domain.AuditInfo{
			ChangedBy:     "user1",
			Source:        "api",
			CorrelationID: "req-123",
		},
	}

	audit, err := getAuditInfo(ctx, job)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if audit.ChangedBy != "user1" {
		t.Errorf("expected ChangedBy=user1, got %s", audit.ChangedBy)
	}
}

func TestGetAuditInfo_Missing(t *testing.T) {
	ctx := context.Background()
	job := &domain.Job{}

	_, err := getAuditInfo(ctx, job)
	if err != storage.ErrInvalidInput {
		t.Errorf("expected ErrInvalidInput, got %v", err)
	}
}

func TestMarshalJobSnapshot(t *testing.T) {
	job := &domain.Job{
		ID:            "test-1",
		User:          "testuser",
		State:         domain.JobStateRunning,
		StartTime:     time.Now(),
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
		CPUUsage:      50.0,
		MemoryUsageMB: 1024,
	}

	data, err := marshalJobSnapshot(job)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(data) == 0 {
		t.Error("expected non-empty JSON")
	}
}
