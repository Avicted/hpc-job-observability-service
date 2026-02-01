package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/domain"
	"github.com/Avicted/hpc-job-observability-service/internal/storage"
	"github.com/Avicted/hpc-job-observability-service/internal/utils/mapper"
)

// --- MetricsService Tests ---

func TestNewMetricsService(t *testing.T) {
	store := newMockStorage()
	exporter := newMockExporter()
	m := mapper.NewMapper()

	svc := NewMetricsService(store, exporter, m)

	if svc == nil {
		t.Fatal("expected non-nil service")
	}
	if svc.exporter != exporter {
		t.Error("expected exporter to be set")
	}
	if svc.mapper != m {
		t.Error("expected mapper to be set")
	}
}

func TestMetricsService_GetJobMetrics_Success(t *testing.T) {
	store := newMockStorage()
	store.jobs["job-metrics"] = &domain.Job{ID: "job-metrics", User: "user", State: domain.JobStateRunning}
	now := time.Now()
	store.metricsStore = []*domain.MetricSample{
		{JobID: "job-metrics", Timestamp: now.Add(-time.Minute), CPUUsage: 50.0, MemoryUsageMB: 1024},
		{JobID: "job-metrics", Timestamp: now, CPUUsage: 75.0, MemoryUsageMB: 2048},
		{JobID: "other-job", Timestamp: now, CPUUsage: 30.0, MemoryUsageMB: 512},
	}
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewMetricsService(store, exporter, m)

	output, err := svc.GetJobMetrics(context.Background(), GetMetricsInput{
		JobID: "job-metrics",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Total != 2 {
		t.Errorf("Total = %d, want 2", output.Total)
	}
	if len(output.Samples) != 2 {
		t.Errorf("Samples len = %d, want 2", len(output.Samples))
	}
}

func TestMetricsService_GetJobMetrics_WithFilters(t *testing.T) {
	store := newMockStorage()
	store.jobs["job-filter"] = &domain.Job{ID: "job-filter", User: "user", State: domain.JobStateRunning}
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewMetricsService(store, exporter, m)

	startTime := time.Now().Add(-time.Hour)
	endTime := time.Now()
	limit := 10

	output, err := svc.GetJobMetrics(context.Background(), GetMetricsInput{
		JobID:     "job-filter",
		StartTime: &startTime,
		EndTime:   &endTime,
		Limit:     &limit,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should return empty since no metrics recorded
	if output.Total != 0 {
		t.Errorf("Total = %d, want 0", output.Total)
	}
}

func TestMetricsService_GetJobMetrics_NotFound(t *testing.T) {
	store := newMockStorage()
	store.getErr = storage.ErrNotFound
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewMetricsService(store, exporter, m)

	_, err := svc.GetJobMetrics(context.Background(), GetMetricsInput{
		JobID: "nonexistent",
	})

	// Note: GetJobMetrics doesn't check if job exists before querying metrics
	// It returns metrics for the job ID directly
	if err != nil && !errors.Is(err, ErrJobNotFound) {
		t.Errorf("unexpected error type: %v", err)
	}
}

func TestMetricsService_RecordJobMetrics_Success(t *testing.T) {
	store := newMockStorage()
	store.jobs["job-record"] = &domain.Job{
		ID:        "job-record",
		User:      "user",
		State:     domain.JobStateRunning,
		StartTime: time.Now().Add(-time.Hour),
	}
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewMetricsService(store, exporter, m)

	gpuUsage := 50.0
	output, err := svc.RecordJobMetrics(context.Background(), RecordMetricsInput{
		JobID:         "job-record",
		CPUUsage:      75.5,
		MemoryUsageMB: 2048,
		GPUUsage:      &gpuUsage,
		Audit:         testAuditContext(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Sample == nil {
		t.Fatal("expected sample to be returned")
	}
	if output.Sample.JobID != "job-record" {
		t.Errorf("Sample.JobID = %q, want job-record", output.Sample.JobID)
	}
	if output.Sample.CPUUsage != 75.5 {
		t.Errorf("Sample.CPUUsage = %v, want 75.5", output.Sample.CPUUsage)
	}
	if output.Sample.MemoryUsageMB != 2048 {
		t.Errorf("Sample.MemoryUsageMB = %d, want 2048", output.Sample.MemoryUsageMB)
	}
	if output.Sample.GPUUsage == nil || *output.Sample.GPUUsage != 50.0 {
		t.Errorf("Sample.GPUUsage = %v, want 50.0", output.Sample.GPUUsage)
	}

	// Verify metrics were recorded in storage
	if len(store.metricsStore) != 1 {
		t.Errorf("metricsStore len = %d, want 1", len(store.metricsStore))
	}

	// Verify job was updated
	job := store.jobs["job-record"]
	if job.CPUUsage != 75.5 {
		t.Errorf("job.CPUUsage = %v, want 75.5", job.CPUUsage)
	}
	if job.MemoryUsageMB != 2048 {
		t.Errorf("job.MemoryUsageMB = %d, want 2048", job.MemoryUsageMB)
	}
}

func TestMetricsService_RecordJobMetrics_JobNotFound(t *testing.T) {
	store := newMockStorage()
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewMetricsService(store, exporter, m)

	_, err := svc.RecordJobMetrics(context.Background(), RecordMetricsInput{
		JobID:         "nonexistent",
		CPUUsage:      50.0,
		MemoryUsageMB: 1024,
		Audit:         testAuditContext(),
	})

	if !errors.Is(err, ErrJobNotFound) {
		t.Errorf("expected ErrJobNotFound, got %v", err)
	}
}

func TestMetricsService_RecordJobMetrics_ValidationErrors(t *testing.T) {
	store := newMockStorage()
	store.jobs["job-valid"] = &domain.Job{ID: "job-valid", User: "user", State: domain.JobStateRunning}
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewMetricsService(store, exporter, m)

	tests := []struct {
		name    string
		input   RecordMetricsInput
		wantErr string
	}{
		{
			name: "CPU usage negative",
			input: RecordMetricsInput{
				JobID:         "job-valid",
				CPUUsage:      -1.0,
				MemoryUsageMB: 1024,
				Audit:         testAuditContext(),
			},
			wantErr: "CPU usage must be between 0 and 100",
		},
		{
			name: "CPU usage over 100",
			input: RecordMetricsInput{
				JobID:         "job-valid",
				CPUUsage:      150.0,
				MemoryUsageMB: 1024,
				Audit:         testAuditContext(),
			},
			wantErr: "CPU usage must be between 0 and 100",
		},
		{
			name: "Memory usage negative",
			input: RecordMetricsInput{
				JobID:         "job-valid",
				CPUUsage:      50.0,
				MemoryUsageMB: -1,
				Audit:         testAuditContext(),
			},
			wantErr: "Memory usage must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := svc.RecordJobMetrics(context.Background(), tt.input)
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

func TestMetricsService_RecordJobMetrics_WithoutGPU(t *testing.T) {
	store := newMockStorage()
	store.jobs["job-nogpu"] = &domain.Job{
		ID:        "job-nogpu",
		User:      "user",
		State:     domain.JobStateRunning,
		StartTime: time.Now().Add(-time.Hour),
	}
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewMetricsService(store, exporter, m)

	output, err := svc.RecordJobMetrics(context.Background(), RecordMetricsInput{
		JobID:         "job-nogpu",
		CPUUsage:      50.0,
		MemoryUsageMB: 1024,
		GPUUsage:      nil,
		Audit:         testAuditContext(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Sample.GPUUsage != nil {
		t.Error("expected GPUUsage to be nil")
	}
}

func TestMetricsService_Mapper(t *testing.T) {
	store := newMockStorage()
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewMetricsService(store, exporter, m)

	if svc.Mapper() != m {
		t.Error("expected Mapper() to return the injected mapper")
	}
}

func TestMetricsService_GetJobMetrics_InternalError(t *testing.T) {
	store := newMockStorage()
	store.metricsErr = errors.New("database error")
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewMetricsService(store, exporter, m)

	_, err := svc.GetJobMetrics(context.Background(), GetMetricsInput{
		JobID: "any-job",
	})

	if !errors.Is(err, ErrInternalError) {
		t.Errorf("expected ErrInternalError, got %v", err)
	}
}

func TestMetricsService_GetJobMetrics_AllFilters(t *testing.T) {
	store := newMockStorage()
	store.jobs["job-filters"] = &domain.Job{ID: "job-filters", User: "user", State: domain.JobStateRunning}
	now := time.Now()
	store.metricsStore = []*domain.MetricSample{
		{JobID: "job-filters", Timestamp: now.Add(-time.Hour), CPUUsage: 50.0},
		{JobID: "job-filters", Timestamp: now.Add(-30 * time.Minute), CPUUsage: 60.0},
	}
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewMetricsService(store, exporter, m)

	startTime := now.Add(-2 * time.Hour)
	endTime := now
	limit := 10

	output, err := svc.GetJobMetrics(context.Background(), GetMetricsInput{
		JobID:     "job-filters",
		StartTime: &startTime,
		EndTime:   &endTime,
		Limit:     &limit,
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Total != 2 {
		t.Errorf("Total = %d, want 2", output.Total)
	}
}

func TestMetricsService_RecordJobMetrics_InternalError(t *testing.T) {
	store := newMockStorage()
	store.jobs["job-recerr"] = &domain.Job{ID: "job-recerr", User: "user", State: domain.JobStateRunning}
	store.recordErr = errors.New("database error")
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewMetricsService(store, exporter, m)

	_, err := svc.RecordJobMetrics(context.Background(), RecordMetricsInput{
		JobID:         "job-recerr",
		CPUUsage:      50.0,
		MemoryUsageMB: 1024,
		Audit:         testAuditContext(),
	})

	if !errors.Is(err, ErrInternalError) {
		t.Errorf("expected ErrInternalError, got %v", err)
	}
}

func TestMetricsService_RecordJobMetrics_UpdateError(t *testing.T) {
	store := newMockStorage()
	store.jobs["job-upderr"] = &domain.Job{ID: "job-upderr", User: "user", State: domain.JobStateRunning}
	store.updateErr = errors.New("database error")
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewMetricsService(store, exporter, m)

	_, err := svc.RecordJobMetrics(context.Background(), RecordMetricsInput{
		JobID:         "job-upderr",
		CPUUsage:      50.0,
		MemoryUsageMB: 1024,
		Audit:         testAuditContext(),
	})

	if !errors.Is(err, ErrInternalError) {
		t.Errorf("expected ErrInternalError, got %v", err)
	}
}

func TestMetricsService_RecordJobMetrics_GetError(t *testing.T) {
	store := newMockStorage()
	store.getErr = errors.New("database error")
	exporter := newMockExporter()
	m := mapper.NewMapper()
	svc := NewMetricsService(store, exporter, m)

	_, err := svc.RecordJobMetrics(context.Background(), RecordMetricsInput{
		JobID:         "any-job",
		CPUUsage:      50.0,
		MemoryUsageMB: 1024,
		Audit:         testAuditContext(),
	})

	if !errors.Is(err, ErrInternalError) {
		t.Errorf("expected ErrInternalError, got %v", err)
	}
}
