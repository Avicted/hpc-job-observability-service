package service

import (
	"context"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/mapper"
	"github.com/Avicted/hpc-job-observability-service/internal/metrics"
	"github.com/Avicted/hpc-job-observability-service/internal/storage"
)

// MetricsService handles metrics-related business logic including recording
// metrics samples and updating Prometheus gauges.
type MetricsService struct {
	store    storage.Storage
	exporter *metrics.Exporter
	mapper   *mapper.Mapper
}

// NewMetricsService creates a new MetricsService with the given dependencies.
func NewMetricsService(store storage.Storage, exporter *metrics.Exporter, mapper *mapper.Mapper) *MetricsService {
	return &MetricsService{
		store:    store,
		exporter: exporter,
		mapper:   mapper,
	}
}

// GetMetricsInput contains the filter parameters for getting job metrics.
type GetMetricsInput struct {
	JobID     string
	StartTime *time.Time
	EndTime   *time.Time
	Limit     *int
}

// GetMetricsOutput contains the result of getting job metrics.
type GetMetricsOutput struct {
	Samples []*storage.MetricSample
	Total   int
}

// GetJobMetrics retrieves metrics samples for a job.
// Returns ErrJobNotFound if the job does not exist.
//
// Parameters:
//   - ctx: Context for cancellation and deadline propagation
//   - input: Filter parameters for the metrics query
func (s *MetricsService) GetJobMetrics(ctx context.Context, input GetMetricsInput) (*GetMetricsOutput, error) {
	filter := storage.MetricsFilter{
		Limit: 1000,
	}

	if input.StartTime != nil {
		filter.StartTime = input.StartTime
	}
	if input.EndTime != nil {
		filter.EndTime = input.EndTime
	}
	if input.Limit != nil {
		filter.Limit = *input.Limit
	}

	samples, total, err := s.store.GetJobMetrics(ctx, input.JobID, filter)
	if err != nil {
		if err == storage.ErrJobNotFound {
			return nil, ErrJobNotFound
		}
		return nil, ErrInternalError
	}

	return &GetMetricsOutput{
		Samples: samples,
		Total:   total,
	}, nil
}

// RecordMetricsInput contains the input data for recording job metrics.
type RecordMetricsInput struct {
	JobID         string
	CPUUsage      float64
	MemoryUsageMB int
	GPUUsage      *float64
	Audit         *AuditContext
}

// RecordMetricsOutput contains the result of recording job metrics.
type RecordMetricsOutput struct {
	Sample *storage.MetricSample
}

// RecordJobMetrics records a metrics sample for a job and updates the job's
// current resource usage.
//
// Validation:
//   - CPU usage must be between 0 and 100
//   - Memory usage must be non-negative
//
// Side effects:
//   - Records a new metrics sample
//   - Updates job's current resource usage
//   - Updates Prometheus job metrics
//
// Parameters:
//   - ctx: Context for cancellation and deadline propagation
//   - input: Metrics data to record
func (s *MetricsService) RecordJobMetrics(ctx context.Context, input RecordMetricsInput) (*RecordMetricsOutput, error) {
	// Check if job exists
	job, err := s.store.GetJob(ctx, input.JobID)
	if err != nil {
		if err == storage.ErrJobNotFound {
			return nil, ErrJobNotFound
		}
		return nil, ErrInternalError
	}

	// Validate metrics
	if input.CPUUsage < 0 || input.CPUUsage > 100 {
		return nil, &ValidationError{Message: "CPU usage must be between 0 and 100"}
	}
	if input.MemoryUsageMB < 0 {
		return nil, &ValidationError{Message: "Memory usage must be non-negative"}
	}

	sample := &storage.MetricSample{
		JobID:         input.JobID,
		Timestamp:     time.Now(),
		CPUUsage:      input.CPUUsage,
		MemoryUsageMB: int64(input.MemoryUsageMB),
		GPUUsage:      input.GPUUsage,
	}

	if err := s.store.RecordMetrics(ctx, sample); err != nil {
		return nil, ErrInternalError
	}

	// Update job's current resource usage
	job.CPUUsage = input.CPUUsage
	job.MemoryUsageMB = int64(input.MemoryUsageMB)
	job.GPUUsage = input.GPUUsage
	job.Audit = input.Audit.ToStorageAuditInfo()

	if err := s.store.UpdateJob(ctx, job); err != nil {
		return nil, ErrInternalError
	}

	// Update Prometheus metrics
	s.exporter.UpdateJobMetrics(job)

	return &RecordMetricsOutput{Sample: sample}, nil
}

// Mapper returns the mapper used by the service for handler convenience.
func (s *MetricsService) Mapper() *mapper.Mapper {
	return s.mapper
}
