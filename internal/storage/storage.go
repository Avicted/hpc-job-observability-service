// Package storage provides database storage for jobs and metrics.
// It supports PostgreSQL as the primary backend.
//
// This package uses domain types directly - no separate storage types.
// All persistence operations work with domain.Job, domain.MetricSample, etc.
package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/domain"
	"github.com/google/uuid"
)

// Storage defines the interface for job and metrics persistence.
// All methods work directly with domain types.
type Storage interface {
	// Job operations
	CreateJob(ctx context.Context, job *domain.Job) error
	GetJob(ctx context.Context, id string) (*domain.Job, error)
	UpdateJob(ctx context.Context, job *domain.Job) error
	UpsertJob(ctx context.Context, job *domain.Job) error
	DeleteJob(ctx context.Context, id string) error
	ListJobs(ctx context.Context, filter domain.JobFilter) ([]*domain.Job, int, error)
	GetAllJobs(ctx context.Context) ([]*domain.Job, error)

	// Metrics operations
	RecordMetrics(ctx context.Context, sample *domain.MetricSample) error
	GetJobMetrics(ctx context.Context, jobID string, filter domain.MetricsFilter) ([]*domain.MetricSample, int, error)
	GetLatestMetrics(ctx context.Context, jobID string) (*domain.MetricSample, error)
	DeleteMetricsBefore(cutoff time.Time) error

	// Lifecycle operations
	Migrate() error
	Close() error
	SeedDemoData() error
}

// auditInfoContextKey is the context key for audit info.
type auditInfoContextKey struct{}

// WithAuditInfo attaches audit info to a context.
func WithAuditInfo(ctx context.Context, info *domain.AuditInfo) context.Context {
	return context.WithValue(ctx, auditInfoContextKey{}, info)
}

// GetAuditInfo retrieves audit info from a context.
func GetAuditInfo(ctx context.Context) (*domain.AuditInfo, bool) {
	info, ok := ctx.Value(auditInfoContextKey{}).(*domain.AuditInfo)
	return info, ok
}

// NewAuditInfo creates audit info with a new correlation ID.
func NewAuditInfo(changedBy, source string) *domain.AuditInfo {
	return &domain.AuditInfo{
		ChangedBy:     strings.TrimSpace(changedBy),
		Source:        strings.TrimSpace(source),
		CorrelationID: uuid.NewString(),
	}
}

// NewAuditInfoWithCorrelation creates audit info with an explicit correlation ID.
func NewAuditInfoWithCorrelation(changedBy, source, correlationID string) *domain.AuditInfo {
	return &domain.AuditInfo{
		ChangedBy:     strings.TrimSpace(changedBy),
		Source:        strings.TrimSpace(source),
		CorrelationID: strings.TrimSpace(correlationID),
	}
}

func validateAuditInfo(info *domain.AuditInfo) error {
	if info == nil {
		return domain.ErrMissingAuditInfo
	}
	if strings.TrimSpace(info.ChangedBy) == "" || strings.TrimSpace(info.Source) == "" || strings.TrimSpace(info.CorrelationID) == "" {
		return domain.ErrMissingAuditInfo
	}
	return nil
}

func auditInfoFromJobOrContext(ctx context.Context, job *domain.Job) (*domain.AuditInfo, error) {
	if job != nil && job.Audit != nil {
		if err := validateAuditInfo(job.Audit); err != nil {
			return nil, err
		}
		return job.Audit, nil
	}
	if info, ok := GetAuditInfo(ctx); ok {
		if err := validateAuditInfo(info); err != nil {
			return nil, err
		}
		if job != nil {
			job.Audit = info
		}
		return info, nil
	}
	return nil, domain.ErrMissingAuditInfo
}

func buildJobSnapshot(job *domain.Job) *domain.JobSnapshot {
	if job == nil {
		return nil
	}
	return &domain.JobSnapshot{
		ID:             job.ID,
		User:           job.User,
		Nodes:          append([]string(nil), job.Nodes...),
		NodeCount:      job.NodeCount,
		State:          job.State,
		StartTime:      job.StartTime,
		EndTime:        job.EndTime,
		RuntimeSeconds: job.RuntimeSeconds,
		CPUUsage:       job.CPUUsage,
		MemoryUsageMB:  job.MemoryUsageMB,
		GPUUsage:       job.GPUUsage,
		RequestedCPUs:  job.RequestedCPUs,
		AllocatedCPUs:  job.AllocatedCPUs,
		RequestedMemMB: job.RequestedMemMB,
		AllocatedMemMB: job.AllocatedMemMB,
		RequestedGPUs:  job.RequestedGPUs,
		AllocatedGPUs:  job.AllocatedGPUs,
		ClusterName:    job.ClusterName,
		SchedulerInst:  job.SchedulerInst,
		IngestVersion:  job.IngestVersion,
		LastSampleAt:   job.LastSampleAt,
		SampleCount:    job.SampleCount,
		AvgCPUUsage:    job.AvgCPUUsage,
		MaxCPUUsage:    job.MaxCPUUsage,
		MaxMemUsageMB:  job.MaxMemUsageMB,
		AvgGPUUsage:    job.AvgGPUUsage,
		MaxGPUUsage:    job.MaxGPUUsage,
		CgroupPath:     job.CgroupPath,
		GPUCount:       job.GPUCount,
		GPUVendor:      job.GPUVendor,
		GPUDevices:     append([]string(nil), job.GPUDevices...),
		Scheduler:      job.Scheduler,
		CreatedAt:      job.CreatedAt,
		UpdatedAt:      job.UpdatedAt,
	}
}

func marshalJobSnapshot(job *domain.Job) ([]byte, error) {
	snapshot := buildJobSnapshot(job)
	if snapshot == nil {
		return nil, fmt.Errorf("job snapshot is nil")
	}
	return json.Marshal(snapshot)
}

// New creates a new storage instance based on the database type.
func New(dbType, dsn string) (Storage, error) {
	switch strings.ToLower(strings.TrimSpace(dbType)) {
	case "", "postgres", "postgresql":
		return NewPostgresStorage(dsn)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbType)
	}
}

// baseStorage contains common SQL operations shared between storage backends.
type baseStorage struct {
	db *sql.DB
}

func (s *baseStorage) Close() error {
	return s.db.Close()
}

// SeedDemoData creates sample jobs and metrics for demonstration purposes.
// Uses proper audit info for all job creation to maintain audit trail consistency.
func (s *baseStorage) SeedDemoData(createJob func(ctx context.Context, job *domain.Job) error) error {
	now := time.Now()

	// Create a system audit context for demo data seeding
	auditInfo := NewAuditInfo("system", "seed-demo")
	ctx := WithAuditInfo(context.Background(), auditInfo)

	// User names for demo jobs
	users := []string{"Alice", "Bob", "Liam", "Sofia", "Noah", "Olivia", "Ethan", "Mia", "Lucas", "Ava",
		"Leo", "Isabella", "Jack", "Amelia", "Oliver", "Charlotte", "Henry", "Ella", "Benjamin", "Grace",
		"Daniel", "Lily", "Samuel", "Hannah", "Matthew", "Nora", "James", "Zoe", "William", "Chloe",
		"Michael", "Aria", "David", "Ruby", "Joseph", "Lucy", "Andrew", "Violet", "Thomas", "Emily",
		"Christopher", "Freya", "Joshua", "Clara", "Nathan", "Iris", "Ryan", "Stella", "Jonathan", "Eliza",
		"Aaron", "Naomi", "Caleb", "Maya", "Isaac", "Eva", "Sebastian", "Rose", "Nicholas", "Aurora",
		"Julian", "Hazel", "Owen", "Willow", "Dylan", "Penelope", "Carter", "Margot", "Eli", "June",
		"Finn", "Scarlett", "Adrian", "Thea", "Miles", "Florence", "Simon", "Beatrice", "Victor", "Helena",
		"Max", "Astrid", "Patrick", "Ingrid", "Rowan", "Sienna", "Theo", "Elin", "Hugo", "Linnea",
		"Marcus", "Josephine", "Paul", "Matilda", "Oscar", "Agnes", "Tobias", "Edith", "Emma", "Diana"}

	partitions := []string{"gpu", "compute", "batch", "debug"}
	states := []domain.JobState{domain.JobStateRunning, domain.JobStateCompleted, domain.JobStateFailed, domain.JobStateCancelled, domain.JobStatePending}

	jobCount := 100
	demoJobs := make([]*domain.Job, 0, jobCount)

	for i := 1; i <= jobCount; i++ {
		user := users[i%len(users)]
		partition := partitions[i%len(partitions)]
		state := states[i%len(states)]

		jobID := fmt.Sprintf("demo-job-%03d", i)

		nodeCount := 1 + (i % 32)
		nodes := make([]string, 0, nodeCount)
		for n := 0; n < nodeCount; n++ {
			nodes = append(nodes, fmt.Sprintf("node-%02d", (i+n)%50+1))
		}

		submitTime := now.Add(-time.Duration(10+i) * time.Minute)
		startTime := now.Add(-time.Duration(5*i) * time.Minute)
		if startTime.Before(submitTime) {
			startTime = submitTime.Add(5 * time.Minute)
		}

		job := &domain.Job{
			ID:            jobID,
			User:          user,
			Nodes:         nodes,
			State:         state,
			StartTime:     startTime,
			CPUUsage:      25 + float64(i%75),
			MemoryUsageMB: int64(1024 * (1 + i%16)),
			Audit:         auditInfo,
		}

		// Add GPU usage for GPU partition jobs
		if partition == "gpu" {
			job.GPUUsage = floatPtr(30 + float64(i%70))
		}

		// Set end time and runtime for completed/failed/cancelled jobs
		switch state {
		case domain.JobStateCompleted:
			end := startTime.Add(time.Duration(30+i) * time.Minute)
			job.EndTime = &end
			job.RuntimeSeconds = end.Sub(startTime).Seconds()
		case domain.JobStateFailed:
			end := startTime.Add(time.Duration(10+i) * time.Minute)
			job.EndTime = &end
			job.RuntimeSeconds = end.Sub(startTime).Seconds()
		case domain.JobStateCancelled:
			end := startTime.Add(time.Duration(5+i) * time.Minute)
			job.EndTime = &end
			job.RuntimeSeconds = end.Sub(startTime).Seconds()
		}

		demoJobs = append(demoJobs, job)
	}

	for _, job := range demoJobs {
		job.CreatedAt = job.StartTime
		job.UpdatedAt = now
		if err := createJob(ctx, job); err != nil && err != domain.ErrJobAlreadyExists {
			return fmt.Errorf("failed to seed job %s: %w", job.ID, err)
		}
	}

	// Generate historical metrics for running and completed jobs
	for _, job := range demoJobs {
		if job.State == domain.JobStateRunning || job.State == domain.JobStateCompleted {
			if err := s.seedJobMetrics(ctx, job, now); err != nil {
				return fmt.Errorf("failed to seed metrics for job %s: %w", job.ID, err)
			}
		}
	}

	return nil
}

func (s *baseStorage) seedJobMetrics(ctx context.Context, job *domain.Job, now time.Time) error {
	// Generate metrics samples every minute for the job's duration
	endTime := now
	if job.EndTime != nil {
		endTime = *job.EndTime
	}

	// Limit to last 2 hours of samples for demo
	startTime := job.StartTime
	if endTime.Sub(startTime) > 2*time.Hour {
		startTime = endTime.Add(-2 * time.Hour)
	}

	for t := startTime; t.Before(endTime); t = t.Add(1 * time.Minute) {
		// Simulate varying resource usage
		cpuVariation := (float64(t.Unix()%10) - 5) * 2
		memVariation := int64((t.Unix() % 5) * 100)

		sample := &domain.MetricSample{
			JobID:         job.ID,
			Timestamp:     t,
			CPUUsage:      clamp(job.CPUUsage+cpuVariation, 0, 100),
			MemoryUsageMB: max(0, job.MemoryUsageMB+memVariation),
		}
		if job.GPUUsage != nil {
			gpuVariation := (float64(t.Unix()%8) - 4) * 3
			gpuUsage := clamp(*job.GPUUsage+gpuVariation, 0, 100)
			sample.GPUUsage = &gpuUsage
		}

		_, err := s.db.ExecContext(ctx, `
			INSERT INTO metric_samples (job_id, timestamp, cpu_usage, memory_usage_mb, gpu_usage)
			VALUES ($1, $2, $3, $4, $5)
		`, sample.JobID, sample.Timestamp, sample.CPUUsage, sample.MemoryUsageMB, sample.GPUUsage)
		if err != nil {
			return err
		}
	}

	return nil
}

func floatPtr(f float64) *float64 {
	return &f
}

func clamp(val, minVal, maxVal float64) float64 {
	if val < minVal {
		return minVal
	}
	if val > maxVal {
		return maxVal
	}
	return val
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
