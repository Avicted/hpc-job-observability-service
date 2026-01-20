// Package scheduler provides an abstraction layer for job scheduling systems.
package scheduler

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// MockJobSource provides a simulated job source for testing and development.
// It generates fake jobs with realistic-looking resource usage patterns.
type MockJobSource struct {
	mu          sync.RWMutex
	jobs        map[string]*Job
	metrics     map[string][]*MetricSample
	stateMapper *StateMapping
}

// NewMockJobSource creates a new mock job source.
func NewMockJobSource() *MockJobSource {
	return &MockJobSource{
		jobs:        make(map[string]*Job),
		metrics:     make(map[string][]*MetricSample),
		stateMapper: NewSlurmStateMapping(),
	}
}

// Type returns the scheduler type.
func (m *MockJobSource) Type() SchedulerType {
	return SchedulerTypeMock
}

// ListJobs returns jobs matching the filter.
func (m *MockJobSource) ListJobs(ctx context.Context, filter JobFilter) ([]*Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*Job
	for _, job := range m.jobs {
		if m.matchesFilter(job, filter) {
			result = append(result, job)
		}
	}

	// Apply pagination
	start := filter.Offset
	if start > len(result) {
		return []*Job{}, nil
	}

	end := start + filter.Limit
	if end > len(result) || filter.Limit == 0 {
		end = len(result)
	}

	return result[start:end], nil
}

// GetJob returns a specific job by ID.
func (m *MockJobSource) GetJob(ctx context.Context, id string) (*Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if job, ok := m.jobs[id]; ok {
		return job, nil
	}
	return nil, nil
}

// GetJobMetrics returns metrics for a job.
func (m *MockJobSource) GetJobMetrics(ctx context.Context, jobID string) ([]*MetricSample, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.metrics[jobID], nil
}

// SupportsMetrics returns true as mock source supports metrics.
func (m *MockJobSource) SupportsMetrics() bool {
	return true
}

// AddJob adds a job to the mock source.
func (m *MockJobSource) AddJob(job *Job) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if job.Scheduler == nil {
		job.Scheduler = &SchedulerInfo{
			Type:          SchedulerTypeMock,
			ExternalJobID: job.ID,
			RawState:      string(job.State),
		}
	}

	m.jobs[job.ID] = job
}

// AddMetrics adds metrics samples for a job.
func (m *MockJobSource) AddMetrics(jobID string, samples []*MetricSample) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.metrics[jobID] = append(m.metrics[jobID], samples...)
}

// GenerateDemoJobs creates a set of demo jobs with realistic patterns.
func (m *MockJobSource) GenerateDemoJobs() {
	now := time.Now()

	demoJobs := []*Job{
		{
			ID:            "mock-job-001",
			User:          "alice",
			Nodes:         []string{"node-01", "node-02"},
			State:         JobStateRunning,
			StartTime:     now.Add(-2 * time.Hour),
			CPUUsage:      78.5,
			MemoryUsageMB: 8192,
			GPUUsage:      floatPtr(45.0),
			Scheduler: &SchedulerInfo{
				Type:          SchedulerTypeMock,
				ExternalJobID: "mock-job-001",
				RawState:      "RUNNING",
				SubmitTime:    timePtr(now.Add(-3 * time.Hour)),
				Partition:     "gpu",
				Account:       "project_a",
				Priority:      intPtr(100),
			},
		},
		{
			ID:            "mock-job-002",
			User:          "bob",
			Nodes:         []string{"node-03"},
			State:         JobStateRunning,
			StartTime:     now.Add(-30 * time.Minute),
			CPUUsage:      92.3,
			MemoryUsageMB: 16384,
			GPUUsage:      floatPtr(88.0),
			Scheduler: &SchedulerInfo{
				Type:          SchedulerTypeMock,
				ExternalJobID: "mock-job-002",
				RawState:      "RUNNING",
				SubmitTime:    timePtr(now.Add(-45 * time.Minute)),
				Partition:     "compute",
				Account:       "project_b",
				Priority:      intPtr(50),
			},
		},
		{
			ID:             "mock-job-003",
			User:           "charlie",
			Nodes:          []string{"node-04", "node-05", "node-06"},
			State:          JobStateCompleted,
			StartTime:      now.Add(-5 * time.Hour),
			EndTime:        timePtr(now.Add(-1 * time.Hour)),
			RuntimeSeconds: 14400,
			Scheduler: &SchedulerInfo{
				Type:          SchedulerTypeMock,
				ExternalJobID: "mock-job-003",
				RawState:      "COMPLETED",
				SubmitTime:    timePtr(now.Add(-6 * time.Hour)),
				Partition:     "batch",
				Account:       "project_a",
				ExitCode:      intPtr(0),
			},
		},
		{
			ID:             "mock-job-004",
			User:           "alice",
			Nodes:          []string{"node-07"},
			State:          JobStateFailed,
			StartTime:      now.Add(-3 * time.Hour),
			EndTime:        timePtr(now.Add(-2*time.Hour - 30*time.Minute)),
			RuntimeSeconds: 1800,
			Scheduler: &SchedulerInfo{
				Type:          SchedulerTypeMock,
				ExternalJobID: "mock-job-004",
				RawState:      "OUT_OF_MEMORY",
				SubmitTime:    timePtr(now.Add(-4 * time.Hour)),
				Partition:     "gpu",
				Account:       "project_a",
				ExitCode:      intPtr(137),
			},
		},
		{
			ID:        "mock-job-005",
			User:      "diana",
			Nodes:     []string{"node-08", "node-09"},
			State:     JobStatePending,
			StartTime: now,
			Scheduler: &SchedulerInfo{
				Type:          SchedulerTypeMock,
				ExternalJobID: "mock-job-005",
				RawState:      "PENDING",
				SubmitTime:    timePtr(now.Add(-10 * time.Minute)),
				Partition:     "debug",
				Account:       "project_c",
				Priority:      intPtr(200),
			},
		},
	}

	for _, job := range demoJobs {
		m.AddJob(job)

		// Generate metrics for running and completed jobs
		if job.State == JobStateRunning || job.State == JobStateCompleted {
			m.generateJobMetrics(job, now)
		}
	}
}

func (m *MockJobSource) generateJobMetrics(job *Job, now time.Time) {
	endTime := now
	if job.EndTime != nil {
		endTime = *job.EndTime
	}

	// Limit to last 2 hours
	startTime := job.StartTime
	if endTime.Sub(startTime) > 2*time.Hour {
		startTime = endTime.Add(-2 * time.Hour)
	}

	var samples []*MetricSample
	for t := startTime; t.Before(endTime); t = t.Add(1 * time.Minute) {
		cpuVariation := (rand.Float64() - 0.5) * 10
		memVariation := int64((rand.Float64() - 0.5) * 500)

		sample := &MetricSample{
			JobID:         job.ID,
			Timestamp:     t,
			CPUUsage:      clamp(job.CPUUsage+cpuVariation, 0, 100),
			MemoryUsageMB: maxInt64(0, job.MemoryUsageMB+memVariation),
		}

		if job.GPUUsage != nil {
			gpuVariation := (rand.Float64() - 0.5) * 15
			gpuUsage := clamp(*job.GPUUsage+gpuVariation, 0, 100)
			sample.GPUUsage = &gpuUsage
		}

		samples = append(samples, sample)
	}

	m.metrics[job.ID] = samples
}

func (m *MockJobSource) matchesFilter(job *Job, filter JobFilter) bool {
	if filter.State != nil && job.State != *filter.State {
		return false
	}
	if filter.User != nil && job.User != *filter.User {
		return false
	}
	if filter.Partition != nil && (job.Scheduler == nil || job.Scheduler.Partition != *filter.Partition) {
		return false
	}
	return true
}

// Helper functions
func floatPtr(f float64) *float64 {
	return &f
}

func timePtr(t time.Time) *time.Time {
	return &t
}

func intPtr(i int) *int {
	return &i
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

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
