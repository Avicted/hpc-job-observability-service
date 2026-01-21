// Package scheduler provides an abstraction layer for job scheduling systems.
package scheduler

import (
	"context"
	"fmt"
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

// ListNodes returns simulated compute nodes for the mock cluster.
func (m *MockJobSource) ListNodes(ctx context.Context) ([]*Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Generate 50 mock nodes to match the demo jobs
	nodes := make([]*Node, 50)
	for i := 0; i < 50; i++ {
		nodeName := fmt.Sprintf("node-%02d", i+1)

		// Calculate load based on running jobs on this node
		runningJobs := 0
		allocCPUs := 0
		allocMemMB := int64(0)
		for _, job := range m.jobs {
			if job.State == JobStateRunning {
				for _, n := range job.Nodes {
					if n == nodeName {
						runningJobs++
						allocCPUs += 4 // Assume 4 CPUs per job
						allocMemMB += job.MemoryUsageMB / int64(len(job.Nodes))
						break
					}
				}
			}
		}

		// Simulate realistic node states
		state := NodeStateIdle
		if runningJobs > 0 {
			state = NodeStateAllocated
		}

		// Vary node specs slightly for realism
		cpus := 32 + (i%3)*16                       // 32, 48, or 64 CPUs
		memoryMB := int64(128*1024 + (i%4)*64*1024) // 128GB to 320GB

		// CPU load based on allocated CPUs (with some variance)
		cpuLoad := float64(allocCPUs) / float64(cpus) * 100
		if cpuLoad > 0 {
			cpuLoad += (rand.Float64() - 0.5) * 10 // Add some variance
			if cpuLoad < 0 {
				cpuLoad = 0
			}
		}

		nodes[i] = &Node{
			Name:           nodeName,
			Hostname:       nodeName,
			State:          state,
			CPUs:           cpus,
			Cores:          cpus / 2,
			Sockets:        2,
			RealMemoryMB:   memoryMB,
			FreeMemoryMB:   memoryMB - allocMemMB,
			CPULoad:        cpuLoad,
			AllocatedCPUs:  allocCPUs,
			AllocatedMemMB: allocMemMB,
			RunningJobs:    runningJobs,
		}
	}

	return nodes, nil
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

	users := []string{"Alice", "Bob", "Liam", "Sofia", "Noah", "Olivia", "Ethan", "Mia", "Lucas", "Ava", "Leo", "Isabella", "Jack", "Amelia", "Oliver", "Charlotte", "Henry", "Ella", "Benjamin", "Grace", "Daniel", "Lily", "Samuel", "Hannah", "Matthew", "Nora", "James", "Zoe", "William", "Chloe", "Michael", "Aria", "David", "Ruby", "Joseph", "Lucy", "Andrew", "Violet", "Thomas", "Emily", "Christopher", "Freya", "Joshua", "Clara", "Nathan", "Iris", "Ryan", "Stella", "Jonathan", "Alice", "Aaron", "Eliza", "Caleb", "Naomi", "Isaac", "Maya", "Sebastian", "Eva", "Nicholas", "Rose", "Julian", "Aurora", "Owen", "Hazel", "Dylan", "Willow", "Zachary", "Penelope", "Carter", "Margot", "Eli", "June", "Finn", "Scarlett", "Adrian", "Thea", "Miles", "Florence", "Simon", "Beatrice", "Victor", "Helena", "Max", "Astrid", "Patrick", "Ingrid", "Rowan", "Sienna", "Theo", "Elin", "Hugo", "Linnea", "Marcus", "Josephine", "Paul", "Matilda", "Oscar", "Agnes", "Tobias", "Edith"}

	partitions := []string{"gpu", "compute", "batch", "debug"}

	accounts := make([]string, 0, 26)
	for ch := 'a'; ch <= 'z'; ch++ {
		accounts = append(accounts, fmt.Sprintf("project_%c", ch))
	}

	qosLevels := []string{"normal", "high", "low", "preempt"}

	states := []JobState{JobStateRunning, JobStateCompleted, JobStateFailed, JobStateCancelled, JobStatePending}

	jobCount := 100
	for i := 1; i <= jobCount; i++ {
		user := users[i%len(users)]
		partition := partitions[i%len(partitions)]
		account := accounts[i%len(accounts)]
		qos := qosLevels[i%len(qosLevels)]
		state := states[i%len(states)]

		jobID := fmt.Sprintf("mock-job-%03d", i)

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

		job := &Job{
			ID:            jobID,
			User:          user,
			Nodes:         nodes,
			State:         state,
			StartTime:     startTime,
			CPUUsage:      25 + float64(i%75),
			MemoryUsageMB: int64(1024 * (1 + i%16)),
			Scheduler: &SchedulerInfo{
				Type:          SchedulerTypeMock,
				ExternalJobID: jobID,
				RawState:      string(state),
				SubmitTime:    timePtr(submitTime),
				Partition:     partition,
				Account:       account,
				QoS:           qos,
				Priority:      int64Ptr(int64(10 + (i % 200))),
			},
		}

		if partition == "gpu" {
			job.GPUUsage = floatPtr(30 + float64(i%70))
		}

		switch state {
		case JobStateCompleted:
			end := startTime.Add(time.Duration(30+i) * time.Minute)
			job.EndTime = &end
			job.RuntimeSeconds = end.Sub(startTime).Seconds()
			job.Scheduler.ExitCode = intPtr(0)
		case JobStateFailed:
			end := startTime.Add(time.Duration(10+i) * time.Minute)
			job.EndTime = &end
			job.RuntimeSeconds = end.Sub(startTime).Seconds()
			job.Scheduler.ExitCode = intPtr(137)
		case JobStateCancelled:
			end := startTime.Add(time.Duration(5+i) * time.Minute)
			job.EndTime = &end
			job.RuntimeSeconds = end.Sub(startTime).Seconds()
		}

		m.AddJob(job)

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

func int64Ptr(i int64) *int64 {
	return &i
}

func strPtr(s string) *string {
	return &s
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
