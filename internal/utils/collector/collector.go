// Package collector provides job metric collection from cgroups and GPU exporters.
// It reads real resource usage from the cgroup filesystem and GPU metrics from
// nvidia-smi (NVIDIA) or rocm-smi (AMD).
package collector

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/domain"
	"github.com/Avicted/hpc-job-observability-service/internal/storage"
	"github.com/Avicted/hpc-job-observability-service/internal/utils/audit"
	"github.com/Avicted/hpc-job-observability-service/internal/utils/cgroup"
	"github.com/Avicted/hpc-job-observability-service/internal/utils/gpu"
	"github.com/Avicted/hpc-job-observability-service/internal/utils/metrics"
)

// CollectorMode defines how the collector gathers metrics.
type CollectorMode string

const (
	// ModeMock simulates metrics for testing/demo purposes.
	ModeMock CollectorMode = "mock"
	// ModeCgroup reads real metrics from cgroup v2 filesystem.
	ModeCgroup CollectorMode = "cgroup"
)

// Config holds the collector configuration.
type Config struct {
	// Mode determines how metrics are collected.
	Mode CollectorMode
	// Interval is how often to collect metrics.
	Interval time.Duration
	// CgroupRoot is the root path for cgroup v2 filesystem.
	CgroupRoot string
}

// DefaultConfig returns the default collector configuration.
func DefaultConfig() Config {
	return Config{
		Mode:       ModeCgroup,
		Interval:   30 * time.Second,
		CgroupRoot: cgroup.DefaultCgroupRoot,
	}
}

// Collector periodically collects and updates job metrics.
// It can operate in mock mode (for testing) or cgroup mode (for production).
type Collector struct {
	store       storage.Store
	exporter    *metrics.Exporter
	config      Config
	cgroupRdr   *cgroup.Reader
	gpuDetector *gpu.Detector

	// prevStats stores previous cgroup stats for CPU percentage calculation
	prevStats   map[string]*cgroup.Stats
	prevStatsMu sync.Mutex

	done chan struct{}
	wg   sync.WaitGroup
}

// New creates a new Collector with default settings.
func New(store storage.Store, exporter *metrics.Exporter) *Collector {
	return NewWithConfig(store, exporter, DefaultConfig())
}

// NewWithInterval creates a new Collector with a custom collection interval.
func NewWithInterval(store storage.Store, exporter *metrics.Exporter, interval time.Duration) *Collector {
	cfg := DefaultConfig()
	cfg.Interval = interval
	return NewWithConfig(store, exporter, cfg)
}

// NewWithConfig creates a new Collector with full configuration.
func NewWithConfig(store storage.Store, exporter *metrics.Exporter, config Config) *Collector {
	c := &Collector{
		store:     store,
		exporter:  exporter,
		config:    config,
		prevStats: make(map[string]*cgroup.Stats),
		done:      make(chan struct{}),
	}

	// Initialize cgroup reader if in cgroup mode
	if config.Mode == ModeCgroup {
		if config.CgroupRoot != "" {
			c.cgroupRdr = cgroup.NewReaderWithRoot(config.CgroupRoot)
		} else {
			c.cgroupRdr = cgroup.NewReader()
		}

		// Check if cgroup v2 is available
		if !c.cgroupRdr.IsCgroupV2() {
			log.Printf("Warning: cgroup v2 not detected, falling back to mock mode")
			c.config.Mode = ModeMock
		}
	}

	// Initialize GPU detector
	c.gpuDetector = gpu.NewDetector()

	return c
}

// Start begins the metric collection loop.
func (c *Collector) Start() {
	c.wg.Add(1)
	go c.run()
	log.Printf("Metric collector started with %v interval (mode: %s)", c.config.Interval, c.config.Mode)
}

// Stop halts the metric collection loop.
func (c *Collector) Stop() {
	close(c.done)
	c.wg.Wait()
	log.Println("Metric collector stopped")
}

func (c *Collector) run() {
	defer c.wg.Done()

	// Initial collection
	c.collectAll()

	ticker := time.NewTicker(c.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.collectAll()
		case <-c.done:
			return
		}
	}
}

// collectAll gathers metrics for all running jobs.
func (c *Collector) collectAll() {
	ctx := context.Background()

	jobs, err := c.store.GetAllJobs(ctx)
	if err != nil {
		log.Printf("Failed to get jobs for metric collection: %v", err)
		return
	}

	for _, job := range jobs {
		if job.State == domain.JobStateRunning {
			c.collectJobMetrics(ctx, job)
		}
	}

	// Update Prometheus metrics
	if err := c.exporter.Collect(ctx); err != nil {
		log.Printf("Failed to update Prometheus metrics: %v", err)
	}
}

// collectJobMetrics collects metrics for a single job.
func (c *Collector) collectJobMetrics(ctx context.Context, job *domain.Job) {
	var sample *domain.MetricSample

	switch c.config.Mode {
	case ModeCgroup:
		sample = c.collectCgroupMetrics(ctx, job)
	case ModeMock:
		sample = c.collectMockMetrics(ctx, job)
	default:
		sample = c.collectMockMetrics(ctx, job)
	}

	if sample == nil {
		return
	}

	// Collect GPU metrics if the job has GPUs allocated
	if job.GPUCount > 0 && len(job.GPUDevices) > 0 {
		gpuMetrics := c.collectGPUMetrics(ctx, job)
		if gpuMetrics != nil {
			sample.GPUUsage = gpuMetrics
		}
	}

	// Record the metric sample
	if err := c.store.RecordMetrics(ctx, sample); err != nil {
		log.Printf("Failed to record metrics for job %s: %v", job.ID, err)
		return
	}

	// Update job's current resource usage
	job.CPUUsage = sample.CPUUsage
	job.MemoryUsageMB = sample.MemoryUsageMB
	job.GPUUsage = sample.GPUUsage
	job.Audit = audit.NewAuditInfo("collector", "metrics")

	if err := c.store.UpdateJob(ctx, job); err != nil {
		log.Printf("Failed to update job %s metrics: %v", job.ID, err)
	}
}

// collectCgroupMetrics reads metrics from the job's cgroup.
func (c *Collector) collectCgroupMetrics(ctx context.Context, job *domain.Job) *domain.MetricSample {
	if c.cgroupRdr == nil {
		return c.collectMockMetrics(ctx, job)
	}

	// Find the cgroup path for the job
	cgroupPath := job.CgroupPath
	if cgroupPath == "" {
		// Try to find the cgroup path automatically
		var err error
		cgroupPath, err = c.cgroupRdr.FindCgroupForJob(job.ID)
		if err != nil {
			// Fall back to mock if cgroup not found
			log.Printf("Cgroup not found for job %s, using mock metrics", job.ID)
			return c.collectMockMetrics(ctx, job)
		}

		// Update job with discovered cgroup path
		job.CgroupPath = cgroupPath
	}

	// Read current cgroup stats
	stats, err := c.cgroupRdr.ReadStats(cgroupPath)
	if err != nil {
		log.Printf("Failed to read cgroup stats for job %s: %v", job.ID, err)
		return c.collectMockMetrics(ctx, job)
	}

	// Calculate CPU percentage from delta
	cpuPercent := c.calculateCPUPercent(job.ID, stats, int(job.AllocatedCPUs))

	// Convert memory from bytes to MB
	memoryMB := int64(stats.MemoryCurrentBytes / (1024 * 1024))

	return &domain.MetricSample{
		JobID:         job.ID,
		Timestamp:     stats.Timestamp,
		CPUUsage:      cpuPercent,
		MemoryUsageMB: memoryMB,
	}
}

// calculateCPUPercent calculates CPU usage percentage from cgroup stats delta.
func (c *Collector) calculateCPUPercent(jobID string, curr *cgroup.Stats, numCPUs int) float64 {
	c.prevStatsMu.Lock()
	defer c.prevStatsMu.Unlock()

	prev := c.prevStats[jobID]
	percent := cgroup.CalculateCPUPercent(prev, curr, numCPUs)

	// Store current stats for next calculation
	c.prevStats[jobID] = curr

	return percent
}

// collectGPUMetrics collects GPU metrics for the job's allocated devices.
func (c *Collector) collectGPUMetrics(ctx context.Context, job *domain.Job) *float64 {
	if c.gpuDetector == nil || len(job.GPUDevices) == 0 {
		return nil
	}

	gpuMetrics, err := c.gpuDetector.GetMetricsForDevices(ctx, job.GPUDevices)
	if err != nil {
		log.Printf("Failed to get GPU metrics for job %s: %v", job.ID, err)
		return nil
	}

	if len(gpuMetrics) == 0 {
		return nil
	}

	// Convert to Prometheus metrics format and push to exporter
	var prometheusMetrics []metrics.GPUDeviceMetric
	node := ""
	if len(job.Nodes) > 0 {
		node = job.Nodes[0]
	}

	var totalUtil float64
	for _, m := range gpuMetrics {
		totalUtil += m.UtilizationPct

		prometheusMetrics = append(prometheusMetrics, metrics.GPUDeviceMetric{
			JobID:          job.ID,
			Node:           node,
			Vendor:         string(job.GPUVendor),
			DeviceID:       m.ID,
			Utilization:    m.UtilizationPct,
			MemoryUsedMB:   m.MemoryUsedMB,
			PowerWatts:     m.PowerWatts,
			TemperatureCel: float64(m.TemperatureC),
		})
	}

	// Push per-device metrics to exporter
	if c.exporter != nil && len(prometheusMetrics) > 0 {
		c.exporter.UpdateGPUDeviceMetrics(prometheusMetrics)
	}

	// Average GPU utilization across all devices
	avgUtil := totalUtil / float64(len(gpuMetrics))

	return &avgUtil
}

// collectMockMetrics simulates metrics for testing/demo purposes.
func (c *Collector) collectMockMetrics(_ context.Context, job *domain.Job) *domain.MetricSample {
	sample := &domain.MetricSample{
		JobID:         job.ID,
		Timestamp:     time.Now(),
		CPUUsage:      c.simulateCPUUsage(job.CPUUsage),
		MemoryUsageMB: c.simulateMemoryUsage(job.MemoryUsageMB),
	}

	if job.GPUUsage != nil {
		gpuUsage := c.simulateGPUUsage(*job.GPUUsage)
		sample.GPUUsage = &gpuUsage
	}

	return sample
}

// CleanupPrevStats removes stored stats for completed jobs.
// Call this when a job finishes to free memory.
func (c *Collector) CleanupPrevStats(jobID string) {
	c.prevStatsMu.Lock()
	defer c.prevStatsMu.Unlock()
	delete(c.prevStats, jobID)
}

// simulateCPUUsage generates realistic CPU usage variation.
func (c *Collector) simulateCPUUsage(baseline float64) float64 {
	// Add random variation of ±10%
	variation := (rand.Float64() - 0.5) * 20
	usage := baseline + variation

	// Clamp to valid range
	if usage < 0 {
		usage = 0
	}
	if usage > 100 {
		usage = 100
	}
	return usage
}

// simulateMemoryUsage generates realistic memory usage variation.
func (c *Collector) simulateMemoryUsage(baseline int64) int64 {
	// Add random variation of ±5%
	variation := int64(float64(baseline) * (rand.Float64() - 0.5) * 0.1)
	usage := baseline + variation

	if usage < 0 {
		usage = 0
	}
	return usage
}

// simulateGPUUsage generates realistic GPU usage variation.
func (c *Collector) simulateGPUUsage(baseline float64) float64 {
	// Add random variation of ±15%
	variation := (rand.Float64() - 0.5) * 30
	usage := baseline + variation

	if usage < 0 {
		usage = 0
	}
	if usage > 100 {
		usage = 100
	}
	return usage
}

// CollectOnce performs a single metric collection cycle.
// Useful for testing or on-demand collection.
func (c *Collector) CollectOnce() {
	c.collectAll()
}
