// Package collector provides mock job metric collection.
// It simulates resource usage updates for running jobs.
package collector

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/metrics"
	"github.com/Avicted/hpc-job-observability-service/internal/storage"
)

// Collector periodically collects and updates job metrics.
// In a real HPC environment, this would interface with Slurm or similar schedulers.
type Collector struct {
	store    storage.Storage
	exporter *metrics.Exporter

	interval time.Duration
	done     chan struct{}
	wg       sync.WaitGroup
}

// New creates a new Collector with default settings.
func New(store storage.Storage, exporter *metrics.Exporter) *Collector {
	return &Collector{
		store:    store,
		exporter: exporter,
		interval: 30 * time.Second, // Collect metrics every 30 seconds
		done:     make(chan struct{}),
	}
}

// NewWithInterval creates a new Collector with a custom collection interval.
func NewWithInterval(store storage.Storage, exporter *metrics.Exporter, interval time.Duration) *Collector {
	return &Collector{
		store:    store,
		exporter: exporter,
		interval: interval,
		done:     make(chan struct{}),
	}
}

// Start begins the metric collection loop.
func (c *Collector) Start() {
	c.wg.Add(1)
	go c.run()
	log.Printf("Metric collector started with %v interval", c.interval)
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

	ticker := time.NewTicker(c.interval)
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
		if job.State == storage.JobStateRunning {
			c.collectJobMetrics(ctx, job)
		}
	}

	// Update Prometheus metrics
	if err := c.exporter.Collect(ctx); err != nil {
		log.Printf("Failed to update Prometheus metrics: %v", err)
	}
}

// collectJobMetrics simulates collecting metrics for a single job.
// In a real system, this would read from /proc, cgroups, or job scheduler APIs.
func (c *Collector) collectJobMetrics(ctx context.Context, job *storage.Job) {
	// Simulate metric variations (in production, read real metrics)
	sample := &storage.MetricSample{
		JobID:         job.ID,
		Timestamp:     time.Now(),
		CPUUsage:      c.simulateCPUUsage(job.CPUUsage),
		MemoryUsageMB: c.simulateMemoryUsage(job.MemoryUsageMB),
	}

	if job.GPUUsage != nil {
		gpuUsage := c.simulateGPUUsage(*job.GPUUsage)
		sample.GPUUsage = &gpuUsage
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
	job.Audit = storage.NewAuditInfo("collector", "metrics")

	if err := c.store.UpdateJob(ctx, job); err != nil {
		log.Printf("Failed to update job %s metrics: %v", job.ID, err)
	}
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
