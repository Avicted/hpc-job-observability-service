// Package metrics provides Prometheus metrics export for HPC job monitoring.
// Metrics follow Prometheus naming conventions and best practices.
package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/scheduler"
	"github.com/Avicted/hpc-job-observability-service/internal/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Exporter collects and exposes job metrics for Prometheus scraping.
type Exporter struct {
	store     storage.Storage
	scheduler scheduler.JobSource // Optional: for fetching node info directly

	// Job runtime gauge - shows current runtime of running jobs
	// Labels: job_id, user, node (first node for multi-node jobs)
	jobRuntimeSeconds *prometheus.GaugeVec

	// CPU usage gauge - current CPU usage percentage per job
	// Labels: job_id, user, node
	jobCPUUsagePercent *prometheus.GaugeVec

	// Memory usage gauge - current memory usage in bytes per job
	// Labels: job_id, user, node
	jobMemoryUsageBytes *prometheus.GaugeVec

	// GPU usage gauge - current GPU usage percentage per job (optional)
	// Labels: job_id, user, node
	jobGPUUsagePercent *prometheus.GaugeVec

	// Job state counter - counts jobs by state
	// Labels: state
	jobStateTotal *prometheus.GaugeVec

	// Jobs total counter - total number of jobs seen
	jobsTotal prometheus.Counter

	// Metric collection timestamp
	lastCollectTime prometheus.Gauge

	// Node-level metrics
	// CPU usage gauge per node - from scheduler or aggregated from running jobs
	// Labels: node
	nodeCPUUsagePercent *prometheus.GaugeVec

	// Memory usage gauge per node - from scheduler or aggregated from running jobs
	// Labels: node
	nodeMemoryUsageBytes *prometheus.GaugeVec

	// GPU usage gauge per node - aggregated from running jobs
	// Labels: node
	nodeGPUUsagePercent *prometheus.GaugeVec

	// Number of jobs running on each node
	// Labels: node
	nodeJobCount *prometheus.GaugeVec

	// Total memory on each node (from scheduler)
	// Labels: node
	nodeTotalMemoryBytes *prometheus.GaugeVec

	// Total CPUs on each node (from scheduler)
	// Labels: node
	nodeTotalCPUs *prometheus.GaugeVec

	// Node state (1 = up, 0 = down/drained)
	// Labels: node, state
	nodeState *prometheus.GaugeVec

	// CPU load average on each node (from scheduler)
	// Labels: node
	nodeCPULoad *prometheus.GaugeVec
}

// NewExporter creates a new metrics exporter.
func NewExporter(store storage.Storage) *Exporter {
	return NewExporterWithScheduler(store, nil)
}

// NewExporterWithScheduler creates a new metrics exporter with scheduler support.
// When a scheduler is provided, node metrics are fetched directly from it.
func NewExporterWithScheduler(store storage.Storage, sched scheduler.JobSource) *Exporter {
	e := &Exporter{
		store:     store,
		scheduler: sched,

		jobRuntimeSeconds: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "hpc",
				Subsystem: "job",
				Name:      "runtime_seconds",
				Help:      "Current runtime of the job in seconds",
			},
			[]string{"job_id", "user", "node"},
		),

		jobCPUUsagePercent: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "hpc",
				Subsystem: "job",
				Name:      "cpu_usage_percent",
				Help:      "Current CPU usage of the job as a percentage (0-100)",
			},
			[]string{"job_id", "user", "node"},
		),

		jobMemoryUsageBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "hpc",
				Subsystem: "job",
				Name:      "memory_usage_bytes",
				Help:      "Current memory usage of the job in bytes",
			},
			[]string{"job_id", "user", "node"},
		),

		jobGPUUsagePercent: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "hpc",
				Subsystem: "job",
				Name:      "gpu_usage_percent",
				Help:      "Current GPU usage of the job as a percentage (0-100)",
			},
			[]string{"job_id", "user", "node"},
		),

		jobStateTotal: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "hpc",
				Subsystem: "job",
				Name:      "state_total",
				Help:      "Number of jobs in each state",
			},
			[]string{"state"},
		),

		jobsTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "hpc",
				Subsystem: "job",
				Name:      "total",
				Help:      "Total number of jobs tracked by the system",
			},
		),

		lastCollectTime: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "hpc",
				Subsystem: "exporter",
				Name:      "last_collect_timestamp_seconds",
				Help:      "Unix timestamp of the last successful metric collection",
			},
		),

		// Node-level metrics
		nodeCPUUsagePercent: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "hpc",
				Subsystem: "node",
				Name:      "cpu_usage_percent",
				Help:      "Average CPU usage on the node from running jobs (0-100)",
			},
			[]string{"node"},
		),

		nodeMemoryUsageBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "hpc",
				Subsystem: "node",
				Name:      "memory_usage_bytes",
				Help:      "Total memory usage on the node from running jobs in bytes",
			},
			[]string{"node"},
		),

		nodeGPUUsagePercent: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "hpc",
				Subsystem: "node",
				Name:      "gpu_usage_percent",
				Help:      "Average GPU usage on the node from running jobs (0-100)",
			},
			[]string{"node"},
		),

		nodeJobCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "hpc",
				Subsystem: "node",
				Name:      "job_count",
				Help:      "Number of running jobs on the node",
			},
			[]string{"node"},
		),

		nodeTotalMemoryBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "hpc",
				Subsystem: "node",
				Name:      "total_memory_bytes",
				Help:      "Total memory on the node in bytes",
			},
			[]string{"node"},
		),

		nodeTotalCPUs: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "hpc",
				Subsystem: "node",
				Name:      "total_cpus",
				Help:      "Total CPUs on the node",
			},
			[]string{"node"},
		),

		nodeState: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "hpc",
				Subsystem: "node",
				Name:      "state",
				Help:      "Node state (1 = active, 0 = inactive)",
			},
			[]string{"node", "state"},
		),

		nodeCPULoad: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "hpc",
				Subsystem: "node",
				Name:      "cpu_load",
				Help:      "CPU load average on the node",
			},
			[]string{"node"},
		),
	}

	return e
}

// Register registers all metrics with the provided registry.
func (e *Exporter) Register(registry prometheus.Registerer) error {
	collectors := []prometheus.Collector{
		e.jobRuntimeSeconds,
		e.jobCPUUsagePercent,
		e.jobMemoryUsageBytes,
		e.jobGPUUsagePercent,
		e.jobStateTotal,
		e.jobsTotal,
		e.lastCollectTime,
		e.nodeCPUUsagePercent,
		e.nodeMemoryUsageBytes,
		e.nodeGPUUsagePercent,
		e.nodeJobCount,
		e.nodeTotalMemoryBytes,
		e.nodeTotalCPUs,
		e.nodeState,
		e.nodeCPULoad,
	}

	for _, c := range collectors {
		if err := registry.Register(c); err != nil {
			return err
		}
	}

	return nil
}

// Collect gathers current job metrics from storage.
func (e *Exporter) Collect(ctx context.Context) error {
	jobs, err := e.store.GetAllJobs(ctx)
	if err != nil {
		return err
	}

	// Reset gauges to avoid stale data for removed jobs
	e.jobRuntimeSeconds.Reset()
	e.jobCPUUsagePercent.Reset()
	e.jobMemoryUsageBytes.Reset()
	e.jobGPUUsagePercent.Reset()
	e.jobStateTotal.Reset()
	e.nodeCPUUsagePercent.Reset()
	e.nodeMemoryUsageBytes.Reset()
	e.nodeGPUUsagePercent.Reset()
	e.nodeJobCount.Reset()
	e.nodeTotalMemoryBytes.Reset()
	e.nodeTotalCPUs.Reset()
	e.nodeState.Reset()
	e.nodeCPULoad.Reset()

	// Count jobs by state
	stateCounts := map[storage.JobState]int{
		storage.JobStatePending:   0,
		storage.JobStateRunning:   0,
		storage.JobStateCompleted: 0,
		storage.JobStateFailed:    0,
		storage.JobStateCancelled: 0,
	}

	// Node-level aggregation structures
	type nodeStats struct {
		cpuTotal    float64
		memoryTotal int64
		gpuTotal    float64
		gpuCount    int
		jobCount    int
	}
	nodeMetrics := make(map[string]*nodeStats)

	now := time.Now()
	for _, job := range jobs {
		stateCounts[job.State]++

		// Use first node as the node label (for multi-node jobs)
		node := ""
		if len(job.Nodes) > 0 {
			node = job.Nodes[0]
		}

		labels := prometheus.Labels{
			"job_id": job.ID,
			"user":   job.User,
			"node":   node,
		}

		// Calculate runtime for running jobs
		var runtime float64
		if job.State == storage.JobStateRunning {
			runtime = now.Sub(job.StartTime).Seconds()
		} else if job.RuntimeSeconds > 0 {
			runtime = job.RuntimeSeconds
		}

		e.jobRuntimeSeconds.With(labels).Set(runtime)
		e.jobCPUUsagePercent.With(labels).Set(job.CPUUsage)
		// Convert MB to bytes
		e.jobMemoryUsageBytes.With(labels).Set(float64(job.MemoryUsageMB) * 1024 * 1024)

		if job.GPUUsage != nil {
			e.jobGPUUsagePercent.With(labels).Set(*job.GPUUsage)
		}

		// Aggregate metrics per node for running jobs only
		if job.State == storage.JobStateRunning {
			for _, nodeName := range job.Nodes {
				if _, exists := nodeMetrics[nodeName]; !exists {
					nodeMetrics[nodeName] = &nodeStats{}
				}
				ns := nodeMetrics[nodeName]
				ns.cpuTotal += job.CPUUsage
				ns.memoryTotal += job.MemoryUsageMB
				ns.jobCount++
				if job.GPUUsage != nil {
					ns.gpuTotal += *job.GPUUsage
					ns.gpuCount++
				}
			}
		}
	}

	// Set node-level metrics from job aggregation
	for nodeName, stats := range nodeMetrics {
		nodeLabel := prometheus.Labels{"node": nodeName}
		e.nodeJobCount.With(nodeLabel).Set(float64(stats.jobCount))
		// Average CPU across jobs on this node
		if stats.jobCount > 0 {
			e.nodeCPUUsagePercent.With(nodeLabel).Set(stats.cpuTotal / float64(stats.jobCount))
		}
		// Total memory used on this node
		e.nodeMemoryUsageBytes.With(nodeLabel).Set(float64(stats.memoryTotal) * 1024 * 1024)
		// Average GPU if any GPU jobs
		if stats.gpuCount > 0 {
			e.nodeGPUUsagePercent.With(nodeLabel).Set(stats.gpuTotal / float64(stats.gpuCount))
		}
	}

	// Fetch node info directly from scheduler if available
	if e.scheduler != nil {
		nodes, err := e.scheduler.ListNodes(ctx)
		if err == nil && nodes != nil {
			for _, node := range nodes {
				nodeLabel := prometheus.Labels{"node": node.Name}

				// Set node hardware info
				e.nodeTotalCPUs.With(nodeLabel).Set(float64(node.CPUs))
				e.nodeTotalMemoryBytes.With(nodeLabel).Set(float64(node.RealMemoryMB) * 1024 * 1024)
				e.nodeCPULoad.With(nodeLabel).Set(node.CPULoad)

				// Set node state (1 = active, 0 = inactive)
				stateValue := 1.0
				switch node.State {
				case scheduler.NodeStateDown, scheduler.NodeStateDrained:
					stateValue = 0.0
				}
				e.nodeState.With(prometheus.Labels{"node": node.Name, "state": string(node.State)}).Set(stateValue)

				// If we have scheduler data for memory usage, use it (more accurate)
				if node.AllocatedMemMB > 0 || node.FreeMemoryMB > 0 {
					usedMemMB := node.RealMemoryMB - node.FreeMemoryMB
					e.nodeMemoryUsageBytes.With(nodeLabel).Set(float64(usedMemMB) * 1024 * 1024)
				}

				// Calculate CPU usage from load if we don't have job-based data
				if _, exists := nodeMetrics[node.Name]; !exists && node.CPULoad > 0 {
					// Convert load average to approximate CPU percentage
					cpuPercent := (node.CPULoad / float64(node.CPUs)) * 100
					if cpuPercent > 100 {
						cpuPercent = 100
					}
					e.nodeCPUUsagePercent.With(nodeLabel).Set(cpuPercent)
				}

				// Set job count from scheduler if available
				if node.RunningJobs > 0 {
					e.nodeJobCount.With(nodeLabel).Set(float64(node.RunningJobs))
				}
			}
		}
	}

	// Set state counters
	for state, count := range stateCounts {
		e.jobStateTotal.With(prometheus.Labels{"state": string(state)}).Set(float64(count))
	}

	// Update total jobs counter
	e.jobsTotal.Add(0) // Just touch the counter to ensure it exists

	e.lastCollectTime.Set(float64(now.Unix()))

	return nil
}

// UpdateJobMetrics updates metrics for a specific job.
func (e *Exporter) UpdateJobMetrics(job *storage.Job) {
	node := ""
	if len(job.Nodes) > 0 {
		node = job.Nodes[0]
	}

	labels := prometheus.Labels{
		"job_id": job.ID,
		"user":   job.User,
		"node":   node,
	}

	var runtime float64
	if job.State == storage.JobStateRunning {
		runtime = time.Since(job.StartTime).Seconds()
	} else if job.RuntimeSeconds > 0 {
		runtime = job.RuntimeSeconds
	}

	e.jobRuntimeSeconds.With(labels).Set(runtime)
	e.jobCPUUsagePercent.With(labels).Set(job.CPUUsage)
	e.jobMemoryUsageBytes.With(labels).Set(float64(job.MemoryUsageMB) * 1024 * 1024)

	if job.GPUUsage != nil {
		e.jobGPUUsagePercent.With(labels).Set(*job.GPUUsage)
	}
}

// IncrementJobsTotal increments the total jobs counter.
func (e *Exporter) IncrementJobsTotal() {
	e.jobsTotal.Inc()
}

// Handler returns an HTTP handler for the /metrics endpoint.
func (e *Exporter) Handler() http.Handler {
	registry := prometheus.NewRegistry()
	if err := e.Register(registry); err != nil {
		// If registration fails, just log and continue with default registry
		return promhttp.Handler()
	}

	// Collect initial metrics
	if err := e.Collect(context.Background()); err != nil {
		_ = err // Log but continue
	}

	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// DefaultHandler returns a handler that uses the default Prometheus registry.
func DefaultHandler() http.Handler {
	return promhttp.Handler()
}
