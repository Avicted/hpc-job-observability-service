// Package syncer provides job synchronization from external schedulers to storage.
// It periodically fetches jobs from a JobSource and upserts them into the database.
package syncer

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/scheduler"
	"github.com/Avicted/hpc-job-observability-service/internal/storage"
	"github.com/google/uuid"
)

// Config holds the syncer configuration.
type Config struct {
	// SyncInterval is how often to sync jobs from the scheduler.
	SyncInterval time.Duration
	// InitialDelay is how long to wait before the first sync.
	InitialDelay time.Duration
	// ClusterName identifies the cluster for audit/observability.
	ClusterName string
	// SchedulerInstance identifies the scheduler instance.
	SchedulerInstance string
	// IngestVersion identifies the ingestion pipeline version.
	IngestVersion string
}

// DefaultConfig returns a default syncer configuration.
func DefaultConfig() Config {
	return Config{
		SyncInterval:  30 * time.Second,
		InitialDelay:  5 * time.Second,
		ClusterName:   "default",
		IngestVersion: "syncer-v1",
	}
}

// Syncer synchronizes jobs from a scheduler to storage.
type Syncer struct {
	source  scheduler.JobSource
	store   storage.Storage
	config  Config
	stopCh  chan struct{}
	wg      sync.WaitGroup
	running bool
	mu      sync.Mutex
}

// New creates a new Syncer.
func New(source scheduler.JobSource, store storage.Storage, config Config) *Syncer {
	return &Syncer{
		source: source,
		store:  store,
		config: config,
		stopCh: make(chan struct{}),
	}
}

// Start begins the background sync process.
func (s *Syncer) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return
	}

	s.running = true
	s.wg.Add(1)
	go s.runLoop()
	log.Printf("Job syncer started (interval: %v, scheduler: %s)", s.config.SyncInterval, s.source.Type())
}

// Stop halts the background sync process.
func (s *Syncer) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	s.mu.Unlock()

	close(s.stopCh)
	s.wg.Wait()
	log.Println("Job syncer stopped")
}

// runLoop is the main sync loop.
func (s *Syncer) runLoop() {
	defer s.wg.Done()

	// Initial delay before first sync
	select {
	case <-time.After(s.config.InitialDelay):
	case <-s.stopCh:
		return
	}

	// Run initial sync
	s.syncOnce()

	// Set up ticker for periodic syncs
	ticker := time.NewTicker(s.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.syncOnce()
		case <-s.stopCh:
			return
		}
	}
}

// syncOnce performs a single sync operation.
func (s *Syncer) syncOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	log.Printf("Starting job sync from %s...", s.source.Type())
	startTime := time.Now()
	correlationID := uuid.NewString()
	clusterName := s.config.ClusterName
	if clusterName == "" {
		clusterName = "default"
	}
	schedulerInstance := s.config.SchedulerInstance
	if schedulerInstance == "" {
		schedulerInstance = string(s.source.Type())
	}
	ingestVersion := s.config.IngestVersion
	if ingestVersion == "" {
		ingestVersion = "syncer-v1"
	}

	// Fetch all jobs from the scheduler
	jobs, err := s.source.ListJobs(ctx, scheduler.JobFilter{})
	if err != nil {
		log.Printf("Job sync error: failed to list jobs from scheduler: %v", err)
		return
	}

	var processed, errors int

	for _, job := range jobs {
		storageJob := convertToStorageJob(job)
		storageJob.Audit = storage.NewAuditInfoWithCorrelation("syncer", string(s.source.Type()), correlationID)
		storageJob.ClusterName = clusterName
		storageJob.SchedulerInst = schedulerInstance
		storageJob.IngestVersion = ingestVersion

		// Try to upsert the job
		err := s.store.UpsertJob(ctx, storageJob)
		if err != nil {
			log.Printf("Job sync error: failed to upsert job %s: %v", job.ID, err)
			errors++
			continue
		}
		processed++
	}

	duration := time.Since(startTime)
	log.Printf("Job sync completed: %d jobs processed (%d errors) in %v", processed, errors, duration)

	// Sync metrics if the scheduler supports it
	if s.source.SupportsMetrics() {
		s.syncMetrics(ctx, jobs)
	}
}

// syncMetrics syncs metrics for the given jobs.
func (s *Syncer) syncMetrics(ctx context.Context, jobs []*scheduler.Job) {
	var metricsCount int

	for _, job := range jobs {
		// Only sync metrics for running jobs
		if job.State != scheduler.JobStateRunning {
			continue
		}

		metrics, err := s.source.GetJobMetrics(ctx, job.ID)
		if err != nil {
			log.Printf("Job sync warning: failed to get metrics for job %s: %v", job.ID, err)
			continue
		}

		for _, m := range metrics {
			sample := &storage.MetricSample{
				JobID:         m.JobID,
				Timestamp:     m.Timestamp,
				CPUUsage:      m.CPUUsage,
				MemoryUsageMB: m.MemoryUsageMB,
				GPUUsage:      m.GPUUsage,
			}

			if err := s.store.RecordMetrics(ctx, sample); err != nil {
				// Ignore duplicate metric errors silently
				continue
			}
			metricsCount++
		}
	}

	if metricsCount > 0 {
		log.Printf("Synced %d metric samples", metricsCount)
	}
}

// convertToStorageJob converts a scheduler.Job to a storage.Job.
func convertToStorageJob(j *scheduler.Job) *storage.Job {
	storageJob := &storage.Job{
		ID:             j.ID,
		User:           j.User,
		Nodes:          j.Nodes,
		NodeCount:      j.NodeCount,
		State:          storage.JobState(j.State),
		StartTime:      j.StartTime,
		EndTime:        j.EndTime,
		RuntimeSeconds: j.RuntimeSeconds,
		CPUUsage:       j.CPUUsage,
		MemoryUsageMB:  j.MemoryUsageMB,
		GPUUsage:       j.GPUUsage,
		RequestedCPUs:  j.RequestedCPUs,
		AllocatedCPUs:  j.AllocatedCPUs,
		RequestedMemMB: j.RequestedMemMB,
		AllocatedMemMB: j.AllocatedMemMB,
		RequestedGPUs:  j.RequestedGPUs,
		AllocatedGPUs:  j.AllocatedGPUs,
	}

	// Convert scheduler info
	if j.Scheduler != nil {
		storageJob.Scheduler = &storage.SchedulerInfo{
			Type:          storage.SchedulerType(j.Scheduler.Type),
			ExternalJobID: j.Scheduler.ExternalJobID,
			RawState:      j.Scheduler.RawState,
			SubmitTime:    j.Scheduler.SubmitTime,
			Partition:     j.Scheduler.Partition,
			Account:       j.Scheduler.Account,
			QoS:           j.Scheduler.QoS,
			Priority:      j.Scheduler.Priority,
			ExitCode:      j.Scheduler.ExitCode,
			StateReason:   j.Scheduler.StateReason,
			TimeLimitMins: j.Scheduler.TimeLimitMins,
			Extra:         j.Scheduler.Extra,
		}
	}

	return storageJob
}

// SyncNow triggers an immediate sync (useful for testing).
func (s *Syncer) SyncNow() {
	s.syncOnce()
}
