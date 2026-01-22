// Package storage provides PostgreSQL-specific storage implementation.
package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// PostgresStorage implements Storage interface using PostgreSQL.
type PostgresStorage struct {
	baseStorage
}

// NewPostgresStorage creates a new PostgreSQL storage instance.
func NewPostgresStorage(dsn string) (*PostgresStorage, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open PostgreSQL database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping PostgreSQL database: %w", err)
	}

	return &PostgresStorage{baseStorage: baseStorage{db: db}}, nil
}

// Migrate creates the database schema.
func (s *PostgresStorage) Migrate() error {
	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		id TEXT PRIMARY KEY,
		user_name TEXT NOT NULL,
		nodes TEXT NOT NULL,
		node_count INTEGER DEFAULT 0,
		state TEXT NOT NULL DEFAULT 'pending',
		start_time TIMESTAMPTZ NOT NULL,
		end_time TIMESTAMPTZ,
		runtime_seconds DOUBLE PRECISION DEFAULT 0,
		cpu_usage DOUBLE PRECISION DEFAULT 0,
		memory_usage_mb BIGINT DEFAULT 0,
		gpu_usage DOUBLE PRECISION,
		external_job_id TEXT,
		scheduler_type TEXT,
		raw_state TEXT,
		partition TEXT,
		account TEXT,
		qos TEXT,
		priority BIGINT,
		submit_time TIMESTAMPTZ,
		exit_code INTEGER,
		state_reason TEXT,
		time_limit_minutes INTEGER,
		requested_cpus BIGINT DEFAULT 0,
		allocated_cpus BIGINT DEFAULT 0,
		requested_memory_mb BIGINT DEFAULT 0,
		allocated_memory_mb BIGINT DEFAULT 0,
		requested_gpus BIGINT DEFAULT 0,
		allocated_gpus BIGINT DEFAULT 0,
		cluster_name TEXT,
		scheduler_instance TEXT,
		ingest_version TEXT,
		last_sample_at TIMESTAMPTZ,
		sample_count BIGINT DEFAULT 0,
		avg_cpu_usage DOUBLE PRECISION DEFAULT 0,
		max_cpu_usage DOUBLE PRECISION DEFAULT 0,
		max_memory_usage_mb BIGINT DEFAULT 0,
		avg_gpu_usage DOUBLE PRECISION DEFAULT 0,
		max_gpu_usage DOUBLE PRECISION DEFAULT 0,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state);
	CREATE INDEX IF NOT EXISTS idx_jobs_user ON jobs(user_name);
	CREATE INDEX IF NOT EXISTS idx_jobs_start_time ON jobs(start_time);
	CREATE INDEX IF NOT EXISTS idx_jobs_cluster_name ON jobs(cluster_name);

	CREATE TABLE IF NOT EXISTS metric_samples (
		id BIGSERIAL PRIMARY KEY,
		job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
		timestamp TIMESTAMPTZ NOT NULL,
		cpu_usage DOUBLE PRECISION NOT NULL,
		memory_usage_mb BIGINT NOT NULL,
		gpu_usage DOUBLE PRECISION
	);

	CREATE TABLE IF NOT EXISTS job_audit_events (
		id BIGSERIAL PRIMARY KEY,
		job_id TEXT NOT NULL,
		change_type TEXT NOT NULL,
		changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		changed_by TEXT NOT NULL,
		source TEXT NOT NULL,
		correlation_id TEXT NOT NULL,
		job_snapshot JSONB NOT NULL
	);

	CREATE INDEX IF NOT EXISTS idx_metrics_job_id ON metric_samples(job_id);
	CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metric_samples(timestamp);
	CREATE INDEX IF NOT EXISTS idx_metrics_job_timestamp ON metric_samples(job_id, timestamp);

	CREATE INDEX IF NOT EXISTS idx_job_audit_job_id ON job_audit_events(job_id);
	CREATE INDEX IF NOT EXISTS idx_job_audit_changed_at ON job_audit_events(changed_at);
	CREATE INDEX IF NOT EXISTS idx_job_audit_change_type ON job_audit_events(change_type);
	CREATE INDEX IF NOT EXISTS idx_job_audit_correlation_id ON job_audit_events(correlation_id);
	`

	_, err := s.db.Exec(schema)
	return err
}

func (s *PostgresStorage) insertJobAuditEvent(ctx context.Context, tx *sql.Tx, job *Job, changeType string, changedAt time.Time, audit *JobAuditInfo) error {
	snapshotJSON, err := marshalJobSnapshot(job)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO job_audit_events (job_id, change_type, changed_at, changed_by, source, correlation_id, job_snapshot)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, job.ID, changeType, changedAt, audit.ChangedBy, audit.Source, audit.CorrelationID, snapshotJSON)
	return err
}

// scanJobRow scans a database row into a Job struct, handling all nullable fields.
// This helper eliminates duplicated null-checking code across query methods.
func scanJobRow(job *Job, nodesStr string, nodeCount sql.NullInt64, endTime sql.NullTime, gpuUsage sql.NullFloat64,
	externalJobID, schedulerType, rawState, partition, account, qos, stateReason sql.NullString,
	submitTime, lastSampleAt sql.NullTime,
	priority, exitCode, timeLimitMins sql.NullInt64,
	requestedCPUs, allocatedCPUs, requestedMemMB, allocatedMemMB sql.NullInt64,
	requestedGPUs, allocatedGPUs sql.NullInt64,
	clusterName, schedulerInstance, ingestVersion sql.NullString,
	sampleCount sql.NullInt64,
	avgCPU, maxCPU, avgGPU, maxGPU sql.NullFloat64,
	maxMem sql.NullInt64) {

	if nodesStr != "" {
		job.Nodes = strings.Split(nodesStr, ",")
	} else {
		job.Nodes = []string{}
	}
	if nodeCount.Valid {
		job.NodeCount = int(nodeCount.Int64)
	}
	if endTime.Valid {
		job.EndTime = &endTime.Time
	}
	if gpuUsage.Valid {
		job.GPUUsage = &gpuUsage.Float64
	}
	if requestedCPUs.Valid {
		job.RequestedCPUs = requestedCPUs.Int64
	}
	if allocatedCPUs.Valid {
		job.AllocatedCPUs = allocatedCPUs.Int64
	}
	if requestedMemMB.Valid {
		job.RequestedMemMB = requestedMemMB.Int64
	}
	if allocatedMemMB.Valid {
		job.AllocatedMemMB = allocatedMemMB.Int64
	}
	if requestedGPUs.Valid {
		job.RequestedGPUs = requestedGPUs.Int64
	}
	if allocatedGPUs.Valid {
		job.AllocatedGPUs = allocatedGPUs.Int64
	}
	if clusterName.Valid {
		job.ClusterName = clusterName.String
	}
	if schedulerInstance.Valid {
		job.SchedulerInst = schedulerInstance.String
	}
	if ingestVersion.Valid {
		job.IngestVersion = ingestVersion.String
	}
	if lastSampleAt.Valid {
		job.LastSampleAt = &lastSampleAt.Time
	}
	if sampleCount.Valid {
		job.SampleCount = sampleCount.Int64
	}
	if avgCPU.Valid {
		job.AvgCPUUsage = avgCPU.Float64
	}
	if maxCPU.Valid {
		job.MaxCPUUsage = maxCPU.Float64
	}
	if maxMem.Valid {
		job.MaxMemUsageMB = maxMem.Int64
	}
	if avgGPU.Valid {
		job.AvgGPUUsage = avgGPU.Float64
	}
	if maxGPU.Valid {
		job.MaxGPUUsage = maxGPU.Float64
	}
	if externalJobID.Valid || rawState.Valid || partition.Valid || account.Valid || qos.Valid || submitTime.Valid || priority.Valid || exitCode.Valid || stateReason.Valid || timeLimitMins.Valid || schedulerType.Valid {
		job.Scheduler = &SchedulerInfo{}
		if schedulerType.Valid {
			job.Scheduler.Type = SchedulerType(schedulerType.String)
		}
		if externalJobID.Valid {
			job.Scheduler.ExternalJobID = externalJobID.String
		}
		if rawState.Valid {
			job.Scheduler.RawState = rawState.String
		}
		if partition.Valid {
			job.Scheduler.Partition = partition.String
		}
		if account.Valid {
			job.Scheduler.Account = account.String
		}
		if qos.Valid {
			job.Scheduler.QoS = qos.String
		}
		if submitTime.Valid {
			job.Scheduler.SubmitTime = &submitTime.Time
		}
		if priority.Valid {
			p := priority.Int64
			job.Scheduler.Priority = &p
		}
		if exitCode.Valid {
			code := int(exitCode.Int64)
			job.Scheduler.ExitCode = &code
		}
		if stateReason.Valid {
			job.Scheduler.StateReason = stateReason.String
		}
		if timeLimitMins.Valid {
			limit := int(timeLimitMins.Int64)
			job.Scheduler.TimeLimitMins = &limit
		}
	}
}

func (s *PostgresStorage) getJobByIDTx(ctx context.Context, tx *sql.Tx, id string) (*Job, error) {
	job := &Job{}
	var nodesStr string
	var endTime sql.NullTime
	var gpuUsage sql.NullFloat64

	var nodeCount sql.NullInt64
	var externalJobID, schedulerType, rawState, partition, account, qos, stateReason sql.NullString
	var submitTime, lastSampleAt sql.NullTime
	var priority, exitCode, timeLimitMins sql.NullInt64
	var requestedCPUs, allocatedCPUs, requestedMemMB, allocatedMemMB sql.NullInt64
	var requestedGPUs, allocatedGPUs sql.NullInt64
	var clusterName, schedulerInstance, ingestVersion sql.NullString
	var sampleCount sql.NullInt64
	var avgCPU, maxCPU, avgGPU, maxGPU sql.NullFloat64
	var maxMem sql.NullInt64

	err := tx.QueryRowContext(ctx, `
		SELECT id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,
		       cpu_usage, memory_usage_mb, gpu_usage,
		       external_job_id, scheduler_type, raw_state, partition, account, qos, priority, submit_time, exit_code, state_reason, time_limit_minutes,
		       requested_cpus, allocated_cpus, requested_memory_mb, allocated_memory_mb, requested_gpus, allocated_gpus,
		       cluster_name, scheduler_instance, ingest_version,
		       last_sample_at, sample_count, avg_cpu_usage, max_cpu_usage, max_memory_usage_mb, avg_gpu_usage, max_gpu_usage,
		       created_at, updated_at
		FROM jobs WHERE id = $1
	`, id).Scan(&job.ID, &job.User, &nodesStr, &nodeCount, &job.State, &job.StartTime, &endTime,
		&job.RuntimeSeconds, &job.CPUUsage, &job.MemoryUsageMB, &gpuUsage,
		&externalJobID, &schedulerType, &rawState, &partition, &account, &qos, &priority, &submitTime, &exitCode, &stateReason, &timeLimitMins,
		&requestedCPUs, &allocatedCPUs, &requestedMemMB, &allocatedMemMB, &requestedGPUs, &allocatedGPUs,
		&clusterName, &schedulerInstance, &ingestVersion,
		&lastSampleAt, &sampleCount, &avgCPU, &maxCPU, &maxMem, &avgGPU, &maxGPU,
		&job.CreatedAt, &job.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, ErrJobNotFound
	}
	if err != nil {
		return nil, err
	}

	scanJobRow(job, nodesStr, nodeCount, endTime, gpuUsage,
		externalJobID, schedulerType, rawState, partition, account, qos, stateReason,
		submitTime, lastSampleAt,
		priority, exitCode, timeLimitMins,
		requestedCPUs, allocatedCPUs, requestedMemMB, allocatedMemMB,
		requestedGPUs, allocatedGPUs,
		clusterName, schedulerInstance, ingestVersion,
		sampleCount,
		avgCPU, maxCPU, avgGPU, maxGPU,
		maxMem)

	return job, nil
}

// CreateJob inserts a new job into the database.
func (s *PostgresStorage) CreateJob(ctx context.Context, job *Job) (err error) {
	auditInfo, err := auditInfoFromJobOrContext(ctx, job)
	if err != nil {
		return err
	}

	if job.NodeCount == 0 && len(job.Nodes) > 0 {
		job.NodeCount = len(job.Nodes)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Check if job already exists
	var exists bool
	err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM jobs WHERE id = $1)", job.ID).Scan(&exists)
	if err != nil {
		return err
	}
	if exists {
		return ErrJobAlreadyExists
	}

	now := time.Now()
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	job.UpdatedAt = now
	if job.StartTime.IsZero() {
		job.StartTime = now
	}
	if job.State == "" {
		job.State = JobStatePending
	}

	var externalJobID, schedulerType, rawState, partition, account, qos, stateReason string
	var submitTime *time.Time
	var priority *int64
	var exitCode, timeLimitMins *int
	if job.Scheduler != nil {
		externalJobID = job.Scheduler.ExternalJobID
		schedulerType = string(job.Scheduler.Type)
		rawState = job.Scheduler.RawState
		partition = job.Scheduler.Partition
		account = job.Scheduler.Account
		qos = job.Scheduler.QoS
		submitTime = job.Scheduler.SubmitTime
		priority = job.Scheduler.Priority
		exitCode = job.Scheduler.ExitCode
		stateReason = job.Scheduler.StateReason
		timeLimitMins = job.Scheduler.TimeLimitMins
	}

	nodesStr := strings.Join(job.Nodes, ",")
	_, err = tx.ExecContext(ctx, `
		INSERT INTO jobs (
			id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,
			cpu_usage, memory_usage_mb, gpu_usage,
			external_job_id, scheduler_type, raw_state, partition, account, qos, priority, submit_time, exit_code, state_reason, time_limit_minutes,
			requested_cpus, allocated_cpus, requested_memory_mb, allocated_memory_mb, requested_gpus, allocated_gpus,
			cluster_name, scheduler_instance, ingest_version,
			last_sample_at, sample_count, avg_cpu_usage, max_cpu_usage, max_memory_usage_mb, avg_gpu_usage, max_gpu_usage,
			created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40)
	`, job.ID, job.User, nodesStr, job.NodeCount, job.State, job.StartTime, job.EndTime, job.RuntimeSeconds,
		job.CPUUsage, job.MemoryUsageMB, job.GPUUsage,
		externalJobID, schedulerType, rawState, partition, account, qos, priority, submitTime, exitCode, stateReason, timeLimitMins,
		job.RequestedCPUs, job.AllocatedCPUs, job.RequestedMemMB, job.AllocatedMemMB, job.RequestedGPUs, job.AllocatedGPUs,
		job.ClusterName, job.SchedulerInst, job.IngestVersion,
		job.LastSampleAt, job.SampleCount, job.AvgCPUUsage, job.MaxCPUUsage, job.MaxMemUsageMB, job.AvgGPUUsage, job.MaxGPUUsage,
		job.CreatedAt, job.UpdatedAt)
	if err != nil {
		return err
	}

	if err = s.insertJobAuditEvent(ctx, tx, job, "create", job.CreatedAt, auditInfo); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

// GetJob retrieves a job by ID.
func (s *PostgresStorage) GetJob(ctx context.Context, id string) (*Job, error) {
	job := &Job{}
	var nodesStr string
	var endTime sql.NullTime
	var gpuUsage sql.NullFloat64
	var nodeCount sql.NullInt64
	var externalJobID, schedulerType, rawState, partition, account, qos, stateReason sql.NullString
	var submitTime, lastSampleAt sql.NullTime
	var priority, exitCode, timeLimitMins sql.NullInt64
	var requestedCPUs, allocatedCPUs, requestedMemMB, allocatedMemMB sql.NullInt64
	var requestedGPUs, allocatedGPUs sql.NullInt64
	var clusterName, schedulerInstance, ingestVersion sql.NullString
	var sampleCount sql.NullInt64
	var avgCPU, maxCPU, avgGPU, maxGPU sql.NullFloat64
	var maxMem sql.NullInt64

	err := s.db.QueryRowContext(ctx, `
		SELECT id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,
		       cpu_usage, memory_usage_mb, gpu_usage,
		       external_job_id, scheduler_type, raw_state, partition, account, qos, priority, submit_time, exit_code, state_reason, time_limit_minutes,
		       requested_cpus, allocated_cpus, requested_memory_mb, allocated_memory_mb, requested_gpus, allocated_gpus,
		       cluster_name, scheduler_instance, ingest_version,
		       last_sample_at, sample_count, avg_cpu_usage, max_cpu_usage, max_memory_usage_mb, avg_gpu_usage, max_gpu_usage,
		       created_at, updated_at
		FROM jobs WHERE id = $1
	`, id).Scan(&job.ID, &job.User, &nodesStr, &nodeCount, &job.State, &job.StartTime, &endTime,
		&job.RuntimeSeconds, &job.CPUUsage, &job.MemoryUsageMB, &gpuUsage,
		&externalJobID, &schedulerType, &rawState, &partition, &account, &qos, &priority, &submitTime, &exitCode, &stateReason, &timeLimitMins,
		&requestedCPUs, &allocatedCPUs, &requestedMemMB, &allocatedMemMB, &requestedGPUs, &allocatedGPUs,
		&clusterName, &schedulerInstance, &ingestVersion,
		&lastSampleAt, &sampleCount, &avgCPU, &maxCPU, &maxMem, &avgGPU, &maxGPU,
		&job.CreatedAt, &job.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, ErrJobNotFound
	}
	if err != nil {
		return nil, err
	}

	scanJobRow(job, nodesStr, nodeCount, endTime, gpuUsage,
		externalJobID, schedulerType, rawState, partition, account, qos, stateReason,
		submitTime, lastSampleAt,
		priority, exitCode, timeLimitMins,
		requestedCPUs, allocatedCPUs, requestedMemMB, allocatedMemMB,
		requestedGPUs, allocatedGPUs,
		clusterName, schedulerInstance, ingestVersion,
		sampleCount,
		avgCPU, maxCPU, avgGPU, maxGPU,
		maxMem)

	return job, nil
}

// UpdateJob updates an existing job.
func (s *PostgresStorage) UpdateJob(ctx context.Context, job *Job) (err error) {
	auditInfo, err := auditInfoFromJobOrContext(ctx, job)
	if err != nil {
		return err
	}

	if job.NodeCount == 0 && len(job.Nodes) > 0 {
		job.NodeCount = len(job.Nodes)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Check if the existing job is already in a terminal state; if so, skip the update.
	existingJob, err := s.getJobByIDTx(ctx, tx, job.ID)
	if err != nil {
		return err
	}
	if existingJob.State.IsTerminal() {
		// Job is frozen; commit empty transaction and return nil (skip update).
		return tx.Commit()
	}

	job.UpdatedAt = time.Now()

	// Calculate runtime if job is completing
	if job.State == JobStateCompleted || job.State == JobStateFailed || job.State == JobStateCancelled {
		if job.EndTime == nil {
			now := time.Now()
			job.EndTime = &now
		}
		job.RuntimeSeconds = job.EndTime.Sub(job.StartTime).Seconds()
	}

	var externalJobID, schedulerType, rawState, partition, account, qos, stateReason string
	var submitTime *time.Time
	var priority *int64
	var exitCode, timeLimitMins *int
	if job.Scheduler != nil {
		externalJobID = job.Scheduler.ExternalJobID
		schedulerType = string(job.Scheduler.Type)
		rawState = job.Scheduler.RawState
		partition = job.Scheduler.Partition
		account = job.Scheduler.Account
		qos = job.Scheduler.QoS
		submitTime = job.Scheduler.SubmitTime
		priority = job.Scheduler.Priority
		exitCode = job.Scheduler.ExitCode
		stateReason = job.Scheduler.StateReason
		timeLimitMins = job.Scheduler.TimeLimitMins
	}

	nodesStr := strings.Join(job.Nodes, ",")
	result, err := tx.ExecContext(ctx, `
		UPDATE jobs SET user_name = $1, nodes = $2, node_count = $3, state = $4, start_time = $5, end_time = $6,
		                runtime_seconds = $7, cpu_usage = $8, memory_usage_mb = $9, gpu_usage = $10,
		                external_job_id = $11, scheduler_type = $12, raw_state = $13, partition = $14, account = $15, qos = $16,
		                priority = $17, submit_time = $18, exit_code = $19, state_reason = $20, time_limit_minutes = $21,
		                requested_cpus = $22, allocated_cpus = $23, requested_memory_mb = $24, allocated_memory_mb = $25,
		                requested_gpus = $26, allocated_gpus = $27,
		                cluster_name = $28, scheduler_instance = $29, ingest_version = $30,
		                updated_at = $31
		WHERE id = $32
	`, job.User, nodesStr, job.NodeCount, job.State, job.StartTime, job.EndTime, job.RuntimeSeconds,
		job.CPUUsage, job.MemoryUsageMB, job.GPUUsage,
		externalJobID, schedulerType, rawState, partition, account, qos, priority, submitTime, exitCode, stateReason, timeLimitMins,
		job.RequestedCPUs, job.AllocatedCPUs, job.RequestedMemMB, job.AllocatedMemMB, job.RequestedGPUs, job.AllocatedGPUs,
		job.ClusterName, job.SchedulerInst, job.IngestVersion,
		job.UpdatedAt, job.ID)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrJobNotFound
	}

	if err = s.insertJobAuditEvent(ctx, tx, job, "update", job.UpdatedAt, auditInfo); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

// UpsertJob creates a job if it doesn't exist, or updates it if it does.
func (s *PostgresStorage) UpsertJob(ctx context.Context, job *Job) (err error) {
	auditInfo, err := auditInfoFromJobOrContext(ctx, job)
	if err != nil {
		return err
	}

	if job.NodeCount == 0 && len(job.Nodes) > 0 {
		job.NodeCount = len(job.Nodes)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Check if the existing job is already in a terminal state; if so, skip the upsert.
	existingJob, err := s.getJobByIDTx(ctx, tx, job.ID)
	if err == nil && existingJob.State.IsTerminal() {
		// Job exists and is frozen; commit empty transaction and return nil (skip upsert).
		return tx.Commit()
	}
	// If ErrJobNotFound, we proceed with insert (new job).
	// If any other error, it's unexpected, but we'll proceed and let the insert/update handle it.

	now := time.Now()
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	job.UpdatedAt = now
	if job.StartTime.IsZero() {
		job.StartTime = now
	}
	if job.State == "" {
		job.State = JobStatePending
	}

	var externalJobID, schedulerType, rawState, partition, account, qos, stateReason string
	var submitTime *time.Time
	var priority *int64
	var exitCode, timeLimitMins *int
	if job.Scheduler != nil {
		externalJobID = job.Scheduler.ExternalJobID
		schedulerType = string(job.Scheduler.Type)
		rawState = job.Scheduler.RawState
		partition = job.Scheduler.Partition
		account = job.Scheduler.Account
		qos = job.Scheduler.QoS
		submitTime = job.Scheduler.SubmitTime
		priority = job.Scheduler.Priority
		exitCode = job.Scheduler.ExitCode
		stateReason = job.Scheduler.StateReason
		timeLimitMins = job.Scheduler.TimeLimitMins
	}

	// Calculate runtime if job is completing
	if job.State == JobStateCompleted || job.State == JobStateFailed || job.State == JobStateCancelled {
		if job.EndTime != nil && !job.EndTime.IsZero() {
			job.RuntimeSeconds = job.EndTime.Sub(job.StartTime).Seconds()
		}
	}

	nodesStr := strings.Join(job.Nodes, ",")
	_, err = tx.ExecContext(ctx, `
		INSERT INTO jobs (
			id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,
			cpu_usage, memory_usage_mb, gpu_usage,
			external_job_id, scheduler_type, raw_state, partition, account, qos, priority, submit_time, exit_code, state_reason, time_limit_minutes,
			requested_cpus, allocated_cpus, requested_memory_mb, allocated_memory_mb, requested_gpus, allocated_gpus,
			cluster_name, scheduler_instance, ingest_version,
			last_sample_at, sample_count, avg_cpu_usage, max_cpu_usage, max_memory_usage_mb, avg_gpu_usage, max_gpu_usage,
			created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40)
		ON CONFLICT (id) DO UPDATE SET
			user_name = EXCLUDED.user_name,
			nodes = EXCLUDED.nodes,
			node_count = EXCLUDED.node_count,
			state = EXCLUDED.state,
			start_time = EXCLUDED.start_time,
			end_time = EXCLUDED.end_time,
			runtime_seconds = EXCLUDED.runtime_seconds,
			cpu_usage = EXCLUDED.cpu_usage,
			memory_usage_mb = EXCLUDED.memory_usage_mb,
			gpu_usage = EXCLUDED.gpu_usage,
			external_job_id = EXCLUDED.external_job_id,
			scheduler_type = EXCLUDED.scheduler_type,
			raw_state = EXCLUDED.raw_state,
			partition = EXCLUDED.partition,
			account = EXCLUDED.account,
			qos = EXCLUDED.qos,
			priority = EXCLUDED.priority,
			submit_time = EXCLUDED.submit_time,
			exit_code = EXCLUDED.exit_code,
			state_reason = EXCLUDED.state_reason,
			time_limit_minutes = EXCLUDED.time_limit_minutes,
			requested_cpus = EXCLUDED.requested_cpus,
			allocated_cpus = EXCLUDED.allocated_cpus,
			requested_memory_mb = EXCLUDED.requested_memory_mb,
			allocated_memory_mb = EXCLUDED.allocated_memory_mb,
			requested_gpus = EXCLUDED.requested_gpus,
			allocated_gpus = EXCLUDED.allocated_gpus,
			cluster_name = EXCLUDED.cluster_name,
			scheduler_instance = EXCLUDED.scheduler_instance,
			ingest_version = EXCLUDED.ingest_version,
			updated_at = EXCLUDED.updated_at
	`, job.ID, job.User, nodesStr, job.NodeCount, job.State, job.StartTime, job.EndTime, job.RuntimeSeconds,
		job.CPUUsage, job.MemoryUsageMB, job.GPUUsage,
		externalJobID, schedulerType, rawState, partition, account, qos, priority, submitTime, exitCode, stateReason, timeLimitMins,
		job.RequestedCPUs, job.AllocatedCPUs, job.RequestedMemMB, job.AllocatedMemMB, job.RequestedGPUs, job.AllocatedGPUs,
		job.ClusterName, job.SchedulerInst, job.IngestVersion,
		job.LastSampleAt, job.SampleCount, job.AvgCPUUsage, job.MaxCPUUsage, job.MaxMemUsageMB, job.AvgGPUUsage, job.MaxGPUUsage,
		job.CreatedAt, job.UpdatedAt)
	if err != nil {
		return err
	}

	if err = s.insertJobAuditEvent(ctx, tx, job, "upsert", job.UpdatedAt, auditInfo); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

// DeleteJob removes a job and its metrics from the database.
func (s *PostgresStorage) DeleteJob(ctx context.Context, id string) (err error) {
	auditInfo, err := auditInfoFromJobOrContext(ctx, nil)
	if err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	job, err := s.getJobByIDTx(ctx, tx, id)
	if err != nil {
		return err
	}

	result, err := tx.ExecContext(ctx, "DELETE FROM jobs WHERE id = $1", id)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrJobNotFound
	}

	if err = s.insertJobAuditEvent(ctx, tx, job, "delete", time.Now(), auditInfo); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

// ListJobs returns jobs matching the filter criteria.
func (s *PostgresStorage) ListJobs(ctx context.Context, filter JobFilter) ([]*Job, int, error) {
	// Build query with filters
	whereClause := []string{}
	args := []interface{}{}
	argIndex := 1

	if filter.State != nil {
		whereClause = append(whereClause, fmt.Sprintf("state = $%d", argIndex))
		args = append(args, *filter.State)
		argIndex++
	}
	if filter.User != nil {
		whereClause = append(whereClause, fmt.Sprintf("user_name = $%d", argIndex))
		args = append(args, *filter.User)
		argIndex++
	}
	if filter.Node != nil {
		whereClause = append(whereClause, fmt.Sprintf("nodes LIKE $%d", argIndex))
		args = append(args, "%"+*filter.Node+"%")
		argIndex++
	}

	where := ""
	if len(whereClause) > 0 {
		where = "WHERE " + strings.Join(whereClause, " AND ")
	}

	// Count total matching jobs
	var total int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM jobs %s", where)
	if err := s.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	// Apply pagination
	if filter.Limit <= 0 {
		filter.Limit = 100
	}
	if filter.Limit > 1000 {
		filter.Limit = 1000
	}

	query := fmt.Sprintf(`
		SELECT id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,
		       cpu_usage, memory_usage_mb, gpu_usage,
		       external_job_id, scheduler_type, raw_state, partition, account, qos, priority, submit_time, exit_code, state_reason, time_limit_minutes,
		       requested_cpus, allocated_cpus, requested_memory_mb, allocated_memory_mb, requested_gpus, allocated_gpus,
		       cluster_name, scheduler_instance, ingest_version,
		       last_sample_at, sample_count, avg_cpu_usage, max_cpu_usage, max_memory_usage_mb, avg_gpu_usage, max_gpu_usage,
		       created_at, updated_at
		FROM jobs %s
		ORDER BY start_time DESC
		LIMIT $%d OFFSET $%d
	`, where, argIndex, argIndex+1)
	args = append(args, filter.Limit, filter.Offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	jobs := []*Job{}
	for rows.Next() {
		job := &Job{}
		var nodesStr string
		var endTime sql.NullTime
		var gpuUsage sql.NullFloat64
		var nodeCount sql.NullInt64
		var externalJobID, schedulerType, rawState, partition, account, qos, stateReason sql.NullString
		var submitTime, lastSampleAt sql.NullTime
		var priority, exitCode, timeLimitMins sql.NullInt64
		var requestedCPUs, allocatedCPUs, requestedMemMB, allocatedMemMB sql.NullInt64
		var requestedGPUs, allocatedGPUs sql.NullInt64
		var clusterName, schedulerInstance, ingestVersion sql.NullString
		var sampleCount sql.NullInt64
		var avgCPU, maxCPU, avgGPU, maxGPU sql.NullFloat64
		var maxMem sql.NullInt64

		if err := rows.Scan(&job.ID, &job.User, &nodesStr, &nodeCount, &job.State, &job.StartTime, &endTime,
			&job.RuntimeSeconds, &job.CPUUsage, &job.MemoryUsageMB, &gpuUsage,
			&externalJobID, &schedulerType, &rawState, &partition, &account, &qos, &priority, &submitTime, &exitCode, &stateReason, &timeLimitMins,
			&requestedCPUs, &allocatedCPUs, &requestedMemMB, &allocatedMemMB, &requestedGPUs, &allocatedGPUs,
			&clusterName, &schedulerInstance, &ingestVersion,
			&lastSampleAt, &sampleCount, &avgCPU, &maxCPU, &maxMem, &avgGPU, &maxGPU,
			&job.CreatedAt, &job.UpdatedAt); err != nil {
			return nil, 0, err
		}

		scanJobRow(job, nodesStr, nodeCount, endTime, gpuUsage,
			externalJobID, schedulerType, rawState, partition, account, qos, stateReason,
			submitTime, lastSampleAt,
			priority, exitCode, timeLimitMins,
			requestedCPUs, allocatedCPUs, requestedMemMB, allocatedMemMB,
			requestedGPUs, allocatedGPUs,
			clusterName, schedulerInstance, ingestVersion,
			sampleCount,
			avgCPU, maxCPU, avgGPU, maxGPU,
			maxMem)

		jobs = append(jobs, job)
	}

	return jobs, total, nil
}

// GetAllJobs returns all jobs (used for metrics collection).
func (s *PostgresStorage) GetAllJobs(ctx context.Context) ([]*Job, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,
		       cpu_usage, memory_usage_mb, gpu_usage,
		       external_job_id, scheduler_type, raw_state, partition, account, qos, priority, submit_time, exit_code, state_reason, time_limit_minutes,
		       requested_cpus, allocated_cpus, requested_memory_mb, allocated_memory_mb, requested_gpus, allocated_gpus,
		       cluster_name, scheduler_instance, ingest_version,
		       last_sample_at, sample_count, avg_cpu_usage, max_cpu_usage, max_memory_usage_mb, avg_gpu_usage, max_gpu_usage,
		       created_at, updated_at
		FROM jobs
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := []*Job{}
	for rows.Next() {
		job := &Job{}
		var nodesStr string
		var endTime sql.NullTime
		var gpuUsage sql.NullFloat64
		var nodeCount sql.NullInt64
		var externalJobID, schedulerType, rawState, partition, account, qos, stateReason sql.NullString
		var submitTime, lastSampleAt sql.NullTime
		var priority, exitCode, timeLimitMins sql.NullInt64
		var requestedCPUs, allocatedCPUs, requestedMemMB, allocatedMemMB sql.NullInt64
		var requestedGPUs, allocatedGPUs sql.NullInt64
		var clusterName, schedulerInstance, ingestVersion sql.NullString
		var sampleCount sql.NullInt64
		var avgCPU, maxCPU, avgGPU, maxGPU sql.NullFloat64
		var maxMem sql.NullInt64

		if err := rows.Scan(&job.ID, &job.User, &nodesStr, &nodeCount, &job.State, &job.StartTime, &endTime,
			&job.RuntimeSeconds, &job.CPUUsage, &job.MemoryUsageMB, &gpuUsage,
			&externalJobID, &schedulerType, &rawState, &partition, &account, &qos, &priority, &submitTime, &exitCode, &stateReason, &timeLimitMins,
			&requestedCPUs, &allocatedCPUs, &requestedMemMB, &allocatedMemMB, &requestedGPUs, &allocatedGPUs,
			&clusterName, &schedulerInstance, &ingestVersion,
			&lastSampleAt, &sampleCount, &avgCPU, &maxCPU, &maxMem, &avgGPU, &maxGPU,
			&job.CreatedAt, &job.UpdatedAt); err != nil {
			return nil, err
		}

		scanJobRow(job, nodesStr, nodeCount, endTime, gpuUsage,
			externalJobID, schedulerType, rawState, partition, account, qos, stateReason,
			submitTime, lastSampleAt,
			priority, exitCode, timeLimitMins,
			requestedCPUs, allocatedCPUs, requestedMemMB, allocatedMemMB,
			requestedGPUs, allocatedGPUs,
			clusterName, schedulerInstance, ingestVersion,
			sampleCount,
			avgCPU, maxCPU, avgGPU, maxGPU,
			maxMem)

		jobs = append(jobs, job)
	}

	return jobs, nil
}

// RecordMetrics inserts a new metric sample.
func (s *PostgresStorage) RecordMetrics(ctx context.Context, sample *MetricSample) error {
	if sample.Timestamp.IsZero() {
		sample.Timestamp = time.Now()
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Convert *float64 to sql.NullFloat64 for proper NULL handling in PostgreSQL
	var gpuUsage sql.NullFloat64
	if sample.GPUUsage != nil {
		gpuUsage = sql.NullFloat64{Float64: *sample.GPUUsage, Valid: true}
	}

	err = tx.QueryRowContext(ctx, `
		INSERT INTO metric_samples (job_id, timestamp, cpu_usage, memory_usage_mb, gpu_usage)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id
	`, sample.JobID, sample.Timestamp, sample.CPUUsage, sample.MemoryUsageMB, gpuUsage).Scan(&sample.ID)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE jobs SET
			last_sample_at = $1,
			sample_count = sample_count + 1,
			avg_cpu_usage = CASE WHEN sample_count = 0 THEN $2 ELSE (avg_cpu_usage * sample_count + $2) / (sample_count + 1) END,
			max_cpu_usage = GREATEST(max_cpu_usage, $2),
			max_memory_usage_mb = GREATEST(max_memory_usage_mb, $3),
			avg_gpu_usage = CASE WHEN $4::float8 IS NULL THEN avg_gpu_usage WHEN sample_count = 0 THEN $4::float8 ELSE (avg_gpu_usage * sample_count + $4::float8) / (sample_count + 1) END,
			max_gpu_usage = CASE WHEN $4::float8 IS NULL THEN max_gpu_usage ELSE GREATEST(max_gpu_usage, $4::float8) END
		WHERE id = $5
	`, sample.Timestamp, sample.CPUUsage, sample.MemoryUsageMB, gpuUsage, sample.JobID)
	if err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

// GetJobMetrics retrieves metric samples for a job.
func (s *PostgresStorage) GetJobMetrics(ctx context.Context, jobID string, filter MetricsFilter) ([]*MetricSample, int, error) {
	// Check if job exists
	var exists bool
	if err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM jobs WHERE id = $1)", jobID).Scan(&exists); err != nil {
		return nil, 0, err
	}
	if !exists {
		return nil, 0, ErrJobNotFound
	}

	whereClause := []string{"job_id = $1"}
	args := []interface{}{jobID}
	argIndex := 2

	if filter.StartTime != nil {
		whereClause = append(whereClause, fmt.Sprintf("timestamp >= $%d", argIndex))
		args = append(args, *filter.StartTime)
		argIndex++
	}
	if filter.EndTime != nil {
		whereClause = append(whereClause, fmt.Sprintf("timestamp <= $%d", argIndex))
		args = append(args, *filter.EndTime)
		argIndex++
	}

	where := "WHERE " + strings.Join(whereClause, " AND ")

	// Count total
	var total int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM metric_samples %s", where)
	if err := s.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	// Apply limit
	if filter.Limit <= 0 {
		filter.Limit = 1000
	}
	if filter.Limit > 10000 {
		filter.Limit = 10000
	}

	query := fmt.Sprintf(`
		SELECT id, job_id, timestamp, cpu_usage, memory_usage_mb, gpu_usage
		FROM metric_samples %s
		ORDER BY timestamp DESC
		LIMIT $%d
	`, where, argIndex)
	args = append(args, filter.Limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	samples := []*MetricSample{}
	for rows.Next() {
		sample := &MetricSample{}
		var gpuUsage sql.NullFloat64

		if err := rows.Scan(&sample.ID, &sample.JobID, &sample.Timestamp, &sample.CPUUsage,
			&sample.MemoryUsageMB, &gpuUsage); err != nil {
			return nil, 0, err
		}

		if gpuUsage.Valid {
			sample.GPUUsage = &gpuUsage.Float64
		}
		samples = append(samples, sample)
	}

	return samples, total, nil
}

// GetLatestMetrics retrieves the most recent metric sample for a job.
func (s *PostgresStorage) GetLatestMetrics(ctx context.Context, jobID string) (*MetricSample, error) {
	sample := &MetricSample{}
	var gpuUsage sql.NullFloat64

	err := s.db.QueryRowContext(ctx, `
		SELECT id, job_id, timestamp, cpu_usage, memory_usage_mb, gpu_usage
		FROM metric_samples
		WHERE job_id = $1
		ORDER BY timestamp DESC
		LIMIT 1
	`, jobID).Scan(&sample.ID, &sample.JobID, &sample.Timestamp, &sample.CPUUsage,
		&sample.MemoryUsageMB, &gpuUsage)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if gpuUsage.Valid {
		sample.GPUUsage = &gpuUsage.Float64
	}

	return sample, nil
}

// DeleteMetricsBefore removes metric samples older than the cutoff time.
func (s *PostgresStorage) DeleteMetricsBefore(cutoff time.Time) error {
	_, err := s.db.Exec("DELETE FROM metric_samples WHERE timestamp < $1", cutoff)
	return err
}
