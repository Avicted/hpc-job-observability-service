package postgres

// SQL queries for the storage layer.
// All queries use explicit column lists (no SELECT *) and named constants.
//
// Index requirements:
// - jobs: PRIMARY KEY (id), idx_jobs_state, idx_jobs_user, idx_jobs_start_time, idx_jobs_cluster_name
// - metric_samples: PRIMARY KEY (id), idx_metrics_job_id, idx_metrics_job_timestamp (job_id, timestamp DESC)
// - job_audit_events: PRIMARY KEY (id), idx_job_audit_job_id, idx_job_audit_changed_at, idx_job_audit_change_type, idx_job_audit_correlation_id

const (
	// jobColumns lists all job table columns in consistent order for SELECT statements.
	jobColumns = `id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,
		cpu_usage, memory_usage_mb, gpu_usage,
		external_job_id, scheduler_type, raw_state, partition, account, qos, priority, submit_time, exit_code, state_reason, time_limit_minutes,
		requested_cpus, allocated_cpus, requested_memory_mb, allocated_memory_mb, requested_gpus, allocated_gpus,
		cluster_name, scheduler_instance, ingest_version,
		last_sample_at, sample_count, avg_cpu_usage, max_cpu_usage, max_memory_usage_mb, avg_gpu_usage, max_gpu_usage,
		cgroup_path, gpu_count, gpu_vendor, gpu_devices,
		created_at, updated_at`
)

// Job queries
const (
	queryJobExists = `SELECT EXISTS(SELECT 1 FROM jobs WHERE id = $1)`

	queryGetJob = `SELECT ` + jobColumns + ` FROM jobs WHERE id = $1`

	queryInsertJob = `
		INSERT INTO jobs (
			id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,
			cpu_usage, memory_usage_mb, gpu_usage,
			external_job_id, scheduler_type, raw_state, partition, account, qos, priority, submit_time, exit_code, state_reason, time_limit_minutes,
			requested_cpus, allocated_cpus, requested_memory_mb, allocated_memory_mb, requested_gpus, allocated_gpus,
			cluster_name, scheduler_instance, ingest_version,
			last_sample_at, sample_count, avg_cpu_usage, max_cpu_usage, max_memory_usage_mb, avg_gpu_usage, max_gpu_usage,
			cgroup_path, gpu_count, gpu_vendor, gpu_devices,
			created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44)`

	queryUpdateJob = `
		UPDATE jobs SET
			user_name = $2,
			nodes = $3,
			node_count = $4,
			state = $5,
			start_time = $6,
			end_time = $7,
			runtime_seconds = $8,
			cpu_usage = $9,
			memory_usage_mb = $10,
			gpu_usage = $11,
			external_job_id = $12,
			scheduler_type = $13,
			raw_state = $14,
			partition = $15,
			account = $16,
			qos = $17,
			priority = $18,
			submit_time = $19,
			exit_code = $20,
			state_reason = $21,
			time_limit_minutes = $22,
			requested_cpus = $23,
			allocated_cpus = $24,
			requested_memory_mb = $25,
			allocated_memory_mb = $26,
			requested_gpus = $27,
			allocated_gpus = $28,
			cluster_name = $29,
			scheduler_instance = $30,
			ingest_version = $31,
			last_sample_at = $32,
			sample_count = $33,
			avg_cpu_usage = $34,
			max_cpu_usage = $35,
			max_memory_usage_mb = $36,
			avg_gpu_usage = $37,
			max_gpu_usage = $38,
			cgroup_path = $39,
			gpu_count = $40,
			gpu_vendor = $41,
			gpu_devices = $42,
			updated_at = $43
		WHERE id = $1`

	queryUpsertJob = `
		INSERT INTO jobs (
			id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,
			cpu_usage, memory_usage_mb, gpu_usage,
			external_job_id, scheduler_type, raw_state, partition, account, qos, priority, submit_time, exit_code, state_reason, time_limit_minutes,
			requested_cpus, allocated_cpus, requested_memory_mb, allocated_memory_mb, requested_gpus, allocated_gpus,
			cluster_name, scheduler_instance, ingest_version,
			last_sample_at, sample_count, avg_cpu_usage, max_cpu_usage, max_memory_usage_mb, avg_gpu_usage, max_gpu_usage,
			cgroup_path, gpu_count, gpu_vendor, gpu_devices,
			created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44)
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
			last_sample_at = EXCLUDED.last_sample_at,
			sample_count = EXCLUDED.sample_count,
			avg_cpu_usage = EXCLUDED.avg_cpu_usage,
			max_cpu_usage = EXCLUDED.max_cpu_usage,
			max_memory_usage_mb = EXCLUDED.max_memory_usage_mb,
			avg_gpu_usage = EXCLUDED.avg_gpu_usage,
			max_gpu_usage = EXCLUDED.max_gpu_usage,
			cgroup_path = EXCLUDED.cgroup_path,
			gpu_count = EXCLUDED.gpu_count,
			gpu_vendor = EXCLUDED.gpu_vendor,
			gpu_devices = EXCLUDED.gpu_devices,
			updated_at = EXCLUDED.updated_at`

	queryDeleteJob = `DELETE FROM jobs WHERE id = $1`

	queryListJobsBase = `SELECT ` + jobColumns + ` FROM jobs`

	queryCountJobsBase = `SELECT COUNT(*) FROM jobs`
)

// Metric queries
const (
	queryInsertMetric = `
		INSERT INTO metric_samples (job_id, timestamp, cpu_usage, memory_usage_mb, gpu_usage)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id`

	queryGetLatestMetric = `
		SELECT id, job_id, timestamp, cpu_usage, memory_usage_mb, gpu_usage
		FROM metric_samples
		WHERE job_id = $1
		ORDER BY timestamp DESC
		LIMIT 1`

	queryGetJobMetricsBase = `
		SELECT id, job_id, timestamp, cpu_usage, memory_usage_mb, gpu_usage
		FROM metric_samples
		WHERE job_id = $1`

	queryCountJobMetricsBase = `
		SELECT COUNT(*)
		FROM metric_samples
		WHERE job_id = $1`

	queryDeleteMetricsBefore = `DELETE FROM metric_samples WHERE timestamp < $1`

	// queryUpdateJobStats updates job aggregated stats after metric insertion.
	// This is called within the same transaction as metric recording.
	queryUpdateJobStats = `
		UPDATE jobs SET
			last_sample_at = $2,
			sample_count = sample_count + 1,
			avg_cpu_usage = (avg_cpu_usage * sample_count + $3) / (sample_count + 1),
			max_cpu_usage = GREATEST(max_cpu_usage, $3),
			max_memory_usage_mb = GREATEST(max_memory_usage_mb, $4),
			avg_gpu_usage = CASE
				WHEN $5::double precision IS NULL THEN avg_gpu_usage
				ELSE (COALESCE(avg_gpu_usage, 0) * sample_count + $5) / (sample_count + 1)
			END,
			max_gpu_usage = CASE
				WHEN $5::double precision IS NULL THEN max_gpu_usage
				ELSE GREATEST(COALESCE(max_gpu_usage, 0), $5)
			END
		WHERE id = $1`
)

// Audit queries
const (
	queryInsertAuditEvent = `
		INSERT INTO job_audit_events (job_id, change_type, changed_at, changed_by, source, correlation_id, job_snapshot)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`
)

// Schema migration queries
const (
	schemaJobs = `
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
			cgroup_path TEXT,
			gpu_count INTEGER DEFAULT 0,
			gpu_vendor TEXT,
			gpu_devices TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`

	schemaJobsIndexes = `
		CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state);
		CREATE INDEX IF NOT EXISTS idx_jobs_user ON jobs(user_name);
		CREATE INDEX IF NOT EXISTS idx_jobs_start_time ON jobs(start_time);
		CREATE INDEX IF NOT EXISTS idx_jobs_cluster_name ON jobs(cluster_name)`

	schemaMetrics = `
		CREATE TABLE IF NOT EXISTS metric_samples (
			id BIGSERIAL PRIMARY KEY,
			job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
			timestamp TIMESTAMPTZ NOT NULL,
			cpu_usage DOUBLE PRECISION NOT NULL,
			memory_usage_mb BIGINT NOT NULL,
			gpu_usage DOUBLE PRECISION
		)`

	schemaMetricsIndexes = `
		CREATE INDEX IF NOT EXISTS idx_metrics_job_id ON metric_samples(job_id);
		CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metric_samples(timestamp);
		CREATE INDEX IF NOT EXISTS idx_metrics_job_timestamp ON metric_samples(job_id, timestamp DESC)`

	schemaAudit = `
		CREATE TABLE IF NOT EXISTS job_audit_events (
			id BIGSERIAL PRIMARY KEY,
			job_id TEXT NOT NULL,
			change_type TEXT NOT NULL,
			changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			changed_by TEXT NOT NULL,
			source TEXT NOT NULL,
			correlation_id TEXT NOT NULL,
			job_snapshot JSONB NOT NULL
		)`

	schemaAuditIndexes = `
		CREATE INDEX IF NOT EXISTS idx_job_audit_job_id ON job_audit_events(job_id);
		CREATE INDEX IF NOT EXISTS idx_job_audit_changed_at ON job_audit_events(changed_at);
		CREATE INDEX IF NOT EXISTS idx_job_audit_change_type ON job_audit_events(change_type);
		CREATE INDEX IF NOT EXISTS idx_job_audit_correlation_id ON job_audit_events(correlation_id)`
)
