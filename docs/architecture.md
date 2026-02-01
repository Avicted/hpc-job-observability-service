# Architecture

This document describes the architecture and design decisions of the HPC Job Observability Service.

## Overview

The HPC Job Observability Service is a microservice designed to track and monitor High Performance Computing (HPC) job resource utilization. It provides a REST API for job management, stores metrics in a relational database, and exposes Prometheus-compatible metrics for monitoring systems.

## System Architecture

```
                        ┌─────────────────────────────────────┐
                        │         External Systems            │
                        │  (HPC Schedulers, Monitoring Tools) │
                        └──────────────┬──────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                           HPC Observability Service                          │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                           HTTP Server (net/http)                       │  │
│  │                                                                        │  │
│  │   /v1/health    /v1/jobs    /v1/jobs/{id}/metrics    /metrics          │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                       │
│                                      ▼                                       │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                           API Handlers                                 │  │
│  │                                                                        │  │
│  │   - Request validation                                                 │  │
│  │   - Business logic coordination                                        │  │
│  │   - Response formatting                                                │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                    │                                    │                    │
│                    ▼                                    ▼                    │
│  ┌─────────────────────────────────┐  ┌─────────────────────────────────┐    │
│  │         Storage Layer           │  │       Metrics Exporter          │    │
│  │                                 │  │                                 │    │
│  │   - Job CRUD operations         │  │   - Prometheus gauges/counters  │    │
│  │   - Metrics recording           │  │   - Job state aggregation       │    │
│  │   - Retention management        │  │   - Resource usage tracking     │    │
│  └─────────────────────────────────┘  └─────────────────────────────────┘    │
│                    │                                                         │
│                    ▼                                                         │
│  ┌─────────────────────────────────┐  ┌─────────────────────────────────┐    │
│  │          Collector              │  │                                 │    │
│  │                                 │  │                                 │    │
│  │   - Periodic metric sampling    │◀─│   Background Goroutine          │    │
│  │   - Cgroup/GPU metrics          │  │                                 │    │
│  └─────────────────────────────────┘  └─────────────────────────────────┘    │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
                    │
                    ▼
      ┌────────────────────────┐
      │       Database         │
      │      PostgreSQL        │
      └────────────────────────┘
```

## Clean Architecture - Component & Dependency Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                      API Layer (internal/api)                   │
│  - Thin HTTP handlers (≤50 lines per endpoint)                  │
│  - Request parsing, response writing                            │
│  - Maps service errors to HTTP status codes                     │
│  - Uses mapper for domain ↔ API type conversion                 │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Service Layer (internal/service)               │
│  - Business logic, validation, orchestration                    │
│  - Depends on storage.Storage interface                         │
│  - Uses domain entities exclusively                             │
│  - Updates Prometheus metrics via Exporter                      │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Domain Layer (internal/domain)                 │
│  - Core business entities (Job, JobState, MetricSample)         │
│  - Domain errors (ErrJobNotFound, ErrJobAlreadyExists, etc.)    │
│  - No external dependencies (only stdlib)                       │
│  - Business rule methods (IsTerminal(), IsValid())              │
└─────────────────────────────────────────────────────────────────┘
                               ▲
                               │
┌─────────────────────────────────────────────────────────────────┐
│                 Storage Layer (internal/storage)                │
│  - Persistence interfaces using domain entities directly        │
│  - PostgresStorage implements storage.Storage                   │
│  - Handles database operations, migrations, audit logging       │
└─────────────────────────────────────────────────────────────────┘
```

**Key principles:**
- **Domain** has zero dependencies - it's the innermost layer containing entities and errors
- **Storage** interface uses domain types directly (no separate repository layer)
- **Services** depend on storage.Storage interface (not concrete implementations)
- **Handlers** are thin HTTP glue (≤50 lines per endpoint)
- **Mapper** converts between domain and API types

**Dependency Flow:**

Dependency Flow: All dependencies point inward toward the Domain. API and Service depend on Domain; Storage depends on Domain and is injected into Service via interfaces.

```
API → Service → Domain
          ↑
       Storage
```

### HTTP Layer

The service uses Go's standard library `net/http` with the Go 1.22+ enhanced routing patterns. Routes are defined with method and path patterns like `GET /v1/jobs/{jobId}`.

The API follows REST conventions:
- Versioned under `/v1` prefix
- JSON request/response bodies
- Standard HTTP status codes
- Idempotent operations where applicable

### API Handlers

Handlers implement the `ServerInterface` generated from the OpenAPI specification. They are intentionally thin (≤50 lines per endpoint) and:
- Parse HTTP requests and extract audit context from headers
- Delegate to service layer for business logic
- Transform between API types and domain types using the mapper
- Map service/domain errors to HTTP status codes

### Domain Layer

The domain layer (`internal/domain`) contains core business entities with **zero external dependencies**:

- **Job**: Core job entity with all fields
- **JobState**: Type-safe job state enum with validation methods (`IsTerminal()`, `IsValid()`)
- **MetricSample**: CPU, memory, and GPU usage at a point in time
- **SchedulerInfo**: External scheduler metadata (Slurm partition, account, QoS, etc.)
- **AuditInfo**: Change tracking metadata (who, when, correlation ID)
- **JobFilter/MetricsFilter**: Query filter parameters
- **Errors**: Domain errors (`ErrJobNotFound`, `ErrJobAlreadyExists`, `ErrInvalidJobState`, `ErrMissingAuditInfo`, `ErrJobTerminalFrozen`)

### Service Layer

The service layer (`internal/service`) contains business logic and orchestration:

- **JobService**: Job CRUD operations, state validation, audit logging
- **MetricsService**: Metric recording and retrieval, aggregation
- **EventService**: Job lifecycle events (prolog/epilog handling)

Services depend on `storage.Storage` interface, not concrete implementations.

### Storage Layer

The storage layer (`internal/storage`) provides a PostgreSQL-backed implementation using **pgx/v5** with connection pooling via **pgxpool**. The implementation is organized into specialized sub-stores that support:

- **Job CRUD operations** using domain entities directly
- **Metrics recording and retrieval** with batch operations for efficiency
- **Automatic schema migrations** on startup
- **Retention-based cleanup** for historical metrics
- **Audit event logging** with correlation IDs for traceability
- **Transaction support** for atomic multi-step operations
- **Prometheus metrics** for storage operation observability

#### Storage Architecture

The storage implementation uses a **sub-store pattern** for separation of concerns:

```go
type Store struct {
    pool *pgxpool.Pool  // Connection pool managed by pgx
    // Prometheus metrics for observability
}

// Sub-stores accessible via:
store.Jobs()    // JobStore - Job CRUD operations
store.Metrics() // MetricStore - Metrics recording and retrieval
store.Audit()   // AuditStore - Audit event logging
```

**Key Design Features:**

1. **DBTX Interface**: An abstraction that allows storage methods to work with both pooled connections (`*pgxpool.Pool`) and transactions (`pgx.Tx`) without exposing pgx types outside the storage layer.

2. **Connection Pooling**: Uses `pgxpool` for efficient connection management with configurable pool size, connection lifetime, and idle connection limits.

3. **Transaction Support**: The `WithTx()` method provides transactional operations with automatic rollback on error or panic:
   ```go
   store.WithTx(ctx, func(tx storage.Tx) error {
       // All operations within this function share a transaction
       return tx.Jobs().CreateJob(ctx, job)
   })
   ```

4. **Batch Operations**: Metrics can be recorded in batches using `pgx.Batch` for improved throughput:
   ```go
   store.RecordMetricsBatch(ctx, samples)  // Efficient bulk insert
   ```

5. **Observability**: All storage operations are instrumented with Prometheus metrics:
   - `storage_operation_duration_seconds` - Histogram of operation durations
   - `storage_operation_errors_total` - Counter of errors by type

6. **Error Classification**: Errors are classified for monitoring:
   - `not_found` - Entity not found
   - `conflict` - Unique constraint violation
   - `job_terminal` - Attempt to modify completed job
   - `invalid_input` - Validation error
   - `context_canceled` / `context_deadline` - Context errors
   - `unknown` - Other errors

#### Connection Pool Configuration

The PostgreSQL connection pool can be configured via `Config` struct:

| Setting | Default | Description |
|---------|---------|-------------|
| `MaxConns` | 25 | Maximum concurrent connections |
| `MinConns` | 5 | Minimum idle connections |
| `MaxConnLifetime` | 5 minutes | Maximum connection reuse duration |
| `MaxConnIdleTime` | 30 seconds | Maximum connection idle time |

**Implementation Files:**
- `internal/storage/postgres/store.go` - Main store, connection pooling, transactions
- `internal/storage/postgres/jobs.go` - Job persistence operations
- `internal/storage/postgres/metrics.go` - Metrics recording and retrieval
- `internal/storage/postgres/audit.go` - Audit event logging
- `internal/storage/postgres/queries.go` - SQL query constants

#### Jobs Table Schema

The `jobs` table stores the current, canonical view of each job (as created in `Migrate()`):

| Column | Type | Description |
|--------|------|-------------|
| id | text | Primary key (API job ID) |
| user_name | text | Job owner |
| nodes | text | Comma-separated node list |
| node_count | integer | Count of allocated nodes |
| state | text | Normalized state (pending, running, completed, failed, cancelled) |
| start_time | timestamptz | Job start time |
| end_time | timestamptz | Job end time (nullable) |
| runtime_seconds | double precision | Runtime in seconds |
| cpu_usage | double precision | Current CPU usage |
| memory_usage_mb | bigint | Current memory usage in MB |
| gpu_usage | double precision | Current GPU usage (nullable) |
| external_job_id | text | External scheduler job ID |
| scheduler_type | text | Scheduler type (mock, slurm) |
| raw_state | text | Raw scheduler state |
| partition | text | Scheduler partition/queue |
| account | text | Project/account |
| qos | text | Scheduler QoS |
| priority | bigint | Scheduler priority |
| submit_time | timestamptz | Scheduler submit time |
| exit_code | integer | Scheduler exit code |
| state_reason | text | Scheduler state reason |
| time_limit_minutes | integer | Time limit in minutes |
| requested_cpus | bigint | Requested CPUs |
| allocated_cpus | bigint | Allocated CPUs |
| requested_memory_mb | bigint | Requested memory in MB |
| allocated_memory_mb | bigint | Allocated memory in MB |
| requested_gpus | bigint | Requested GPUs |
| allocated_gpus | bigint | Allocated GPUs |
| cluster_name | text | Cluster name |
| scheduler_instance | text | Scheduler instance identifier |
| ingest_version | text | Ingest pipeline version |
| last_sample_at | timestamptz | Timestamp of most recent sample |
| sample_count | bigint | Count of samples recorded |
| avg_cpu_usage | double precision | Average CPU usage |
| max_cpu_usage | double precision | Max CPU usage |
| max_memory_usage_mb | bigint | Max memory usage in MB |
| avg_gpu_usage | double precision | Average GPU usage |
| max_gpu_usage | double precision | Max GPU usage |
| created_at | timestamptz | Creation time |
| updated_at | timestamptz | Last update time |

> Note: Scheduler-related columns are nullable and only populated when data is sourced from an external scheduler.

#### Metrics Table Schema

The `metric_samples` table stores time-series samples for each job:

| Column | Type | Description |
|--------|------|-------------|
| id | bigserial | Primary key |
| job_id | text | Foreign key to jobs.id |
| timestamp | timestamptz | Sample timestamp |
| cpu_usage | double precision | CPU usage at sample time |
| memory_usage_mb | bigint | Memory usage at sample time (MB) |
| gpu_usage | double precision | GPU usage at sample time (nullable) |

The metrics retention job periodically deletes rows older than the configured retention window.

### Audit System

The storage layer includes a comprehensive audit system that tracks all job changes:

**Audit Table Schema:**
| Column | Type | Description |
|--------|------|-------------|
| id | bigint | Primary key |
| job_id | text | Job identifier |
| change_type | text | Type of change (upsert, update, delete) |
| changed_at | timestamp | When the change occurred |
| changed_by | text | Who made the change (slurm-prolog, slurm-epilog, collector, api) |
| source | text | Data source (slurm, mock, api) |
| correlation_id | text | Groups related operations |
| job_snapshot | jsonb | Complete job state at change time |

**Correlation IDs:**
- A single UUID is generated for each job lifecycle operation
- All events for the same job share correlation IDs from the originating source
- Enables tracing and debugging of related operations
- Useful for auditing and distributed tracing

### Metrics Exporter

The Prometheus exporter maintains real-time metrics:

#### Job-Level Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `hpc_job_runtime_seconds` | Gauge | job_id, user, node | Current runtime |
| `hpc_job_cpu_usage_percent` | Gauge | job_id, user, node | CPU utilization |
| `hpc_job_memory_usage_bytes` | Gauge | job_id, user, node | Memory usage |
| `hpc_job_gpu_usage_percent` | Gauge | job_id, user, node | GPU utilization |
| `hpc_job_state_total` | Gauge | state | Jobs by state |
| `hpc_job_total` | Counter | - | Total jobs created |

#### Node-Level Metrics

Node metrics are aggregated from running jobs only:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `hpc_node_cpu_usage_percent` | Gauge | node | Avg CPU usage across running jobs |
| `hpc_node_memory_usage_bytes` | Gauge | node | Total memory from running jobs |
| `hpc_node_gpu_usage_percent` | Gauge | node | Avg GPU usage across GPU jobs |
| `hpc_node_job_count` | Gauge | node | Count of running jobs on node |

Node metrics enable cluster-wide visibility:
- Identify hot spots (nodes with high resource usage)
- Track job distribution across the cluster
- Monitor capacity utilization per node

### Collector

The collector runs as a background goroutine that:
- Periodically samples metrics from running jobs (configurable interval)
- Records samples to the storage layer
- Updates the Prometheus exporter
- Simulates resource variations for demo purposes

## Data Flow

### Job Creation (Event-Based)

In production with Slurm, jobs are created via prolog events:

1. Slurm starts a job on a compute node
2. Prolog script sends `POST /v1/events/job-started` with job details
3. Handler creates job record with `running` state
4. Prometheus metrics updated
5. Response confirms event processed

### Job Completion (Event-Based)

1. Job finishes execution on compute node
2. Epilog script sends `POST /v1/events/job-finished` with exit code and signal
3. Handler determines final state (completed/failed/cancelled)
4. Job record updated with end time and final state
5. Prometheus metrics updated

### Manual Job Creation

For testing or non-Slurm use cases:

1. Client sends `POST /v1/jobs` with job details
2. Handler validates request
3. Storage creates job record with `running` state
4. Prometheus counter incremented
5. Response returned with created job

### Metrics Recording

1. Client sends `POST /v1/jobs/{id}/metrics` with resource data
2. Handler validates job exists and metrics are valid
3. Storage records metric sample with timestamp
4. Job's current usage fields updated
5. Prometheus gauges updated

### Metrics Collection (Background)

1. Collector ticker fires (default: 30 seconds)
2. Running jobs queried from storage
3. For each job, current metrics sampled
4. Metrics recorded to storage
5. Prometheus exporter updated

### Retention Cleanup

1. Cleanup ticker fires (default: hourly)
2. Metrics older than retention period deleted
3. Configurable via `METRICS_RETENTION_DAYS`

## API-First Design

The service follows API-first development:

1. OpenAPI specification defines the contract (`config/openapi/service/openapi.yaml`)
2. Code generation produces types and interfaces (`oapi-codegen`)
3. Handlers implement the generated interface
4. Changes start with the spec, then regenerate

This ensures:
- API documentation is always accurate
- Type safety between layers
- Consistent validation
- Easy client generation

## Scheduler Integration

The service is designed to integrate with external HPC workload managers like SLURM. A scheduler abstraction layer (`internal/scheduler`) provides:

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Scheduler Abstraction Layer                  │
│                                                                 │
│  ┌───────────────────────┐  ┌──────────────────────┐            │
│  │   JobSource Interface │  │   StateMapping       │            │
│  │                       │  │                      │            │
│  │   - ListJobs()        │  │   - MapState()       │            │
│  │   - GetJob()          │  │   - NormalizeState() │            │
│  │   - GetJobMetrics()   │  │                      │            │
│  └───────────────────────┘  └──────────────────────┘            │
│             │                                                   │
│      ┌──────┴───────┐                                           │
│      ▼              ▼                                           │
│  ┌───────────┐  ┌────────────┐                                  │
│  │   Mock    │  │   SLURM    │                                  │
│  │  Source   │  │   Source   │                                  │
│  │           │  │            │                                  │
│  │ (testing) │  │(slurmrestd)│                                  │
│  └───────────┘  └────────────┘                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Job Model with Scheduler Metadata

Jobs include optional `scheduler` metadata to preserve information from the source system:

```go
type Job struct {
    ID             string
    User           string
    Nodes          []string
    State          JobState       // Normalized: pending, running, completed, failed, cancelled
    Scheduler      *SchedulerInfo // Optional external scheduler metadata
    // ... other fields
}

type SchedulerInfo struct {
    Type          SchedulerType // mock, slurm
    ExternalJobID string        // Original job ID in scheduler
    RawState      string        // Original state before normalization
    Partition     string        // Queue/partition name
    Account       string        // Project/account
    Priority      *int          // Queue priority
    // ... other fields
}
```

### State Normalization

External scheduler states are mapped to the API's 5-state model:

| Normalized | SLURM States |
|------------|-----------------------------------------------------------|
| pending | PENDING, CONFIGURING, REQUEUED, RESIZING, SUSPENDED, RESV_DEL_HOLD, REQUEUE_FED, REQUEUE_HOLD, SPECIAL_EXIT |
| running | RUNNING, COMPLETING, SIGNALING, STAGE_OUT |
| completed | COMPLETED |
| failed | FAILED, BOOT_FAIL, DEADLINE, NODE_FAIL, OUT_OF_MEMORY, TIMEOUT |
| cancelled | CANCELLED, PREEMPTED, REVOKED |

The original state is preserved in `scheduler.raw_state` for detailed analysis.

### SLURM Integration

The service integrates with SLURM clusters via prolog/epilog lifecycle events:

1. Install prolog/epilog scripts on compute nodes
2. Prolog creates jobs via `POST /v1/events/job-started` when jobs start
3. Epilog updates jobs via `POST /v1/events/job-finished` when jobs complete
4. Exit codes and signals are captured for accurate state mapping
5. Audit events track all changes with correlation IDs
6. Optional: Configure `SLURM_BASE_URL` for node metrics via Slurm REST API

## Configuration

Configuration follows the 12-factor app methodology:

| Source | Purpose |
|--------|---------|
| Environment variables | Runtime configuration |
| Command-line flags | Operational overrides |
| Defaults | Sensible fallbacks |

No configuration files are required at runtime.

## Error Handling

Errors are returned as JSON with consistent structure:

```json
{
  "error": "error_code",
  "message": "Human-readable description"
}
```

HTTP status codes follow REST conventions:
- `400` - Invalid request
- `404` - Resource not found
- `409` - Conflict (duplicate)
- `500` - Internal error

## Concurrency

The service is designed for concurrent access:
- HTTP handlers are stateless
- Database operations use connection pooling
- Prometheus metrics use atomic operations
- Background collector runs independently

## Testing Strategy

- Unit tests for storage operations
- Handler tests with mock storage
- Integration tests via Docker Compose
- Generated types ensure API compliance
