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
│  │   - Resource simulation (demo)  │  │                                 │    │
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

## Component Design

### HTTP Layer

The service uses Go's standard library `net/http` with the Go 1.22+ enhanced routing patterns. Routes are defined with method and path patterns like `GET /v1/jobs/{jobId}`.

The API follows REST conventions:
- Versioned under `/v1` prefix
- JSON request/response bodies
- Standard HTTP status codes
- Idempotent operations where applicable

### API Handlers

Handlers implement the `ServerInterface` generated from the OpenAPI specification. They:
- Validate incoming requests
- Coordinate between storage and metrics layers
- Transform between API types and storage types
- Handle errors consistently

### Storage Layer

The storage layer provides a PostgreSQL-backed implementation that supports:
- Job CRUD operations
- Metrics recording and retrieval
- Automatic schema migrations
- Retention-based cleanup
- Audit event logging

### Audit System

The storage layer includes a comprehensive audit system that tracks all job changes:

**Audit Table Schema:**
| Column | Type | Description |
|--------|------|-------------|
| id | bigint | Primary key |
| job_id | text | Job identifier |
| change_type | text | Type of change (upsert, update, delete) |
| changed_at | timestamp | When the change occurred |
| changed_by | text | Who made the change (syncer, collector, api) |
| source | text | Data source (slurm, mock, api) |
| correlation_id | text | Groups related operations |
| job_snapshot | jsonb | Complete job state at change time |

**Correlation IDs:**
- A single UUID is generated for each sync batch
- All jobs processed in that batch share the same correlation ID
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

### Job Creation

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

1. OpenAPI specification defines the contract (`config/openapi.yaml`)
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

The service integrates with SLURM clusters via slurmrestd:

1. Configure the SLURM REST API endpoint via `SLURM_BASE_URL`
2. The syncer periodically fetches jobs from Slurm (default: every 30 seconds)
3. Jobs are automatically normalized to the API model
4. Existing endpoints and Prometheus metrics work unchanged
5. Audit events track all job changes with correlation IDs for traceability

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
