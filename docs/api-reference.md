# API Reference

This document provides detailed information about the HPC Job Observability Service REST API.

## Base URL

All API endpoints are prefixed with `/v1`:

```
http://localhost:8080/v1
```

## Authentication

The current version does not implement authentication. In production deployments, add authentication via a reverse proxy or API gateway.

## Content Types

- Request bodies: `application/json`
- Response bodies: `application/json`
- Prometheus metrics: `text/plain`

## Endpoints

### Health Check

Check service health status.

```
GET /v1/health
```

**Response**

```json
{
  "status": "healthy",
  "timestamp": "2026-01-20T12:00:00Z",
  "version": "1.0.0"
}
```

| Field | Type | Description |
|-------|------|-------------|
| status | string | Service status: `healthy` or `unhealthy` |
| timestamp | string | ISO 8601 timestamp |
| version | string | Service version |

---

### List Jobs

Retrieve a paginated list of jobs with optional filters.

```
GET /v1/jobs
```

**Query Parameters**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| state | string | - | Filter by job state |
| user | string | - | Filter by username |
| node | string | - | Filter by node name |
| limit | integer | 100 | Maximum results to return |
| offset | integer | 0 | Number of results to skip |

**Response**

```json
{
  "jobs": [
    {
      "id": "job-001",
      "user": "alice",
      "nodes": ["node-01", "node-02"],
      "state": "running",
      "start_time": "2026-01-20T10:00:00Z",
      "cpu_usage": 75.5,
      "memory_usage_mb": 4096,
      "gpu_usage": 50.0
    }
  ],
  "total": 1,
  "limit": 100,
  "offset": 0
}
```

---

### Create Job

Create a new job entry.

```
POST /v1/jobs
```

**Request Body**

```json
{
  "id": "job-001",
  "user": "alice",
  "nodes": ["node-01", "node-02"]
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| id | string | yes | Unique job identifier |
| user | string | yes | Username who owns the job |
| nodes | array | yes | List of compute nodes |

**Response** (201 Created)

```json
{
  "id": "job-001",
  "user": "alice",
  "nodes": ["node-01", "node-02"],
  "state": "running",
  "start_time": "2026-01-20T10:00:00Z",
  "cpu_usage": 0,
  "memory_usage_mb": 0
}
```

**Errors**

| Status | Error | Description |
|--------|-------|-------------|
| 400 | validation_error | Missing required fields |
| 409 | conflict | Job ID already exists |

---

### Get Job

Retrieve a single job by ID.

```
GET /v1/jobs/{jobId}
```

**Path Parameters**

| Parameter | Type | Description |
|-----------|------|-------------|
| jobId | string | Job identifier |

**Response**

```json
{
  "id": "job-001",
  "user": "alice",
  "nodes": ["node-01", "node-02"],
  "state": "running",
  "start_time": "2026-01-20T10:00:00Z",
  "cpu_usage": 75.5,
  "memory_usage_mb": 4096,
  "gpu_usage": 50.0
}
```

**Errors**

| Status | Error | Description |
|--------|-------|-------------|
| 404 | not_found | Job does not exist |

---

### Update Job

Update job state and/or resource usage.

```
PATCH /v1/jobs/{jobId}
```

**Path Parameters**

| Parameter | Type | Description |
|-----------|------|-------------|
| jobId | string | Job identifier |

**Request Body**

```json
{
  "state": "completed",
  "cpu_usage": 80.0,
  "memory_usage_mb": 8192,
  "gpu_usage": 60.0
}
```

All fields are optional. Only provided fields are updated.

| Field | Type | Description |
|-------|------|-------------|
| state | string | New job state |
| cpu_usage | number | CPU usage percentage (0-100) |
| memory_usage_mb | integer | Memory usage in megabytes |
| gpu_usage | number | GPU usage percentage (0-100) |

**Valid States**

- `pending` - Job queued but not started
- `running` - Job currently executing
- `completed` - Job finished successfully
- `failed` - Job terminated with error
- `cancelled` - Job cancelled by user

---

## Job Lifecycle Events

These endpoints are used by Slurm prolog/epilog scripts to notify the service of job lifecycle events.

### Job Started Event

Notify the service that a job has started running.

```
POST /v1/events/job-started
```

**Request Body**

```json
{
  "job_id": "12345",
  "user": "alice",
  "node_list": ["node-01", "node-02"],
  "cpu_allocation": 16,
  "gpu_allocation": 2,
  "gpu_vendor": "nvidia",
  "gpu_devices": ["GPU-abc123", "GPU-def456"],
  "cgroup_path": "/sys/fs/cgroup/system.slice/slurmstepd.scope/job_12345",
  "timestamp": "2026-01-22T10:00:00Z"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| job_id | string | yes | Slurm job ID |
| user | string | yes | Job owner |
| node_list | array | yes | List of allocated nodes |
| cpu_allocation | integer | no | Number of allocated CPUs |
| gpu_allocation | integer | no | Number of allocated GPUs |
| gpu_vendor | string | no | GPU vendor: `nvidia` or `amd` |
| gpu_devices | array | no | GPU device IDs/UUIDs |
| cgroup_path | string | no | Job's cgroup v2 path for metric collection |
| timestamp | string | no | Event timestamp (ISO 8601) |

**Response** (200 OK)

```json
{
  "status": "processed",
  "job_id": "slurm-12345",
  "message": "Job started event recorded"
}
```

---

### Job Finished Event

Notify the service that a job has finished.

```
POST /v1/events/job-finished
```

**Request Body**

```json
{
  "job_id": "12345",
  "final_state": "completed",
  "exit_code": 0,
  "signal": 0,
  "timestamp": "2026-01-22T12:30:00Z"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| job_id | string | yes | Slurm job ID |
| final_state | string | no | Final job state (completed, failed, cancelled) |
| exit_code | integer | no | Job exit code (non-zero indicates failure) |
| signal | integer | no | Signal number (15=SIGTERM, 9=SIGKILL → cancelled) |
| timestamp | string | no | Event timestamp (ISO 8601) |

**State Determination Logic**

The service determines the final job state using:
1. If `signal` is 9 (SIGKILL) or 15 (SIGTERM): state = `cancelled`
2. If `exit_code` is non-zero: state = `failed`
3. Otherwise: state = `completed`

**Response** (200 OK)

```json
{
  "status": "processed",
  "job_id": "slurm-12345",
  "message": "Job finished event recorded"
}
```

**Errors**

| Status | Error | Description |
|--------|-------|-------------|
| 400 | validation_error | Invalid event payload |
| 404 | not_found | Job not found (prolog event not received) |

---

## Scheduler Integration

The API is designed to support integration with external HPC workload managers like SLURM. Jobs can include optional scheduler metadata that preserves information from the source system.

### Scheduler Info Object

When creating or retrieving jobs, the `scheduler` field contains metadata from the external scheduler:

```json
{
  "id": "job-001",
  "user": "alice",
  "nodes": ["node-01"],
  "state": "running",
  "start_time": "2026-01-20T10:00:00Z",
  "scheduler": {
    "type": "slurm",
    "external_job_id": "12345",
    "raw_state": "RUNNING",
    "submit_time": "2026-01-20T09:55:00Z",
    "partition": "gpu",
    "account": "project_123",
    "qos": "normal",
    "priority": 100
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| type | string | Scheduler type: `mock` or `slurm` |
| external_job_id | string | Original job ID in the scheduler |
| raw_state | string | Original state string before normalization |
| submit_time | string | Time job was submitted (ISO 8601) |
| partition | string | Scheduler partition/queue name |
| account | string | Account/project charged for the job |
| qos | string | Quality of Service level |
| priority | integer | Job priority in the scheduler queue |
| exit_code | integer | Job exit code (after completion) |
| extra | object | Additional scheduler-specific metadata |

### SLURM State Mapping

When integrating with SLURM, job states are normalized to the API's 5-state model:

| Normalized State | SLURM States |
|------------------|-----------------------------------------------------------|
| `pending` | PENDING, CONFIGURING, REQUEUED, RESIZING, SUSPENDED, RESV_DEL_HOLD, REQUEUE_FED, REQUEUE_HOLD, SPECIAL_EXIT |
| `running` | RUNNING, COMPLETING, SIGNALING, STAGE_OUT |
| `completed` | COMPLETED |
| `failed` | FAILED, BOOT_FAIL, DEADLINE, NODE_FAIL, OUT_OF_MEMORY, TIMEOUT |
| `cancelled` | CANCELLED, PREEMPTED, REVOKED |

The original SLURM state is preserved in `scheduler.raw_state` for applications that need the detailed state information.

### Creating Jobs with Scheduler Info

When creating jobs from an external scheduler, include the scheduler metadata:

```
POST /v1/jobs
```

```json
{
  "id": "slurm-12345",
  "user": "alice",
  "nodes": ["node-01", "node-02"],
  "scheduler": {
    "type": "slurm",
    "external_job_id": "12345",
    "raw_state": "RUNNING",
    "partition": "gpu",
    "account": "project_a"
  }
}
```

**Response**

Returns the updated job object.

**Errors**

| Status | Error | Description |
|--------|-------|-------------|
| 400 | validation_error | Invalid state value |
| 404 | not_found | Job does not exist |

---

### Delete Job

Remove a job from the system.

```
DELETE /v1/jobs/{jobId}
```

**Path Parameters**

| Parameter | Type | Description |
|-----------|------|-------------|
| jobId | string | Job identifier |

**Response** (204 No Content)

No response body.

**Errors**

| Status | Error | Description |
|--------|-------|-------------|
| 404 | not_found | Job does not exist |

---

### Get Job Metrics

Retrieve historical metrics for a job.

```
GET /v1/jobs/{jobId}/metrics
```

**Path Parameters**

| Parameter | Type | Description |
|-----------|------|-------------|
| jobId | string | Job identifier |

**Query Parameters**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| start_time | string | - | Filter metrics after this time (ISO 8601) |
| end_time | string | - | Filter metrics before this time (ISO 8601) |
| limit | integer | 1000 | Maximum samples to return |

**Response**

```json
{
  "job_id": "job-001",
  "samples": [
    {
      "timestamp": "2026-01-20T10:00:00Z",
      "cpu_usage": 75.5,
      "memory_usage_mb": 4096,
      "gpu_usage": 50.0
    },
    {
      "timestamp": "2026-01-20T10:00:30Z",
      "cpu_usage": 78.2,
      "memory_usage_mb": 4200,
      "gpu_usage": 52.0
    }
  ],
  "total": 2
}
```

**Errors**

| Status | Error | Description |
|--------|-------|-------------|
| 404 | not_found | Job does not exist |

---

### Record Job Metrics

Record a metrics sample for a job.

```
POST /v1/jobs/{jobId}/metrics
```

**Path Parameters**

| Parameter | Type | Description |
|-----------|------|-------------|
| jobId | string | Job identifier |

**Request Body**

```json
{
  "cpu_usage": 75.5,
  "memory_usage_mb": 4096,
  "gpu_usage": 50.0
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| cpu_usage | number | yes | CPU usage percentage (0-100) |
| memory_usage_mb | integer | yes | Memory usage in megabytes |
| gpu_usage | number | no | GPU usage percentage (0-100) |

**Response** (201 Created)

```json
{
  "timestamp": "2026-01-20T10:00:00Z",
  "cpu_usage": 75.5,
  "memory_usage_mb": 4096,
  "gpu_usage": 50.0
}
```

**Errors**

| Status | Error | Description |
|--------|-------|-------------|
| 400 | validation_error | Invalid metric values |
| 404 | not_found | Job does not exist |

---

## Prometheus Metrics

The service exposes Prometheus metrics at `/metrics`.

```
GET /metrics
```

### Job-Level Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| hpc_job_runtime_seconds | gauge | job_id, user, node | Job runtime in seconds |
| hpc_job_cpu_usage_percent | gauge | job_id, user, node | CPU usage (0-100) |
| hpc_job_memory_usage_bytes | gauge | job_id, user, node | Memory in bytes |
| hpc_job_gpu_usage_percent | gauge | job_id, user, node | GPU usage (0-100) |
| hpc_job_state_total | gauge | state | Count of jobs by state |
| hpc_job_total | counter | - | Total jobs created |

### Node-Level Metrics

Aggregated metrics from running jobs per node:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| hpc_node_cpu_usage_percent | gauge | node | Avg CPU usage on node |
| hpc_node_memory_usage_bytes | gauge | node | Total memory on node |
| hpc_node_gpu_usage_percent | gauge | node | Avg GPU usage on node |
| hpc_node_job_count | gauge | node | Running job count |

**Example Output**

```
# HELP hpc_job_cpu_usage_percent Current CPU usage of the job
# TYPE hpc_job_cpu_usage_percent gauge
hpc_job_cpu_usage_percent{job_id="job-001",node="node-01",user="alice"} 75.5

# HELP hpc_job_state_total Number of jobs in each state
# TYPE hpc_job_state_total gauge
hpc_job_state_total{state="running"} 2
hpc_job_state_total{state="completed"} 5

# HELP hpc_node_cpu_usage_percent Average CPU usage on the node
# TYPE hpc_node_cpu_usage_percent gauge
hpc_node_cpu_usage_percent{node="node-01"} 65.3
hpc_node_cpu_usage_percent{node="node-02"} 82.1

# HELP hpc_node_job_count Number of running jobs on the node
# TYPE hpc_node_job_count gauge
hpc_node_job_count{node="node-01"} 3
hpc_node_job_count{node="node-02"} 2
```

---

## Error Response Format

All errors return a JSON object:

```json
{
  "error": "error_code",
  "message": "Human-readable description"
}
```

**Common Error Codes**

| Code | HTTP Status | Description |
|------|-------------|-------------|
| invalid_request | 400 | Malformed request body |
| validation_error | 400 | Field validation failed |
| not_found | 404 | Resource does not exist |
| conflict | 409 | Resource already exists |
| internal_error | 500 | Server error |


### Example Requests

```bash
# Create a job
curl -X POST http://localhost:8080/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{"id": "job-001", "user": "researcher", "nodes": ["node-1", "node-2"]}'

# Get job
curl http://localhost:8080/v1/jobs/job-001

# Record metrics
curl -X POST http://localhost:8080/v1/jobs/job-001/metrics \
  -H "Content-Type: application/json" \
  -d '{"cpuUsage": 75.5, "memoryUsageMb": 4096, "gpuUsage": 50.0}'

# Update job state
curl -X PATCH http://localhost:8080/v1/jobs/job-001 \
  -H "Content-Type: application/json" \
  -d '{"state": "completed"}'

# List jobs filtered by state
curl "http://localhost:8080/v1/jobs?state=running&limit=10"
```