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

**Available Metrics**

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| hpc_job_runtime_seconds | gauge | job_id, user, node | Job runtime in seconds |
| hpc_job_cpu_usage_percent | gauge | job_id, user, node | CPU usage (0-100) |
| hpc_job_memory_usage_bytes | gauge | job_id, user, node | Memory in bytes |
| hpc_job_gpu_usage_percent | gauge | job_id, user, node | GPU usage (0-100) |
| hpc_job_state_total | gauge | state | Count of jobs by state |
| hpc_job_total | counter | - | Total jobs created |

**Example Output**

```
# HELP hpc_job_cpu_usage_percent Current CPU usage of the job
# TYPE hpc_job_cpu_usage_percent gauge
hpc_job_cpu_usage_percent{job_id="job-001",node="node-01",user="alice"} 75.5

# HELP hpc_job_state_total Number of jobs in each state
# TYPE hpc_job_state_total gauge
hpc_job_state_total{state="running"} 2
hpc_job_state_total{state="completed"} 5
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
