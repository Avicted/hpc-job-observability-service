# HPC Job Observability Service

A microservice for tracking and monitoring HPC (High Performance Computing) job resource utilization with Prometheus metrics export.

## Features

- **Job Management**: Create, update, list, and delete HPC jobs
- **Resource Metrics**: Track CPU, memory, and GPU usage over time
- **Prometheus Integration**: Export metrics in Prometheus format with best practices
- **Database Support**: SQLite (default) or PostgreSQL storage backends
- **Demo Data**: Seed sample data for testing and demonstration
- **Mock Server**: OpenAPI-driven mock server for learning and testing
- **Configurable Retention**: Automatic cleanup of old metrics

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   HPC Cluster   │────▶│  Observability  │───▶│   Prometheus    │
│   Schedulers    │     │    Service      │     │   + Grafana     │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                               │
                               ▼
                        ┌─────────────────┐
                        │   PostgreSQL    │
                        │   or SQLite     │
                        └─────────────────┘
```

## Quick Start

### Prerequisites

- Go 1.22+
- Docker and Docker Compose (optional)

### Running Locally

```bash
# Clone and build
go build -o server ./cmd/server

# Run with SQLite (default)
./server

# Run with demo data
./server --seed-demo

# Run with PostgreSQL
DATABASE_TYPE=postgres DATABASE_URL="postgres://user:pass@localhost/hpc?sslmode=disable" ./server
```

### Running with Docker Compose

```bash
# Start all services (app + PostgreSQL + Prometheus)
docker-compose up

# Start with mock server for testing
docker-compose --profile mock up

# View Prometheus at http://localhost:9090
# View app at http://localhost:8080
```

## API Endpoints

All endpoints are versioned under `/v1`:

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/v1/health` | Health check |
| GET | `/v1/jobs` | List all jobs (with filters) |
| POST | `/v1/jobs` | Create a new job |
| GET | `/v1/jobs/{jobId}` | Get job details |
| PATCH | `/v1/jobs/{jobId}` | Update job |
| DELETE | `/v1/jobs/{jobId}` | Delete job |
| GET | `/v1/jobs/{jobId}/metrics` | Get job metrics history |
| POST | `/v1/jobs/{jobId}/metrics` | Record new metrics |
| GET | `/metrics` | Prometheus metrics endpoint |

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

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | Server port |
| `HOST` | `0.0.0.0` | Server host |
| `DATABASE_TYPE` | `sqlite` | Database type (`sqlite` or `postgres`) |
| `DATABASE_URL` | `./hpc-jobs.db` | Database connection string |
| `METRICS_RETENTION_DAYS` | `7` | Days to retain metrics before cleanup |

Command-line flags:

| Flag | Description |
|------|-------------|
| `--seed-demo` | Seed database with demo data on startup |

## Prometheus Metrics

The service exports the following metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `hpc_job_runtime_seconds` | Gauge | Current job runtime in seconds |
| `hpc_job_cpu_usage_percent` | Gauge | Current CPU usage percentage |
| `hpc_job_memory_usage_bytes` | Gauge | Current memory usage in bytes |
| `hpc_job_gpu_usage_percent` | Gauge | Current GPU usage percentage |
| `hpc_job_state_total` | Gauge | Number of jobs in each state |
| `hpc_job_total` | Counter | Total number of jobs created |

### Example Queries

```promql
# Average CPU usage across all running jobs
avg(hpc_job_cpu_usage_percent{state="running"})

# Jobs using more than 80% memory
count(hpc_job_memory_usage_bytes > 80 * 1024 * 1024 * 1024)

# Job count by state
hpc_job_state_total
```

## Development

### Project Structure

```
├── cmd/
│   ├── server/         # Main application
│   └── mockserver/     # OpenAPI mock server
├── internal/
│   ├── api/
│   │   ├── types/      # Generated types from OpenAPI
│   │   ├── server/     # Generated server interface
│   │   ├── handler.go  # API handlers implementation
│   │   └── handler_test.go
│   ├── storage/        # Database layer (SQLite + PostgreSQL)
│   ├── collector/      # Background metric collector
│   └── metrics/        # Prometheus exporter
├── openapi.yaml        # OpenAPI 3.0 specification
├── docker-compose.yml  # Docker Compose configuration
└── Dockerfile          # Multi-stage Docker build
```

### Generate API Code

```bash
# Install oapi-codegen
go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@latest

# Regenerate types and server stubs
go generate ./...
```

### Running Tests

```bash
# Run all tests
go test ./...

# Run with verbose output
go test ./... -v

# Run with coverage
go test ./... -cover
```

### Mock Server

The mock server returns example responses from the OpenAPI specification:

```bash
# Build and run
go build -o mockserver ./cmd/mockserver
./mockserver

# Or with Docker Compose
docker-compose --profile mock up mockserver
```

## License

MIT License
