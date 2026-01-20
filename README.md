# HPC Job Observability Service

A microservice for tracking and monitoring HPC (High Performance Computing) job resource utilization with Prometheus metrics export.

> This project is a proof of concept for building an observability service for HPC job schedulers like Slurm. It provides a RESTful API to manage jobs and record resource usage metrics (CPU, memory, GPU) over time. The service exports metrics in Prometheus format for easy integration with monitoring systems.

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
│   HPC Cluster   │────▶│  Observability  │────▶│   Prometheus    │
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

# Copy and configure environment variables
cp .env.example .env
# Edit .env with your settings

# Run with SQLite (default)
./server

# Run with demo data
./server --seed-demo

# Run with PostgreSQL
DATABASE_TYPE=postgres DATABASE_URL="postgres://user:pass@localhost/hpc?sslmode=disable" ./server
```

### Running with Docker Compose

```bash
# Copy environment file (required for secrets)
cp .env.example .env
# Edit .env with your settings (especially passwords!)

# Start all services (app + PostgreSQL + Prometheus + Grafana)
docker-compose up

# Start with mock server for testing
docker-compose --profile mock up

# View Prometheus at http://localhost:9090
# View Grafana at http://localhost:3000 (credentials from .env)
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

### Environment Variables

The service uses environment variables for configuration. For local development, copy `.env.example` to `.env` and customize:

```bash
cp .env.example .env
```

> ⚠️ **Security Note**: Never commit `.env` files containing secrets to version control. The `.env` file is already in `.gitignore`.

#### Server Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | Server port |
| `HOST` | `0.0.0.0` | Server host |

#### Database Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_TYPE` | `sqlite` | Database type (`sqlite` or `postgres`) |
| `DATABASE_URL` | `file:hpc_jobs.db...` | Database connection string |
| `POSTGRES_USER` | `hpc` | PostgreSQL username (Docker) |
| `POSTGRES_PASSWORD` | - | PostgreSQL password (Docker) |
| `POSTGRES_DB` | `hpc_jobs` | PostgreSQL database name (Docker) |

#### Metrics & Grafana Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `METRICS_RETENTION_DAYS` | `7` | Days to retain metrics before cleanup |
| `GF_SECURITY_ADMIN_USER` | `admin` | Grafana admin username |
| `GF_SECURITY_ADMIN_PASSWORD` | - | Grafana admin password |

### Command-line Flags

| Flag | Description |
|------|-------------|
| `--seed-demo` | Seed database with demo data on startup |

## Prometheus Metrics

The service exports the following metrics:

### Job-Level Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `hpc_job_runtime_seconds` | Gauge | job_id, user, node | Current job runtime in seconds |
| `hpc_job_cpu_usage_percent` | Gauge | job_id, user, node | Current CPU usage percentage |
| `hpc_job_memory_usage_bytes` | Gauge | job_id, user, node | Current memory usage in bytes |
| `hpc_job_gpu_usage_percent` | Gauge | job_id, user, node | Current GPU usage percentage |
| `hpc_job_state_total` | Gauge | state | Number of jobs in each state |
| `hpc_job_total` | Counter | - | Total number of jobs created |

### Node-Level Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `hpc_node_cpu_usage_percent` | Gauge | node | Average CPU usage on node (from running jobs) |
| `hpc_node_memory_usage_bytes` | Gauge | node | Total memory usage on node (from running jobs) |
| `hpc_node_gpu_usage_percent` | Gauge | node | Average GPU usage on node (from running jobs) |
| `hpc_node_job_count` | Gauge | node | Number of running jobs on node |

### Example Queries

```promql
# Average CPU usage across all running jobs
avg(hpc_job_cpu_usage_percent)

# Jobs using more than 80% CPU
count(hpc_job_cpu_usage_percent > 80)

# Job count by state
hpc_job_state_total

# Total jobs
sum(hpc_job_state_total)

# Node with highest CPU usage
topk(5, hpc_node_cpu_usage_percent)

# Total memory used across all nodes
sum(hpc_node_memory_usage_bytes) / 1024 / 1024 / 1024

# Nodes with more than 2 running jobs
hpc_node_job_count > 2
```

## Development

### Project Structure

```
├── cmd/
│   ├── server/           # Main application
│   └── mockserver/       # OpenAPI mock server
├── config/               # Configuration files
│   ├── openapi.yaml      # OpenAPI 3.0 specification
│   ├── prometheus.yml    # Prometheus scrape config
│   └── oapi-codegen-*.yaml
├── docs/                 # Documentation
│   ├── architecture.md   # System design
│   ├── api-reference.md  # API documentation
│   └── development.md    # Development guide
├── internal/
│   ├── api/              # HTTP handlers and generated code
│   ├── storage/          # Database layer (SQLite + PostgreSQL)
│   ├── collector/        # Background metric collector
│   └── metrics/          # Prometheus exporter
├── docker-compose.yml
└── Dockerfile
```

### Generate API Code

```bash
go generate ./...
```

### Running Tests

```bash
go test ./...
./scripts/coverage.sh
```

The coverage script excludes non-testable packages (cmd/* and generated API server/types) from coverage totals.
The current test coverage is 91.2%.

To run the concurrent job stress test:

```bash
STRESS_TEST=1 go test ./internal/storage -run TestStressConcurrentJobs
```

### Mock Server

```bash
go build -o mockserver ./cmd/mockserver
./mockserver
```

## Documentation

- [Architecture](docs/architecture.md) - System design and component overview
- [API Reference](docs/api-reference.md) - Detailed endpoint documentation
- [Development Guide](docs/development.md) - Setup and contribution guidelines

## License

Apache 2.0 License. See [LICENSE](LICENSE) for details.
