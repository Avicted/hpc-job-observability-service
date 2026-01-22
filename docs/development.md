# Development Guide

This document covers development setup, workflows, and guidelines for contributing to the HPC Job Observability Service.

## Prerequisites

- Go 1.22 or later
- Docker and Docker Compose (for integration testing)
- oapi-codegen (for API code generation)

## Project Structure

```
hpc-job-observability-service/
├── cmd/
│   └── server/                             # Main application entry point
├── config/                                 # Configuration files
│   ├── openapi/                            # OpenAPI specifications
│   │   ├── service/                        # Service API spec
│   │   └── slurm/                          # Slurm REST API spec
│   ├── grafana/                            # Grafana dashboards
│   ├── slurm/                              # Slurm container config
│   └── prometheus/                         # Prometheus config
├── docs/                                   # Documentation
├── internal/
│   ├── api/                                # HTTP handlers and generated types
│   ├── cgroup/                             # Linux cgroups v2 metric collection
│   ├── collector/                          # Background metric collector
│   ├── e2e/                                # End-to-end integration tests
│   ├── gpu/                                # NVIDIA/AMD GPU metric collection
│   ├── metrics/                            # Prometheus exporter
│   ├── scheduler/                          # Scheduler abstraction layer
│   ├── slurmclient/                        # Generated Slurm REST API client
│   └── storage/                            # Database layer (PostgreSQL)
├── scripts/                                # Utility scripts
│   ├── slurm/                              # Prolog/epilog scripts
│   ├── coverage.sh
│   ├── filter-slurm-openapi.py
│   └── fix-slurm-openapi-types.py
├── Dockerfile
├── docker-compose.yml
├── go.mod
└── README.md
```

## Getting Started

### Clone and Build

```bash
git clone <repository-url>
cd hpc-job-observability-service

# Download dependencies
go mod download

# Build binaries
go build -o server ./cmd/server
```

### Run Locally

```bash
# Run with PostgreSQL
DATABASE_URL="postgres://user:pass@localhost/hpc?sslmode=disable" ./server

# Run with demo data (mock backend only)
SEED_DEMO=true SCHEDULER_BACKEND=mock DATABASE_URL="postgres://user:pass@localhost/hpc?sslmode=disable" ./server
```

### Run with Docker Compose

```bash
# Start the full stack with Slurm integration
docker-compose --profile slurm up --build --force-recreate

# Or start only the Slurm container (for testing scheduler module)
docker-compose --profile slurm up slurm

# (Optional) Seed demo data when using mock backend
# Note: demo seeding is ignored when SCHEDULER_BACKEND=slurm
SEED_DEMO=true SCHEDULER_BACKEND=mock docker-compose up --build

# Stop
docker-compose down
```

#### Troubleshooting: demo data persists

If you previously ran with the mock backend or `SEED_DEMO=true`, the PostgreSQL data is stored in the `postgres_data` volume. When you switch to `SCHEDULER_BACKEND=slurm`, you may still see old demo jobs until the volume is cleared.

To reset to a clean database:

```bash
docker-compose down -v
docker volume rm <project>_postgres_data  # only if it still exists
docker-compose --profile slurm up --build --force-recreate
```

#### Troubleshooting: network not found

If you see an error like “failed to set up container networking: network … not found”, a Compose-managed network was likely removed while containers still referenced it.

Fix by recreating the stack and its network:

```bash
docker-compose down
docker network rm <project>_hpc-network  # only if it still exists
docker-compose --profile slurm up --build --force-recreate
```

Notes:
- If you manually deleted the network, add `--force-recreate` to ensure containers are rebuilt against the new network.
- Compose creates a project-scoped network named `<project>_hpc-network`.
- Keep a consistent project name if you use `COMPOSE_PROJECT_NAME` or `-p`.
- Avoid `docker start` on Compose-managed containers; use `docker-compose up` instead.

## Slurm Integration Testing

The project includes a complete Slurm cluster setup for integration testing. This allows you to test the `SlurmJobSource` against real Slurm services without installing Slurm on your host system.

### Slurm Readiness Check

When `SCHEDULER_BACKEND=slurm`, the service now waits (up to 60 seconds) for `slurmrestd` to become reachable before starting. If it cannot reach `SLURM_BASE_URL`, the service exits with a clear error message indicating that the Slurm container is not ready.

If you see this error, ensure you started Docker Compose with the `slurm` profile and wait for the slurm container to be healthy:

```bash
docker-compose --profile slurm up --build --force-recreate
docker-compose --profile slurm ps
```

### Job Lifecycle Events

With the Slurm backend, jobs are tracked via prolog/epilog lifecycle events:

| Event | Endpoint | When | Action |
|-------|----------|------|--------|
| Job Started | `POST /v1/events/job-started` | Prolog runs | Creates job with RUNNING state |
| Job Finished | `POST /v1/events/job-finished` | Epilog runs | Updates job with final state |

State determination:
- Signal 9 (SIGKILL) or 15 (SIGTERM) = cancelled
- Exit code non-zero = failed
- Exit code 0 = completed

With the mock backend:
- Jobs are created manually via API or demo data
- Set `SEED_DEMO=true` to seed 100 demo jobs

### Architecture

The Slurm integration testing stack uses a single container that bundles:

- **slurmctld** - Slurm controller daemon
- **slurmd** - Compute daemon
- **slurmdbd** - Database daemon for accounting
- **slurmrestd** - REST API daemon (exposed on port 6820)

This single-container setup is intended for local development and testing.

### Quick Start

Use the commands in “Run with Docker Compose” above. For Slurm-only testing, you can start just the Slurm container:

```bash
docker-compose --profile slurm up slurm
```

### Environment Configuration

When using Slurm, set these environment variables in your `.env` file:

```bash
# Required: Select slurm backend
SCHEDULER_BACKEND=slurm

# Required: Point to slurmrestd
# Local/native runs: http://localhost:6820
# Docker Compose:     http://slurm:6820 (container hostname)
SLURM_BASE_URL=http://localhost:6820

# Optional: API version (default: v0.0.37 for the test container)
SLURM_API_VERSION=v0.0.37

# Optional: Auth token (depends on slurmrestd auth config)
SLURM_AUTH_TOKEN=
```

### Node Metrics

When running with the Slurm backend, node-level metrics are pulled from the
Slurm nodes API and exported to Prometheus. This provides node load and capacity
metrics even when no jobs are running, similar to the mock backend.

Key metrics include:
- `hpc_node_cpu_load`
- `hpc_node_cpu_usage_percent`
- `hpc_node_memory_usage_bytes`
- `hpc_node_total_cpus`
- `hpc_node_total_memory_bytes`
- `hpc_node_state`

### Audit System

All job changes are logged to the `job_audit_events` table for traceability:

| Column | Description |
|--------|-------------|
| job_id | Job identifier |
| change_type | Type of change (create, update, delete) |
| changed_at | Timestamp of the change |
| changed_by | Actor making the change |
| source | Data source |
| correlation_id | Groups related operations |
| job_snapshot | Complete job state as JSONB |

**changed_by values:**
- `slurm-prolog` - Job created by prolog script
- `slurm-epilog` - Job updated by epilog script
- `collector` - Metrics updated by collector
- `api` - Manual change via REST API

**Correlation IDs** group related operations for traceability. This enables:
- Tracing job lifecycle events (prolog to epilog to collector)
- Debugging job state issues
- Auditing and compliance

Query audit events:
```sql
-- View recent audit events
SELECT job_id, change_type, changed_by, correlation_id, changed_at
FROM job_audit_events ORDER BY changed_at DESC LIMIT 20;

-- Find all events for a specific job
SELECT * FROM job_audit_events
WHERE job_id = 'slurm-123' ORDER BY changed_at;

-- View changes by source
SELECT changed_by, COUNT(*) FROM job_audit_events GROUP BY changed_by;
```

### Running Unit Tests

The unit tests for Slurm use mock clients to simulate slurmrestd responses:

```bash
# Run all tests including Slurm unit tests
go test ./...

# Run only Slurm scheduler tests
go test ./internal/scheduler/... -v

# Run with coverage
./scripts/coverage.sh
```

### Running End-to-End Integration Tests

The project includes comprehensive E2E tests that run against a real Slurm cluster.
These tests use the **event-based architecture** where:
- Slurm prolog creates jobs via `/v1/events/job-started`
- Slurm epilog updates jobs via `/v1/events/job-finished`
- The collector gathers real-time metrics from running jobs

**Prerequisites:**
- Docker and Docker Compose
- Slurm stack running via `docker-compose --profile slurm up -d`

**Running E2E Tests:**

```bash
# 1. Start the Slurm stack (PostgreSQL + Slurm + App)
docker-compose --profile slurm up -d

# 2. Wait for services to be healthy
docker-compose --profile slurm ps

# 3. Run E2E tests with the slurm_e2e build tag
DATABASE_URL="postgres://hpc:CHANGE_ME_IN_PRODUCTION@localhost:5432/hpc_jobs?sslmode=disable" \
  go test ./internal/e2e/... -tags=slurm_e2e -v

# 4. Run specific E2E test
DATABASE_URL="postgres://hpc:CHANGE_ME_IN_PRODUCTION@localhost:5432/hpc_jobs?sslmode=disable" \
  go test ./internal/e2e/... -tags=slurm_e2e -v -run TestJobLifecycle_Completed

# 5. Stop Slurm when done
docker-compose --profile slurm down
```

**What the E2E tests cover:**

*Lifecycle Tests (lifecycle_e2e_test.go):*
- `TestJobLifecycle_Completed` - Jobs that complete successfully (exit code 0)
- `TestJobLifecycle_Failed` - Jobs that fail with various exit codes (1, 2, 42, 126, 127, 128, 255)
- `TestJobLifecycle_Cancelled` - Jobs cancelled by user (SIGTERM/SIGKILL detection)
- `TestJobLifecycle_Timeout` - Jobs killed by Slurm time limit
- `TestJobLifecycle_Signal` - Jobs terminated by various signals (SIGSEGV, SIGABRT, etc.)
- `TestJobLifecycle_Concurrent` - Multiple concurrent jobs in different states

*Basic E2E Tests (slurm_e2e_test.go):*
- `TestSlurmE2E_SlurmConnectivity` - Verify connectivity to slurmrestd and list nodes
- `TestSlurmE2E_AuditTableCreation` - Verify job_audit_events table schema
- `TestSlurmE2E_AuditEventCreation` - Verify audit events with changed_by, source, correlation_id

**Note:** E2E tests are skipped automatically if Slurm or PostgreSQL is not available,
allowing `go test ./...` to run without Docker.

### Manual Integration Testing

With the Slurm stack running, you can submit real jobs and verify the integration:

```bash
# 1. Start Slurm stack
docker-compose --profile slurm up -d

# 2. Wait for services to be healthy
docker-compose --profile slurm ps

# 3. Submit a test job
docker-compose exec slurm sbatch --wrap="echo 'Hello from Slurm'; sleep 30"

# 4. Check job status via slurmrestd
curl http://localhost:6820/slurm/v0.0.37/jobs/

# 5. Verify node status
docker-compose exec slurm sinfo

# 6. Check job queue
docker-compose exec slurm squeue

# 7. View OpenAPI spec
curl http://localhost:6820/openapi/v3 | head -100
```

### Testing the Service with Slurm

The service uses an **event-based architecture** where Slurm prolog/epilog scripts
notify the service of job lifecycle events. To test the full integration:

```bash
# Create .env with Slurm config
cat > .env << 'EOF'
SCHEDULER_BACKEND=slurm
SLURM_BASE_URL=http://slurm:6820
SLURM_API_VERSION=v0.0.37
DATABASE_URL=postgres://hpc:hpc_password@postgres:5432/hpc_jobs?sslmode=disable
EOF

# Start everything (includes prolog/epilog configuration)
docker-compose --profile slurm up --build --force-recreate

# The service receives job events from:
# - Prolog: /v1/events/job-started (creates job with RUNNING state)
# - Epilog: /v1/events/job-finished (updates job with exit code/signal)
# - Collector: Updates running jobs with real-time metrics
```

### Stopping Slurm Services

```bash
# Stop all services including Slurm
docker-compose --profile slurm down

# Stop and remove volumes (clean slate)
docker-compose --profile slurm down -v
```

### Troubleshooting Slurm Integration

**Slurm container not starting:**
```bash
# Check logs
docker-compose --profile slurm logs slurm

# Verify services inside the container
docker-compose exec slurm sinfo
```

**slurmrestd connection refused:**
```bash
# Check if slurmrestd is running
docker-compose exec slurm ps aux | grep slurmrestd

# Test the OpenAPI endpoint
curl http://localhost:6820/openapi/v3 | head -20
```

**Jobs stuck in pending:**
```bash
# Check node status
docker-compose exec slurm sinfo

# Check reasons for pending
docker-compose exec slurm squeue -t pending -o "%i %j %T %r"
```

## Development Workflow

### API-First Development

This project follows API-first development. The OpenAPI specification is the source of truth.

**To make API changes:**

1. Edit `config/openapi/service/openapi.yaml`
2. Regenerate code:
   ```bash
   go generate ./...
   ```
3. Update handler implementation in `internal/api/handler.go`
4. Update tests in `internal/api/handler_test.go`

### Code Generation

The project uses `oapi-codegen` to generate Go types and server interfaces from OpenAPI specs.

**Install oapi-codegen:**

```bash
go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@latest
```

**Regenerate all code:**

```bash
go generate ./...
```

This runs the `//go:generate` directives in:
- `internal/api/types/generate.go` - Generates API types
- `internal/api/server/generate.go` - Generates server interface
- `internal/slurmclient/generate.go` - Generates Slurm REST API client

### Slurm Client Code Generation

The project uses a runtime-free Go client generated from the official Slurm OpenAPI specification.
This provides type-safe access to slurmrestd endpoints.

**Files:**
- `config/openapi/slurm/slurm-openapi.json` - Full Slurm OpenAPI spec (fetched from slurmrestd)
- `config/openapi/slurm/slurm-openapi-v0.0.37.json` - Filtered spec (v0.0.37 paths only)
- `config/openapi/slurm/oapi-codegen-slurm-client.yaml` - oapi-codegen configuration
- `internal/slurmclient/client.gen.go` - Generated client code
- `scripts/filter-slurm-openapi.py` - Script to filter spec to a single API version
- `scripts/fix-slurm-openapi-types.py` - Script to fix type issues in the spec

**To update the Slurm OpenAPI spec:**

1. Start the Slurm container:
   ```bash
   docker-compose --profile slurm up -d slurm
   ```

2. Fetch the latest OpenAPI spec:
   ```bash
   curl -s http://localhost:6820/openapi/v3 > config/openapi/slurm/slurm-openapi.json
   ```

3. Filter to a single API version (to avoid duplicate operation IDs):
   ```bash
   ./scripts/filter-slurm-openapi.py --version v0.0.37
   ```

4. Fix known type mismatches in the filtered spec (use this whenever updating the Slurm spec):
    ```bash
    ./scripts/fix-slurm-openapi-types.py \
      --input config/openapi/slurm/slurm-openapi-v0.0.37.json
    ```

5. Regenerate the client:
   ```bash
   go generate ./internal/slurmclient/...
   ```

**To use a different API version:**

```bash
# Filter to a different version
./scripts/filter-slurm-openapi.py --version v0.0.37

# Update the generate.go to point to the new filtered spec
# Then regenerate
go generate ./internal/slurmclient/...
```

**Why runtime-free client?**

The generated client uses oapi-codegen's runtime-free mode, which:
- Generates typed request/response helpers without extra dependencies
- Lets you control HTTP behavior, auth, retries, and middleware yourself
- Keeps the dependency footprint minimal
- Works well with the existing `SlurmJobSource` HTTP handling

### Running Tests

```bash
# Run all tests
go test ./...

# Run with verbose output
go test ./... -v

# Run with coverage
./scripts/coverage.sh

# View coverage report
go tool cover -html=coverage.out
```

The coverage script excludes cmd/* and generated API server/types packages from totals.

### Linting

```bash
# Install golangci-lint
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Run linter
golangci-lint run
```

## VS Code Tasks

The project includes VS Code tasks in `.vscode/tasks.json`:

| Task | Description |
|------|-------------|
| Build Server | Build the main server binary |
| Build All | Build all packages |
| Run Server | Run server locally |
| Run Server (with demo data) | Run server with demo data |
| Test All | Run all tests |
| Test with Coverage | Run tests with coverage report |
| Generate OpenAPI Code | Regenerate code from OpenAPI |
| Docker Compose Up | Start Docker Compose stack |
| Docker Compose Down | Stop Docker Compose stack |
| Lint | Run golangci-lint |

Access via: `Ctrl+Shift+P` > `Tasks: Run Task`

## Code Guidelines

### Package Organization

- `cmd/` - Entry points only, minimal logic
- `internal/` - Private packages, not importable externally
- `config/` - Configuration files (non-Go)
- `docs/` - Documentation

### Error Handling

Return errors with context:

```go
if err != nil {
    return fmt.Errorf("failed to create job: %w", err)
}
```

Use sentinel errors for expected conditions:

```go
var ErrJobNotFound = errors.New("job not found")
```

### Logging

Use the standard library `log` package:

```go
log.Printf("Starting server on %s:%d", host, port)
```

### Testing

- Write table-driven tests
- Use subtests for related cases
- Mock dependencies via interfaces

```go
func TestJobOperations(t *testing.T) {
    tests := []struct {
        name    string
        input   string
        want    string
        wantErr bool
    }{
        {"valid job", "job-001", "job-001", false},
        {"empty id", "", "", true},
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // test logic
        })
    }
}
```

## Database Migrations

Migrations run automatically on startup. The storage layer handles schema creation.

Tables are created in `internal/storage/postgres.go`.

## Adding New Features

### Adding a New Endpoint

1. Add path and operation to `config/openapi.yaml`
2. Define request/response schemas
3. Run `go generate ./...`
4. Implement handler method in `internal/api/handler.go`
5. Add tests in `internal/api/handler_test.go`

### Adding a New Metric

1. Define metric in `internal/metrics/exporter.go`
2. Register with Prometheus registry
3. Update metric in relevant handlers/collector
4. Document in `docs/api-reference.md`

### Adding a New Storage Backend

1. Create new file in `internal/storage/`
2. Implement the `Storage` interface
3. Add factory function
4. Update `cmd/server/main.go` to support new backend

## Debugging

### Enable Debug Logging

Set environment variable:

```bash
DEBUG=1 ./server
```

### Inspect Database (PostgreSQL)

```bash
docker-compose exec postgres psql -U hpc -d hpc_jobs
\dt
SELECT * FROM jobs;
```

### Check Prometheus Metrics

```bash
curl http://localhost:8080/metrics | grep hpc_
```

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

## Release Process

1. Update version in `internal/api/handler.go` (health endpoint)
2. Update CHANGELOG if present
3. Create git tag
4. Build Docker image
5. Push to registry
