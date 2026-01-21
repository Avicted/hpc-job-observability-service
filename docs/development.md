# Development Guide

This document covers development setup, workflows, and guidelines for contributing to the HPC Job Observability Service.

## Prerequisites

- Go 1.25 or later
- Docker and Docker Compose (for integration testing)
- oapi-codegen (for API code generation)

## Project Structure

```
hpc-job-observability-service/
├── cmd/
│   ├── server/             # Main application entry point
│   │   └── main.go
│   └── mockserver/         # OpenAPI mock server
│       └── main.go
├── config/                 # Configuration files
│   ├── openapi.yaml        # OpenAPI 3.0 specification
│   ├── oapi-codegen-types.yaml
│   ├── oapi-codegen-server.yaml
│   └── prometheus.yml
├── docs/                   # Documentation
│   ├── architecture.md
│   ├── api-reference.md
│   └── development.md
├── internal/
│   ├── api/
│   │   ├── types/          # Generated API types
│   │   │   ├── generate.go
│   │   │   └── types.gen.go
│   │   ├── server/         # Generated server interface
│   │   │   ├── generate.go
│   │   │   └── server.gen.go
│   │   ├── handler.go      # API handler implementation
│   │   └── handler_test.go
│   ├── collector/          # Background metric collector
│   │   ├── collector.go
│   │   └── collector_test.go
│   ├── metrics/            # Prometheus exporter
│   │   └── exporter.go
│   ├── scheduler/          # Scheduler abstraction layer
│   │   ├── scheduler.go    # Interface and types
│   │   ├── mock.go         # Mock job source
│   │   ├── slurm.go        # Slurm job source
│   │   └── *_test.go
│   ├── syncer/             # Job synchronization
│   │   ├── syncer.go       # Syncs jobs from scheduler to storage
│   │   └── syncer_test.go
│   └── storage/            # Database layer
│       ├── storage.go      # Interface and types
│       ├── sqlite.go       # SQLite implementation
│       ├── postgres.go     # PostgreSQL implementation
│       └── storage_test.go
├── .vscode/
│   └── tasks.json          # VS Code tasks
├── Dockerfile
├── docker-compose.yml
├── go.mod
├── go.sum
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
go build -o mockserver ./cmd/mockserver
```

### Run Locally

```bash
# Run with SQLite (default)
./server

# Run with demo data
./server --seed-demo

# Run with PostgreSQL
DATABASE_TYPE=postgres DATABASE_URL="postgres://user:pass@localhost/hpc?sslmode=disable" ./server
```

### Run with Docker Compose

```bash
# Start full stack (app + PostgreSQL + Prometheus)
docker-compose up --build

# Start with mock server
docker-compose --profile mock up

# Stop and clean up
docker-compose down -v
```

## Slurm Integration Testing

The project includes a complete Slurm cluster setup for integration testing. This allows you to test the `SlurmJobSource` against real Slurm services without installing Slurm on your host system.

### Job Synchronization

When using the `slurm` backend, the service automatically syncs jobs from the Slurm scheduler to the database:

- **Sync Interval**: Jobs are synced every 30 seconds
- **Initial Delay**: First sync occurs 5 seconds after startup
- **Upsert Logic**: Jobs are created or updated based on their ID
- **Demo Data**: When using `slurm` backend, the `-seed-demo` flag is ignored (jobs come from Slurm)

When using the `mock` backend:
- **No Syncer**: Jobs must be created manually or via demo data
- **Demo Data**: The `-seed-demo` flag seeds 100 demo jobs

### Architecture

The Slurm integration testing stack includes:

- **slurmctld** - Slurm controller daemon
- **slurmd1, slurmd2** - Two compute nodes for running jobs
- **slurmrestd** - REST API daemon (exposed on port 6820)
- **slurmdbd** - Database daemon for accounting
- **mysql** - MySQL database for Slurm accounting data

### Quick Start

```bash
# Start the full stack with Slurm integration
docker-compose --profile slurm up --build

# Or start Slurm services only (for testing scheduler module)
docker-compose --profile slurm up slurmctld slurmd1 slurmd2 slurmrestd slurmdbd mysql
```

### Environment Configuration

When using Slurm, set these environment variables in your `.env` file:

```bash
# Required: Select slurm backend
SCHEDULER_BACKEND=slurm

# Required: Point to slurmrestd
SLURM_BASE_URL=http://slurmrestd:6820

# Optional: API version (default: v0.0.44)
SLURM_API_VERSION=v0.0.44

# Optional: Auth token (depends on slurmrestd auth config)
SLURM_AUTH_TOKEN=
```

### Running Unit Tests

The existing unit tests for Slurm use `httptest.Server` to mock slurmrestd responses:

```bash
# Run all tests including Slurm unit tests
go test ./...

# Run only Slurm scheduler tests
go test ./internal/scheduler/... -v

# Run with coverage
./scripts/coverage.sh
```

### Running Integration Tests

With the Slurm stack running, you can submit real jobs and verify the integration:

```bash
# 1. Start Slurm stack
docker-compose --profile slurm up -d

# 2. Wait for services to be healthy
docker-compose --profile slurm ps

# 3. Submit a test job
docker-compose exec slurm sbatch --wrap="echo 'Hello from Slurm'; sleep 30"

# 4. Check job status via slurmrestd (uses slurmdb API v0.0.36)
curl http://localhost:6820/slurmdb/v0.0.36/jobs

# 5. Verify slurmctld is running
docker-compose exec slurm sinfo

# 6. Check job queue
docker-compose exec slurm squeue

# 7. View OpenAPI spec
curl http://localhost:6820/openapi/v3 | head -100
```

### Testing the Service with Slurm

To test the full integration (service + Slurm):

```bash
# Create .env with Slurm config
cat > .env << 'EOF'
SCHEDULER_BACKEND=slurm
SLURM_BASE_URL=http://slurmrestd:6820
SLURM_API_VERSION=v0.0.44
DATABASE_TYPE=postgres
DATABASE_URL=postgres://hpc:hpc_password@postgres:5432/hpc_jobs?sslmode=disable
EOF

# Start everything
docker-compose --profile slurm up --build

# The app will now use SlurmJobSource instead of MockJobSource
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

1. Edit `config/openapi.yaml`
2. Regenerate code:
   ```bash
   go generate ./...
   ```
3. Update handler implementation in `internal/api/handler.go`
4. Update tests in `internal/api/handler_test.go`

### Code Generation

The project uses `oapi-codegen` to generate Go types and server interfaces from the OpenAPI spec.

**Install oapi-codegen:**

```bash
go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@latest
```

**Regenerate code:**

```bash
go generate ./...
```

This runs the `//go:generate` directives in:
- `internal/api/types/generate.go` - Generates API types
- `internal/api/server/generate.go` - Generates server interface

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

The coverage script excludes cmd/* and generated API server/types packages from totals.
```

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
| Build Mock Server | Build the mock server binary |
| Build All | Build all packages |
| Run Server | Run server locally |
| Run Server (with demo data) | Run server with demo data |
| Run Mock Server | Run the mock server |
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

**SQLite Schema:**

Tables are created in `internal/storage/sqlite.go`:
- `jobs` - Job records
- `metrics` - Metric samples

**PostgreSQL Schema:**

Tables are created in `internal/storage/postgres.go` with the same structure.

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

### Inspect Database

**SQLite:**

```bash
sqlite3 hpc-jobs.db
.tables
SELECT * FROM jobs;
```

**PostgreSQL:**

```bash
docker-compose exec postgres psql -U hpc -d hpc_jobs
\dt
SELECT * FROM jobs;
```

### Check Prometheus Metrics

```bash
curl http://localhost:8080/metrics | grep hpc_
```

## Release Process

1. Update version in `internal/api/handler.go` (health endpoint)
2. Update CHANGELOG if present
3. Create git tag
4. Build Docker image
5. Push to registry
