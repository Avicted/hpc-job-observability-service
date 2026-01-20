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
