# Contributing to HPC Job Observability Service

Thank you for your interest in contributing to this project. This document provides guidelines and information for contributors.

## Getting Started

1. Fork the repository
2. Clone your fork locally
3. Set up the development environment (see [Development Guide](docs/development.md))
4. Create a feature branch from `main`

## Development Setup

```bash
# Clone and setup
git clone <your-fork-url>
cd hpc-job-observability-service

# Install dependencies
go mod download

# Copy environment file
cp .env.example .env

# Run tests to verify setup
go test ./...
```

## Making Changes

### Code Style

- Follow standard Go conventions and `gofmt`
- Run `golangci-lint run` before committing
- Write clear, descriptive commit messages
- Add tests for new functionality

### API Changes

This project follows API-first development. To make API changes:

1. Edit `config/openapi/service/openapi.yaml`
2. Regenerate code: `go generate ./...`
3. Update handler implementation in `internal/api/handler.go`
4. Update tests and documentation

### Testing

```bash
# Run all unit tests
go test ./...

# Run with coverage
./scripts/coverage.sh

# Run linter
golangci-lint run

# Run E2E tests (requires Docker)
docker-compose --profile slurm up -d postgres slurm
DATABASE_URL="postgres://hpc:CHANGE_ME_IN_PRODUCTION@localhost:5432/hpc_jobs?sslmode=disable" \
  go test ./internal/e2e/... -tags=slurm_e2e -v
```

All tests must pass before submitting a pull request.

## Pull Request Process

1. Update documentation if you are changing functionality
2. Add or update tests as appropriate
3. Ensure all tests pass and linting is clean
4. Update the README if needed
5. Submit your pull request with a clear description

## Reporting Issues

When reporting issues, please include:

- A clear description of the problem
- Steps to reproduce
- Expected vs actual behavior
- Environment details (OS, Go version, etc.)

## Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help maintain a welcoming community

## Questions?

If you have questions, feel free to open an issue for discussion.

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
