# HPC Job Observability Service

A microservice for tracking and monitoring HPC (High Performance Computing) job resource utilization with Prometheus metrics export.

> This project is a proof of concept for building an observability service for HPC job schedulers like Slurm. It provides a RESTful API to manage jobs and record resource usage metrics (CPU, memory, GPU) over time. The service exports metrics in Prometheus format for easy integration with monitoring systems.

## Features

- **Slurm Integration**: Native support for Slurm via its REST API
- **Job Management**: Create, update, list, and delete HPC jobs
- **Resource Metrics**: Track CPU, memory, and GPU usage over time
- **Prometheus Integration**: Export metrics in Prometheus format with best practices
- **Database Support**: PostgreSQL storage backend
- **Demo Data**: Seed sample data for testing and demonstration
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

# Run with PostgreSQL
DATABASE_URL="postgres://user:pass@localhost/hpc?sslmode=disable" ./server

# Run with demo data (mock backend only)
SEED_DEMO=true SCHEDULER_BACKEND=mock DATABASE_URL="postgres://user:pass@localhost/hpc?sslmode=disable" ./server
```
**Security Note:** Never commit `.env` files containing secrets to version control. The `.env` file is already in `.gitignore`.

### Running with Docker Compose

```bash
# Copy environment file (required for secrets)
cp .env.example .env
# Edit .env with your settings (especially passwords!)

# Start the full stack with Slurm integration
docker-compose --profile slurm up --build --force-recreate

# (Optional) Seed demo data when using mock backend
# Note: demo seeding is ignored when SCHEDULER_BACKEND=slurm
SEED_DEMO=true SCHEDULER_BACKEND=mock docker-compose up --build

# Or start only the Slurm container (for testing scheduler module)
docker-compose --profile slurm up slurm

# View Prometheus at http://localhost:9090
# View Grafana at http://localhost:3000 (credentials from .env)
# View app at http://localhost:8080
```



## Grafana Dashboards

The project ships with a pre-provisioned Grafana dashboard. Below are example views from the example dashboard:

**Job Metrics**

![Job metrics dashboard](docs/grafana_job_metrics.png)

**Node Metrics (overview)**

![Node metrics dashboard overview](docs/grafana_node_metrics_01.png)

**Node Metrics (detail)**

![Node metrics dashboard detail](docs/grafana_node_metrics_02.png)


## Configuration

#### Server Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | Server port |
| `HOST` | `0.0.0.0` | Server host |

#### Database Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgres://...` | PostgreSQL connection string |
| `POSTGRES_USER` | `hpc` | PostgreSQL username (Docker) |
| `POSTGRES_PASSWORD` | - | PostgreSQL password (Docker) |
| `POSTGRES_DB` | `hpc_jobs` | PostgreSQL database name (Docker) |

#### Metrics & Grafana Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `METRICS_RETENTION_DAYS` | `7` | Days to retain metrics before cleanup |
| `GF_SECURITY_ADMIN_USER` | `admin` | Grafana admin username |
| `GF_SECURITY_ADMIN_PASSWORD` | - | Grafana admin password |

### Demo Data

Use the `SEED_DEMO` environment variable to seed demo data on startup (mock backend only).


## Development
- [Development Guide](docs/development.md)


### Running Tests

- [Unit testing](docs/development.md#running-unit-tests)
- [End-to-end testing](docs/development.md#running-end-to-end-integration-tests)


## Scheduler Integration (SLURM)

For detailed integration documentation, see [Architecture - Scheduler Integration](docs/architecture.md#scheduler-integration).

## Documentation

- [Architecture](docs/architecture.md) - System design and component overview
- [API Reference](docs/api-reference.md) - Detailed endpoint documentation
- [Development Guide](docs/development.md) - Setup and contribution guidelines

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Apache 2.0 License. See [LICENSE](LICENSE) for details.
