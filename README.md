# HPC Job Observability Service

A microservice for tracking and monitoring HPC (High Performance Computing) job resource utilization with Prometheus metrics export.

> This project is a proof of concept for building an observability service for HPC job schedulers like Slurm. It provides a RESTful API to manage jobs and record resource usage metrics (CPU, memory, GPU) over time. The service exports metrics in Prometheus format for easy integration with monitoring systems.

## Features

- **Slurm Integration**: Event-based integration via prolog/epilog scripts
- **Real-time Job Tracking**: Jobs created instantly when Slurm starts them
- **Accurate State Detection**: Exit codes and signals captured for correct state mapping
- **Resource Metrics**: CPU, memory, and GPU usage collected via Linux cgroups v2
- **Prometheus Export**: Metrics in Prometheus format for Grafana dashboards
- **PostgreSQL Storage**: Persistent storage with full audit trail
- **GPU Support**: NVIDIA and AMD GPU metrics via vendor tools
- **Demo Mode**: Mock backend with sample data for testing

## Architecture

```
┌─────────────────┐                          ┌─────────────────┐
│   HPC Cluster   │   Lifecycle Events       │   Prometheus    │
│  (Slurm Jobs)   │ ─────────────────────┐   │   + Grafana     │
└─────────────────┘                      │   └────────┬────────┘
        │                                │            │ scrape
        │ prolog: job-started            ▼            ▼
        │ epilog: job-finished    ┌─────────────────────────┐
        │ collector: metrics      │  Observability Service  │
        └────────────────────────▶│     (Go + REST API)     │
                                  └───────────┬─────────────┘
                                              │
                                              ▼
                                  ┌─────────────────────────┐
                                  │      PostgreSQL         │
                                  │  (jobs, metrics, audit) │
                                  └─────────────────────────┘
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


## Slurm Integration

The service uses an event-based architecture for Slurm integration:

| Component | Endpoint | Purpose |
|-----------|----------|--------|
| Prolog script | `POST /v1/events/job-started` | Creates job when Slurm starts it |
| Epilog script | `POST /v1/events/job-finished` | Updates job with exit code/signal |
| Collector | `POST /v1/jobs/{id}/metrics` | Records CPU/memory/GPU metrics |

State detection uses exit codes and signals:
- Exit code 0 = completed
- Exit code non-zero = failed
- Signal 9 (SIGKILL) or 15 (SIGTERM) = cancelled

For detailed setup instructions, see:
- [Slurm Integration Guide](docs/slurm-integration.md)
- [Architecture](docs/architecture.md#scheduler-integration)

## Documentation

- [Architecture](docs/architecture.md) - System design and component overview
- [API Reference](docs/api-reference.md) - Detailed endpoint documentation
- [Development Guide](docs/development.md) - Setup and contribution guidelines

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Apache 2.0 License. See [LICENSE](LICENSE) for details.
