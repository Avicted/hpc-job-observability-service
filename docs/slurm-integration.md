# Slurm Integration Guide

This guide explains how to configure Slurm to integrate with the HPC Job Observability Service.

## Overview

The service uses an event-based architecture where Slurm prolog and epilog scripts notify the service of job lifecycle events. The collector then monitors running jobs using Linux cgroups v2.

**Data Flow:**
1. Job starts: Prolog script sends `POST /v1/events/job-started`
2. Job runs: Collector periodically reads cgroup metrics
3. Job ends: Epilog script sends `POST /v1/events/job-finished` with exit code/signal

**Benefits:**
- Real-time job tracking without API polling
- Accurate state detection via exit codes and signals
- Precise resource accounting via cgroups v2
- GPU support for NVIDIA and AMD

## Prerequisites

- Slurm 21.08+ with cgroup v2 support
- Linux kernel 5.0+ with cgroup v2 enabled
- Network access from compute nodes to the observability service
- For GPU monitoring: `nvidia-smi` (NVIDIA) or `rocm-smi` (AMD)

## Service Configuration

Set these environment variables for the observability service:

```bash
# Select Slurm backend
SCHEDULER_BACKEND=slurm

# Slurm REST API endpoint (for node metrics)
SLURM_BASE_URL=http://slurm:6820
SLURM_API_VERSION=v0.0.37

# Database connection
DATABASE_URL=postgres://hpc:password@postgres:5432/hpc_jobs?sslmode=disable
```

## Slurm Configuration

### 1. Enable cgroups in `slurm.conf`

Add or modify the following settings in `/etc/slurm/slurm.conf`:

```
# Enable cgroup plugin for resource isolation
ProctrackType=proctrack/cgroup
TaskPlugin=task/cgroup

# Enable prolog and epilog scripts
Prolog=/etc/slurm/prolog.d/*
Epilog=/etc/slurm/epilog.d/*
PrologSlurmctld=/etc/slurm/prolog.slurmctld.d/*
EpilogSlurmctld=/etc/slurm/epilog.slurmctld.d/*
```

### 2. Configure `cgroup.conf`

Create or modify `/etc/slurm/cgroup.conf`:

```
###
### Slurm cgroup.conf - Configuration for cgroup v2
###

# Enable cgroup v2 mode
CgroupPlugin=cgroup/v2

# Memory cgroup settings
ConstrainRAMSpace=yes
ConstrainSwapSpace=yes
AllowedRAMSpace=100
AllowedSwapSpace=0

# CPU cgroup settings
ConstrainCores=yes

# Device cgroup settings (for GPU isolation)
ConstrainDevices=yes

# Automatically release resources
CgroupReleaseAgentDir=/etc/slurm/cgroup

# cgroup mount point
CgroupMountpoint=/sys/fs/cgroup
```

### 3. Configure GPU support with `gres.conf`

For GPU nodes, configure `/etc/slurm/gres.conf`:

```
# NVIDIA GPUs
NodeName=gpu-node[01-10] Name=gpu Type=nvidia File=/dev/nvidia[0-3]

# AMD GPUs (if using)
# NodeName=amd-node[01-05] Name=gpu Type=amd File=/dev/dri/renderD[128-131]
```

Also add to `slurm.conf`:
```
GresTypes=gpu
```

## Prolog and Epilog Scripts

### Job Started Event (Prolog)

Copy the prolog script to `/etc/slurm/prolog.d/`:

```bash
cp scripts/slurm/prolog.sh /etc/slurm/prolog.d/50-observability.sh
chmod +x /etc/slurm/prolog.d/50-observability.sh
```

The prolog script:
- Detects the job's cgroup path
- Identifies allocated GPUs and their vendor
- Expands node list for multi-node jobs
- Sends a `job-started` event to the observability service

### Job Finished Event (Epilog)

Copy the epilog script to `/etc/slurm/epilog.d/`:

```bash
cp scripts/slurm/epilog.sh /etc/slurm/epilog.d/50-observability.sh
chmod +x /etc/slurm/epilog.d/50-observability.sh
```

The epilog script:
- Collects final resource usage statistics
- Sends a `job-finished` event with exit code and final metrics

### Configuration

Both scripts read the observability service URL from environment or default:

```bash
# In /etc/slurm/slurm.conf or node-specific environment
export OBSERVABILITY_URL="http://observability-service:8080"
```

Or set in the scripts directly:
```bash
OBSERVABILITY_URL="${OBSERVABILITY_URL:-http://localhost:8080}"
```

## Multi-Node Jobs

For multi-node jobs, the prolog and epilog run on each allocated node. The scripts:

1. Detect whether they're running on the first node (head node)
2. Only the first node sends the lifecycle event with the full node list
3. Other nodes remain silent to avoid duplicate events

This is handled automatically by the scripts using `SLURM_NODELIST` and `SLURM_NODEID`.

## Verifying the Setup

### 1. Check cgroup v2 is enabled

```bash
mount | grep cgroup2
# Should show: cgroup2 on /sys/fs/cgroup type cgroup2 ...
```

### 2. Verify Slurm cgroup integration

```bash
# Submit a test job
srun --job-name=test hostname

# Check if cgroup was created
ls /sys/fs/cgroup/system.slice/slurmstepd.scope/job_*/
```

### 3. Test prolog/epilog manually

```bash
# Set up test environment
export SLURM_JOB_ID=99999
export SLURM_JOB_USER=$USER
export SLURM_NODELIST=$(hostname)
export SLURM_JOB_NUM_NODES=1
export SLURM_NODEID=0

# Run prolog
/etc/slurm/prolog.d/50-observability.sh

# Check if event was received
curl http://localhost:8080/v1/jobs/slurm-99999

# Run epilog
export SLURM_JOB_EXIT_CODE=0
/etc/slurm/epilog.d/50-observability.sh
```

### 4. Check observability service logs

```bash
# If running with docker-compose
docker-compose logs observability-service

# Check for received events
curl http://localhost:8080/v1/jobs | jq
```

## GPU Monitoring

### NVIDIA GPUs

The service uses `nvidia-smi` to collect GPU metrics:

```bash
# Verify nvidia-smi is available
which nvidia-smi

# Test GPU detection
nvidia-smi --query-gpu=gpu_name,utilization.gpu,memory.used --format=csv
```

### AMD GPUs

The service uses `rocm-smi` for AMD GPU metrics:

```bash
# Verify rocm-smi is available
which rocm-smi

# Test GPU detection
rocm-smi --showuse --json
```

## Prometheus Metrics

The observability service exposes metrics at `/metrics`:

### Job Metrics
- `hpc_job_runtime_seconds{job_id, user, node}` - Job runtime
- `hpc_job_cpu_usage_percent{job_id, user, node}` - CPU usage
- `hpc_job_memory_usage_bytes{job_id, user, node}` - Memory usage
- `hpc_job_gpu_usage_percent{job_id, user, node}` - Average GPU usage

### Per-GPU Device Metrics
- `hpc_job_gpu_device_utilization_percent{job_id, node, gpu_vendor, gpu_id}` - GPU utilization
- `hpc_job_gpu_device_memory_used_bytes{job_id, node, gpu_vendor, gpu_id}` - GPU memory
- `hpc_job_gpu_device_power_watts{job_id, node, gpu_vendor, gpu_id}` - Power consumption
- `hpc_job_gpu_device_temperature_celsius{job_id, node, gpu_vendor, gpu_id}` - Temperature

### Node Metrics
- `hpc_node_cpu_usage_percent{node}` - Node CPU usage
- `hpc_node_memory_usage_bytes{node}` - Node memory usage
- `hpc_node_job_count{node}` - Running jobs per node

## Troubleshooting

### Prolog/Epilog not running

1. Check script permissions:
   ```bash
   ls -la /etc/slurm/prolog.d/
   # Should be executable
   ```

2. Check Slurm logs:
   ```bash
   journalctl -u slurmctld -f
   journalctl -u slurmd -f
   ```

3. Verify prolog/epilog paths in `slurm.conf`

### Cgroup not found for jobs

1. Ensure cgroup v2 is the unified hierarchy:
   ```bash
   cat /proc/mounts | grep cgroup
   ```

2. Check Slurm cgroup configuration:
   ```bash
   scontrol show config | grep -i cgroup
   ```

3. Verify cgroup plugin is loaded:
   ```bash
   slurmd -C 2>&1 | grep cgroup
   ```

### GPU metrics not collected

1. Verify GPU tools are in PATH for Slurm:
   ```bash
   sudo -u slurm which nvidia-smi
   sudo -u slurm which rocm-smi
   ```

2. Check GRES configuration:
   ```bash
   scontrol show node | grep Gres
   ```

3. Ensure GPU isolation is working:
   ```bash
   srun --gres=gpu:1 nvidia-smi
   ```

### Network connectivity issues

1. Test connectivity from compute nodes:
   ```bash
   curl -v http://observability-service:8080/v1/health
   ```

2. Check firewall rules allow outbound connections

3. Verify DNS resolution for service hostname

## Security Considerations

1. **Authentication**: Consider adding authentication to the observability service API
2. **Network isolation**: Run the service on an internal network
3. **TLS**: Use HTTPS for production deployments
4. **Audit logging**: The service logs all job events for auditing
