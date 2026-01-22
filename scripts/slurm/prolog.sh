#!/bin/bash
# =============================================================================
# Slurm Prolog Script - Job Started Event Emitter
# =============================================================================
#
# This script is called by Slurm when a job starts on a compute node.
# It emits a job_started event to the HPC Job Observability Service.
#
# Installation:
#   1. Copy this script to /etc/slurm/prolog.d/job-observability.sh
#   2. Make it executable: chmod +x /etc/slurm/prolog.d/job-observability.sh
#   3. Configure Slurm to use prolog scripts in slurm.conf:
#        Prolog=/etc/slurm/prolog.d/*
#      Or directly:
#        Prolog=/etc/slurm/prolog.d/job-observability.sh
#
# Configuration (via environment variables or config file):
#   OBSERVABILITY_API_URL - Base URL of the observability service
#   OBSERVABILITY_TIMEOUT - HTTP timeout in seconds (default: 5)
#
# =============================================================================

set -e

# Configuration
OBSERVABILITY_API_URL="${OBSERVABILITY_API_URL:-http://localhost:8080}"
OBSERVABILITY_TIMEOUT="${OBSERVABILITY_TIMEOUT:-5}"
LOG_FILE="/var/log/slurm/observability-prolog.log"

# Ensure log directory exists
mkdir -p "$(dirname "$LOG_FILE")"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [PROLOG] $*" >> "$LOG_FILE"
}

log_error() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [PROLOG] ERROR: $*" >> "$LOG_FILE"
}

# Function to detect GPU vendor
detect_gpu_vendor() {
    local vendor="none"
    
    # Check for NVIDIA GPUs
    if command -v nvidia-smi &> /dev/null; then
        if nvidia-smi -L 2>/dev/null | grep -q "GPU"; then
            vendor="nvidia"
        fi
    fi
    
    # Check for AMD GPUs (only if not already nvidia)
    if [ "$vendor" = "none" ] && command -v rocm-smi &> /dev/null; then
        if rocm-smi --showproductname 2>/dev/null | grep -qi "card\|gpu"; then
            vendor="amd"
        fi
    fi
    
    echo "$vendor"
}

# Function to get GPU device IDs for NVIDIA
get_nvidia_gpu_devices() {
    if [ -n "$SLURM_JOB_GPUS" ]; then
        # SLURM_JOB_GPUS contains the GPU indices
        local devices=""
        for idx in $(echo "$SLURM_JOB_GPUS" | tr ',' ' '); do
            local uuid=$(nvidia-smi -i "$idx" --query-gpu=uuid --format=csv,noheader,nounits 2>/dev/null | tr -d ' ')
            if [ -n "$uuid" ]; then
                if [ -n "$devices" ]; then
                    devices="$devices,\"$uuid\""
                else
                    devices="\"$uuid\""
                fi
            fi
        done
        echo "[$devices]"
    elif [ -n "$CUDA_VISIBLE_DEVICES" ]; then
        # Fallback to CUDA_VISIBLE_DEVICES
        local devices=""
        for idx in $(echo "$CUDA_VISIBLE_DEVICES" | tr ',' ' '); do
            if [ -n "$devices" ]; then
                devices="$devices,\"$idx\""
            else
                devices="\"$idx\""
            fi
        done
        echo "[$devices]"
    else
        echo "[]"
    fi
}

# Function to get GPU device IDs for AMD
get_amd_gpu_devices() {
    if [ -n "$SLURM_JOB_GPUS" ]; then
        local devices=""
        for idx in $(echo "$SLURM_JOB_GPUS" | tr ',' ' '); do
            if [ -n "$devices" ]; then
                devices="$devices,\"card$idx\""
            else
                devices="\"card$idx\""
            fi
        done
        echo "[$devices]"
    else
        echo "[]"
    fi
}

# Function to find job's cgroup path
find_cgroup_path() {
    local job_id="$1"
    local paths=(
        "/sys/fs/cgroup/system.slice/slurmstepd.scope/job_${job_id}"
        "/sys/fs/cgroup/slurm/uid_$(id -u ${SLURM_JOB_USER})/job_${job_id}"
        "/sys/fs/cgroup/slurm/job_${job_id}"
    )
    
    for path in "${paths[@]}"; do
        if [ -d "$path" ]; then
            echo "$path"
            return
        fi
    done
    
    # Return empty if not found (cgroup might not exist yet in prolog)
    echo ""
}

# Main execution
main() {
    log "Starting prolog for job $SLURM_JOB_ID"
    
    # Get job information from Slurm environment
    local job_id="${SLURM_JOB_ID:-}"
    local user="${SLURM_JOB_USER:-}"
    local nodes="${SLURM_JOB_NODELIST:-}"
    local cpus="${SLURM_CPUS_ON_NODE:-}"
    local gpus="${SLURM_GPUS_ON_NODE:-0}"
    local partition="${SLURM_JOB_PARTITION:-}"
    local account="${SLURM_JOB_ACCOUNT:-}"
    local mem_mb="${SLURM_MEM_PER_NODE:-}"
    
    if [ -z "$job_id" ]; then
        log_error "SLURM_JOB_ID not set, skipping"
        exit 0
    fi
    
    if [ -z "$user" ]; then
        log_error "SLURM_JOB_USER not set, skipping"
        exit 0
    fi
    
    # Expand nodelist to array
    local expanded_nodes=""
    if command -v scontrol &> /dev/null && [ -n "$nodes" ]; then
        expanded_nodes=$(scontrol show hostnames "$nodes" 2>/dev/null | paste -sd ',' - | sed 's/,/","/g')
        if [ -n "$expanded_nodes" ]; then
            expanded_nodes="[\"$expanded_nodes\"]"
        else
            expanded_nodes="[\"$nodes\"]"
        fi
    else
        expanded_nodes="[\"$nodes\"]"
    fi
    
    # Detect GPU info
    local gpu_vendor=$(detect_gpu_vendor)
    local gpu_devices="[]"
    
    if [ "$gpu_vendor" = "nvidia" ]; then
        gpu_devices=$(get_nvidia_gpu_devices)
    elif [ "$gpu_vendor" = "amd" ]; then
        gpu_devices=$(get_amd_gpu_devices)
    fi
    
    # Count GPUs
    local gpu_count=0
    if [ -n "$SLURM_GPUS_ON_NODE" ] && [ "$SLURM_GPUS_ON_NODE" != "0" ]; then
        gpu_count=$SLURM_GPUS_ON_NODE
    elif [ -n "$SLURM_JOB_GPUS" ]; then
        gpu_count=$(echo "$SLURM_JOB_GPUS" | tr ',' '\n' | wc -l)
    fi
    
    # Find cgroup path
    local cgroup_path=$(find_cgroup_path "$job_id")
    
    # Get current timestamp in ISO 8601 format
    local timestamp=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
    
    # Build JSON payload
    local payload=$(cat <<EOF
{
    "job_id": "$job_id",
    "user": "$user",
    "node_list": $expanded_nodes,
    "cpu_allocation": ${cpus:-0},
    "gpu_allocation": $gpu_count,
    "gpu_vendor": "$gpu_vendor",
    "gpu_devices": $gpu_devices,
    "cgroup_path": "$cgroup_path",
    "partition": "$partition",
    "account": "$account",
    "timestamp": "$timestamp"
}
EOF
)
    
    # Add memory if available
    if [ -n "$mem_mb" ]; then
        payload=$(echo "$payload" | jq --arg mem "$mem_mb" '. + {memory_allocation_mb: ($mem | tonumber)}' 2>/dev/null || echo "$payload")
    fi
    
    log "Sending job_started event for job $job_id"
    log "Payload: $payload"
    
    # Send HTTP POST request (non-blocking)
    local response
    response=$(curl -s -S -X POST \
        --max-time "$OBSERVABILITY_TIMEOUT" \
        --connect-timeout 2 \
        -H "Content-Type: application/json" \
        -d "$payload" \
        "${OBSERVABILITY_API_URL}/v1/events/job-started" 2>&1) || {
        log_error "Failed to send event: $response"
        # Don't fail the prolog - observability is optional
        exit 0
    }
    
    log "Response: $response"
    log "Prolog completed successfully for job $job_id"
}

# Run main, but don't fail the job if observability service is unavailable
main "$@" || {
    log_error "Prolog failed but continuing: $?"
    exit 0
}
