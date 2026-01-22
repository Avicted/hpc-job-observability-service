#!/bin/bash
# =============================================================================
# Slurm Epilog Script - Job Finished Event Emitter
# =============================================================================
#
# This script is called by Slurm when a job finishes on a compute node.
# It emits a job_finished event to the HPC Job Observability Service.
#
# Installation:
#   1. Copy this script to /etc/slurm/epilog.d/job-observability.sh
#   2. Make it executable: chmod +x /etc/slurm/epilog.d/job-observability.sh
#   3. Configure Slurm to use epilog scripts in slurm.conf:
#        Epilog=/etc/slurm/epilog.d/*
#      Or directly:
#        Epilog=/etc/slurm/epilog.d/job-observability.sh
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
LOG_FILE="/var/log/slurm/observability-epilog.log"

# Ensure log directory exists
mkdir -p "$(dirname "$LOG_FILE")"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [EPILOG] $*" >> "$LOG_FILE"
}

log_error() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [EPILOG] ERROR: $*" >> "$LOG_FILE"
}

# Function to map Slurm job state to our state
# Note: Slurm reports COMPLETED for jobs that finished (even with non-zero exit code)
# We need to check the exit code and signal to determine the real state
map_slurm_state() {
    local slurm_state="${1:-}"
    local exit_code="${2:-0}"
    local signal="${3:-0}"
    
    # First check for explicit terminal states
    case "$slurm_state" in
        FAILED|NODE_FAIL|BOOT_FAIL|DEADLINE|OUT_OF_MEMORY|PREEMPTED)
            echo "failed"
            return
            ;;
        CANCELLED|TIMEOUT|REVOKED)
            echo "cancelled"
            return
            ;;
        PENDING|SUSPENDED|REQUEUED|REQUEUE_FED|REQUEUE_HOLD|SPECIAL_EXIT)
            echo "pending"
            return
            ;;
        RUNNING|CONFIGURING|STAGE_OUT|RESIZING|SIGNALING)
            echo "running"
            return
            ;;
    esac
    
    # For COMPLETED/COMPLETING states, check signal and exit code
    # Signal 15 (SIGTERM) or 9 (SIGKILL) usually means cancellation
    if [ "$signal" != "0" ] && [ -n "$signal" ]; then
        case "$signal" in
            9|15)
                # SIGKILL or SIGTERM - job was cancelled
                echo "cancelled"
                return
                ;;
            *)
                # Other signals typically indicate a crash/failure
                echo "failed"
                return
                ;;
        esac
    fi
    
    # Check exit code - non-zero means failed
    if [ "$exit_code" != "0" ] && [ -n "$exit_code" ]; then
        echo "failed"
        return
    fi
    
    # Default to completed
    echo "completed"
}

# Function to get job exit code from Slurm
get_exit_code() {
    local job_id="$1"
    
    # Try to get exit code from scontrol
    if command -v scontrol &> /dev/null; then
        local exit_code=$(scontrol show job "$job_id" 2>/dev/null | grep -oP 'ExitCode=\K[0-9]+' | head -1)
        if [ -n "$exit_code" ]; then
            echo "$exit_code"
            return
        fi
    fi
    
    # Fallback to environment variable or 0
    echo "${SLURM_JOB_EXIT_CODE:-0}"
}

# Function to get job state from Slurm
get_job_state() {
    local job_id="$1"
    
    # Try to get state from scontrol
    if command -v scontrol &> /dev/null; then
        local state=$(scontrol show job "$job_id" 2>/dev/null | grep -oP 'JobState=\K\w+' | head -1)
        if [ -n "$state" ]; then
            echo "$state"
            return
        fi
    fi
    
    # Fallback to environment or COMPLETED
    echo "${SLURM_JOB_STATE:-COMPLETED}"
}

# Main execution
main() {
    log "Starting epilog for job $SLURM_JOB_ID"
    
    # Get job information from Slurm environment
    local job_id="${SLURM_JOB_ID:-}"
    
    if [ -z "$job_id" ]; then
        log_error "SLURM_JOB_ID not set, skipping"
        exit 0
    fi
    
    # Get job state and exit code
    local slurm_state=$(get_job_state "$job_id")
    local exit_code=$(get_exit_code "$job_id")
    
    # Get signal if the job was killed
    local signal="0"
    if command -v scontrol &> /dev/null; then
        local sig=$(scontrol show job "$job_id" 2>/dev/null | grep -oP 'ExitCode=\d+:\K\d+' | head -1)
        if [ -n "$sig" ]; then
            signal="$sig"
        fi
    fi
    
    # Map Slurm state to our internal state (handles exit codes and signals)
    local final_state=$(map_slurm_state "$slurm_state" "$exit_code" "$signal")
    
    # Get current timestamp in ISO 8601 format
    local timestamp=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
    
    # Build JSON payload
    local payload
    if [ -n "$signal" ] && [ "$signal" != "0" ]; then
        payload=$(cat <<EOF
{
    "job_id": "$job_id",
    "final_state": "$final_state",
    "exit_code": $exit_code,
    "signal": $signal,
    "timestamp": "$timestamp"
}
EOF
)
    else
        payload=$(cat <<EOF
{
    "job_id": "$job_id",
    "final_state": "$final_state",
    "exit_code": $exit_code,
    "timestamp": "$timestamp"
}
EOF
)
    fi
    
    log "Sending job_finished event for job $job_id (state: $final_state, exit: $exit_code)"
    log "Payload: $payload"
    
    # Send HTTP POST request (non-blocking)
    local response
    response=$(curl -s -S -X POST \
        --max-time "$OBSERVABILITY_TIMEOUT" \
        --connect-timeout 2 \
        -H "Content-Type: application/json" \
        -d "$payload" \
        "${OBSERVABILITY_API_URL}/v1/events/job-finished" 2>&1) || {
        log_error "Failed to send event: $response"
        # Don't fail the epilog - observability is optional
        exit 0
    }
    
    log "Response: $response"
    log "Epilog completed successfully for job $job_id"
}

# Run main, but don't fail the job if observability service is unavailable
main "$@" || {
    log_error "Epilog failed but continuing: $?"
    exit 0
}
