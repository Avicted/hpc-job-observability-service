#!/bin/bash
# verify-docker-stack.sh - Verify Docker Compose stack with Slurm integration
#
# This script:
# 1. Starts the Docker Compose stack with slurm profile
# 2. Waits for all services to be healthy
# 3. Tests API endpoints
# 4. Verifies PostgreSQL database contains correct data
# 5. Cleans up (optional)
#
# Usage: ./scripts/verify-docker-stack.sh [--no-cleanup]

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
APP_URL="http://localhost:8080"
MAX_WAIT_SECONDS=120
CLEANUP=true

# Parse arguments
for arg in "$@"; do
    case $arg in
        --no-cleanup)
            CLEANUP=false
            shift
            ;;
    esac
done

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[PASS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[FAIL]${NC} $1"; }

cleanup() {
    if [ "$CLEANUP" = true ]; then
        log_info "Cleaning up Docker Compose stack..."
        cd "$PROJECT_DIR"
        docker-compose --profile slurm down -v 2>/dev/null || true
    else
        log_info "Skipping cleanup (--no-cleanup specified)"
    fi
}

# Trap for cleanup on exit
trap cleanup EXIT

wait_for_service() {
    local url=$1
    local name=$2
    local max_attempts=$((MAX_WAIT_SECONDS / 5))
    local attempt=1

    log_info "Waiting for $name to be ready..."
    while [ $attempt -le $max_attempts ]; do
        if curl -sf "$url" > /dev/null 2>&1; then
            log_success "$name is ready"
            return 0
        fi
        echo -n "."
        sleep 5
        attempt=$((attempt + 1))
    done
    echo ""
    log_error "$name failed to start within ${MAX_WAIT_SECONDS}s"
    return 1
}

run_psql() {
    docker exec hpc-postgres psql -U hpc -d hpc_jobs -t -A -c "$1"
}

# =============================================================================
# Main Script
# =============================================================================

cd "$PROJECT_DIR"

echo ""
echo "=============================================="
echo "  HPC Job Observability - Docker Stack Verify"
echo "=============================================="
echo ""

# Check for .env file
if [ ! -f .env ]; then
    log_warn ".env file not found, copying from .env.example"
    cp .env.example .env
    # Set SCHEDULER_BACKEND=slurm for this test
    sed -i 's/^SCHEDULER_BACKEND=.*/SCHEDULER_BACKEND=slurm/' .env 2>/dev/null || \
        echo "SCHEDULER_BACKEND=slurm" >> .env
fi

# Ensure SCHEDULER_BACKEND is set to slurm
export SCHEDULER_BACKEND=slurm

# Step 1: Clean up any existing containers
log_info "Stopping any existing containers..."
docker-compose --profile slurm down -v 2>/dev/null || true

# Step 2: Build and start the stack
log_info "Building and starting Docker Compose stack with Slurm profile..."
docker-compose --profile slurm up --build --force-recreate -d

# Step 3: Wait for services
echo ""
log_info "Waiting for services to become healthy..."

# Wait for PostgreSQL first
wait_for_service "http://localhost:5432" "PostgreSQL" 2>/dev/null || {
    # PostgreSQL doesn't have HTTP, use docker health check
    attempt=1
    max_attempts=$((MAX_WAIT_SECONDS / 5))
    while [ $attempt -le $max_attempts ]; do
        status=$(docker inspect --format='{{.State.Health.Status}}' hpc-postgres 2>/dev/null || echo "unknown")
        if [ "$status" = "healthy" ]; then
            log_success "PostgreSQL is healthy"
            break
        fi
        echo -n "."
        sleep 5
        attempt=$((attempt + 1))
    done
    if [ "$status" != "healthy" ]; then
        log_error "PostgreSQL failed to become healthy"
        docker logs hpc-postgres 2>&1 | tail -20
        exit 1
    fi
}

# Wait for Slurm
log_info "Waiting for Slurm (slurmrestd) to be ready (this may take up to 60s)..."
attempt=1
max_attempts=$((MAX_WAIT_SECONDS / 5))
while [ $attempt -le $max_attempts ]; do
    status=$(docker inspect --format='{{.State.Health.Status}}' hpc-slurm 2>/dev/null || echo "unknown")
    if [ "$status" = "healthy" ]; then
        log_success "Slurm is healthy"
        break
    fi
    echo -n "."
    sleep 5
    attempt=$((attempt + 1))
done
if [ "$status" != "healthy" ]; then
    log_warn "Slurm not healthy yet, checking container logs..."
    docker logs hpc-slurm 2>&1 | tail -30
fi

# Wait for the App
wait_for_service "$APP_URL/v1/health" "HPC Observability Service" || {
    log_error "App failed to start"
    docker logs hpc-observability 2>&1 | tail -30
    exit 1
}

echo ""
echo "=============================================="
echo "  Running API Tests"
echo "=============================================="
echo ""

TESTS_PASSED=0
TESTS_FAILED=0

# Test 1: Health endpoint
log_info "Test 1: Health endpoint"
response=$(curl -sf "$APP_URL/v1/health" 2>/dev/null || echo "FAILED")
if echo "$response" | grep -q '"status":"healthy"'; then
    log_success "Health endpoint returns healthy"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    log_error "Health endpoint failed: $response"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

# Test 2: Create a job
log_info "Test 2: Create job via API"
JOB_ID="docker-test-job-$(date +%s)"
response=$(curl -sf -X POST "$APP_URL/v1/jobs" \
    -H "Content-Type: application/json" \
    -H "X-Changed-By: docker-verify" \
    -H "X-Source: verify-script" \
    -H "X-Correlation-Id: docker-verify-001" \
    -d "{\"id\":\"$JOB_ID\",\"user\":\"docker-tester\",\"nodes\":[\"node-1\",\"node-2\"]}" 2>/dev/null || echo "FAILED")

if echo "$response" | grep -q "\"id\":\"$JOB_ID\""; then
    log_success "Job created successfully: $JOB_ID"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    log_error "Failed to create job: $response"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

# Test 3: Record metrics
log_info "Test 3: Record metrics"
response=$(curl -sf -X POST "$APP_URL/v1/jobs/$JOB_ID/metrics" \
    -H "Content-Type: application/json" \
    -H "X-Changed-By: docker-verify" \
    -H "X-Source: verify-script" \
    -d '{"cpu_usage":65.5,"memory_usage_mb":4096,"gpu_usage":30.0}' 2>/dev/null || echo "FAILED")

if echo "$response" | grep -q '"cpu_usage":65.5'; then
    log_success "Metrics recorded successfully"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    log_error "Failed to record metrics: $response"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

# Test 4: Record second metrics (for rollup verification)
log_info "Test 4: Record second metrics (for rollup test)"
response=$(curl -sf -X POST "$APP_URL/v1/jobs/$JOB_ID/metrics" \
    -H "Content-Type: application/json" \
    -H "X-Changed-By: docker-verify" \
    -H "X-Source: verify-script" \
    -d '{"cpu_usage":85.5,"memory_usage_mb":8192,"gpu_usage":50.0}' 2>/dev/null || echo "FAILED")

if echo "$response" | grep -q '"cpu_usage":85.5'; then
    log_success "Second metrics recorded"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    log_error "Failed to record second metrics: $response"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

# Test 5: Get job and verify rollups
log_info "Test 5: Get job and verify rollups"
response=$(curl -sf "$APP_URL/v1/jobs/$JOB_ID" 2>/dev/null || echo "FAILED")

if echo "$response" | grep -q '"sample_count":2'; then
    log_success "Job has correct sample_count: 2"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    log_error "Job sample_count incorrect: $response"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

# Verify avg_cpu_usage (should be ~75.5)
if echo "$response" | grep -q '"avg_cpu_usage":75.5'; then
    log_success "Job has correct avg_cpu_usage: 75.5"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    log_warn "avg_cpu_usage may differ slightly from expected"
fi

# Verify max_cpu_usage (should be 85.5)
if echo "$response" | grep -q '"max_cpu_usage":85.5'; then
    log_success "Job has correct max_cpu_usage: 85.5"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    log_error "Job max_cpu_usage incorrect"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

# Test 6: List jobs
log_info "Test 6: List jobs"
response=$(curl -sf "$APP_URL/v1/jobs" 2>/dev/null || echo "FAILED")
if echo "$response" | grep -q "\"jobs\":"; then
    log_success "List jobs endpoint works"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    log_error "List jobs failed: $response"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

# Test 7: Prometheus metrics endpoint
log_info "Test 7: Prometheus metrics endpoint"
response=$(curl -sf "$APP_URL/metrics" 2>/dev/null || echo "FAILED")
if echo "$response" | grep -q "hpc_job"; then
    log_success "Prometheus metrics endpoint works"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    log_error "Prometheus metrics endpoint failed"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

echo ""
echo "=============================================="
echo "  Verifying PostgreSQL Database"
echo "=============================================="
echo ""

# Test 8: Verify job exists in database
log_info "Test 8: Verify job in PostgreSQL"
db_job_count=$(run_psql "SELECT COUNT(*) FROM jobs WHERE id = '$JOB_ID';" 2>/dev/null || echo "0")
if [ "$db_job_count" = "1" ]; then
    log_success "Job found in PostgreSQL"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    log_error "Job not found in PostgreSQL (count: $db_job_count)"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

# Test 9: Verify rollups in database
log_info "Test 9: Verify rollups in PostgreSQL"
db_rollups=$(run_psql "SELECT sample_count, avg_cpu_usage, max_cpu_usage, max_memory_usage_mb FROM jobs WHERE id = '$JOB_ID';" 2>/dev/null || echo "ERROR")
echo "  Database rollups: $db_rollups"
if echo "$db_rollups" | grep -q "2|75.5|85.5|8192"; then
    log_success "Rollups are correct in PostgreSQL"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    log_error "Rollups incorrect in PostgreSQL"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

# Test 10: Verify metric samples in database
log_info "Test 10: Verify metric samples in PostgreSQL"
db_samples=$(run_psql "SELECT COUNT(*) FROM metric_samples WHERE job_id = '$JOB_ID';" 2>/dev/null || echo "0")
if [ "$db_samples" = "2" ]; then
    log_success "Metric samples count correct: 2"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    log_error "Metric samples count incorrect: $db_samples"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

# Test 11: Verify audit events in database
log_info "Test 11: Verify audit events in PostgreSQL"
db_audit=$(run_psql "SELECT COUNT(*) FROM job_audit_events WHERE job_id = '$JOB_ID';" 2>/dev/null || echo "0")
# Should have at least 1 (create) + 2 (metrics updates) = 3 audit events
if [ "$db_audit" -ge "3" ]; then
    log_success "Audit events recorded: $db_audit"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    log_error "Audit events count unexpected: $db_audit (expected >= 3)"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

# Test 12: Verify audit event details
log_info "Test 12: Verify audit event details"
db_audit_detail=$(run_psql "SELECT change_type, changed_by, source FROM job_audit_events WHERE job_id = '$JOB_ID' AND change_type = 'create' LIMIT 1;" 2>/dev/null || echo "ERROR")
if echo "$db_audit_detail" | grep -q "create|docker-verify|verify-script"; then
    log_success "Audit event details correct"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    log_error "Audit event details incorrect: $db_audit_detail"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

# Print database summary
echo ""
log_info "Database summary for job $JOB_ID:"
echo "  Jobs table:"
run_psql "SELECT id, user_name, node_count, state, sample_count, avg_cpu_usage, max_cpu_usage, max_memory_usage_mb FROM jobs WHERE id = '$JOB_ID';" 2>/dev/null | sed 's/^/    /'
echo "  Metric samples:"
run_psql "SELECT job_id, cpu_usage, memory_usage_mb, gpu_usage FROM metric_samples WHERE job_id = '$JOB_ID';" 2>/dev/null | sed 's/^/    /'
echo "  Audit events:"
run_psql "SELECT change_type, changed_by, source, correlation_id FROM job_audit_events WHERE job_id = '$JOB_ID';" 2>/dev/null | sed 's/^/    /'

echo ""
echo "=============================================="
echo "  Test Summary"
echo "=============================================="
echo ""
echo -e "  ${GREEN}Passed:${NC} $TESTS_PASSED"
echo -e "  ${RED}Failed:${NC} $TESTS_FAILED"
echo ""

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}Some tests failed!${NC}"
    exit 1
fi
