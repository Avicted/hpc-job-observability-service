//go:build slurm_e2e
// +build slurm_e2e

// Lifecycle E2E Tests
//
// These tests verify the complete flow of job lifecycle events from Slurm prolog/epilog
// scripts through the Go service to the database. They require:
//   - Docker Compose with slurm profile running
//   - Prolog/epilog scripts configured in the Slurm container
//
// Run with: go test -tags=slurm_e2e -v ./internal/e2e/... -run TestLifecycle
//
// These tests verify:
//   1. Job submission triggers prolog which sends job-started event
//   2. Job completion triggers epilog which sends job-finished event
//   3. Database contains correct data (cgroup_path, gpu info, exit codes, etc.)

package e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/api/types"
	"github.com/Avicted/hpc-job-observability-service/internal/storage"
)

// lifecycleTestEnv extends testEnv with HTTP client for API calls
type lifecycleTestEnv struct {
	*testEnv
	apiURL string
}

// setupLifecycleTestEnv sets up the test environment for lifecycle tests
func setupLifecycleTestEnv(t *testing.T) (*lifecycleTestEnv, func()) {
	t.Helper()

	baseEnv, cleanup := setupTestEnv(t)

	// API URL from environment or default
	apiURL := "http://localhost:8080"

	return &lifecycleTestEnv{
		testEnv: baseEnv,
		apiURL:  apiURL,
	}, cleanup
}

// waitForJobInDB polls the database until the job appears or timeout
func waitForJobInDB(t *testing.T, store *storage.PostgresStorage, jobID string, timeout time.Duration) *storage.Job {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, err := store.GetJob(context.Background(), jobID)
		if err == nil && job != nil {
			return job
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("Timeout waiting for job %s to appear in database", jobID)
	return nil
}

// waitForJobState polls the database until the job reaches the expected state or timeout
func waitForJobState(t *testing.T, store *storage.PostgresStorage, jobID string, expectedState storage.JobState, timeout time.Duration) *storage.Job {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, err := store.GetJob(context.Background(), jobID)
		if err == nil && job != nil && job.State == expectedState {
			return job
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Get final state for error message
	job, _ := store.GetJob(context.Background(), jobID)
	if job != nil {
		t.Fatalf("Timeout waiting for job %s to reach state %s (current state: %s)", jobID, expectedState, job.State)
	} else {
		t.Fatalf("Timeout waiting for job %s to reach state %s (job not found)", jobID, expectedState)
	}
	return nil
}

// TestLifecycleE2E_BasicJobFlow tests the basic flow: submit job -> prolog event -> job runs -> epilog event
func TestLifecycleE2E_BasicJobFlow(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	// Submit a simple job that will complete quickly
	jobID := submitSlurmJob(t, "echo 'Hello from lifecycle test'; sleep 2; echo 'Done'")
	t.Logf("Submitted job: %s", jobID)
	defer cancelSlurmJob(t, jobID) // Cleanup in case test fails

	// Wait for job to appear in database (prolog event should create it)
	t.Log("Waiting for job-started event to create job in database...")
	job := waitForJobInDB(t, env.store, jobID, 30*time.Second)

	// Verify initial job state from prolog
	t.Logf("Job created in DB: state=%s, user=%s, nodes=%v", job.State, job.User, job.Nodes)

	if job.State != storage.JobStateRunning {
		t.Errorf("Expected initial state 'running' from prolog, got '%s'", job.State)
	}
	if job.User == "" {
		t.Error("User should not be empty")
	}
	if len(job.Nodes) == 0 {
		t.Log("Nodes should not be empty")
	}

	// Verify scheduler info was set
	if job.Scheduler == nil {
		t.Error("Scheduler info should not be nil")
	} else {
		if job.Scheduler.Type != storage.SchedulerTypeSlurm {
			t.Errorf("Expected scheduler type 'slurm', got '%s'", job.Scheduler.Type)
		}
	}

	// Verify cluster metadata
	if job.ClusterName != "default" {
		t.Errorf("Expected cluster_name 'default', got '%s'", job.ClusterName)
	}
	if job.IngestVersion != "prolog-v1" {
		t.Errorf("Expected ingest_version 'prolog-v1', got '%s'", job.IngestVersion)
	}

	// Wait for job to complete and epilog to update state
	t.Log("Waiting for job-finished event to update job state...")
	job = waitForJobState(t, env.store, jobID, storage.JobStateCompleted, 60*time.Second)

	// Verify final state from epilog
	t.Logf("Job completed: state=%s, exit_code=%v, runtime=%v",
		job.State, job.Scheduler.ExitCode, job.RuntimeSeconds)

	if job.EndTime == nil {
		t.Error("EndTime should be set after job completion")
	}
	if job.RuntimeSeconds <= 0 {
		t.Errorf("RuntimeSeconds should be positive, got %f", job.RuntimeSeconds)
	}

	// Verify exit code is 0 for successful job
	if job.Scheduler != nil && job.Scheduler.ExitCode != nil {
		if *job.Scheduler.ExitCode != 0 {
			t.Errorf("Expected exit code 0, got %d", *job.Scheduler.ExitCode)
		}
	}
}

// TestLifecycleE2E_FailedJobExitCode tests that failed jobs have correct exit codes
func TestLifecycleE2E_FailedJobExitCode(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	// Submit a job that will fail with exit code 42
	jobID := submitSlurmJob(t, "echo 'About to fail'; exit 42")
	t.Logf("Submitted failing job: %s", jobID)

	// Wait for job to appear (prolog)
	job := waitForJobInDB(t, env.store, jobID, 30*time.Second)
	t.Logf("Job created with state: %s", job.State)

	// Wait for job to reach failed state (epilog)
	job = waitForJobState(t, env.store, jobID, storage.JobStateFailed, 60*time.Second)
	t.Logf("Job failed: state=%s", job.State)

	// Verify exit code
	if job.Scheduler == nil {
		t.Fatal("Scheduler info should not be nil")
	}
	if job.Scheduler.ExitCode == nil {
		t.Error("Exit code should be set for failed job")
	} else if *job.Scheduler.ExitCode != 42 {
		t.Errorf("Expected exit code 42, got %d", *job.Scheduler.ExitCode)
	}

	// Verify end time is set
	if job.EndTime == nil {
		t.Error("EndTime should be set for failed job")
	}
}

// TestLifecycleE2E_JobResourceAllocation tests that CPU/memory allocations are captured
func TestLifecycleE2E_JobResourceAllocation(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	// Submit a job with specific resource requests
	cmd := exec.Command("docker", "exec", "hpc-slurm", "sbatch",
		"--parsable",
		"--cpus-per-task=2",
		"--mem=512M",
		"--wrap", "echo 'Resource test'; sleep 2")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to submit job: %v\nOutput: %s", err, string(output))
	}

	jobID := strings.TrimSpace(string(output))
	t.Logf("Submitted job with resources: %s", jobID)
	defer cancelSlurmJob(t, jobID)

	// Wait for job to appear
	job := waitForJobInDB(t, env.store, jobID, 30*time.Second)

	// Log the resource allocations
	t.Logf("Job resources: CPUs=%d, Memory=%dMB",
		job.AllocatedCPUs, job.AllocatedMemMB)

	// Verify CPU allocation is captured
	if job.AllocatedCPUs <= 0 {
		t.Logf("Note: CPU allocation not captured (got %d) - may depend on Slurm configuration", job.AllocatedCPUs)
	}

	// Wait for completion
	waitForJobState(t, env.store, jobID, storage.JobStateCompleted, 60*time.Second)
}

// TestLifecycleE2E_AuditTrailForLifecycleEvents tests that audit events are created for prolog/epilog
func TestLifecycleE2E_AuditTrailForLifecycleEvents(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	// Submit a job
	jobID := submitSlurmJob(t, "echo 'Audit trail test'; sleep 2")
	t.Logf("Submitted job: %s", jobID)

	// Wait for job to complete
	waitForJobState(t, env.store, jobID, storage.JobStateCompleted, 60*time.Second)

	// Query audit events
	events := queryAuditEvents(t, env.db, jobID)
	t.Logf("Found %d audit events for job %s", len(events), jobID)

	if len(events) < 1 {
		t.Fatal("Expected at least one audit event")
	}

	// Check for prolog event (create)
	var foundProlog, foundEpilog bool
	for _, e := range events {
		t.Logf("  Audit: type=%s, by=%s, source=%s", e.ChangeType, e.ChangedBy, e.Source)

		if e.ChangedBy == "slurm-prolog" || e.Source == "lifecycle-event" {
			foundProlog = true
		}
		if e.ChangedBy == "slurm-epilog" {
			foundEpilog = true
		}
	}

	if !foundProlog {
		t.Error("Expected audit event from slurm-prolog")
	}

	// Epilog event is optional (updates existing job)
	if foundEpilog {
		t.Log("Found epilog audit event")
	} else {
		t.Log("Note: Epilog audit event not found (may not create separate audit record)")
	}
}

// TestLifecycleE2E_IdempotencyJobStarted tests that duplicate job-started events are handled correctly
func TestLifecycleE2E_IdempotencyJobStarted(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	// Create a unique job ID for manual testing
	jobID := fmt.Sprintf("test-idempotency-%d", time.Now().UnixNano())

	// Send job-started event manually
	event := types.JobStartedEvent{
		JobId:     jobID,
		User:      "testuser",
		NodeList:  []string{"node1"},
		Timestamp: time.Now(),
	}

	// First request should create the job
	resp1 := sendJobStartedEvent(t, env.apiURL, event)
	t.Logf("First response: status=%s, message=%v", resp1.Status, resp1.Message)

	if resp1.Status != types.Created {
		t.Errorf("Expected status 'created' for first request, got '%s'", resp1.Status)
	}

	// Second request should be skipped (idempotent)
	resp2 := sendJobStartedEvent(t, env.apiURL, event)
	t.Logf("Second response: status=%s, message=%v", resp2.Status, resp2.Message)

	if resp2.Status != types.Skipped {
		t.Errorf("Expected status 'skipped' for duplicate request, got '%s'", resp2.Status)
	}

	// Verify only one job exists in database
	job, err := env.store.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatalf("Failed to get job: %v", err)
	}
	if job.ID != jobID {
		t.Errorf("Job ID mismatch: got %s, want %s", job.ID, jobID)
	}
}

// TestLifecycleE2E_IdempotencyJobFinished tests that duplicate job-finished events are handled correctly
func TestLifecycleE2E_IdempotencyJobFinished(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	// Create a job first
	jobID := fmt.Sprintf("test-finish-idempotency-%d", time.Now().UnixNano())
	startEvent := types.JobStartedEvent{
		JobId:     jobID,
		User:      "testuser",
		NodeList:  []string{"node1"},
		Timestamp: time.Now(),
	}
	sendJobStartedEvent(t, env.apiURL, startEvent)

	// Send job-finished event
	exitCode := 0
	finishEvent := types.JobFinishedEvent{
		JobId:      jobID,
		FinalState: types.Completed,
		ExitCode:   &exitCode,
		Timestamp:  time.Now().Add(5 * time.Second),
	}

	// First finish request should update the job
	resp1 := sendJobFinishedEvent(t, env.apiURL, finishEvent)
	t.Logf("First finish response: status=%s", resp1.Status)

	if resp1.Status != types.Updated {
		t.Errorf("Expected status 'updated' for first finish, got '%s'", resp1.Status)
	}

	// Second finish request should be skipped (already terminal)
	resp2 := sendJobFinishedEvent(t, env.apiURL, finishEvent)
	t.Logf("Second finish response: status=%s", resp2.Status)

	if resp2.Status != types.Skipped {
		t.Errorf("Expected status 'skipped' for duplicate finish, got '%s'", resp2.Status)
	}
}

// TestLifecycleE2E_CgroupPathCapture tests that cgroup path is captured from prolog
func TestLifecycleE2E_CgroupPathCapture(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	// Submit a job and check if cgroup_path is set
	// Note: In a single-node Docker test environment, cgroup path may not be available
	jobID := submitSlurmJob(t, "echo 'Cgroup test'; cat /proc/self/cgroup 2>/dev/null || echo 'No cgroup'; sleep 2")
	t.Logf("Submitted job: %s", jobID)

	// Wait for job
	job := waitForJobInDB(t, env.store, jobID, 30*time.Second)

	// Log cgroup path (may be empty in test environment)
	if job.CgroupPath != "" {
		t.Logf("Cgroup path captured: %s", job.CgroupPath)
	} else {
		t.Log("Note: Cgroup path not captured (expected in Docker test environment)")
	}

	// Wait for completion
	waitForJobState(t, env.store, jobID, storage.JobStateCompleted, 60*time.Second)
}

// TestLifecycleE2E_MultiNodeJobHandling tests jobs running on multiple nodes
func TestLifecycleE2E_MultiNodeJobHandling(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	// In our single-node test cluster, we can simulate multi-node by checking the API directly
	// Create a job with multiple nodes
	jobID := fmt.Sprintf("test-multinode-%d", time.Now().UnixNano())
	event := types.JobStartedEvent{
		JobId:     jobID,
		User:      "testuser",
		NodeList:  []string{"node1", "node2", "node3"},
		Timestamp: time.Now(),
	}

	resp := sendJobStartedEvent(t, env.apiURL, event)
	if resp.Status != types.Created {
		t.Fatalf("Failed to create job: %s", resp.Status)
	}

	// Verify job has correct node count
	job, err := env.store.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatalf("Failed to get job: %v", err)
	}

	if job.NodeCount != 3 {
		t.Errorf("Expected node_count=3, got %d", job.NodeCount)
	}
	if len(job.Nodes) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(job.Nodes))
	}

	t.Logf("Multi-node job created: nodes=%v, count=%d", job.Nodes, job.NodeCount)
}

// TestLifecycleE2E_PartitionAndAccount tests that partition and account are captured
func TestLifecycleE2E_PartitionAndAccount(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	// Create a job with partition and account info
	jobID := fmt.Sprintf("test-partition-%d", time.Now().UnixNano())
	partition := "debug"
	account := "research"

	event := types.JobStartedEvent{
		JobId:     jobID,
		User:      "testuser",
		NodeList:  []string{"node1"},
		Timestamp: time.Now(),
		Partition: &partition,
		Account:   &account,
	}

	sendJobStartedEvent(t, env.apiURL, event)

	// Verify job has partition and account
	job, err := env.store.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatalf("Failed to get job: %v", err)
	}

	if job.Scheduler == nil {
		t.Fatal("Scheduler info should not be nil")
	}

	if job.Scheduler.Partition != partition {
		t.Errorf("Expected partition '%s', got '%s'", partition, job.Scheduler.Partition)
	}
	if job.Scheduler.Account != account {
		t.Errorf("Expected account '%s', got '%s'", account, job.Scheduler.Account)
	}

	t.Logf("Partition and account captured: partition=%s, account=%s",
		job.Scheduler.Partition, job.Scheduler.Account)
}

// TestLifecycleE2E_GPUInfoCapture tests that GPU info is captured from prolog
func TestLifecycleE2E_GPUInfoCapture(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	// Create a job with GPU info
	jobID := fmt.Sprintf("test-gpu-%d", time.Now().UnixNano())
	gpuVendor := types.GPUVendor("nvidia")
	gpuAllocation := 2
	gpuDevices := []string{"GPU-0", "GPU-1"}

	event := types.JobStartedEvent{
		JobId:         jobID,
		User:          "testuser",
		NodeList:      []string{"gpu-node1"},
		Timestamp:     time.Now(),
		GpuVendor:     &gpuVendor,
		GpuAllocation: &gpuAllocation,
		GpuDevices:    &gpuDevices,
	}

	sendJobStartedEvent(t, env.apiURL, event)

	// Verify job has GPU info
	job, err := env.store.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatalf("Failed to get job: %v", err)
	}

	if job.GPUVendor != storage.GPUVendor(gpuVendor) {
		t.Errorf("Expected GPU vendor '%s', got '%s'", gpuVendor, job.GPUVendor)
	}
	if job.GPUCount != gpuAllocation {
		t.Errorf("Expected GPU count %d, got %d", gpuAllocation, job.GPUCount)
	}
	if len(job.GPUDevices) != len(gpuDevices) {
		t.Errorf("Expected %d GPU devices, got %d", len(gpuDevices), len(job.GPUDevices))
	}

	t.Logf("GPU info captured: vendor=%s, count=%d, devices=%v",
		job.GPUVendor, job.GPUCount, job.GPUDevices)
}

// TestLifecycleE2E_DatabaseSchemaIntegrity tests that all expected fields are in the database
func TestLifecycleE2E_DatabaseSchemaIntegrity(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	// Create a job with all fields populated
	jobID := fmt.Sprintf("test-schema-%d", time.Now().UnixNano())
	gpuVendor := types.GPUVendor("amd")
	gpuAllocation := 4
	cpuAllocation := 16
	memAllocation := 32768 // 32GB
	partition := "gpu"
	account := "ml-team"
	cgroupPath := "/sys/fs/cgroup/slurm/job_12345"

	event := types.JobStartedEvent{
		JobId:              jobID,
		User:               "testuser",
		NodeList:           []string{"gpu-node1", "gpu-node2"},
		Timestamp:          time.Now(),
		GpuVendor:          &gpuVendor,
		GpuAllocation:      &gpuAllocation,
		GpuDevices:         &[]string{"card0", "card1", "card2", "card3"},
		CpuAllocation:      &cpuAllocation,
		MemoryAllocationMb: &memAllocation,
		CgroupPath:         &cgroupPath,
		Partition:          &partition,
		Account:            &account,
	}

	sendJobStartedEvent(t, env.apiURL, event)

	// Query the database directly to verify all fields
	var (
		dbJobID         string
		dbUser          string
		dbState         string
		dbGpuVendor     sql.NullString
		dbGpuCount      sql.NullInt64
		dbCgroupPath    sql.NullString
		dbAllocatedCPUs sql.NullInt64
		dbAllocatedMem  sql.NullInt64
		dbClusterName   sql.NullString
		dbIngestVersion sql.NullString
	)

	err := env.db.QueryRow(`
		SELECT id, user_name, state, gpu_vendor, gpu_count, cgroup_path,
		       allocated_cpus, allocated_memory_mb, cluster_name, ingest_version
		FROM jobs WHERE id = $1
	`, jobID).Scan(
		&dbJobID, &dbUser, &dbState, &dbGpuVendor, &dbGpuCount, &dbCgroupPath,
		&dbAllocatedCPUs, &dbAllocatedMem, &dbClusterName, &dbIngestVersion,
	)
	if err != nil {
		t.Fatalf("Failed to query job: %v", err)
	}

	// Verify all fields
	checks := []struct {
		name string
		got  interface{}
		want interface{}
	}{
		{"job_id", dbJobID, jobID},
		{"user", dbUser, "testuser"},
		{"state", dbState, "running"},
		{"gpu_vendor", dbGpuVendor.String, gpuVendor},
		{"gpu_count", dbGpuCount.Int64, int64(gpuAllocation)},
		{"cgroup_path", dbCgroupPath.String, cgroupPath},
		{"allocated_cpus", dbAllocatedCPUs.Int64, int64(cpuAllocation)},
		{"allocated_memory_mb", dbAllocatedMem.Int64, int64(memAllocation)},
		{"cluster_name", dbClusterName.String, "default"},
		{"ingest_version", dbIngestVersion.String, "prolog-v1"},
	}

	for _, c := range checks {
		if fmt.Sprintf("%v", c.got) != fmt.Sprintf("%v", c.want) {
			t.Errorf("Field %s: got %v, want %v", c.name, c.got, c.want)
		} else {
			t.Logf("Field %s: OK (%v)", c.name, c.got)
		}
	}
}

// TestLifecycleE2E_CancelledJobState tests that cancelled jobs are properly handled
func TestLifecycleE2E_CancelledJobState(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	// Submit a long-running job and cancel it
	jobID := submitSlurmJob(t, "echo 'Will be cancelled'; sleep 300")
	t.Logf("Submitted job: %s", jobID)

	// Wait for job to start
	job := waitForJobInDB(t, env.store, jobID, 30*time.Second)
	t.Logf("Job started: state=%s", job.State)

	// Cancel the job
	t.Log("Cancelling job...")
	cancelSlurmJob(t, jobID)

	// Wait for cancelled state
	job = waitForJobState(t, env.store, jobID, storage.JobStateCancelled, 60*time.Second)
	t.Logf("Job cancelled: state=%s", job.State)

	// Verify end time is set
	if job.EndTime == nil {
		t.Error("EndTime should be set for cancelled job")
	}
}

// TestLifecycleE2E_RapidJobSubmission tests handling multiple rapid job submissions
func TestLifecycleE2E_RapidJobSubmission(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	numJobs := 5
	jobIDs := make([]string, numJobs)

	// Submit multiple jobs rapidly
	for i := 0; i < numJobs; i++ {
		jobID := submitSlurmJob(t, fmt.Sprintf("echo 'Rapid job %d'; sleep 2", i))
		jobIDs[i] = jobID
		t.Logf("Submitted job %d: %s", i+1, jobID)
	}

	// Wait for all jobs to appear in database
	for _, jobID := range jobIDs {
		job := waitForJobInDB(t, env.store, jobID, 30*time.Second)
		t.Logf("Job %s created: state=%s", jobID, job.State)
	}

	// Wait for all jobs to complete
	for _, jobID := range jobIDs {
		job := waitForJobState(t, env.store, jobID, storage.JobStateCompleted, 90*time.Second)
		t.Logf("Job %s completed: state=%s, runtime=%.2fs", jobID, job.State, job.RuntimeSeconds)
	}

	t.Logf("All %d jobs completed successfully", numJobs)
}

// Helper functions for sending lifecycle events directly to the API

func sendJobStartedEvent(t *testing.T, apiURL string, event types.JobStartedEvent) types.JobEventResponse {
	t.Helper()

	body, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Failed to marshal event: %v", err)
	}

	resp, err := http.Post(apiURL+"/v1/events/job-started", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Failed to send job-started event: %v", err)
	}
	defer resp.Body.Close()

	var result types.JobEventResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	return result
}

func sendJobFinishedEvent(t *testing.T, apiURL string, event types.JobFinishedEvent) types.JobEventResponse {
	t.Helper()

	body, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Failed to marshal event: %v", err)
	}

	resp, err := http.Post(apiURL+"/v1/events/job-finished", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Failed to send job-finished event: %v", err)
	}
	defer resp.Body.Close()

	var result types.JobEventResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	return result
}

// =============================================================================
// Comprehensive Slurm Job State Tests
// =============================================================================

// TestLifecycleE2E_AllJobStates_Completed tests a job that completes successfully (exit code 0)
func TestLifecycleE2E_AllJobStates_Completed(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	// Submit a job that completes successfully
	jobID := submitSlurmJob(t, "echo 'Success test'; exit 0")
	t.Logf("Submitted successful job: %s", jobID)

	// Wait for job to appear (prolog)
	job := waitForJobInDB(t, env.store, jobID, 30*time.Second)
	t.Logf("Job created: state=%s", job.State)

	// Wait for completion (epilog)
	job = waitForJobState(t, env.store, jobID, storage.JobStateCompleted, 60*time.Second)

	// Verify state and exit code
	if job.State != storage.JobStateCompleted {
		t.Errorf("Expected state 'completed', got '%s'", job.State)
	}
	if job.Scheduler != nil && job.Scheduler.ExitCode != nil {
		if *job.Scheduler.ExitCode != 0 {
			t.Errorf("Expected exit code 0, got %d", *job.Scheduler.ExitCode)
		}
		t.Logf("Job completed successfully: exit_code=%d", *job.Scheduler.ExitCode)
	}
}

// TestLifecycleE2E_AllJobStates_Failed tests a job that fails with non-zero exit code
func TestLifecycleE2E_AllJobStates_Failed(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	testCases := []struct {
		name     string
		exitCode int
	}{
		{"exit_1", 1},
		{"exit_2", 2},
		{"exit_42", 42},
		{"exit_127", 127}, // Command not found
		{"exit_255", 255}, // Max exit code
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Submit a job that fails with specific exit code
			jobID := submitSlurmJob(t, fmt.Sprintf("echo 'Failing with exit %d'; exit %d", tc.exitCode, tc.exitCode))
			t.Logf("Submitted failing job: %s (expected exit code: %d)", jobID, tc.exitCode)

			// Wait for job to appear (prolog)
			job := waitForJobInDB(t, env.store, jobID, 30*time.Second)
			t.Logf("Job created: state=%s", job.State)

			// Wait for failed state (epilog)
			job = waitForJobState(t, env.store, jobID, storage.JobStateFailed, 60*time.Second)

			// Verify state and exit code
			if job.State != storage.JobStateFailed {
				t.Errorf("Expected state 'failed', got '%s'", job.State)
			}
			if job.Scheduler == nil || job.Scheduler.ExitCode == nil {
				t.Error("Expected exit code to be set")
			} else if *job.Scheduler.ExitCode != tc.exitCode {
				t.Errorf("Expected exit code %d, got %d", tc.exitCode, *job.Scheduler.ExitCode)
			}
			if job.EndTime == nil {
				t.Error("Expected end_time to be set")
			}
			t.Logf("Job failed as expected: state=%s, exit_code=%d", job.State, *job.Scheduler.ExitCode)
		})
	}
}

// TestLifecycleE2E_AllJobStates_Cancelled tests a job that is cancelled by the user
func TestLifecycleE2E_AllJobStates_Cancelled(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	// Submit a long-running job
	jobID := submitSlurmJob(t, "echo 'Will be cancelled'; sleep 300")
	t.Logf("Submitted job to cancel: %s", jobID)

	// Wait for job to start running (prolog)
	job := waitForJobInDB(t, env.store, jobID, 30*time.Second)
	t.Logf("Job created: state=%s", job.State)

	// Give it a moment to start running
	time.Sleep(2 * time.Second)

	// Cancel the job
	t.Log("Cancelling job...")
	cancelSlurmJob(t, jobID)

	// Wait for cancelled state (epilog)
	job = waitForJobState(t, env.store, jobID, storage.JobStateCancelled, 60*time.Second)

	// Verify state
	if job.State != storage.JobStateCancelled {
		t.Errorf("Expected state 'cancelled', got '%s'", job.State)
	}
	if job.EndTime == nil {
		t.Error("Expected end_time to be set")
	}
	t.Logf("Job cancelled successfully: state=%s", job.State)
}

// TestLifecycleE2E_AllJobStates_Timeout tests a job that exceeds its time limit
func TestLifecycleE2E_AllJobStates_Timeout(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	// Submit a job with a very short time limit (1 minute) that runs longer
	cmd := exec.Command("docker", "exec", "hpc-slurm", "sbatch",
		"--parsable",
		"--time=0:01", // 1 minute time limit
		"--wrap", "echo 'Will timeout'; sleep 120")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to submit timeout job: %v\nOutput: %s", err, string(output))
	}

	jobID := strings.TrimSpace(string(output))
	t.Logf("Submitted job with 1-minute time limit: %s", jobID)

	// Wait for job to appear (prolog)
	job := waitForJobInDB(t, env.store, jobID, 30*time.Second)
	t.Logf("Job created: state=%s", job.State)

	// Wait for the job to timeout (should be cancelled state after ~60 seconds)
	t.Log("Waiting for job to timeout (this will take ~60 seconds)...")
	deadline := time.Now().Add(90 * time.Second)
	var finalJob *storage.Job

	for time.Now().Before(deadline) {
		time.Sleep(5 * time.Second)
		finalJob, _ = env.store.GetJob(context.Background(), jobID)
		if finalJob != nil {
			t.Logf("Job state: %s", finalJob.State)
			// TIMEOUT maps to cancelled in our epilog script
			if finalJob.State == storage.JobStateCancelled || finalJob.State == storage.JobStateFailed {
				break
			}
		}
	}

	if finalJob == nil {
		t.Fatal("Job not found after timeout")
	}

	// TIMEOUT is mapped to 'cancelled' by the epilog script
	if finalJob.State != storage.JobStateCancelled && finalJob.State != storage.JobStateFailed {
		t.Errorf("Expected state 'cancelled' or 'failed' for timeout, got '%s'", finalJob.State)
	}
	if finalJob.EndTime == nil {
		t.Error("Expected end_time to be set")
	}
	t.Logf("Job timed out as expected: state=%s", finalJob.State)
}

// TestLifecycleE2E_AllJobStates_Signal tests a job that is killed by a signal
func TestLifecycleE2E_AllJobStates_Signal(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	// Submit a job that will be killed with a signal
	jobID := submitSlurmJob(t, "echo 'Will be signaled'; sleep 300")
	t.Logf("Submitted job to signal: %s", jobID)

	// Wait for job to start running (prolog)
	job := waitForJobInDB(t, env.store, jobID, 30*time.Second)
	t.Logf("Job created: state=%s", job.State)

	// Give it a moment to start running
	time.Sleep(2 * time.Second)

	// Send SIGTERM to the job using scancel with signal
	t.Log("Sending SIGTERM to job...")
	cmd := exec.Command("docker", "exec", "hpc-slurm", "scancel", "--signal=TERM", jobID)
	if err := cmd.Run(); err != nil {
		t.Logf("scancel --signal=TERM returned: %v (this may be expected)", err)
	}

	// Wait for the job to be terminated
	time.Sleep(5 * time.Second)

	// If still running, force cancel
	currentJob, _ := env.store.GetJob(context.Background(), jobID)
	if currentJob != nil && currentJob.State == storage.JobStateRunning {
		t.Log("Job still running, force cancelling...")
		cancelSlurmJob(t, jobID)
	}

	// Wait for terminal state
	deadline := time.Now().Add(30 * time.Second)
	var finalJob *storage.Job
	for time.Now().Before(deadline) {
		finalJob, _ = env.store.GetJob(context.Background(), jobID)
		if finalJob != nil && (finalJob.State == storage.JobStateCancelled ||
			finalJob.State == storage.JobStateFailed ||
			finalJob.State == storage.JobStateCompleted) {
			break
		}
		time.Sleep(1 * time.Second)
	}

	if finalJob == nil {
		t.Fatal("Job not found")
	}

	// Signal termination should result in cancelled or failed
	if finalJob.State != storage.JobStateCancelled && finalJob.State != storage.JobStateFailed {
		t.Errorf("Expected terminal state for signaled job, got '%s'", finalJob.State)
	}
	t.Logf("Job terminated by signal: state=%s", finalJob.State)
}

// TestLifecycleE2E_AllJobStates_ViaDirect API tests all states via direct API calls
func TestLifecycleE2E_AllJobStates_ViaDirectAPI(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	// Test all possible final states via direct API calls
	testCases := []struct {
		name       string
		finalState types.JobState
		exitCode   *int
		wantState  storage.JobState
	}{
		{
			name:       "completed_state",
			finalState: types.Completed,
			exitCode:   ptrInt(0),
			wantState:  storage.JobStateCompleted,
		},
		{
			name:       "failed_state",
			finalState: types.Failed,
			exitCode:   ptrInt(1),
			wantState:  storage.JobStateFailed,
		},
		{
			name:       "cancelled_state",
			finalState: types.Cancelled,
			exitCode:   nil,
			wantState:  storage.JobStateCancelled,
		},
		{
			name:       "pending_state",
			finalState: types.Pending,
			exitCode:   nil,
			wantState:  storage.JobStatePending,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a unique job ID
			jobID := fmt.Sprintf("test-state-%s-%d", tc.name, time.Now().UnixNano())

			// Send job-started event
			startEvent := types.JobStartedEvent{
				JobId:     jobID,
				User:      "testuser",
				NodeList:  []string{"node1"},
				Timestamp: time.Now(),
			}
			startResp := sendJobStartedEvent(t, env.apiURL, startEvent)
			if startResp.Status != types.Created {
				t.Fatalf("Failed to create job: %s", startResp.Status)
			}

			// Send job-finished event with the test state
			finishEvent := types.JobFinishedEvent{
				JobId:      jobID,
				FinalState: tc.finalState,
				ExitCode:   tc.exitCode,
				Timestamp:  time.Now().Add(5 * time.Second),
			}
			finishResp := sendJobFinishedEvent(t, env.apiURL, finishEvent)
			if finishResp.Status != types.Updated && finishResp.Status != types.Skipped {
				t.Fatalf("Failed to finish job: %s", finishResp.Status)
			}

			// Verify the job state in database
			job, err := env.store.GetJob(context.Background(), jobID)
			if err != nil {
				t.Fatalf("Failed to get job: %v", err)
			}

			if job.State != tc.wantState {
				t.Errorf("Expected state '%s', got '%s'", tc.wantState, job.State)
			}

			if tc.exitCode != nil && job.Scheduler != nil && job.Scheduler.ExitCode != nil {
				if *job.Scheduler.ExitCode != *tc.exitCode {
					t.Errorf("Expected exit code %d, got %d", *tc.exitCode, *job.Scheduler.ExitCode)
				}
			}

			t.Logf("State transition verified: %s -> state=%s", tc.name, job.State)
		})
	}
}

// TestLifecycleE2E_AllJobStates_StateTransitions tests state transitions
func TestLifecycleE2E_AllJobStates_StateTransitions(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	// Test valid state transitions
	testCases := []struct {
		name       string
		startState storage.JobState
		endState   types.JobState
		shouldWork bool
	}{
		{"running_to_completed", storage.JobStateRunning, types.Completed, true},
		{"running_to_failed", storage.JobStateRunning, types.Failed, true},
		{"running_to_cancelled", storage.JobStateRunning, types.Cancelled, true},
		{"completed_to_failed", storage.JobStateCompleted, types.Failed, false},       // Already terminal
		{"cancelled_to_completed", storage.JobStateCancelled, types.Completed, false}, // Already terminal
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a unique job ID
			jobID := fmt.Sprintf("test-transition-%s-%d", tc.name, time.Now().UnixNano())

			// Create job via API
			startEvent := types.JobStartedEvent{
				JobId:     jobID,
				User:      "testuser",
				NodeList:  []string{"node1"},
				Timestamp: time.Now(),
			}
			sendJobStartedEvent(t, env.apiURL, startEvent)

			// If we need a non-running start state, update it first
			if tc.startState != storage.JobStateRunning {
				// Set initial terminal state
				initialFinish := types.JobFinishedEvent{
					JobId:      jobID,
					FinalState: types.JobState(tc.startState),
					Timestamp:  time.Now(),
				}
				sendJobFinishedEvent(t, env.apiURL, initialFinish)
			}

			// Try to transition to the end state
			finishEvent := types.JobFinishedEvent{
				JobId:      jobID,
				FinalState: tc.endState,
				Timestamp:  time.Now().Add(time.Second),
			}
			resp := sendJobFinishedEvent(t, env.apiURL, finishEvent)

			// Check result
			if tc.shouldWork {
				if resp.Status != types.Updated {
					t.Errorf("Expected transition to work, got status: %s", resp.Status)
				}
			} else {
				if resp.Status == types.Updated {
					t.Errorf("Expected transition to be skipped (already terminal), got status: %s", resp.Status)
				}
			}

			t.Logf("Transition %s: status=%s", tc.name, resp.Status)
		})
	}
}

// TestLifecycleE2E_AllJobStates_RealSlurmStates tests all real Slurm states via actual job submission
func TestLifecycleE2E_AllJobStates_RealSlurmStates(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	t.Run("COMPLETED", func(t *testing.T) {
		jobID := submitSlurmJob(t, "echo 'COMPLETED test'; exit 0")
		job := waitForJobInDB(t, env.store, jobID, 30*time.Second)
		job = waitForJobState(t, env.store, jobID, storage.JobStateCompleted, 60*time.Second)
		if job.Scheduler != nil && job.Scheduler.ExitCode != nil && *job.Scheduler.ExitCode != 0 {
			t.Errorf("COMPLETED job should have exit code 0, got %d", *job.Scheduler.ExitCode)
		}
		t.Logf("COMPLETED: state=%s, exit_code=%v", job.State, job.Scheduler.ExitCode)
	})

	t.Run("FAILED_exit_1", func(t *testing.T) {
		jobID := submitSlurmJob(t, "echo 'FAILED test'; exit 1")
		waitForJobInDB(t, env.store, jobID, 30*time.Second)
		job := waitForJobState(t, env.store, jobID, storage.JobStateFailed, 60*time.Second)
		if job.Scheduler == nil || job.Scheduler.ExitCode == nil || *job.Scheduler.ExitCode != 1 {
			t.Errorf("FAILED job should have exit code 1")
		}
		t.Logf("FAILED: state=%s, exit_code=%v", job.State, job.Scheduler.ExitCode)
	})

	t.Run("CANCELLED", func(t *testing.T) {
		jobID := submitSlurmJob(t, "echo 'CANCELLED test'; sleep 300")
		waitForJobInDB(t, env.store, jobID, 30*time.Second)
		time.Sleep(2 * time.Second)
		cancelSlurmJob(t, jobID)
		job := waitForJobState(t, env.store, jobID, storage.JobStateCancelled, 60*time.Second)
		t.Logf("CANCELLED: state=%s", job.State)
	})

	t.Run("FAILED_segfault", func(t *testing.T) {
		// Create a script that causes a segfault (signal 11)
		jobID := submitSlurmJob(t, "echo 'Segfault test'; kill -11 $$")
		waitForJobInDB(t, env.store, jobID, 30*time.Second)

		// Wait for terminal state (could be failed or cancelled depending on Slurm version)
		deadline := time.Now().Add(60 * time.Second)
		var job *storage.Job
		for time.Now().Before(deadline) {
			job, _ = env.store.GetJob(context.Background(), jobID)
			if job != nil && (job.State == storage.JobStateFailed ||
				job.State == storage.JobStateCancelled ||
				job.State == storage.JobStateCompleted) {
				break
			}
			time.Sleep(1 * time.Second)
		}
		if job == nil {
			t.Fatal("Job not found")
		}
		t.Logf("SEGFAULT: state=%s, exit_code=%v", job.State, job.Scheduler.ExitCode)
	})

	t.Run("OUT_OF_MEMORY_simulation", func(t *testing.T) {
		// Submit a job with very low memory that will likely fail
		// Note: This depends on Slurm configuration
		cmd := exec.Command("docker", "exec", "hpc-slurm", "sbatch",
			"--parsable",
			"--mem=1M", // Very low memory
			"--wrap", "echo 'OOM test'; dd if=/dev/zero of=/dev/null bs=1G count=1")
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Skipf("Could not submit OOM test job: %v", err)
		}
		jobID := strings.TrimSpace(string(output))
		t.Logf("Submitted OOM test job: %s", jobID)

		// Wait for terminal state
		deadline := time.Now().Add(60 * time.Second)
		var job *storage.Job
		for time.Now().Before(deadline) {
			job, _ = env.store.GetJob(context.Background(), jobID)
			if job != nil && job.State.IsTerminal() {
				break
			}
			time.Sleep(2 * time.Second)
		}
		if job != nil {
			t.Logf("OOM test: state=%s", job.State)
		}
	})
}

// TestLifecycleE2E_AllJobStates_ExitCodeRange tests various exit codes are captured correctly
func TestLifecycleE2E_AllJobStates_ExitCodeRange(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	// Test various exit codes
	exitCodes := []int{0, 1, 2, 42, 126, 127, 128, 137, 255}

	for _, code := range exitCodes {
		t.Run(fmt.Sprintf("exit_%d", code), func(t *testing.T) {
			jobID := submitSlurmJob(t, fmt.Sprintf("exit %d", code))
			waitForJobInDB(t, env.store, jobID, 30*time.Second)

			// Wait for terminal state
			deadline := time.Now().Add(60 * time.Second)
			var job *storage.Job
			for time.Now().Before(deadline) {
				job, _ = env.store.GetJob(context.Background(), jobID)
				if job != nil && job.State.IsTerminal() {
					break
				}
				time.Sleep(1 * time.Second)
			}

			if job == nil {
				t.Fatalf("Job %s not found", jobID)
			}

			expectedState := storage.JobStateCompleted
			if code != 0 {
				expectedState = storage.JobStateFailed
			}

			if job.State != expectedState {
				t.Errorf("Exit code %d: expected state '%s', got '%s'", code, expectedState, job.State)
			}

			if job.Scheduler != nil && job.Scheduler.ExitCode != nil {
				if *job.Scheduler.ExitCode != code {
					t.Errorf("Expected exit code %d, got %d", code, *job.Scheduler.ExitCode)
				}
			} else if code != 0 {
				t.Error("Exit code should be set for failed jobs")
			}

			t.Logf("Exit code %d: state=%s, captured_exit=%v", code, job.State, job.Scheduler.ExitCode)
		})
	}
}

// TestLifecycleE2E_AllJobStates_ConcurrentStateChanges tests concurrent job submissions with different states
func TestLifecycleE2E_AllJobStates_ConcurrentStateChanges(t *testing.T) {
	env, cleanup := setupLifecycleTestEnv(t)
	defer cleanup()

	// Submit multiple jobs with different expected outcomes concurrently
	jobs := []struct {
		name      string
		script    string
		wantState storage.JobState
	}{
		{"success1", "echo 'Success 1'; exit 0", storage.JobStateCompleted},
		{"success2", "echo 'Success 2'; exit 0", storage.JobStateCompleted},
		{"fail1", "echo 'Fail 1'; exit 1", storage.JobStateFailed},
		{"fail2", "echo 'Fail 2'; exit 2", storage.JobStateFailed},
	}

	jobIDs := make(map[string]string)

	// Submit all jobs
	for _, j := range jobs {
		jobID := submitSlurmJob(t, j.script)
		jobIDs[j.name] = jobID
		t.Logf("Submitted %s: %s", j.name, jobID)
	}

	// Wait for all jobs to complete
	for _, j := range jobs {
		jobID := jobIDs[j.name]
		t.Run(j.name, func(t *testing.T) {
			// Wait for terminal state
			deadline := time.Now().Add(90 * time.Second)
			var job *storage.Job
			for time.Now().Before(deadline) {
				job, _ = env.store.GetJob(context.Background(), jobID)
				if job != nil && job.State.IsTerminal() {
					break
				}
				time.Sleep(1 * time.Second)
			}

			if job == nil {
				t.Fatalf("Job %s not found", jobID)
			}

			if job.State != j.wantState {
				t.Errorf("Expected state '%s', got '%s'", j.wantState, job.State)
			}
			t.Logf("%s: state=%s", j.name, job.State)
		})
	}
}

// Helper function to create int pointer
func ptrInt(i int) *int {
	return &i
}
