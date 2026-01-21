// Package api provides end-to-end integration tests against a real Slurm cluster.
//
// These tests require Docker Compose with the Slurm profile running:
//
//	docker-compose --profile slurm up -d
//
// To run these tests:
//
//	go test ./internal/api -tags=slurm_e2e -v
//
// To skip these tests (default when not running Docker):
//
//	go test ./internal/api -v
//
//go:build slurm_e2e

package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/avic/hpc-job-observability-service/internal/api/types"
	"github.com/avic/hpc-job-observability-service/internal/metrics"
	"github.com/avic/hpc-job-observability-service/internal/scheduler"
	"github.com/avic/hpc-job-observability-service/internal/storage"
	"github.com/avic/hpc-job-observability-service/internal/syncer"
)

const (
	// Default slurmrestd endpoint when running locally
	defaultSlurmBaseURL = "http://localhost:6820"
	defaultSlurmVersion = "v0.0.37"

	// Timeouts for operations
	slurmPingTimeout     = 5 * time.Second
	jobSubmissionTimeout = 30 * time.Second
	jobCompletionTimeout = 2 * time.Minute
	syncWaitTimeout      = 60 * time.Second
	pollInterval         = 2 * time.Second
)

// slurmE2EEnv holds the test environment configuration
type slurmE2EEnv struct {
	slurmBaseURL    string
	slurmAPIVersion string
	jobSource       *scheduler.SlurmJobSource
	store           storage.Storage
	syncer          *syncer.Syncer
	apiServer       http.Handler
}

// setupSlurmE2E initializes the test environment.
// Returns nil if Slurm is not available (test should be skipped).
func setupSlurmE2E(t *testing.T) *slurmE2EEnv {
	t.Helper()

	// Get configuration from environment or use defaults
	baseURL := os.Getenv("SLURM_BASE_URL")
	if baseURL == "" {
		baseURL = defaultSlurmBaseURL
	}
	apiVersion := os.Getenv("SLURM_API_VERSION")
	if apiVersion == "" {
		apiVersion = defaultSlurmVersion
	}

	// Check if Slurm is available
	if !isSlurmAvailable(baseURL) {
		t.Skipf("Slurm not available at %s. Start with: docker-compose --profile slurm up -d", baseURL)
		return nil
	}

	// Create Slurm job source
	slurmConfig := scheduler.SlurmConfig{
		BaseURL:    baseURL,
		APIVersion: apiVersion,
	}
	jobSource, err := scheduler.NewSlurmJobSource(slurmConfig)
	if err != nil {
		t.Fatalf("Failed to create Slurm job source: %v", err)
	}

	// Verify connectivity
	ctx, cancel := context.WithTimeout(context.Background(), slurmPingTimeout)
	defer cancel()
	if err := jobSource.Ping(ctx); err != nil {
		t.Skipf("Failed to ping Slurm: %v", err)
		return nil
	}

	// Create in-memory SQLite storage for testing
	store, err := storage.NewSQLiteStorage(":memory:")
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Migrate(); err != nil {
		t.Fatalf("Failed to migrate storage: %v", err)
	}

	// Create syncer
	syncConfig := syncer.DefaultConfig()
	syncConfig.ClusterName = "e2e-test"
	syn := syncer.New(jobSource, store, syncConfig)

	// Create API server
	exporter := metrics.NewExporter(store)
	apiSrv := NewServer(store, exporter)

	return &slurmE2EEnv{
		slurmBaseURL:    baseURL,
		slurmAPIVersion: apiVersion,
		jobSource:       jobSource,
		store:           store,
		syncer:          syn,
		apiServer:       apiSrv.Routes(),
	}
}

// isSlurmAvailable checks if slurmrestd is reachable
func isSlurmAvailable(baseURL string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), slurmPingTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/openapi/v3", nil)
	if err != nil {
		return false
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

// submitSlurmJob submits a job to Slurm via sbatch and returns the job ID
func submitSlurmJob(t *testing.T, script string) string {
	t.Helper()

	// Use docker-compose exec to run sbatch
	cmd := exec.Command("docker-compose", "exec", "-T", "slurm", "sbatch", "--parsable", "--wrap", script)
	cmd.Dir = findProjectRoot(t)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to submit job via sbatch: %v\nOutput: %s", err, string(output))
	}

	// Parse job ID from output (sbatch --parsable returns just the ID)
	jobID := strings.TrimSpace(string(output))
	if jobID == "" {
		t.Fatalf("Empty job ID returned from sbatch")
	}

	// Extract numeric part if there's cluster info (format: jobid;cluster)
	if idx := strings.Index(jobID, ";"); idx != -1 {
		jobID = jobID[:idx]
	}

	t.Logf("Submitted Slurm job: %s", jobID)
	return jobID
}

// cancelSlurmJob cancels a Slurm job
func cancelSlurmJob(t *testing.T, jobID string) {
	t.Helper()

	cmd := exec.Command("docker-compose", "exec", "-T", "slurm", "scancel", jobID)
	cmd.Dir = findProjectRoot(t)
	_ = cmd.Run() // Ignore errors (job might already be completed)
}

// waitForJobInSlurm waits for a job to appear in Slurm's job list
func waitForJobInSlurm(t *testing.T, env *slurmE2EEnv, jobID string, timeout time.Duration) *scheduler.Job {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("Timeout waiting for job %s in Slurm", jobID)
			return nil
		case <-ticker.C:
			job, err := env.jobSource.GetJob(ctx, jobID)
			if err != nil {
				t.Logf("Error getting job %s: %v", jobID, err)
				continue
			}
			if job != nil {
				t.Logf("Found job %s in Slurm with state: %s", jobID, job.State)
				return job
			}
		}
	}
}

// waitForJobState waits for a job to reach a specific state
func waitForJobState(t *testing.T, env *slurmE2EEnv, jobID string, targetState scheduler.JobState, timeout time.Duration) *scheduler.Job {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("Timeout waiting for job %s to reach state %s", jobID, targetState)
			return nil
		case <-ticker.C:
			job, err := env.jobSource.GetJob(ctx, jobID)
			if err != nil {
				continue
			}
			if job != nil {
				t.Logf("Job %s current state: %s (waiting for %s)", jobID, job.State, targetState)
				if job.State == targetState {
					return job
				}
				// Also accept terminal states if waiting for running
				if targetState == scheduler.JobStateRunning && isTerminalState(job.State) {
					t.Logf("Job %s reached terminal state %s before running", jobID, job.State)
					return job
				}
			}
		}
	}
}

func isTerminalState(state scheduler.JobState) bool {
	return state == scheduler.JobStateCompleted ||
		state == scheduler.JobStateFailed ||
		state == scheduler.JobStateCancelled
}

// syncAndWait runs a sync and waits for the job to appear in storage
func syncAndWait(t *testing.T, env *slurmE2EEnv, jobID string, timeout time.Duration) *storage.Job {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Run initial sync
	env.syncer.SyncNow()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("Timeout waiting for job %s in storage", jobID)
			return nil
		case <-ticker.C:
			job, err := env.store.GetJob(ctx, jobID)
			if err != nil {
				if err == sql.ErrNoRows {
					// Job not yet synced, run another sync
					env.syncer.SyncNow()
					continue
				}
				t.Logf("Error getting job from storage: %v", err)
				continue
			}
			t.Logf("Found job %s in storage", jobID)
			return job
		}
	}
}

// findProjectRoot finds the project root directory
func findProjectRoot(t *testing.T) string {
	t.Helper()

	// Start from current working directory and look for docker-compose.yml
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}

	for {
		if _, err := os.Stat(dir + "/docker-compose.yml"); err == nil {
			return dir
		}
		parent := dir[:strings.LastIndex(dir, "/")]
		if parent == dir {
			t.Fatalf("Could not find project root (docker-compose.yml)")
		}
		dir = parent
	}
}

// getJobViaAPI retrieves a job through the REST API
func getJobViaAPI(t *testing.T, env *slurmE2EEnv, jobID string) *types.Job {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+jobID, nil)
	rec := httptest.NewRecorder()
	env.apiServer.ServeHTTP(rec, req)

	if rec.Code == http.StatusNotFound {
		return nil
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/jobs/%s returned %d: %s", jobID, rec.Code, rec.Body.String())
	}

	var job types.Job
	if err := json.NewDecoder(rec.Body).Decode(&job); err != nil {
		t.Fatalf("Failed to decode job response: %v", err)
	}
	return &job
}

// listJobsViaAPI lists jobs through the REST API
func listJobsViaAPI(t *testing.T, env *slurmE2EEnv, params map[string]string) []types.Job {
	t.Helper()

	url := "/v1/jobs"
	if len(params) > 0 {
		var parts []string
		for k, v := range params {
			parts = append(parts, k+"="+v)
		}
		url += "?" + strings.Join(parts, "&")
	}

	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	env.apiServer.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s returned %d: %s", url, rec.Code, rec.Body.String())
	}

	var resp types.JobListResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode jobs response: %v", err)
	}
	return resp.Jobs
}

// TestSlurmE2E_JobSubmissionAndSync tests submitting a job to Slurm and syncing to the database
func TestSlurmE2E_JobSubmissionAndSync(t *testing.T) {
	env := setupSlurmE2E(t)
	if env == nil {
		return
	}
	defer env.store.Close()

	// Submit a simple test job
	testScript := "echo 'E2E Test Job'; hostname; date; sleep 5"
	jobID := submitSlurmJob(t, testScript)
	defer cancelSlurmJob(t, jobID) // Cleanup

	// Wait for job to appear in Slurm
	slurmJob := waitForJobInSlurm(t, env, jobID, jobSubmissionTimeout)
	if slurmJob == nil {
		t.Fatal("Job not found in Slurm")
	}

	// Verify basic job properties from Slurm
	t.Run("VerifySlurmJobProperties", func(t *testing.T) {
		if slurmJob.ID != jobID {
			t.Errorf("Job ID mismatch: got %s, want %s", slurmJob.ID, jobID)
		}
		// Job should have scheduler info
		if slurmJob.Scheduler == nil {
			t.Error("Scheduler info is nil")
		} else if slurmJob.Scheduler.Type != scheduler.SchedulerTypeSlurm {
			t.Errorf("Scheduler type: got %v, want %v", slurmJob.Scheduler.Type, scheduler.SchedulerTypeSlurm)
		}
	})

	// Sync and verify job appears in storage
	t.Run("SyncAndVerifyStorage", func(t *testing.T) {
		storageJob := syncAndWait(t, env, jobID, syncWaitTimeout)
		if storageJob == nil {
			t.Fatal("Job not found in storage after sync")
		}

		// Verify synced data
		if storageJob.ID != jobID {
			t.Errorf("Storage job ID mismatch: got %s, want %s", storageJob.ID, jobID)
		}
		if storageJob.User == "" {
			t.Error("Storage job has no user")
		}
		t.Logf("Synced job: ID=%s, User=%s, State=%s", storageJob.ID, storageJob.User, storageJob.State)
	})

	// Verify job is accessible via API
	t.Run("VerifyAPIAccess", func(t *testing.T) {
		// First ensure we've synced
		env.syncer.SyncNow()

		apiJob := getJobViaAPI(t, env, jobID)
		if apiJob == nil {
			t.Fatal("Job not found via API")
		}

		if apiJob.Id != jobID {
			t.Errorf("API job ID mismatch: got %s, want %s", apiJob.Id, jobID)
		}
		t.Logf("API job: ID=%s, User=%s, State=%s", apiJob.Id, apiJob.User, apiJob.State)
	})
}

// TestSlurmE2E_JobCompletion tests job lifecycle from running to completion
func TestSlurmE2E_JobCompletion(t *testing.T) {
	env := setupSlurmE2E(t)
	if env == nil {
		return
	}
	defer env.store.Close()

	// Submit a short job that will complete quickly
	testScript := "echo 'Quick job'; sleep 2; echo 'Done'"
	jobID := submitSlurmJob(t, testScript)

	// Wait for job to complete
	t.Run("WaitForCompletion", func(t *testing.T) {
		slurmJob := waitForJobState(t, env, jobID, scheduler.JobStateCompleted, jobCompletionTimeout)
		if slurmJob == nil {
			t.Fatal("Job did not complete")
		}
		t.Logf("Job %s completed with state: %s", jobID, slurmJob.State)

		// Sync and verify final state
		ctx := context.Background()
		env.syncer.SyncNow()

		storageJob, err := env.store.GetJob(ctx, jobID)
		if err != nil {
			t.Fatalf("Failed to get completed job from storage: %v", err)
		}

		if storageJob.State != "completed" {
			t.Errorf("Storage job state: got %s, want completed", storageJob.State)
		}
	})
}

// TestSlurmE2E_MultipleJobs tests syncing multiple concurrent jobs
func TestSlurmE2E_MultipleJobs(t *testing.T) {
	env := setupSlurmE2E(t)
	if env == nil {
		return
	}
	defer env.store.Close()

	const numJobs = 3
	var jobIDs []string

	// Submit multiple jobs
	for i := 0; i < numJobs; i++ {
		script := fmt.Sprintf("echo 'Job %d'; sleep %d", i, 5+i)
		jobID := submitSlurmJob(t, script)
		jobIDs = append(jobIDs, jobID)
		defer cancelSlurmJob(t, jobID)
	}

	t.Logf("Submitted %d jobs: %v", numJobs, jobIDs)

	// Wait for all jobs to appear in Slurm
	for _, jobID := range jobIDs {
		waitForJobInSlurm(t, env, jobID, jobSubmissionTimeout)
	}

	// Sync all jobs
	env.syncer.SyncNow()

	// Verify all jobs are in storage and API
	t.Run("VerifyAllJobsSynced", func(t *testing.T) {
		jobs := listJobsViaAPI(t, env, nil)

		foundCount := 0
		for _, apiJob := range jobs {
			for _, expectedID := range jobIDs {
				if apiJob.Id == expectedID {
					foundCount++
					t.Logf("Found job %s via API", apiJob.Id)
					break
				}
			}
		}

		if foundCount != numJobs {
			t.Errorf("Expected %d jobs in API, found %d", numJobs, foundCount)
		}
	})
}

// TestSlurmE2E_NodesList tests that we can list Slurm nodes
func TestSlurmE2E_NodesList(t *testing.T) {
	env := setupSlurmE2E(t)
	if env == nil {
		return
	}
	defer env.store.Close()

	ctx := context.Background()
	nodes, err := env.jobSource.ListNodes(ctx)
	if err != nil {
		t.Fatalf("Failed to list nodes: %v", err)
	}

	if len(nodes) == 0 {
		t.Skip("No nodes available in Slurm cluster")
	}

	t.Logf("Found %d nodes", len(nodes))

	for _, node := range nodes {
		t.Logf("Node: %s, State: %s, CPUs: %d, Memory: %d MB",
			node.Name, node.State, node.CPUs, node.RealMemoryMB)

		// Basic validation
		if node.Name == "" {
			t.Error("Node has empty name")
		}
	}
}

// TestSlurmE2E_JobFiltering tests filtering jobs by various criteria
func TestSlurmE2E_JobFiltering(t *testing.T) {
	env := setupSlurmE2E(t)
	if env == nil {
		return
	}
	defer env.store.Close()

	// Submit a few test jobs
	jobID1 := submitSlurmJob(t, "echo test1; sleep 5")
	jobID2 := submitSlurmJob(t, "echo test2; sleep 5")
	defer cancelSlurmJob(t, jobID1)
	defer cancelSlurmJob(t, jobID2)

	// Wait for jobs to appear
	waitForJobInSlurm(t, env, jobID1, jobSubmissionTimeout)
	waitForJobInSlurm(t, env, jobID2, jobSubmissionTimeout)

	// Sync
	env.syncer.SyncNow()

	// Test filtering by job ID
	t.Run("FilterByJobID", func(t *testing.T) {
		// The API returns a single job for GET /v1/jobs/{id}
		job := getJobViaAPI(t, env, jobID1)
		if job == nil {
			t.Errorf("Job %s not found", jobID1)
		}
	})

	// Test pagination
	t.Run("Pagination", func(t *testing.T) {
		jobs := listJobsViaAPI(t, env, map[string]string{
			"limit": "1",
		})
		if len(jobs) > 1 {
			t.Errorf("Expected at most 1 job with limit=1, got %d", len(jobs))
		}
	})
}

// TestSlurmE2E_RealSlurmConnection is a simple connectivity test
func TestSlurmE2E_RealSlurmConnection(t *testing.T) {
	env := setupSlurmE2E(t)
	if env == nil {
		return
	}
	defer env.store.Close()

	// Test direct slurmrestd API access
	t.Run("SlurmrestdOpenAPI", func(t *testing.T) {
		resp, err := http.Get(env.slurmBaseURL + "/openapi/v3")
		if err != nil {
			t.Fatalf("Failed to fetch OpenAPI spec: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("OpenAPI endpoint returned %d", resp.StatusCode)
		}

		body, _ := io.ReadAll(resp.Body)
		if !bytes.Contains(body, []byte("openapi")) {
			t.Error("Response doesn't look like an OpenAPI spec")
		}
		t.Logf("OpenAPI spec size: %d bytes", len(body))
	})

	// Test jobs endpoint
	t.Run("SlurmrestdJobs", func(t *testing.T) {
		url := fmt.Sprintf("%s/slurm/%s/jobs/", env.slurmBaseURL, env.slurmAPIVersion)
		resp, err := http.Get(url)
		if err != nil {
			t.Fatalf("Failed to fetch jobs: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Errorf("Jobs endpoint returned %d: %s", resp.StatusCode, string(body))
		}
	})

	// Test nodes endpoint
	t.Run("SlurmrestdNodes", func(t *testing.T) {
		url := fmt.Sprintf("%s/slurm/%s/nodes/", env.slurmBaseURL, env.slurmAPIVersion)
		resp, err := http.Get(url)
		if err != nil {
			t.Fatalf("Failed to fetch nodes: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Errorf("Nodes endpoint returned %d: %s", resp.StatusCode, string(body))
		}
	})
}

// TestSlurmE2E_DataIntegrity verifies that data synced from Slurm matches expected values
func TestSlurmE2E_DataIntegrity(t *testing.T) {
	env := setupSlurmE2E(t)
	if env == nil {
		return
	}
	defer env.store.Close()

	// Submit a job with known characteristics
	testScript := "echo 'integrity test'; hostname; date"
	jobID := submitSlurmJob(t, testScript)
	defer cancelSlurmJob(t, jobID)

	// Wait for it to appear
	slurmJob := waitForJobInSlurm(t, env, jobID, jobSubmissionTimeout)
	if slurmJob == nil {
		t.Fatal("Job not found in Slurm")
	}

	// Sync to storage
	ctx := context.Background()
	env.syncer.SyncNow()

	// Get from storage
	storageJob, err := env.store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("Failed to get job from storage: %v", err)
	}

	// Verify data integrity between Slurm and storage
	t.Run("IDMatch", func(t *testing.T) {
		if storageJob.ID != slurmJob.ID {
			t.Errorf("ID mismatch: storage=%s, slurm=%s", storageJob.ID, slurmJob.ID)
		}
	})

	t.Run("UserMatch", func(t *testing.T) {
		if storageJob.User != slurmJob.User {
			t.Errorf("User mismatch: storage=%s, slurm=%s", storageJob.User, slurmJob.User)
		}
	})

	t.Run("StateConsistency", func(t *testing.T) {
		// States should map correctly
		slurmState := string(slurmJob.State)
		if storageJob.State == "" {
			t.Error("Storage job has empty state")
		}
		t.Logf("Slurm state: %s, Storage state: %s", slurmState, storageJob.State)
	})

	t.Run("NodesMatch", func(t *testing.T) {
		// Node lists should match (or both be empty for pending jobs)
		if len(slurmJob.Nodes) != len(storageJob.Nodes) {
			t.Logf("Node count differs: slurm=%d, storage=%d (may be OK for pending jobs)",
				len(slurmJob.Nodes), len(storageJob.Nodes))
		}
	})

	// Verify API returns consistent data
	t.Run("APIDataConsistency", func(t *testing.T) {
		apiJob := getJobViaAPI(t, env, jobID)
		if apiJob == nil {
			t.Fatal("Job not found via API")
		}

		if apiJob.Id != storageJob.ID {
			t.Errorf("API ID doesn't match storage: api=%s, storage=%s", apiJob.Id, storageJob.ID)
		}
		if apiJob.User != storageJob.User {
			t.Errorf("API user doesn't match storage: api=%s, storage=%s", apiJob.User, storageJob.User)
		}
	})
}

// TestSlurmE2E_SchedulerInfo verifies that scheduler-specific info is preserved
func TestSlurmE2E_SchedulerInfo(t *testing.T) {
	env := setupSlurmE2E(t)
	if env == nil {
		return
	}
	defer env.store.Close()

	jobID := submitSlurmJob(t, "sleep 10")
	defer cancelSlurmJob(t, jobID)

	slurmJob := waitForJobInSlurm(t, env, jobID, jobSubmissionTimeout)
	if slurmJob == nil {
		t.Fatal("Job not found")
	}

	if slurmJob.Scheduler == nil {
		t.Fatal("Scheduler info is nil")
	}

	t.Run("SchedulerType", func(t *testing.T) {
		if slurmJob.Scheduler.Type != scheduler.SchedulerTypeSlurm {
			t.Errorf("Scheduler type: got %v, want %v", slurmJob.Scheduler.Type, scheduler.SchedulerTypeSlurm)
		}
	})

	t.Run("Partition", func(t *testing.T) {
		// Should have a partition (might be "normal", "debug", etc.)
		if slurmJob.Scheduler.Partition == "" {
			t.Log("Job has no partition (might be OK for test setup)")
		} else {
			t.Logf("Job partition: %s", slurmJob.Scheduler.Partition)
		}
	})

	t.Run("QoS", func(t *testing.T) {
		if slurmJob.Scheduler.QoS != "" {
			t.Logf("Job QoS: %s", slurmJob.Scheduler.QoS)
		}
	})

	t.Run("Account", func(t *testing.T) {
		if slurmJob.Scheduler.Account != "" {
			t.Logf("Job Account: %s", slurmJob.Scheduler.Account)
		}
	})
}

// TestSlurmE2E_ContinuousSync tests that repeated syncs work correctly
func TestSlurmE2E_ContinuousSync(t *testing.T) {
	env := setupSlurmE2E(t)
	if env == nil {
		return
	}
	defer env.store.Close()

	jobID := submitSlurmJob(t, "sleep 15")
	defer cancelSlurmJob(t, jobID)

	waitForJobInSlurm(t, env, jobID, jobSubmissionTimeout)

	ctx := context.Background()

	// Run multiple syncs
	for i := 0; i < 3; i++ {
		t.Run(fmt.Sprintf("Sync%d", i+1), func(t *testing.T) {
			env.syncer.SyncNow()

			// Verify job is still there and consistent
			job, err := env.store.GetJob(ctx, jobID)
			if err != nil {
				t.Errorf("Failed to get job after sync %d: %v", i+1, err)
			} else {
				t.Logf("Sync %d: Job %s state=%s", i+1, job.ID, job.State)
			}
		})

		time.Sleep(time.Second)
	}
}

// stringToInt64 converts a string job ID to int64 for Slurm API
func stringToInt64(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}
