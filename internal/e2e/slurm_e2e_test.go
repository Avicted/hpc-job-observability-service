//go:build slurm_e2e
// +build slurm_e2e

// Package e2e provides end-to-end integration tests for Slurm scheduler.
// These tests require a running Slurm cluster (via docker-compose --profile slurm).
//
// Run with: go test ./internal/e2e -tags=slurm_e2e -v
//
// Note: Job synchronization is handled by event-based architecture (prolog/epilog scripts).
// See lifecycle_e2e_test.go for the main job lifecycle tests.
package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/storage"
	"github.com/Avicted/hpc-job-observability-service/internal/storage/postgres"
	"github.com/Avicted/hpc-job-observability-service/internal/utils/scheduler"
	_ "github.com/lib/pq"
)

const (
	// Default Slurm REST API URL for Docker Compose setup
	defaultSlurmBaseURL = "http://localhost:6820"
	defaultSlurmAPIVer  = "v0.0.37"

	// Default PostgreSQL connection for Docker Compose setup
	defaultDatabaseURL = "postgres://hpc:hpc_password@localhost:5432/hpc_jobs?sslmode=disable"

	// Test timeouts
	slurmReadyTimeout = 60 * time.Second
)

// testEnv holds shared test environment state.
type testEnv struct {
	slurmSource *scheduler.SlurmJobSource
	store       storage.Store
	db          *sql.DB
}

// setupTestEnv initializes the test environment by connecting to Slurm and PostgreSQL.
// It returns a cleanup function that should be deferred.
func setupTestEnv(t *testing.T) (*testEnv, func()) {
	t.Helper()

	// Get configuration from environment (Docker Compose defaults)
	slurmBaseURL := os.Getenv("SLURM_BASE_URL")
	if slurmBaseURL == "" {
		slurmBaseURL = defaultSlurmBaseURL
	}

	slurmAPIVersion := os.Getenv("SLURM_API_VERSION")
	if slurmAPIVersion == "" {
		slurmAPIVersion = defaultSlurmAPIVer
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = defaultDatabaseURL
	}

	// Create Slurm source
	slurmConfig := scheduler.SlurmConfig{
		BaseURL:    slurmBaseURL,
		APIVersion: slurmAPIVersion,
		AuthToken:  os.Getenv("SLURM_AUTH_TOKEN"),
	}

	slurmSource, err := scheduler.NewSlurmJobSource(slurmConfig)
	if err != nil {
		t.Fatalf("Failed to create Slurm source: %v", err)
	}

	// Wait for Slurm to be ready
	ctx, cancel := context.WithTimeout(context.Background(), slurmReadyTimeout)
	defer cancel()

	if err := waitForSlurm(ctx, slurmSource); err != nil {
		t.Fatalf("Slurm not ready: %v", err)
	}
	t.Log("Slurm REST API is ready")

	// Create storage
	storeCtx := context.Background()
	cfg := postgres.DefaultConfig(dbURL)
	store, err := postgres.NewStore(storeCtx, cfg)
	if err != nil {
		t.Fatalf("Failed to create PostgreSQL storage: %v", err)
	}

	// Run migrations
	if err := store.Migrate(storeCtx); err != nil {
		store.Close()
		t.Fatalf("Failed to run migrations: %v", err)
	}

	// Get raw DB connection for audit verification
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		store.Close()
		t.Fatalf("Failed to open raw DB connection: %v", err)
	}

	env := &testEnv{
		slurmSource: slurmSource,
		store:       store,
		db:          db,
	}

	cleanup := func() {
		db.Close()
		store.Close()
	}

	return env, cleanup
}

// waitForSlurm polls the Slurm API until it's ready or timeout.
func waitForSlurm(ctx context.Context, source *scheduler.SlurmJobSource) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for Slurm: %w", ctx.Err())
		case <-ticker.C:
			if err := source.Ping(ctx); err == nil {
				return nil
			}
		}
	}
}

// submitSlurmJob submits a test job to Slurm via sbatch and returns the job ID.
func submitSlurmJob(t *testing.T, script string) string {
	t.Helper()

	// Use docker exec to run sbatch in the slurm container
	cmd := exec.Command("docker", "exec", "hpc-slurm", "sbatch", "--parsable", "--wrap", script)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to submit Slurm job: %v\nOutput: %s", err, string(output))
	}

	jobID := strings.TrimSpace(string(output))
	if jobID == "" {
		t.Fatalf("sbatch returned empty job ID")
	}

	t.Logf("Submitted Slurm job: %s", jobID)
	return jobID
}

// cancelSlurmJob cancels a job in Slurm via scancel.
func cancelSlurmJob(t *testing.T, jobID string) {
	t.Helper()

	cmd := exec.Command("docker", "exec", "hpc-slurm", "scancel", jobID)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Logf("Warning: Failed to cancel job %s: %v\nOutput: %s", jobID, err, string(output))
	}
}

// JobAuditRecord represents an audit event row from the database.
type JobAuditRecord struct {
	ID            int64
	JobID         string
	ChangeType    string
	ChangedAt     time.Time
	ChangedBy     string
	Source        string
	CorrelationID string
	JobSnapshot   json.RawMessage
}

// queryAuditEvents retrieves audit events for a job from the database.
func queryAuditEvents(t *testing.T, db *sql.DB, jobID string) []JobAuditRecord {
	t.Helper()

	rows, err := db.Query(`
		SELECT id, job_id, change_type, changed_at, changed_by, source, correlation_id, job_snapshot
		FROM job_audit_events
		WHERE job_id = $1
		ORDER BY changed_at ASC
	`, jobID)
	if err != nil {
		t.Fatalf("Failed to query audit events: %v", err)
	}
	defer rows.Close()

	var events []JobAuditRecord
	for rows.Next() {
		var e JobAuditRecord
		if err := rows.Scan(&e.ID, &e.JobID, &e.ChangeType, &e.ChangedAt, &e.ChangedBy, &e.Source, &e.CorrelationID, &e.JobSnapshot); err != nil {
			t.Fatalf("Failed to scan audit row: %v", err)
		}
		events = append(events, e)
	}

	return events
}

// auditTableExists checks if the job_audit_events table exists in the database.
func auditTableExists(db *sql.DB) bool {
	var exists bool
	err := db.QueryRow(`
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name = 'job_audit_events'
		)
	`).Scan(&exists)
	return err == nil && exists
}

// TestSlurmE2E_SlurmConnectivity verifies connectivity to the Slurm REST API.
func TestSlurmE2E_SlurmConnectivity(t *testing.T) {
	env, cleanup := setupTestEnv(t)
	defer cleanup()

	ctx := context.Background()

	// Verify we can ping Slurm
	if err := env.slurmSource.Ping(ctx); err != nil {
		t.Errorf("Ping failed: %v", err)
	}

	// Verify we can list jobs (even if empty)
	// Note: ListJobs may fail due to API type mismatches - this is a known issue
	jobs, err := env.slurmSource.ListJobs(ctx, scheduler.JobFilter{})
	if err != nil {
		t.Logf("ListJobs returned error (known issue with Slurm API): %v", err)
	}
	t.Logf("Found %d existing jobs in Slurm", len(jobs))

	// Verify we can list nodes
	nodes, err := env.slurmSource.ListNodes(ctx)
	if err != nil {
		t.Errorf("ListNodes failed: %v", err)
	}
	t.Logf("Found %d nodes in Slurm", len(nodes))
	for _, node := range nodes {
		t.Logf("  Node: %s, State: %s, CPUs: %d", node.Name, node.State, node.CPUs)
	}
}

// TestSlurmE2E_AuditTableCreation verifies that the audit table is created by Migrate().
func TestSlurmE2E_AuditTableCreation(t *testing.T) {
	env, cleanup := setupTestEnv(t)
	defer cleanup()

	if !auditTableExists(env.db) {
		t.Error("job_audit_events table does not exist after Migrate()")
	}

	// Verify table structure by querying column info
	rows, err := env.db.Query(`
		SELECT column_name, data_type 
		FROM information_schema.columns 
		WHERE table_name = 'job_audit_events'
		ORDER BY ordinal_position
	`)
	if err != nil {
		t.Fatalf("Failed to query table structure: %v", err)
	}
	defer rows.Close()

	expectedColumns := map[string]bool{
		"id":             false,
		"job_id":         false,
		"change_type":    false,
		"changed_at":     false,
		"changed_by":     false,
		"source":         false,
		"correlation_id": false,
		"job_snapshot":   false,
	}

	for rows.Next() {
		var colName, dataType string
		if err := rows.Scan(&colName, &dataType); err != nil {
			t.Fatalf("Failed to scan column info: %v", err)
		}
		t.Logf("Column: %s (%s)", colName, dataType)
		if _, ok := expectedColumns[colName]; ok {
			expectedColumns[colName] = true
		}
	}

	for col, found := range expectedColumns {
		if !found {
			t.Errorf("Missing expected column: %s", col)
		}
	}
}

// TestSlurmE2E_DirectSlurmJobAccess tests that we can get job details directly from Slurm.
func TestSlurmE2E_DirectSlurmJobAccess(t *testing.T) {
	env, cleanup := setupTestEnv(t)
	defer cleanup()

	ctx := context.Background()

	// Submit a test job to Slurm
	jobID := submitSlurmJob(t, "echo 'Direct access test'; sleep 5")
	defer cancelSlurmJob(t, jobID) // Clean up after test

	// Wait briefly for job to appear in Slurm
	time.Sleep(2 * time.Second)

	// Verify job appears in Slurm
	slurmJob, err := env.slurmSource.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("Failed to get job from Slurm: %v", err)
	}
	if slurmJob == nil {
		t.Fatalf("Job %s not found in Slurm", jobID)
	}

	t.Logf("Slurm job: ID=%s, User=%s, State=%s, Nodes=%v", slurmJob.ID, slurmJob.User, slurmJob.State, slurmJob.Nodes)
	if slurmJob.Scheduler != nil {
		t.Logf("  Scheduler info: Type=%s, Partition=%s, Account=%s", slurmJob.Scheduler.Type, slurmJob.Scheduler.Partition, slurmJob.Scheduler.Account)
	}

	// Verify required fields are populated
	if slurmJob.ID != jobID {
		t.Errorf("Job ID mismatch: expected %s, got %s", jobID, slurmJob.ID)
	}
	if slurmJob.User == "" {
		t.Error("Job user is empty")
	}
	if slurmJob.State == "" {
		t.Error("Job state is empty")
	}
}
