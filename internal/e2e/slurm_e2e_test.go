//go:build slurm_e2e

// Package e2e provides end-to-end integration tests for Slurm scheduler.
// These tests require a running Slurm cluster (via docker-compose --profile slurm).
//
// Run with: go test ./internal/e2e -tags=slurm_e2e -v
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

	"github.com/avic/hpc-job-observability-service/internal/scheduler"
	"github.com/avic/hpc-job-observability-service/internal/storage"
	"github.com/avic/hpc-job-observability-service/internal/syncer"
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
	jobSyncTimeout    = 30 * time.Second
)

// testEnv holds shared test environment state.
type testEnv struct {
	slurmSource *scheduler.SlurmJobSource
	store       *storage.PostgresStorage
	db          *sql.DB
	syncerInst  *syncer.Syncer
}

// setupTestEnv initializes the test environment by connecting to Slurm and PostgreSQL.
// It returns a cleanup function that should be deferred.
func setupTestEnv(t *testing.T) (*testEnv, func()) {
	t.Helper()

	// Get configuration from environment or use defaults
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

	// Create Slurm job source
	slurmConfig := scheduler.SlurmConfig{
		BaseURL:     slurmBaseURL,
		APIVersion:  slurmAPIVersion,
		ClusterName: "e2e-test",
	}

	slurmSource, err := scheduler.NewSlurmJobSource(slurmConfig)
	if err != nil {
		t.Skipf("Cannot create Slurm job source (Slurm not available?): %v", err)
	}

	// Check Slurm is reachable
	ctx, cancel := context.WithTimeout(context.Background(), slurmReadyTimeout)
	defer cancel()

	if err := waitForSlurm(ctx, slurmSource); err != nil {
		t.Skipf("Slurm not ready within timeout: %v", err)
	}

	// Connect to PostgreSQL
	store, err := storage.NewPostgresStorage(dbURL)
	if err != nil {
		t.Skipf("Cannot connect to PostgreSQL: %v", err)
	}

	// Run migrations to ensure schema exists (including job_audit_events)
	if err := store.Migrate(); err != nil {
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
		if env.syncerInst != nil {
			env.syncerInst.Stop()
		}
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
	jobs, err := env.slurmSource.ListJobs(ctx, scheduler.JobFilter{})
	if err != nil {
		t.Errorf("ListJobs failed: %v", err)
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

// TestSlurmE2E_JobSubmissionAndSync tests submitting a job to Slurm and syncing to DB.
func TestSlurmE2E_JobSubmissionAndSync(t *testing.T) {
	env, cleanup := setupTestEnv(t)
	defer cleanup()

	ctx := context.Background()

	// Submit a test job to Slurm
	jobID := submitSlurmJob(t, "echo 'E2E test job'; sleep 5")
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

	// Create syncer and run a single sync
	syncConfig := syncer.Config{
		SyncInterval:      5 * time.Second,
		InitialDelay:      0,
		ClusterName:       "e2e-test",
		SchedulerInstance: "slurm-e2e",
		IngestVersion:     "e2e-test-v1",
	}
	env.syncerInst = syncer.New(env.slurmSource, env.store, syncConfig)
	env.syncerInst.SyncNow()

	// Verify job is now in the database
	dbJob, err := env.store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("Failed to get job from DB: %v", err)
	}

	t.Logf("DB job: ID=%s, User=%s, State=%s, ClusterName=%s", dbJob.ID, dbJob.User, dbJob.State, dbJob.ClusterName)

	// Verify job data matches Slurm
	if dbJob.User != slurmJob.User {
		t.Errorf("User mismatch: DB=%s, Slurm=%s", dbJob.User, slurmJob.User)
	}
	if dbJob.ClusterName != "e2e-test" {
		t.Errorf("ClusterName=%s, want e2e-test", dbJob.ClusterName)
	}
	if dbJob.IngestVersion != "e2e-test-v1" {
		t.Errorf("IngestVersion=%s, want e2e-test-v1", dbJob.IngestVersion)
	}

	// Verify scheduler metadata is preserved
	if dbJob.Scheduler == nil {
		t.Error("Scheduler info is nil in DB")
	} else {
		if dbJob.Scheduler.Type != storage.SchedulerTypeSlurm {
			t.Errorf("Scheduler.Type=%s, want slurm", dbJob.Scheduler.Type)
		}
	}
}

// TestSlurmE2E_AuditEventCreation tests that audit events are created during sync.
func TestSlurmE2E_AuditEventCreation(t *testing.T) {
	env, cleanup := setupTestEnv(t)
	defer cleanup()

	// Submit a unique test job
	jobID := submitSlurmJob(t, "echo 'Audit test job'; sleep 3")
	defer cancelSlurmJob(t, jobID)

	// Wait for job to appear
	time.Sleep(2 * time.Second)

	// Sync job to database
	syncConfig := syncer.Config{
		SyncInterval:      5 * time.Second,
		InitialDelay:      0,
		ClusterName:       "audit-e2e-test",
		SchedulerInstance: "slurm-audit-e2e",
		IngestVersion:     "audit-e2e-v1",
	}
	env.syncerInst = syncer.New(env.slurmSource, env.store, syncConfig)
	env.syncerInst.SyncNow()

	// Query audit events for this job
	events := queryAuditEvents(t, env.db, jobID)
	if len(events) == 0 {
		t.Fatalf("No audit events found for job %s", jobID)
	}

	t.Logf("Found %d audit event(s) for job %s", len(events), jobID)

	// Verify first audit event (should be 'create' or 'upsert')
	firstEvent := events[0]
	t.Logf("First audit event: type=%s, by=%s, source=%s, correlation=%s",
		firstEvent.ChangeType, firstEvent.ChangedBy, firstEvent.Source, firstEvent.CorrelationID)

	// Verify audit metadata from syncer
	if firstEvent.ChangedBy != "syncer" {
		t.Errorf("ChangedBy=%s, want syncer", firstEvent.ChangedBy)
	}
	if firstEvent.Source != "slurm" {
		t.Errorf("Source=%s, want slurm", firstEvent.Source)
	}
	if firstEvent.CorrelationID == "" {
		t.Error("CorrelationID is empty")
	}

	// Verify job snapshot is valid JSON
	var snapshot map[string]interface{}
	if err := json.Unmarshal(firstEvent.JobSnapshot, &snapshot); err != nil {
		t.Errorf("Invalid job snapshot JSON: %v", err)
	} else {
		// Check some expected fields in snapshot
		if snapshotID, ok := snapshot["id"].(string); !ok || snapshotID != jobID {
			t.Errorf("Snapshot ID=%v, want %s", snapshot["id"], jobID)
		}
		t.Logf("Job snapshot keys: %v", mapKeys(snapshot))
	}
}

// TestSlurmE2E_JobStateTransition tests job state changes are captured in audit.
func TestSlurmE2E_JobStateTransition(t *testing.T) {
	env, cleanup := setupTestEnv(t)
	defer cleanup()

	// Submit a job that completes quickly
	jobID := submitSlurmJob(t, "echo 'Quick job'; sleep 1")

	// Initial sync to capture PENDING/RUNNING state
	syncConfig := syncer.Config{
		SyncInterval:      5 * time.Second,
		InitialDelay:      0,
		ClusterName:       "transition-e2e",
		SchedulerInstance: "slurm-transition-e2e",
		IngestVersion:     "transition-e2e-v1",
	}
	env.syncerInst = syncer.New(env.slurmSource, env.store, syncConfig)
	env.syncerInst.SyncNow()

	// Wait for job to complete
	t.Log("Waiting for job to complete...")
	time.Sleep(5 * time.Second)

	// Sync again to capture COMPLETED state
	env.syncerInst.SyncNow()

	// Query final job state
	ctx := context.Background()
	dbJob, err := env.store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("Failed to get job from DB: %v", err)
	}

	t.Logf("Final job state: %s", dbJob.State)

	// Query all audit events for this job
	events := queryAuditEvents(t, env.db, jobID)
	t.Logf("Total audit events: %d", len(events))

	for i, e := range events {
		var snapshot map[string]interface{}
		json.Unmarshal(e.JobSnapshot, &snapshot)
		state := snapshot["state"]
		t.Logf("  Event %d: type=%s, state=%v, time=%s", i+1, e.ChangeType, state, e.ChangedAt.Format(time.RFC3339))
	}

	// Verify we have audit trail
	if len(events) < 1 {
		t.Error("Expected at least one audit event for job lifecycle")
	}
}

// TestSlurmE2E_MultipleJobsSync tests syncing multiple jobs and their audit trails.
func TestSlurmE2E_MultipleJobsSync(t *testing.T) {
	env, cleanup := setupTestEnv(t)
	defer cleanup()

	// Submit multiple jobs
	jobIDs := make([]string, 3)
	for i := 0; i < 3; i++ {
		script := fmt.Sprintf("echo 'Multi-job test %d'; sleep 2", i+1)
		jobIDs[i] = submitSlurmJob(t, script)
	}

	// Clean up all jobs after test
	defer func() {
		for _, id := range jobIDs {
			cancelSlurmJob(t, id)
		}
	}()

	// Wait for jobs to appear
	time.Sleep(2 * time.Second)

	// Sync all jobs
	syncConfig := syncer.Config{
		SyncInterval:      5 * time.Second,
		InitialDelay:      0,
		ClusterName:       "multi-e2e",
		SchedulerInstance: "slurm-multi-e2e",
		IngestVersion:     "multi-e2e-v1",
	}
	env.syncerInst = syncer.New(env.slurmSource, env.store, syncConfig)
	env.syncerInst.SyncNow()

	// Verify all jobs have audit events with same correlation ID
	var correlationIDs []string
	for _, jobID := range jobIDs {
		events := queryAuditEvents(t, env.db, jobID)
		if len(events) == 0 {
			t.Errorf("No audit events for job %s", jobID)
			continue
		}
		correlationIDs = append(correlationIDs, events[0].CorrelationID)
		t.Logf("Job %s: correlation_id=%s", jobID, events[0].CorrelationID)
	}

	// All jobs from same sync batch should share correlation ID
	if len(correlationIDs) >= 2 {
		if correlationIDs[0] != correlationIDs[1] {
			t.Errorf("Jobs from same sync have different correlation IDs: %s vs %s",
				correlationIDs[0], correlationIDs[1])
		} else {
			t.Logf("All jobs share correlation ID: %s", correlationIDs[0])
		}
	}
}

// TestSlurmE2E_SchedulerMetadataPreservation tests that Slurm scheduler metadata is preserved in DB.
func TestSlurmE2E_SchedulerMetadataPreservation(t *testing.T) {
	env, cleanup := setupTestEnv(t)
	defer cleanup()

	ctx := context.Background()

	// Submit a job with specific partition (if available)
	jobID := submitSlurmJob(t, "echo 'Metadata test'; sleep 2")
	defer cancelSlurmJob(t, jobID)

	time.Sleep(2 * time.Second)

	// Get job from Slurm
	slurmJob, err := env.slurmSource.GetJob(ctx, jobID)
	if err != nil || slurmJob == nil {
		t.Fatalf("Failed to get job from Slurm: %v", err)
	}

	// Sync to DB
	syncConfig := syncer.Config{
		SyncInterval:  5 * time.Second,
		InitialDelay:  0,
		ClusterName:   "metadata-e2e",
		IngestVersion: "metadata-e2e-v1",
	}
	env.syncerInst = syncer.New(env.slurmSource, env.store, syncConfig)
	env.syncerInst.SyncNow()

	// Get job from DB
	dbJob, err := env.store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("Failed to get job from DB: %v", err)
	}

	// Compare scheduler metadata
	if slurmJob.Scheduler == nil || dbJob.Scheduler == nil {
		t.Fatal("Scheduler info is nil")
	}

	t.Logf("Slurm scheduler: Type=%s, Partition=%s, RawState=%s",
		slurmJob.Scheduler.Type, slurmJob.Scheduler.Partition, slurmJob.Scheduler.RawState)
	t.Logf("DB scheduler: Type=%s, Partition=%s, RawState=%s",
		dbJob.Scheduler.Type, dbJob.Scheduler.Partition, dbJob.Scheduler.RawState)

	// Verify key metadata fields are preserved
	if string(dbJob.Scheduler.Type) != string(slurmJob.Scheduler.Type) {
		t.Errorf("Scheduler.Type mismatch: DB=%s, Slurm=%s",
			dbJob.Scheduler.Type, slurmJob.Scheduler.Type)
	}
	if dbJob.Scheduler.Partition != slurmJob.Scheduler.Partition {
		t.Errorf("Scheduler.Partition mismatch: DB=%s, Slurm=%s",
			dbJob.Scheduler.Partition, slurmJob.Scheduler.Partition)
	}
	if dbJob.Scheduler.RawState != slurmJob.Scheduler.RawState {
		t.Errorf("Scheduler.RawState mismatch: DB=%s, Slurm=%s",
			dbJob.Scheduler.RawState, slurmJob.Scheduler.RawState)
	}

	// Verify audit snapshot contains scheduler info
	events := queryAuditEvents(t, env.db, jobID)
	if len(events) > 0 {
		var snapshot map[string]interface{}
		json.Unmarshal(events[0].JobSnapshot, &snapshot)
		if schedInfo, ok := snapshot["scheduler"].(map[string]interface{}); ok {
			t.Logf("Audit snapshot scheduler: %v", schedInfo)
		} else {
			t.Error("Audit snapshot missing scheduler info")
		}
	}
}

// TestSlurmE2E_DataIntegrity tests data consistency between Slurm, storage, and audit.
func TestSlurmE2E_DataIntegrity(t *testing.T) {
	env, cleanup := setupTestEnv(t)
	defer cleanup()

	ctx := context.Background()

	// Submit a job
	jobID := submitSlurmJob(t, "hostname; whoami; sleep 3")
	defer cancelSlurmJob(t, jobID)

	time.Sleep(2 * time.Second)

	// Get job from Slurm
	slurmJob, err := env.slurmSource.GetJob(ctx, jobID)
	if err != nil || slurmJob == nil {
		t.Fatalf("Failed to get job from Slurm: %v", err)
	}

	// Sync to DB
	syncConfig := syncer.Config{
		SyncInterval:  5 * time.Second,
		InitialDelay:  0,
		ClusterName:   "integrity-e2e",
		IngestVersion: "integrity-e2e-v1",
	}
	env.syncerInst = syncer.New(env.slurmSource, env.store, syncConfig)
	env.syncerInst.SyncNow()

	// Get job from DB
	dbJob, err := env.store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("Failed to get job from DB: %v", err)
	}

	// Verify DB job matches Slurm job
	if dbJob.ID != slurmJob.ID {
		t.Errorf("ID mismatch: DB=%s, Slurm=%s", dbJob.ID, slurmJob.ID)
	}
	if dbJob.User != slurmJob.User {
		t.Errorf("User mismatch: DB=%s, Slurm=%s", dbJob.User, slurmJob.User)
	}
	// State may differ slightly due to timing, but should be consistent
	t.Logf("Slurm state: %s, DB state: %s", slurmJob.State, dbJob.State)

	// Verify audit snapshot matches DB job
	events := queryAuditEvents(t, env.db, jobID)
	if len(events) == 0 {
		t.Fatal("No audit events found")
	}

	var snapshot storage.JobSnapshot
	if err := json.Unmarshal(events[len(events)-1].JobSnapshot, &snapshot); err != nil {
		t.Fatalf("Failed to unmarshal snapshot: %v", err)
	}

	if snapshot.ID != dbJob.ID {
		t.Errorf("Snapshot ID mismatch: snapshot=%s, DB=%s", snapshot.ID, dbJob.ID)
	}
	if snapshot.User != dbJob.User {
		t.Errorf("Snapshot User mismatch: snapshot=%s, DB=%s", snapshot.User, dbJob.User)
	}

	t.Logf("Data integrity verified: Slurm -> DB -> Audit snapshot")
}

// TestSlurmE2E_CancelledJobState tests that cancelled jobs are correctly tracked in DB and audit.
func TestSlurmE2E_CancelledJobState(t *testing.T) {
	env, cleanup := setupTestEnv(t)
	defer cleanup()

	ctx := context.Background()

	// Submit a long-running job that we will cancel
	jobID := submitSlurmJob(t, "echo 'Will be cancelled'; sleep 120")

	// Wait for job to start
	time.Sleep(2 * time.Second)

	// Sync to capture PENDING/RUNNING state
	syncConfig := syncer.Config{
		SyncInterval:      5 * time.Second,
		InitialDelay:      0,
		ClusterName:       "cancelled-e2e",
		SchedulerInstance: "slurm-cancelled-e2e",
		IngestVersion:     "cancelled-e2e-v1",
	}
	env.syncerInst = syncer.New(env.slurmSource, env.store, syncConfig)
	env.syncerInst.SyncNow()

	// Verify initial state in DB
	dbJob, err := env.store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("Failed to get job from DB: %v", err)
	}
	initialState := dbJob.State
	t.Logf("Initial job state: %s (raw: %s)", initialState, dbJob.Scheduler.RawState)

	// Cancel the job
	t.Log("Cancelling job...")
	cancelSlurmJob(t, jobID)

	// Wait for cancellation to propagate
	time.Sleep(2 * time.Second)

	// Sync again to capture CANCELLED state
	env.syncerInst.SyncNow()

	// Verify job is now cancelled in DB
	dbJob, err = env.store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("Failed to get job from DB after cancel: %v", err)
	}

	t.Logf("Final job state: %s (raw: %s)", dbJob.State, dbJob.Scheduler.RawState)

	if dbJob.State != storage.JobStateCancelled {
		t.Errorf("Expected state 'cancelled', got '%s'", dbJob.State)
	}

	// Verify raw state is CANCELLED
	if dbJob.Scheduler != nil && dbJob.Scheduler.RawState != "CANCELLED" {
		t.Logf("Note: Raw state is '%s' (may include reason suffix)", dbJob.Scheduler.RawState)
	}

	// Verify audit trail shows state transition
	events := queryAuditEvents(t, env.db, jobID)
	t.Logf("Audit events for cancelled job: %d", len(events))

	if len(events) < 2 {
		t.Errorf("Expected at least 2 audit events (initial + cancelled), got %d", len(events))
	}

	// Check last event shows cancelled state
	if len(events) > 0 {
		lastEvent := events[len(events)-1]
		var snapshot map[string]interface{}
		json.Unmarshal(lastEvent.JobSnapshot, &snapshot)
		finalState := snapshot["state"]
		t.Logf("Last audit snapshot state: %v", finalState)

		if finalState != "cancelled" {
			t.Errorf("Last audit snapshot state=%v, want 'cancelled'", finalState)
		}
	}
}

// TestSlurmE2E_FailedJobState tests that failed jobs (e.g., TIMEOUT) are correctly tracked.
func TestSlurmE2E_FailedJobState(t *testing.T) {
	env, cleanup := setupTestEnv(t)
	defer cleanup()

	ctx := context.Background()

	// Submit a job with a very short time limit that will timeout
	// Use sbatch with --time=0:01 (1 minute) and a job that sleeps longer
	cmd := exec.Command("docker", "exec", "hpc-slurm", "sbatch",
		"--parsable",
		"--time=0:01", // 1 minute time limit
		"--wrap", "echo 'Will timeout'; sleep 120")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to submit timeout job: %v\nOutput: %s", err, string(output))
	}

	jobID := strings.TrimSpace(string(output))
	t.Logf("Submitted timeout job: %s", jobID)

	// Sync to capture initial state
	syncConfig := syncer.Config{
		SyncInterval:      5 * time.Second,
		InitialDelay:      0,
		ClusterName:       "failed-e2e",
		SchedulerInstance: "slurm-failed-e2e",
		IngestVersion:     "failed-e2e-v1",
	}
	env.syncerInst = syncer.New(env.slurmSource, env.store, syncConfig)
	env.syncerInst.SyncNow()

	// Get initial state
	dbJob, err := env.store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("Failed to get job from DB: %v", err)
	}
	t.Logf("Initial job state: %s", dbJob.State)

	// Wait for the job to timeout (should be ~1 minute)
	t.Log("Waiting for job to timeout (this may take ~60 seconds)...")
	deadline := time.Now().Add(90 * time.Second)

	var finalState storage.JobState
	for time.Now().Before(deadline) {
		time.Sleep(10 * time.Second)
		env.syncerInst.SyncNow()

		dbJob, err = env.store.GetJob(ctx, jobID)
		if err != nil {
			t.Fatalf("Failed to get job: %v", err)
		}

		t.Logf("Job state: %s (raw: %s)", dbJob.State, dbJob.Scheduler.RawState)
		finalState = dbJob.State

		// Check if job reached a terminal state
		if dbJob.State == storage.JobStateFailed ||
			dbJob.State == storage.JobStateCancelled ||
			dbJob.State == storage.JobStateCompleted {
			break
		}
	}

	// Verify job ended in failed state (TIMEOUT maps to failed)
	if finalState != storage.JobStateFailed {
		t.Errorf("Expected state 'failed' (from TIMEOUT), got '%s'", finalState)
		if dbJob.Scheduler != nil {
			t.Logf("Raw state was: %s", dbJob.Scheduler.RawState)
		}
	}

	// Verify audit trail
	events := queryAuditEvents(t, env.db, jobID)
	t.Logf("Audit events for failed job: %d", len(events))

	for i, e := range events {
		var snapshot map[string]interface{}
		json.Unmarshal(e.JobSnapshot, &snapshot)
		t.Logf("  Event %d: state=%v, raw_state=%v",
			i+1, snapshot["state"], getSchedulerRawState(snapshot))
	}
}

// TestSlurmE2E_PendingJobState tests that pending jobs are correctly captured before resources are available.
func TestSlurmE2E_PendingJobState(t *testing.T) {
	env, cleanup := setupTestEnv(t)
	defer cleanup()

	ctx := context.Background()

	// Submit a job requesting many CPUs to likely stay pending
	// (assuming our test cluster has limited resources)
	cmd := exec.Command("docker", "exec", "hpc-slurm", "sbatch",
		"--parsable",
		"--cpus-per-task=1000", // Request many CPUs to force PENDING
		"--wrap", "echo 'Should stay pending'")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to submit pending job: %v\nOutput: %s", err, string(output))
	}

	jobID := strings.TrimSpace(string(output))
	t.Logf("Submitted job requesting many CPUs: %s", jobID)
	defer cancelSlurmJob(t, jobID) // Clean up

	// Wait briefly
	time.Sleep(2 * time.Second)

	// Sync to capture state
	syncConfig := syncer.Config{
		SyncInterval:      5 * time.Second,
		InitialDelay:      0,
		ClusterName:       "pending-e2e",
		SchedulerInstance: "slurm-pending-e2e",
		IngestVersion:     "pending-e2e-v1",
	}
	env.syncerInst = syncer.New(env.slurmSource, env.store, syncConfig)
	env.syncerInst.SyncNow()

	// Get job state
	dbJob, err := env.store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("Failed to get job from DB: %v", err)
	}

	t.Logf("Job state: %s (raw: %s)", dbJob.State, dbJob.Scheduler.RawState)

	// Verify job is pending
	if dbJob.State != storage.JobStatePending {
		t.Errorf("Expected state 'pending', got '%s'", dbJob.State)
	}

	// Verify scheduler metadata shows why it's pending
	if dbJob.Scheduler != nil {
		t.Logf("State reason: %s", dbJob.Scheduler.StateReason)
		// Common reasons: "Resources", "Priority", "Dependency"
	}

	// Verify audit captures pending state
	events := queryAuditEvents(t, env.db, jobID)
	if len(events) == 0 {
		t.Error("Expected at least one audit event for pending job")
	} else {
		var snapshot map[string]interface{}
		json.Unmarshal(events[0].JobSnapshot, &snapshot)
		if snapshot["state"] != "pending" {
			t.Errorf("Audit snapshot state=%v, want 'pending'", snapshot["state"])
		}
		t.Logf("Pending job audit captured correctly")
	}
}

// TestSlurmE2E_AllTerminalStates tests the mapping of various Slurm terminal states.
func TestSlurmE2E_AllTerminalStates(t *testing.T) {
	env, cleanup := setupTestEnv(t)
	defer cleanup()

	// Test that state mapping works correctly by checking the synced states
	testCases := []struct {
		name         string
		script       string
		expectState  storage.JobState
		extraArgs    []string
		waitForState bool
		maxWait      time.Duration
	}{
		{
			name:         "completed_job",
			script:       "echo 'success'; exit 0",
			expectState:  storage.JobStateCompleted,
			waitForState: true,
			maxWait:      30 * time.Second,
		},
		{
			name:         "failed_exit_code",
			script:       "echo 'failing'; exit 1",
			expectState:  storage.JobStateFailed,
			waitForState: true,
			maxWait:      30 * time.Second,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			// Submit job
			args := []string{"exec", "hpc-slurm", "sbatch", "--parsable"}
			args = append(args, tc.extraArgs...)
			args = append(args, "--wrap", tc.script)

			cmd := exec.Command("docker", args...)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("Failed to submit job: %v\nOutput: %s", err, string(output))
			}

			jobID := strings.TrimSpace(string(output))
			t.Logf("Submitted job %s: %s", tc.name, jobID)

			// Create syncer for this test
			syncConfig := syncer.Config{
				SyncInterval:  5 * time.Second,
				InitialDelay:  0,
				ClusterName:   "states-e2e",
				IngestVersion: "states-e2e-v1",
			}
			syncerInst := syncer.New(env.slurmSource, env.store, syncConfig)
			defer syncerInst.Stop()

			// Wait for expected state if needed
			if tc.waitForState {
				deadline := time.Now().Add(tc.maxWait)
				var dbJob *storage.Job

				for time.Now().Before(deadline) {
					time.Sleep(3 * time.Second)
					syncerInst.SyncNow()

					dbJob, err = env.store.GetJob(ctx, jobID)
					if err != nil {
						continue
					}

					t.Logf("  Job %s state: %s", jobID, dbJob.State)

					if dbJob.State == tc.expectState {
						t.Logf("Job reached expected state: %s", tc.expectState)
						return
					}

					// Also accept if it reached any terminal state
					if dbJob.State == storage.JobStateCompleted ||
						dbJob.State == storage.JobStateFailed ||
						dbJob.State == storage.JobStateCancelled {
						break
					}
				}

				if dbJob == nil {
					t.Fatalf("Failed to get job %s from DB", jobID)
				}

				if dbJob.State != tc.expectState {
					t.Errorf("Expected state '%s', got '%s' (raw: %s)",
						tc.expectState, dbJob.State, dbJob.Scheduler.RawState)
				}
			}
		})
	}
}

// getSchedulerRawState extracts the raw scheduler state from a job snapshot.
func getSchedulerRawState(snapshot map[string]interface{}) string {
	if sched, ok := snapshot["scheduler"].(map[string]interface{}); ok {
		if raw, ok := sched["raw_state"].(string); ok {
			return raw
		}
	}
	return ""
}

// Helper function to get map keys
func mapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
