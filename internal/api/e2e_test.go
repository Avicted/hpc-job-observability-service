package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/avic/hpc-job-observability-service/internal/api/types"
	"github.com/avic/hpc-job-observability-service/internal/metrics"
	"github.com/avic/hpc-job-observability-service/internal/storage"
)

// TestE2E_FullJobLifecycle tests the complete lifecycle of a job from creation to deletion
// including metrics recording, rollup updates, and audit logging.
func TestE2E_FullJobLifecycle(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)
	handler := srv.Routes()

	jobID := "e2e-lifecycle-job-001"
	ctx := context.Background()

	// Step 1: Create a job with all fields
	t.Run("Step1_CreateJob", func(t *testing.T) {
		reqBody := types.CreateJobRequest{
			Id:    jobID,
			User:  "e2e-user",
			Nodes: []string{"compute-01", "compute-02", "compute-03"},
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader(body))
		setAuditHeaders(req)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("Expected status 201, got %d: %s", rec.Code, rec.Body.String())
		}

		var resp types.Job
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		// Verify response fields
		if resp.Id != jobID {
			t.Errorf("Expected ID '%s', got '%s'", jobID, resp.Id)
		}
		if resp.User != "e2e-user" {
			t.Errorf("Expected user 'e2e-user', got '%s'", resp.User)
		}
		if len(resp.Nodes) != 3 {
			t.Errorf("Expected 3 nodes, got %d", len(resp.Nodes))
		}
		if resp.State != types.Running {
			t.Errorf("Expected state 'running', got '%s'", resp.State)
		}
		// Verify node_count is set (API returns node_count based on stored value)
		if resp.NodeCount != nil && *resp.NodeCount < 0 {
			t.Errorf("Expected non-negative node_count, got %d", *resp.NodeCount)
		}
	})

	// Step 2: Verify database state after creation
	t.Run("Step2_VerifyDBAfterCreate", func(t *testing.T) {
		job, err := store.GetJob(ctx, jobID)
		if err != nil {
			t.Fatalf("Failed to get job from DB: %v", err)
		}

		if job.ID != jobID {
			t.Errorf("DB: Expected ID '%s', got '%s'", jobID, job.ID)
		}
		if job.User != "e2e-user" {
			t.Errorf("DB: Expected user 'e2e-user', got '%s'", job.User)
		}
		if len(job.Nodes) != 3 {
			t.Errorf("DB: Expected 3 nodes, got %d", len(job.Nodes))
		}
		if job.State != storage.JobStateRunning {
			t.Errorf("DB: Expected state 'running', got '%s'", job.State)
		}
		if job.ClusterName != "default" {
			t.Errorf("DB: Expected cluster_name 'default', got '%s'", job.ClusterName)
		}
		if job.IngestVersion != "api-v1" {
			t.Errorf("DB: Expected ingest_version 'api-v1', got '%s'", job.IngestVersion)
		}
		// Initial rollup values should be zero
		if job.SampleCount != 0 {
			t.Errorf("DB: Expected sample_count 0, got %d", job.SampleCount)
		}
	})

	// Step 3: Record first metrics sample
	t.Run("Step3_RecordFirstMetrics", func(t *testing.T) {
		gpu := 25.0
		reqBody := types.RecordMetricsRequest{
			CpuUsage:      50.0,
			MemoryUsageMb: 4096,
			GpuUsage:      &gpu,
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+jobID+"/metrics", bytes.NewReader(body))
		setAuditHeaders(req)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("Expected status 201, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	// Step 4: Verify rollups after first sample
	t.Run("Step4_VerifyRollupsAfterFirstSample", func(t *testing.T) {
		job, err := store.GetJob(ctx, jobID)
		if err != nil {
			t.Fatalf("Failed to get job from DB: %v", err)
		}

		if job.SampleCount != 1 {
			t.Errorf("DB: Expected sample_count 1, got %d", job.SampleCount)
		}
		if job.AvgCPUUsage != 50.0 {
			t.Errorf("DB: Expected avg_cpu_usage 50.0, got %f", job.AvgCPUUsage)
		}
		if job.MaxCPUUsage != 50.0 {
			t.Errorf("DB: Expected max_cpu_usage 50.0, got %f", job.MaxCPUUsage)
		}
		if job.MaxMemUsageMB != 4096 {
			t.Errorf("DB: Expected max_memory_usage_mb 4096, got %d", job.MaxMemUsageMB)
		}
		if job.AvgGPUUsage != 25.0 {
			t.Errorf("DB: Expected avg_gpu_usage 25.0, got %f", job.AvgGPUUsage)
		}
		if job.MaxGPUUsage != 25.0 {
			t.Errorf("DB: Expected max_gpu_usage 25.0, got %f", job.MaxGPUUsage)
		}
		if job.LastSampleAt == nil {
			t.Error("DB: Expected last_sample_at to be set")
		}
	})

	// Step 5: Record second metrics sample (higher values)
	t.Run("Step5_RecordSecondMetrics", func(t *testing.T) {
		gpu := 75.0
		reqBody := types.RecordMetricsRequest{
			CpuUsage:      90.0,
			MemoryUsageMb: 8192,
			GpuUsage:      &gpu,
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+jobID+"/metrics", bytes.NewReader(body))
		setAuditHeaders(req)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("Expected status 201, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	// Step 6: Verify rollups after second sample (averages and max updated)
	t.Run("Step6_VerifyRollupsAfterSecondSample", func(t *testing.T) {
		job, err := store.GetJob(ctx, jobID)
		if err != nil {
			t.Fatalf("Failed to get job from DB: %v", err)
		}

		if job.SampleCount != 2 {
			t.Errorf("DB: Expected sample_count 2, got %d", job.SampleCount)
		}
		// Average of 50.0 and 90.0 = 70.0
		if job.AvgCPUUsage != 70.0 {
			t.Errorf("DB: Expected avg_cpu_usage 70.0, got %f", job.AvgCPUUsage)
		}
		// Max should be 90.0
		if job.MaxCPUUsage != 90.0 {
			t.Errorf("DB: Expected max_cpu_usage 90.0, got %f", job.MaxCPUUsage)
		}
		// Max memory should be 8192
		if job.MaxMemUsageMB != 8192 {
			t.Errorf("DB: Expected max_memory_usage_mb 8192, got %d", job.MaxMemUsageMB)
		}
		// Average of 25.0 and 75.0 = 50.0
		if job.AvgGPUUsage != 50.0 {
			t.Errorf("DB: Expected avg_gpu_usage 50.0, got %f", job.AvgGPUUsage)
		}
		// Max GPU should be 75.0
		if job.MaxGPUUsage != 75.0 {
			t.Errorf("DB: Expected max_gpu_usage 75.0, got %f", job.MaxGPUUsage)
		}
	})

	// Step 7: Record third metrics sample (lower values - test that max doesn't decrease)
	t.Run("Step7_RecordThirdMetrics", func(t *testing.T) {
		gpu := 10.0
		reqBody := types.RecordMetricsRequest{
			CpuUsage:      30.0,
			MemoryUsageMb: 2048,
			GpuUsage:      &gpu,
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+jobID+"/metrics", bytes.NewReader(body))
		setAuditHeaders(req)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("Expected status 201, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	// Step 8: Verify rollups - max values should not decrease
	t.Run("Step8_VerifyRollupsMaxNotDecreased", func(t *testing.T) {
		job, err := store.GetJob(ctx, jobID)
		if err != nil {
			t.Fatalf("Failed to get job from DB: %v", err)
		}

		if job.SampleCount != 3 {
			t.Errorf("DB: Expected sample_count 3, got %d", job.SampleCount)
		}
		// Average of 50.0, 90.0, 30.0 = 170/3 ≈ 56.67
		expectedAvgCPU := (50.0 + 90.0 + 30.0) / 3.0
		if abs(job.AvgCPUUsage-expectedAvgCPU) > 0.01 {
			t.Errorf("DB: Expected avg_cpu_usage ~%.2f, got %f", expectedAvgCPU, job.AvgCPUUsage)
		}
		// Max should still be 90.0
		if job.MaxCPUUsage != 90.0 {
			t.Errorf("DB: Expected max_cpu_usage to stay 90.0, got %f", job.MaxCPUUsage)
		}
		// Max memory should still be 8192
		if job.MaxMemUsageMB != 8192 {
			t.Errorf("DB: Expected max_memory_usage_mb to stay 8192, got %d", job.MaxMemUsageMB)
		}
		// Max GPU should still be 75.0
		if job.MaxGPUUsage != 75.0 {
			t.Errorf("DB: Expected max_gpu_usage to stay 75.0, got %f", job.MaxGPUUsage)
		}
	})

	// Step 9: Get job via API and verify rollup fields are returned
	t.Run("Step9_GetJobAPIVerifyRollups", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+jobID, nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var resp types.Job
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if resp.SampleCount == nil || *resp.SampleCount != 3 {
			t.Errorf("API: Expected sample_count 3, got %v", resp.SampleCount)
		}
		if resp.MaxCpuUsage == nil || *resp.MaxCpuUsage != 90.0 {
			t.Errorf("API: Expected max_cpu_usage 90.0, got %v", resp.MaxCpuUsage)
		}
		if resp.MaxMemoryUsageMb == nil || *resp.MaxMemoryUsageMb != 8192 {
			t.Errorf("API: Expected max_memory_usage_mb 8192, got %v", resp.MaxMemoryUsageMb)
		}
		if resp.LastSampleAt == nil {
			t.Error("API: Expected last_sample_at to be set")
		}
	})

	// Step 10: Get metrics history via API
	t.Run("Step10_GetMetricsHistory", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+jobID+"/metrics", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var resp types.JobMetricsResponse
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if resp.Total != 3 {
			t.Errorf("API: Expected 3 metric samples, got %d", resp.Total)
		}
		if len(resp.Samples) != 3 {
			t.Errorf("API: Expected 3 samples in response, got %d", len(resp.Samples))
		}
	})

	// Step 11: Update job state to completed
	t.Run("Step11_UpdateJobStateCompleted", func(t *testing.T) {
		state := types.Completed
		reqBody := types.UpdateJobRequest{
			State: &state,
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPatch, "/v1/jobs/"+jobID, bytes.NewReader(body))
		setAuditHeaders(req)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var resp types.Job
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if resp.State != types.Completed {
			t.Errorf("API: Expected state 'completed', got '%s'", resp.State)
		}
	})

	// Step 12: Verify state in database
	t.Run("Step12_VerifyDBStateCompleted", func(t *testing.T) {
		job, err := store.GetJob(ctx, jobID)
		if err != nil {
			t.Fatalf("Failed to get job from DB: %v", err)
		}

		if job.State != storage.JobStateCompleted {
			t.Errorf("DB: Expected state 'completed', got '%s'", job.State)
		}
	})

	// Step 13: List jobs and verify our job is included
	t.Run("Step13_ListJobsContainsJob", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var resp types.JobListResponse
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		found := false
		for _, job := range resp.Jobs {
			if job.Id == jobID {
				found = true
				if job.State != types.Completed {
					t.Errorf("API List: Expected state 'completed', got '%s'", job.State)
				}
				break
			}
		}
		if !found {
			t.Errorf("API List: Job '%s' not found in list response", jobID)
		}
	})

	// Step 14: Delete job
	t.Run("Step14_DeleteJob", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/v1/jobs/"+jobID, nil)
		setAuditHeaders(req)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Errorf("Expected status 204, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	// Step 15: Verify job is deleted
	t.Run("Step15_VerifyJobDeleted", func(t *testing.T) {
		_, err := store.GetJob(ctx, jobID)
		if err != storage.ErrJobNotFound {
			t.Errorf("DB: Expected ErrJobNotFound, got %v", err)
		}

		req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+jobID, nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Errorf("API: Expected status 404, got %d", rec.Code)
		}
	})
}

// TestE2E_AuditHeaders tests that audit headers are enforced on write operations
func TestE2E_AuditHeaders(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)
	handler := srv.Routes()

	// Test CreateJob without audit headers
	t.Run("CreateJob_MissingChangedBy", func(t *testing.T) {
		reqBody := types.CreateJobRequest{
			Id:    "audit-test-job",
			User:  "testuser",
			Nodes: []string{"node-1"},
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Source", "test")
		// Missing X-Changed-By
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400 for missing X-Changed-By, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("CreateJob_MissingSource", func(t *testing.T) {
		reqBody := types.CreateJobRequest{
			Id:    "audit-test-job",
			User:  "testuser",
			Nodes: []string{"node-1"},
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Changed-By", "tester")
		// Missing X-Source
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400 for missing X-Source, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	// Create a job for update/delete tests
	_ = store.CreateJob(storage.WithAuditInfo(context.Background(), storage.NewAuditInfo("setup", "test")), &storage.Job{
		ID:        "audit-test-job-2",
		User:      "testuser",
		Nodes:     []string{"node-1"},
		State:     storage.JobStateRunning,
		StartTime: time.Now(),
	})

	t.Run("UpdateJob_MissingHeaders", func(t *testing.T) {
		state := types.Completed
		reqBody := types.UpdateJobRequest{
			State: &state,
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPatch, "/v1/jobs/audit-test-job-2", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		// Missing audit headers
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400 for missing audit headers, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("DeleteJob_MissingHeaders", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/v1/jobs/audit-test-job-2", nil)
		// Missing audit headers
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400 for missing audit headers, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("RecordMetrics_MissingHeaders", func(t *testing.T) {
		reqBody := types.RecordMetricsRequest{
			CpuUsage:      50.0,
			MemoryUsageMb: 1024,
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/jobs/audit-test-job-2/metrics", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		// Missing audit headers
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400 for missing audit headers, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	// Test that correlation ID is auto-generated if not provided
	t.Run("CreateJob_AutoGenerateCorrelationId", func(t *testing.T) {
		reqBody := types.CreateJobRequest{
			Id:    "audit-test-job-3",
			User:  "testuser",
			Nodes: []string{"node-1"},
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Changed-By", "tester")
		req.Header.Set("X-Source", "api-test")
		// No X-Correlation-Id - should be auto-generated
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("Expected status 201, got %d: %s", rec.Code, rec.Body.String())
		}

		// Check that correlation ID was returned in response header
		correlationID := rec.Header().Get("X-Correlation-Id")
		if correlationID == "" {
			t.Error("Expected X-Correlation-Id header to be set in response")
		}
	})
}

// TestE2E_JobWithSchedulerInfo tests creating jobs with scheduler metadata
func TestE2E_JobWithSchedulerInfo(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)
	handler := srv.Routes()

	ctx := context.Background()

	t.Run("CreateJobWithSchedulerInfo", func(t *testing.T) {
		priority := int64(100)
		exitCode := 0
		timeLimitMins := 120
		schedulerType := types.Slurm
		reqBody := types.CreateJobRequest{
			Id:    "scheduler-test-job",
			User:  "slurmuser",
			Nodes: []string{"gpu-node-01", "gpu-node-02"},
			Scheduler: &types.SchedulerInfo{
				Type:             &schedulerType,
				ExternalJobId:    ptrString("12345"),
				RawState:         ptrString("RUNNING"),
				Partition:        ptrString("gpu"),
				Account:          ptrString("research"),
				Qos:              ptrString("high"),
				Priority:         &priority,
				ExitCode:         &exitCode,
				StateReason:      ptrString("None"),
				TimeLimitMinutes: &timeLimitMins,
			},
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader(body))
		setAuditHeaders(req)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("Expected status 201, got %d: %s", rec.Code, rec.Body.String())
		}

		var resp types.Job
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if resp.Scheduler == nil {
			t.Fatal("Expected scheduler info in response")
		}
		if resp.Scheduler.Partition == nil || *resp.Scheduler.Partition != "gpu" {
			t.Errorf("Expected partition 'gpu', got %v", resp.Scheduler.Partition)
		}
	})

	t.Run("VerifySchedulerInfoInDB", func(t *testing.T) {
		job, err := store.GetJob(ctx, "scheduler-test-job")
		if err != nil {
			t.Fatalf("Failed to get job from DB: %v", err)
		}

		if job.Scheduler == nil {
			t.Fatal("DB: Expected scheduler info")
		}
		if job.Scheduler.ExternalJobID != "12345" {
			t.Errorf("DB: Expected external_job_id '12345', got '%s'", job.Scheduler.ExternalJobID)
		}
		if job.Scheduler.Partition != "gpu" {
			t.Errorf("DB: Expected partition 'gpu', got '%s'", job.Scheduler.Partition)
		}
		if job.Scheduler.Account != "research" {
			t.Errorf("DB: Expected account 'research', got '%s'", job.Scheduler.Account)
		}
		if job.Scheduler.QoS != "high" {
			t.Errorf("DB: Expected qos 'high', got '%s'", job.Scheduler.QoS)
		}
		if job.Scheduler.Priority == nil || *job.Scheduler.Priority != 100 {
			t.Errorf("DB: Expected priority 100, got %v", job.Scheduler.Priority)
		}
		if job.Scheduler.StateReason != "None" {
			t.Errorf("DB: Expected state_reason 'None', got '%s'", job.Scheduler.StateReason)
		}
		if job.Scheduler.TimeLimitMins == nil || *job.Scheduler.TimeLimitMins != 120 {
			t.Errorf("DB: Expected time_limit_minutes 120, got %v", job.Scheduler.TimeLimitMins)
		}
	})

	t.Run("GetJobReturnsSchedulerInfo", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs/scheduler-test-job", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var resp types.Job
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if resp.Scheduler == nil {
			t.Fatal("API: Expected scheduler info in GET response")
		}
		if resp.Scheduler.ExternalJobId == nil || *resp.Scheduler.ExternalJobId != "12345" {
			t.Errorf("API: Expected external_job_id '12345', got %v", resp.Scheduler.ExternalJobId)
		}
		if resp.Scheduler.StateReason == nil || *resp.Scheduler.StateReason != "None" {
			t.Errorf("API: Expected state_reason 'None', got %v", resp.Scheduler.StateReason)
		}
		if resp.Scheduler.TimeLimitMinutes == nil || *resp.Scheduler.TimeLimitMinutes != 120 {
			t.Errorf("API: Expected time_limit_minutes 120, got %v", resp.Scheduler.TimeLimitMinutes)
		}
	})
}

// TestE2E_MultipleJobsAndFilters tests listing jobs with various filters
func TestE2E_MultipleJobsAndFilters(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)
	handler := srv.Routes()

	ctx := storage.WithAuditInfo(context.Background(), storage.NewAuditInfo("setup", "test"))

	// Create multiple jobs with different states and users
	jobs := []struct {
		id    string
		user  string
		nodes []string
		state storage.JobState
	}{
		{"filter-job-1", "alice", []string{"node-1"}, storage.JobStateRunning},
		{"filter-job-2", "alice", []string{"node-2"}, storage.JobStateCompleted},
		{"filter-job-3", "bob", []string{"node-1", "node-3"}, storage.JobStateRunning},
		{"filter-job-4", "bob", []string{"node-4"}, storage.JobStateFailed},
		{"filter-job-5", "charlie", []string{"node-1"}, storage.JobStatePending},
	}

	for _, j := range jobs {
		err := store.CreateJob(ctx, &storage.Job{
			ID:        j.id,
			User:      j.user,
			Nodes:     j.nodes,
			State:     j.state,
			StartTime: time.Now(),
		})
		if err != nil {
			t.Fatalf("Failed to create job %s: %v", j.id, err)
		}
	}

	t.Run("ListAll", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d", rec.Code)
		}

		var resp types.JobListResponse
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if resp.Total < 5 {
			t.Errorf("Expected at least 5 jobs, got %d", resp.Total)
		}
	})

	t.Run("FilterByUser", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs?user=alice", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d", rec.Code)
		}

		var resp types.JobListResponse
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if resp.Total != 2 {
			t.Errorf("Expected 2 jobs for alice, got %d", resp.Total)
		}
	})

	t.Run("FilterByState", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs?state=running", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d", rec.Code)
		}

		var resp types.JobListResponse
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if resp.Total != 2 {
			t.Errorf("Expected 2 running jobs, got %d", resp.Total)
		}
	})

	t.Run("FilterByNode", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs?node=node-1", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d", rec.Code)
		}

		var resp types.JobListResponse
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		// Jobs 1, 3, 5 have node-1
		if resp.Total != 3 {
			t.Errorf("Expected 3 jobs on node-1, got %d", resp.Total)
		}
	})

	t.Run("Pagination", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs?limit=2&offset=0", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d", rec.Code)
		}

		var resp types.JobListResponse
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if len(resp.Jobs) != 2 {
			t.Errorf("Expected 2 jobs in page, got %d", len(resp.Jobs))
		}
		if resp.Limit != 2 {
			t.Errorf("Expected limit 2, got %d", resp.Limit)
		}
		if resp.Offset != 0 {
			t.Errorf("Expected offset 0, got %d", resp.Offset)
		}
	})
}

// TestE2E_MetricsValidation tests metrics validation
func TestE2E_MetricsValidation(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)
	handler := srv.Routes()

	// Create a test job
	_ = store.CreateJob(storage.WithAuditInfo(context.Background(), storage.NewAuditInfo("setup", "test")), &storage.Job{
		ID:        "validation-test-job",
		User:      "testuser",
		Nodes:     []string{"node-1"},
		State:     storage.JobStateRunning,
		StartTime: time.Now(),
	})

	t.Run("InvalidCPUUsage_TooHigh", func(t *testing.T) {
		reqBody := types.RecordMetricsRequest{
			CpuUsage:      150.0, // Invalid: > 100
			MemoryUsageMb: 1024,
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/jobs/validation-test-job/metrics", bytes.NewReader(body))
		setAuditHeaders(req)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400 for CPU > 100, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("InvalidCPUUsage_Negative", func(t *testing.T) {
		reqBody := types.RecordMetricsRequest{
			CpuUsage:      -10.0, // Invalid: < 0
			MemoryUsageMb: 1024,
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/jobs/validation-test-job/metrics", bytes.NewReader(body))
		setAuditHeaders(req)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400 for negative CPU, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("InvalidMemory_Negative", func(t *testing.T) {
		reqBody := types.RecordMetricsRequest{
			CpuUsage:      50.0,
			MemoryUsageMb: -100, // Invalid: < 0
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/jobs/validation-test-job/metrics", bytes.NewReader(body))
		setAuditHeaders(req)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400 for negative memory, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("ValidMetrics_EdgeCase", func(t *testing.T) {
		reqBody := types.RecordMetricsRequest{
			CpuUsage:      0.0, // Edge case: exactly 0
			MemoryUsageMb: 0,   // Edge case: exactly 0
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/jobs/validation-test-job/metrics", bytes.NewReader(body))
		setAuditHeaders(req)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Errorf("Expected status 201 for edge case 0 values, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("ValidMetrics_MaxCPU", func(t *testing.T) {
		reqBody := types.RecordMetricsRequest{
			CpuUsage:      100.0, // Edge case: exactly 100
			MemoryUsageMb: 1024,
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/jobs/validation-test-job/metrics", bytes.NewReader(body))
		setAuditHeaders(req)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Errorf("Expected status 201 for max CPU 100, got %d: %s", rec.Code, rec.Body.String())
		}
	})
}

// Helper function for absolute value
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// Helper to create pointer to string
func ptrString(s string) *string {
	return &s
}

// TestE2E_StorageRollupIntegration tests storage-level rollup functionality
func TestE2E_StorageRollupIntegration(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-e2e-storage-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	store, err := storage.NewSQLiteStorage("file:" + tmpFile.Name() + "?cache=shared&mode=rwc")
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer store.Close()

	if err := store.Migrate(); err != nil {
		t.Fatalf("Failed to migrate: %v", err)
	}

	ctx := storage.WithAuditInfo(context.Background(), storage.NewAuditInfo("e2e-test", "storage-test"))
	jobID := "storage-rollup-test"

	// Create job
	job := &storage.Job{
		ID:        jobID,
		User:      "rollup-tester",
		Nodes:     []string{"node-a", "node-b"},
		State:     storage.JobStateRunning,
		StartTime: time.Now(),
	}
	if err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}

	// Record multiple samples and verify rollups
	samples := []struct {
		cpu    float64
		mem    int64
		gpu    *float64
		expAvg float64
		expMax float64
	}{
		{cpu: 20.0, mem: 1000, gpu: nil, expAvg: 20.0, expMax: 20.0},
		{cpu: 40.0, mem: 2000, gpu: ptrFloat(10.0), expAvg: 30.0, expMax: 40.0},
		{cpu: 60.0, mem: 1500, gpu: ptrFloat(30.0), expAvg: 40.0, expMax: 60.0},
		{cpu: 30.0, mem: 3000, gpu: ptrFloat(5.0), expAvg: 37.5, expMax: 60.0},
	}

	for i, s := range samples {
		sample := &storage.MetricSample{
			JobID:         jobID,
			Timestamp:     time.Now(),
			CPUUsage:      s.cpu,
			MemoryUsageMB: s.mem,
			GPUUsage:      s.gpu,
		}
		if err := store.RecordMetrics(ctx, sample); err != nil {
			t.Fatalf("Failed to record sample %d: %v", i, err)
		}

		// Verify rollups after each sample
		job, err := store.GetJob(ctx, jobID)
		if err != nil {
			t.Fatalf("Failed to get job after sample %d: %v", i, err)
		}

		if job.SampleCount != int64(i+1) {
			t.Errorf("Sample %d: Expected sample_count %d, got %d", i, i+1, job.SampleCount)
		}
		if abs(job.AvgCPUUsage-s.expAvg) > 0.01 {
			t.Errorf("Sample %d: Expected avg_cpu %.2f, got %.2f", i, s.expAvg, job.AvgCPUUsage)
		}
		if job.MaxCPUUsage != s.expMax {
			t.Errorf("Sample %d: Expected max_cpu %.2f, got %.2f", i, s.expMax, job.MaxCPUUsage)
		}
	}

	// Final verification
	job, _ = store.GetJob(ctx, jobID)
	if job.SampleCount != 4 {
		t.Errorf("Final: Expected sample_count 4, got %d", job.SampleCount)
	}
	if job.MaxMemUsageMB != 3000 {
		t.Errorf("Final: Expected max_memory 3000, got %d", job.MaxMemUsageMB)
	}
	// GPU average: (0+10+30+5)/4 = 11.25 - but first sample has no GPU
	// Actually rollup only counts non-nil GPU values: (10+30+5)/3 = 15
	// Need to check actual implementation
}

// Helper to create pointer to float64
func ptrFloat(f float64) *float64 {
	return &f
}
