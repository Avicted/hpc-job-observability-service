package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/avic/hpc-job-observability-service/internal/api/types"
	"github.com/avic/hpc-job-observability-service/internal/metrics"
	"github.com/avic/hpc-job-observability-service/internal/storage"
)

// testStore creates a temporary SQLite store for testing.
func testStore(t *testing.T) storage.Storage {
	t.Helper()

	tmpFile, err := os.CreateTemp("", "test-api-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	t.Cleanup(func() { os.Remove(tmpFile.Name()) })

	store, err := storage.NewSQLiteStorage("file:" + tmpFile.Name() + "?cache=shared&mode=rwc")
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	if err := store.Migrate(); err != nil {
		t.Fatalf("Failed to migrate: %v", err)
	}

	return store
}

func TestHealthEndpoint(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	server := NewServer(store, exporter)
	handler := server.Routes()

	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	var resp types.HealthResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp.Status != types.Healthy {
		t.Errorf("Expected status 'healthy', got '%s'", resp.Status)
	}
}

func TestJobsEndpoints(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)
	handler := srv.Routes()

	// Test Create Job
	t.Run("CreateJob", func(t *testing.T) {
		reqBody := types.CreateJobRequest{
			Id:    "api-test-job-001",
			User:  "testuser",
			Nodes: []string{"node-1", "node-2"},
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Errorf("Expected status 201, got %d: %s", rec.Code, rec.Body.String())
		}

		var resp types.Job
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if resp.Id != "api-test-job-001" {
			t.Errorf("Expected ID 'api-test-job-001', got '%s'", resp.Id)
		}
		if resp.State != types.Running {
			t.Errorf("Expected state 'running', got '%s'", resp.State)
		}
	})

	// Test Create Job - Duplicate
	t.Run("CreateJob_Duplicate", func(t *testing.T) {
		reqBody := types.CreateJobRequest{
			Id:    "api-test-job-001",
			User:  "testuser",
			Nodes: []string{"node-1"},
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusConflict {
			t.Errorf("Expected status 409, got %d", rec.Code)
		}
	})

	// Test Create Job - Validation
	t.Run("CreateJob_Validation", func(t *testing.T) {
		// Missing ID
		reqBody := types.CreateJobRequest{
			User:  "testuser",
			Nodes: []string{"node-1"},
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400, got %d", rec.Code)
		}
	})

	// Test Get Job
	t.Run("GetJob", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs/api-test-job-001", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", rec.Code)
		}

		var resp types.Job
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if resp.Id != "api-test-job-001" {
			t.Errorf("Expected ID 'api-test-job-001', got '%s'", resp.Id)
		}
	})

	// Test Get Job - Not Found
	t.Run("GetJob_NotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs/nonexistent", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Errorf("Expected status 404, got %d", rec.Code)
		}
	})

	// Test List Jobs
	t.Run("ListJobs", func(t *testing.T) {
		// Create another job
		_ = store.CreateJob(context.Background(), &storage.Job{
			ID:    "api-test-job-002",
			User:  "anotheruser",
			Nodes: []string{"node-3"},
			State: storage.JobStateRunning,
		})

		req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", rec.Code)
		}

		var resp types.JobListResponse
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if resp.Total < 2 {
			t.Errorf("Expected at least 2 jobs, got %d", resp.Total)
		}
	})

	// Test List Jobs with Filter
	t.Run("ListJobs_Filtered", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs?user=testuser", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", rec.Code)
		}

		var resp types.JobListResponse
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if resp.Total != 1 {
			t.Errorf("Expected 1 job for user 'testuser', got %d", resp.Total)
		}
	})

	// Test Update Job
	t.Run("UpdateJob", func(t *testing.T) {
		state := types.Completed
		reqBody := types.UpdateJobRequest{
			State: &state,
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPatch, "/v1/jobs/api-test-job-001", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var resp types.Job
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if resp.State != types.Completed {
			t.Errorf("Expected state 'completed', got '%s'", resp.State)
		}
	})

	// Test Delete Job
	t.Run("DeleteJob", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/v1/jobs/api-test-job-002", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Errorf("Expected status 204, got %d", rec.Code)
		}

		// Verify deletion
		req = httptest.NewRequest(http.MethodGet, "/v1/jobs/api-test-job-002", nil)
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Errorf("Expected status 404 after deletion, got %d", rec.Code)
		}
	})
}

func TestMetricsEndpoints(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)
	handler := srv.Routes()

	// Create a test job first
	_ = store.CreateJob(context.Background(), &storage.Job{
		ID:        "metrics-test-job",
		User:      "testuser",
		Nodes:     []string{"node-1"},
		State:     storage.JobStateRunning,
		StartTime: time.Now(),
	})

	// Test Record Metrics
	t.Run("RecordMetrics", func(t *testing.T) {
		gpuUsage := 45.5
		reqBody := types.RecordMetricsRequest{
			CpuUsage:      75.5,
			MemoryUsageMb: 2048,
			GpuUsage:      &gpuUsage,
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/jobs/metrics-test-job/metrics", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Errorf("Expected status 201, got %d: %s", rec.Code, rec.Body.String())
		}

		var resp types.MetricSample
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if resp.CpuUsage != 75.5 {
			t.Errorf("Expected CpuUsage 75.5, got %f", resp.CpuUsage)
		}
	})

	// Test Record Metrics - Job Not Found
	t.Run("RecordMetrics_NotFound", func(t *testing.T) {
		reqBody := types.RecordMetricsRequest{
			CpuUsage:      75.5,
			MemoryUsageMb: 2048,
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/jobs/nonexistent/metrics", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Errorf("Expected status 404, got %d", rec.Code)
		}
	})

	// Test Get Job Metrics
	t.Run("GetJobMetrics", func(t *testing.T) {
		// Record a few more samples
		for i := 0; i < 5; i++ {
			_ = store.RecordMetrics(context.Background(), &storage.MetricSample{
				JobID:         "metrics-test-job",
				Timestamp:     time.Now().Add(time.Duration(i) * time.Minute),
				CPUUsage:      70.0 + float64(i),
				MemoryUsageMB: 2048,
			})
		}

		req := httptest.NewRequest(http.MethodGet, "/v1/jobs/metrics-test-job/metrics", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", rec.Code)
		}

		var resp types.JobMetricsResponse
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if resp.JobId != "metrics-test-job" {
			t.Errorf("Expected job_id 'metrics-test-job', got '%s'", resp.JobId)
		}
		if len(resp.Samples) < 5 {
			t.Errorf("Expected at least 5 samples, got %d", len(resp.Samples))
		}
	})

	// Test Get Job Metrics - Not Found
	t.Run("GetJobMetrics_NotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs/nonexistent/metrics", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Errorf("Expected status 404, got %d", rec.Code)
		}
	})
}

func TestPrometheusMetricsEndpoint(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)
	handler := srv.Routes()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	// Check that response contains Prometheus metrics format
	body := rec.Body.String()
	if len(body) == 0 {
		t.Error("Expected non-empty metrics response")
	}
}

func TestCreateJob_InvalidJSON(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)
	handler := srv.Routes()

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader([]byte("invalid json")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", rec.Code)
	}
}

func TestCreateJob_MissingUser(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)
	handler := srv.Routes()

	reqBody := types.CreateJobRequest{
		Id:    "job-missing-user",
		Nodes: []string{"node-1"},
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", rec.Code)
	}
}

func TestCreateJob_MissingNodes(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)
	handler := srv.Routes()

	reqBody := types.CreateJobRequest{
		Id:   "job-missing-nodes",
		User: "testuser",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", rec.Code)
	}
}

func TestCreateJob_WithSchedulerInfo(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)
	handler := srv.Routes()

	schedType := types.Slurm
	extJobID := "12345"
	rawState := "RUNNING"
	partition := "gpu"
	account := "project_a"
	qos := "normal"
	priority := 100

	reqBody := types.CreateJobRequest{
		Id:    "job-with-scheduler",
		User:  "testuser",
		Nodes: []string{"node-1"},
		Scheduler: &types.SchedulerInfo{
			Type:          &schedType,
			ExternalJobId: &extJobID,
			RawState:      &rawState,
			Partition:     &partition,
			Account:       &account,
			Qos:           &qos,
			Priority:      &priority,
		},
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp types.Job
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp.Scheduler == nil {
		t.Error("Expected scheduler info in response")
	}
	if resp.Scheduler != nil && *resp.Scheduler.Type != types.Slurm {
		t.Errorf("Expected scheduler type 'slurm', got '%v'", *resp.Scheduler.Type)
	}
}

func TestUpdateJob_InvalidJSON(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)
	handler := srv.Routes()

	// Create a job first
	_ = store.CreateJob(context.Background(), &storage.Job{
		ID:    "update-invalid-json",
		User:  "testuser",
		Nodes: []string{"node-1"},
		State: storage.JobStateRunning,
	})

	req := httptest.NewRequest(http.MethodPatch, "/v1/jobs/update-invalid-json", bytes.NewReader([]byte("invalid")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", rec.Code)
	}
}

func TestUpdateJob_NotFound(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)
	handler := srv.Routes()

	state := types.Completed
	reqBody := types.UpdateJobRequest{State: &state}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPatch, "/v1/jobs/nonexistent", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", rec.Code)
	}
}

func TestUpdateJob_InvalidState(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)
	handler := srv.Routes()

	// Create a job first
	_ = store.CreateJob(context.Background(), &storage.Job{
		ID:    "update-invalid-state",
		User:  "testuser",
		Nodes: []string{"node-1"},
		State: storage.JobStateRunning,
	})

	invalidState := types.JobState("invalid")
	reqBody := types.UpdateJobRequest{State: &invalidState}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPatch, "/v1/jobs/update-invalid-state", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", rec.Code)
	}
}

func TestUpdateJob_AllFields(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)
	handler := srv.Routes()

	// Create a job first
	_ = store.CreateJob(context.Background(), &storage.Job{
		ID:        "update-all-fields",
		User:      "testuser",
		Nodes:     []string{"node-1"},
		State:     storage.JobStateRunning,
		StartTime: time.Now(),
	})

	state := types.Completed
	cpuUsage := 90.5
	memUsage := 8192
	gpuUsage := 75.0
	reqBody := types.UpdateJobRequest{
		State:         &state,
		CpuUsage:      &cpuUsage,
		MemoryUsageMb: &memUsage,
		GpuUsage:      &gpuUsage,
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPatch, "/v1/jobs/update-all-fields", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp types.Job
	_ = json.NewDecoder(rec.Body).Decode(&resp)

	if *resp.CpuUsage != 90.5 {
		t.Errorf("Expected CPU usage 90.5, got %f", *resp.CpuUsage)
	}
	if *resp.MemoryUsageMb != 8192 {
		t.Errorf("Expected memory 8192, got %d", *resp.MemoryUsageMb)
	}
	if *resp.GpuUsage != 75.0 {
		t.Errorf("Expected GPU usage 75.0, got %f", *resp.GpuUsage)
	}
}

func TestDeleteJob_NotFound(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)
	handler := srv.Routes()

	req := httptest.NewRequest(http.MethodDelete, "/v1/jobs/nonexistent", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", rec.Code)
	}
}

func TestListJobs_WithAllFilters(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)
	handler := srv.Routes()

	// Create test jobs
	for i := 0; i < 5; i++ {
		_ = store.CreateJob(context.Background(), &storage.Job{
			ID:    "filter-job-" + string(rune('0'+i)),
			User:  "alice",
			Nodes: []string{"node-01"},
			State: storage.JobStateRunning,
		})
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs?state=running&user=alice&node=node-01&limit=3&offset=1", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	var resp types.JobListResponse
	_ = json.NewDecoder(rec.Body).Decode(&resp)

	if resp.Limit != 3 {
		t.Errorf("Expected limit 3, got %d", resp.Limit)
	}
	if resp.Offset != 1 {
		t.Errorf("Expected offset 1, got %d", resp.Offset)
	}
}

func TestRecordMetrics_InvalidJSON(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)
	handler := srv.Routes()

	// Create a job first
	_ = store.CreateJob(context.Background(), &storage.Job{
		ID:        "metrics-invalid-json",
		User:      "testuser",
		Nodes:     []string{"node-1"},
		State:     storage.JobStateRunning,
		StartTime: time.Now(),
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/metrics-invalid-json/metrics", bytes.NewReader([]byte("invalid")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", rec.Code)
	}
}

func TestRecordMetrics_InvalidCPUUsage(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)
	handler := srv.Routes()

	// Create a job first
	_ = store.CreateJob(context.Background(), &storage.Job{
		ID:        "metrics-invalid-cpu",
		User:      "testuser",
		Nodes:     []string{"node-1"},
		State:     storage.JobStateRunning,
		StartTime: time.Now(),
	})

	// CPU usage > 100
	reqBody := types.RecordMetricsRequest{
		CpuUsage:      150.0,
		MemoryUsageMb: 1024,
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/metrics-invalid-cpu/metrics", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", rec.Code)
	}

	// CPU usage < 0
	reqBody.CpuUsage = -10.0
	body, _ = json.Marshal(reqBody)

	req = httptest.NewRequest(http.MethodPost, "/v1/jobs/metrics-invalid-cpu/metrics", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400 for negative CPU, got %d", rec.Code)
	}
}

func TestRecordMetrics_InvalidMemoryUsage(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)
	handler := srv.Routes()

	// Create a job first
	_ = store.CreateJob(context.Background(), &storage.Job{
		ID:        "metrics-invalid-mem",
		User:      "testuser",
		Nodes:     []string{"node-1"},
		State:     storage.JobStateRunning,
		StartTime: time.Now(),
	})

	reqBody := types.RecordMetricsRequest{
		CpuUsage:      50.0,
		MemoryUsageMb: -100,
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/metrics-invalid-mem/metrics", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", rec.Code)
	}
}

func TestGetJobMetrics_WithTimeFilters(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)
	handler := srv.Routes()

	// Create a job
	_ = store.CreateJob(context.Background(), &storage.Job{
		ID:        "metrics-time-filter",
		User:      "testuser",
		Nodes:     []string{"node-1"},
		State:     storage.JobStateRunning,
		StartTime: time.Now(),
	})

	// Record some metrics
	for i := 0; i < 10; i++ {
		_ = store.RecordMetrics(context.Background(), &storage.MetricSample{
			JobID:         "metrics-time-filter",
			Timestamp:     time.Now().Add(time.Duration(i) * time.Minute),
			CPUUsage:      float64(50 + i),
			MemoryUsageMB: 1024,
		})
	}

	now := time.Now().UTC()
	startTime := url.QueryEscape(now.Add(-5 * time.Minute).Format(time.RFC3339))
	endTime := url.QueryEscape(now.Add(5 * time.Minute).Format(time.RFC3339))

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/metrics-time-filter/metrics?start_time="+startTime+"&end_time="+endTime+"&limit=5", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestStorageJobToAPI_AllFields(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)

	gpuUsage := 50.0
	endTime := time.Now()
	priority := 100
	exitCode := 0
	extra := map[string]interface{}{"key": "value"}
	submitTime := time.Now().Add(-1 * time.Hour)

	job := &storage.Job{
		ID:             "full-job",
		User:           "testuser",
		Nodes:          []string{"node-1", "node-2"},
		State:          storage.JobStateCompleted,
		StartTime:      time.Now().Add(-2 * time.Hour),
		EndTime:        &endTime,
		RuntimeSeconds: 3600,
		CPUUsage:       75.0,
		MemoryUsageMB:  4096,
		GPUUsage:       &gpuUsage,
		Scheduler: &storage.SchedulerInfo{
			Type:          storage.SchedulerTypeSlurm,
			ExternalJobID: "12345",
			RawState:      "COMPLETED",
			SubmitTime:    &submitTime,
			Partition:     "gpu",
			Account:       "project_a",
			QoS:           "normal",
			Priority:      &priority,
			ExitCode:      &exitCode,
			Extra:         extra,
		},
	}

	apiJob := srv.storageJobToAPI(job)

	if apiJob.Id != "full-job" {
		t.Errorf("Expected ID 'full-job', got '%s'", apiJob.Id)
	}
	if apiJob.EndTime == nil {
		t.Error("Expected EndTime to be set")
	}
	if apiJob.RuntimeSeconds == nil || *apiJob.RuntimeSeconds != 3600 {
		t.Error("Expected RuntimeSeconds to be 3600")
	}
	if apiJob.Scheduler == nil {
		t.Fatal("Expected Scheduler info")
	}
	if *apiJob.Scheduler.Type != types.Slurm {
		t.Errorf("Expected scheduler type slurm")
	}
	if apiJob.Scheduler.Extra == nil {
		t.Error("Expected Extra to be set")
	}
}

func TestAPISchedulerToStorage_NilInput(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)

	result := srv.apiSchedulerToStorage(nil)
	if result != nil {
		t.Error("Expected nil result for nil input")
	}
}

func TestStorageSchedulerToAPI_NilInput(t *testing.T) {
	store := testStore(t)
	exporter := metrics.NewExporter(store)
	srv := NewServer(store, exporter)

	result := srv.storageSchedulerToAPI(nil)
	if result != nil {
		t.Error("Expected nil result for nil input")
	}
}

func TestIsValidState(t *testing.T) {
	validStates := []storage.JobState{
		storage.JobStatePending,
		storage.JobStateRunning,
		storage.JobStateCompleted,
		storage.JobStateFailed,
		storage.JobStateCancelled,
	}

	for _, state := range validStates {
		if !isValidState(state) {
			t.Errorf("Expected state '%s' to be valid", state)
		}
	}

	if isValidState(storage.JobState("invalid")) {
		t.Error("Expected 'invalid' state to be invalid")
	}
}
