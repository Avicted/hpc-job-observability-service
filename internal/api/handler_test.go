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
		store.CreateJob(context.Background(), &storage.Job{
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
	store.CreateJob(context.Background(), &storage.Job{
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
			store.RecordMetrics(context.Background(), &storage.MetricSample{
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
