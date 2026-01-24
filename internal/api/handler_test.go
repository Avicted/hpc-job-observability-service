package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/api/server"
	"github.com/Avicted/hpc-job-observability-service/internal/api/types"
	"github.com/Avicted/hpc-job-observability-service/internal/domain"
	"github.com/Avicted/hpc-job-observability-service/internal/storage"
	"github.com/Avicted/hpc-job-observability-service/internal/utils/mapper"
	"github.com/Avicted/hpc-job-observability-service/internal/utils/metrics"
)

// mockRepository implements storage.Storage for testing.
type mockRepository struct {
	jobs    map[string]*domain.Job
	samples map[string][]*domain.MetricSample

	createErr  error
	getErr     error
	updateErr  error
	deleteErr  error
	listErr    error
	recordErr  error
	metricsErr error
}

func newMockRepository() *mockRepository {
	return &mockRepository{
		jobs:    make(map[string]*domain.Job),
		samples: make(map[string][]*domain.MetricSample),
	}
}

func (m *mockRepository) CreateJob(ctx context.Context, job *domain.Job) error {
	if m.createErr != nil {
		return m.createErr
	}
	if _, exists := m.jobs[job.ID]; exists {
		return domain.ErrJobAlreadyExists
	}
	job.CreatedAt = time.Now()
	job.UpdatedAt = time.Now()
	m.jobs[job.ID] = job
	return nil
}

func (m *mockRepository) GetJob(ctx context.Context, id string) (*domain.Job, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	job, exists := m.jobs[id]
	if !exists {
		return nil, domain.ErrJobNotFound
	}
	return job, nil
}

func (m *mockRepository) UpdateJob(ctx context.Context, job *domain.Job) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	if _, exists := m.jobs[job.ID]; !exists {
		return domain.ErrJobNotFound
	}
	job.UpdatedAt = time.Now()
	m.jobs[job.ID] = job
	return nil
}

func (m *mockRepository) UpsertJob(ctx context.Context, job *domain.Job) error {
	job.UpdatedAt = time.Now()
	if _, exists := m.jobs[job.ID]; !exists {
		job.CreatedAt = time.Now()
	}
	m.jobs[job.ID] = job
	return nil
}

func (m *mockRepository) DeleteJob(ctx context.Context, id string) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	if _, exists := m.jobs[id]; !exists {
		return domain.ErrJobNotFound
	}
	delete(m.jobs, id)
	return nil
}

func (m *mockRepository) ListJobs(ctx context.Context, filter domain.JobFilter) ([]*domain.Job, int, error) {
	if m.listErr != nil {
		return nil, 0, m.listErr
	}
	var result []*domain.Job
	for _, job := range m.jobs {
		if filter.State != nil && job.State != *filter.State {
			continue
		}
		if filter.User != nil && job.User != *filter.User {
			continue
		}
		result = append(result, job)
	}
	return result, len(result), nil
}

func (m *mockRepository) GetAllJobs(ctx context.Context) ([]*domain.Job, error) {
	var result []*domain.Job
	for _, job := range m.jobs {
		result = append(result, job)
	}
	return result, nil
}

func (m *mockRepository) RecordMetrics(ctx context.Context, sample *domain.MetricSample) error {
	if m.recordErr != nil {
		return m.recordErr
	}
	m.samples[sample.JobID] = append(m.samples[sample.JobID], sample)
	return nil
}

func (m *mockRepository) GetJobMetrics(ctx context.Context, jobID string, filter domain.MetricsFilter) ([]*domain.MetricSample, int, error) {
	if m.metricsErr != nil {
		return nil, 0, m.metricsErr
	}
	if _, exists := m.jobs[jobID]; !exists {
		return nil, 0, domain.ErrJobNotFound
	}
	s := m.samples[jobID]
	return s, len(s), nil
}

func (m *mockRepository) GetLatestMetrics(ctx context.Context, jobID string) (*domain.MetricSample, error) {
	s := m.samples[jobID]
	if len(s) == 0 {
		return nil, nil
	}
	return s[len(s)-1], nil
}

func (m *mockRepository) DeleteMetricsBefore(cutoff time.Time) error {
	return nil
}

func (m *mockRepository) Migrate() error {
	return nil
}

func (m *mockRepository) Close() error {
	return nil
}

func (m *mockRepository) SeedDemoData() error {
	return nil
}

// mockStorage for metrics.Exporter (which still uses storage.Storage)
type mockStorage struct {
	jobs    map[string]*domain.Job
	samples map[string][]*domain.MetricSample
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		jobs:    make(map[string]*domain.Job),
		samples: make(map[string][]*domain.MetricSample),
	}
}

func (m *mockStorage) CreateJob(ctx context.Context, job *domain.Job) error {
	m.jobs[job.ID] = job
	return nil
}

func (m *mockStorage) GetJob(ctx context.Context, id string) (*domain.Job, error) {
	job, exists := m.jobs[id]
	if !exists {
		return nil, domain.ErrJobNotFound
	}
	return job, nil
}

func (m *mockStorage) UpdateJob(ctx context.Context, job *domain.Job) error {
	m.jobs[job.ID] = job
	return nil
}

func (m *mockStorage) UpsertJob(ctx context.Context, job *domain.Job) error {
	m.jobs[job.ID] = job
	return nil
}

func (m *mockStorage) DeleteJob(ctx context.Context, id string) error {
	delete(m.jobs, id)
	return nil
}

func (m *mockStorage) ListJobs(ctx context.Context, filter domain.JobFilter) ([]*domain.Job, int, error) {
	var result []*domain.Job
	for _, job := range m.jobs {
		result = append(result, job)
	}
	return result, len(result), nil
}

func (m *mockStorage) GetAllJobs(ctx context.Context) ([]*domain.Job, error) {
	var result []*domain.Job
	for _, job := range m.jobs {
		result = append(result, job)
	}
	return result, nil
}

func (m *mockStorage) RecordMetrics(ctx context.Context, sample *domain.MetricSample) error {
	m.samples[sample.JobID] = append(m.samples[sample.JobID], sample)
	return nil
}

func (m *mockStorage) GetJobMetrics(ctx context.Context, jobID string, filter domain.MetricsFilter) ([]*domain.MetricSample, int, error) {
	s := m.samples[jobID]
	return s, len(s), nil
}

func (m *mockStorage) GetLatestMetrics(ctx context.Context, jobID string) (*domain.MetricSample, error) {
	s := m.samples[jobID]
	if len(s) == 0 {
		return nil, nil
	}
	return s[len(s)-1], nil
}

func (m *mockStorage) DeleteMetricsBefore(cutoff time.Time) error {
	return nil
}

func (m *mockStorage) Migrate() error {
	return nil
}

func (m *mockStorage) Close() error {
	return nil
}

func (m *mockStorage) SeedDemoData() error {
	return nil
}

func setupTestServer(repo storage.Storage) *Server {
	store := newMockStorage()
	exporter := metrics.NewExporter(store)
	return NewServer(repo, exporter)
}

func TestNewServer(t *testing.T) {
	repo := newMockRepository()
	store := newMockStorage()
	exporter := metrics.NewExporter(store)
	srv := NewServer(repo, exporter)

	if srv == nil {
		t.Fatal("expected non-nil server")
	}
	if srv.store != repo {
		t.Error("expected repo to be set")
	}
	if srv.exporter != exporter {
		t.Error("expected exporter to be set")
	}
}

func TestGetHealth(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	rec := httptest.NewRecorder()

	srv.GetHealth(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var resp types.HealthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Status != types.Healthy {
		t.Errorf("expected status 'healthy', got %s", resp.Status)
	}
	if resp.Version == nil || *resp.Version != "1.0.0" {
		t.Error("expected version to be 1.0.0")
	}
}

func TestCreateJob_Success(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	reqBody := types.CreateJobRequest{
		Id:    "test-job-1",
		User:  "testuser",
		Nodes: []string{"node-1", "node-2"},
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	params := server.CreateJobParams{
		XChangedBy: "api",
		XSource:    "test",
	}
	srv.CreateJob(rec, req, params)

	if rec.Code != http.StatusCreated {
		t.Errorf("expected status 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp types.Job
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Id != "test-job-1" {
		t.Errorf("expected job ID 'test-job-1', got %s", resp.Id)
	}
	if resp.User != "testuser" {
		t.Errorf("expected user 'testuser', got %s", resp.User)
	}
	if len(resp.Nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(resp.Nodes))
	}
	if resp.State != types.Running {
		t.Errorf("expected state 'running', got %s", resp.State)
	}
}

func TestCreateJob_MissingAuditHeaders(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	reqBody := types.CreateJobRequest{
		Id:    "test-job-1",
		User:  "testuser",
		Nodes: []string{"node-1"},
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	params := server.CreateJobParams{
		XChangedBy: "",
		XSource:    "test",
	}
	srv.CreateJob(rec, req, params)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}

	var errResp types.ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Error != "validation_error" {
		t.Errorf("expected 'validation_error', got %s", errResp.Error)
	}
}

func TestCreateJob_MissingRequiredFields(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	tests := []struct {
		name    string
		req     types.CreateJobRequest
		wantMsg string
	}{
		{
			name:    "missing job ID",
			req:     types.CreateJobRequest{User: "user", Nodes: []string{"node-1"}},
			wantMsg: "Job ID is required",
		},
		{
			name:    "missing user",
			req:     types.CreateJobRequest{Id: "job-1", Nodes: []string{"node-1"}},
			wantMsg: "User is required",
		},
		{
			name:    "missing nodes",
			req:     types.CreateJobRequest{Id: "job-1", User: "user"},
			wantMsg: "At least one node is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, _ := json.Marshal(tt.req)
			req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			params := server.CreateJobParams{XChangedBy: "api", XSource: "test"}
			srv.CreateJob(rec, req, params)

			if rec.Code != http.StatusBadRequest {
				t.Errorf("expected status 400, got %d", rec.Code)
			}

			var errResp types.ErrorResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
				t.Fatalf("failed to decode error response: %v", err)
			}
			if errResp.Message != tt.wantMsg {
				t.Errorf("expected message %q, got %q", tt.wantMsg, errResp.Message)
			}
		})
	}
}

func TestCreateJob_Duplicate(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	repo.jobs["test-job-1"] = &domain.Job{ID: "test-job-1", User: "user", Nodes: []string{"node-1"}}

	reqBody := types.CreateJobRequest{
		Id:    "test-job-1",
		User:  "testuser",
		Nodes: []string{"node-1"},
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	params := server.CreateJobParams{XChangedBy: "api", XSource: "test"}
	srv.CreateJob(rec, req, params)

	if rec.Code != http.StatusConflict {
		t.Errorf("expected status 409, got %d", rec.Code)
	}
}

func TestCreateJob_InvalidJSON(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader([]byte("invalid json")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	params := server.CreateJobParams{XChangedBy: "api", XSource: "test"}
	srv.CreateJob(rec, req, params)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

func TestGetJob_Success(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	startTime := time.Now().Add(-1 * time.Hour)
	repo.jobs["test-job-1"] = &domain.Job{
		ID:            "test-job-1",
		User:          "testuser",
		Nodes:         []string{"node-1"},
		State:         domain.JobStateRunning,
		StartTime:     startTime,
		CPUUsage:      50.5,
		MemoryUsageMB: 2048,
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/test-job-1", nil)
	rec := httptest.NewRecorder()

	srv.GetJob(rec, req, "test-job-1")

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp types.Job
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Id != "test-job-1" {
		t.Errorf("expected job ID 'test-job-1', got %s", resp.Id)
	}
	if resp.State != types.Running {
		t.Errorf("expected state 'running', got %s", resp.State)
	}
	if resp.CpuUsage == nil || *resp.CpuUsage != 50.5 {
		t.Errorf("expected CPU usage 50.5, got %v", resp.CpuUsage)
	}
}

func TestGetJob_NotFound(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/nonexistent", nil)
	rec := httptest.NewRecorder()

	srv.GetJob(rec, req, "nonexistent")

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", rec.Code)
	}
}

func TestListJobs_Success(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	repo.jobs["job-1"] = &domain.Job{ID: "job-1", User: "alice", Nodes: []string{"node-1"}, State: domain.JobStateRunning}
	repo.jobs["job-2"] = &domain.Job{ID: "job-2", User: "bob", Nodes: []string{"node-2"}, State: domain.JobStateCompleted}
	repo.jobs["job-3"] = &domain.Job{ID: "job-3", User: "alice", Nodes: []string{"node-1"}, State: domain.JobStateRunning}

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	rec := httptest.NewRecorder()

	params := server.ListJobsParams{}
	srv.ListJobs(rec, req, params)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var resp types.JobListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Total != 3 {
		t.Errorf("expected 3 jobs, got %d", resp.Total)
	}
}

func TestListJobs_FilterByState(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	repo.jobs["job-1"] = &domain.Job{ID: "job-1", User: "alice", Nodes: []string{"node-1"}, State: domain.JobStateRunning}
	repo.jobs["job-2"] = &domain.Job{ID: "job-2", User: "bob", Nodes: []string{"node-2"}, State: domain.JobStateCompleted}

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs?state=running", nil)
	rec := httptest.NewRecorder()

	state := server.JobState("running")
	params := server.ListJobsParams{State: &state}
	srv.ListJobs(rec, req, params)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var resp types.JobListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Total != 1 {
		t.Errorf("expected 1 job, got %d", resp.Total)
	}
}

func TestListJobs_FilterByUser(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	repo.jobs["job-1"] = &domain.Job{ID: "job-1", User: "alice", Nodes: []string{"node-1"}, State: domain.JobStateRunning}
	repo.jobs["job-2"] = &domain.Job{ID: "job-2", User: "bob", Nodes: []string{"node-2"}, State: domain.JobStateRunning}

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs?user=alice", nil)
	rec := httptest.NewRecorder()

	user := "alice"
	params := server.ListJobsParams{User: &user}
	srv.ListJobs(rec, req, params)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var resp types.JobListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Total != 1 {
		t.Errorf("expected 1 job, got %d", resp.Total)
	}
}

func TestListJobs_Pagination(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	for i := 0; i < 10; i++ {
		id := "job-" + string(rune('0'+i))
		repo.jobs[id] = &domain.Job{ID: id, User: "user", Nodes: []string{"node"}, State: domain.JobStateRunning}
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs?limit=5&offset=3", nil)
	rec := httptest.NewRecorder()

	limit := 5
	offset := 3
	params := server.ListJobsParams{Limit: &limit, Offset: &offset}
	srv.ListJobs(rec, req, params)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var resp types.JobListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Limit != 5 {
		t.Errorf("expected limit 5, got %d", resp.Limit)
	}
	if resp.Offset != 3 {
		t.Errorf("expected offset 3, got %d", resp.Offset)
	}
}

func TestListJobs_Error(t *testing.T) {
	repo := newMockRepository()
	repo.listErr = errors.New("database error")
	srv := setupTestServer(repo)

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	rec := httptest.NewRecorder()

	params := server.ListJobsParams{}
	srv.ListJobs(rec, req, params)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", rec.Code)
	}
}

func TestUpdateJob_Success(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	repo.jobs["test-job-1"] = &domain.Job{
		ID:        "test-job-1",
		User:      "testuser",
		Nodes:     []string{"node-1"},
		State:     domain.JobStateRunning,
		StartTime: time.Now(),
	}

	state := types.JobState("completed")
	cpuUsage := 75.5
	memUsage := 4096
	reqBody := types.UpdateJobRequest{
		State:         &state,
		CpuUsage:      &cpuUsage,
		MemoryUsageMb: &memUsage,
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPatch, "/v1/jobs/test-job-1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	params := server.UpdateJobParams{XChangedBy: "api", XSource: "test"}
	srv.UpdateJob(rec, req, "test-job-1", params)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp types.Job
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.State != types.Completed {
		t.Errorf("expected state 'completed', got %s", resp.State)
	}
	if resp.CpuUsage == nil || *resp.CpuUsage != 75.5 {
		t.Errorf("expected CPU usage 75.5, got %v", resp.CpuUsage)
	}
}

func TestUpdateJob_NotFound(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	state := types.JobState("completed")
	reqBody := types.UpdateJobRequest{State: &state}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPatch, "/v1/jobs/nonexistent", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	params := server.UpdateJobParams{XChangedBy: "api", XSource: "test"}
	srv.UpdateJob(rec, req, "nonexistent", params)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", rec.Code)
	}
}

func TestUpdateJob_InvalidState(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	repo.jobs["test-job-1"] = &domain.Job{
		ID:        "test-job-1",
		User:      "testuser",
		Nodes:     []string{"node-1"},
		State:     domain.JobStateRunning,
		StartTime: time.Now(),
	}

	state := types.JobState("invalid_state")
	reqBody := types.UpdateJobRequest{State: &state}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPatch, "/v1/jobs/test-job-1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	params := server.UpdateJobParams{XChangedBy: "api", XSource: "test"}
	srv.UpdateJob(rec, req, "test-job-1", params)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

func TestUpdateJob_InvalidJSON(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	repo.jobs["test-job-1"] = &domain.Job{
		ID:        "test-job-1",
		User:      "testuser",
		Nodes:     []string{"node-1"},
		State:     domain.JobStateRunning,
		StartTime: time.Now(),
	}

	req := httptest.NewRequest(http.MethodPatch, "/v1/jobs/test-job-1", bytes.NewReader([]byte("invalid json")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	params := server.UpdateJobParams{XChangedBy: "api", XSource: "test"}
	srv.UpdateJob(rec, req, "test-job-1", params)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

func TestDeleteJob_Success(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	repo.jobs["test-job-1"] = &domain.Job{ID: "test-job-1", User: "testuser", Nodes: []string{"node-1"}}

	req := httptest.NewRequest(http.MethodDelete, "/v1/jobs/test-job-1", nil)
	rec := httptest.NewRecorder()

	params := server.DeleteJobParams{XChangedBy: "api", XSource: "test"}
	srv.DeleteJob(rec, req, "test-job-1", params)

	if rec.Code != http.StatusNoContent {
		t.Errorf("expected status 204, got %d", rec.Code)
	}

	if _, exists := repo.jobs["test-job-1"]; exists {
		t.Error("expected job to be deleted")
	}
}

func TestDeleteJob_NotFound(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	req := httptest.NewRequest(http.MethodDelete, "/v1/jobs/nonexistent", nil)
	rec := httptest.NewRecorder()

	params := server.DeleteJobParams{XChangedBy: "api", XSource: "test"}
	srv.DeleteJob(rec, req, "nonexistent", params)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", rec.Code)
	}
}

func TestGetJobMetrics_Success(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	repo.jobs["test-job-1"] = &domain.Job{ID: "test-job-1", User: "testuser", Nodes: []string{"node-1"}}
	repo.samples["test-job-1"] = []*domain.MetricSample{
		{JobID: "test-job-1", Timestamp: time.Now().Add(-5 * time.Minute), CPUUsage: 50.0, MemoryUsageMB: 2048},
		{JobID: "test-job-1", Timestamp: time.Now().Add(-3 * time.Minute), CPUUsage: 60.0, MemoryUsageMB: 2560},
		{JobID: "test-job-1", Timestamp: time.Now(), CPUUsage: 55.0, MemoryUsageMB: 2300},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/test-job-1/metrics", nil)
	rec := httptest.NewRecorder()

	params := server.GetJobMetricsParams{}
	srv.GetJobMetrics(rec, req, "test-job-1", params)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var resp types.JobMetricsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.JobId != "test-job-1" {
		t.Errorf("expected job ID 'test-job-1', got %s", resp.JobId)
	}
	if resp.Total != 3 {
		t.Errorf("expected 3 samples, got %d", resp.Total)
	}
	if len(resp.Samples) != 3 {
		t.Errorf("expected 3 samples, got %d", len(resp.Samples))
	}
}

func TestGetJobMetrics_NotFound(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/nonexistent/metrics", nil)
	rec := httptest.NewRecorder()

	params := server.GetJobMetricsParams{}
	srv.GetJobMetrics(rec, req, "nonexistent", params)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", rec.Code)
	}
}

func TestRecordJobMetrics_Success(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	repo.jobs["test-job-1"] = &domain.Job{
		ID:        "test-job-1",
		User:      "testuser",
		Nodes:     []string{"node-1"},
		State:     domain.JobStateRunning,
		StartTime: time.Now(),
	}

	reqBody := types.RecordMetricsRequest{
		CpuUsage:      75.5,
		MemoryUsageMb: 4096,
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/test-job-1/metrics", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Changed-By", "api")
	req.Header.Set("X-Source", "test")
	rec := httptest.NewRecorder()

	srv.RecordJobMetrics(rec, req, "test-job-1")

	if rec.Code != http.StatusCreated {
		t.Errorf("expected status 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp types.MetricSample
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.CpuUsage != 75.5 {
		t.Errorf("expected CPU usage 75.5, got %f", resp.CpuUsage)
	}
	if resp.MemoryUsageMb != 4096 {
		t.Errorf("expected memory usage 4096, got %d", resp.MemoryUsageMb)
	}

	if len(repo.samples["test-job-1"]) != 1 {
		t.Errorf("expected 1 metric sample, got %d", len(repo.samples["test-job-1"]))
	}
}

func TestRecordJobMetrics_NotFound(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	reqBody := types.RecordMetricsRequest{
		CpuUsage:      75.5,
		MemoryUsageMb: 4096,
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/nonexistent/metrics", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Changed-By", "api")
	req.Header.Set("X-Source", "test")
	rec := httptest.NewRecorder()

	srv.RecordJobMetrics(rec, req, "nonexistent")

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", rec.Code)
	}
}

func TestRecordJobMetrics_ValidationErrors(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	repo.jobs["test-job-1"] = &domain.Job{
		ID:        "test-job-1",
		User:      "testuser",
		Nodes:     []string{"node-1"},
		State:     domain.JobStateRunning,
		StartTime: time.Now(),
	}

	tests := []struct {
		name    string
		req     types.RecordMetricsRequest
		wantMsg string
	}{
		{
			name:    "CPU usage too high",
			req:     types.RecordMetricsRequest{CpuUsage: 150, MemoryUsageMb: 1024},
			wantMsg: "CPU usage must be between 0 and 100",
		},
		{
			name:    "CPU usage negative",
			req:     types.RecordMetricsRequest{CpuUsage: -10, MemoryUsageMb: 1024},
			wantMsg: "CPU usage must be between 0 and 100",
		},
		{
			name:    "Memory usage negative",
			req:     types.RecordMetricsRequest{CpuUsage: 50, MemoryUsageMb: -100},
			wantMsg: "Memory usage must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, _ := json.Marshal(tt.req)
			req := httptest.NewRequest(http.MethodPost, "/v1/jobs/test-job-1/metrics", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Changed-By", "api")
			req.Header.Set("X-Source", "test")
			rec := httptest.NewRecorder()

			srv.RecordJobMetrics(rec, req, "test-job-1")

			if rec.Code != http.StatusBadRequest {
				t.Errorf("expected status 400, got %d", rec.Code)
			}

			var errResp types.ErrorResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
				t.Fatalf("failed to decode error response: %v", err)
			}
			if errResp.Message != tt.wantMsg {
				t.Errorf("expected message %q, got %q", tt.wantMsg, errResp.Message)
			}
		})
	}
}

func TestRecordJobMetrics_MissingAuditHeaders(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	repo.jobs["test-job-1"] = &domain.Job{
		ID:        "test-job-1",
		User:      "testuser",
		Nodes:     []string{"node-1"},
		State:     domain.JobStateRunning,
		StartTime: time.Now(),
	}

	reqBody := types.RecordMetricsRequest{
		CpuUsage:      75.5,
		MemoryUsageMb: 4096,
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/test-job-1/metrics", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.RecordJobMetrics(rec, req, "test-job-1")

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

func TestRoutes(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	handler := srv.Routes()
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestStorageJobToAPI_WithSchedulerInfo(t *testing.T) {
	m := mapper.NewMapper()

	submitTime := time.Now().Add(-1 * time.Hour)
	priority := int64(100)
	exitCode := 0
	timeLimitMins := 60
	gpuUsage := 85.5

	job := &domain.Job{
		ID:             "test-job-1",
		User:           "testuser",
		Nodes:          []string{"node-1", "node-2"},
		NodeCount:      2,
		State:          domain.JobStateCompleted,
		StartTime:      time.Now().Add(-30 * time.Minute),
		RuntimeSeconds: 1800,
		CPUUsage:       75.5,
		MemoryUsageMB:  4096,
		GPUUsage:       &gpuUsage,
		RequestedCPUs:  4,
		AllocatedCPUs:  4,
		RequestedMemMB: 8192,
		AllocatedMemMB: 8192,
		RequestedGPUs:  1,
		AllocatedGPUs:  1,
		ClusterName:    "cluster1",
		SchedulerInst:  "slurm1",
		IngestVersion:  "v1",
		SampleCount:    10,
		AvgCPUUsage:    65.0,
		MaxCPUUsage:    80.0,
		MaxMemUsageMB:  5000,
		AvgGPUUsage:    70.0,
		MaxGPUUsage:    90.0,
		Scheduler: &domain.SchedulerInfo{
			Type:          domain.SchedulerTypeSlurm,
			ExternalJobID: "12345",
			RawState:      "COMPLETED",
			SubmitTime:    &submitTime,
			Partition:     "gpu",
			Account:       "project1",
			QoS:           "normal",
			Priority:      &priority,
			ExitCode:      &exitCode,
			StateReason:   "None",
			TimeLimitMins: &timeLimitMins,
			Extra:         map[string]interface{}{"custom": "value"},
		},
	}

	resp := m.DomainJobToAPI(job)

	if resp.Id != "test-job-1" {
		t.Errorf("expected ID 'test-job-1', got %s", resp.Id)
	}
	if resp.Scheduler == nil {
		t.Fatal("expected scheduler info")
	}
	if resp.Scheduler.Type == nil || *resp.Scheduler.Type != types.Slurm {
		t.Error("expected scheduler type 'slurm'")
	}
	if resp.Scheduler.ExternalJobId == nil || *resp.Scheduler.ExternalJobId != "12345" {
		t.Error("expected external job ID '12345'")
	}
	if resp.Scheduler.Partition == nil || *resp.Scheduler.Partition != "gpu" {
		t.Error("expected partition 'gpu'")
	}
	if resp.NodeCount == nil || *resp.NodeCount != 2 {
		t.Error("expected node count 2")
	}
	if resp.RuntimeSeconds == nil || *resp.RuntimeSeconds != 1800 {
		t.Error("expected runtime 1800")
	}
}

func TestApiSchedulerToStorage(t *testing.T) {
	m := mapper.NewMapper()

	result := m.APISchedulerToDomain(nil)
	if result != nil {
		t.Error("expected nil result for nil input")
	}

	schedType := types.Slurm
	externalJobID := "12345"
	rawState := "RUNNING"
	partition := "gpu"
	account := "project1"
	qos := "normal"
	priority := int64(100)
	exitCode := 1
	stateReason := "Resources"
	timeLimitMins := 120
	submitTime := time.Now()

	apiSched := &types.SchedulerInfo{
		Type:             &schedType,
		ExternalJobId:    &externalJobID,
		RawState:         &rawState,
		SubmitTime:       &submitTime,
		Partition:        &partition,
		Account:          &account,
		Qos:              &qos,
		Priority:         &priority,
		ExitCode:         &exitCode,
		StateReason:      &stateReason,
		TimeLimitMinutes: &timeLimitMins,
		Extra:            &map[string]interface{}{"key": "value"},
	}

	result = m.APISchedulerToDomain(apiSched)

	if result.Type != domain.SchedulerTypeSlurm {
		t.Errorf("expected type 'slurm', got %s", result.Type)
	}
	if result.ExternalJobID != "12345" {
		t.Errorf("expected external job ID '12345', got %s", result.ExternalJobID)
	}
	if result.Partition != "gpu" {
		t.Errorf("expected partition 'gpu', got %s", result.Partition)
	}
	if result.Priority == nil || *result.Priority != 100 {
		t.Error("expected priority 100")
	}
}

func TestIsValidState(t *testing.T) {
	m := mapper.NewMapper()

	validStates := []domain.JobState{
		domain.JobStatePending,
		domain.JobStateRunning,
		domain.JobStateCompleted,
		domain.JobStateFailed,
		domain.JobStateCancelled,
	}

	for _, state := range validStates {
		if !m.IsValidJobState(state) {
			t.Errorf("expected state %s to be valid", state)
		}
	}

	invalidStates := []domain.JobState{"invalid", "unknown", ""}
	for _, state := range invalidStates {
		if m.IsValidJobState(state) {
			t.Errorf("expected state %s to be invalid", state)
		}
	}
}

func TestHandleError(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()

	srv.handleError(rec, req, errors.New("test error"))

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}

	var errResp types.ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Error != "invalid_request" {
		t.Errorf("expected error 'invalid_request', got %s", errResp.Error)
	}
}

func TestCreateJob_StorageError(t *testing.T) {
	repo := newMockRepository()
	repo.createErr = errors.New("database error")
	srv := setupTestServer(repo)

	reqBody := types.CreateJobRequest{
		Id:    "test-job-1",
		User:  "testuser",
		Nodes: []string{"node-1"},
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	params := server.CreateJobParams{XChangedBy: "api", XSource: "test"}
	srv.CreateJob(rec, req, params)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", rec.Code)
	}
}

func TestGetJob_StorageError(t *testing.T) {
	repo := newMockRepository()
	repo.getErr = errors.New("database error")
	srv := setupTestServer(repo)

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/test-job-1", nil)
	rec := httptest.NewRecorder()

	srv.GetJob(rec, req, "test-job-1")

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", rec.Code)
	}
}

func TestUpdateJob_GPUUsage(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	repo.jobs["test-job-1"] = &domain.Job{
		ID:        "test-job-1",
		User:      "testuser",
		Nodes:     []string{"node-1"},
		State:     domain.JobStateRunning,
		StartTime: time.Now(),
	}

	gpuUsage := 85.5
	reqBody := types.UpdateJobRequest{
		GpuUsage: &gpuUsage,
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPatch, "/v1/jobs/test-job-1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	params := server.UpdateJobParams{XChangedBy: "api", XSource: "test"}
	srv.UpdateJob(rec, req, "test-job-1", params)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp types.Job
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.GpuUsage == nil || *resp.GpuUsage != 85.5 {
		t.Errorf("expected GPU usage 85.5, got %v", resp.GpuUsage)
	}
}

func TestRecordJobMetrics_WithGPU(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	repo.jobs["test-job-1"] = &domain.Job{
		ID:        "test-job-1",
		User:      "testuser",
		Nodes:     []string{"node-1"},
		State:     domain.JobStateRunning,
		StartTime: time.Now(),
	}

	gpuUsage := 90.0
	reqBody := types.RecordMetricsRequest{
		CpuUsage:      75.5,
		MemoryUsageMb: 4096,
		GpuUsage:      &gpuUsage,
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/test-job-1/metrics", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Changed-By", "api")
	req.Header.Set("X-Source", "test")
	rec := httptest.NewRecorder()

	srv.RecordJobMetrics(rec, req, "test-job-1")

	if rec.Code != http.StatusCreated {
		t.Errorf("expected status 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp types.MetricSample
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.GpuUsage == nil || *resp.GpuUsage != 90.0 {
		t.Errorf("expected GPU usage 90.0, got %v", resp.GpuUsage)
	}
}

func TestDeleteJob_StorageError(t *testing.T) {
	repo := newMockRepository()
	repo.deleteErr = errors.New("database error")
	repo.jobs["test-job-1"] = &domain.Job{ID: "test-job-1", User: "testuser", Nodes: []string{"node-1"}}
	srv := setupTestServer(repo)

	req := httptest.NewRequest(http.MethodDelete, "/v1/jobs/test-job-1", nil)
	rec := httptest.NewRecorder()

	params := server.DeleteJobParams{XChangedBy: "api", XSource: "test"}
	srv.DeleteJob(rec, req, "test-job-1", params)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", rec.Code)
	}
}

func TestUpdateJob_StorageError(t *testing.T) {
	repo := newMockRepository()
	repo.updateErr = errors.New("database error")
	repo.jobs["test-job-1"] = &domain.Job{
		ID:        "test-job-1",
		User:      "testuser",
		Nodes:     []string{"node-1"},
		State:     domain.JobStateRunning,
		StartTime: time.Now(),
	}
	srv := setupTestServer(repo)

	state := types.JobState("completed")
	reqBody := types.UpdateJobRequest{State: &state}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPatch, "/v1/jobs/test-job-1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	params := server.UpdateJobParams{XChangedBy: "api", XSource: "test"}
	srv.UpdateJob(rec, req, "test-job-1", params)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", rec.Code)
	}
}

func TestGetJobMetrics_WithFilters(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	repo.jobs["test-job-1"] = &domain.Job{ID: "test-job-1", User: "testuser", Nodes: []string{"node-1"}}
	repo.samples["test-job-1"] = []*domain.MetricSample{
		{JobID: "test-job-1", Timestamp: time.Now(), CPUUsage: 50.0, MemoryUsageMB: 2048},
	}

	startTime := time.Now().Add(-1 * time.Hour)
	endTime := time.Now()
	limit := 100

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/test-job-1/metrics", nil)
	rec := httptest.NewRecorder()

	params := server.GetJobMetricsParams{
		StartTime: &startTime,
		EndTime:   &endTime,
		Limit:     &limit,
	}
	srv.GetJobMetrics(rec, req, "test-job-1", params)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestGetJobMetrics_StorageError(t *testing.T) {
	repo := newMockRepository()
	repo.metricsErr = errors.New("database error")
	repo.jobs["test-job-1"] = &domain.Job{ID: "test-job-1", User: "testuser", Nodes: []string{"node-1"}}
	srv := setupTestServer(repo)

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/test-job-1/metrics", nil)
	rec := httptest.NewRecorder()

	params := server.GetJobMetricsParams{}
	srv.GetJobMetrics(rec, req, "test-job-1", params)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", rec.Code)
	}
}

func TestRecordJobMetrics_StorageError(t *testing.T) {
	repo := newMockRepository()
	repo.recordErr = errors.New("database error")
	repo.jobs["test-job-1"] = &domain.Job{
		ID:        "test-job-1",
		User:      "testuser",
		Nodes:     []string{"node-1"},
		State:     domain.JobStateRunning,
		StartTime: time.Now(),
	}
	srv := setupTestServer(repo)

	reqBody := types.RecordMetricsRequest{
		CpuUsage:      75.5,
		MemoryUsageMb: 4096,
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/test-job-1/metrics", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Changed-By", "api")
	req.Header.Set("X-Source", "test")
	rec := httptest.NewRecorder()

	srv.RecordJobMetrics(rec, req, "test-job-1")

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", rec.Code)
	}
}

func TestRecordJobMetrics_InvalidJSON(t *testing.T) {
	repo := newMockRepository()
	repo.jobs["test-job-1"] = &domain.Job{
		ID:        "test-job-1",
		User:      "testuser",
		Nodes:     []string{"node-1"},
		State:     domain.JobStateRunning,
		StartTime: time.Now(),
	}
	srv := setupTestServer(repo)

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/test-job-1/metrics", bytes.NewReader([]byte("invalid")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Changed-By", "api")
	req.Header.Set("X-Source", "test")
	rec := httptest.NewRecorder()

	srv.RecordJobMetrics(rec, req, "test-job-1")

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

// =========================================================================
// Lifecycle Event Handler Tests
// =========================================================================

func TestJobStartedEvent_Success(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	gpuVendor := types.Nvidia
	gpuAllocation := 2
	cpuAllocation := 4
	memAllocation := 16384
	partition := "gpu"
	account := "ml-team"
	cgroupPath := "/sys/fs/cgroup/slurm/job_12345"
	gpuDevices := []string{"GPU-0", "GPU-1"}

	event := types.JobStartedEvent{
		JobId:              "test-job-123",
		User:               "testuser",
		NodeList:           []string{"node1", "node2"},
		Timestamp:          time.Now(),
		GpuVendor:          &gpuVendor,
		GpuAllocation:      &gpuAllocation,
		CpuAllocation:      &cpuAllocation,
		MemoryAllocationMb: &memAllocation,
		Partition:          &partition,
		Account:            &account,
		CgroupPath:         &cgroupPath,
		GpuDevices:         &gpuDevices,
	}

	body, _ := json.Marshal(event)
	req := httptest.NewRequest(http.MethodPost, "/v1/events/job-started", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.JobStartedEvent(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp types.JobEventResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.Status != types.Created {
		t.Errorf("expected status 'created', got '%s'", resp.Status)
	}
	if resp.JobId != "test-job-123" {
		t.Errorf("expected job_id 'test-job-123', got '%s'", resp.JobId)
	}

	// Verify job was created in storage
	job, err := repo.GetJob(context.Background(), "test-job-123")
	if err != nil {
		t.Fatalf("failed to get job from storage: %v", err)
	}

	if job.User != "testuser" {
		t.Errorf("expected user 'testuser', got '%s'", job.User)
	}
	if job.State != domain.JobStateRunning {
		t.Errorf("expected state 'running', got '%s'", job.State)
	}
	if job.NodeCount != 2 {
		t.Errorf("expected node_count 2, got %d", job.NodeCount)
	}
	if job.GPUVendor != domain.GPUVendor(gpuVendor) {
		t.Errorf("expected gpu_vendor '%s', got '%s'", string(gpuVendor), job.GPUVendor)
	}
	if job.GPUCount != gpuAllocation {
		t.Errorf("expected gpu_count %d, got %d", gpuAllocation, job.GPUCount)
	}
	if job.AllocatedCPUs != int64(cpuAllocation) {
		t.Errorf("expected allocated_cpus %d, got %d", cpuAllocation, job.AllocatedCPUs)
	}
	if job.AllocatedMemMB != int64(memAllocation) {
		t.Errorf("expected allocated_mem_mb %d, got %d", memAllocation, job.AllocatedMemMB)
	}
	if job.CgroupPath != cgroupPath {
		t.Errorf("expected cgroup_path '%s', got '%s'", cgroupPath, job.CgroupPath)
	}
	if len(job.GPUDevices) != len(gpuDevices) {
		t.Errorf("expected %d gpu_devices, got %d", len(gpuDevices), len(job.GPUDevices))
	}
	if job.Scheduler == nil {
		t.Fatal("expected scheduler info to be set")
	}
	if job.Scheduler.Partition != partition {
		t.Errorf("expected partition '%s', got '%s'", partition, job.Scheduler.Partition)
	}
	if job.Scheduler.Account != account {
		t.Errorf("expected account '%s', got '%s'", account, job.Scheduler.Account)
	}
}

func TestJobStartedEvent_Idempotent(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	event := types.JobStartedEvent{
		JobId:     "test-job-idempotent",
		User:      "testuser",
		NodeList:  []string{"node1"},
		Timestamp: time.Now(),
	}

	// First request
	body, _ := json.Marshal(event)
	req1 := httptest.NewRequest(http.MethodPost, "/v1/events/job-started", bytes.NewReader(body))
	req1.Header.Set("Content-Type", "application/json")
	rec1 := httptest.NewRecorder()
	srv.JobStartedEvent(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("first request failed: %s", rec1.Body.String())
	}

	var resp1 types.JobEventResponse
	json.Unmarshal(rec1.Body.Bytes(), &resp1)
	if resp1.Status != types.Created {
		t.Errorf("first request status should be 'created', got '%s'", resp1.Status)
	}

	// Second request (duplicate)
	req2 := httptest.NewRequest(http.MethodPost, "/v1/events/job-started", bytes.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()
	srv.JobStartedEvent(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("second request failed: %s", rec2.Body.String())
	}

	var resp2 types.JobEventResponse
	json.Unmarshal(rec2.Body.Bytes(), &resp2)
	if resp2.Status != types.Skipped {
		t.Errorf("second request status should be 'skipped', got '%s'", resp2.Status)
	}
}

func TestJobStartedEvent_MissingRequiredFields(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	testCases := []struct {
		name  string
		event types.JobStartedEvent
	}{
		{
			name: "missing job_id",
			event: types.JobStartedEvent{
				User:      "testuser",
				NodeList:  []string{"node1"},
				Timestamp: time.Now(),
			},
		},
		{
			name: "missing user",
			event: types.JobStartedEvent{
				JobId:     "test-job",
				NodeList:  []string{"node1"},
				Timestamp: time.Now(),
			},
		},
		{
			name: "missing node_list",
			event: types.JobStartedEvent{
				JobId:     "test-job",
				User:      "testuser",
				Timestamp: time.Now(),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			body, _ := json.Marshal(tc.event)
			req := httptest.NewRequest(http.MethodPost, "/v1/events/job-started", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			srv.JobStartedEvent(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Errorf("expected status 400, got %d: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestJobStartedEvent_InvalidJSON(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	req := httptest.NewRequest(http.MethodPost, "/v1/events/job-started", bytes.NewReader([]byte("invalid")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.JobStartedEvent(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

func TestJobFinishedEvent_Success(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	// Create a job first
	startTime := time.Now().Add(-5 * time.Minute)
	repo.jobs["test-job-finish"] = &domain.Job{
		ID:        "test-job-finish",
		User:      "testuser",
		Nodes:     []string{"node1"},
		State:     domain.JobStateRunning,
		StartTime: startTime,
		Scheduler: &domain.SchedulerInfo{
			Type: domain.SchedulerTypeSlurm,
		},
	}

	exitCode := 0
	event := types.JobFinishedEvent{
		JobId:      "test-job-finish",
		FinalState: types.Completed,
		ExitCode:   &exitCode,
		Timestamp:  time.Now(),
	}

	body, _ := json.Marshal(event)
	req := httptest.NewRequest(http.MethodPost, "/v1/events/job-finished", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.JobFinishedEvent(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp types.JobEventResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.Status != types.Updated {
		t.Errorf("expected status 'updated', got '%s'", resp.Status)
	}

	// Verify job was updated in storage
	job, _ := repo.GetJob(context.Background(), "test-job-finish")
	if job.State != domain.JobStateCompleted {
		t.Errorf("expected state 'completed', got '%s'", job.State)
	}
	if job.EndTime == nil {
		t.Error("expected end_time to be set")
	}
	if job.RuntimeSeconds <= 0 {
		t.Errorf("expected positive runtime_seconds, got %f", job.RuntimeSeconds)
	}
	if job.Scheduler.ExitCode == nil || *job.Scheduler.ExitCode != 0 {
		t.Errorf("expected exit_code 0, got %v", job.Scheduler.ExitCode)
	}
}

func TestJobFinishedEvent_Failed(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	// Create a job first
	repo.jobs["test-job-fail"] = &domain.Job{
		ID:        "test-job-fail",
		User:      "testuser",
		Nodes:     []string{"node1"},
		State:     domain.JobStateRunning,
		StartTime: time.Now().Add(-1 * time.Minute),
		Scheduler: &domain.SchedulerInfo{
			Type: domain.SchedulerTypeSlurm,
		},
	}

	exitCode := 42
	event := types.JobFinishedEvent{
		JobId:      "test-job-fail",
		FinalState: types.Failed,
		ExitCode:   &exitCode,
		Timestamp:  time.Now(),
	}

	body, _ := json.Marshal(event)
	req := httptest.NewRequest(http.MethodPost, "/v1/events/job-finished", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.JobFinishedEvent(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	job, _ := repo.GetJob(context.Background(), "test-job-fail")
	if job.State != domain.JobStateFailed {
		t.Errorf("expected state 'failed', got '%s'", job.State)
	}
	if job.Scheduler.ExitCode == nil || *job.Scheduler.ExitCode != 42 {
		t.Errorf("expected exit_code 42, got %v", job.Scheduler.ExitCode)
	}
}

func TestJobFinishedEvent_Idempotent(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	// Create a job that's already completed
	repo.jobs["test-job-already-done"] = &domain.Job{
		ID:        "test-job-already-done",
		User:      "testuser",
		Nodes:     []string{"node1"},
		State:     domain.JobStateCompleted, // Already terminal
		StartTime: time.Now().Add(-1 * time.Minute),
	}

	exitCode := 0
	event := types.JobFinishedEvent{
		JobId:      "test-job-already-done",
		FinalState: types.Completed,
		ExitCode:   &exitCode,
		Timestamp:  time.Now(),
	}

	body, _ := json.Marshal(event)
	req := httptest.NewRequest(http.MethodPost, "/v1/events/job-finished", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.JobFinishedEvent(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp types.JobEventResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Status != types.Skipped {
		t.Errorf("expected status 'skipped' for already terminal job, got '%s'", resp.Status)
	}
}

func TestJobFinishedEvent_NotFound(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	exitCode := 0
	event := types.JobFinishedEvent{
		JobId:      "nonexistent-job",
		FinalState: types.Completed,
		ExitCode:   &exitCode,
		Timestamp:  time.Now(),
	}

	body, _ := json.Marshal(event)
	req := httptest.NewRequest(http.MethodPost, "/v1/events/job-finished", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.JobFinishedEvent(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestJobFinishedEvent_MissingJobId(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	event := types.JobFinishedEvent{
		FinalState: types.Completed,
		Timestamp:  time.Now(),
	}

	body, _ := json.Marshal(event)
	req := httptest.NewRequest(http.MethodPost, "/v1/events/job-finished", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.JobFinishedEvent(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

func TestJobFinishedEvent_InvalidJSON(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	req := httptest.NewRequest(http.MethodPost, "/v1/events/job-finished", bytes.NewReader([]byte("invalid")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.JobFinishedEvent(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

func TestJobFinishedEvent_CancelledState(t *testing.T) {
	repo := newMockRepository()
	srv := setupTestServer(repo)

	// Create a job first
	repo.jobs["test-job-cancel"] = &domain.Job{
		ID:        "test-job-cancel",
		User:      "testuser",
		Nodes:     []string{"node1"},
		State:     domain.JobStateRunning,
		StartTime: time.Now().Add(-1 * time.Minute),
		Scheduler: &domain.SchedulerInfo{
			Type: domain.SchedulerTypeSlurm,
		},
	}

	event := types.JobFinishedEvent{
		JobId:      "test-job-cancel",
		FinalState: types.Cancelled,
		Timestamp:  time.Now(),
	}

	body, _ := json.Marshal(event)
	req := httptest.NewRequest(http.MethodPost, "/v1/events/job-finished", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.JobFinishedEvent(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	job, _ := repo.GetJob(context.Background(), "test-job-cancel")
	if job.State != domain.JobStateCancelled {
		t.Errorf("expected state 'cancelled', got '%s'", job.State)
	}
}
