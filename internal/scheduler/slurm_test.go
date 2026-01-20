package scheduler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewSlurmJobSource(t *testing.T) {
	// Test with default HTTP client
	config := SlurmConfig{
		BaseURL:    "http://localhost:6820",
		APIVersion: "v0.0.44",
		AuthToken:  "test-token",
	}
	source := NewSlurmJobSource(config)
	if source == nil {
		t.Fatal("NewSlurmJobSource returned nil")
	}
	if source.Type() != SchedulerTypeSlurm {
		t.Errorf("Type() = %v, want %v", source.Type(), SchedulerTypeSlurm)
	}
	if source.SupportsMetrics() {
		t.Error("SupportsMetrics() = true, want false")
	}

	// Test with custom HTTP client
	customClient := &http.Client{Timeout: 60 * time.Second}
	configWithClient := SlurmConfig{
		BaseURL:    "http://localhost:6820",
		APIVersion: "v0.0.44",
		HTTPClient: customClient,
	}
	sourceWithClient := NewSlurmJobSource(configWithClient)
	if sourceWithClient.httpClient != customClient {
		t.Error("Custom HTTP client not used")
	}
}

func TestSlurmJobSource_ListJobs(t *testing.T) {
	// Create mock SLURM server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("Expected GET, got %s", r.Method)
		}
		if r.Header.Get("X-SLURM-USER-TOKEN") != "test-token" {
			t.Errorf("Missing or incorrect auth token")
		}
		if r.Header.Get("Accept") != "application/json" {
			t.Error("Expected Accept: application/json")
		}

		response := slurmJobsResponse{
			Jobs: []slurmJob{
				{
					JobID:      12345,
					Name:       "test-job-1",
					UserName:   "alice",
					Nodes:      "node01,node02",
					JobState:   "RUNNING",
					Partition:  "compute",
					Account:    "research",
					QoS:        "normal",
					Priority:   1000,
					StartTime:  uint64(time.Now().Add(-1 * time.Hour).Unix()),
					SubmitTime: uint64(time.Now().Add(-2 * time.Hour).Unix()),
					Cpus:       32,
					Memory:     64000,
				},
				{
					JobID:      12346,
					Name:       "test-job-2",
					UserName:   "bob",
					Nodes:      "node03",
					JobState:   "PENDING",
					Partition:  "gpu",
					Account:    "ml",
					QoS:        "high",
					Priority:   2000,
					SubmitTime: uint64(time.Now().Add(-30 * time.Minute).Unix()),
					Cpus:       8,
					Memory:     32000,
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	config := SlurmConfig{
		BaseURL:    server.URL,
		APIVersion: "v0.0.44",
		AuthToken:  "test-token",
	}
	source := NewSlurmJobSource(config)

	ctx := context.Background()
	jobs, err := source.ListJobs(ctx, JobFilter{})
	if err != nil {
		t.Fatalf("ListJobs() error = %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("ListJobs() returned %d jobs, want 2", len(jobs))
	}

	// Verify first job
	job := jobs[0]
	if job.ID != "12345" {
		t.Errorf("Job ID = %s, want 12345", job.ID)
	}
	if job.User != "alice" {
		t.Errorf("User = %s, want alice", job.User)
	}
	if job.State != JobStateRunning {
		t.Errorf("State = %v, want %v", job.State, JobStateRunning)
	}
	if len(job.Nodes) != 2 {
		t.Errorf("Nodes count = %d, want 2", len(job.Nodes))
	}
	if job.Scheduler == nil {
		t.Fatal("Scheduler info is nil")
	}
	if job.Scheduler.Type != SchedulerTypeSlurm {
		t.Errorf("Scheduler.Type = %v, want %v", job.Scheduler.Type, SchedulerTypeSlurm)
	}
	if job.Scheduler.Partition != "compute" {
		t.Errorf("Scheduler.Partition = %s, want compute", job.Scheduler.Partition)
	}
	if job.Scheduler.Account != "research" {
		t.Errorf("Scheduler.Account = %s, want research", job.Scheduler.Account)
	}
	if job.Scheduler.Priority == nil || *job.Scheduler.Priority != 1000 {
		t.Error("Priority not set correctly")
	}
}

func TestSlurmJobSource_ListJobs_WithFilter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := slurmJobsResponse{
			Jobs: []slurmJob{
				{JobID: 1, UserName: "alice", JobState: "RUNNING", Partition: "compute"},
				{JobID: 2, UserName: "bob", JobState: "RUNNING", Partition: "gpu"},
				{JobID: 3, UserName: "alice", JobState: "PENDING", Partition: "compute"},
			},
		}
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	source := NewSlurmJobSource(SlurmConfig{
		BaseURL:    server.URL,
		APIVersion: "v0.0.44",
	})

	ctx := context.Background()

	// Test user filter
	user := "alice"
	jobs, _ := source.ListJobs(ctx, JobFilter{User: &user})
	if len(jobs) != 2 {
		t.Errorf("ListJobs(user=alice) returned %d jobs, want 2", len(jobs))
	}

	// Test state filter
	state := JobStateRunning
	jobs, _ = source.ListJobs(ctx, JobFilter{State: &state})
	if len(jobs) != 2 {
		t.Errorf("ListJobs(state=running) returned %d jobs, want 2", len(jobs))
	}

	// Test partition filter
	partition := "gpu"
	jobs, _ = source.ListJobs(ctx, JobFilter{Partition: &partition})
	if len(jobs) != 1 {
		t.Errorf("ListJobs(partition=gpu) returned %d jobs, want 1", len(jobs))
	}

	// Test combined filter
	jobs, _ = source.ListJobs(ctx, JobFilter{User: &user, State: &state})
	if len(jobs) != 1 {
		t.Errorf("ListJobs(user=alice, state=running) returned %d jobs, want 1", len(jobs))
	}
}

func TestSlurmJobSource_ListJobs_Pagination(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := slurmJobsResponse{
			Jobs: []slurmJob{
				{JobID: 1, UserName: "user", JobState: "RUNNING"},
				{JobID: 2, UserName: "user", JobState: "RUNNING"},
				{JobID: 3, UserName: "user", JobState: "RUNNING"},
				{JobID: 4, UserName: "user", JobState: "RUNNING"},
				{JobID: 5, UserName: "user", JobState: "RUNNING"},
			},
		}
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	source := NewSlurmJobSource(SlurmConfig{
		BaseURL:    server.URL,
		APIVersion: "v0.0.44",
	})

	ctx := context.Background()

	// Test limit
	jobs, _ := source.ListJobs(ctx, JobFilter{Limit: 2})
	if len(jobs) != 2 {
		t.Errorf("ListJobs(limit=2) returned %d jobs, want 2", len(jobs))
	}

	// Test offset
	jobs, _ = source.ListJobs(ctx, JobFilter{Offset: 2, Limit: 2})
	if len(jobs) != 2 {
		t.Errorf("ListJobs(offset=2, limit=2) returned %d jobs, want 2", len(jobs))
	}
	if jobs[0].ID != "3" {
		t.Errorf("First job ID = %s, want 3", jobs[0].ID)
	}

	// Test offset beyond available jobs
	jobs, _ = source.ListJobs(ctx, JobFilter{Offset: 10})
	if len(jobs) != 0 {
		t.Errorf("ListJobs(offset=10) returned %d jobs, want 0", len(jobs))
	}
}

func TestSlurmJobSource_ListJobs_Error(t *testing.T) {
	// Test API error response
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("Internal Server Error"))
	}))
	defer server.Close()

	source := NewSlurmJobSource(SlurmConfig{
		BaseURL:    server.URL,
		APIVersion: "v0.0.44",
	})

	ctx := context.Background()
	_, err := source.ListJobs(ctx, JobFilter{})
	if err == nil {
		t.Error("Expected error for 500 response")
	}
}

func TestSlurmJobSource_ListJobs_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("invalid json"))
	}))
	defer server.Close()

	source := NewSlurmJobSource(SlurmConfig{
		BaseURL:    server.URL,
		APIVersion: "v0.0.44",
	})

	ctx := context.Background()
	_, err := source.ListJobs(ctx, JobFilter{})
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}

func TestSlurmJobSource_ListJobs_ConnectionError(t *testing.T) {
	source := NewSlurmJobSource(SlurmConfig{
		BaseURL:    "http://localhost:99999", // Non-existent server
		APIVersion: "v0.0.44",
	})

	ctx := context.Background()
	_, err := source.ListJobs(ctx, JobFilter{})
	if err == nil {
		t.Error("Expected connection error")
	}
}

func TestSlurmJobSource_GetJob(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/slurm/v0.0.44/job/12345" {
			t.Errorf("Unexpected path: %s", r.URL.Path)
		}

		response := slurmJobsResponse{
			Jobs: []slurmJob{
				{
					JobID:       12345,
					Name:        "detailed-job",
					UserName:    "alice",
					Nodes:       "node[01-04]",
					JobState:    "COMPLETED",
					Partition:   "compute",
					Account:     "research",
					StartTime:   uint64(time.Now().Add(-1 * time.Hour).Unix()),
					EndTime:     uint64(time.Now().Add(-30 * time.Minute).Unix()),
					SubmitTime:  uint64(time.Now().Add(-2 * time.Hour).Unix()),
					ExitCode:    0,
					StateReason: "",
				},
			},
		}
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	source := NewSlurmJobSource(SlurmConfig{
		BaseURL:    server.URL,
		APIVersion: "v0.0.44",
	})

	ctx := context.Background()
	job, err := source.GetJob(ctx, "12345")
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}
	if job == nil {
		t.Fatal("GetJob() returned nil")
	}
	if job.ID != "12345" {
		t.Errorf("Job ID = %s, want 12345", job.ID)
	}
	if job.State != JobStateCompleted {
		t.Errorf("State = %v, want %v", job.State, JobStateCompleted)
	}
	if job.EndTime == nil {
		t.Error("EndTime should be set for completed job")
	}
	if job.RuntimeSeconds <= 0 {
		t.Error("RuntimeSeconds should be positive for completed job")
	}
	// Note: node[01-04] is returned as-is since expansion is not implemented
	if len(job.Nodes) != 1 {
		t.Errorf("Nodes count = %d, want 1 (unexpanded)", len(job.Nodes))
	}
}

func TestSlurmJobSource_GetJob_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	source := NewSlurmJobSource(SlurmConfig{
		BaseURL:    server.URL,
		APIVersion: "v0.0.44",
	})

	ctx := context.Background()
	job, err := source.GetJob(ctx, "99999")
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}
	if job != nil {
		t.Error("GetJob() should return nil for not found")
	}
}

func TestSlurmJobSource_GetJob_EmptyResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := slurmJobsResponse{Jobs: []slurmJob{}}
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	source := NewSlurmJobSource(SlurmConfig{
		BaseURL:    server.URL,
		APIVersion: "v0.0.44",
	})

	ctx := context.Background()
	job, err := source.GetJob(ctx, "12345")
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}
	if job != nil {
		t.Error("GetJob() should return nil for empty response")
	}
}

func TestSlurmJobSource_GetJob_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("Server Error"))
	}))
	defer server.Close()

	source := NewSlurmJobSource(SlurmConfig{
		BaseURL:    server.URL,
		APIVersion: "v0.0.44",
	})

	ctx := context.Background()
	_, err := source.GetJob(ctx, "12345")
	if err == nil {
		t.Error("Expected error for 500 response")
	}
}

func TestSlurmJobSource_GetJobMetrics(t *testing.T) {
	source := NewSlurmJobSource(SlurmConfig{
		BaseURL:    "http://localhost:6820",
		APIVersion: "v0.0.44",
	})

	ctx := context.Background()
	metrics, err := source.GetJobMetrics(ctx, "12345")
	if err != nil {
		t.Errorf("GetJobMetrics() error = %v", err)
	}
	if metrics != nil {
		t.Error("GetJobMetrics() should return nil (not supported)")
	}
}

func TestSlurmJobSource_ConvertSlurmJob(t *testing.T) {
	source := NewSlurmJobSource(SlurmConfig{
		BaseURL:    "http://localhost:6820",
		APIVersion: "v0.0.44",
	})

	startTime := time.Now().Add(-1 * time.Hour)
	endTime := time.Now().Add(-30 * time.Minute)
	submitTime := time.Now().Add(-2 * time.Hour)

	tests := []struct {
		name      string
		slurmJob  slurmJob
		checkFunc func(t *testing.T, job *Job)
	}{
		{
			name: "running job without end time",
			slurmJob: slurmJob{
				JobID:     1,
				Name:      "running-job",
				UserName:  "user1",
				JobState:  "RUNNING",
				StartTime: uint64(startTime.Unix()),
			},
			checkFunc: func(t *testing.T, job *Job) {
				if job.State != JobStateRunning {
					t.Errorf("State = %v, want running", job.State)
				}
				if job.EndTime != nil {
					t.Error("EndTime should be nil for running job")
				}
				if job.RuntimeSeconds <= 0 {
					t.Error("RuntimeSeconds should be positive for running job")
				}
			},
		},
		{
			name: "completed job with all fields",
			slurmJob: slurmJob{
				JobID:        2,
				Name:         "completed-job",
				UserName:     "user2",
				JobState:     "COMPLETED",
				Partition:    "gpu",
				Account:      "ml",
				QoS:          "high",
				Priority:     5000,
				StartTime:    uint64(startTime.Unix()),
				EndTime:      uint64(endTime.Unix()),
				SubmitTime:   uint64(submitTime.Unix()),
				ExitCode:     0,
				Cpus:         16,
				Memory:       32000,
				TresAllocStr: "cpu=16,mem=32G",
				TresReqStr:   "cpu=16,mem=32G",
				StateReason:  "",
			},
			checkFunc: func(t *testing.T, job *Job) {
				if job.State != JobStateCompleted {
					t.Errorf("State = %v, want completed", job.State)
				}
				if job.EndTime == nil {
					t.Error("EndTime should be set for completed job")
				}
				if job.Scheduler == nil {
					t.Fatal("Scheduler info is nil")
				}
				if job.Scheduler.QoS != "high" {
					t.Errorf("QoS = %s, want high", job.Scheduler.QoS)
				}
				if job.Scheduler.SubmitTime == nil {
					t.Error("SubmitTime should be set")
				}
				if job.Scheduler.ExitCode != nil {
					t.Error("ExitCode should be nil when 0")
				}
				extra := job.Scheduler.Extra
				if extra == nil {
					t.Fatal("Extra is nil")
				}
				if extra["cpus"] != uint32(16) {
					t.Errorf("Extra[cpus] = %v, want 16", extra["cpus"])
				}
			},
		},
		{
			name: "failed job with exit code",
			slurmJob: slurmJob{
				JobID:       3,
				UserName:    "user3",
				JobState:    "FAILED",
				ExitCode:    1,
				StateReason: "NonZeroExitCode",
			},
			checkFunc: func(t *testing.T, job *Job) {
				if job.State != JobStateFailed {
					t.Errorf("State = %v, want failed", job.State)
				}
				if job.Scheduler.ExitCode == nil || *job.Scheduler.ExitCode != 1 {
					t.Error("ExitCode should be 1")
				}
			},
		},
		{
			name: "pending job",
			slurmJob: slurmJob{
				JobID:       4,
				UserName:    "user4",
				JobState:    "PENDING",
				Priority:    0,
				StateReason: "Priority",
			},
			checkFunc: func(t *testing.T, job *Job) {
				if job.State != JobStatePending {
					t.Errorf("State = %v, want pending", job.State)
				}
				// Priority 0 should not be set
				if job.Scheduler.Priority != nil {
					t.Error("Priority should be nil when 0")
				}
			},
		},
		{
			name: "cancelled job",
			slurmJob: slurmJob{
				JobID:    5,
				UserName: "user5",
				JobState: "CANCELLED",
			},
			checkFunc: func(t *testing.T, job *Job) {
				if job.State != JobStateCancelled {
					t.Errorf("State = %v, want cancelled", job.State)
				}
			},
		},
		{
			name: "job with zero end time",
			slurmJob: slurmJob{
				JobID:     6,
				UserName:  "user6",
				JobState:  "RUNNING",
				StartTime: uint64(startTime.Unix()),
				EndTime:   0,
			},
			checkFunc: func(t *testing.T, job *Job) {
				if job.EndTime != nil {
					t.Error("EndTime should be nil when SLURM sends 0")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := source.convertSlurmJob(&tt.slurmJob)
			if job == nil {
				t.Fatal("convertSlurmJob returned nil")
			}
			tt.checkFunc(t, job)
		})
	}
}

func TestSlurmJobSource_ParseNodes(t *testing.T) {
	source := NewSlurmJobSource(SlurmConfig{
		BaseURL:    "http://localhost:6820",
		APIVersion: "v0.0.44",
	})

	tests := []struct {
		input    string
		expected []string
	}{
		{"", nil},
		{"node01", []string{"node01"}},
		{"node01,node02,node03", []string{"node01", "node02", "node03"}},
		{"node[01-04]", []string{"node[01-04]"}}, // Unexpanded for now
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := source.parseNodes(tt.input)
			if tt.expected == nil && result != nil {
				t.Errorf("parseNodes(%q) = %v, want nil", tt.input, result)
				return
			}
			if len(result) != len(tt.expected) {
				t.Errorf("parseNodes(%q) = %v, want %v", tt.input, result, tt.expected)
				return
			}
			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("parseNodes(%q)[%d] = %s, want %s", tt.input, i, result[i], tt.expected[i])
				}
			}
		})
	}
}

func TestSlurmJobSource_MatchesFilter(t *testing.T) {
	source := NewSlurmJobSource(SlurmConfig{
		BaseURL:    "http://localhost:6820",
		APIVersion: "v0.0.44",
	})

	job := &Job{
		ID:    "1",
		User:  "alice",
		State: JobStateRunning,
		Scheduler: &SchedulerInfo{
			Partition: "compute",
		},
	}

	tests := []struct {
		name    string
		filter  JobFilter
		matches bool
	}{
		{"empty filter", JobFilter{}, true},
		{"matching user", JobFilter{User: stringPtr("alice")}, true},
		{"non-matching user", JobFilter{User: stringPtr("bob")}, false},
		{"matching state", JobFilter{State: jobStatePtr(JobStateRunning)}, true},
		{"non-matching state", JobFilter{State: jobStatePtr(JobStatePending)}, false},
		{"matching partition", JobFilter{Partition: stringPtr("compute")}, true},
		{"non-matching partition", JobFilter{Partition: stringPtr("gpu")}, false},
		{"combined matching", JobFilter{User: stringPtr("alice"), State: jobStatePtr(JobStateRunning)}, true},
		{"combined non-matching", JobFilter{User: stringPtr("alice"), State: jobStatePtr(JobStatePending)}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := source.matchesFilter(job, tt.filter); got != tt.matches {
				t.Errorf("matchesFilter() = %v, want %v", got, tt.matches)
			}
		})
	}

	// Test job without scheduler info
	jobNoScheduler := &Job{
		ID:        "2",
		User:      "bob",
		State:     JobStateRunning,
		Scheduler: nil,
	}
	partFilter := JobFilter{Partition: stringPtr("compute")}
	if source.matchesFilter(jobNoScheduler, partFilter) {
		t.Error("matchesFilter() should return false for job without scheduler when filtering by partition")
	}
}

func TestSlurmJobSource_BuildURL(t *testing.T) {
	source := NewSlurmJobSource(SlurmConfig{
		BaseURL:    "http://localhost:6820",
		APIVersion: "v0.0.44",
	})

	url := source.buildURL("/slurm/%s/jobs/", "v0.0.44")
	expected := "http://localhost:6820/slurm/v0.0.44/jobs/"
	if url != expected {
		t.Errorf("buildURL() = %s, want %s", url, expected)
	}

	url = source.buildURL("/slurm/%s/job/%s", "v0.0.44", "12345")
	expected = "http://localhost:6820/slurm/v0.0.44/job/12345"
	if url != expected {
		t.Errorf("buildURL() = %s, want %s", url, expected)
	}
}

func TestSlurmJobSource_DoRequest_WithoutAuth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-SLURM-USER-TOKEN") != "" {
			t.Error("Auth token should not be set")
		}
		_ = json.NewEncoder(w).Encode(slurmJobsResponse{Jobs: []slurmJob{}})
	}))
	defer server.Close()

	source := NewSlurmJobSource(SlurmConfig{
		BaseURL:    server.URL,
		APIVersion: "v0.0.44",
		AuthToken:  "", // No auth
	})

	ctx := context.Background()
	_, err := source.ListJobs(ctx, JobFilter{})
	if err != nil {
		t.Errorf("ListJobs() error = %v", err)
	}
}

func TestSlurmJobSource_DoRequest_WithBody(t *testing.T) {
	// While SLURM REST API typically uses GET, test the body marshaling path
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	source := NewSlurmJobSource(SlurmConfig{
		BaseURL:    server.URL,
		APIVersion: "v0.0.44",
	})

	ctx := context.Background()
	// Call doRequest directly with a body (even though our API doesn't use POST)
	resp, err := source.doRequest(ctx, "POST", server.URL+"/test", map[string]string{"key": "value"})
	if err != nil {
		t.Errorf("doRequest() error = %v", err)
	}
	resp.Body.Close()
}

// Helper functions for test pointers
func stringPtr(s string) *string {
	return &s
}

func jobStatePtr(s JobState) *JobState {
	return &s
}
