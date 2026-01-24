package scheduler

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/slurmclient"
)

// mockSlurmClient implements SlurmClient for testing.
type mockSlurmClient struct {
	jobsResponse  *slurmclient.SlurmctldGetJobsResponse
	jobResponse   *slurmclient.SlurmctldGetJobResponse
	nodesResponse *slurmclient.SlurmctldGetNodesResponse
	pingResponse  *slurmclient.SlurmctldPingResponse
	err           error
}

func (m *mockSlurmClient) SlurmctldGetJobsWithResponse(ctx context.Context, params *slurmclient.SlurmctldGetJobsParams, reqEditors ...slurmclient.RequestEditorFn) (*slurmclient.SlurmctldGetJobsResponse, error) {
	return m.jobsResponse, m.err
}

func (m *mockSlurmClient) SlurmctldGetJobWithResponse(ctx context.Context, jobId string, reqEditors ...slurmclient.RequestEditorFn) (*slurmclient.SlurmctldGetJobResponse, error) {
	return m.jobResponse, m.err
}

func (m *mockSlurmClient) SlurmctldGetNodesWithResponse(ctx context.Context, params *slurmclient.SlurmctldGetNodesParams, reqEditors ...slurmclient.RequestEditorFn) (*slurmclient.SlurmctldGetNodesResponse, error) {
	return m.nodesResponse, m.err
}

func (m *mockSlurmClient) SlurmctldPingWithResponse(ctx context.Context, reqEditors ...slurmclient.RequestEditorFn) (*slurmclient.SlurmctldPingResponse, error) {
	return m.pingResponse, m.err
}

// Helper functions strPtr, intPtr, int64Ptr are defined in mock.go

func TestSlurmJobSource_Type(t *testing.T) {
	source := NewSlurmJobSourceWithClient(SlurmConfig{}, &mockSlurmClient{})
	if source.Type() != SchedulerTypeSlurm {
		t.Errorf("Type() = %v, want %v", source.Type(), SchedulerTypeSlurm)
	}
}

func TestSlurmJobSource_SupportsMetrics(t *testing.T) {
	source := NewSlurmJobSourceWithClient(SlurmConfig{}, &mockSlurmClient{})
	if source.SupportsMetrics() {
		t.Error("SupportsMetrics() = true, want false")
	}
}

func TestSlurmJobSource_ListJobs(t *testing.T) {
	startTime := int64Ptr(1737478800) // Unix timestamp
	submitTime := int64Ptr(1737475200)

	jobs := []slurmclient.V0037JobResponseProperties{
		{
			JobId:     intPtr(12345),
			UserName:  strPtr("alice"),
			Nodes:     strPtr("node01,node02"),
			JobState:  strPtr("RUNNING"),
			Partition: strPtr("compute"),
			Account:   strPtr("research"),
			Qos:       strPtr("normal"),
			Priority:  intPtr(1000),
			StartTime: startTime,
			Cpus:      intPtr(32),
		},
		{
			JobId:      intPtr(12346),
			UserName:   strPtr("bob"),
			Nodes:      strPtr("node03"),
			JobState:   strPtr("PENDING"),
			Partition:  strPtr("gpu"),
			Account:    strPtr("ml"),
			Qos:        strPtr("high"),
			Priority:   intPtr(2000),
			SubmitTime: submitTime,
			Cpus:       intPtr(8),
		},
	}

	client := &mockSlurmClient{
		jobsResponse: &slurmclient.SlurmctldGetJobsResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200: &slurmclient.V0037JobsResponse{
				Jobs: &jobs,
			},
		},
	}

	source := NewSlurmJobSourceWithClient(SlurmConfig{}, client)

	ctx := context.Background()
	result, err := source.ListJobs(ctx, JobFilter{})
	if err != nil {
		t.Fatalf("ListJobs() error = %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("ListJobs() returned %d jobs, want 2", len(result))
	}

	// Verify first job
	job := result[0]
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
	if job.AllocatedCPUs != 32 {
		t.Errorf("AllocatedCPUs = %d, want 32", job.AllocatedCPUs)
	}
}

func TestSlurmJobSource_ListJobs_WithFilter(t *testing.T) {
	jobs := []slurmclient.V0037JobResponseProperties{
		{JobId: intPtr(1), UserName: strPtr("alice"), JobState: strPtr("RUNNING"), Partition: strPtr("compute")},
		{JobId: intPtr(2), UserName: strPtr("bob"), JobState: strPtr("RUNNING"), Partition: strPtr("gpu")},
		{JobId: intPtr(3), UserName: strPtr("alice"), JobState: strPtr("PENDING"), Partition: strPtr("compute")},
	}

	client := &mockSlurmClient{
		jobsResponse: &slurmclient.SlurmctldGetJobsResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &slurmclient.V0037JobsResponse{Jobs: &jobs},
		},
	}

	source := NewSlurmJobSourceWithClient(SlurmConfig{}, client)
	ctx := context.Background()

	// Test user filter
	user := "alice"
	result, _ := source.ListJobs(ctx, JobFilter{User: &user})
	if len(result) != 2 {
		t.Errorf("ListJobs(user=alice) returned %d jobs, want 2", len(result))
	}

	// Test state filter
	state := JobStateRunning
	result, _ = source.ListJobs(ctx, JobFilter{State: &state})
	if len(result) != 2 {
		t.Errorf("ListJobs(state=running) returned %d jobs, want 2", len(result))
	}

	// Test partition filter
	partition := "gpu"
	result, _ = source.ListJobs(ctx, JobFilter{Partition: &partition})
	if len(result) != 1 {
		t.Errorf("ListJobs(partition=gpu) returned %d jobs, want 1", len(result))
	}
}

func TestSlurmJobSource_ListJobs_Pagination(t *testing.T) {
	jobs := []slurmclient.V0037JobResponseProperties{
		{JobId: intPtr(1), UserName: strPtr("user"), JobState: strPtr("RUNNING")},
		{JobId: intPtr(2), UserName: strPtr("user"), JobState: strPtr("RUNNING")},
		{JobId: intPtr(3), UserName: strPtr("user"), JobState: strPtr("RUNNING")},
		{JobId: intPtr(4), UserName: strPtr("user"), JobState: strPtr("RUNNING")},
		{JobId: intPtr(5), UserName: strPtr("user"), JobState: strPtr("RUNNING")},
	}

	client := &mockSlurmClient{
		jobsResponse: &slurmclient.SlurmctldGetJobsResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &slurmclient.V0037JobsResponse{Jobs: &jobs},
		},
	}

	source := NewSlurmJobSourceWithClient(SlurmConfig{}, client)
	ctx := context.Background()

	// Test limit
	result, _ := source.ListJobs(ctx, JobFilter{Limit: 2})
	if len(result) != 2 {
		t.Errorf("ListJobs(limit=2) returned %d jobs, want 2", len(result))
	}

	// Test offset
	result, _ = source.ListJobs(ctx, JobFilter{Offset: 2, Limit: 2})
	if len(result) != 2 {
		t.Errorf("ListJobs(offset=2, limit=2) returned %d jobs, want 2", len(result))
	}
	if result[0].ID != "3" {
		t.Errorf("First job ID = %s, want 3", result[0].ID)
	}

	// Test offset beyond available jobs
	result, _ = source.ListJobs(ctx, JobFilter{Offset: 10})
	if len(result) != 0 {
		t.Errorf("ListJobs(offset=10) returned %d jobs, want 0", len(result))
	}
}

func TestSlurmJobSource_ListJobs_Error(t *testing.T) {
	client := &mockSlurmClient{
		jobsResponse: &slurmclient.SlurmctldGetJobsResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusInternalServerError},
			Body:         []byte("Internal Server Error"),
		},
	}

	source := NewSlurmJobSourceWithClient(SlurmConfig{}, client)

	ctx := context.Background()
	_, err := source.ListJobs(ctx, JobFilter{})
	if err == nil {
		t.Error("Expected error for 500 response")
	}
}

func TestSlurmJobSource_GetJob(t *testing.T) {
	jobs := []slurmclient.V0037JobResponseProperties{
		{
			JobId:     intPtr(12345),
			UserName:  strPtr("alice"),
			JobState:  strPtr("RUNNING"),
			Partition: strPtr("compute"),
		},
	}

	client := &mockSlurmClient{
		jobResponse: &slurmclient.SlurmctldGetJobResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &slurmclient.V0037JobsResponse{Jobs: &jobs},
		},
	}

	source := NewSlurmJobSourceWithClient(SlurmConfig{}, client)

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
}

func TestSlurmJobSource_GetJob_NotFound(t *testing.T) {
	client := &mockSlurmClient{
		jobResponse: &slurmclient.SlurmctldGetJobResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusNotFound},
		},
	}

	source := NewSlurmJobSourceWithClient(SlurmConfig{}, client)

	ctx := context.Background()
	job, err := source.GetJob(ctx, "99999")
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}
	if job != nil {
		t.Error("GetJob() should return nil for not found")
	}
}

func TestSlurmJobSource_GetJob_InvalidID(t *testing.T) {
	// With v0.0.37, job IDs are strings passed directly to the API
	// The API will return 404 for non-existent jobs
	client := &mockSlurmClient{
		jobResponse: &slurmclient.SlurmctldGetJobResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusNotFound},
		},
	}
	source := NewSlurmJobSourceWithClient(SlurmConfig{}, client)

	ctx := context.Background()
	job, err := source.GetJob(ctx, "not-a-number")
	// Should not error, just return nil for not found
	if err != nil {
		t.Errorf("GetJob() error = %v, want nil", err)
	}
	if job != nil {
		t.Error("GetJob() should return nil for not found job")
	}
}

func TestSlurmJobSource_ListNodes(t *testing.T) {
	cpuLoad := int64(240) // 2.40 load
	nodes := []slurmclient.V0037Node{
		{
			Name:       strPtr("node01"),
			Hostname:   strPtr("node01.cluster"),
			State:      strPtr("idle"),
			Cpus:       intPtr(64),
			Cores:      intPtr(32),
			Sockets:    intPtr(2),
			RealMemory: intPtr(256000),
			FreeMemory: intPtr(240000),
			CpuLoad:    &cpuLoad,
			Features:   strPtr("avx2,gpu"),
		},
		{
			Name:     strPtr("node02"),
			Hostname: strPtr("node02.cluster"),
			State:    strPtr("allocated"),
			Cpus:     intPtr(64),
		},
	}

	client := &mockSlurmClient{
		nodesResponse: &slurmclient.SlurmctldGetNodesResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &slurmclient.V0037NodesResponse{Nodes: &nodes},
		},
	}

	source := NewSlurmJobSourceWithClient(SlurmConfig{}, client)

	ctx := context.Background()
	result, err := source.ListNodes(ctx)
	if err != nil {
		t.Fatalf("ListNodes() error = %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("ListNodes() returned %d nodes, want 2", len(result))
	}

	// Verify first node
	node := result[0]
	if node.Name != "node01" {
		t.Errorf("Node name = %s, want node01", node.Name)
	}
	if node.State != NodeStateIdle {
		t.Errorf("Node state = %v, want %v", node.State, NodeStateIdle)
	}
	if node.CPUs != 64 {
		t.Errorf("CPUs = %d, want 64", node.CPUs)
	}
	if node.CPULoad != 2.40 {
		t.Errorf("CPULoad = %f, want 2.40", node.CPULoad)
	}
	if len(node.Features) != 2 {
		t.Errorf("Features count = %d, want 2", len(node.Features))
	}
}

func TestSlurmJobSource_MapNodeState(t *testing.T) {
	source := NewSlurmJobSourceWithClient(SlurmConfig{}, &mockSlurmClient{})

	tests := []struct {
		input string
		want  NodeState
	}{
		{"idle", NodeStateIdle},
		{"IDLE", NodeStateIdle},
		{"idle+cloud", NodeStateIdle},
		{"alloc", NodeStateAllocated},
		{"allocated", NodeStateAllocated},
		{"mix", NodeStateMixed},
		{"mixed", NodeStateMixed},
		{"drain", NodeStateDrained},
		{"drained", NodeStateDrained},
		{"draining", NodeStateDrained},
		{"down", NodeStateDown},
		{"down*", NodeStateDown},
		{"unknown", NodeStateUnknown},
		{"", NodeStateUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := source.mapNodeState(tt.input)
			if got != tt.want {
				t.Errorf("mapNodeState(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseNodeList(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"", 0},
		{"node01", 1},
		{"node01,node02", 2},
		{"node01,node02,node03", 3},
		{"node[01-04]", 1}, // Not expanded yet
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseNodeList(tt.input)
			if len(got) != tt.want {
				t.Errorf("parseNodeList(%q) returned %d items, want %d", tt.input, len(got), tt.want)
			}
		})
	}
}

func TestParseTRESInt(t *testing.T) {
	tests := []struct {
		tres string
		key  string
		want int64
	}{
		{"cpu=4,mem=8192M,node=1", "cpu", 4},
		{"cpu=4,mem=8192M,node=1", "node", 1},
		{"cpu=4,mem=8192M,node=1", "gpu", 0},
		{"", "cpu", 0},
		{"gres/gpu=2", "gres/gpu", 2},
	}

	for _, tt := range tests {
		t.Run(tt.tres+"_"+tt.key, func(t *testing.T) {
			got := parseTRESInt(tt.tres, tt.key)
			if got != tt.want {
				t.Errorf("parseTRESInt(%q, %q) = %d, want %d", tt.tres, tt.key, got, tt.want)
			}
		})
	}
}

func TestParseTRESMemory(t *testing.T) {
	tests := []struct {
		tres string
		want int64
	}{
		{"cpu=4,mem=8192M,node=1", 8192},
		{"cpu=4,mem=8G,node=1", 8192},
		{"cpu=4,mem=1024K,node=1", 1},
		{"cpu=4,node=1", 0},
		{"", 0},
	}

	for _, tt := range tests {
		t.Run(tt.tres, func(t *testing.T) {
			got := parseTRESMemory(tt.tres)
			if got != tt.want {
				t.Errorf("parseTRESMemory(%q) = %d, want %d", tt.tres, got, tt.want)
			}
		})
	}
}

func TestParseMemoryString(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"8192M", 8192},
		{"8G", 8192},
		{"1T", 1048576},
		{"1024K", 1},
		{"8388608", 8}, // Assumed bytes, becomes 8MB
		{"", 0},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseMemoryString(tt.input)
			if got != tt.want {
				t.Errorf("parseMemoryString(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestSlurmJobStateMapping(t *testing.T) {
	mapper := NewSlurmStateMapping()

	tests := []struct {
		input string
		want  JobState
	}{
		{"PENDING", JobStatePending},
		{"RUNNING", JobStateRunning},
		{"COMPLETED", JobStateCompleted},
		{"FAILED", JobStateFailed},
		{"CANCELLED", JobStateCancelled},
		{"TIMEOUT", JobStateFailed},
		{"NODE_FAIL", JobStateFailed},
		{"PREEMPTED", JobStateCancelled},
		{"SUSPENDED", JobStatePending},
		{"UNKNOWN", JobStatePending}, // Default
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := mapper.NormalizeState(tt.input)
			if got != tt.want {
				t.Errorf("NormalizeState(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestNewSlurmJobSource(t *testing.T) {
	config := SlurmConfig{
		BaseURL:    "http://localhost:6820",
		APIVersion: "v0.0.36",
		AuthToken:  "test-token",
	}

	source, err := NewSlurmJobSource(config)
	if err != nil {
		t.Fatalf("NewSlurmJobSource() error = %v", err)
	}
	if source == nil {
		t.Fatal("NewSlurmJobSource() returned nil")
	}
	if source.Type() != SchedulerTypeSlurm {
		t.Errorf("Type() = %v, want %v", source.Type(), SchedulerTypeSlurm)
	}
}

func TestNewSlurmJobSource_CustomHTTPClient(t *testing.T) {
	customClient := &http.Client{Timeout: 60 * time.Second}
	config := SlurmConfig{
		BaseURL:    "http://localhost:6820",
		APIVersion: "v0.0.36",
		HTTPClient: customClient,
	}

	source, err := NewSlurmJobSource(config)
	if err != nil {
		t.Fatalf("NewSlurmJobSource() error = %v", err)
	}
	if source == nil {
		t.Fatal("NewSlurmJobSource() returned nil")
	}
}

func TestSlurmJobSource_Ping(t *testing.T) {
	client := &mockSlurmClient{
		pingResponse: &slurmclient.SlurmctldPingResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		},
	}

	source := NewSlurmJobSourceWithClient(SlurmConfig{}, client)

	ctx := context.Background()
	err := source.Ping(ctx)
	if err != nil {
		t.Errorf("Ping() error = %v", err)
	}
}

func TestSlurmJobSource_Ping_Error(t *testing.T) {
	client := &mockSlurmClient{
		pingResponse: &slurmclient.SlurmctldPingResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusServiceUnavailable},
		},
	}

	source := NewSlurmJobSourceWithClient(SlurmConfig{}, client)

	ctx := context.Background()
	err := source.Ping(ctx)
	if err == nil {
		t.Error("Expected error for unavailable service")
	}
}
