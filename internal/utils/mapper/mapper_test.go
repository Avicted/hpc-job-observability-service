package mapper

import (
	"testing"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/api/types"
	"github.com/Avicted/hpc-job-observability-service/internal/domain"
)

func TestNewMapper(t *testing.T) {
	m := NewMapper()
	if m == nil {
		t.Fatal("NewMapper() returned nil")
	}
}

func TestDomainJobToAPI_NilJob(t *testing.T) {
	m := NewMapper()
	result := m.DomainJobToAPI(nil)
	if result.Id != "" {
		t.Errorf("expected empty Id for nil job, got %q", result.Id)
	}
}

func TestDomainJobToAPI_FullJob(t *testing.T) {
	m := NewMapper()

	startTime := time.Now().Add(-time.Hour)
	endTime := time.Now()
	lastSample := time.Now().Add(-time.Minute)
	submitTime := time.Now().Add(-2 * time.Hour)
	priority := int64(100)
	exitCode := 0
	timeLimit := 60
	gpuUsage := 55.0

	job := &domain.Job{
		ID:             "job-123",
		User:           "testuser",
		Nodes:          []string{"node1", "node2"},
		State:          domain.JobStateCompleted,
		StartTime:      startTime,
		EndTime:        &endTime,
		RuntimeSeconds: 3600,
		CPUUsage:       75.5,
		MemoryUsageMB:  2048,
		GPUUsage:       &gpuUsage,
		LastSampleAt:   &lastSample,
		NodeCount:      2,
		RequestedCPUs:  8,
		AllocatedCPUs:  8,
		RequestedMemMB: 4096,
		AllocatedMemMB: 4096,
		RequestedGPUs:  2,
		AllocatedGPUs:  2,
		ClusterName:    "cluster1",
		SchedulerInst:  "slurm-01",
		IngestVersion:  "v1.0",
		SampleCount:    100,
		AvgCPUUsage:    70.0,
		MaxCPUUsage:    95.0,
		MaxMemUsageMB:  3500,
		AvgGPUUsage:    55.0,
		MaxGPUUsage:    80.0,
		Scheduler: &domain.SchedulerInfo{
			Type:          domain.SchedulerTypeSlurm,
			ExternalJobID: "slurm-456",
			RawState:      "COMPLETED",
			SubmitTime:    &submitTime,
			Partition:     "compute",
			Account:       "research",
			QoS:           "normal",
			Priority:      &priority,
			ExitCode:      &exitCode,
			StateReason:   "None",
			TimeLimitMins: &timeLimit,
			Extra:         map[string]interface{}{"key": "value"},
		},
	}

	result := m.DomainJobToAPI(job)

	if result.Id != "job-123" {
		t.Errorf("Id = %q, want job-123", result.Id)
	}
	if result.User != "testuser" {
		t.Errorf("User = %q, want testuser", result.User)
	}
	if len(result.Nodes) != 2 {
		t.Errorf("Nodes len = %d, want 2", len(result.Nodes))
	}
	if result.State != types.Completed {
		t.Errorf("State = %v, want completed", result.State)
	}
	if result.EndTime == nil {
		t.Error("EndTime should not be nil")
	}
	if result.RuntimeSeconds == nil || *result.RuntimeSeconds != 3600 {
		t.Errorf("RuntimeSeconds = %v, want 3600", result.RuntimeSeconds)
	}
	if result.CpuUsage == nil || *result.CpuUsage != 75.5 {
		t.Errorf("CpuUsage = %v, want 75.5", result.CpuUsage)
	}
	if result.MemoryUsageMb == nil || *result.MemoryUsageMb != 2048 {
		t.Errorf("MemoryUsageMb = %v, want 2048", result.MemoryUsageMb)
	}
	if result.GpuUsage == nil || *result.GpuUsage != 55.0 {
		t.Errorf("GpuUsage = %v, want 55.0", result.GpuUsage)
	}
	if result.NodeCount == nil || *result.NodeCount != 2 {
		t.Errorf("NodeCount = %v, want 2", result.NodeCount)
	}
	if result.RequestedCpus == nil || *result.RequestedCpus != 8 {
		t.Errorf("RequestedCpus = %v, want 8", result.RequestedCpus)
	}
	if result.AllocatedCpus == nil || *result.AllocatedCpus != 8 {
		t.Errorf("AllocatedCpus = %v, want 8", result.AllocatedCpus)
	}
	if result.ClusterName == nil || *result.ClusterName != "cluster1" {
		t.Errorf("ClusterName = %v, want cluster1", result.ClusterName)
	}
	if result.Scheduler == nil {
		t.Fatal("Scheduler should not be nil")
	}
	if result.Scheduler.Type == nil || *result.Scheduler.Type != types.Slurm {
		t.Errorf("Scheduler.Type = %v, want slurm", result.Scheduler.Type)
	}
	if result.Scheduler.Partition == nil || *result.Scheduler.Partition != "compute" {
		t.Errorf("Scheduler.Partition = %v, want compute", result.Scheduler.Partition)
	}
}

func TestDomainJobsToAPI(t *testing.T) {
	m := NewMapper()

	jobs := []*domain.Job{
		{ID: "job-1", User: "user1", State: domain.JobStateRunning},
		{ID: "job-2", User: "user2", State: domain.JobStateCompleted},
	}

	result := m.DomainJobsToAPI(jobs)

	if len(result) != 2 {
		t.Fatalf("len = %d, want 2", len(result))
	}
	if result[0].Id != "job-1" {
		t.Errorf("result[0].Id = %q, want job-1", result[0].Id)
	}
	if result[1].Id != "job-2" {
		t.Errorf("result[1].Id = %q, want job-2", result[1].Id)
	}
}

func TestDomainJobsToAPI_Empty(t *testing.T) {
	m := NewMapper()

	result := m.DomainJobsToAPI([]*domain.Job{})

	if len(result) != 0 {
		t.Errorf("len = %d, want 0", len(result))
	}
}

func TestDomainSchedulerToAPI_Nil(t *testing.T) {
	m := NewMapper()

	result := m.DomainSchedulerToAPI(nil)

	if result != nil {
		t.Error("expected nil for nil scheduler")
	}
}

func TestDomainSchedulerToAPI_Full(t *testing.T) {
	m := NewMapper()

	submitTime := time.Now()
	priority := int64(100)
	exitCode := 0
	timeLimit := 60

	sched := &domain.SchedulerInfo{
		Type:          domain.SchedulerTypeSlurm,
		ExternalJobID: "slurm-123",
		RawState:      "RUNNING",
		SubmitTime:    &submitTime,
		Partition:     "gpu",
		Account:       "research",
		QoS:           "high",
		Priority:      &priority,
		ExitCode:      &exitCode,
		StateReason:   "None",
		TimeLimitMins: &timeLimit,
		Extra:         map[string]interface{}{"key": "value"},
	}

	result := m.DomainSchedulerToAPI(sched)

	if result.Type == nil || *result.Type != types.Slurm {
		t.Errorf("Type = %v, want slurm", result.Type)
	}
	if result.ExternalJobId == nil || *result.ExternalJobId != "slurm-123" {
		t.Errorf("ExternalJobId = %v, want slurm-123", result.ExternalJobId)
	}
	if result.RawState == nil || *result.RawState != "RUNNING" {
		t.Errorf("RawState = %v, want RUNNING", result.RawState)
	}
	if result.Partition == nil || *result.Partition != "gpu" {
		t.Errorf("Partition = %v, want gpu", result.Partition)
	}
	if result.Account == nil || *result.Account != "research" {
		t.Errorf("Account = %v, want research", result.Account)
	}
	if result.Qos == nil || *result.Qos != "high" {
		t.Errorf("Qos = %v, want high", result.Qos)
	}
	if result.Priority == nil || *result.Priority != 100 {
		t.Errorf("Priority = %v, want 100", result.Priority)
	}
	if result.ExitCode == nil || *result.ExitCode != 0 {
		t.Errorf("ExitCode = %v, want 0", result.ExitCode)
	}
	if result.StateReason == nil || *result.StateReason != "None" {
		t.Errorf("StateReason = %v, want None", result.StateReason)
	}
	if result.TimeLimitMinutes == nil || *result.TimeLimitMinutes != 60 {
		t.Errorf("TimeLimitMinutes = %v, want 60", result.TimeLimitMinutes)
	}
	if result.Extra == nil {
		t.Error("Extra should not be nil")
	}
}

func TestAPISchedulerToDomain_Nil(t *testing.T) {
	m := NewMapper()

	result := m.APISchedulerToDomain(nil)

	if result != nil {
		t.Error("expected nil for nil scheduler")
	}
}

func TestAPISchedulerToDomain_Full(t *testing.T) {
	m := NewMapper()

	submitTime := time.Now()
	priority := int64(100)
	exitCode := 1
	timeLimit := 120
	schedType := types.Slurm
	extJobID := "slurm-789"
	rawState := "FAILED"
	partition := "compute"
	account := "project"
	qos := "normal"
	stateReason := "OOM"
	extra := map[string]interface{}{"signal": "SIGKILL"}

	sched := &types.SchedulerInfo{
		Type:             &schedType,
		ExternalJobId:    &extJobID,
		RawState:         &rawState,
		SubmitTime:       &submitTime,
		Partition:        &partition,
		Account:          &account,
		Qos:              &qos,
		Priority:         &priority,
		ExitCode:         &exitCode,
		StateReason:      &stateReason,
		TimeLimitMinutes: &timeLimit,
		Extra:            &extra,
	}

	result := m.APISchedulerToDomain(sched)

	if result.Type != domain.SchedulerTypeSlurm {
		t.Errorf("Type = %v, want slurm", result.Type)
	}
	if result.ExternalJobID != "slurm-789" {
		t.Errorf("ExternalJobID = %q, want slurm-789", result.ExternalJobID)
	}
	if result.RawState != "FAILED" {
		t.Errorf("RawState = %q, want FAILED", result.RawState)
	}
	if result.Partition != "compute" {
		t.Errorf("Partition = %q, want compute", result.Partition)
	}
	if result.Account != "project" {
		t.Errorf("Account = %q, want project", result.Account)
	}
	if result.QoS != "normal" {
		t.Errorf("QoS = %q, want normal", result.QoS)
	}
	if result.Priority == nil || *result.Priority != 100 {
		t.Errorf("Priority = %v, want 100", result.Priority)
	}
	if result.ExitCode == nil || *result.ExitCode != 1 {
		t.Errorf("ExitCode = %v, want 1", result.ExitCode)
	}
	if result.StateReason != "OOM" {
		t.Errorf("StateReason = %q, want OOM", result.StateReason)
	}
	if result.TimeLimitMins == nil || *result.TimeLimitMins != 120 {
		t.Errorf("TimeLimitMins = %v, want 120", result.TimeLimitMins)
	}
}

func TestAPISchedulerToDomain_EmptyFields(t *testing.T) {
	m := NewMapper()

	sched := &types.SchedulerInfo{}

	result := m.APISchedulerToDomain(sched)

	if result.Type != "" {
		t.Errorf("Type = %v, want empty", result.Type)
	}
	if result.ExternalJobID != "" {
		t.Errorf("ExternalJobID = %q, want empty", result.ExternalJobID)
	}
}

func TestDomainMetricSampleToAPI_Nil(t *testing.T) {
	m := NewMapper()

	result := m.DomainMetricSampleToAPI(nil)

	if !result.Timestamp.IsZero() {
		t.Error("expected zero timestamp for nil sample")
	}
}

func TestDomainMetricSampleToAPI_Full(t *testing.T) {
	m := NewMapper()

	now := time.Now()
	gpuUsage := 75.0
	sample := &domain.MetricSample{
		JobID:         "job-123",
		Timestamp:     now,
		CPUUsage:      85.5,
		MemoryUsageMB: 3072,
		GPUUsage:      &gpuUsage,
	}

	result := m.DomainMetricSampleToAPI(sample)

	if !result.Timestamp.Equal(now) {
		t.Errorf("Timestamp = %v, want %v", result.Timestamp, now)
	}
	if result.CpuUsage != 85.5 {
		t.Errorf("CpuUsage = %v, want 85.5", result.CpuUsage)
	}
	if result.MemoryUsageMb != 3072 {
		t.Errorf("MemoryUsageMb = %d, want 3072", result.MemoryUsageMb)
	}
	if result.GpuUsage == nil || *result.GpuUsage != 75.0 {
		t.Errorf("GpuUsage = %v, want 75.0", result.GpuUsage)
	}
}

func TestDomainMetricSamplesToAPI(t *testing.T) {
	m := NewMapper()

	now := time.Now()
	samples := []*domain.MetricSample{
		{JobID: "job-1", Timestamp: now, CPUUsage: 50.0, MemoryUsageMB: 1024},
		{JobID: "job-2", Timestamp: now, CPUUsage: 75.0, MemoryUsageMB: 2048},
	}

	result := m.DomainMetricSamplesToAPI(samples)

	if len(result) != 2 {
		t.Fatalf("len = %d, want 2", len(result))
	}
	if result[0].CpuUsage != 50.0 {
		t.Errorf("result[0].CpuUsage = %v, want 50.0", result[0].CpuUsage)
	}
	if result[1].CpuUsage != 75.0 {
		t.Errorf("result[1].CpuUsage = %v, want 75.0", result[1].CpuUsage)
	}
}

func TestDomainMetricSamplesToAPI_Empty(t *testing.T) {
	m := NewMapper()

	result := m.DomainMetricSamplesToAPI([]*domain.MetricSample{})

	if len(result) != 0 {
		t.Errorf("len = %d, want 0", len(result))
	}
}

func TestAPIJobStateToDomain(t *testing.T) {
	m := NewMapper()

	tests := []struct {
		apiState    types.JobState
		domainState domain.JobState
	}{
		{types.Pending, domain.JobStatePending},
		{types.Running, domain.JobStateRunning},
		{types.Completed, domain.JobStateCompleted},
		{types.Failed, domain.JobStateFailed},
		{types.Cancelled, domain.JobStateCancelled},
		{types.JobState("unknown"), domain.JobState("")},
	}

	for _, tt := range tests {
		t.Run(string(tt.apiState), func(t *testing.T) {
			result := m.APIJobStateToDomain(tt.apiState)
			if result != tt.domainState {
				t.Errorf("APIJobStateToDomain(%v) = %v, want %v", tt.apiState, result, tt.domainState)
			}
		})
	}
}

func TestIsValidJobState(t *testing.T) {
	m := NewMapper()

	validStates := []domain.JobState{
		domain.JobStatePending,
		domain.JobStateRunning,
		domain.JobStateCompleted,
		domain.JobStateFailed,
		domain.JobStateCancelled,
	}

	for _, state := range validStates {
		t.Run(string(state), func(t *testing.T) {
			if !m.IsValidJobState(state) {
				t.Errorf("IsValidJobState(%v) = false, want true", state)
			}
		})
	}

	invalidStates := []domain.JobState{
		domain.JobState("invalid"),
		domain.JobState(""),
		domain.JobState("RUNNING"), // case sensitive
	}

	for _, state := range invalidStates {
		t.Run(string(state), func(t *testing.T) {
			if m.IsValidJobState(state) {
				t.Errorf("IsValidJobState(%v) = true, want false", state)
			}
		})
	}
}
