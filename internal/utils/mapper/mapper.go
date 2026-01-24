// Package mapper provides conversion functions between domain models and API types.
// All conversions between internal domain representations and external API types
// should go through this package to maintain separation of concerns.
//
// The mapper layer sits between API handlers and services, converting:
//   - API request types → domain entities (for input)
//   - Domain entities → API response types (for output)
package mapper

import (
	"github.com/Avicted/hpc-job-observability-service/internal/api/types"
	"github.com/Avicted/hpc-job-observability-service/internal/domain"
)

// Mapper handles conversions between domain and API types.
type Mapper struct{}

// NewMapper creates a new Mapper instance.
func NewMapper() *Mapper {
	return &Mapper{}
}

// DomainJobToAPI converts a domain.Job to a types.Job API response.
// This is used when returning job data to API clients.
func (m *Mapper) DomainJobToAPI(job *domain.Job) types.Job {
	if job == nil {
		return types.Job{}
	}

	cpuUsage := job.CPUUsage
	memUsage := int(job.MemoryUsageMB)

	resp := types.Job{
		Id:            job.ID,
		User:          job.User,
		Nodes:         job.Nodes,
		State:         types.JobState(job.State),
		StartTime:     job.StartTime,
		CpuUsage:      &cpuUsage,
		MemoryUsageMb: &memUsage,
		GpuUsage:      job.GPUUsage,
	}

	if job.EndTime != nil {
		resp.EndTime = job.EndTime
	}

	if job.RuntimeSeconds > 0 {
		resp.RuntimeSeconds = &job.RuntimeSeconds
	}
	if job.LastSampleAt != nil {
		resp.LastSampleAt = job.LastSampleAt
	}
	if job.NodeCount > 0 {
		nodeCount := job.NodeCount
		resp.NodeCount = &nodeCount
	}
	if job.RequestedCPUs > 0 {
		v := int(job.RequestedCPUs)
		resp.RequestedCpus = &v
	}
	if job.AllocatedCPUs > 0 {
		v := int(job.AllocatedCPUs)
		resp.AllocatedCpus = &v
	}
	if job.RequestedMemMB > 0 {
		v := int(job.RequestedMemMB)
		resp.RequestedMemoryMb = &v
	}
	if job.AllocatedMemMB > 0 {
		v := int(job.AllocatedMemMB)
		resp.AllocatedMemoryMb = &v
	}
	if job.RequestedGPUs > 0 {
		v := int(job.RequestedGPUs)
		resp.RequestedGpus = &v
	}
	if job.AllocatedGPUs > 0 {
		v := int(job.AllocatedGPUs)
		resp.AllocatedGpus = &v
	}
	if job.ClusterName != "" {
		v := job.ClusterName
		resp.ClusterName = &v
	}
	if job.SchedulerInst != "" {
		v := job.SchedulerInst
		resp.SchedulerInstance = &v
	}
	if job.IngestVersion != "" {
		v := job.IngestVersion
		resp.IngestVersion = &v
	}
	if job.SampleCount > 0 {
		v := int(job.SampleCount)
		resp.SampleCount = &v
	}
	if job.AvgCPUUsage > 0 {
		v := job.AvgCPUUsage
		resp.AvgCpuUsage = &v
	}
	if job.MaxCPUUsage > 0 {
		v := job.MaxCPUUsage
		resp.MaxCpuUsage = &v
	}
	if job.MaxMemUsageMB > 0 {
		v := int(job.MaxMemUsageMB)
		resp.MaxMemoryUsageMb = &v
	}
	if job.AvgGPUUsage > 0 {
		v := job.AvgGPUUsage
		resp.AvgGpuUsage = &v
	}
	if job.MaxGPUUsage > 0 {
		v := job.MaxGPUUsage
		resp.MaxGpuUsage = &v
	}

	// Convert scheduler info if present
	if job.Scheduler != nil {
		resp.Scheduler = m.DomainSchedulerToAPI(job.Scheduler)
	}

	return resp
}

// DomainJobsToAPI converts a slice of domain.Job to a slice of types.Job.
func (m *Mapper) DomainJobsToAPI(jobs []*domain.Job) []types.Job {
	result := make([]types.Job, len(jobs))
	for i, job := range jobs {
		result[i] = m.DomainJobToAPI(job)
	}
	return result
}

// DomainSchedulerToAPI converts a domain.SchedulerInfo to a types.SchedulerInfo.
func (m *Mapper) DomainSchedulerToAPI(sched *domain.SchedulerInfo) *types.SchedulerInfo {
	if sched == nil {
		return nil
	}

	schedType := types.SchedulerInfoType(sched.Type)
	result := &types.SchedulerInfo{
		Type: &schedType,
	}

	if sched.ExternalJobID != "" {
		result.ExternalJobId = &sched.ExternalJobID
	}
	if sched.RawState != "" {
		result.RawState = &sched.RawState
	}
	if sched.SubmitTime != nil {
		result.SubmitTime = sched.SubmitTime
	}
	if sched.Partition != "" {
		result.Partition = &sched.Partition
	}
	if sched.Account != "" {
		result.Account = &sched.Account
	}
	if sched.QoS != "" {
		result.Qos = &sched.QoS
	}
	if sched.Priority != nil {
		result.Priority = sched.Priority
	}
	if sched.ExitCode != nil {
		result.ExitCode = sched.ExitCode
	}
	if sched.StateReason != "" {
		result.StateReason = &sched.StateReason
	}
	if sched.TimeLimitMins != nil {
		result.TimeLimitMinutes = sched.TimeLimitMins
	}
	if len(sched.Extra) > 0 {
		result.Extra = &sched.Extra
	}

	return result
}

// APISchedulerToDomain converts a types.SchedulerInfo to a domain.SchedulerInfo.
func (m *Mapper) APISchedulerToDomain(sched *types.SchedulerInfo) *domain.SchedulerInfo {
	if sched == nil {
		return nil
	}

	result := &domain.SchedulerInfo{}

	if sched.Type != nil {
		result.Type = domain.SchedulerType(*sched.Type)
	}
	if sched.ExternalJobId != nil {
		result.ExternalJobID = *sched.ExternalJobId
	}
	if sched.RawState != nil {
		result.RawState = *sched.RawState
	}
	if sched.SubmitTime != nil {
		result.SubmitTime = sched.SubmitTime
	}
	if sched.Partition != nil {
		result.Partition = *sched.Partition
	}
	if sched.Account != nil {
		result.Account = *sched.Account
	}
	if sched.Qos != nil {
		result.QoS = *sched.Qos
	}
	if sched.Priority != nil {
		result.Priority = sched.Priority
	}
	if sched.ExitCode != nil {
		result.ExitCode = sched.ExitCode
	}
	if sched.StateReason != nil {
		result.StateReason = *sched.StateReason
	}
	if sched.TimeLimitMinutes != nil {
		result.TimeLimitMins = sched.TimeLimitMinutes
	}
	if sched.Extra != nil {
		result.Extra = *sched.Extra
	}

	return result
}

// DomainMetricSampleToAPI converts a domain.MetricSample to a types.MetricSample.
func (m *Mapper) DomainMetricSampleToAPI(sample *domain.MetricSample) types.MetricSample {
	if sample == nil {
		return types.MetricSample{}
	}
	return types.MetricSample{
		Timestamp:     sample.Timestamp,
		CpuUsage:      sample.CPUUsage,
		MemoryUsageMb: int(sample.MemoryUsageMB),
		GpuUsage:      sample.GPUUsage,
	}
}

// DomainMetricSamplesToAPI converts a slice of domain.MetricSample to a slice of types.MetricSample.
func (m *Mapper) DomainMetricSamplesToAPI(samples []*domain.MetricSample) []types.MetricSample {
	result := make([]types.MetricSample, len(samples))
	for i, sample := range samples {
		result[i] = m.DomainMetricSampleToAPI(sample)
	}
	return result
}

// APIJobStateToDomain converts a types.JobState to a domain.JobState.
func (m *Mapper) APIJobStateToDomain(state types.JobState) domain.JobState {
	switch state {
	case types.Pending:
		return domain.JobStatePending
	case types.Running:
		return domain.JobStateRunning
	case types.Completed:
		return domain.JobStateCompleted
	case types.Failed:
		return domain.JobStateFailed
	case types.Cancelled:
		return domain.JobStateCancelled
	default:
		return ""
	}
}

// IsValidJobState returns true if the domain.JobState is valid.
func (m *Mapper) IsValidJobState(state domain.JobState) bool {
	return state.IsValid()
}
