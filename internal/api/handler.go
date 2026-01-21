// Package api provides the HTTP API server for the HPC Job Observability Service.
// It implements the generated ServerInterface from the OpenAPI spec.
package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/avic/hpc-job-observability-service/internal/api/server"
	"github.com/avic/hpc-job-observability-service/internal/api/types"
	"github.com/avic/hpc-job-observability-service/internal/metrics"
	"github.com/avic/hpc-job-observability-service/internal/storage"
	"github.com/google/uuid"
)

// Server implements the generated ServerInterface.
type Server struct {
	store    storage.Storage
	exporter *metrics.Exporter
}

// Ensure Server implements ServerInterface
var _ server.ServerInterface = (*Server)(nil)

// NewServer creates a new API server.
func NewServer(store storage.Storage, exporter *metrics.Exporter) *Server {
	return &Server{
		store:    store,
		exporter: exporter,
	}
}

// Routes returns an HTTP handler with all API routes configured.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	// Use generated handler with /v1 prefix
	wrapper := server.ServerInterfaceWrapper{
		Handler:          s,
		ErrorHandlerFunc: s.handleError,
	}

	// Health check
	mux.HandleFunc("GET /v1/health", wrapper.GetHealth)

	// Jobs endpoints
	mux.HandleFunc("GET /v1/jobs", wrapper.ListJobs)
	mux.HandleFunc("POST /v1/jobs", wrapper.CreateJob)
	mux.HandleFunc("GET /v1/jobs/{jobId}", wrapper.GetJob)
	mux.HandleFunc("PATCH /v1/jobs/{jobId}", wrapper.UpdateJob)
	mux.HandleFunc("DELETE /v1/jobs/{jobId}", wrapper.DeleteJob)

	// Metrics endpoints
	mux.HandleFunc("GET /v1/jobs/{jobId}/metrics", wrapper.GetJobMetrics)
	mux.HandleFunc("POST /v1/jobs/{jobId}/metrics", wrapper.RecordJobMetrics)

	// Prometheus metrics endpoint
	mux.Handle("GET /metrics", s.exporter.Handler())

	// Wrap with middleware
	return s.withMiddleware(mux)
}

func (s *Server) withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Add common headers
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleError(w http.ResponseWriter, r *http.Request, err error) {
	s.writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
}

// GetHealth implements ServerInterface.GetHealth
func (s *Server) GetHealth(w http.ResponseWriter, r *http.Request) {
	version := "1.0.0"
	resp := types.HealthResponse{
		Status:    types.Healthy,
		Timestamp: time.Now(),
		Version:   &version,
	}
	s.writeJSON(w, http.StatusOK, resp)
}

// ListJobs implements ServerInterface.ListJobs
func (s *Server) ListJobs(w http.ResponseWriter, r *http.Request, params server.ListJobsParams) {
	filter := storage.JobFilter{
		Limit:  100,
		Offset: 0,
	}

	// Apply query parameters
	if params.State != nil {
		state := storage.JobState(*params.State)
		filter.State = &state
	}
	if params.User != nil {
		filter.User = params.User
	}
	if params.Node != nil {
		filter.Node = params.Node
	}
	if params.Limit != nil {
		filter.Limit = *params.Limit
	}
	if params.Offset != nil {
		filter.Offset = *params.Offset
	}

	jobs, total, err := s.store.ListJobs(r.Context(), filter)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	jobResponses := make([]types.Job, len(jobs))
	for i, job := range jobs {
		jobResponses[i] = s.storageJobToAPI(job)
	}

	resp := types.JobListResponse{
		Jobs:   jobResponses,
		Total:  total,
		Limit:  filter.Limit,
		Offset: filter.Offset,
	}
	s.writeJSON(w, http.StatusOK, resp)
}

// CreateJob implements ServerInterface.CreateJob
func (s *Server) CreateJob(w http.ResponseWriter, r *http.Request, params server.CreateJobParams) {
	auditInfo, ok := s.auditInfoFromParams(w, params.XChangedBy, params.XSource, params.XCorrelationId)
	if !ok {
		return
	}

	var req types.CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid_request", "Invalid JSON body")
		return
	}

	// Validate request
	if req.Id == "" {
		s.writeError(w, http.StatusBadRequest, "validation_error", "Job ID is required")
		return
	}
	if req.User == "" {
		s.writeError(w, http.StatusBadRequest, "validation_error", "User is required")
		return
	}
	if len(req.Nodes) == 0 {
		s.writeError(w, http.StatusBadRequest, "validation_error", "At least one node is required")
		return
	}

	job := &storage.Job{
		ID:            req.Id,
		User:          req.User,
		Nodes:         req.Nodes,
		State:         storage.JobStateRunning,
		StartTime:     time.Now(),
		Scheduler:     s.apiSchedulerToStorage(req.Scheduler),
		Audit:         auditInfo,
		ClusterName:   "default",
		SchedulerInst: "api",
		IngestVersion: "api-v1",
	}

	if err := s.store.CreateJob(r.Context(), job); err != nil {
		if err == storage.ErrJobAlreadyExists {
			s.writeError(w, http.StatusConflict, "conflict", "Job with this ID already exists")
			return
		}
		s.writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	// Update Prometheus metrics
	s.exporter.IncrementJobsTotal()

	s.writeJSON(w, http.StatusCreated, s.storageJobToAPI(job))
}

// GetJob implements ServerInterface.GetJob
func (s *Server) GetJob(w http.ResponseWriter, r *http.Request, jobId server.JobId) {
	job, err := s.store.GetJob(r.Context(), string(jobId))
	if err != nil {
		if err == storage.ErrJobNotFound {
			s.writeError(w, http.StatusNotFound, "not_found", "Job not found")
			return
		}
		s.writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, s.storageJobToAPI(job))
}

// UpdateJob implements ServerInterface.UpdateJob
func (s *Server) UpdateJob(w http.ResponseWriter, r *http.Request, jobId server.JobId, params server.UpdateJobParams) {
	auditInfo, ok := s.auditInfoFromParams(w, params.XChangedBy, params.XSource, params.XCorrelationId)
	if !ok {
		return
	}

	var req types.UpdateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid_request", "Invalid JSON body")
		return
	}

	job, err := s.store.GetJob(r.Context(), string(jobId))
	if err != nil {
		if err == storage.ErrJobNotFound {
			s.writeError(w, http.StatusNotFound, "not_found", "Job not found")
			return
		}
		s.writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	// Apply updates
	if req.State != nil {
		state := storage.JobState(*req.State)
		if !isValidState(state) {
			s.writeError(w, http.StatusBadRequest, "validation_error", "Invalid job state")
			return
		}
		job.State = state
	}
	if req.CpuUsage != nil {
		job.CPUUsage = *req.CpuUsage
	}
	if req.MemoryUsageMb != nil {
		job.MemoryUsageMB = int64(*req.MemoryUsageMb)
	}
	if req.GpuUsage != nil {
		job.GPUUsage = req.GpuUsage
	}
	job.Audit = auditInfo

	if err := s.store.UpdateJob(r.Context(), job); err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	// Update Prometheus metrics
	s.exporter.UpdateJobMetrics(job)

	s.writeJSON(w, http.StatusOK, s.storageJobToAPI(job))
}

// DeleteJob implements ServerInterface.DeleteJob
func (s *Server) DeleteJob(w http.ResponseWriter, r *http.Request, jobId server.JobId, params server.DeleteJobParams) {
	auditInfo, ok := s.auditInfoFromParams(w, params.XChangedBy, params.XSource, params.XCorrelationId)
	if !ok {
		return
	}

	ctx := storage.WithAuditInfo(r.Context(), auditInfo)
	if err := s.store.DeleteJob(ctx, string(jobId)); err != nil {
		if err == storage.ErrJobNotFound {
			s.writeError(w, http.StatusNotFound, "not_found", "Job not found")
			return
		}
		s.writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) auditInfoFromRequest(w http.ResponseWriter, r *http.Request) (*storage.JobAuditInfo, bool) {
	changedBy := strings.TrimSpace(r.Header.Get("X-Changed-By"))
	source := strings.TrimSpace(r.Header.Get("X-Source"))
	correlationID := strings.TrimSpace(r.Header.Get("X-Correlation-Id"))

	if changedBy == "" {
		s.writeError(w, http.StatusBadRequest, "validation_error", "X-Changed-By header is required")
		return nil, false
	}
	if source == "" {
		s.writeError(w, http.StatusBadRequest, "validation_error", "X-Source header is required")
		return nil, false
	}
	if correlationID == "" {
		correlationID = uuid.NewString()
	}
	w.Header().Set("X-Correlation-Id", correlationID)

	return storage.NewAuditInfoWithCorrelation(changedBy, source, correlationID), true
}

func (s *Server) auditInfoFromParams(w http.ResponseWriter, changedBy string, source string, correlationID *string) (*storage.JobAuditInfo, bool) {
	changedBy = strings.TrimSpace(changedBy)
	source = strings.TrimSpace(source)
	if changedBy == "" {
		s.writeError(w, http.StatusBadRequest, "validation_error", "X-Changed-By header is required")
		return nil, false
	}
	if source == "" {
		s.writeError(w, http.StatusBadRequest, "validation_error", "X-Source header is required")
		return nil, false
	}
	finalCorrelationID := ""
	if correlationID != nil {
		finalCorrelationID = strings.TrimSpace(*correlationID)
	}
	if finalCorrelationID == "" {
		finalCorrelationID = uuid.NewString()
	}
	w.Header().Set("X-Correlation-Id", finalCorrelationID)

	return storage.NewAuditInfoWithCorrelation(changedBy, source, finalCorrelationID), true
}

// GetJobMetrics implements ServerInterface.GetJobMetrics
func (s *Server) GetJobMetrics(w http.ResponseWriter, r *http.Request, jobId server.JobId, params server.GetJobMetricsParams) {
	filter := storage.MetricsFilter{
		Limit: 1000,
	}

	if params.StartTime != nil {
		filter.StartTime = params.StartTime
	}
	if params.EndTime != nil {
		filter.EndTime = params.EndTime
	}
	if params.Limit != nil {
		filter.Limit = *params.Limit
	}

	samples, total, err := s.store.GetJobMetrics(r.Context(), string(jobId), filter)
	if err != nil {
		if err == storage.ErrJobNotFound {
			s.writeError(w, http.StatusNotFound, "not_found", "Job not found")
			return
		}
		s.writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	sampleResponses := make([]types.MetricSample, len(samples))
	for i, sample := range samples {
		sampleResponses[i] = types.MetricSample{
			Timestamp:     sample.Timestamp,
			CpuUsage:      sample.CPUUsage,
			MemoryUsageMb: int(sample.MemoryUsageMB),
			GpuUsage:      sample.GPUUsage,
		}
	}

	resp := types.JobMetricsResponse{
		JobId:   string(jobId),
		Samples: sampleResponses,
		Total:   total,
	}
	s.writeJSON(w, http.StatusOK, resp)
}

// RecordJobMetrics implements ServerInterface.RecordJobMetrics
func (s *Server) RecordJobMetrics(w http.ResponseWriter, r *http.Request, jobId server.JobId) {
	auditInfo, ok := s.auditInfoFromRequest(w, r)
	if !ok {
		return
	}

	// Check if job exists
	job, err := s.store.GetJob(r.Context(), string(jobId))
	if err != nil {
		if err == storage.ErrJobNotFound {
			s.writeError(w, http.StatusNotFound, "not_found", "Job not found")
			return
		}
		s.writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	var req types.RecordMetricsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid_request", "Invalid JSON body")
		return
	}

	// Validate
	if req.CpuUsage < 0 || req.CpuUsage > 100 {
		s.writeError(w, http.StatusBadRequest, "validation_error", "CPU usage must be between 0 and 100")
		return
	}
	if req.MemoryUsageMb < 0 {
		s.writeError(w, http.StatusBadRequest, "validation_error", "Memory usage must be non-negative")
		return
	}

	sample := &storage.MetricSample{
		JobID:         string(jobId),
		Timestamp:     time.Now(),
		CPUUsage:      req.CpuUsage,
		MemoryUsageMB: int64(req.MemoryUsageMb),
		GPUUsage:      req.GpuUsage,
	}

	if err := s.store.RecordMetrics(r.Context(), sample); err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	// Update job's current resource usage
	job.CPUUsage = req.CpuUsage
	job.MemoryUsageMB = int64(req.MemoryUsageMb)
	job.GPUUsage = req.GpuUsage
	job.Audit = auditInfo
	if err := s.store.UpdateJob(r.Context(), job); err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	// Update Prometheus metrics
	s.exporter.UpdateJobMetrics(job)

	resp := types.MetricSample{
		Timestamp:     sample.Timestamp,
		CpuUsage:      sample.CPUUsage,
		MemoryUsageMb: int(sample.MemoryUsageMB),
		GpuUsage:      sample.GPUUsage,
	}
	s.writeJSON(w, http.StatusCreated, resp)
}

// Helper methods

func (s *Server) storageJobToAPI(job *storage.Job) types.Job {
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
		resp.Scheduler = s.storageSchedulerToAPI(job.Scheduler)
	}

	return resp
}

func (s *Server) storageSchedulerToAPI(sched *storage.SchedulerInfo) *types.SchedulerInfo {
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

func (s *Server) apiSchedulerToStorage(sched *types.SchedulerInfo) *storage.SchedulerInfo {
	if sched == nil {
		return nil
	}

	result := &storage.SchedulerInfo{}

	if sched.Type != nil {
		result.Type = storage.SchedulerType(*sched.Type)
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

func (s *Server) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func (s *Server) writeError(w http.ResponseWriter, status int, errCode, message string) {
	s.writeJSON(w, status, types.ErrorResponse{
		Error:   errCode,
		Message: message,
	})
}

func isValidState(state storage.JobState) bool {
	validStates := []storage.JobState{
		storage.JobStatePending,
		storage.JobStateRunning,
		storage.JobStateCompleted,
		storage.JobStateFailed,
		storage.JobStateCancelled,
	}
	for _, s := range validStates {
		if state == s {
			return true
		}
	}
	return false
}
