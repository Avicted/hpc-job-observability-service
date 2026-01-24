// Package api provides the HTTP API server for the HPC Job Observability Service.
// It implements the generated ServerInterface from the OpenAPI spec.
//
// Handlers in this package are thin HTTP glue that:
//   - Parse and validate HTTP requests
//   - Extract audit context from headers
//   - Delegate to services for business logic
//   - Map service results/errors to HTTP responses
//
// Business logic lives in internal/service, conversions in internal/mapper.
package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/api/server"
	"github.com/Avicted/hpc-job-observability-service/internal/api/types"
	"github.com/Avicted/hpc-job-observability-service/internal/mapper"
	"github.com/Avicted/hpc-job-observability-service/internal/metrics"
	"github.com/Avicted/hpc-job-observability-service/internal/service"
	"github.com/Avicted/hpc-job-observability-service/internal/storage"
	"github.com/google/uuid"
)

// Server implements the generated ServerInterface.
type Server struct {
	store          storage.Storage
	exporter       *metrics.Exporter
	jobService     *service.JobService
	metricsService *service.MetricsService
	eventService   *service.EventService
	mapper         *mapper.Mapper
}

// Ensure Server implements ServerInterface
var _ server.ServerInterface = (*Server)(nil)

// NewServer creates a new API server with all required services.
func NewServer(store storage.Storage, exporter *metrics.Exporter) *Server {
	m := mapper.NewMapper()
	return &Server{
		store:          store,
		exporter:       exporter,
		jobService:     service.NewJobService(store, exporter, m),
		metricsService: service.NewMetricsService(store, exporter, m),
		eventService:   service.NewEventService(store, exporter, m),
		mapper:         m,
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

	// Job lifecycle event endpoints (for Slurm prolog/epilog scripts)
	mux.HandleFunc("POST /v1/events/job-started", wrapper.JobStartedEvent)
	mux.HandleFunc("POST /v1/events/job-finished", wrapper.JobFinishedEvent)

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
	var stateStr *string
	if params.State != nil {
		s := string(*params.State)
		stateStr = &s
	}

	input := service.ListJobsInput{
		State:  stateStr,
		User:   params.User,
		Node:   params.Node,
		Limit:  params.Limit,
		Offset: params.Offset,
	}

	output, err := s.jobService.ListJobs(r.Context(), input)
	if err != nil {
		s.handleServiceError(w, err)
		return
	}

	resp := types.JobListResponse{
		Jobs:   s.mapper.StorageJobsToAPI(output.Jobs),
		Total:  output.Total,
		Limit:  output.Limit,
		Offset: output.Offset,
	}
	s.writeJSON(w, http.StatusOK, resp)
}

// CreateJob implements ServerInterface.CreateJob
func (s *Server) CreateJob(w http.ResponseWriter, r *http.Request, params server.CreateJobParams) {
	audit, ok := s.auditFromParams(w, params.XChangedBy, params.XSource, params.XCorrelationId)
	if !ok {
		return
	}

	var req types.CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid_request", "Invalid JSON body")
		return
	}

	input := service.CreateJobInput{
		ID:        req.Id,
		User:      req.User,
		Nodes:     req.Nodes,
		Scheduler: req.Scheduler,
		Audit:     audit,
	}

	job, err := s.jobService.CreateJob(r.Context(), input)
	if err != nil {
		s.handleServiceError(w, err)
		return
	}

	s.writeJSON(w, http.StatusCreated, s.mapper.StorageJobToAPI(job))
}

// GetJob implements ServerInterface.GetJob
func (s *Server) GetJob(w http.ResponseWriter, r *http.Request, jobId server.JobId) {
	job, err := s.jobService.GetJob(r.Context(), string(jobId))
	if err != nil {
		s.handleServiceError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, s.mapper.StorageJobToAPI(job))
}

// UpdateJob implements ServerInterface.UpdateJob
func (s *Server) UpdateJob(w http.ResponseWriter, r *http.Request, jobId server.JobId, params server.UpdateJobParams) {
	audit, ok := s.auditFromParams(w, params.XChangedBy, params.XSource, params.XCorrelationId)
	if !ok {
		return
	}

	var req types.UpdateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid_request", "Invalid JSON body")
		return
	}

	input := service.UpdateJobInput{
		State:    req.State,
		CPUUsage: req.CpuUsage,
		GPUUsage: req.GpuUsage,
		Audit:    audit,
	}
	if req.MemoryUsageMb != nil {
		v := *req.MemoryUsageMb
		input.MemoryUsageMB = &v
	}

	job, err := s.jobService.UpdateJob(r.Context(), string(jobId), input)
	if err != nil {
		s.handleServiceError(w, err)
		return
	}

	s.writeJSON(w, http.StatusOK, s.mapper.StorageJobToAPI(job))
}

// DeleteJob implements ServerInterface.DeleteJob
func (s *Server) DeleteJob(w http.ResponseWriter, r *http.Request, jobId server.JobId, params server.DeleteJobParams) {
	audit, ok := s.auditFromParams(w, params.XChangedBy, params.XSource, params.XCorrelationId)
	if !ok {
		return
	}

	if err := s.jobService.DeleteJob(r.Context(), string(jobId), audit); err != nil {
		s.handleServiceError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// GetJobMetrics implements ServerInterface.GetJobMetrics
func (s *Server) GetJobMetrics(w http.ResponseWriter, r *http.Request, jobId server.JobId, params server.GetJobMetricsParams) {
	input := service.GetMetricsInput{
		JobID:     string(jobId),
		StartTime: params.StartTime,
		EndTime:   params.EndTime,
		Limit:     params.Limit,
	}

	output, err := s.metricsService.GetJobMetrics(r.Context(), input)
	if err != nil {
		s.handleServiceError(w, err)
		return
	}

	resp := types.JobMetricsResponse{
		JobId:   string(jobId),
		Samples: s.mapper.StorageMetricSamplesToAPI(output.Samples),
		Total:   output.Total,
	}
	s.writeJSON(w, http.StatusOK, resp)
}

// RecordJobMetrics implements ServerInterface.RecordJobMetrics
func (s *Server) RecordJobMetrics(w http.ResponseWriter, r *http.Request, jobId server.JobId) {
	audit, ok := s.auditFromRequest(w, r)
	if !ok {
		return
	}

	var req types.RecordMetricsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid_request", "Invalid JSON body")
		return
	}

	input := service.RecordMetricsInput{
		JobID:         string(jobId),
		CPUUsage:      req.CpuUsage,
		MemoryUsageMB: req.MemoryUsageMb,
		GPUUsage:      req.GpuUsage,
		Audit:         audit,
	}

	output, err := s.metricsService.RecordJobMetrics(r.Context(), input)
	if err != nil {
		s.handleServiceError(w, err)
		return
	}

	s.writeJSON(w, http.StatusCreated, s.mapper.StorageMetricSampleToAPI(output.Sample))
}

// JobStartedEvent implements ServerInterface.JobStartedEvent
// This endpoint receives job started events from Slurm prolog scripts.
func (s *Server) JobStartedEvent(w http.ResponseWriter, r *http.Request) {
	var event types.JobStartedEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid_request", "Invalid JSON body")
		return
	}

	input := service.JobStartedInput{
		JobID:              event.JobId,
		User:               event.User,
		NodeList:           event.NodeList,
		Timestamp:          event.Timestamp,
		CgroupPath:         event.CgroupPath,
		GPUVendor:          event.GpuVendor,
		GPUDevices:         event.GpuDevices,
		GPUAllocation:      event.GpuAllocation,
		CPUAllocation:      event.CpuAllocation,
		MemoryAllocationMB: event.MemoryAllocationMb,
		Partition:          event.Partition,
		Account:            event.Account,
	}

	output, err := s.eventService.JobStarted(r.Context(), input)
	if err != nil {
		s.handleServiceError(w, err)
		return
	}

	resp := types.JobEventResponse{
		JobId:   output.JobID,
		Status:  output.Status,
		Message: ptrString(output.Message),
	}
	s.writeJSON(w, http.StatusOK, resp)
}

// JobFinishedEvent implements ServerInterface.JobFinishedEvent
// This endpoint receives job finished events from Slurm epilog scripts.
func (s *Server) JobFinishedEvent(w http.ResponseWriter, r *http.Request) {
	var event types.JobFinishedEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid_request", "Invalid JSON body")
		return
	}

	input := service.JobFinishedInput{
		JobID:      event.JobId,
		FinalState: event.FinalState,
		Timestamp:  event.Timestamp,
		ExitCode:   event.ExitCode,
		Signal:     event.Signal,
	}

	output, err := s.eventService.JobFinished(r.Context(), input)
	if err != nil {
		s.handleServiceError(w, err)
		return
	}

	resp := types.JobEventResponse{
		JobId:   output.JobID,
		Status:  output.Status,
		Message: ptrString(output.Message),
	}
	s.writeJSON(w, http.StatusOK, resp)
}

// Audit context helpers

// validateAuditInfo validates the required audit fields and generates a correlation ID if needed.
func (s *Server) validateAuditInfo(w http.ResponseWriter, changedBy, source, correlationID string) (string, string, string, bool) {
	changedBy = strings.TrimSpace(changedBy)
	source = strings.TrimSpace(source)
	correlationID = strings.TrimSpace(correlationID)

	if changedBy == "" {
		s.writeError(w, http.StatusBadRequest, "validation_error", "X-Changed-By header is required")
		return "", "", "", false
	}
	if source == "" {
		s.writeError(w, http.StatusBadRequest, "validation_error", "X-Source header is required")
		return "", "", "", false
	}
	if correlationID == "" {
		correlationID = uuid.NewString()
	}
	w.Header().Set("X-Correlation-Id", correlationID)
	return changedBy, source, correlationID, true
}

func (s *Server) auditFromRequest(w http.ResponseWriter, r *http.Request) (*service.AuditContext, bool) {
	changedBy := r.Header.Get("X-Changed-By")
	source := r.Header.Get("X-Source")
	correlationID := r.Header.Get("X-Correlation-Id")

	changedBy, source, correlationID, ok := s.validateAuditInfo(w, changedBy, source, correlationID)
	if !ok {
		return nil, false
	}

	return &service.AuditContext{
		ChangedBy:     changedBy,
		Source:        source,
		CorrelationID: correlationID,
	}, true
}

func (s *Server) auditFromParams(w http.ResponseWriter, changedBy string, source string, correlationID *string) (*service.AuditContext, bool) {
	finalCorrelationID := ""
	if correlationID != nil {
		finalCorrelationID = *correlationID
	}

	changedBy, source, finalCorrelationID, ok := s.validateAuditInfo(w, changedBy, source, finalCorrelationID)
	if !ok {
		return nil, false
	}

	return &service.AuditContext{
		ChangedBy:     changedBy,
		Source:        source,
		CorrelationID: finalCorrelationID,
	}, true
}

// Response helpers

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

// handleServiceError maps service errors to HTTP responses.
func (s *Server) handleServiceError(w http.ResponseWriter, err error) {
	switch err {
	case service.ErrJobNotFound:
		s.writeError(w, http.StatusNotFound, "not_found", "Job not found")
	case service.ErrJobAlreadyExists:
		s.writeError(w, http.StatusConflict, "conflict", "Job with this ID already exists")
	case service.ErrInvalidJobState:
		s.writeError(w, http.StatusBadRequest, "validation_error", "Invalid job state")
	case service.ErrJobTerminalFrozen:
		s.writeError(w, http.StatusConflict, "conflict", "Job is in terminal state and cannot be updated")
	case service.ErrInternalError:
		s.writeError(w, http.StatusInternalServerError, "internal_error", "Internal server error")
	default:
		// Check for ValidationError
		if ve, ok := err.(*service.ValidationError); ok {
			s.writeError(w, http.StatusBadRequest, "validation_error", ve.Message)
			return
		}
		s.writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
	}
}

func ptrString(s string) *string {
	return &s
}
