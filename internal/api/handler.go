// Package api provides the HTTP API server for the HPC Job Observability Service.
// It implements the generated StrictServerInterface from the OpenAPI spec.
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
	"context"
	"strings"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/api/server"
	"github.com/Avicted/hpc-job-observability-service/internal/api/types"
	"github.com/Avicted/hpc-job-observability-service/internal/service"
	"github.com/Avicted/hpc-job-observability-service/internal/storage"
	"github.com/Avicted/hpc-job-observability-service/internal/utils/audit"
	"github.com/Avicted/hpc-job-observability-service/internal/utils/mapper"
	"github.com/Avicted/hpc-job-observability-service/internal/utils/metrics"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	echoSwagger "github.com/swaggo/echo-swagger"
)

// Server implements the generated StrictServerInterface.
type Server struct {
	store          storage.Store
	exporter       *metrics.Exporter
	jobService     *service.JobService
	metricsService *service.MetricsService
	eventService   *service.EventService
	mapper         *mapper.Mapper
}

// Ensure Server implements StrictServerInterface
var _ server.StrictServerInterface = (*Server)(nil)

// NewServer creates a new API server with all required services.
func NewServer(store storage.Store, exporter *metrics.Exporter) *Server {
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

// RegisterRoutes registers all API routes with the Echo instance.
func (s *Server) RegisterRoutes(e *echo.Echo) {
	// Create strict handler
	strictHandler := server.NewStrictHandler(s, nil)

	// Register API routes with /v1 prefix
	v1 := e.Group("/v1")
	server.RegisterHandlers(v1, strictHandler)

	// OpenAPI spec endpoint
	e.GET("/openapi.json", s.serveOpenAPISpec)

	// Swagger UI endpoint
	e.GET("/swagger/*", echoSwagger.EchoWrapHandler(echoSwagger.URL("/openapi.json")))

	// Prometheus metrics endpoint (no /v1 prefix)
	// Use the exporter's handler to include custom metrics
	e.GET("/metrics", echo.WrapHandler(s.exporter.Handler()))
}

// serveOpenAPISpec serves the embedded OpenAPI specification as JSON
func (s *Server) serveOpenAPISpec(c echo.Context) error {
	swagger, err := server.GetSwagger()
	if err != nil {
		return echo.NewHTTPError(500, "Failed to load OpenAPI spec")
	}
	return c.JSON(200, swagger)
}

// GetHealth implements StrictServerInterface.GetHealth
func (s *Server) GetHealth(ctx context.Context, request server.GetHealthRequestObject) (server.GetHealthResponseObject, error) {
	version := "1.0.0"
	return server.GetHealth200JSONResponse{
		Status:    server.Healthy,
		Timestamp: time.Now(),
		Version:   &version,
	}, nil
}

// ListJobs implements StrictServerInterface.ListJobs
func (s *Server) ListJobs(ctx context.Context, request server.ListJobsRequestObject) (server.ListJobsResponseObject, error) {
	var stateStr *string
	if request.Params.State != nil {
		s := string(*request.Params.State)
		stateStr = &s
	}

	input := service.ListJobsInput{
		State:  stateStr,
		User:   request.Params.User,
		Node:   request.Params.Node,
		Limit:  request.Params.Limit,
		Offset: request.Params.Offset,
	}

	output, err := s.jobService.ListJobs(ctx, input)
	if err != nil {
		return s.handleListJobsError(err), nil
	}

	return server.ListJobs200JSONResponse{
		Jobs:   typesJobsToServer(s.mapper.DomainJobsToAPI(output.Jobs)),
		Total:  output.Total,
		Limit:  output.Limit,
		Offset: output.Offset,
	}, nil
}

// CreateJob implements StrictServerInterface.CreateJob
func (s *Server) CreateJob(ctx context.Context, request server.CreateJobRequestObject) (server.CreateJobResponseObject, error) {
	auditCtx, err := s.auditFromParams(request.Params.XChangedBy, request.Params.XSource, request.Params.XCorrelationId)
	if err != nil {
		return server.CreateJob400JSONResponse{
			Error:   "validation_error",
			Message: err.Error(),
		}, nil
	}

	if request.Body == nil {
		return server.CreateJob400JSONResponse{
			Error:   "invalid_request",
			Message: "Request body is required",
		}, nil
	}

	input := service.CreateJobInput{
		ID:        request.Body.Id,
		User:      request.Body.User,
		Nodes:     request.Body.Nodes,
		Scheduler: serverSchedulerInfoToTypes(request.Body.Scheduler),
		Audit:     auditCtx,
	}

	job, err := s.jobService.CreateJob(ctx, input)
	if err != nil {
		return s.handleCreateJobError(err), nil
	}

	return server.CreateJob201JSONResponse(typesJobToServer(s.mapper.DomainJobToAPI(job))), nil
}

// GetJob implements StrictServerInterface.GetJob
func (s *Server) GetJob(ctx context.Context, request server.GetJobRequestObject) (server.GetJobResponseObject, error) {
	job, err := s.jobService.GetJob(ctx, string(request.JobId))
	if err != nil {
		return s.handleGetJobError(err), nil
	}
	return server.GetJob200JSONResponse(typesJobToServer(s.mapper.DomainJobToAPI(job))), nil
}

// UpdateJob implements StrictServerInterface.UpdateJob
func (s *Server) UpdateJob(ctx context.Context, request server.UpdateJobRequestObject) (server.UpdateJobResponseObject, error) {
	auditCtx, err := s.auditFromParams(request.Params.XChangedBy, request.Params.XSource, request.Params.XCorrelationId)
	if err != nil {
		return server.UpdateJob400JSONResponse{
			Error:   "validation_error",
			Message: err.Error(),
		}, nil
	}

	if request.Body == nil {
		return server.UpdateJob400JSONResponse{
			Error:   "invalid_request",
			Message: "Request body is required",
		}, nil
	}

	var state *types.JobState
	if request.Body.State != nil {
		v := serverJobStateToTypes(*request.Body.State)
		state = &v
	}

	input := service.UpdateJobInput{
		State:    state,
		CPUUsage: request.Body.CpuUsage,
		GPUUsage: request.Body.GpuUsage,
		Audit:    auditCtx,
	}
	if request.Body.MemoryUsageMb != nil {
		v := *request.Body.MemoryUsageMb
		input.MemoryUsageMB = &v
	}

	job, err := s.jobService.UpdateJob(ctx, string(request.JobId), input)
	if err != nil {
		return s.handleUpdateJobError(err), nil
	}

	return server.UpdateJob200JSONResponse(typesJobToServer(s.mapper.DomainJobToAPI(job))), nil
}

// DeleteJob implements StrictServerInterface.DeleteJob
func (s *Server) DeleteJob(ctx context.Context, request server.DeleteJobRequestObject) (server.DeleteJobResponseObject, error) {
	auditCtx, err := s.auditFromParams(request.Params.XChangedBy, request.Params.XSource, request.Params.XCorrelationId)
	if err != nil {
		return nil, err
	}

	if err := s.jobService.DeleteJob(ctx, string(request.JobId), auditCtx); err != nil {
		return s.handleDeleteJobError(err), nil
	}

	return server.DeleteJob204Response{}, nil
}

// GetJobMetrics implements StrictServerInterface.GetJobMetrics
func (s *Server) GetJobMetrics(ctx context.Context, request server.GetJobMetricsRequestObject) (server.GetJobMetricsResponseObject, error) {
	input := service.GetMetricsInput{
		JobID:     string(request.JobId),
		StartTime: request.Params.StartTime,
		EndTime:   request.Params.EndTime,
		Limit:     request.Params.Limit,
	}

	output, err := s.metricsService.GetJobMetrics(ctx, input)
	if err != nil {
		return s.handleGetJobMetricsError(err), nil
	}

	return server.GetJobMetrics200JSONResponse{
		JobId:   string(request.JobId),
		Samples: typesMetricSamplesToServer(s.mapper.DomainMetricSamplesToAPI(output.Samples)),
		Total:   output.Total,
	}, nil
}

// RecordJobMetrics implements StrictServerInterface.RecordJobMetrics
func (s *Server) RecordJobMetrics(ctx context.Context, request server.RecordJobMetricsRequestObject) (server.RecordJobMetricsResponseObject, error) {
	auditCtx, err := s.auditFromParams(request.Params.XChangedBy, request.Params.XSource, request.Params.XCorrelationId)
	if err != nil {
		return server.RecordJobMetrics400JSONResponse{
			Error:   "validation_error",
			Message: err.Error(),
		}, nil
	}

	if request.Body == nil {
		return server.RecordJobMetrics400JSONResponse{
			Error:   "invalid_request",
			Message: "Request body is required",
		}, nil
	}

	input := service.RecordMetricsInput{
		JobID:         string(request.JobId),
		CPUUsage:      request.Body.CpuUsage,
		MemoryUsageMB: request.Body.MemoryUsageMb,
		GPUUsage:      request.Body.GpuUsage,
		Audit:         auditCtx,
	}

	output, err := s.metricsService.RecordJobMetrics(ctx, input)
	if err != nil {
		return s.handleRecordJobMetricsError(err), nil
	}

	return server.RecordJobMetrics201JSONResponse(typesMetricSampleToServer(s.mapper.DomainMetricSampleToAPI(output.Sample))), nil
}

// JobStartedEvent implements StrictServerInterface.JobStartedEvent
// This endpoint receives job started events from Slurm prolog scripts.
func (s *Server) JobStartedEvent(ctx context.Context, request server.JobStartedEventRequestObject) (server.JobStartedEventResponseObject, error) {
	if request.Body == nil {
		return server.JobStartedEvent400JSONResponse{
			Error:   "invalid_request",
			Message: "Request body is required",
		}, nil
	}

	input := service.JobStartedInput{
		JobID:              request.Body.JobId,
		User:               request.Body.User,
		NodeList:           request.Body.NodeList,
		Timestamp:          request.Body.Timestamp,
		CgroupPath:         request.Body.CgroupPath,
		GPUVendor:          serverGPUVendorToTypes(request.Body.GpuVendor),
		GPUDevices:         request.Body.GpuDevices,
		GPUAllocation:      request.Body.GpuAllocation,
		CPUAllocation:      request.Body.CpuAllocation,
		MemoryAllocationMB: request.Body.MemoryAllocationMb,
		Partition:          request.Body.Partition,
		Account:            request.Body.Account,
	}

	output, err := s.eventService.JobStarted(ctx, input)
	if err != nil {
		return s.handleJobStartedEventError(err), nil
	}

	return server.JobStartedEvent200JSONResponse{
		JobId:   output.JobID,
		Status:  server.JobEventResponseStatus(output.Status),
		Message: ptrString(output.Message),
	}, nil
}

// JobFinishedEvent implements StrictServerInterface.JobFinishedEvent
// This endpoint receives job finished events from Slurm epilog scripts.
func (s *Server) JobFinishedEvent(ctx context.Context, request server.JobFinishedEventRequestObject) (server.JobFinishedEventResponseObject, error) {
	if request.Body == nil {
		return server.JobFinishedEvent400JSONResponse{
			Error:   "invalid_request",
			Message: "Request body is required",
		}, nil
	}

	input := service.JobFinishedInput{
		JobID:      request.Body.JobId,
		FinalState: serverJobStateToTypes(request.Body.FinalState),
		Timestamp:  request.Body.Timestamp,
		ExitCode:   request.Body.ExitCode,
		Signal:     request.Body.Signal,
	}

	output, err := s.eventService.JobFinished(ctx, input)
	if err != nil {
		return s.handleJobFinishedEventError(err), nil
	}

	return server.JobFinishedEvent200JSONResponse{
		JobId:   output.JobID,
		Status:  server.JobEventResponseStatus(output.Status),
		Message: ptrString(output.Message),
	}, nil
}

// Audit context helpers

// auditFromParams extracts audit info from request params and validates it.
func (s *Server) auditFromParams(changedBy string, source string, correlationID *string) (*audit.Context, error) {
	changedBy = strings.TrimSpace(changedBy)
	source = strings.TrimSpace(source)

	if changedBy == "" {
		return nil, echo.NewHTTPError(400, "X-Changed-By header is required")
	}
	if source == "" {
		return nil, echo.NewHTTPError(400, "X-Source header is required")
	}

	finalCorrelationID := ""
	if correlationID != nil {
		finalCorrelationID = strings.TrimSpace(*correlationID)
	}
	if finalCorrelationID == "" {
		finalCorrelationID = uuid.NewString()
	}

	return &audit.Context{
		ChangedBy:     changedBy,
		Source:        source,
		CorrelationID: finalCorrelationID,
	}, nil
}

// auditFromEchoContext extracts audit info from Echo context headers.
func (s *Server) auditFromEchoContext(c echo.Context) (*audit.Context, error) {
	changedBy := strings.TrimSpace(c.Request().Header.Get("X-Changed-By"))
	source := strings.TrimSpace(c.Request().Header.Get("X-Source"))
	correlationID := strings.TrimSpace(c.Request().Header.Get("X-Correlation-Id"))

	if changedBy == "" {
		return nil, echo.NewHTTPError(400, "X-Changed-By header is required")
	}
	if source == "" {
		return nil, echo.NewHTTPError(400, "X-Source header is required")
	}
	if correlationID == "" {
		correlationID = uuid.NewString()
	}

	// Set correlation ID in response header
	c.Response().Header().Set("X-Correlation-Id", correlationID)

	return &audit.Context{
		ChangedBy:     changedBy,
		Source:        source,
		CorrelationID: correlationID,
	}, nil
}

// handleServiceError maps service errors to appropriate response objects.
// This returns the error response as a ResponseObject, not as an error.
func (s *Server) handleServiceError(err error) (interface{}, error) {
	switch err {
	case service.ErrJobNotFound:
		return server.GetJob404JSONResponse{
			Error:   "not_found",
			Message: "Job not found",
		}, nil
	case service.ErrJobAlreadyExists:
		return server.CreateJob409JSONResponse{
			Error:   "conflict",
			Message: "Job with this ID already exists",
		}, nil
	case service.ErrInvalidJobState:
		return server.UpdateJob400JSONResponse{
			Error:   "validation_error",
			Message: "Invalid job state",
		}, nil
	case service.ErrJobTerminalFrozen:
		// Map to 400 since UpdateJob doesn't have 409 in OpenAPI spec
		return server.UpdateJob400JSONResponse{
			Error:   "conflict",
			Message: "Job is in terminal state and cannot be updated",
		}, nil
	case service.ErrInternalError:
		return server.GetJob500JSONResponse{
			Error:   "internal_error",
			Message: "Internal server error",
		}, nil
	default:
		// Check for ValidationError
		if ve, ok := err.(*service.ValidationError); ok {
			return server.CreateJob400JSONResponse{
				Error:   "validation_error",
				Message: ve.Message,
			}, nil
		}
		// Return generic internal error for unexpected errors
		return server.GetJob500JSONResponse{
			Error:   "internal_error",
			Message: err.Error(),
		}, nil
	}
}

// handleJobStartedEventError maps service errors to JobStartedEvent response types
func (s *Server) handleJobStartedEventError(err error) server.JobStartedEventResponseObject {
	switch err {
	case service.ErrInternalError:
		return server.JobStartedEvent500JSONResponse{
			Error:   "internal_error",
			Message: "Internal server error",
		}
	default:
		if ve, ok := err.(*service.ValidationError); ok {
			return server.JobStartedEvent400JSONResponse{
				Error:   "validation_error",
				Message: ve.Message,
			}
		}
		return server.JobStartedEvent500JSONResponse{
			Error:   "internal_error",
			Message: err.Error(),
		}
	}
}

// handleJobFinishedEventError maps service errors to JobFinishedEvent response types
func (s *Server) handleJobFinishedEventError(err error) server.JobFinishedEventResponseObject {
	switch err {
	case service.ErrJobNotFound:
		return server.JobFinishedEvent404JSONResponse{
			Error:   "not_found",
			Message: "Job not found",
		}
	case service.ErrInternalError:
		return server.JobFinishedEvent500JSONResponse{
			Error:   "internal_error",
			Message: "Internal server error",
		}
	default:
		if ve, ok := err.(*service.ValidationError); ok {
			return server.JobFinishedEvent400JSONResponse{
				Error:   "validation_error",
				Message: ve.Message,
			}
		}
		return server.JobFinishedEvent500JSONResponse{
			Error:   "internal_error",
			Message: err.Error(),
		}
	}
}

// handleListJobsError maps service errors to ListJobs response types
func (s *Server) handleListJobsError(err error) server.ListJobsResponseObject {
	return server.ListJobs500JSONResponse{
		Error:   "internal_error",
		Message: err.Error(),
	}
}

// handleGetJobError maps service errors to GetJob response types
func (s *Server) handleGetJobError(err error) server.GetJobResponseObject {
	if err == service.ErrJobNotFound {
		return server.GetJob404JSONResponse{
			Error:   "not_found",
			Message: "Job not found",
		}
	}
	return server.GetJob500JSONResponse{
		Error:   "internal_error",
		Message: err.Error(),
	}
}

// handleUpdateJobError maps service errors to UpdateJob response types
func (s *Server) handleUpdateJobError(err error) server.UpdateJobResponseObject {
	switch err {
	case service.ErrJobNotFound:
		return server.UpdateJob404JSONResponse{
			Error:   "not_found",
			Message: "Job not found",
		}
	case service.ErrInvalidJobState, service.ErrJobTerminalFrozen:
		return server.UpdateJob400JSONResponse{
			Error:   "validation_error",
			Message: err.Error(),
		}
	default:
		if ve, ok := err.(*service.ValidationError); ok {
			return server.UpdateJob400JSONResponse{
				Error:   "validation_error",
				Message: ve.Message,
			}
		}
		return server.UpdateJob500JSONResponse{
			Error:   "internal_error",
			Message: err.Error(),
		}
	}
}

// handleDeleteJobError maps service errors to DeleteJob response types
func (s *Server) handleDeleteJobError(err error) server.DeleteJobResponseObject {
	if err == service.ErrJobNotFound {
		return server.DeleteJob404JSONResponse{
			Error:   "not_found",
			Message: "Job not found",
		}
	}
	return server.DeleteJob500JSONResponse{
		Error:   "internal_error",
		Message: err.Error(),
	}
}

// handleGetJobMetricsError maps service errors to GetJobMetrics response types
func (s *Server) handleGetJobMetricsError(err error) server.GetJobMetricsResponseObject {
	if err == service.ErrJobNotFound {
		return server.GetJobMetrics404JSONResponse{
			Error:   "not_found",
			Message: "Job not found",
		}
	}
	return server.GetJobMetrics500JSONResponse{
		Error:   "internal_error",
		Message: err.Error(),
	}
}

// handleRecordJobMetricsError maps service errors to RecordJobMetrics response types
func (s *Server) handleRecordJobMetricsError(err error) server.RecordJobMetricsResponseObject {
	switch err {
	case service.ErrJobNotFound:
		return server.RecordJobMetrics404JSONResponse{
			Error:   "not_found",
			Message: "Job not found",
		}
	default:
		if ve, ok := err.(*service.ValidationError); ok {
			return server.RecordJobMetrics400JSONResponse{
				Error:   "validation_error",
				Message: ve.Message,
			}
		}
		return server.RecordJobMetrics500JSONResponse{
			Error:   "internal_error",
			Message: err.Error(),
		}
	}
}

// handleCreateJobError maps service errors to CreateJob response types
func (s *Server) handleCreateJobError(err error) server.CreateJobResponseObject {
	switch err {
	case service.ErrJobAlreadyExists:
		return server.CreateJob409JSONResponse{
			Error:   "conflict",
			Message: "Job with this ID already exists",
		}
	default:
		if ve, ok := err.(*service.ValidationError); ok {
			return server.CreateJob400JSONResponse{
				Error:   "validation_error",
				Message: ve.Message,
			}
		}
		return server.CreateJob500JSONResponse{
			Error:   "internal_error",
			Message: err.Error(),
		}
	}
}

func ptrString(s string) *string {
	return &s
}

// Type conversion helpers between types and server packages
// Both packages have the same types generated from OpenAPI spec,
// but we need to convert between them as they're different types.

func serverJobToTypes(j server.Job) types.Job {
	return types.Job{
		Id:                j.Id,
		User:              j.User,
		Nodes:             j.Nodes,
		State:             types.JobState(j.State),
		StartTime:         j.StartTime,
		EndTime:           j.EndTime,
		CpuUsage:          j.CpuUsage,
		MemoryUsageMb:     j.MemoryUsageMb,
		GpuUsage:          j.GpuUsage,
		RuntimeSeconds:    j.RuntimeSeconds,
		LastSampleAt:      j.LastSampleAt,
		NodeCount:         j.NodeCount,
		RequestedCpus:     j.RequestedCpus,
		AllocatedCpus:     j.AllocatedCpus,
		RequestedMemoryMb: j.RequestedMemoryMb,
		AllocatedMemoryMb: j.AllocatedMemoryMb,
		RequestedGpus:     j.RequestedGpus,
		AllocatedGpus:     j.AllocatedGpus,
		ClusterName:       j.ClusterName,
		SchedulerInstance: j.SchedulerInstance,
		IngestVersion:     j.IngestVersion,
		SampleCount:       j.SampleCount,
		AvgCpuUsage:       j.AvgCpuUsage,
		MaxCpuUsage:       j.MaxCpuUsage,
		MaxMemoryUsageMb:  j.MaxMemoryUsageMb,
		AvgGpuUsage:       j.AvgGpuUsage,
		MaxGpuUsage:       j.MaxGpuUsage,
		Scheduler:         serverSchedulerInfoToTypes(j.Scheduler),
	}
}

func typesJobToServer(j types.Job) server.Job {
	return server.Job{
		Id:                j.Id,
		User:              j.User,
		Nodes:             j.Nodes,
		State:             server.JobState(j.State),
		StartTime:         j.StartTime,
		EndTime:           j.EndTime,
		CpuUsage:          j.CpuUsage,
		MemoryUsageMb:     j.MemoryUsageMb,
		GpuUsage:          j.GpuUsage,
		RuntimeSeconds:    j.RuntimeSeconds,
		LastSampleAt:      j.LastSampleAt,
		NodeCount:         j.NodeCount,
		RequestedCpus:     j.RequestedCpus,
		AllocatedCpus:     j.AllocatedCpus,
		RequestedMemoryMb: j.RequestedMemoryMb,
		AllocatedMemoryMb: j.AllocatedMemoryMb,
		RequestedGpus:     j.RequestedGpus,
		AllocatedGpus:     j.AllocatedGpus,
		ClusterName:       j.ClusterName,
		SchedulerInstance: j.SchedulerInstance,
		IngestVersion:     j.IngestVersion,
		SampleCount:       j.SampleCount,
		AvgCpuUsage:       j.AvgCpuUsage,
		MaxCpuUsage:       j.MaxCpuUsage,
		MaxMemoryUsageMb:  j.MaxMemoryUsageMb,
		AvgGpuUsage:       j.AvgGpuUsage,
		MaxGpuUsage:       j.MaxGpuUsage,
		Scheduler:         typesSchedulerInfoToServer(j.Scheduler),
	}
}

func typesJobsToServer(jobs []types.Job) []server.Job {
	result := make([]server.Job, len(jobs))
	for i, job := range jobs {
		result[i] = typesJobToServer(job)
	}
	return result
}

func serverSchedulerInfoToTypes(s *server.SchedulerInfo) *types.SchedulerInfo {
	if s == nil {
		return nil
	}
	var t *types.SchedulerInfoType
	if s.Type != nil {
		v := types.SchedulerInfoType(*s.Type)
		t = &v
	}
	return &types.SchedulerInfo{
		Type:             t,
		ExternalJobId:    s.ExternalJobId,
		Account:          s.Account,
		Partition:        s.Partition,
		Qos:              s.Qos,
		Priority:         s.Priority,
		SubmitTime:       s.SubmitTime,
		TimeLimitMinutes: s.TimeLimitMinutes,
		RawState:         s.RawState,
		StateReason:      s.StateReason,
		ExitCode:         s.ExitCode,
		Extra:            s.Extra,
	}
}

func typesSchedulerInfoToServer(s *types.SchedulerInfo) *server.SchedulerInfo {
	if s == nil {
		return nil
	}
	var t *server.SchedulerInfoType
	if s.Type != nil {
		v := server.SchedulerInfoType(*s.Type)
		t = &v
	}
	return &server.SchedulerInfo{
		Type:             t,
		ExternalJobId:    s.ExternalJobId,
		Account:          s.Account,
		Partition:        s.Partition,
		Qos:              s.Qos,
		Priority:         s.Priority,
		SubmitTime:       s.SubmitTime,
		TimeLimitMinutes: s.TimeLimitMinutes,
		RawState:         s.RawState,
		StateReason:      s.StateReason,
		ExitCode:         s.ExitCode,
		Extra:            s.Extra,
	}
}

func typesMetricSamplesToServer(samples []types.MetricSample) []server.MetricSample {
	result := make([]server.MetricSample, len(samples))
	for i, sample := range samples {
		result[i] = typesMetricSampleToServer(sample)
	}
	return result
}

func typesMetricSampleToServer(s types.MetricSample) server.MetricSample {
	return server.MetricSample{
		Timestamp:     s.Timestamp,
		CpuUsage:      s.CpuUsage,
		MemoryUsageMb: s.MemoryUsageMb,
		GpuUsage:      s.GpuUsage,
	}
}

func serverGPUVendorToTypes(v *server.GPUVendor) *types.GPUVendor {
	if v == nil {
		return nil
	}
	tv := types.GPUVendor(*v)
	return &tv
}

func serverJobStateToTypes(s server.JobState) types.JobState {
	return types.JobState(s)
}
