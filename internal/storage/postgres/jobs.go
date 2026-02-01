package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/domain"
	"github.com/Avicted/hpc-job-observability-service/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// jobStore implements storage.JobStore using pgx.
type jobStore struct {
	db    DBTX
	store *Store
}

// mapPgError maps PostgreSQL errors to storage-level typed errors.
func mapPgError(err error) error {
	if err == nil {
		return nil
	}

	// Check for no rows
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.ErrNotFound
	}

	// Check for PostgreSQL-specific errors
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505": // unique_violation
			return storage.ErrConflict
		case "23503": // foreign_key_violation
			return storage.ErrInvalidInput
		case "23514": // check_violation
			return storage.ErrInvalidInput
		case "23502": // not_null_violation
			return storage.ErrInvalidInput
		}
	}

	return err
}

// scanJobRow scans a pgx.Row into a domain.Job, converting pgtype nulls to Go types.
func scanJobRow(row pgx.Row) (*domain.Job, error) {
	var job domain.Job
	var nodesStr string
	var stateStr string

	// pgtype nullable fields
	var nodeCount pgtype.Int8
	var endTime pgtype.Timestamptz
	var gpuUsage pgtype.Float8
	var externalJobID, schedulerType, rawState, partition, account, qos, stateReason pgtype.Text
	var submitTime, lastSampleAt pgtype.Timestamptz
	var priority, exitCode, timeLimitMins pgtype.Int8
	var requestedCPUs, allocatedCPUs, requestedMemMB, allocatedMemMB pgtype.Int8
	var requestedGPUs, allocatedGPUs pgtype.Int8
	var clusterName, schedulerInstance, ingestVersion pgtype.Text
	var sampleCount pgtype.Int8
	var avgCPU, maxCPU, avgGPU, maxGPU pgtype.Float8
	var maxMem pgtype.Int8
	var cgroupPath, gpuVendor, gpuDevicesStr pgtype.Text
	var gpuCount pgtype.Int8

	err := row.Scan(
		&job.ID, &job.User, &nodesStr, &nodeCount, &stateStr, &job.StartTime, &endTime,
		&job.RuntimeSeconds, &job.CPUUsage, &job.MemoryUsageMB, &gpuUsage,
		&externalJobID, &schedulerType, &rawState, &partition, &account, &qos, &priority, &submitTime, &exitCode, &stateReason, &timeLimitMins,
		&requestedCPUs, &allocatedCPUs, &requestedMemMB, &allocatedMemMB, &requestedGPUs, &allocatedGPUs,
		&clusterName, &schedulerInstance, &ingestVersion,
		&lastSampleAt, &sampleCount, &avgCPU, &maxCPU, &maxMem, &avgGPU, &maxGPU,
		&cgroupPath, &gpuCount, &gpuVendor, &gpuDevicesStr,
		&job.CreatedAt, &job.UpdatedAt,
	)
	if err != nil {
		return nil, mapPgError(err)
	}

	job.State = domain.JobState(stateStr)

	// Convert nodes string to slice
	if nodesStr != "" {
		job.Nodes = strings.Split(nodesStr, ",")
	} else {
		job.Nodes = []string{}
	}

	// Convert pgtype nulls to Go types
	if nodeCount.Valid {
		job.NodeCount = int(nodeCount.Int64)
	}
	if endTime.Valid {
		t := endTime.Time
		job.EndTime = &t
	}
	if gpuUsage.Valid {
		f := gpuUsage.Float64
		job.GPUUsage = &f
	}
	if requestedCPUs.Valid {
		job.RequestedCPUs = requestedCPUs.Int64
	}
	if allocatedCPUs.Valid {
		job.AllocatedCPUs = allocatedCPUs.Int64
	}
	if requestedMemMB.Valid {
		job.RequestedMemMB = requestedMemMB.Int64
	}
	if allocatedMemMB.Valid {
		job.AllocatedMemMB = allocatedMemMB.Int64
	}
	if requestedGPUs.Valid {
		job.RequestedGPUs = requestedGPUs.Int64
	}
	if allocatedGPUs.Valid {
		job.AllocatedGPUs = allocatedGPUs.Int64
	}
	if clusterName.Valid {
		job.ClusterName = clusterName.String
	}
	if schedulerInstance.Valid {
		job.SchedulerInst = schedulerInstance.String
	}
	if ingestVersion.Valid {
		job.IngestVersion = ingestVersion.String
	}
	if lastSampleAt.Valid {
		t := lastSampleAt.Time
		job.LastSampleAt = &t
	}
	if sampleCount.Valid {
		job.SampleCount = sampleCount.Int64
	}
	if avgCPU.Valid {
		job.AvgCPUUsage = avgCPU.Float64
	}
	if maxCPU.Valid {
		job.MaxCPUUsage = maxCPU.Float64
	}
	if maxMem.Valid {
		job.MaxMemUsageMB = maxMem.Int64
	}
	if avgGPU.Valid {
		job.AvgGPUUsage = avgGPU.Float64
	}
	if maxGPU.Valid {
		job.MaxGPUUsage = maxGPU.Float64
	}
	if cgroupPath.Valid {
		job.CgroupPath = cgroupPath.String
	}
	if gpuCount.Valid {
		job.GPUCount = int(gpuCount.Int64)
	}
	if gpuVendor.Valid {
		job.GPUVendor = domain.GPUVendor(gpuVendor.String)
	}
	if gpuDevicesStr.Valid && gpuDevicesStr.String != "" {
		job.GPUDevices = strings.Split(gpuDevicesStr.String, ",")
	}

	// Build SchedulerInfo if any scheduler field is present
	if externalJobID.Valid || rawState.Valid || partition.Valid || account.Valid || qos.Valid || submitTime.Valid || priority.Valid || exitCode.Valid || stateReason.Valid || timeLimitMins.Valid || schedulerType.Valid {
		job.Scheduler = &domain.SchedulerInfo{}
		if schedulerType.Valid {
			job.Scheduler.Type = domain.SchedulerType(schedulerType.String)
		}
		if externalJobID.Valid {
			job.Scheduler.ExternalJobID = externalJobID.String
		}
		if rawState.Valid {
			job.Scheduler.RawState = rawState.String
		}
		if partition.Valid {
			job.Scheduler.Partition = partition.String
		}
		if account.Valid {
			job.Scheduler.Account = account.String
		}
		if qos.Valid {
			job.Scheduler.QoS = qos.String
		}
		if submitTime.Valid {
			t := submitTime.Time
			job.Scheduler.SubmitTime = &t
		}
		if priority.Valid {
			p := priority.Int64
			job.Scheduler.Priority = &p
		}
		if exitCode.Valid {
			code := int(exitCode.Int64)
			job.Scheduler.ExitCode = &code
		}
		if stateReason.Valid {
			job.Scheduler.StateReason = stateReason.String
		}
		if timeLimitMins.Valid {
			limit := int(timeLimitMins.Int64)
			job.Scheduler.TimeLimitMins = &limit
		}
	}

	return &job, nil
}

// prepareJobArgs prepares job fields for INSERT/UPDATE, handling nulls and conversion.
func prepareJobArgs(job *domain.Job) []any {
	nodesStr := strings.Join(job.Nodes, ",")
	gpuDevicesStr := strings.Join(job.GPUDevices, ",")

	// Extract scheduler fields (nil if no scheduler)
	var externalJobID, schedulerType, rawState, partition, account, qos, stateReason any
	var submitTime any
	var priority, exitCode, timeLimitMins any

	if job.Scheduler != nil {
		if job.Scheduler.ExternalJobID != "" {
			externalJobID = job.Scheduler.ExternalJobID
		}
		if job.Scheduler.Type != "" {
			schedulerType = string(job.Scheduler.Type)
		}
		if job.Scheduler.RawState != "" {
			rawState = job.Scheduler.RawState
		}
		if job.Scheduler.Partition != "" {
			partition = job.Scheduler.Partition
		}
		if job.Scheduler.Account != "" {
			account = job.Scheduler.Account
		}
		if job.Scheduler.QoS != "" {
			qos = job.Scheduler.QoS
		}
		if job.Scheduler.SubmitTime != nil {
			submitTime = job.Scheduler.SubmitTime
		}
		if job.Scheduler.Priority != nil {
			priority = job.Scheduler.Priority
		}
		if job.Scheduler.ExitCode != nil {
			exitCode = job.Scheduler.ExitCode
		}
		if job.Scheduler.StateReason != "" {
			stateReason = job.Scheduler.StateReason
		}
		if job.Scheduler.TimeLimitMins != nil {
			timeLimitMins = job.Scheduler.TimeLimitMins
		}
	}

	// Prepare cgroup and GPU fields
	var cgroupPath, gpuVendor any
	var gpuCountVal any
	if job.CgroupPath != "" {
		cgroupPath = job.CgroupPath
	}
	if job.GPUCount > 0 {
		gpuCountVal = job.GPUCount
	}
	if job.GPUVendor != "" && job.GPUVendor != domain.GPUVendorNone {
		gpuVendor = string(job.GPUVendor)
	}
	var gpuDevices any
	if len(job.GPUDevices) > 0 {
		gpuDevices = gpuDevicesStr
	}

	return []any{
		job.ID, job.User, nodesStr, job.NodeCount, string(job.State), job.StartTime, job.EndTime, job.RuntimeSeconds,
		job.CPUUsage, job.MemoryUsageMB, job.GPUUsage,
		externalJobID, schedulerType, rawState, partition, account, qos, priority, submitTime, exitCode, stateReason, timeLimitMins,
		job.RequestedCPUs, job.AllocatedCPUs, job.RequestedMemMB, job.AllocatedMemMB, job.RequestedGPUs, job.AllocatedGPUs,
		job.ClusterName, job.SchedulerInst, job.IngestVersion,
		job.LastSampleAt, job.SampleCount, job.AvgCPUUsage, job.MaxCPUUsage, job.MaxMemUsageMB, job.AvgGPUUsage, job.MaxGPUUsage,
		cgroupPath, gpuCountVal, gpuVendor, gpuDevices,
		job.CreatedAt, job.UpdatedAt,
	}
}

// getAuditInfo extracts audit info from job or context.
func getAuditInfo(ctx context.Context, job *domain.Job) (*domain.AuditInfo, error) {
	if job.Audit != nil {
		if job.Audit.ChangedBy == "" || job.Audit.Source == "" || job.Audit.CorrelationID == "" {
			return nil, storage.ErrInvalidInput
		}
		return job.Audit, nil
	}

	// Try to extract from context (if handlers set it)
	// For now, return error if not present in job
	return nil, storage.ErrInvalidInput
}

// marshalJobSnapshot converts a job to JSON for audit logging.
func marshalJobSnapshot(job *domain.Job) ([]byte, error) {
	snapshot := &domain.JobSnapshot{
		ID:             job.ID,
		User:           job.User,
		Nodes:          job.Nodes,
		NodeCount:      job.NodeCount,
		State:          job.State,
		StartTime:      job.StartTime,
		EndTime:        job.EndTime,
		RuntimeSeconds: job.RuntimeSeconds,
		CPUUsage:       job.CPUUsage,
		MemoryUsageMB:  job.MemoryUsageMB,
		GPUUsage:       job.GPUUsage,
		RequestedCPUs:  job.RequestedCPUs,
		AllocatedCPUs:  job.AllocatedCPUs,
		RequestedMemMB: job.RequestedMemMB,
		AllocatedMemMB: job.AllocatedMemMB,
		RequestedGPUs:  job.RequestedGPUs,
		AllocatedGPUs:  job.AllocatedGPUs,
		ClusterName:    job.ClusterName,
		SchedulerInst:  job.SchedulerInst,
		IngestVersion:  job.IngestVersion,
		LastSampleAt:   job.LastSampleAt,
		SampleCount:    job.SampleCount,
		AvgCPUUsage:    job.AvgCPUUsage,
		MaxCPUUsage:    job.MaxCPUUsage,
		MaxMemUsageMB:  job.MaxMemUsageMB,
		AvgGPUUsage:    job.AvgGPUUsage,
		MaxGPUUsage:    job.MaxGPUUsage,
		CgroupPath:     job.CgroupPath,
		GPUCount:       job.GPUCount,
		GPUVendor:      job.GPUVendor,
		GPUDevices:     job.GPUDevices,
		Scheduler:      job.Scheduler,
		CreatedAt:      job.CreatedAt,
		UpdatedAt:      job.UpdatedAt,
	}
	return json.Marshal(snapshot)
}

// CreateJob creates a new job with audit logging.
func (s *jobStore) CreateJob(ctx context.Context, job *domain.Job) (err error) {
	defer s.store.observe("create_job", time.Now(), &err)

	auditInfo, err := getAuditInfo(ctx, job)
	if err != nil {
		return err
	}

	// Normalize job data
	if job.NodeCount == 0 && len(job.Nodes) > 0 {
		job.NodeCount = len(job.Nodes)
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = time.Now()
	}
	job.UpdatedAt = time.Now()
	if job.StartTime.IsZero() {
		job.StartTime = time.Now()
	}
	if job.State == "" {
		job.State = domain.JobStatePending
	}

	// Check if job already exists
	var exists bool
	err = s.db.QueryRow(ctx, queryJobExists, job.ID).Scan(&exists)
	if err != nil {
		return mapPgError(err)
	}
	if exists {
		return storage.ErrConflict
	}

	// Insert job
	args := prepareJobArgs(job)
	_, err = s.db.Exec(ctx, queryInsertJob, args...)
	if err != nil {
		return mapPgError(err)
	}

	// Insert audit event
	auditEvent := &domain.JobAuditEvent{
		JobID:         job.ID,
		ChangeType:    "create",
		ChangedAt:     job.CreatedAt,
		ChangedBy:     auditInfo.ChangedBy,
		Source:        auditInfo.Source,
		CorrelationID: auditInfo.CorrelationID,
		Snapshot:      buildJobSnapshot(job),
	}
	auditStore := &auditStore{db: s.db, store: s.store}
	if err = auditStore.RecordAuditEvent(ctx, auditEvent); err != nil {
		return err
	}

	return nil
}

// GetJob retrieves a job by ID.
func (s *jobStore) GetJob(ctx context.Context, id string) (job *domain.Job, err error) {
	defer s.store.observe("get_job", time.Now(), &err)

	row := s.db.QueryRow(ctx, queryGetJob, id)
	job, err = scanJobRow(row)
	return job, err
}

// UpdateJob updates an existing job with terminal state validation.
func (s *jobStore) UpdateJob(ctx context.Context, job *domain.Job) (err error) {
	defer s.store.observe("update_job", time.Now(), &err)

	auditInfo, err := getAuditInfo(ctx, job)
	if err != nil {
		return err
	}

	// Normalize job data
	if job.NodeCount == 0 && len(job.Nodes) > 0 {
		job.NodeCount = len(job.Nodes)
	}

	// Check if existing job is in terminal state (data integrity invariant)
	existingJob, err := s.GetJob(ctx, job.ID)
	if err != nil {
		return err
	}
	if existingJob.State.IsTerminal() {
		return storage.ErrJobTerminal
	}

	job.UpdatedAt = time.Now()

	// Calculate runtime if job is completing
	if job.State == domain.JobStateCompleted || job.State == domain.JobStateFailed || job.State == domain.JobStateCancelled {
		if job.EndTime == nil {
			now := time.Now()
			job.EndTime = &now
		}
		job.RuntimeSeconds = job.EndTime.Sub(job.StartTime).Seconds()
	}

	// Prepare args for UPDATE (id is first param, then all fields except id)
	nodesStr := strings.Join(job.Nodes, ",")
	gpuDevicesStr := strings.Join(job.GPUDevices, ",")

	var externalJobID, schedulerType, rawState, partition, account, qos, stateReason any
	var submitTime any
	var priority, exitCode, timeLimitMins any

	if job.Scheduler != nil {
		if job.Scheduler.ExternalJobID != "" {
			externalJobID = job.Scheduler.ExternalJobID
		}
		if job.Scheduler.Type != "" {
			schedulerType = string(job.Scheduler.Type)
		}
		if job.Scheduler.RawState != "" {
			rawState = job.Scheduler.RawState
		}
		if job.Scheduler.Partition != "" {
			partition = job.Scheduler.Partition
		}
		if job.Scheduler.Account != "" {
			account = job.Scheduler.Account
		}
		if job.Scheduler.QoS != "" {
			qos = job.Scheduler.QoS
		}
		if job.Scheduler.SubmitTime != nil {
			submitTime = job.Scheduler.SubmitTime
		}
		if job.Scheduler.Priority != nil {
			priority = job.Scheduler.Priority
		}
		if job.Scheduler.ExitCode != nil {
			exitCode = job.Scheduler.ExitCode
		}
		if job.Scheduler.StateReason != "" {
			stateReason = job.Scheduler.StateReason
		}
		if job.Scheduler.TimeLimitMins != nil {
			timeLimitMins = job.Scheduler.TimeLimitMins
		}
	}

	var cgroupPath, gpuVendor any
	var gpuCountVal any
	if job.CgroupPath != "" {
		cgroupPath = job.CgroupPath
	}
	if job.GPUCount > 0 {
		gpuCountVal = job.GPUCount
	}
	if job.GPUVendor != "" && job.GPUVendor != domain.GPUVendorNone {
		gpuVendor = string(job.GPUVendor)
	}
	var gpuDevices any
	if len(job.GPUDevices) > 0 {
		gpuDevices = gpuDevicesStr
	}

	args := []any{
		job.ID, // $1 for WHERE clause
		job.User, nodesStr, job.NodeCount, string(job.State), job.StartTime, job.EndTime, job.RuntimeSeconds,
		job.CPUUsage, job.MemoryUsageMB, job.GPUUsage,
		externalJobID, schedulerType, rawState, partition, account, qos, priority, submitTime, exitCode, stateReason, timeLimitMins,
		job.RequestedCPUs, job.AllocatedCPUs, job.RequestedMemMB, job.AllocatedMemMB, job.RequestedGPUs, job.AllocatedGPUs,
		job.ClusterName, job.SchedulerInst, job.IngestVersion,
		job.LastSampleAt, job.SampleCount, job.AvgCPUUsage, job.MaxCPUUsage, job.MaxMemUsageMB, job.AvgGPUUsage, job.MaxGPUUsage,
		cgroupPath, gpuCountVal, gpuVendor, gpuDevices,
		job.UpdatedAt,
	}

	tag, err := s.db.Exec(ctx, queryUpdateJob, args...)
	if err != nil {
		return mapPgError(err)
	}
	if tag.RowsAffected() == 0 {
		return storage.ErrNotFound
	}

	// Insert audit event
	auditEvent := &domain.JobAuditEvent{
		JobID:         job.ID,
		ChangeType:    "update",
		ChangedAt:     job.UpdatedAt,
		ChangedBy:     auditInfo.ChangedBy,
		Source:        auditInfo.Source,
		CorrelationID: auditInfo.CorrelationID,
		Snapshot:      buildJobSnapshot(job),
	}
	auditStore := &auditStore{db: s.db, store: s.store}
	if err = auditStore.RecordAuditEvent(ctx, auditEvent); err != nil {
		return err
	}

	return nil
}

// UpsertJob creates or updates a job with terminal state protection.
func (s *jobStore) UpsertJob(ctx context.Context, job *domain.Job) (err error) {
	defer s.store.observe("upsert_job", time.Now(), &err)

	auditInfo, err := getAuditInfo(ctx, job)
	if err != nil {
		return err
	}

	// Normalize job data
	if job.NodeCount == 0 && len(job.Nodes) > 0 {
		job.NodeCount = len(job.Nodes)
	}

	// Check if existing job is in terminal state
	existingJob, err := s.GetJob(ctx, job.ID)
	if err == nil && existingJob.State.IsTerminal() {
		return storage.ErrJobTerminal
	}
	// If not found, that's okay - we'll insert

	if job.CreatedAt.IsZero() {
		job.CreatedAt = time.Now()
	}
	job.UpdatedAt = time.Now()
	if job.StartTime.IsZero() {
		job.StartTime = time.Now()
	}
	if job.State == "" {
		job.State = domain.JobStatePending
	}

	// Calculate runtime if job is completing
	if job.State == domain.JobStateCompleted || job.State == domain.JobStateFailed || job.State == domain.JobStateCancelled {
		if job.EndTime == nil {
			now := time.Now()
			job.EndTime = &now
		}
		job.RuntimeSeconds = job.EndTime.Sub(job.StartTime).Seconds()
	}

	args := prepareJobArgs(job)
	_, err = s.db.Exec(ctx, queryUpsertJob, args...)
	if err != nil {
		return mapPgError(err)
	}

	// Insert audit event
	changeType := "upsert"
	if existingJob == nil {
		changeType = "create"
	} else {
		changeType = "update"
	}
	auditEvent := &domain.JobAuditEvent{
		JobID:         job.ID,
		ChangeType:    changeType,
		ChangedAt:     job.UpdatedAt,
		ChangedBy:     auditInfo.ChangedBy,
		Source:        auditInfo.Source,
		CorrelationID: auditInfo.CorrelationID,
		Snapshot:      buildJobSnapshot(job),
	}
	auditStore := &auditStore{db: s.db, store: s.store}
	if err = auditStore.RecordAuditEvent(ctx, auditEvent); err != nil {
		return err
	}

	return nil
}

// DeleteJob deletes a job by ID with audit logging.
func (s *jobStore) DeleteJob(ctx context.Context, id string) (err error) {
	defer s.store.observe("delete_job", time.Now(), &err)

	// TODO: Get job for audit before deletion and record audit event
	// Currently DELETE operations don't support audit logging as there's no job object passed in.
	// Consider adding DeleteJobWithAudit(ctx, id, auditInfo) method.

	tag, err := s.db.Exec(ctx, queryDeleteJob, id)
	if err != nil {
		return mapPgError(err)
	}
	if tag.RowsAffected() == 0 {
		return storage.ErrNotFound
	}

	return nil
}

// ListJobs retrieves jobs matching the filter with pagination.
func (s *jobStore) ListJobs(ctx context.Context, filter domain.JobFilter) (jobs []*domain.Job, total int, err error) {
	defer s.store.observe("list_jobs", time.Now(), &err)

	// Build dynamic WHERE clause
	var conditions []string
	var args []any
	argPos := 1

	if filter.State != nil {
		conditions = append(conditions, fmt.Sprintf("state = $%d", argPos))
		args = append(args, string(*filter.State))
		argPos++
	}
	if filter.User != nil {
		conditions = append(conditions, fmt.Sprintf("user_name = $%d", argPos))
		args = append(args, *filter.User)
		argPos++
	}
	if filter.Node != nil {
		conditions = append(conditions, fmt.Sprintf("nodes LIKE $%d", argPos))
		args = append(args, "%"+*filter.Node+"%")
		argPos++
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = " WHERE " + strings.Join(conditions, " AND ")
	}

	// Get total count
	countQuery := queryCountJobsBase + whereClause
	err = s.db.QueryRow(ctx, countQuery, args...).Scan(&total)
	if err != nil {
		return nil, 0, mapPgError(err)
	}

	// Build query with pagination
	query := queryListJobsBase + whereClause + " ORDER BY created_at DESC"
	if filter.Limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argPos)
		args = append(args, filter.Limit)
		argPos++
	}
	if filter.Offset > 0 {
		query += fmt.Sprintf(" OFFSET $%d", argPos)
		args = append(args, filter.Offset)
		argPos++
	}

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, mapPgError(err)
	}
	defer rows.Close()

	jobs = make([]*domain.Job, 0)
	for rows.Next() {
		job, err := scanJobRow(rows)
		if err != nil {
			return nil, 0, err
		}
		jobs = append(jobs, job)
	}

	if err = rows.Err(); err != nil {
		return nil, 0, mapPgError(err)
	}

	return jobs, total, nil
}

// GetAllJobs retrieves all jobs without pagination.
func (s *jobStore) GetAllJobs(ctx context.Context) (jobs []*domain.Job, err error) {
	defer s.store.observe("get_all_jobs", time.Now(), &err)

	query := queryListJobsBase + " ORDER BY created_at DESC"
	rows, err := s.db.Query(ctx, query)
	if err != nil {
		return nil, mapPgError(err)
	}
	defer rows.Close()

	jobs = make([]*domain.Job, 0)
	for rows.Next() {
		job, err := scanJobRow(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}

	if err = rows.Err(); err != nil {
		return nil, mapPgError(err)
	}

	return jobs, nil
}

// buildJobSnapshot creates a JobSnapshot from a Job for audit logging.
func buildJobSnapshot(job *domain.Job) *domain.JobSnapshot {
	if job == nil {
		return nil
	}

	snapshot := &domain.JobSnapshot{
		ID:             job.ID,
		User:           job.User,
		Nodes:          append([]string(nil), job.Nodes...),
		NodeCount:      job.NodeCount,
		State:          job.State,
		StartTime:      job.StartTime,
		EndTime:        job.EndTime,
		RuntimeSeconds: job.RuntimeSeconds,
		CPUUsage:       job.CPUUsage,
		MemoryUsageMB:  job.MemoryUsageMB,
		GPUUsage:       job.GPUUsage,
		RequestedCPUs:  job.RequestedCPUs,
		AllocatedCPUs:  job.AllocatedCPUs,
		RequestedMemMB: job.RequestedMemMB,
		AllocatedMemMB: job.AllocatedMemMB,
		RequestedGPUs:  job.RequestedGPUs,
		AllocatedGPUs:  job.AllocatedGPUs,
		ClusterName:    job.ClusterName,
		SchedulerInst:  job.SchedulerInst,
		IngestVersion:  job.IngestVersion,
		LastSampleAt:   job.LastSampleAt,
		SampleCount:    job.SampleCount,
		AvgCPUUsage:    job.AvgCPUUsage,
		MaxCPUUsage:    job.MaxCPUUsage,
		MaxMemUsageMB:  job.MaxMemUsageMB,
		AvgGPUUsage:    job.AvgGPUUsage,
		MaxGPUUsage:    job.MaxGPUUsage,
		CgroupPath:     job.CgroupPath,
		GPUCount:       job.GPUCount,
		GPUVendor:      job.GPUVendor,
		GPUDevices:     append([]string(nil), job.GPUDevices...),
		Scheduler:      job.Scheduler,
		CreatedAt:      job.CreatedAt,
		UpdatedAt:      job.UpdatedAt,
	}

	return snapshot
}
