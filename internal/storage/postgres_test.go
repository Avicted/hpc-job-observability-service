package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/domain"
	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func testAuditContext() context.Context {
	return WithAuditInfo(context.Background(), NewAuditInfo("test", "storage-test"))
}

func jobRowColumns() []string {
	return []string{
		"id", "user_name", "nodes", "node_count", "state", "start_time", "end_time", "runtime_seconds",
		"cpu_usage", "memory_usage_mb", "gpu_usage",
		"external_job_id", "scheduler_type", "raw_state", "partition", "account", "qos", "priority", "submit_time", "exit_code", "state_reason", "time_limit_minutes",
		"requested_cpus", "allocated_cpus", "requested_memory_mb", "allocated_memory_mb", "requested_gpus", "allocated_gpus",
		"cluster_name", "scheduler_instance", "ingest_version",
		"last_sample_at", "sample_count", "avg_cpu_usage", "max_cpu_usage", "max_memory_usage_mb", "avg_gpu_usage", "max_gpu_usage",
		"cgroup_path", "gpu_count", "gpu_vendor", "gpu_devices",
		"created_at", "updated_at",
	}
}

func jobRowValues(id string, state domain.JobState, start time.Time, end *time.Time) []driver.Value {
	var endVal driver.Value
	if end != nil {
		endVal = *end
	}
	return []driver.Value{
		id, "user", "node-1", int64(1), state, start, endVal, float64(0),
		float64(0), int64(0), nil,
		"", "", "", "", "", "", nil, nil, nil, "", nil,
		int64(0), int64(0), int64(0), int64(0), int64(0), int64(0),
		"default", "test", "test",
		nil, int64(0), float64(0), float64(0), int64(0), float64(0), float64(0),
		nil, nil, nil, nil, // cgroup_path, gpu_count, gpu_vendor, gpu_devices
		time.Now(), time.Now(),
	}
}

func TestPostgresStorage_CreateJob_Duplicate(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT EXISTS\\(SELECT 1 FROM jobs WHERE id = \\$1\\)").
		WithArgs("job-1").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectRollback()

	err = store.CreateJob(testAuditContext(), &domain.Job{ID: "job-1"})
	if !errors.Is(err, domain.ErrJobAlreadyExists) {
		t.Fatalf("expected domain.ErrJobAlreadyExists, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_CreateJob_InsertError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}
	job := &domain.Job{ID: "job-2", User: "alice", Nodes: []string{"node-1"}, State: domain.JobStateRunning, StartTime: time.Now()}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT EXISTS\\(SELECT 1 FROM jobs WHERE id = \\$1\\)").
		WithArgs("job-2").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	mock.ExpectExec("INSERT INTO jobs").
		WillReturnError(errors.New("insert failed"))
	mock.ExpectRollback()

	err = store.CreateJob(testAuditContext(), job)
	if err == nil || err.Error() != "insert failed" {
		t.Fatalf("expected insert error, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_CreateJob_ExistsQueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT EXISTS\\(SELECT 1 FROM jobs WHERE id = \\$1\\)").
		WithArgs("job-err").
		WillReturnError(errors.New("query failed"))
	mock.ExpectRollback()

	err = store.CreateJob(testAuditContext(), &domain.Job{ID: "job-err"})
	if err == nil || err.Error() != "query failed" {
		t.Fatalf("expected query error, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_GetJob_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	mock.ExpectQuery("SELECT id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,").
		WithArgs("missing").
		WillReturnError(sql.ErrNoRows)

	_, err = store.GetJob(context.Background(), "missing")
	if !errors.Is(err, domain.ErrJobNotFound) {
		t.Fatalf("expected domain.ErrJobNotFound, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_GetJob_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	mock.ExpectQuery("SELECT id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,").
		WithArgs("job-err").
		WillReturnError(errors.New("db error"))

	_, err = store.GetJob(context.Background(), "job-err")
	if err == nil || err.Error() != "db error" {
		t.Fatalf("expected db error, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_GetJob_WithNulls(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	rows := sqlmock.NewRows(jobRowColumns()).AddRow(jobRowValues("job-3", domain.JobStateRunning, time.Now(), nil)...)

	mock.ExpectQuery("SELECT id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,").
		WithArgs("job-3").
		WillReturnRows(rows)

	job, err := store.GetJob(context.Background(), "job-3")
	if err != nil {
		t.Fatalf("GetJob error: %v", err)
	}
	if job.EndTime != nil {
		t.Fatal("expected nil EndTime")
	}
	if job.GPUUsage != nil {
		t.Fatal("expected nil GPUUsage")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_UpdateJob_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	job := &domain.Job{ID: "job-4", User: "bob", Nodes: []string{"node-1"}, State: domain.JobStateCompleted, StartTime: time.Now().Add(-time.Hour)}

	mock.ExpectBegin()
	// UpdateJob now calls getJobByIDTx first to check terminal state
	mock.ExpectQuery("SELECT id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,").
		WithArgs("job-4").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	err = store.UpdateJob(testAuditContext(), job)
	if !errors.Is(err, domain.ErrJobNotFound) {
		t.Fatalf("expected domain.ErrJobNotFound, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_UpdateJob_ExecError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	startTime := time.Now()
	job := &domain.Job{ID: "job-5", User: "bob", Nodes: []string{"node-1"}, State: domain.JobStateRunning, StartTime: startTime}

	mock.ExpectBegin()
	// UpdateJob now calls getJobByIDTx first to check terminal state; return a running job
	mock.ExpectQuery("SELECT id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,").
		WithArgs("job-5").
		WillReturnRows(sqlmock.NewRows(jobRowColumns()).AddRow(jobRowValues("job-5", domain.JobStateRunning, startTime, nil)...))
	mock.ExpectExec("UPDATE jobs SET").
		WillReturnError(errors.New("update failed"))
	mock.ExpectRollback()

	err = store.UpdateJob(testAuditContext(), job)
	if err == nil || err.Error() != "update failed" {
		t.Fatalf("expected update error, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_DeleteJob_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,").
		WithArgs("missing").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	err = store.DeleteJob(testAuditContext(), "missing")
	if !errors.Is(err, domain.ErrJobNotFound) {
		t.Fatalf("expected domain.ErrJobNotFound, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_DeleteJob_ExecError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	rows := sqlmock.NewRows(jobRowColumns()).AddRow(jobRowValues("job-err", domain.JobStateRunning, time.Now(), nil)...)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,").
		WithArgs("job-err").
		WillReturnRows(rows)
	mock.ExpectExec("DELETE FROM jobs WHERE id = \\$1").
		WithArgs("job-err").
		WillReturnError(errors.New("delete failed"))
	mock.ExpectRollback()

	err = store.DeleteJob(testAuditContext(), "job-err")
	if err == nil || err.Error() != "delete failed" {
		t.Fatalf("expected delete error, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_GetJobMetrics_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	mock.ExpectQuery("SELECT EXISTS\\(SELECT 1 FROM jobs WHERE id = \\$1\\)").
		WithArgs("missing").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	_, _, err = store.GetJobMetrics(context.Background(), "missing", domain.MetricsFilter{Limit: 10})
	if !errors.Is(err, domain.ErrJobNotFound) {
		t.Fatalf("expected domain.ErrJobNotFound, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_GetLatestMetrics_NoRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	mock.ExpectQuery("SELECT id, job_id, timestamp, cpu_usage, memory_usage_mb, gpu_usage").
		WithArgs("job-1").
		WillReturnError(sql.ErrNoRows)

	sample, err := store.GetLatestMetrics(context.Background(), "job-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sample != nil {
		t.Fatalf("expected nil sample, got %v", sample)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestNewPostgresStorage_InvalidDSN(t *testing.T) {
	store, err := NewPostgresStorage("postgres://invalid:invalid@localhost:1/db?sslmode=disable&connect_timeout=1")
	if err == nil {
		if store != nil {
			store.Close()
		}
		t.Fatal("expected error for invalid DSN")
	}
}

func TestPostgresStorage_DeleteJob_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	rows := sqlmock.NewRows(jobRowColumns()).AddRow(jobRowValues("job-1", domain.JobStateRunning, time.Now(), nil)...)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,").
		WithArgs("job-1").
		WillReturnRows(rows)
	mock.ExpectExec("DELETE FROM jobs WHERE id = \\$1").
		WithArgs("job-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO job_audit_events").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := store.DeleteJob(testAuditContext(), "job-1"); err != nil {
		t.Fatalf("DeleteJob error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_GetLatestMetrics_WithGPU(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	metricRows := sqlmock.NewRows([]string{
		"id", "job_id", "timestamp", "cpu_usage", "memory_usage_mb", "gpu_usage",
	}).AddRow(1, "job-1", time.Now(), 20.0, int64(512), 33.3)

	mock.ExpectQuery("SELECT id, job_id, timestamp, cpu_usage, memory_usage_mb, gpu_usage").
		WithArgs("job-1").
		WillReturnRows(metricRows)

	sample, err := store.GetLatestMetrics(context.Background(), "job-1")
	if err != nil {
		t.Fatalf("GetLatestMetrics error: %v", err)
	}
	if sample == nil || sample.GPUUsage == nil {
		t.Fatal("expected GPU usage to be set")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_Migrate(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS jobs").
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := store.Migrate(); err != nil {
		t.Fatalf("Migrate error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_ListJobs(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	state := domain.JobStateRunning
	user := "alice"
	node := "node-1"
	filter := domain.JobFilter{State: &state, User: &user, Node: &node, Limit: 5, Offset: 1}

	countRows := sqlmock.NewRows([]string{"count"}).AddRow(1)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM jobs WHERE state = \\$1 AND user_name = \\$2 AND nodes LIKE \\$3").
		WithArgs(state, user, "%"+node+"%").
		WillReturnRows(countRows)

	jobRows := sqlmock.NewRows(jobRowColumns()).AddRow(jobRowValues("job-1", domain.JobStateRunning, time.Now(), nil)...)

	mock.ExpectQuery("SELECT id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,").
		WithArgs(state, user, "%"+node+"%", filter.Limit, filter.Offset).
		WillReturnRows(jobRows)

	jobs, total, err := store.ListJobs(context.Background(), filter)
	if err != nil {
		t.Fatalf("ListJobs error: %v", err)
	}
	if total != 1 {
		t.Fatalf("expected total 1, got %d", total)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_ListJobs_LimitDefaultAndCap(t *testing.T) {
	for _, tc := range []struct {
		name     string
		limit    int
		expected int
	}{
		{"default", 0, 100},
		{"cap", 5000, 1000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("failed to create sqlmock: %v", err)
			}
			defer db.Close()

			store := &PostgresStorage{baseStorage: baseStorage{db: db}}

			countRows := sqlmock.NewRows([]string{"count"}).AddRow(0)
			mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM jobs").
				WillReturnRows(countRows)

			jobRows := sqlmock.NewRows(jobRowColumns())

			mock.ExpectQuery("SELECT id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,").
				WithArgs(tc.expected, 0).
				WillReturnRows(jobRows)

			_, _, err = store.ListJobs(context.Background(), domain.JobFilter{Limit: tc.limit})
			if err != nil {
				t.Fatalf("ListJobs error: %v", err)
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet expectations: %v", err)
			}
		})
	}
}

func TestPostgresStorage_GetAllJobs(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	jobRows := sqlmock.NewRows(jobRowColumns()).AddRow(jobRowValues("job-1", domain.JobStateRunning, time.Now(), nil)...)

	mock.ExpectQuery("SELECT id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,").
		WillReturnRows(jobRows)

	jobs, err := store.GetAllJobs(context.Background())
	if err != nil {
		t.Fatalf("GetAllJobs error: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_RecordMetrics(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	rows := sqlmock.NewRows([]string{"id"}).AddRow(int64(10))
	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO metric_samples").
		WillReturnRows(rows)
	mock.ExpectExec("UPDATE jobs SET").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	sample := &domain.MetricSample{JobID: "job-1", CPUUsage: 10, MemoryUsageMB: 256}
	if err := store.RecordMetrics(context.Background(), sample); err != nil {
		t.Fatalf("RecordMetrics error: %v", err)
	}
	if sample.ID == 0 {
		t.Fatal("expected sample ID to be set")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_GetJobMetrics(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	start := time.Now().Add(-time.Hour)
	end := time.Now()
	filter := domain.MetricsFilter{StartTime: &start, EndTime: &end, Limit: 2}

	mock.ExpectQuery("SELECT EXISTS\\(SELECT 1 FROM jobs WHERE id = \\$1\\)").
		WithArgs("job-1").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	countRows := sqlmock.NewRows([]string{"count"}).AddRow(2)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM metric_samples WHERE job_id = \\$1 AND timestamp >= \\$2 AND timestamp <= \\$3").
		WithArgs("job-1", start, end).
		WillReturnRows(countRows)

	metricRows := sqlmock.NewRows([]string{
		"id", "job_id", "timestamp", "cpu_usage", "memory_usage_mb", "gpu_usage",
	}).AddRow(1, "job-1", time.Now(), 20.0, int64(512), nil)

	mock.ExpectQuery("SELECT id, job_id, timestamp, cpu_usage, memory_usage_mb, gpu_usage").
		WithArgs("job-1", start, end, filter.Limit).
		WillReturnRows(metricRows)

	samples, total, err := store.GetJobMetrics(context.Background(), "job-1", filter)
	if err != nil {
		t.Fatalf("GetJobMetrics error: %v", err)
	}
	if total != 2 {
		t.Fatalf("expected total 2, got %d", total)
	}
	if len(samples) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(samples))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_GetJobMetrics_LimitDefault(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	mock.ExpectQuery("SELECT EXISTS\\(SELECT 1 FROM jobs WHERE id = \\$1\\)").
		WithArgs("job-1").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	countRows := sqlmock.NewRows([]string{"count"}).AddRow(0)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM metric_samples WHERE job_id = \\$1").
		WithArgs("job-1").
		WillReturnRows(countRows)

	metricRows := sqlmock.NewRows([]string{
		"id", "job_id", "timestamp", "cpu_usage", "memory_usage_mb", "gpu_usage",
	})

	mock.ExpectQuery("SELECT id, job_id, timestamp, cpu_usage, memory_usage_mb, gpu_usage").
		WithArgs("job-1", 1000).
		WillReturnRows(metricRows)

	_, _, err = store.GetJobMetrics(context.Background(), "job-1", domain.MetricsFilter{Limit: 0})
	if err != nil {
		t.Fatalf("GetJobMetrics error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_DeleteMetricsBefore(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	cutoff := time.Now().Add(-time.Hour)
	mock.ExpectExec("DELETE FROM metric_samples WHERE timestamp < \\$1").
		WithArgs(cutoff).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := store.DeleteMetricsBefore(cutoff); err != nil {
		t.Fatalf("DeleteMetricsBefore error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// --- Helper function tests ---

func TestFloatPtr(t *testing.T) {
	result := floatPtr(3.14)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if *result != 3.14 {
		t.Errorf("*result = %v, want 3.14", *result)
	}
}

func TestClamp(t *testing.T) {
	tests := []struct {
		name          string
		val, min, max float64
		expected      float64
	}{
		{"within range", 50.0, 0.0, 100.0, 50.0},
		{"below min", -10.0, 0.0, 100.0, 0.0},
		{"above max", 150.0, 0.0, 100.0, 100.0},
		{"at min", 0.0, 0.0, 100.0, 0.0},
		{"at max", 100.0, 0.0, 100.0, 100.0},
		{"negative range", -5.0, -10.0, -1.0, -5.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := clamp(tt.val, tt.min, tt.max)
			if result != tt.expected {
				t.Errorf("clamp(%v, %v, %v) = %v, want %v", tt.val, tt.min, tt.max, result, tt.expected)
			}
		})
	}
}

func TestMax(t *testing.T) {
	tests := []struct {
		name     string
		a, b     int64
		expected int64
	}{
		{"a greater", 10, 5, 10},
		{"b greater", 5, 10, 10},
		{"equal", 7, 7, 7},
		{"negative a greater", -5, -10, -5},
		{"negative b greater", -10, -5, -5},
		{"zero and positive", 0, 5, 5},
		{"zero and negative", 0, -5, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := max(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("max(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestNewAuditInfoWithCorrelation(t *testing.T) {
	info := NewAuditInfoWithCorrelation("  user1  ", "  source1  ", "  corr-123  ")

	if info.ChangedBy != "user1" {
		t.Errorf("ChangedBy = %q, want user1", info.ChangedBy)
	}
	if info.Source != "source1" {
		t.Errorf("Source = %q, want source1", info.Source)
	}
	if info.CorrelationID != "corr-123" {
		t.Errorf("CorrelationID = %q, want corr-123", info.CorrelationID)
	}
}

func TestPostgresStorage_UpsertJob_NewJob(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	job := &domain.Job{
		ID:    "new-job-1",
		User:  "alice",
		Nodes: []string{"node-1"},
		State: domain.JobStateRunning,
	}

	mock.ExpectBegin()
	// getJobByIDTx returns not found for new job
	mock.ExpectQuery("SELECT id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,").
		WithArgs("new-job-1").
		WillReturnError(sql.ErrNoRows)
	// Insert/update executes
	mock.ExpectExec("INSERT INTO jobs").
		WillReturnResult(sqlmock.NewResult(1, 1))
	// Audit event is written
	mock.ExpectExec("INSERT INTO job_audit_events").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err = store.UpsertJob(testAuditContext(), job)
	if err != nil {
		t.Fatalf("UpsertJob error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_UpsertJob_TerminalState(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	endTime := time.Now().Add(-time.Hour)
	job := &domain.Job{
		ID:    "terminal-job-1",
		User:  "alice",
		Nodes: []string{"node-1"},
		State: domain.JobStateRunning, // Trying to update to running
	}

	mock.ExpectBegin()
	// getJobByIDTx returns a completed job (terminal state)
	mock.ExpectQuery("SELECT id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,").
		WithArgs("terminal-job-1").
		WillReturnRows(sqlmock.NewRows(jobRowColumns()).AddRow(jobRowValues("terminal-job-1", domain.JobStateCompleted, time.Now().Add(-2*time.Hour), &endTime)...))
	// Should skip upsert and commit
	mock.ExpectCommit()

	err = store.UpsertJob(testAuditContext(), job)
	if err != nil {
		t.Fatalf("UpsertJob error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_UpsertJob_BeginError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	job := &domain.Job{
		ID:    "error-job-1",
		User:  "alice",
		Nodes: []string{"node-1"},
		State: domain.JobStateRunning,
	}

	mock.ExpectBegin().WillReturnError(errors.New("begin error"))

	err = store.UpsertJob(testAuditContext(), job)
	if err == nil || err.Error() != "begin error" {
		t.Fatalf("expected begin error, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_UpsertJob_ExecError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	job := &domain.Job{
		ID:    "exec-error-job",
		User:  "alice",
		Nodes: []string{"node-1"},
		State: domain.JobStateRunning,
	}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,").
		WithArgs("exec-error-job").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO jobs").
		WillReturnError(errors.New("exec error"))
	mock.ExpectRollback()

	err = store.UpsertJob(testAuditContext(), job)
	if err == nil || err.Error() != "exec error" {
		t.Fatalf("expected exec error, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_SeedDemoData(t *testing.T) {
	// SeedDemoData calls baseStorage.SeedDemoData which requires a lot of mocking
	// We test by verifying it doesn't panic with mocked inserts
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	// SeedDemoData creates 100 jobs - expect 100 CreateJob calls + metrics for each
	// For simplicity, we test that it handles errors properly
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT EXISTS").
		WithArgs(sqlmock.AnyArg()).
		WillReturnError(errors.New("test error"))
	mock.ExpectRollback()

	err = store.SeedDemoData()
	if err == nil {
		t.Fatal("expected error from SeedDemoData")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestBaseStorage_Close(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}

	store := &baseStorage{db: db}

	mock.ExpectClose()

	err = store.Close()
	if err != nil {
		t.Fatalf("Close error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestNew_UnknownType(t *testing.T) {
	_, err := New("unknown", "connection-string")
	if err == nil {
		t.Fatal("expected error for unknown storage type")
	}
	expectedMsg := "unsupported database type: unknown"
	if err.Error() != expectedMsg {
		t.Fatalf("expected error %q, got %q", expectedMsg, err.Error())
	}
}

func TestSeedJobMetrics(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &baseStorage{db: db}

	// Create a job that spans 2 minutes
	now := time.Now()
	startTime := now.Add(-2 * time.Minute)
	gpuUsage := 50.0
	job := &domain.Job{
		ID:            "metrics-job-1",
		User:          "alice",
		StartTime:     startTime,
		CPUUsage:      70.0,
		MemoryUsageMB: 4096,
		GPUUsage:      &gpuUsage,
	}

	// Expect 2 metric inserts (one per minute)
	mock.ExpectExec("INSERT INTO metric_samples").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO metric_samples").
		WillReturnResult(sqlmock.NewResult(2, 1))

	ctx := context.Background()
	err = store.seedJobMetrics(ctx, job, now)
	if err != nil {
		t.Fatalf("seedJobMetrics error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestSeedJobMetrics_WithEndTime(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &baseStorage{db: db}

	// Create a completed job with explicit end time
	now := time.Now()
	startTime := now.Add(-3 * time.Minute)
	endTime := now.Add(-1 * time.Minute)
	job := &domain.Job{
		ID:            "metrics-job-2",
		User:          "bob",
		StartTime:     startTime,
		EndTime:       &endTime,
		CPUUsage:      50.0,
		MemoryUsageMB: 2048,
	}

	// Expect 2 metric inserts (from startTime to endTime, which is 2 minutes)
	mock.ExpectExec("INSERT INTO metric_samples").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO metric_samples").
		WillReturnResult(sqlmock.NewResult(2, 1))

	ctx := context.Background()
	err = store.seedJobMetrics(ctx, job, now)
	if err != nil {
		t.Fatalf("seedJobMetrics error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestSeedJobMetrics_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &baseStorage{db: db}

	now := time.Now()
	startTime := now.Add(-2 * time.Minute)
	job := &domain.Job{
		ID:            "metrics-job-err",
		StartTime:     startTime,
		CPUUsage:      50.0,
		MemoryUsageMB: 1024,
	}

	// First insert fails
	mock.ExpectExec("INSERT INTO metric_samples").
		WillReturnError(errors.New("insert error"))

	ctx := context.Background()
	err = store.seedJobMetrics(ctx, job, now)
	if err == nil {
		t.Fatal("expected error from seedJobMetrics")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestValidateAuditInfo(t *testing.T) {
	tests := []struct {
		name    string
		info    *domain.AuditInfo
		wantErr bool
	}{
		{
			name:    "nil audit info",
			info:    nil,
			wantErr: true,
		},
		{
			name:    "empty ChangedBy",
			info:    &domain.AuditInfo{ChangedBy: "", Source: "test", CorrelationID: "123"},
			wantErr: true,
		},
		{
			name:    "empty Source",
			info:    &domain.AuditInfo{ChangedBy: "user", Source: "", CorrelationID: "123"},
			wantErr: true,
		},
		{
			name:    "empty CorrelationID",
			info:    &domain.AuditInfo{ChangedBy: "user", Source: "test", CorrelationID: ""},
			wantErr: true,
		},
		{
			name:    "whitespace only ChangedBy",
			info:    &domain.AuditInfo{ChangedBy: "   ", Source: "test", CorrelationID: "123"},
			wantErr: true,
		},
		{
			name:    "valid audit info",
			info:    &domain.AuditInfo{ChangedBy: "user", Source: "test", CorrelationID: "123"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAuditInfo(tt.info)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateAuditInfo() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAuditInfoFromJobOrContext(t *testing.T) {
	t.Run("from job audit", func(t *testing.T) {
		job := &domain.Job{
			ID: "test-job",
			Audit: &domain.AuditInfo{
				ChangedBy:     "job-user",
				Source:        "job-source",
				CorrelationID: "job-123",
			},
		}

		info, err := auditInfoFromJobOrContext(context.Background(), job)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if info.ChangedBy != "job-user" {
			t.Errorf("ChangedBy = %q, want job-user", info.ChangedBy)
		}
	})

	t.Run("from context", func(t *testing.T) {
		ctx := WithAuditInfo(context.Background(), &domain.AuditInfo{
			ChangedBy:     "ctx-user",
			Source:        "ctx-source",
			CorrelationID: "ctx-123",
		})
		job := &domain.Job{ID: "test-job"}

		info, err := auditInfoFromJobOrContext(ctx, job)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if info.ChangedBy != "ctx-user" {
			t.Errorf("ChangedBy = %q, want ctx-user", info.ChangedBy)
		}
		// Job should have audit info set
		if job.Audit == nil || job.Audit.ChangedBy != "ctx-user" {
			t.Error("job.Audit should be set from context")
		}
	})

	t.Run("no audit info", func(t *testing.T) {
		job := &domain.Job{ID: "test-job"}
		_, err := auditInfoFromJobOrContext(context.Background(), job)
		if !errors.Is(err, domain.ErrMissingAuditInfo) {
			t.Errorf("expected ErrMissingAuditInfo, got %v", err)
		}
	})

	t.Run("invalid job audit info", func(t *testing.T) {
		job := &domain.Job{
			ID: "test-job",
			Audit: &domain.AuditInfo{
				ChangedBy: "", // Invalid
				Source:    "test",
			},
		}
		_, err := auditInfoFromJobOrContext(context.Background(), job)
		if !errors.Is(err, domain.ErrMissingAuditInfo) {
			t.Errorf("expected ErrMissingAuditInfo, got %v", err)
		}
	})
}

func TestBuildJobSnapshot(t *testing.T) {
	t.Run("nil job", func(t *testing.T) {
		snapshot := buildJobSnapshot(nil)
		if snapshot != nil {
			t.Error("expected nil snapshot for nil job")
		}
	})

	t.Run("valid job", func(t *testing.T) {
		now := time.Now()
		gpuUsage := 75.5
		job := &domain.Job{
			ID:            "snap-job",
			User:          "alice",
			Nodes:         []string{"node1", "node2"},
			NodeCount:     2,
			State:         domain.JobStateRunning,
			StartTime:     now,
			CPUUsage:      50.0,
			MemoryUsageMB: 2048,
			GPUUsage:      &gpuUsage,
			RequestedCPUs: 8,
			AllocatedCPUs: 8,
			ClusterName:   "cluster1",
			GPUDevices:    []string{"gpu0", "gpu1"},
		}

		snapshot := buildJobSnapshot(job)
		if snapshot == nil {
			t.Fatal("expected non-nil snapshot")
		}
		if snapshot.ID != "snap-job" {
			t.Errorf("ID = %q, want snap-job", snapshot.ID)
		}
		if snapshot.User != "alice" {
			t.Errorf("User = %q, want alice", snapshot.User)
		}
		if len(snapshot.Nodes) != 2 {
			t.Errorf("Nodes len = %d, want 2", len(snapshot.Nodes))
		}
		if snapshot.State != domain.JobStateRunning {
			t.Errorf("State = %v, want running", snapshot.State)
		}
		if snapshot.GPUUsage == nil || *snapshot.GPUUsage != 75.5 {
			t.Errorf("GPUUsage = %v, want 75.5", snapshot.GPUUsage)
		}
		if len(snapshot.GPUDevices) != 2 {
			t.Errorf("GPUDevices len = %d, want 2", len(snapshot.GPUDevices))
		}
	})
}

func TestMarshalJobSnapshot(t *testing.T) {
	t.Run("nil job", func(t *testing.T) {
		_, err := marshalJobSnapshot(nil)
		if err == nil {
			t.Error("expected error for nil job")
		}
	})

	t.Run("valid job", func(t *testing.T) {
		job := &domain.Job{
			ID:    "marshal-job",
			User:  "bob",
			State: domain.JobStateCompleted,
		}

		data, err := marshalJobSnapshot(job)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(data) == 0 {
			t.Error("expected non-empty data")
		}
		// Verify it's valid JSON
		if data[0] != '{' {
			t.Error("expected JSON object")
		}
	})
}

func TestPostgresStorage_CreateJob_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	job := &domain.Job{
		ID:    "new-job",
		User:  "alice",
		Nodes: []string{"node-1", "node-2"},
		State: domain.JobStateRunning,
	}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT EXISTS\\(SELECT 1 FROM jobs WHERE id = \\$1\\)").
		WithArgs("new-job").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectExec("INSERT INTO jobs").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO job_audit_events").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err = store.CreateJob(testAuditContext(), job)
	if err != nil {
		t.Fatalf("CreateJob error: %v", err)
	}

	// Verify job fields were set
	if job.State != domain.JobStateRunning {
		t.Errorf("State = %v, want running", job.State)
	}
	if job.CreatedAt.IsZero() {
		t.Error("CreatedAt should be set")
	}
	if job.UpdatedAt.IsZero() {
		t.Error("UpdatedAt should be set")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_UpdateJob_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	startTime := time.Now().Add(-time.Hour)
	job := &domain.Job{
		ID:        "update-job",
		User:      "alice",
		Nodes:     []string{"node-1"},
		State:     domain.JobStateCompleted,
		StartTime: startTime,
	}

	mock.ExpectBegin()
	// getJobByIDTx returns running job (not terminal)
	mock.ExpectQuery("SELECT id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,").
		WithArgs("update-job").
		WillReturnRows(sqlmock.NewRows(jobRowColumns()).AddRow(jobRowValues("update-job", domain.JobStateRunning, startTime, nil)...))
	mock.ExpectExec("UPDATE jobs SET").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO job_audit_events").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err = store.UpdateJob(testAuditContext(), job)
	if err != nil {
		t.Fatalf("UpdateJob error: %v", err)
	}

	// Verify EndTime was set for terminal state
	if job.EndTime == nil {
		t.Error("EndTime should be set for completed job")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_UpdateJob_TerminalState(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	startTime := time.Now().Add(-time.Hour)
	endTime := time.Now().Add(-30 * time.Minute)
	job := &domain.Job{
		ID:    "frozen-job",
		User:  "alice",
		State: domain.JobStateRunning, // Trying to update
	}

	mock.ExpectBegin()
	// Existing job is already completed (terminal state)
	mock.ExpectQuery("SELECT id, user_name, nodes, node_count, state, start_time, end_time, runtime_seconds,").
		WithArgs("frozen-job").
		WillReturnRows(sqlmock.NewRows(jobRowColumns()).AddRow(jobRowValues("frozen-job", domain.JobStateCompleted, startTime, &endTime)...))
	// Should skip update and just commit
	mock.ExpectCommit()

	err = store.UpdateJob(testAuditContext(), job)
	if err != nil {
		t.Fatalf("UpdateJob error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_RecordMetrics_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	sample := &domain.MetricSample{
		JobID:         "metrics-job",
		Timestamp:     time.Now(),
		CPUUsage:      75.5,
		MemoryUsageMB: 2048,
	}

	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO metric_samples").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectExec("UPDATE jobs SET").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err = store.RecordMetrics(context.Background(), sample)
	if err != nil {
		t.Fatalf("RecordMetrics error: %v", err)
	}

	if sample.ID != 1 {
		t.Errorf("sample.ID = %d, want 1", sample.ID)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresStorage_RecordMetrics_InsertError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := &PostgresStorage{baseStorage: baseStorage{db: db}}

	sample := &domain.MetricSample{
		JobID:     "metrics-err",
		Timestamp: time.Now(),
		CPUUsage:  50.0,
	}

	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO metric_samples").
		WillReturnError(errors.New("insert failed"))
	mock.ExpectRollback()

	err = store.RecordMetrics(context.Background(), sample)
	if err == nil {
		t.Fatal("expected error")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
