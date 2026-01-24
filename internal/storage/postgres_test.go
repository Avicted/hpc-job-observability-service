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
