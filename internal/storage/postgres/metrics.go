package postgres

import (
	"context"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// metricStore implements storage.MetricStore using pgx.
type metricStore struct {
	db    DBTX
	store *Store
}

// scanMetricRow scans a pgx.Row into a domain.MetricSample.
func scanMetricRow(row pgx.Row) (*domain.MetricSample, error) {
	var sample domain.MetricSample
	var gpuUsage pgtype.Float8

	err := row.Scan(
		&sample.ID,
		&sample.JobID,
		&sample.Timestamp,
		&sample.CPUUsage,
		&sample.MemoryUsageMB,
		&gpuUsage,
	)
	if err != nil {
		return nil, mapPgError(err)
	}

	if gpuUsage.Valid {
		f := gpuUsage.Float64
		sample.GPUUsage = &f
	}

	return &sample, nil
}

// RecordMetrics records a single metric sample and updates job statistics.
func (s *metricStore) RecordMetrics(ctx context.Context, sample *domain.MetricSample) (err error) {
	defer s.store.observe("record_metrics", time.Now(), &err)

	// Insert metric
	err = s.db.QueryRow(ctx, queryInsertMetric,
		sample.JobID,
		sample.Timestamp,
		sample.CPUUsage,
		sample.MemoryUsageMB,
		sample.GPUUsage,
	).Scan(&sample.ID)
	if err != nil {
		return mapPgError(err)
	}

	// Update job aggregated stats
	_, err = s.db.Exec(ctx, queryUpdateJobStats,
		sample.JobID,
		sample.Timestamp,
		sample.CPUUsage,
		sample.MemoryUsageMB,
		sample.GPUUsage,
	)
	if err != nil {
		return mapPgError(err)
	}

	return nil
}

// RecordMetricsBatch efficiently records multiple metrics using pgx.Batch.
func (s *metricStore) RecordMetricsBatch(ctx context.Context, samples []*domain.MetricSample) (err error) {
	defer s.store.observe("record_metrics_batch", time.Now(), &err)

	if len(samples) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, sample := range samples {
		batch.Queue(queryInsertMetric,
			sample.JobID,
			sample.Timestamp,
			sample.CPUUsage,
			sample.MemoryUsageMB,
			sample.GPUUsage,
		)
	}

	// Send batch
	results := s.db.SendBatch(ctx, batch)
	defer results.Close()

	// Collect IDs from batch results
	for i := 0; i < len(samples); i++ {
		err := results.QueryRow().Scan(&samples[i].ID)
		if err != nil {
			return mapPgError(err)
		}
	}

	// Update job stats for each unique job (after successful batch insert)
	// Group samples by job ID to minimize updates
	jobMetrics := make(map[string]*domain.MetricSample)
	for _, sample := range samples {
		// Keep the latest metric per job for stats update
		if existing, ok := jobMetrics[sample.JobID]; !ok || sample.Timestamp.After(existing.Timestamp) {
			jobMetrics[sample.JobID] = sample
		}
	}

	// Update job stats for each job
	for _, sample := range jobMetrics {
		_, err := s.db.Exec(ctx, queryUpdateJobStats,
			sample.JobID,
			sample.Timestamp,
			sample.CPUUsage,
			sample.MemoryUsageMB,
			sample.GPUUsage,
		)
		if err != nil {
			return mapPgError(err)
		}
	}

	return nil
}

// GetLatestMetrics retrieves the most recent metric sample for a job.
func (s *metricStore) GetLatestMetrics(ctx context.Context, jobID string) (sample *domain.MetricSample, err error) {
	defer s.store.observe("get_latest_metrics", time.Now(), &err)

	row := s.db.QueryRow(ctx, queryGetLatestMetric, jobID)
	sample, err = scanMetricRow(row)
	return sample, err
}

// GetJobMetrics retrieves metrics for a job with optional time filtering and pagination.
func (s *metricStore) GetJobMetrics(ctx context.Context, jobID string, filter domain.MetricsFilter) (samples []*domain.MetricSample, total int, err error) {
	defer s.store.observe("get_job_metrics", time.Now(), &err)

	// Build dynamic WHERE clause for time filtering
	query := queryGetJobMetricsBase
	countQuery := queryCountJobMetricsBase
	args := []any{jobID}
	argPos := 2

	if filter.StartTime != nil {
		query += " AND timestamp >= $2"
		countQuery += " AND timestamp >= $2"
		args = append(args, *filter.StartTime)
		argPos++
	}
	if filter.EndTime != nil {
		query += " AND timestamp <= $3"
		countQuery += " AND timestamp <= $3"
		args = append(args, *filter.EndTime)
		argPos++
	}

	// Get total count
	err = s.db.QueryRow(ctx, countQuery, args...).Scan(&total)
	if err != nil {
		return nil, 0, mapPgError(err)
	}

	// Add ordering and pagination
	query += " ORDER BY timestamp DESC"
	if filter.Limit > 0 {
		query += " LIMIT $4"
		args = append(args, filter.Limit)
		argPos++
	}

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, mapPgError(err)
	}
	defer rows.Close()

	samples = make([]*domain.MetricSample, 0)
	for rows.Next() {
		sample, err := scanMetricRow(rows)
		if err != nil {
			return nil, 0, err
		}
		samples = append(samples, sample)
	}

	if err = rows.Err(); err != nil {
		return nil, 0, mapPgError(err)
	}

	return samples, total, nil
}

// DeleteMetricsBefore deletes all metrics older than the cutoff time.
func (s *metricStore) DeleteMetricsBefore(ctx context.Context, cutoff time.Time) (err error) {
	defer s.store.observe("delete_metrics_before", time.Now(), &err)

	_, err = s.db.Exec(ctx, queryDeleteMetricsBefore, cutoff)
	return mapPgError(err)
}
