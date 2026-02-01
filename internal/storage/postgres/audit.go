package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/domain"
)

// auditStore implements storage.AuditStore using pgx.
type auditStore struct {
	db    DBTX
	store *Store
}

// RecordAuditEvent records an audit event for a job change.
// This participates in the same transaction as the job mutation when called within WithTx.
func (s *auditStore) RecordAuditEvent(ctx context.Context, event *domain.JobAuditEvent) (err error) {
	defer s.store.observe("record_audit_event", time.Now(), &err)

	// Marshal snapshot to JSON if present
	// Note: The current implementation expects the snapshot to be set by the caller
	// If we need to fetch the job and create the snapshot here, we'd need the job passed in
	var snapshotJSON []byte
	if event.Snapshot != nil {
		snapshotJSON, err = json.Marshal(event.Snapshot)
		if err != nil {
			return err
		}
	}

	_, err = s.db.Exec(ctx, queryInsertAuditEvent,
		event.JobID,
		event.ChangeType,
		event.ChangedAt,
		event.ChangedBy,
		event.Source,
		event.CorrelationID,
		snapshotJSON,
	)

	return mapPgError(err)
}
