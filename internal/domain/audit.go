package domain

import (
	"context"
	"strings"

	"github.com/google/uuid"
)

// auditInfoContextKey is the context key for audit info.
type auditInfoContextKey struct{}

// WithAuditInfo attaches audit info to a context.
func WithAuditInfo(ctx context.Context, info *AuditInfo) context.Context {
	return context.WithValue(ctx, auditInfoContextKey{}, info)
}

// GetAuditInfo retrieves audit info from a context.
func GetAuditInfo(ctx context.Context) (*AuditInfo, bool) {
	info, ok := ctx.Value(auditInfoContextKey{}).(*AuditInfo)
	return info, ok
}

// NewAuditInfo creates audit info with a new correlation ID.
func NewAuditInfo(changedBy, source string) *AuditInfo {
	return &AuditInfo{
		ChangedBy:     strings.TrimSpace(changedBy),
		Source:        strings.TrimSpace(source),
		CorrelationID: uuid.NewString(),
	}
}

// NewAuditInfoWithCorrelation creates audit info with an explicit correlation ID.
func NewAuditInfoWithCorrelation(changedBy, source, correlationID string) *AuditInfo {
	return &AuditInfo{
		ChangedBy:     strings.TrimSpace(changedBy),
		Source:        strings.TrimSpace(source),
		CorrelationID: strings.TrimSpace(correlationID),
	}
}
