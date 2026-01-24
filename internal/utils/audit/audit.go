// Package audit provides audit context handling for tracking changes
// across the HPC Job Observability Service. AuditContext captures who
// made a change, from what source, and a correlation ID for tracing.
package audit

import (
	"context"
	"strings"

	"github.com/Avicted/hpc-job-observability-service/internal/domain"
	"github.com/google/uuid"
)

// Context contains audit information extracted from HTTP headers or other sources.
// This is used to track who made changes and for correlation across services.
type Context struct {
	ChangedBy     string
	Source        string
	CorrelationID string
}

// ToDomainAuditInfo converts audit.Context to domain.AuditInfo.
func (c *Context) ToDomainAuditInfo() *domain.AuditInfo {
	return &domain.AuditInfo{
		ChangedBy:     strings.TrimSpace(c.ChangedBy),
		Source:        strings.TrimSpace(c.Source),
		CorrelationID: strings.TrimSpace(c.CorrelationID),
	}
}

// NewContext creates a new audit Context with the given values.
// If correlationID is empty, a new UUID is generated.
func NewContext(changedBy, source, correlationID string) *Context {
	if correlationID == "" {
		correlationID = uuid.NewString()
	}
	return &Context{
		ChangedBy:     strings.TrimSpace(changedBy),
		Source:        strings.TrimSpace(source),
		CorrelationID: strings.TrimSpace(correlationID),
	}
}

// NewAuditInfo creates a new domain.AuditInfo with a new correlation ID.
func NewAuditInfo(changedBy, source string) *domain.AuditInfo {
	return &domain.AuditInfo{
		ChangedBy:     strings.TrimSpace(changedBy),
		Source:        strings.TrimSpace(source),
		CorrelationID: uuid.NewString(),
	}
}

// NewAuditInfoWithCorrelation creates a new domain.AuditInfo with an explicit correlation ID.
func NewAuditInfoWithCorrelation(changedBy, source, correlationID string) *domain.AuditInfo {
	return &domain.AuditInfo{
		ChangedBy:     strings.TrimSpace(changedBy),
		Source:        strings.TrimSpace(source),
		CorrelationID: strings.TrimSpace(correlationID),
	}
}

// auditInfoContextKey is the context key for audit info.
type auditInfoContextKey struct{}

// WithAuditInfo attaches audit info to a context.
func WithAuditInfo(ctx context.Context, info *domain.AuditInfo) context.Context {
	return context.WithValue(ctx, auditInfoContextKey{}, info)
}

// GetAuditInfo retrieves audit info from a context.
func GetAuditInfo(ctx context.Context) (*domain.AuditInfo, bool) {
	info, ok := ctx.Value(auditInfoContextKey{}).(*domain.AuditInfo)
	return info, ok
}

// ValidateAuditInfo validates that all required audit fields are present.
func ValidateAuditInfo(info *domain.AuditInfo) bool {
	if info == nil {
		return false
	}
	return strings.TrimSpace(info.ChangedBy) != "" &&
		strings.TrimSpace(info.Source) != "" &&
		strings.TrimSpace(info.CorrelationID) != ""
}
