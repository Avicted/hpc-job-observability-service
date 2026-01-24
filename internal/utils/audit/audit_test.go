package audit

import (
	"context"
	"testing"

	"github.com/Avicted/hpc-job-observability-service/internal/domain"
)

func TestNewContext(t *testing.T) {
	tests := []struct {
		name          string
		changedBy     string
		source        string
		correlationID string
		wantCorrelID  bool // true if we expect the provided correlationID
	}{
		{
			name:          "with correlation ID",
			changedBy:     "user1",
			source:        "api",
			correlationID: "test-corr-id",
			wantCorrelID:  true,
		},
		{
			name:          "without correlation ID generates UUID",
			changedBy:     "user2",
			source:        "cli",
			correlationID: "",
			wantCorrelID:  false,
		},
		{
			name:          "trims whitespace",
			changedBy:     "  user3  ",
			source:        "  service  ",
			correlationID: "  corr-123  ",
			wantCorrelID:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.changedBy, tt.source, tt.correlationID)

			if tt.wantCorrelID {
				want := tt.correlationID
				if tt.name == "trims whitespace" {
					want = "corr-123"
				}
				if ctx.CorrelationID != want {
					t.Errorf("CorrelationID = %q, want %q", ctx.CorrelationID, want)
				}
			} else {
				// Should have generated a non-empty UUID
				if ctx.CorrelationID == "" {
					t.Error("expected generated CorrelationID, got empty")
				}
				if len(ctx.CorrelationID) != 36 { // UUID format
					t.Errorf("CorrelationID = %q, expected UUID format", ctx.CorrelationID)
				}
			}

			// Check trimmed values
			wantChangedBy := "user1"
			if tt.name == "without correlation ID generates UUID" {
				wantChangedBy = "user2"
			} else if tt.name == "trims whitespace" {
				wantChangedBy = "user3"
			}
			if ctx.ChangedBy != wantChangedBy {
				t.Errorf("ChangedBy = %q, want %q", ctx.ChangedBy, wantChangedBy)
			}
		})
	}
}

func TestContext_ToDomainAuditInfo(t *testing.T) {
	ctx := &Context{
		ChangedBy:     "  test-user  ",
		Source:        "  test-source  ",
		CorrelationID: "  test-corr  ",
	}

	info := ctx.ToDomainAuditInfo()

	if info.ChangedBy != "test-user" {
		t.Errorf("ChangedBy = %q, want %q", info.ChangedBy, "test-user")
	}
	if info.Source != "test-source" {
		t.Errorf("Source = %q, want %q", info.Source, "test-source")
	}
	if info.CorrelationID != "test-corr" {
		t.Errorf("CorrelationID = %q, want %q", info.CorrelationID, "test-corr")
	}
}

func TestNewAuditInfo(t *testing.T) {
	info := NewAuditInfo("  user1  ", "  api  ")

	if info.ChangedBy != "user1" {
		t.Errorf("ChangedBy = %q, want %q", info.ChangedBy, "user1")
	}
	if info.Source != "api" {
		t.Errorf("Source = %q, want %q", info.Source, "api")
	}
	if info.CorrelationID == "" {
		t.Error("expected generated CorrelationID, got empty")
	}
	if len(info.CorrelationID) != 36 {
		t.Errorf("CorrelationID = %q, expected UUID format", info.CorrelationID)
	}
}

func TestNewAuditInfoWithCorrelation(t *testing.T) {
	info := NewAuditInfoWithCorrelation("  user1  ", "  api  ", "  custom-corr  ")

	if info.ChangedBy != "user1" {
		t.Errorf("ChangedBy = %q, want %q", info.ChangedBy, "user1")
	}
	if info.Source != "api" {
		t.Errorf("Source = %q, want %q", info.Source, "api")
	}
	if info.CorrelationID != "custom-corr" {
		t.Errorf("CorrelationID = %q, want %q", info.CorrelationID, "custom-corr")
	}
}

func TestWithAuditInfo_GetAuditInfo(t *testing.T) {
	info := &domain.AuditInfo{
		ChangedBy:     "user",
		Source:        "test",
		CorrelationID: "corr-123",
	}

	ctx := WithAuditInfo(context.Background(), info)

	got, ok := GetAuditInfo(ctx)
	if !ok {
		t.Fatal("expected to find audit info in context")
	}

	if got.ChangedBy != "user" {
		t.Errorf("ChangedBy = %q, want %q", got.ChangedBy, "user")
	}
	if got.Source != "test" {
		t.Errorf("Source = %q, want %q", got.Source, "test")
	}
	if got.CorrelationID != "corr-123" {
		t.Errorf("CorrelationID = %q, want %q", got.CorrelationID, "corr-123")
	}
}

func TestGetAuditInfo_NotFound(t *testing.T) {
	ctx := context.Background()

	_, ok := GetAuditInfo(ctx)
	if ok {
		t.Error("expected not to find audit info in empty context")
	}
}

func TestValidateAuditInfo(t *testing.T) {
	tests := []struct {
		name  string
		info  *domain.AuditInfo
		valid bool
	}{
		{
			name:  "nil info",
			info:  nil,
			valid: false,
		},
		{
			name: "all fields present",
			info: &domain.AuditInfo{
				ChangedBy:     "user",
				Source:        "api",
				CorrelationID: "corr-123",
			},
			valid: true,
		},
		{
			name: "missing ChangedBy",
			info: &domain.AuditInfo{
				ChangedBy:     "",
				Source:        "api",
				CorrelationID: "corr-123",
			},
			valid: false,
		},
		{
			name: "missing Source",
			info: &domain.AuditInfo{
				ChangedBy:     "user",
				Source:        "",
				CorrelationID: "corr-123",
			},
			valid: false,
		},
		{
			name: "missing CorrelationID",
			info: &domain.AuditInfo{
				ChangedBy:     "user",
				Source:        "api",
				CorrelationID: "",
			},
			valid: false,
		},
		{
			name: "whitespace only fields",
			info: &domain.AuditInfo{
				ChangedBy:     "   ",
				Source:        "api",
				CorrelationID: "corr-123",
			},
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ValidateAuditInfo(tt.info)
			if got != tt.valid {
				t.Errorf("ValidateAuditInfo() = %v, want %v", got, tt.valid)
			}
		})
	}
}
