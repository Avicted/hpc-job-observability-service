package gpu

import (
	"context"
	"testing"
)

func TestNewDetector(t *testing.T) {
	detector := NewDetector()
	if detector == nil {
		t.Error("expected non-nil detector")
	}
}

func TestDetectVendors(t *testing.T) {
	detector := NewDetector()
	ctx := context.Background()

	vendors := detector.DetectVendors(ctx)

	// Should not panic, vendors can be empty if no GPUs present
	if vendors == nil {
		t.Error("expected non-nil vendor slice")
	}
}

func TestListDevices(t *testing.T) {
	detector := NewDetector()
	ctx := context.Background()

	devices, err := detector.ListDevices(ctx)
	if err != nil {
		// Error is acceptable if no GPUs are present
		t.Logf("ListDevices returned error (expected if no GPUs): %v", err)
		return
	}

	// If devices returned, verify they have required fields
	for _, d := range devices {
		if d.ID == "" {
			t.Error("device ID should not be empty")
		}
		if d.Vendor != VendorNvidia && d.Vendor != VendorAMD {
			t.Errorf("unexpected vendor: %s", d.Vendor)
		}
	}
}

func TestGetMetrics(t *testing.T) {
	detector := NewDetector()
	ctx := context.Background()

	metrics, err := detector.GetMetrics(ctx)
	if err != nil {
		// Error is acceptable if no GPUs are present
		t.Logf("GetMetrics returned error (expected if no GPUs): %v", err)
		return
	}

	// If metrics returned, verify they have valid values
	for _, m := range metrics {
		if m.ID == "" {
			t.Error("metric device ID should not be empty")
		}
		if m.UtilizationPct < 0 || m.UtilizationPct > 100 {
			t.Errorf("utilization percentage out of range: %.2f", m.UtilizationPct)
		}
		if m.MemoryUsagePct < 0 || m.MemoryUsagePct > 100 {
			t.Errorf("memory usage percentage out of range: %.2f", m.MemoryUsagePct)
		}
	}
}

func TestGetMetricsForDevices(t *testing.T) {
	detector := NewDetector()
	ctx := context.Background()

	// Test with empty device list
	metrics, err := detector.GetMetricsForDevices(ctx, []string{})
	if err != nil {
		t.Errorf("unexpected error with empty device list: %v", err)
	}
	if len(metrics) != 0 {
		t.Errorf("expected empty metrics for empty device list, got %d", len(metrics))
	}

	// Test with specific devices - may fail if devices don't exist
	_, err = detector.GetMetricsForDevices(ctx, []string{"0"})
	// Error is acceptable if device doesn't exist
	if err != nil {
		t.Logf("GetMetricsForDevices returned error (expected if device doesn't exist): %v", err)
	}
}

func TestVendorConstants(t *testing.T) {
	if VendorNvidia != "nvidia" {
		t.Errorf("expected 'nvidia', got '%s'", VendorNvidia)
	}
	if VendorAMD != "amd" {
		t.Errorf("expected 'amd', got '%s'", VendorAMD)
	}
	if VendorNone != "none" {
		t.Errorf("expected 'none', got '%s'", VendorNone)
	}
	if VendorMixed != "mixed" {
		t.Errorf("expected 'mixed', got '%s'", VendorMixed)
	}
}
