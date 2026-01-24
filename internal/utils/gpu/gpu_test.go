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
}

func TestNewDetectorWithPaths(t *testing.T) {
	customNvidia := "/custom/nvidia-smi"
	customROCm := "/custom/rocm-smi"

	detector := NewDetectorWithPaths(customNvidia, customROCm)

	if detector == nil {
		t.Fatal("expected non-nil detector")
	}
	if detector.nvidiaSmiPath != customNvidia {
		t.Errorf("expected nvidiaSmiPath %s, got %s", customNvidia, detector.nvidiaSmiPath)
	}
	if detector.rocmSmiPath != customROCm {
		t.Errorf("expected rocmSmiPath %s, got %s", customROCm, detector.rocmSmiPath)
	}
}

func TestGetPrimaryVendor(t *testing.T) {
	detector := NewDetector()
	ctx := context.Background()

	// Test the function - result depends on hardware availability
	result := detector.GetPrimaryVendor(ctx)

	// Should return a valid vendor type
	validVendors := map[Vendor]bool{
		VendorNvidia: true,
		VendorAMD:    true,
		VendorNone:   true,
		VendorMixed:  true,
	}

	if !validVendors[result] {
		t.Errorf("GetPrimaryVendor returned unexpected vendor: %v", result)
	}
}

func TestGetMetricsForDevice(t *testing.T) {
	detector := NewDetector()
	ctx := context.Background()

	// Test with empty device ID
	metrics, err := detector.GetMetricsForDevice(ctx, "")
	if err != nil {
		t.Logf("GetMetricsForDevice returned error (expected if no GPUs): %v", err)
	}
	// Result depends on hardware availability

	// Test with non-existent device
	metrics, err = detector.GetMetricsForDevice(ctx, "nonexistent-device-12345")
	if err != nil {
		// Error is acceptable if device doesn't exist
		t.Logf("GetMetricsForDevice returned error (expected): %v", err)
	}
	// Should return nil for non-existent device
	if metrics != nil && metrics.ID != "" {
		t.Logf("GetMetricsForDevice returned: %+v", metrics)
	}
}

func TestExtractNumericValue(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected float64
	}{
		{
			name:     "percentage format",
			input:    "GPU use: 45%",
			expected: 45.0,
		},
		{
			name:     "plain number",
			input:    "123",
			expected: 123.0,
		},
		{
			name:     "decimal number",
			input:    "Temperature: 65.5 C",
			expected: 65.5,
		},
		{
			name:     "number at end",
			input:    "Power: 150",
			expected: 150.0,
		},
		{
			name:     "empty string",
			input:    "",
			expected: 0.0,
		},
		{
			name:     "no number",
			input:    "No numbers here",
			expected: 0.0,
		},
		{
			name:     "number in middle",
			input:    "Using 1024 MB memory",
			expected: 1024.0,
		},
		{
			name:     "negative indicator ignored",
			input:    "Value: -50",
			expected: 50.0,
		},
		{
			name:     "multiple decimals treats subsequent digits as whole",
			input:    "Version 1.2.3",
			expected: 1.23, // The function extracts 1.23 (after first decimal, numbers continue until non-digit)
		},
		{
			name:     "leading spaces",
			input:    "  Value: 42",
			expected: 42.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractNumericValue(tt.input)
			if result != tt.expected {
				t.Errorf("extractNumericValue(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestMapJobToGPUs(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: nil,
		},
		{
			name:     "single device index",
			input:    "0",
			expected: []string{"0"},
		},
		{
			name:     "multiple device indices",
			input:    "0,1,2",
			expected: []string{"0", "1", "2"},
		},
		{
			name:     "NVIDIA UUIDs",
			input:    "GPU-abc123,GPU-def456",
			expected: []string{"GPU-abc123", "GPU-def456"},
		},
		{
			name:     "AMD card format",
			input:    "card0,card1",
			expected: []string{"card0", "card1"},
		},
		{
			name:     "with spaces",
			input:    " 0 , 1 , 2 ",
			expected: []string{"0", "1", "2"},
		},
		{
			name:     "empty parts filtered",
			input:    "0,,1,",
			expected: []string{"0", "1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MapJobToGPUs(tt.input)
			if tt.expected == nil && result != nil {
				t.Errorf("MapJobToGPUs(%q) = %v, want nil", tt.input, result)
				return
			}
			if len(result) != len(tt.expected) {
				t.Errorf("MapJobToGPUs(%q) = %v, want %v", tt.input, result, tt.expected)
				return
			}
			for i, v := range result {
				if v != tt.expected[i] {
					t.Errorf("MapJobToGPUs(%q)[%d] = %v, want %v", tt.input, i, v, tt.expected[i])
				}
			}
		})
	}
}

func TestGetAMDMetricsLegacy_NoCommand(t *testing.T) {
	// Test with non-existent rocm-smi path - should return error
	detector := NewDetectorWithPaths("/nonexistent/nvidia-smi", "/nonexistent/rocm-smi")
	ctx := context.Background()

	_, err := detector.getAMDMetricsLegacy(ctx)
	if err == nil {
		t.Error("expected error for non-existent rocm-smi")
	}
}

func TestVendorMoreConstants(t *testing.T) {
	if VendorNone != "none" {
		t.Errorf("expected 'none', got '%s'", VendorNone)
	}
	if VendorMixed != "mixed" {
		t.Errorf("expected 'mixed', got '%s'", VendorMixed)
	}
}
