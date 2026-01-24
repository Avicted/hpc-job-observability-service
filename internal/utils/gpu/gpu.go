// Package gpu provides GPU detection and metrics collection for NVIDIA and AMD GPUs.
// It supports vendor-agnostic GPU discovery and per-device metrics.
package gpu

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// Vendor represents the GPU vendor.
type Vendor string

const (
	VendorNone   Vendor = "none"
	VendorNvidia Vendor = "nvidia"
	VendorAMD    Vendor = "amd"
	VendorMixed  Vendor = "mixed" // When a node has GPUs from multiple vendors
)

// DeviceInfo contains static information about a GPU device.
type DeviceInfo struct {
	ID         string // Device ID (e.g., "GPU-abc123" for NVIDIA, "card0" for AMD)
	Index      int    // Device index (0, 1, 2, ...)
	Vendor     Vendor // GPU vendor
	Name       string // GPU model name (e.g., "NVIDIA A100", "AMD MI250X")
	UUID       string // Unique device UUID
	MemoryMB   int64  // Total memory in MB
	PCIBusID   string // PCI bus ID
	ComputeCap string // Compute capability (NVIDIA) or architecture (AMD)
}

// DeviceMetrics contains runtime metrics for a GPU device.
type DeviceMetrics struct {
	ID              string    // Device ID
	Index           int       // Device index
	Vendor          Vendor    // GPU vendor
	UtilizationPct  float64   // GPU utilization percentage (0-100)
	MemoryUsedMB    int64     // Memory used in MB
	MemoryTotalMB   int64     // Total memory in MB
	MemoryUsagePct  float64   // Memory utilization percentage (0-100)
	TemperatureC    int       // Temperature in Celsius
	PowerWatts      float64   // Power consumption in Watts
	PowerLimitWatts float64   // Power limit in Watts
	ClockMHz        int       // Current SM/GFX clock in MHz
	MemoryClockMHz  int       // Memory clock in MHz
	FanSpeedPct     int       // Fan speed percentage (0-100)
	Timestamp       time.Time // When the metrics were collected
}

// Detector discovers and queries GPUs on the system.
type Detector struct {
	nvidiaSmiPath string
	rocmSmiPath   string
	timeout       time.Duration
}

// NewDetector creates a new GPU detector with default settings.
func NewDetector() *Detector {
	return &Detector{
		nvidiaSmiPath: "nvidia-smi",
		rocmSmiPath:   "rocm-smi",
		timeout:       5 * time.Second,
	}
}

// NewDetectorWithPaths creates a new GPU detector with custom executable paths.
func NewDetectorWithPaths(nvidiaSmi, rocmSmi string) *Detector {
	d := NewDetector()
	if nvidiaSmi != "" {
		d.nvidiaSmiPath = nvidiaSmi
	}
	if rocmSmi != "" {
		d.rocmSmiPath = rocmSmi
	}
	return d
}

// DetectVendors returns all GPU vendors available on the system.
func (d *Detector) DetectVendors(ctx context.Context) []Vendor {
	vendors := make([]Vendor, 0)

	if d.hasNvidiaGPUs(ctx) {
		vendors = append(vendors, VendorNvidia)
	}

	if d.hasAMDGPUs(ctx) {
		vendors = append(vendors, VendorAMD)
	}

	if len(vendors) == 0 {
		return []Vendor{VendorNone}
	}

	return vendors
}

// GetPrimaryVendor returns the primary GPU vendor (or "mixed" if multiple).
func (d *Detector) GetPrimaryVendor(ctx context.Context) Vendor {
	vendors := d.DetectVendors(ctx)

	switch len(vendors) {
	case 0:
		return VendorNone
	case 1:
		return vendors[0]
	default:
		return VendorMixed
	}
}

// hasNvidiaGPUs checks if NVIDIA GPUs are available.
func (d *Detector) hasNvidiaGPUs(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, d.nvidiaSmiPath, "-L")
	output, err := cmd.Output()
	if err != nil {
		return false
	}

	return strings.Contains(string(output), "GPU")
}

// hasAMDGPUs checks if AMD GPUs are available.
func (d *Detector) hasAMDGPUs(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, d.rocmSmiPath, "--showproductname")
	output, err := cmd.Output()
	if err != nil {
		return false
	}

	// rocm-smi returns output with GPU info if GPUs are present
	return strings.Contains(string(output), "GPU") || strings.Contains(string(output), "card")
}

// ListDevices returns information about all GPUs on the system.
func (d *Detector) ListDevices(ctx context.Context) ([]DeviceInfo, error) {
	devices := make([]DeviceInfo, 0)

	// Query NVIDIA GPUs
	nvidiaDevices, err := d.listNvidiaDevices(ctx)
	if err == nil {
		devices = append(devices, nvidiaDevices...)
	}

	// Query AMD GPUs
	amdDevices, err := d.listAMDDevices(ctx)
	if err == nil {
		devices = append(devices, amdDevices...)
	}

	return devices, nil
}

// listNvidiaDevices queries NVIDIA GPUs using nvidia-smi.
func (d *Detector) listNvidiaDevices(ctx context.Context) ([]DeviceInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	// Query format: index, uuid, name, memory.total, pci.bus_id, compute_cap
	cmd := exec.CommandContext(ctx, d.nvidiaSmiPath,
		"--query-gpu=index,uuid,name,memory.total,pci.bus_id,compute_cap",
		"--format=csv,noheader,nounits")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("nvidia-smi failed: %w", err)
	}

	devices := make([]DeviceInfo, 0)
	reader := csv.NewReader(bytes.NewReader(output))
	reader.TrimLeadingSpace = true

	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		if len(record) < 6 {
			continue
		}

		index, _ := strconv.Atoi(strings.TrimSpace(record[0]))
		memoryMB, _ := strconv.ParseInt(strings.TrimSpace(record[3]), 10, 64)

		devices = append(devices, DeviceInfo{
			ID:         fmt.Sprintf("GPU-%s", strings.TrimSpace(record[1])),
			Index:      index,
			Vendor:     VendorNvidia,
			Name:       strings.TrimSpace(record[2]),
			UUID:       strings.TrimSpace(record[1]),
			MemoryMB:   memoryMB,
			PCIBusID:   strings.TrimSpace(record[4]),
			ComputeCap: strings.TrimSpace(record[5]),
		})
	}

	return devices, nil
}

// listAMDDevices queries AMD GPUs using rocm-smi.
func (d *Detector) listAMDDevices(ctx context.Context) ([]DeviceInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	// Get list of devices
	cmd := exec.CommandContext(ctx, d.rocmSmiPath, "--showhw")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("rocm-smi failed: %w", err)
	}

	devices := make([]DeviceInfo, 0)
	scanner := bufio.NewScanner(bytes.NewReader(output))
	index := 0

	for scanner.Scan() {
		line := scanner.Text()

		// Parse lines containing GPU info (format varies by rocm-smi version)
		if strings.Contains(line, "card") || strings.Contains(line, "GPU") {
			// Try to extract device info from the line
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				device := DeviceInfo{
					ID:     fmt.Sprintf("card%d", index),
					Index:  index,
					Vendor: VendorAMD,
					Name:   "AMD GPU", // Will be enriched by detailed query
				}
				devices = append(devices, device)
				index++
			}
		}
	}

	// Enrich device info with memory and other details
	d.enrichAMDDeviceInfo(ctx, devices)

	return devices, nil
}

// enrichAMDDeviceInfo adds memory and other details to AMD devices.
func (d *Detector) enrichAMDDeviceInfo(ctx context.Context, devices []DeviceInfo) {
	for i := range devices {
		// Get memory info
		cmd := exec.CommandContext(ctx, d.rocmSmiPath, "-d", fmt.Sprintf("%d", devices[i].Index), "--showmeminfo", "vram")
		output, err := cmd.Output()
		if err != nil {
			continue
		}

		// Parse memory total from output
		lines := strings.Split(string(output), "\n")
		for _, line := range lines {
			if strings.Contains(line, "Total") {
				fields := strings.Fields(line)
				for j, field := range fields {
					if strings.Contains(field, "Total") && j+1 < len(fields) {
						memBytes, _ := strconv.ParseInt(fields[j+1], 10, 64)
						devices[i].MemoryMB = memBytes / (1024 * 1024)
					}
				}
			}
		}

		// Get product name
		cmd = exec.CommandContext(ctx, d.rocmSmiPath, "-d", fmt.Sprintf("%d", devices[i].Index), "--showproductname")
		output, err = cmd.Output()
		if err != nil {
			continue
		}

		lines = strings.Split(string(output), "\n")
		for _, line := range lines {
			if strings.Contains(line, "Card series") || strings.Contains(line, "Product Name") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					devices[i].Name = strings.TrimSpace(parts[1])
				}
			}
		}
	}
}

// GetMetrics returns runtime metrics for all GPUs.
func (d *Detector) GetMetrics(ctx context.Context) ([]DeviceMetrics, error) {
	metrics := make([]DeviceMetrics, 0)

	// Query NVIDIA metrics
	nvidiaMetrics, err := d.getNvidiaMetrics(ctx)
	if err == nil {
		metrics = append(metrics, nvidiaMetrics...)
	}

	// Query AMD metrics
	amdMetrics, err := d.getAMDMetrics(ctx)
	if err == nil {
		metrics = append(metrics, amdMetrics...)
	}

	return metrics, nil
}

// GetMetricsForDevice returns metrics for a specific device.
func (d *Detector) GetMetricsForDevice(ctx context.Context, deviceID string) (*DeviceMetrics, error) {
	allMetrics, err := d.GetMetrics(ctx)
	if err != nil {
		return nil, err
	}

	for _, m := range allMetrics {
		if m.ID == deviceID {
			return &m, nil
		}
	}

	return nil, fmt.Errorf("device %s not found", deviceID)
}

// getNvidiaMetrics queries NVIDIA GPU metrics using nvidia-smi.
func (d *Detector) getNvidiaMetrics(ctx context.Context) ([]DeviceMetrics, error) {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	// Query comprehensive metrics
	cmd := exec.CommandContext(ctx, d.nvidiaSmiPath,
		"--query-gpu=index,uuid,utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw,power.limit,clocks.sm,clocks.mem,fan.speed",
		"--format=csv,noheader,nounits")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("nvidia-smi failed: %w", err)
	}

	metrics := make([]DeviceMetrics, 0)
	reader := csv.NewReader(bytes.NewReader(output))
	reader.TrimLeadingSpace = true
	timestamp := time.Now()

	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		if len(record) < 11 {
			continue
		}

		index, _ := strconv.Atoi(strings.TrimSpace(record[0]))
		util, _ := strconv.ParseFloat(strings.TrimSpace(record[2]), 64)
		memUsed, _ := strconv.ParseInt(strings.TrimSpace(record[3]), 10, 64)
		memTotal, _ := strconv.ParseInt(strings.TrimSpace(record[4]), 10, 64)
		temp, _ := strconv.Atoi(strings.TrimSpace(record[5]))
		power, _ := strconv.ParseFloat(strings.TrimSpace(record[6]), 64)
		powerLimit, _ := strconv.ParseFloat(strings.TrimSpace(record[7]), 64)
		clock, _ := strconv.Atoi(strings.TrimSpace(record[8]))
		memClock, _ := strconv.Atoi(strings.TrimSpace(record[9]))
		fanSpeed, _ := strconv.Atoi(strings.TrimSpace(record[10]))

		memUsagePct := float64(0)
		if memTotal > 0 {
			memUsagePct = float64(memUsed) / float64(memTotal) * 100
		}

		metrics = append(metrics, DeviceMetrics{
			ID:              fmt.Sprintf("GPU-%s", strings.TrimSpace(record[1])),
			Index:           index,
			Vendor:          VendorNvidia,
			UtilizationPct:  util,
			MemoryUsedMB:    memUsed,
			MemoryTotalMB:   memTotal,
			MemoryUsagePct:  memUsagePct,
			TemperatureC:    temp,
			PowerWatts:      power,
			PowerLimitWatts: powerLimit,
			ClockMHz:        clock,
			MemoryClockMHz:  memClock,
			FanSpeedPct:     fanSpeed,
			Timestamp:       timestamp,
		})
	}

	return metrics, nil
}

// getAMDMetrics queries AMD GPU metrics using rocm-smi.
func (d *Detector) getAMDMetrics(ctx context.Context) ([]DeviceMetrics, error) {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	// Get GPU utilization and other metrics
	cmd := exec.CommandContext(ctx, d.rocmSmiPath, "-a", "--json")
	output, err := cmd.Output()
	if err != nil {
		// Fall back to non-JSON output
		return d.getAMDMetricsLegacy(ctx)
	}

	// Parse JSON output (rocm-smi >= 5.0)
	return d.parseAMDMetricsJSON(output)
}

// getAMDMetricsLegacy queries AMD metrics using legacy rocm-smi format.
func (d *Detector) getAMDMetricsLegacy(ctx context.Context) ([]DeviceMetrics, error) {
	metrics := make([]DeviceMetrics, 0)
	timestamp := time.Now()

	// Get device count first
	cmd := exec.CommandContext(ctx, d.rocmSmiPath, "--showallinfo")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("rocm-smi failed: %w", err)
	}

	// Parse the output
	scanner := bufio.NewScanner(bytes.NewReader(output))
	currentDevice := -1
	currentMetrics := DeviceMetrics{Vendor: VendorAMD, Timestamp: timestamp}

	for scanner.Scan() {
		line := scanner.Text()

		// Detect device blocks
		if strings.Contains(line, "GPU[") {
			if currentDevice >= 0 {
				currentMetrics.Index = currentDevice
				currentMetrics.ID = fmt.Sprintf("card%d", currentDevice)
				metrics = append(metrics, currentMetrics)
			}
			currentDevice++
			currentMetrics = DeviceMetrics{Vendor: VendorAMD, Timestamp: timestamp}
		}

		// Parse metrics from lines
		if strings.Contains(line, "GPU use") || strings.Contains(line, "GPU Activity") {
			value := extractNumericValue(line)
			currentMetrics.UtilizationPct = value
		}
		if strings.Contains(line, "GPU memory use") || strings.Contains(line, "VRAM Use") {
			value := extractNumericValue(line)
			currentMetrics.MemoryUsagePct = value
		}
		if strings.Contains(line, "Temperature") && strings.Contains(line, "edge") {
			value := extractNumericValue(line)
			currentMetrics.TemperatureC = int(value)
		}
		if strings.Contains(line, "Power") && !strings.Contains(line, "Cap") {
			value := extractNumericValue(line)
			currentMetrics.PowerWatts = value
		}
		if strings.Contains(line, "Power Cap") || strings.Contains(line, "Power Limit") {
			value := extractNumericValue(line)
			currentMetrics.PowerLimitWatts = value
		}
		if strings.Contains(line, "sclk") || strings.Contains(line, "SCLK") {
			value := extractNumericValue(line)
			currentMetrics.ClockMHz = int(value)
		}
		if strings.Contains(line, "mclk") || strings.Contains(line, "MCLK") {
			value := extractNumericValue(line)
			currentMetrics.MemoryClockMHz = int(value)
		}
		if strings.Contains(line, "Fan") && strings.Contains(line, "%") {
			value := extractNumericValue(line)
			currentMetrics.FanSpeedPct = int(value)
		}
	}

	// Don't forget the last device
	if currentDevice >= 0 {
		currentMetrics.Index = currentDevice
		currentMetrics.ID = fmt.Sprintf("card%d", currentDevice)
		metrics = append(metrics, currentMetrics)
	}

	return metrics, nil
}

// parseAMDMetricsJSON parses JSON output from rocm-smi.
func (d *Detector) parseAMDMetricsJSON(_ []byte) ([]DeviceMetrics, error) {
	// Simple JSON parsing - in production, use encoding/json
	// For now, fall back to legacy parsing as JSON format varies significantly
	return nil, fmt.Errorf("JSON parsing not implemented, using legacy")
}

// extractNumericValue extracts a numeric value from a line like "GPU use: 45%"
func extractNumericValue(line string) float64 {
	// Find numeric characters
	var numStr strings.Builder
	foundDecimal := false
	started := false

	for _, r := range line {
		if r >= '0' && r <= '9' {
			numStr.WriteRune(r)
			started = true
		} else if r == '.' && started && !foundDecimal {
			numStr.WriteRune(r)
			foundDecimal = true
		} else if started && (r < '0' || r > '9') && r != '.' {
			break
		}
	}

	value, _ := strconv.ParseFloat(numStr.String(), 64)
	return value
}

// MapJobToGPUs maps a Slurm job's GPU allocation to device IDs.
// gpuList is the value from SLURM_JOB_GPUS or CUDA_VISIBLE_DEVICES.
func MapJobToGPUs(gpuList string) []string {
	if gpuList == "" {
		return nil
	}

	// Split by comma and clean up
	parts := strings.Split(gpuList, ",")
	devices := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Handle various formats:
		// - "0,1,2" (device indices)
		// - "GPU-abc123,GPU-def456" (NVIDIA UUIDs)
		// - "card0,card1" (AMD devices)
		devices = append(devices, part)
	}

	return devices
}

// GetMetricsForDevices returns metrics for specific device IDs.
func (d *Detector) GetMetricsForDevices(ctx context.Context, deviceIDs []string) ([]DeviceMetrics, error) {
	allMetrics, err := d.GetMetrics(ctx)
	if err != nil {
		return nil, err
	}

	// Create a lookup set
	deviceSet := make(map[string]bool)
	for _, id := range deviceIDs {
		deviceSet[id] = true
		// Also match by index
		if idx, err := strconv.Atoi(id); err == nil {
			deviceSet[fmt.Sprintf("GPU-%d", idx)] = true
			deviceSet[fmt.Sprintf("card%d", idx)] = true
		}
	}

	metrics := make([]DeviceMetrics, 0)
	for _, m := range allMetrics {
		if deviceSet[m.ID] || deviceSet[fmt.Sprintf("%d", m.Index)] {
			metrics = append(metrics, m)
		}
	}

	return metrics, nil
}
