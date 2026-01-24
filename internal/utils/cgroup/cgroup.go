// Package cgroup provides cgroup v2 metrics reading for job resource monitoring.
// It reads CPU, memory, and IO statistics from the cgroup filesystem.
package cgroup

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	// DefaultCgroupRoot is the default mount point for cgroup v2.
	DefaultCgroupRoot = "/sys/fs/cgroup"

	// SlurmCgroupPrefix is the typical path prefix for Slurm job cgroups.
	SlurmCgroupPrefix = "system.slice/slurmstepd.scope"
)

// Stats represents aggregated resource usage from cgroup metrics.
type Stats struct {
	// CPU statistics
	CPUUsageUsec  uint64  // Total CPU usage in microseconds
	CPUUserUsec   uint64  // User mode CPU usage in microseconds
	CPUSystemUsec uint64  // System mode CPU usage in microseconds
	CPUPercent    float64 // Calculated CPU usage percentage (requires two samples)
	NrPeriods     uint64  // Number of enforcement intervals
	NrThrottled   uint64  // Number of times throttled
	ThrottledUsec uint64  // Time throttled in microseconds

	// Memory statistics
	MemoryCurrentBytes uint64 // Current memory usage in bytes
	MemoryMaxBytes     uint64 // Memory limit in bytes (max means unlimited)
	MemoryPeakBytes    uint64 // Peak memory usage in bytes (memory.peak)
	MemorySwapBytes    uint64 // Swap usage in bytes

	// IO statistics (aggregated across all devices)
	IOReadBytes  uint64 // Total bytes read
	IOWriteBytes uint64 // Total bytes written
	IOReadOps    uint64 // Total read operations
	IOWriteOps   uint64 // Total write operations

	// Metadata
	CgroupPath string    // Path to the cgroup
	Timestamp  time.Time // When the stats were collected
}

// Reader reads cgroup v2 metrics from the filesystem.
type Reader struct {
	cgroupRoot string
}

// NewReader creates a new cgroup reader with the default root.
func NewReader() *Reader {
	return &Reader{
		cgroupRoot: DefaultCgroupRoot,
	}
}

// NewReaderWithRoot creates a new cgroup reader with a custom root path.
// Useful for testing or when cgroups are mounted elsewhere.
func NewReaderWithRoot(root string) *Reader {
	return &Reader{
		cgroupRoot: root,
	}
}

// IsCgroupV2 checks if cgroup v2 is available on the system.
func (r *Reader) IsCgroupV2() bool {
	// Check for cgroup.controllers file which only exists in v2
	controllersPath := filepath.Join(r.cgroupRoot, "cgroup.controllers")
	_, err := os.Stat(controllersPath)
	return err == nil
}

// GetJobCgroupPath constructs the expected cgroup path for a Slurm job.
// The actual path depends on Slurm configuration but typically follows:
// /sys/fs/cgroup/system.slice/slurmstepd.scope/job_<jobid>
func (r *Reader) GetJobCgroupPath(jobID string) string {
	return filepath.Join(r.cgroupRoot, SlurmCgroupPrefix, fmt.Sprintf("job_%s", jobID))
}

// CgroupExists checks if a cgroup path exists.
func (r *Reader) CgroupExists(cgroupPath string) bool {
	fullPath := cgroupPath
	if !filepath.IsAbs(cgroupPath) {
		fullPath = filepath.Join(r.cgroupRoot, cgroupPath)
	}
	info, err := os.Stat(fullPath)
	return err == nil && info.IsDir()
}

// ReadStats reads all available cgroup statistics from the given path.
func (r *Reader) ReadStats(cgroupPath string) (*Stats, error) {
	fullPath := cgroupPath
	if !filepath.IsAbs(cgroupPath) {
		fullPath = filepath.Join(r.cgroupRoot, cgroupPath)
	}

	stats := &Stats{
		CgroupPath: cgroupPath,
		Timestamp:  time.Now(),
	}

	// Read CPU stats
	if err := r.readCPUStats(fullPath, stats); err != nil {
		// CPU stats are important but don't fail if unavailable
		// (might be a memory-only cgroup)
	}

	// Read memory stats
	if err := r.readMemoryStats(fullPath, stats); err != nil {
		// Memory stats are important but don't fail if unavailable
	}

	// Read IO stats
	if err := r.readIOStats(fullPath, stats); err != nil {
		// IO stats are optional
	}

	return stats, nil
}

// readCPUStats reads cpu.stat file.
func (r *Reader) readCPUStats(cgroupPath string, stats *Stats) error {
	cpuStatPath := filepath.Join(cgroupPath, "cpu.stat")
	file, err := os.Open(cpuStatPath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) != 2 {
			continue
		}

		key := parts[0]
		value, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			continue
		}

		switch key {
		case "usage_usec":
			stats.CPUUsageUsec = value
		case "user_usec":
			stats.CPUUserUsec = value
		case "system_usec":
			stats.CPUSystemUsec = value
		case "nr_periods":
			stats.NrPeriods = value
		case "nr_throttled":
			stats.NrThrottled = value
		case "throttled_usec":
			stats.ThrottledUsec = value
		}
	}

	return scanner.Err()
}

// readMemoryStats reads memory.current, memory.max, and memory.peak files.
func (r *Reader) readMemoryStats(cgroupPath string, stats *Stats) error {
	// Read current memory usage
	currentPath := filepath.Join(cgroupPath, "memory.current")
	current, err := readUint64File(currentPath)
	if err == nil {
		stats.MemoryCurrentBytes = current
	}

	// Read memory limit (max)
	maxPath := filepath.Join(cgroupPath, "memory.max")
	max, err := readUint64OrMax(maxPath)
	if err == nil {
		stats.MemoryMaxBytes = max
	}

	// Read peak memory usage (if available)
	peakPath := filepath.Join(cgroupPath, "memory.peak")
	peak, err := readUint64File(peakPath)
	if err == nil {
		stats.MemoryPeakBytes = peak
	}

	// Read swap usage
	swapPath := filepath.Join(cgroupPath, "memory.swap.current")
	swap, err := readUint64File(swapPath)
	if err == nil {
		stats.MemorySwapBytes = swap
	}

	return nil
}

// readIOStats reads io.stat file and aggregates across all devices.
func (r *Reader) readIOStats(cgroupPath string, stats *Stats) error {
	ioStatPath := filepath.Join(cgroupPath, "io.stat")
	file, err := os.Open(ioStatPath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// Format: "major:minor rbytes=N wbytes=N rios=N wios=N ..."
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		for _, part := range parts[1:] {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) != 2 {
				continue
			}

			value, err := strconv.ParseUint(kv[1], 10, 64)
			if err != nil {
				continue
			}

			switch kv[0] {
			case "rbytes":
				stats.IOReadBytes += value
			case "wbytes":
				stats.IOWriteBytes += value
			case "rios":
				stats.IOReadOps += value
			case "wios":
				stats.IOWriteOps += value
			}
		}
	}

	return scanner.Err()
}

// CalculateCPUPercent calculates CPU usage percentage between two samples.
// Returns the percentage of CPU time used between the two samples.
func CalculateCPUPercent(prev, curr *Stats, numCPUs int) float64 {
	if prev == nil || curr == nil {
		return 0
	}

	// Time elapsed in microseconds
	elapsed := curr.Timestamp.Sub(prev.Timestamp).Microseconds()
	if elapsed <= 0 {
		return 0
	}

	// CPU time used in microseconds
	cpuDelta := curr.CPUUsageUsec - prev.CPUUsageUsec

	// Calculate percentage (considering number of CPUs available to the cgroup)
	// 100% means fully utilizing all allocated CPUs
	if numCPUs <= 0 {
		numCPUs = 1
	}

	percent := (float64(cpuDelta) / float64(elapsed)) * 100.0 / float64(numCPUs)

	// Clamp to valid range
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}

	return percent
}

// readUint64File reads a single uint64 value from a file.
func readUint64File(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
}

// readUint64OrMax reads a uint64 or returns MaxUint64 if the file contains "max".
func readUint64OrMax(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	content := strings.TrimSpace(string(data))
	if content == "max" {
		return ^uint64(0), nil // MaxUint64
	}
	return strconv.ParseUint(content, 10, 64)
}

// ListJobCgroups lists all job cgroups under the Slurm cgroup hierarchy.
// Returns a map of job ID to cgroup path.
func (r *Reader) ListJobCgroups() (map[string]string, error) {
	slurmCgroupPath := filepath.Join(r.cgroupRoot, SlurmCgroupPrefix)

	entries, err := os.ReadDir(slurmCgroupPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read slurm cgroup directory: %w", err)
	}

	result := make(map[string]string)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		name := entry.Name()
		// Look for directories matching job_<id> pattern
		if strings.HasPrefix(name, "job_") {
			jobID := strings.TrimPrefix(name, "job_")
			result[jobID] = filepath.Join(slurmCgroupPath, name)
		}
	}

	return result, nil
}

// FindCgroupForJob attempts to find the cgroup path for a given job ID.
// It checks both the standard Slurm path and custom paths.
func (r *Reader) FindCgroupForJob(jobID string) (string, error) {
	// Try standard Slurm path first
	standardPath := r.GetJobCgroupPath(jobID)
	if r.CgroupExists(standardPath) {
		return standardPath, nil
	}

	// Try alternative Slurm cgroup layouts
	alternatives := []string{
		filepath.Join(r.cgroupRoot, "slurm", fmt.Sprintf("uid_%s", jobID)),
		filepath.Join(r.cgroupRoot, "slurm", fmt.Sprintf("job_%s", jobID)),
		filepath.Join(r.cgroupRoot, "system.slice", fmt.Sprintf("slurmstepd-job_%s.scope", jobID)),
	}

	for _, path := range alternatives {
		if r.CgroupExists(path) {
			return path, nil
		}
	}

	return "", fmt.Errorf("cgroup not found for job %s", jobID)
}
