package cgroup

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewReader(t *testing.T) {
	reader := NewReader()
	if reader.cgroupRoot != DefaultCgroupRoot {
		t.Errorf("expected default cgroup root %s, got %s", DefaultCgroupRoot, reader.cgroupRoot)
	}
}

func TestNewReaderWithRoot(t *testing.T) {
	customRoot := "/custom/cgroup"
	reader := NewReaderWithRoot(customRoot)
	if reader.cgroupRoot != customRoot {
		t.Errorf("expected custom cgroup root %s, got %s", customRoot, reader.cgroupRoot)
	}
}

func TestGetJobCgroupPath(t *testing.T) {
	reader := NewReader()
	path := reader.GetJobCgroupPath("12345")
	expected := filepath.Join(DefaultCgroupRoot, SlurmCgroupPrefix, "job_12345")
	if path != expected {
		t.Errorf("expected path %s, got %s", expected, path)
	}
}

func TestCgroupExists(t *testing.T) {
	// Create a temporary directory to simulate cgroup
	tmpDir, err := os.MkdirTemp("", "cgroup-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	reader := NewReaderWithRoot(tmpDir)

	// Test non-existent cgroup
	if reader.CgroupExists("nonexistent") {
		t.Error("expected false for non-existent cgroup")
	}

	// Create a test cgroup directory
	testCgroup := filepath.Join(tmpDir, "test_cgroup")
	if err := os.Mkdir(testCgroup, 0755); err != nil {
		t.Fatalf("failed to create test cgroup: %v", err)
	}

	// Test existing cgroup
	if !reader.CgroupExists(testCgroup) {
		t.Error("expected true for existing cgroup")
	}
}

func TestReadStats(t *testing.T) {
	// Create a temporary directory with mock cgroup files
	tmpDir, err := os.MkdirTemp("", "cgroup-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create mock cpu.stat
	cpuStat := `usage_usec 1000000
user_usec 600000
system_usec 400000
nr_periods 100
nr_throttled 5
throttled_usec 50000`
	if err := os.WriteFile(filepath.Join(tmpDir, "cpu.stat"), []byte(cpuStat), 0644); err != nil {
		t.Fatalf("failed to write cpu.stat: %v", err)
	}

	// Create mock memory files
	if err := os.WriteFile(filepath.Join(tmpDir, "memory.current"), []byte("104857600\n"), 0644); err != nil {
		t.Fatalf("failed to write memory.current: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "memory.max"), []byte("1073741824\n"), 0644); err != nil {
		t.Fatalf("failed to write memory.max: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "memory.peak"), []byte("209715200\n"), 0644); err != nil {
		t.Fatalf("failed to write memory.peak: %v", err)
	}

	// Create mock io.stat
	ioStat := `259:0 rbytes=1048576 wbytes=2097152 rios=100 wios=50
259:1 rbytes=524288 wbytes=1048576 rios=50 wios=25`
	if err := os.WriteFile(filepath.Join(tmpDir, "io.stat"), []byte(ioStat), 0644); err != nil {
		t.Fatalf("failed to write io.stat: %v", err)
	}

	reader := NewReaderWithRoot("/")
	stats, err := reader.ReadStats(tmpDir)
	if err != nil {
		t.Fatalf("ReadStats failed: %v", err)
	}

	// Verify CPU stats
	if stats.CPUUsageUsec != 1000000 {
		t.Errorf("expected CPUUsageUsec 1000000, got %d", stats.CPUUsageUsec)
	}
	if stats.CPUUserUsec != 600000 {
		t.Errorf("expected CPUUserUsec 600000, got %d", stats.CPUUserUsec)
	}
	if stats.CPUSystemUsec != 400000 {
		t.Errorf("expected CPUSystemUsec 400000, got %d", stats.CPUSystemUsec)
	}
	if stats.NrThrottled != 5 {
		t.Errorf("expected NrThrottled 5, got %d", stats.NrThrottled)
	}

	// Verify memory stats
	if stats.MemoryCurrentBytes != 104857600 {
		t.Errorf("expected MemoryCurrentBytes 104857600, got %d", stats.MemoryCurrentBytes)
	}
	if stats.MemoryMaxBytes != 1073741824 {
		t.Errorf("expected MemoryMaxBytes 1073741824, got %d", stats.MemoryMaxBytes)
	}
	if stats.MemoryPeakBytes != 209715200 {
		t.Errorf("expected MemoryPeakBytes 209715200, got %d", stats.MemoryPeakBytes)
	}

	// Verify IO stats (aggregated)
	expectedReadBytes := uint64(1048576 + 524288)
	if stats.IOReadBytes != expectedReadBytes {
		t.Errorf("expected IOReadBytes %d, got %d", expectedReadBytes, stats.IOReadBytes)
	}
	expectedWriteBytes := uint64(2097152 + 1048576)
	if stats.IOWriteBytes != expectedWriteBytes {
		t.Errorf("expected IOWriteBytes %d, got %d", expectedWriteBytes, stats.IOWriteBytes)
	}
}

func TestReadMemoryMax(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cgroup-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Test "max" value
	if err := os.WriteFile(filepath.Join(tmpDir, "memory.max"), []byte("max\n"), 0644); err != nil {
		t.Fatalf("failed to write memory.max: %v", err)
	}
	max, err := readUint64OrMax(filepath.Join(tmpDir, "memory.max"))
	if err != nil {
		t.Fatalf("readUint64OrMax failed: %v", err)
	}
	if max != ^uint64(0) {
		t.Errorf("expected MaxUint64 for 'max', got %d", max)
	}
}

func TestCalculateCPUPercent(t *testing.T) {
	now := time.Now()
	prev := &Stats{
		CPUUsageUsec: 1000000, // 1 second
		Timestamp:    now,
	}
	curr := &Stats{
		CPUUsageUsec: 2000000, // 2 seconds (1 second delta)
		Timestamp:    now.Add(time.Second),
	}

	// With 1 CPU, 1 second of CPU time in 1 second = 100%
	percent := CalculateCPUPercent(prev, curr, 1)
	if percent != 100.0 {
		t.Errorf("expected 100%% CPU usage, got %.2f%%", percent)
	}

	// With 4 CPUs, 1 second of CPU time in 1 second = 25%
	percent = CalculateCPUPercent(prev, curr, 4)
	if percent != 25.0 {
		t.Errorf("expected 25%% CPU usage with 4 CPUs, got %.2f%%", percent)
	}
}

func TestCalculateCPUPercentEdgeCases(t *testing.T) {
	// Test with nil prev
	percent := CalculateCPUPercent(nil, &Stats{}, 1)
	if percent != 0 {
		t.Errorf("expected 0%% with nil prev, got %.2f%%", percent)
	}

	// Test with nil curr
	percent = CalculateCPUPercent(&Stats{}, nil, 1)
	if percent != 0 {
		t.Errorf("expected 0%% with nil curr, got %.2f%%", percent)
	}

	// Test with zero elapsed time
	now := time.Now()
	prev := &Stats{CPUUsageUsec: 1000000, Timestamp: now}
	curr := &Stats{CPUUsageUsec: 2000000, Timestamp: now} // Same timestamp
	percent = CalculateCPUPercent(prev, curr, 1)
	if percent != 0 {
		t.Errorf("expected 0%% with zero elapsed time, got %.2f%%", percent)
	}
}

func TestListJobCgroups(t *testing.T) {
	// Create a temporary directory structure
	tmpDir, err := os.MkdirTemp("", "cgroup-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create mock Slurm cgroup structure
	slurmPath := filepath.Join(tmpDir, SlurmCgroupPrefix)
	if err := os.MkdirAll(slurmPath, 0755); err != nil {
		t.Fatalf("failed to create slurm path: %v", err)
	}

	// Create job cgroups
	jobs := []string{"job_123", "job_456", "job_789"}
	for _, job := range jobs {
		if err := os.Mkdir(filepath.Join(slurmPath, job), 0755); err != nil {
			t.Fatalf("failed to create job cgroup %s: %v", job, err)
		}
	}

	// Create a non-job directory (should be ignored)
	if err := os.Mkdir(filepath.Join(slurmPath, "step_0"), 0755); err != nil {
		t.Fatalf("failed to create step cgroup: %v", err)
	}

	reader := NewReaderWithRoot(tmpDir)
	jobCgroups, err := reader.ListJobCgroups()
	if err != nil {
		t.Fatalf("ListJobCgroups failed: %v", err)
	}

	if len(jobCgroups) != 3 {
		t.Errorf("expected 3 job cgroups, got %d", len(jobCgroups))
	}

	expectedJobs := map[string]bool{"123": true, "456": true, "789": true}
	for jobID := range jobCgroups {
		if !expectedJobs[jobID] {
			t.Errorf("unexpected job ID: %s", jobID)
		}
	}
}

func TestIsCgroupV2(t *testing.T) {
	// Create a temporary directory structure
	tmpDir, err := os.MkdirTemp("", "cgroup-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	reader := NewReaderWithRoot(tmpDir)

	// Without cgroup.controllers file, should return false
	if reader.IsCgroupV2() {
		t.Error("expected false without cgroup.controllers file")
	}

	// Create cgroup.controllers file
	if err := os.WriteFile(filepath.Join(tmpDir, "cgroup.controllers"), []byte("cpu memory io"), 0644); err != nil {
		t.Fatalf("failed to create cgroup.controllers: %v", err)
	}

	// With cgroup.controllers file, should return true
	if !reader.IsCgroupV2() {
		t.Error("expected true with cgroup.controllers file")
	}
}
