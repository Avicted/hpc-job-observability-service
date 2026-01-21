// Package scheduler provides an abstraction layer for job scheduling systems.
package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// SlurmConfig contains configuration for connecting to a SLURM REST API.
type SlurmConfig struct {
	// BaseURL is the base URL of the slurmrestd server (e.g., "http://localhost:6820")
	BaseURL string
	// APIVersion is the SLURM REST API version (e.g., "v0.0.44")
	APIVersion string
	// AuthToken is the JWT or other auth token for slurmrestd
	AuthToken string
	// HTTPClient is an optional custom HTTP client
	HTTPClient *http.Client
}

// SlurmJobSource provides job information from a SLURM cluster via slurmrestd.
type SlurmJobSource struct {
	config      SlurmConfig
	httpClient  *http.Client
	stateMapper *StateMapping
}

// NewSlurmJobSource creates a new SLURM job source.
func NewSlurmJobSource(config SlurmConfig) *SlurmJobSource {
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout: 30 * time.Second,
		}
	}

	return &SlurmJobSource{
		config:      config,
		httpClient:  client,
		stateMapper: NewSlurmStateMapping(),
	}
}

// Type returns the scheduler type.
func (s *SlurmJobSource) Type() SchedulerType {
	return SchedulerTypeSlurm
}

// ListJobs fetches jobs from SLURM.
func (s *SlurmJobSource) ListJobs(ctx context.Context, filter JobFilter) ([]*Job, error) {
	url := s.buildURL("/slurm/%s/jobs/", s.config.APIVersion)

	resp, err := s.doRequest(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to list jobs: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("SLURM API error: %s - %s", resp.Status, string(body))
	}

	var slurmResp slurmJobsResponse
	if err := json.NewDecoder(resp.Body).Decode(&slurmResp); err != nil {
		return nil, fmt.Errorf("failed to decode SLURM response: %w", err)
	}

	var jobs []*Job
	for _, sj := range slurmResp.Jobs {
		job := s.convertSlurmJob(&sj)
		if s.matchesFilter(job, filter) {
			jobs = append(jobs, job)
		}
	}

	// Apply pagination
	start := filter.Offset
	if start > len(jobs) {
		return []*Job{}, nil
	}

	end := start + filter.Limit
	if end > len(jobs) || filter.Limit == 0 {
		end = len(jobs)
	}

	return jobs[start:end], nil
}

// GetJob fetches a specific job from SLURM.
func (s *SlurmJobSource) GetJob(ctx context.Context, id string) (*Job, error) {
	url := s.buildURL("/slurm/%s/job/%s", s.config.APIVersion, id)

	resp, err := s.doRequest(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("SLURM API error: %s - %s", resp.Status, string(body))
	}

	var slurmResp slurmJobsResponse
	if err := json.NewDecoder(resp.Body).Decode(&slurmResp); err != nil {
		return nil, fmt.Errorf("failed to decode SLURM response: %w", err)
	}

	if len(slurmResp.Jobs) == 0 {
		return nil, nil
	}

	return s.convertSlurmJob(&slurmResp.Jobs[0]), nil
}

// GetJobMetrics is not fully supported by SLURM REST API.
// For detailed metrics, use Slurmdb accounting endpoints or external monitoring.
func (s *SlurmJobSource) GetJobMetrics(ctx context.Context, jobID string) ([]*MetricSample, error) {
	// SLURM REST API doesn't provide time-series metrics directly.
	// This would require integration with slurmdb or external monitoring.
	return nil, nil
}

// SupportsMetrics returns false as SLURM REST API has limited metrics support.
func (s *SlurmJobSource) SupportsMetrics() bool {
	return false
}

// buildURL constructs the full URL for a SLURM API endpoint.
func (s *SlurmJobSource) buildURL(format string, args ...interface{}) string {
	path := fmt.Sprintf(format, args...)
	return s.config.BaseURL + path
}

// doRequest performs an HTTP request to the SLURM API.
func (s *SlurmJobSource) doRequest(ctx context.Context, method, url string, body interface{}) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(jsonBody)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	if s.config.AuthToken != "" {
		req.Header.Set("X-SLURM-USER-TOKEN", s.config.AuthToken)
	}

	return s.httpClient.Do(req)
}

// convertSlurmJob converts a SLURM job to our normalized Job model.
func (s *SlurmJobSource) convertSlurmJob(sj *slurmJob) *Job {
	// Use user_name if available, otherwise fall back to account
	user := sj.UserName
	if user == "" {
		user = sj.Account
	}

	job := &Job{
		ID:    strconv.FormatUint(sj.JobID, 10),
		User:  user,
		Nodes: s.parseNodes(sj.Nodes),
		State: s.stateMapper.NormalizeState(sj.JobState),
	}

	// Set allocated resources (Slurm doesn't provide real-time usage)
	// We use allocated values as initial estimates
	if sj.Cpus > 0 {
		// Store allocated CPUs as a percentage placeholder (will be updated by collector)
		job.CPUUsage = 0 // Actual usage unknown, collector will update for running jobs
	}

	// Try to get memory from various sources (in order of preference)
	if sj.MemoryPerNode > 0 {
		job.MemoryUsageMB = int64(sj.MemoryPerNode)
	} else if sj.MemoryPerCpu > 0 && sj.Cpus > 0 {
		job.MemoryUsageMB = int64(sj.MemoryPerCpu * uint64(sj.Cpus))
	} else if sj.Memory > 0 {
		job.MemoryUsageMB = int64(sj.Memory)
	} else if mem := parseMemoryFromTRES(sj.TresAllocStr); mem > 0 {
		job.MemoryUsageMB = mem
	} else if mem := parseMemoryFromTRES(sj.TresReqStr); mem > 0 {
		job.MemoryUsageMB = mem
	}

	// Parse timestamps (SLURM uses Unix timestamps)
	if sj.StartTime > 0 {
		t := time.Unix(int64(sj.StartTime), 0)
		job.StartTime = t
	}

	if sj.EndTime > 0 && sj.EndTime != 0 {
		t := time.Unix(int64(sj.EndTime), 0)
		job.EndTime = &t
	}

	// Calculate runtime
	if job.EndTime != nil {
		job.RuntimeSeconds = job.EndTime.Sub(job.StartTime).Seconds()
	} else if job.State == JobStateRunning {
		job.RuntimeSeconds = time.Since(job.StartTime).Seconds()
	}

	// Build scheduler info with raw SLURM data
	job.Scheduler = &SchedulerInfo{
		Type:          SchedulerTypeSlurm,
		ExternalJobID: strconv.FormatUint(sj.JobID, 10),
		RawState:      sj.JobState,
		Partition:     sj.Partition,
		Account:       sj.Account,
		QoS:           sj.QoS,
	}

	if sj.SubmitTime > 0 {
		t := time.Unix(int64(sj.SubmitTime), 0)
		job.Scheduler.SubmitTime = &t
	}

	if sj.Priority > 0 {
		priority := int(sj.Priority)
		job.Scheduler.Priority = &priority
	}

	if sj.ExitCode != 0 {
		exitCode := int(sj.ExitCode)
		job.Scheduler.ExitCode = &exitCode
	}

	// Store extra SLURM-specific fields
	job.Scheduler.Extra = map[string]interface{}{
		"job_name":      sj.Name,
		"tres_alloc":    sj.TresAllocStr,
		"tres_req":      sj.TresReqStr,
		"cpus":          sj.Cpus,
		"memory":        sj.Memory,
		"time_limit":    sj.TimeLimit,
		"state_reason":  sj.StateReason,
		"eligible_time": sj.EligibleTime,
	}

	return job
}

// parseNodes converts SLURM node list format to slice.
// SLURM uses formats like "node[01-04]" or "node01,node02".
func (s *SlurmJobSource) parseNodes(nodeList string) []string {
	if nodeList == "" {
		return nil
	}

	// Simple case: comma-separated list
	if !strings.Contains(nodeList, "[") {
		return strings.Split(nodeList, ",")
	}

	// TODO: Implement proper SLURM nodelist expansion (e.g., "node[01-04]" -> ["node01", "node02", "node03", "node04"])
	// For now, return as-is
	return []string{nodeList}
}

// parseMemoryFromTRES extracts memory in MB from TRES string.
// TRES format: "cpu=1,mem=31995M,node=1,billing=1" or "mem=1G"
func parseMemoryFromTRES(tresStr string) int64 {
	if tresStr == "" {
		return 0
	}

	// Split by comma to get individual TRES components
	parts := strings.Split(tresStr, ",")
	for _, part := range parts {
		if strings.HasPrefix(part, "mem=") {
			memStr := strings.TrimPrefix(part, "mem=")
			return parseMemoryValue(memStr)
		}
	}
	return 0
}

// parseMemoryValue converts Slurm memory string to MB.
// Handles formats like "31995M", "32G", "1024K", "1073741824" (bytes)
func parseMemoryValue(memStr string) int64 {
	if memStr == "" {
		return 0
	}

	memStr = strings.TrimSpace(memStr)
	if len(memStr) == 0 {
		return 0
	}

	// Check for suffix
	lastChar := memStr[len(memStr)-1]
	if lastChar >= '0' && lastChar <= '9' {
		// No suffix, assume bytes
		val, err := strconv.ParseInt(memStr, 10, 64)
		if err != nil {
			return 0
		}
		return val / (1024 * 1024) // Convert bytes to MB
	}

	numPart := memStr[:len(memStr)-1]
	val, err := strconv.ParseInt(numPart, 10, 64)
	if err != nil {
		return 0
	}

	switch lastChar {
	case 'K', 'k':
		return val / 1024 // KB to MB
	case 'M', 'm':
		return val // Already in MB
	case 'G', 'g':
		return val * 1024 // GB to MB
	case 'T', 't':
		return val * 1024 * 1024 // TB to MB
	default:
		return val // Unknown suffix, return as-is
	}
}

func (s *SlurmJobSource) matchesFilter(job *Job, filter JobFilter) bool {
	if filter.State != nil && job.State != *filter.State {
		return false
	}
	if filter.User != nil && job.User != *filter.User {
		return false
	}
	if filter.Partition != nil && (job.Scheduler == nil || job.Scheduler.Partition != *filter.Partition) {
		return false
	}
	return true
}

// ListNodes fetches all compute nodes from SLURM.
func (s *SlurmJobSource) ListNodes(ctx context.Context) ([]*Node, error) {
	url := s.buildURL("/slurm/%s/nodes/", s.config.APIVersion)

	resp, err := s.doRequest(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to list nodes: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("SLURM API error: %s - %s", resp.Status, string(body))
	}

	var slurmResp slurmNodesResponse
	if err := json.NewDecoder(resp.Body).Decode(&slurmResp); err != nil {
		return nil, fmt.Errorf("failed to decode SLURM nodes response: %w", err)
	}

	var nodes []*Node
	for _, sn := range slurmResp.Nodes {
		nodes = append(nodes, s.convertSlurmNode(&sn))
	}

	return nodes, nil
}

// convertSlurmNode converts a SLURM node to our normalized Node model.
func (s *SlurmJobSource) convertSlurmNode(sn *slurmNode) *Node {
	node := &Node{
		Name:         sn.Name,
		Hostname:     sn.Hostname,
		State:        s.mapNodeState(sn.State),
		CPUs:         int(sn.Cpus),
		Cores:        int(sn.Cores),
		Sockets:      int(sn.Sockets),
		RealMemoryMB: int64(sn.RealMemory),
		FreeMemoryMB: int64(sn.FreeMemory),
		// Slurm returns cpu_load as load*100 (e.g., 240 = 2.40)
		CPULoad: float64(sn.CPULoad) / 100.0,
	}

	// Parse features if present
	if sn.Features != "" {
		node.Features = strings.Split(sn.Features, ",")
	}

	// Parse allocated resources from TRES string if available
	// Format: "cpu=4,mem=8192M"
	if sn.TresUsed != "" {
		if allocCPU := parseTRESValue(sn.TresUsed, "cpu"); allocCPU > 0 {
			node.AllocatedCPUs = int(allocCPU)
		}
		if allocMem := parseMemoryFromTRES(sn.TresUsed); allocMem > 0 {
			node.AllocatedMemMB = allocMem
		}
	}

	return node
}

// mapNodeState converts SLURM node state to normalized NodeState.
func (s *SlurmJobSource) mapNodeState(state string) NodeState {
	// Slurm states can have modifiers like "idle+cloud", "down*"
	state = strings.ToLower(state)
	state = strings.Split(state, "+")[0]
	state = strings.TrimSuffix(state, "*")
	state = strings.TrimSuffix(state, "~")
	state = strings.TrimSuffix(state, "#")

	switch state {
	case "idle":
		return NodeStateIdle
	case "alloc", "allocated":
		return NodeStateAllocated
	case "mix", "mixed":
		return NodeStateMixed
	case "drain", "drained", "draining":
		return NodeStateDrained
	case "down":
		return NodeStateDown
	default:
		return NodeStateUnknown
	}
}

// parseTRESValue extracts a numeric value from a TRES string for a given key.
// Format: "cpu=4,mem=8192M,node=1"
func parseTRESValue(tresStr, key string) int64 {
	if tresStr == "" {
		return 0
	}

	parts := strings.Split(tresStr, ",")
	for _, part := range parts {
		if strings.HasPrefix(part, key+"=") {
			valStr := strings.TrimPrefix(part, key+"=")
			// Remove any suffix like M, G, etc. for non-memory values
			valStr = strings.TrimRight(valStr, "MGKTmgkt")
			val, err := strconv.ParseInt(valStr, 10, 64)
			if err != nil {
				return 0
			}
			return val
		}
	}
	return 0
}

// SLURM API response structures

type slurmJobsResponse struct {
	Jobs   []slurmJob `json:"jobs"`
	Errors []struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	} `json:"errors,omitempty"`
}

type slurmNodesResponse struct {
	Nodes  []slurmNode `json:"nodes"`
	Errors []struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	} `json:"errors,omitempty"`
}

type slurmNode struct {
	Name        string `json:"name"`
	Hostname    string `json:"hostname"`
	Address     string `json:"address"`
	State       string `json:"state"`
	Cpus        uint32 `json:"cpus"`
	Cores       uint32 `json:"cores"`
	Sockets     uint32 `json:"sockets"`
	Threads     uint32 `json:"threads"`
	RealMemory  uint64 `json:"real_memory"`
	FreeMemory  uint64 `json:"free_memory"`
	CPULoad     uint32 `json:"cpu_load"`
	Features    string `json:"features"`
	TresUsed    string `json:"tres_used"`
	AllocCpus   uint32 `json:"alloc_cpus"`
	AllocMemory uint64 `json:"alloc_memory"`
	Partitions  string `json:"partitions"`
	BootTime    uint64 `json:"boot_time"`
	SlurmdStart uint64 `json:"slurmd_start_time"`
}

type slurmJob struct {
	JobID         uint64 `json:"job_id"`
	Name          string `json:"name"`
	UserName      string `json:"user_name"`
	UserID        uint32 `json:"user_id"`
	Nodes         string `json:"nodes"`
	NodeCount     uint32 `json:"node_count"`
	JobState      string `json:"job_state"`
	Partition     string `json:"partition"`
	Account       string `json:"account"`
	QoS           string `json:"qos"`
	Priority      uint32 `json:"priority"`
	StartTime     uint64 `json:"start_time"`
	EndTime       uint64 `json:"end_time"`
	SubmitTime    uint64 `json:"submit_time"`
	EligibleTime  uint64 `json:"eligible_time"`
	TimeLimit     uint32 `json:"time_limit"`
	Cpus          uint32 `json:"cpus"`
	Memory        uint64 `json:"memory"`
	MemoryPerNode uint64 `json:"memory_per_node"`
	MemoryPerCpu  uint64 `json:"memory_per_cpu"`
	TresAllocStr  string `json:"tres_alloc_str"`
	TresReqStr    string `json:"tres_req_str"`
	StateReason   string `json:"state_reason"`
	ExitCode      int32  `json:"exit_code"`
}
