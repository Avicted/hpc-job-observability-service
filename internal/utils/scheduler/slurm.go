// Package scheduler provides an abstraction layer for job scheduling systems.
package scheduler

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Avicted/hpc-job-observability-service/internal/slurmclient"
)

// SlurmConfig contains configuration for connecting to a Slurm REST API.
type SlurmConfig struct {
	// BaseURL is the base URL of the slurmrestd server (e.g., "http://localhost:6820")
	BaseURL string

	// APIVersion is the Slurm REST API version (e.g., "v0.0.36")
	APIVersion string

	// AuthToken is the JWT token for slurmrestd authentication
	AuthToken string

	// ClusterName identifies the Slurm cluster for multi-cluster setups
	ClusterName string

	// HTTPClient is an optional custom HTTP client (uses default if nil)
	HTTPClient *http.Client
}

// SlurmClient defines the interface for Slurm API operations.
// This interface allows mocking in tests.
type SlurmClient interface {
	SlurmctldGetJobsWithResponse(ctx context.Context, params *slurmclient.SlurmctldGetJobsParams, reqEditors ...slurmclient.RequestEditorFn) (*slurmclient.SlurmctldGetJobsResponse, error)
	SlurmctldGetJobWithResponse(ctx context.Context, jobId string, reqEditors ...slurmclient.RequestEditorFn) (*slurmclient.SlurmctldGetJobResponse, error)
	SlurmctldGetNodesWithResponse(ctx context.Context, params *slurmclient.SlurmctldGetNodesParams, reqEditors ...slurmclient.RequestEditorFn) (*slurmclient.SlurmctldGetNodesResponse, error)
	SlurmctldPingWithResponse(ctx context.Context, reqEditors ...slurmclient.RequestEditorFn) (*slurmclient.SlurmctldPingResponse, error)
}

// SlurmJobSource provides job information from a Slurm cluster via slurmrestd.
type SlurmJobSource struct {
	config      SlurmConfig
	client      SlurmClient
	stateMapper *StateMapping
	authEditor  slurmclient.RequestEditorFn
}

// NewSlurmJobSource creates a new Slurm job source.
func NewSlurmJobSource(config SlurmConfig) (*SlurmJobSource, error) {
	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 30 * time.Second,
		}
	}

	// Use just the base URL - the generated client already includes /slurm/v0.0.36 in paths
	serverURL := config.BaseURL

	client, err := slurmclient.NewClientWithResponses(
		serverURL,
		slurmclient.WithHTTPClient(httpClient),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create slurm client: %w", err)
	}

	source := &SlurmJobSource{
		config:      config,
		client:      client,
		stateMapper: NewSlurmStateMapping(),
	}

	// Set up auth header if token provided
	if config.AuthToken != "" {
		source.authEditor = func(ctx context.Context, req *http.Request) error {
			req.Header.Set("X-SLURM-USER-TOKEN", config.AuthToken)
			return nil
		}
	}

	return source, nil
}

// NewSlurmJobSourceWithClient creates a Slurm job source with a custom client (for testing).
func NewSlurmJobSourceWithClient(config SlurmConfig, client SlurmClient) *SlurmJobSource {
	source := &SlurmJobSource{
		config:      config,
		client:      client,
		stateMapper: NewSlurmStateMapping(),
	}

	if config.AuthToken != "" {
		source.authEditor = func(ctx context.Context, req *http.Request) error {
			req.Header.Set("X-SLURM-USER-TOKEN", config.AuthToken)
			return nil
		}
	}

	return source
}

// Type returns the scheduler type.
func (s *SlurmJobSource) Type() SchedulerType {
	return SchedulerTypeSlurm
}

// ListJobs fetches jobs from Slurm.
func (s *SlurmJobSource) ListJobs(ctx context.Context, filter JobFilter) ([]*Job, error) {
	resp, err := s.client.SlurmctldGetJobsWithResponse(ctx, nil, s.requestEditors()...)
	if err != nil {
		return nil, fmt.Errorf("failed to list jobs: %w", err)
	}

	if resp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("slurm API error: %s - %s", resp.Status(), string(resp.Body))
	}

	if resp.JSON200 == nil {
		return nil, fmt.Errorf("slurm API returned no data")
	}

	if resp.JSON200.Errors != nil && len(*resp.JSON200.Errors) > 0 {
		errs := *resp.JSON200.Errors
		if errs[0].Error != nil {
			return nil, fmt.Errorf("slurm API error: %s", *errs[0].Error)
		}
	}

	var jobs []*Job
	if resp.JSON200.Jobs != nil {
		for _, sj := range *resp.JSON200.Jobs {
			job := s.convertJob(&sj)
			if s.matchesFilter(job, filter) {
				jobs = append(jobs, job)
			}
		}
	}

	// Apply pagination
	return s.applyPagination(jobs, filter), nil
}

// GetJob fetches a specific job from Slurm.
func (s *SlurmJobSource) GetJob(ctx context.Context, id string) (*Job, error) {
	resp, err := s.client.SlurmctldGetJobWithResponse(ctx, id, s.requestEditors()...)
	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	if resp.StatusCode() == http.StatusNotFound {
		return nil, nil
	}

	if resp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("slurm API error: %s - %s", resp.Status(), string(resp.Body))
	}

	if resp.JSON200 == nil || resp.JSON200.Jobs == nil || len(*resp.JSON200.Jobs) == 0 {
		return nil, nil
	}

	jobs := *resp.JSON200.Jobs
	return s.convertJob(&jobs[0]), nil
}

// GetJobMetrics returns metrics for a job.
// Slurm REST API does not provide time-series metrics directly.
func (s *SlurmJobSource) GetJobMetrics(ctx context.Context, jobID string) ([]*MetricSample, error) {
	return nil, nil
}

// SupportsMetrics returns false as Slurm REST API has limited metrics support.
func (s *SlurmJobSource) SupportsMetrics() bool {
	return false
}

// ListNodes fetches all compute nodes from Slurm.
func (s *SlurmJobSource) ListNodes(ctx context.Context) ([]*Node, error) {
	resp, err := s.client.SlurmctldGetNodesWithResponse(ctx, nil, s.requestEditors()...)
	if err != nil {
		return nil, fmt.Errorf("failed to list nodes: %w", err)
	}

	if resp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("slurm API error: %s - %s", resp.Status(), string(resp.Body))
	}

	if resp.JSON200 == nil {
		return nil, fmt.Errorf("slurm API returned no data")
	}

	var nodes []*Node
	if resp.JSON200.Nodes != nil {
		for _, sn := range *resp.JSON200.Nodes {
			nodes = append(nodes, s.convertNode(&sn))
		}
	}

	return nodes, nil
}

// Ping checks if the Slurm API is reachable.
func (s *SlurmJobSource) Ping(ctx context.Context) error {
	resp, err := s.client.SlurmctldPingWithResponse(ctx, s.requestEditors()...)
	if err != nil {
		return fmt.Errorf("failed to ping slurm: %w", err)
	}

	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("slurm ping failed: %s", resp.Status())
	}

	return nil
}

// requestEditors returns the request editors to use for API calls.
func (s *SlurmJobSource) requestEditors() []slurmclient.RequestEditorFn {
	if s.authEditor != nil {
		return []slurmclient.RequestEditorFn{s.authEditor}
	}
	return nil
}

// convertJob converts a Slurm API job to our normalized Job model.
func (s *SlurmJobSource) convertJob(sj *slurmclient.V0037JobResponseProperties) *Job {
	jobID := fmt.Sprintf("%d", derefInt(sj.JobId))

	// Get initial state from the state mapper
	rawState := derefStr(sj.JobState)
	state := s.stateMapper.NormalizeState(rawState)

	// Slurm reports COMPLETED for jobs that finished execution, even with non-zero exit codes.
	// We need to check the exit code to determine if the job actually failed.
	exitCode := sj.ExitCode
	if state == JobStateCompleted && exitCode != nil && *exitCode != 0 {
		state = JobStateFailed
	}

	job := &Job{
		ID:    jobID,
		User:  s.extractUser(sj),
		Nodes: parseNodeList(derefStr(sj.Nodes)),
		State: state,
	}

	// Node count
	job.NodeCount = derefInt(sj.NodeCount)
	if job.NodeCount == 0 && len(job.Nodes) > 0 {
		job.NodeCount = len(job.Nodes)
	}

	// CPU resources
	job.AllocatedCPUs = int64(derefInt(sj.Cpus))
	job.RequestedCPUs = parseTRESInt(derefStr(sj.TresReqStr), "cpu")
	if job.RequestedCPUs == 0 {
		job.RequestedCPUs = job.AllocatedCPUs
	}

	// Memory resources
	job.AllocatedMemMB = parseTRESMemory(derefStr(sj.TresAllocStr))
	job.RequestedMemMB = parseTRESMemory(derefStr(sj.TresReqStr))
	if job.RequestedMemMB == 0 {
		job.RequestedMemMB = job.AllocatedMemMB
	}

	// Memory usage (best effort from various sources)
	job.MemoryUsageMB = s.extractMemoryUsage(sj)

	// GPU resources
	job.AllocatedGPUs = parseTRESInt(derefStr(sj.TresAllocStr), "gres/gpu")
	job.RequestedGPUs = parseTRESInt(derefStr(sj.TresReqStr), "gres/gpu")
	if job.RequestedGPUs == 0 {
		job.RequestedGPUs = job.AllocatedGPUs
	}

	// Timestamps (now int64 Unix timestamps)
	job.StartTime = parseUnixTimeInt64(derefInt64(sj.StartTime))
	if endTime := parseUnixTimeInt64(derefInt64(sj.EndTime)); !endTime.IsZero() {
		if job.State == JobStateCompleted || job.State == JobStateFailed || job.State == JobStateCancelled {
			job.EndTime = &endTime
		}
	}

	// Runtime calculation
	job.RuntimeSeconds = s.calculateRuntime(job)

	// Scheduler-specific metadata
	job.Scheduler = s.buildSchedulerInfo(sj, jobID)

	return job
}

// extractUser gets the user from job properties.
func (s *SlurmJobSource) extractUser(sj *slurmclient.V0037JobResponseProperties) string {
	if user := derefStr(sj.UserName); user != "" {
		return user
	}
	if account := derefStr(sj.Account); account != "" {
		return account
	}
	// Fall back to user ID if nothing else is available
	if uid := derefInt(sj.UserId); uid > 0 {
		return fmt.Sprintf("uid:%d", uid)
	}
	return "unknown"
}

// extractMemoryUsage tries to get memory usage from various sources.
func (s *SlurmJobSource) extractMemoryUsage(sj *slurmclient.V0037JobResponseProperties) int64 {
	if mem := parseIntStr(derefStr(sj.MemoryPerNode)); mem > 0 {
		return int64(mem)
	}
	if memPerCpu := parseIntStr(derefStr(sj.MemoryPerCpu)); memPerCpu > 0 {
		cpus := derefInt(sj.Cpus)
		if cpus > 0 {
			return int64(memPerCpu * cpus)
		}
	}
	if mem := parseTRESMemory(derefStr(sj.TresAllocStr)); mem > 0 {
		return mem
	}
	return parseTRESMemory(derefStr(sj.TresReqStr))
}

// calculateRuntime calculates job runtime in seconds.
func (s *SlurmJobSource) calculateRuntime(job *Job) float64 {
	if job.EndTime != nil {
		return job.EndTime.Sub(job.StartTime).Seconds()
	}
	if job.State == JobStateRunning && !job.StartTime.IsZero() {
		return time.Since(job.StartTime).Seconds()
	}
	return 0
}

// buildSchedulerInfo creates the scheduler metadata block.
func (s *SlurmJobSource) buildSchedulerInfo(sj *slurmclient.V0037JobResponseProperties, jobID string) *SchedulerInfo {
	info := &SchedulerInfo{
		Type:          SchedulerTypeSlurm,
		ExternalJobID: jobID,
		RawState:      derefStr(sj.JobState),
		Partition:     derefStr(sj.Partition),
		Account:       derefStr(sj.Account),
		QoS:           derefStr(sj.Qos),
		StateReason:   derefStr(sj.StateReason),
	}

	if submitTime := parseUnixTimeInt64(derefInt64(sj.SubmitTime)); !submitTime.IsZero() {
		info.SubmitTime = &submitTime
	}

	if priority := derefInt(sj.Priority); priority > 0 {
		p := int64(priority)
		info.Priority = &p
	}

	if sj.ExitCode != nil && *sj.ExitCode != 0 {
		info.ExitCode = sj.ExitCode
	}

	if timeLimit := derefInt64(sj.TimeLimit); timeLimit > 0 {
		t := int(timeLimit)
		info.TimeLimitMins = &t
	}

	info.Extra = map[string]interface{}{
		"job_name":   derefStr(sj.Name),
		"tres_alloc": derefStr(sj.TresAllocStr),
		"tres_req":   derefStr(sj.TresReqStr),
	}

	return info
}

// convertNode converts a Slurm API node to our normalized Node model.
func (s *SlurmJobSource) convertNode(sn *slurmclient.V0037Node) *Node {
	node := &Node{
		Name:         derefStr(sn.Name),
		Hostname:     derefStr(sn.Hostname),
		State:        s.mapNodeState(derefStr(sn.State)),
		CPUs:         derefInt(sn.Cpus),
		Cores:        derefInt(sn.Cores),
		Sockets:      derefInt(sn.Sockets),
		RealMemoryMB: int64(derefInt(sn.RealMemory)),
		FreeMemoryMB: int64(derefInt(sn.FreeMemory)),
	}

	// Slurm returns cpu_load as load*100 (e.g., 240 = 2.40)
	if sn.CpuLoad != nil {
		node.CPULoad = float64(*sn.CpuLoad) / 100.0
	}

	if features := derefStr(sn.Features); features != "" {
		node.Features = strings.Split(features, ",")
	}

	// Parse allocated resources from TRES
	if tres := derefStr(sn.Tres); tres != "" {
		node.AllocatedCPUs = int(parseTRESInt(tres, "cpu"))
		node.AllocatedMemMB = parseTRESMemory(tres)
	}

	return node
}

// mapNodeState converts Slurm node state to normalized NodeState.
func (s *SlurmJobSource) mapNodeState(state string) NodeState {
	state = strings.ToLower(state)
	state = strings.Split(state, "+")[0]
	state = strings.TrimRight(state, "*~#")

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

func (s *SlurmJobSource) applyPagination(jobs []*Job, filter JobFilter) []*Job {
	start := filter.Offset
	if start > len(jobs) {
		return []*Job{}
	}

	end := len(jobs)
	if filter.Limit > 0 && start+filter.Limit < end {
		end = start + filter.Limit
	}

	return jobs[start:end]
}

// Helper functions

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func derefInt(i *int) int {
	if i == nil {
		return 0
	}
	return *i
}

func derefInt64(i *int64) int64 {
	if i == nil {
		return 0
	}
	return *i
}

func parseIntStr(s string) int {
	if s == "" {
		return 0
	}
	v, _ := strconv.Atoi(s)
	return v
}

func parseUnixTimeInt64(ts int64) time.Time {
	if ts <= 0 {
		return time.Time{}
	}
	return time.Unix(ts, 0)
}

// parseNodeList converts Slurm node list format to slice.
// Handles formats like "node01,node02" or "node[01-04]".
func parseNodeList(nodeList string) []string {
	if nodeList == "" {
		return nil
	}

	// Simple comma-separated list
	if !strings.Contains(nodeList, "[") {
		return strings.Split(nodeList, ",")
	}

	// TODO: Implement proper Slurm nodelist expansion
	return []string{nodeList}
}

// parseTRESInt extracts an integer value from a TRES string.
// Format: "cpu=4,mem=8192M,node=1"
func parseTRESInt(tres, key string) int64 {
	if tres == "" {
		return 0
	}

	for _, part := range strings.Split(tres, ",") {
		if strings.HasPrefix(part, key+"=") {
			valStr := strings.TrimPrefix(part, key+"=")
			valStr = strings.TrimRight(valStr, "MGKTmgkt")
			val, _ := strconv.ParseInt(valStr, 10, 64)
			return val
		}
	}
	return 0
}

// parseTRESMemory extracts memory in MB from a TRES string.
// Format: "cpu=1,mem=31995M,node=1"
func parseTRESMemory(tres string) int64 {
	if tres == "" {
		return 0
	}

	for _, part := range strings.Split(tres, ",") {
		if strings.HasPrefix(part, "mem=") {
			return parseMemoryString(strings.TrimPrefix(part, "mem="))
		}
	}
	return 0
}

// parseMemoryString converts Slurm memory string to MB.
// Handles: "31995M", "32G", "1024K", "1073741824" (bytes)
func parseMemoryString(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}

	lastChar := s[len(s)-1]
	if lastChar >= '0' && lastChar <= '9' {
		// No suffix, assume bytes
		val, _ := strconv.ParseInt(s, 10, 64)
		return val / (1024 * 1024)
	}

	numPart := s[:len(s)-1]
	val, _ := strconv.ParseInt(numPart, 10, 64)

	switch lastChar {
	case 'K', 'k':
		return val / 1024
	case 'M', 'm':
		return val
	case 'G', 'g':
		return val * 1024
	case 'T', 't':
		return val * 1024 * 1024
	default:
		return val
	}
}
