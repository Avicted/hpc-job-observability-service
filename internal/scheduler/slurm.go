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
	job := &Job{
		ID:    strconv.FormatUint(sj.JobID, 10),
		User:  sj.UserName,
		Nodes: s.parseNodes(sj.Nodes),
		State: s.stateMapper.NormalizeState(sj.JobState),
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

// SLURM API response structures

type slurmJobsResponse struct {
	Jobs   []slurmJob `json:"jobs"`
	Errors []struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	} `json:"errors,omitempty"`
}

type slurmJob struct {
	JobID        uint64 `json:"job_id"`
	Name         string `json:"name"`
	UserName     string `json:"user_name"`
	Nodes        string `json:"nodes"`
	JobState     string `json:"job_state"`
	Partition    string `json:"partition"`
	Account      string `json:"account"`
	QoS          string `json:"qos"`
	Priority     uint32 `json:"priority"`
	StartTime    uint64 `json:"start_time"`
	EndTime      uint64 `json:"end_time"`
	SubmitTime   uint64 `json:"submit_time"`
	EligibleTime uint64 `json:"eligible_time"`
	TimeLimit    uint32 `json:"time_limit"`
	Cpus         uint32 `json:"cpus"`
	Memory       uint64 `json:"memory"`
	TresAllocStr string `json:"tres_alloc_str"`
	TresReqStr   string `json:"tres_req_str"`
	StateReason  string `json:"state_reason"`
	ExitCode     int32  `json:"exit_code"`
}
