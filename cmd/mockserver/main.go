// Package main provides the mock server for development and testing.
// It serves OpenAPI-driven mock responses from the spec examples.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/avic/hpc-job-observability-service/internal/scheduler"
	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

// Config holds mock server configuration.
type Config struct {
	Port int
	Host string
}

// OpenAPISpec represents a minimal OpenAPI document structure for extracting examples.
type OpenAPISpec struct {
	Paths map[string]map[string]Operation `yaml:"paths"`
}

// Operation represents an OpenAPI operation.
type Operation struct {
	OperationID string                 `yaml:"operationId"`
	Responses   map[string]ResponseDef `yaml:"responses"`
}

// ResponseDef represents an OpenAPI response definition.
type ResponseDef struct {
	Description string                `yaml:"description"`
	Content     map[string]ContentDef `yaml:"content"`
}

// ContentDef represents content type definition.
type ContentDef struct {
	Example interface{} `yaml:"example"`
}

// MockServer serves mock responses based on OpenAPI examples.
type MockServer struct {
	spec      *OpenAPISpec
	jobSource *scheduler.MockJobSource
}

func main() {
	// Load .env file if it exists (for local development)
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	cfg := loadConfig()

	// Load OpenAPI spec
	spec, err := loadOpenAPISpec("config/openapi.yaml")
	if err != nil {
		log.Fatalf("Failed to load OpenAPI spec: %v", err)
	}

	jobSource := scheduler.NewMockJobSource()
	jobSource.GenerateDemoJobs()

	server := &MockServer{spec: spec, jobSource: jobSource}
	mux := server.Routes()

	addr := cfg.Host + ":" + strconv.Itoa(cfg.Port)
	log.Printf("Mock server starting on %s", addr)
	log.Printf("Serving mock responses from openapi.yaml examples")

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

func loadConfig() *Config {
	cfg := &Config{}
	flag.Parse()

	cfg.Port = getEnvInt("MOCK_PORT", 8081)
	cfg.Host = getEnv("MOCK_HOST", "0.0.0.0")

	return cfg
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			return intVal
		}
	}
	return defaultVal
}

func loadOpenAPISpec(path string) (*OpenAPISpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var spec OpenAPISpec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return nil, err
	}

	return &spec, nil
}

// Routes returns the mock server HTTP handler.
func (s *MockServer) Routes() http.Handler {
	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("GET /v1/health", s.mockHandler("/health", "get", "200"))

	// Jobs endpoints
	mux.HandleFunc("GET /v1/jobs", s.listJobsHandler())
	mux.HandleFunc("POST /v1/jobs", s.mockHandler("/jobs", "post", "201"))
	mux.HandleFunc("GET /v1/jobs/{jobId}", s.getJobHandler())
	mux.HandleFunc("PATCH /v1/jobs/{jobId}", s.mockHandler("/jobs/{jobId}", "patch", "200"))
	mux.HandleFunc("DELETE /v1/jobs/{jobId}", s.mockDeleteHandler())

	// Metrics endpoints
	mux.HandleFunc("GET /v1/jobs/{jobId}/metrics", s.getJobMetricsHandler())
	mux.HandleFunc("POST /v1/jobs/{jobId}/metrics", s.mockHandler("/jobs/{jobId}/metrics", "post", "201"))

	// Prometheus metrics endpoint (simple mock)
	mux.HandleFunc("GET /metrics", s.mockMetricsHandler())

	return s.withMiddleware(mux)
}

func (s *MockServer) withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("[MOCK] %s %s", r.Method, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Mock-Server", "true")
		next.ServeHTTP(w, r)
	})
}

func (s *MockServer) mockHandler(path, method, statusCode string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		example := s.getExample(path, method, statusCode)
		if example == nil {
			// Return a generic response
			example = map[string]interface{}{
				"message": "Mock response",
				"path":    path,
				"method":  method,
			}
		}

		status, _ := strconv.Atoi(statusCode)
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(example)
	}
}

func (s *MockServer) mockDeleteHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *MockServer) mockMetricsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		jobs, _ := s.jobSource.ListJobs(r.Context(), scheduler.JobFilter{})
		stateCounts := map[scheduler.JobState]int{
			scheduler.JobStatePending:   0,
			scheduler.JobStateRunning:   0,
			scheduler.JobStateCompleted: 0,
			scheduler.JobStateFailed:    0,
			scheduler.JobStateCancelled: 0,
		}

		var b strings.Builder
		b.WriteString("# HELP hpc_job_runtime_seconds Current runtime of the job in seconds\n")
		b.WriteString("# TYPE hpc_job_runtime_seconds gauge\n")
		b.WriteString("# HELP hpc_job_cpu_usage_percent Current CPU usage of the job as a percentage\n")
		b.WriteString("# TYPE hpc_job_cpu_usage_percent gauge\n")
		b.WriteString("# HELP hpc_job_memory_usage_bytes Current memory usage of the job in bytes\n")
		b.WriteString("# TYPE hpc_job_memory_usage_bytes gauge\n")
		b.WriteString("# HELP hpc_job_gpu_usage_percent Current GPU usage of the job as a percentage\n")
		b.WriteString("# TYPE hpc_job_gpu_usage_percent gauge\n")

		now := time.Now()
		for _, job := range jobs {
			stateCounts[job.State]++
			node := ""
			if len(job.Nodes) > 0 {
				node = job.Nodes[0]
			}
			runtime := job.RuntimeSeconds
			if job.State == scheduler.JobStateRunning {
				runtime = now.Sub(job.StartTime).Seconds()
			}

			b.WriteString(fmt.Sprintf("hpc_job_runtime_seconds{job_id=\"%s\",user=\"%s\",node=\"%s\"} %.0f\n", job.ID, job.User, node, runtime))
			b.WriteString(fmt.Sprintf("hpc_job_cpu_usage_percent{job_id=\"%s\",user=\"%s\",node=\"%s\"} %.2f\n", job.ID, job.User, node, job.CPUUsage))
			b.WriteString(fmt.Sprintf("hpc_job_memory_usage_bytes{job_id=\"%s\",user=\"%s\",node=\"%s\"} %.0f\n", job.ID, job.User, node, float64(job.MemoryUsageMB)*1024*1024))
			if job.GPUUsage != nil {
				b.WriteString(fmt.Sprintf("hpc_job_gpu_usage_percent{job_id=\"%s\",user=\"%s\",node=\"%s\"} %.2f\n", job.ID, job.User, node, *job.GPUUsage))
			}
		}

		b.WriteString("# HELP hpc_job_state_total Number of jobs in each state\n")
		b.WriteString("# TYPE hpc_job_state_total gauge\n")
		for state, count := range stateCounts {
			b.WriteString(fmt.Sprintf("hpc_job_state_total{state=\"%s\"} %d\n", state, count))
		}
		b.WriteString("# HELP hpc_job_total Total number of jobs tracked by the system\n")
		b.WriteString("# TYPE hpc_job_total counter\n")
		b.WriteString(fmt.Sprintf("hpc_job_total %d\n", len(jobs)))
		b.WriteString("# HELP hpc_exporter_last_collect_timestamp_seconds Unix timestamp of the last successful metric collection\n")
		b.WriteString("# TYPE hpc_exporter_last_collect_timestamp_seconds gauge\n")
		b.WriteString(fmt.Sprintf("hpc_exporter_last_collect_timestamp_seconds %d\n", time.Now().Unix()))

		w.Write([]byte(b.String()))
	}
}

func (s *MockServer) listJobsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()

		var filter scheduler.JobFilter
		if state := query.Get("state"); state != "" {
			jobState := scheduler.JobState(state)
			filter.State = &jobState
		}
		if user := query.Get("user"); user != "" {
			filter.User = &user
		}
		if limitStr := query.Get("limit"); limitStr != "" {
			if limit, err := strconv.Atoi(limitStr); err == nil {
				filter.Limit = limit
			}
		}
		if offsetStr := query.Get("offset"); offsetStr != "" {
			if offset, err := strconv.Atoi(offsetStr); err == nil {
				filter.Offset = offset
			}
		}

		jobs, _ := s.jobSource.ListJobs(r.Context(), filter)
		nodeFilter := query.Get("node")
		if nodeFilter != "" {
			filtered := make([]*scheduler.Job, 0, len(jobs))
			for _, job := range jobs {
				for _, node := range job.Nodes {
					if node == nodeFilter {
						filtered = append(filtered, job)
						break
					}
				}
			}
			jobs = filtered
		}

		limit := filter.Limit
		if limit == 0 {
			limit = 100
		}
		response := map[string]interface{}{
			"jobs":   jobs,
			"total":  len(jobs),
			"limit":  limit,
			"offset": filter.Offset,
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
	}
}

func (s *MockServer) getJobHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		jobID := strings.TrimPrefix(r.URL.Path, "/v1/jobs/")
		job, _ := s.jobSource.GetJob(r.Context(), jobID)
		if job == nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error":   "not_found",
				"message": fmt.Sprintf("Job with ID '%s' not found", jobID),
			})
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(job)
	}
}

func (s *MockServer) getJobMetricsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/v1/jobs/")
		jobID := strings.TrimSuffix(path, "/metrics")
		job, _ := s.jobSource.GetJob(r.Context(), jobID)
		if job == nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error":   "not_found",
				"message": fmt.Sprintf("Job with ID '%s' not found", jobID),
			})
			return
		}

		samples, _ := s.jobSource.GetJobMetrics(r.Context(), jobID)
		query := r.URL.Query()
		var startTime, endTime *time.Time
		if start := query.Get("start_time"); start != "" {
			if t, err := time.Parse(time.RFC3339, start); err == nil {
				startTime = &t
			}
		}
		if end := query.Get("end_time"); end != "" {
			if t, err := time.Parse(time.RFC3339, end); err == nil {
				endTime = &t
			}
		}

		filtered := make([]*scheduler.MetricSample, 0, len(samples))
		for _, sample := range samples {
			if startTime != nil && sample.Timestamp.Before(*startTime) {
				continue
			}
			if endTime != nil && sample.Timestamp.After(*endTime) {
				continue
			}
			filtered = append(filtered, sample)
		}
		samples = filtered

		limit := 1000
		if limitStr := query.Get("limit"); limitStr != "" {
			if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
				limit = l
			}
		}
		if len(samples) > limit {
			samples = samples[:limit]
		}

		response := map[string]interface{}{
			"job_id":  jobID,
			"samples": samples,
			"total":   len(samples),
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
	}
}

func (s *MockServer) getExample(path, method, statusCode string) interface{} {
	pathDef, ok := s.spec.Paths[path]
	if !ok {
		return nil
	}

	operation, ok := pathDef[method]
	if !ok {
		return nil
	}

	responseDef, ok := operation.Responses[statusCode]
	if !ok {
		return nil
	}

	contentDef, ok := responseDef.Content["application/json"]
	if !ok {
		return nil
	}

	return contentDef.Example
}
