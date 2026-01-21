// Package main provides the entry point for the HPC Job Observability Service.
// This service collects, stores, and exposes resource usage metrics for batch jobs.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/avic/hpc-job-observability-service/internal/api"
	"github.com/avic/hpc-job-observability-service/internal/collector"
	"github.com/avic/hpc-job-observability-service/internal/metrics"
	"github.com/avic/hpc-job-observability-service/internal/scheduler"
	"github.com/avic/hpc-job-observability-service/internal/storage"
	"github.com/avic/hpc-job-observability-service/internal/syncer"
	"github.com/joho/godotenv"
)

// Config holds application configuration loaded from environment variables and flags.
type Config struct {
	// Server settings
	Port int
	Host string

	// Database settings
	DatabaseURL  string
	DatabaseType string // "sqlite" or "postgres"

	// Metrics settings
	MetricsRetentionDays int

	// Scheduler settings
	SchedulerBackend string // "mock" or "slurm" (required)
	SlurmBaseURL     string // Base URL of slurmrestd (e.g., "http://localhost:6820")
	SlurmAPIVersion  string // SLURM REST API version (e.g., "v0.0.44")
	SlurmAuthToken   string // JWT or auth token for slurmrestd

	// Feature flags
	SeedDemo bool
}

func main() {
	// Load .env file if it exists (for local development)
	// In production, use actual environment variables
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	// Initialize scheduler source
	jobSource, err := initScheduler(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize scheduler: %v", err)
	}
	log.Printf("Scheduler backend: %s", cfg.SchedulerBackend)

	// Initialize storage
	store, err := storage.New(cfg.DatabaseType, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	defer store.Close()

	// Run migrations
	if err := store.Migrate(); err != nil {
		log.Fatalf("Failed to run migrations: %v", err)
	}

	// Seed demo data if requested AND using mock backend
	// Demo data is only useful for testing without a real scheduler
	if cfg.SeedDemo && cfg.SchedulerBackend == "mock" {
		log.Println("Seeding demo data (mock backend)...")
		if err := store.SeedDemoData(); err != nil {
			log.Fatalf("Failed to seed demo data: %v", err)
		}
	} else if cfg.SeedDemo && cfg.SchedulerBackend != "mock" {
		log.Println("Note: SEED_DEMO is ignored when using slurm backend (jobs will be synced from scheduler)")
	}

	// Start job syncer for real scheduler backends
	var jobSyncer *syncer.Syncer
	if cfg.SchedulerBackend != "mock" {
		syncerConfig := syncer.DefaultConfig()
		jobSyncer = syncer.New(jobSource, store, syncerConfig)
		jobSyncer.Start()
		defer jobSyncer.Stop()
	}

	// Initialize metrics exporter with scheduler for node metrics
	metricsExporter := metrics.NewExporterWithScheduler(store, jobSource)

	// Initialize metric collector
	coll := collector.New(store, metricsExporter)
	coll.Start()
	defer coll.Stop()

	// Start retention cleanup routine
	go runRetentionCleanup(store, cfg.MetricsRetentionDays)

	// Initialize API server
	apiServer := api.NewServer(store, metricsExporter)
	mux := apiServer.Routes()

	// Create HTTP server
	addr := cfg.Host + ":" + strconv.Itoa(cfg.Port)
	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("Starting server on %s", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	<-done
	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server shutdown failed: %v", err)
	}
	log.Println("Server stopped")
}

func loadConfig() (*Config, error) {
	cfg := &Config{}

	// Environment variables with defaults
	cfg.Port = getEnvInt("PORT", 8080)
	cfg.Host = getEnv("HOST", "0.0.0.0")
	cfg.DatabaseType = getEnv("DATABASE_TYPE", "sqlite")
	cfg.DatabaseURL = getEnv("DATABASE_URL", "file:hpc_jobs.db?cache=shared&mode=rwc")
	cfg.MetricsRetentionDays = getEnvInt("METRICS_RETENTION_DAYS", 7)

	// Demo seed (opt-in)
	cfg.SeedDemo = getEnvBool("SEED_DEMO", false)

	// Scheduler configuration (required)
	cfg.SchedulerBackend = getEnv("SCHEDULER_BACKEND", "")
	if cfg.SchedulerBackend == "" {
		return nil, fmt.Errorf("SCHEDULER_BACKEND is required (must be 'mock' or 'slurm')")
	}
	if cfg.SchedulerBackend != "mock" && cfg.SchedulerBackend != "slurm" {
		return nil, fmt.Errorf("SCHEDULER_BACKEND must be 'mock' or 'slurm', got '%s'", cfg.SchedulerBackend)
	}

	// Slurm configuration (required when backend is slurm)
	cfg.SlurmBaseURL = getEnv("SLURM_BASE_URL", "")
	cfg.SlurmAPIVersion = getEnv("SLURM_API_VERSION", "v0.0.44")
	cfg.SlurmAuthToken = getEnv("SLURM_AUTH_TOKEN", "")

	if cfg.SchedulerBackend == "slurm" && cfg.SlurmBaseURL == "" {
		return nil, fmt.Errorf("SLURM_BASE_URL is required when SCHEDULER_BACKEND is 'slurm'")
	}

	return cfg, nil
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

func getEnvBool(key string, defaultVal bool) bool {
	if val := os.Getenv(key); val != "" {
		if boolVal, err := strconv.ParseBool(val); err == nil {
			return boolVal
		}
	}
	return defaultVal
}

// initScheduler creates the appropriate job source based on configuration.
func initScheduler(cfg *Config) (scheduler.JobSource, error) {
	switch cfg.SchedulerBackend {
	case "mock":
		return scheduler.NewMockJobSource(), nil
	case "slurm":
		slurmCfg := scheduler.SlurmConfig{
			BaseURL:    cfg.SlurmBaseURL,
			APIVersion: cfg.SlurmAPIVersion,
			AuthToken:  cfg.SlurmAuthToken,
		}
		return scheduler.NewSlurmJobSource(slurmCfg), nil
	default:
		return nil, fmt.Errorf("unknown scheduler backend: %s", cfg.SchedulerBackend)
	}
}

// runRetentionCleanup periodically removes old metric samples based on retention policy.
func runRetentionCleanup(store storage.Storage, retentionDays int) {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		cutoff := time.Now().AddDate(0, 0, -retentionDays)
		if err := store.DeleteMetricsBefore(cutoff); err != nil {
			log.Printf("Retention cleanup error: %v", err)
		} else {
			log.Printf("Retention cleanup completed, removed samples older than %v", cutoff)
		}
	}
}
