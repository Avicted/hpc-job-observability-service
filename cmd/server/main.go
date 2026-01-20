// Package main provides the entry point for the HPC Job Observability Service.
// This service collects, stores, and exposes resource usage metrics for batch jobs.
package main

import (
	"context"
	"flag"
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
	"github.com/avic/hpc-job-observability-service/internal/storage"
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

	// Feature flags
	SeedDemo bool
}

func main() {
	// Load .env file if it exists (for local development)
	// In production, use actual environment variables
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	cfg := loadConfig()

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

	// Seed demo data if requested
	if cfg.SeedDemo {
		log.Println("Seeding demo data...")
		if err := store.SeedDemoData(); err != nil {
			log.Fatalf("Failed to seed demo data: %v", err)
		}
	}

	// Initialize metrics exporter
	metricsExporter := metrics.NewExporter(store)

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

func loadConfig() *Config {
	cfg := &Config{}

	// Flags
	flag.BoolVar(&cfg.SeedDemo, "seed-demo", false, "Seed database with demo data")
	flag.Parse()

	// Environment variables with defaults
	cfg.Port = getEnvInt("PORT", 8080)
	cfg.Host = getEnv("HOST", "0.0.0.0")
	cfg.DatabaseType = getEnv("DATABASE_TYPE", "sqlite")
	cfg.DatabaseURL = getEnv("DATABASE_URL", "file:hpc_jobs.db?cache=shared&mode=rwc")
	cfg.MetricsRetentionDays = getEnvInt("METRICS_RETENTION_DAYS", 7)

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
