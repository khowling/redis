package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// HealthChecker provides health check functionality for the Redis client
type HealthChecker struct {
	client         *AzureRedisClient
	logger         *logrus.Logger
	lastCheckTime  time.Time
	lastCheckError error
	checkInterval  time.Duration
	mutex          sync.RWMutex
}

// HealthStatus represents the health status of the Redis connection
type HealthStatus struct {
	Status      string           `json:"status"`
	Timestamp   time.Time        `json:"timestamp"`
	Uptime      string           `json:"uptime"`
	RedisInfo   *RedisHealthInfo `json:"redis_info,omitempty"`
	Error       string           `json:"error,omitempty"`
	Metrics     *HealthMetrics   `json:"metrics"`
	Environment string           `json:"environment"`
}

// RedisHealthInfo contains Redis-specific health information
type RedisHealthInfo struct {
	Connected        bool   `json:"connected"`
	Version          string `json:"version,omitempty"`
	UsedMemory       string `json:"used_memory,omitempty"`
	ConnectedClients int64  `json:"connected_clients,omitempty"`
	Uptime           int64  `json:"uptime_seconds,omitempty"`
}

// HealthMetrics contains operational metrics
type HealthMetrics struct {
	TotalConnections    int64         `json:"total_connections"`
	FailedConnections   int64         `json:"failed_connections"`
	StreamOperations    int64         `json:"stream_operations"`
	FailedOperations    int64         `json:"failed_operations"`
	AverageResponseTime time.Duration `json:"average_response_time"`
	LastSuccessfulOp    time.Time     `json:"last_successful_operation"`
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(client *AzureRedisClient, checkInterval time.Duration) *HealthChecker {
	return &HealthChecker{
		client:        client,
		logger:        client.logger,
		checkInterval: checkInterval,
	}
}

// StartHealthChecks starts periodic health checks
func (h *HealthChecker) StartHealthChecks(ctx context.Context) {
	ticker := time.NewTicker(h.checkInterval)
	defer ticker.Stop()

	// Perform initial health check
	h.performHealthCheck(ctx)

	for {
		select {
		case <-ctx.Done():
			h.logger.Info("Health checker stopping due to context cancellation")
			return
		case <-ticker.C:
			h.performHealthCheck(ctx)
		}
	}
}

// performHealthCheck performs a health check
func (h *HealthChecker) performHealthCheck(ctx context.Context) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.lastCheckTime = time.Now()
	h.lastCheckError = nil

	// Check basic connectivity
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err := h.client.client.Ping(ctx).Err()
	if err != nil {
		h.lastCheckError = err
		h.logger.WithError(err).Error("Redis health check failed")
		return
	}

	h.logger.Debug("Redis health check passed")
}

// GetHealthStatus returns the current health status
func (h *HealthChecker) GetHealthStatus(ctx context.Context) *HealthStatus {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	status := &HealthStatus{
		Timestamp:   time.Now(),
		Environment: string(NewConfigManager().GetEnvironment()),
		Metrics:     &HealthMetrics{},
	}

	// Determine overall status
	if h.lastCheckError != nil {
		status.Status = "unhealthy"
		status.Error = h.lastCheckError.Error()
	} else {
		status.Status = "healthy"
	}

	// Get Redis info if connected
	if h.lastCheckError == nil {
		redisInfo, err := h.getRedisInfo(ctx)
		if err == nil {
			status.RedisInfo = redisInfo
		}
	}

	return status
}

// getRedisInfo retrieves Redis server information
func (h *HealthChecker) getRedisInfo(ctx context.Context) (*RedisHealthInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	// Get Redis INFO
	_, err := h.client.client.Info(ctx).Result()
	if err != nil {
		return nil, err
	}

	// Parse basic information (simplified parsing)
	redisInfo := &RedisHealthInfo{
		Connected: true,
	}

	// You can extend this to parse specific fields from the INFO command
	// For example: redis_version, used_memory, connected_clients, uptime_in_seconds

	return redisInfo, nil
}

// HTTP handler for health checks
func (h *HealthChecker) HealthHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	status := h.GetHealthStatus(ctx)

	w.Header().Set("Content-Type", "application/json")

	// Set HTTP status code based on health
	if status.Status == "healthy" {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(status)
}

// ReadinessHandler checks if the service is ready to accept requests
func (h *HealthChecker) ReadinessHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Perform a quick ping to check readiness
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	err := h.client.client.Ping(ctx).Err()

	w.Header().Set("Content-Type", "application/json")

	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "not ready",
			"error":  err.Error(),
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ready",
	})
}

// LivenessHandler checks if the service is alive
func (h *HealthChecker) LivenessHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "alive",
	})
}

// StartHealthServer starts an HTTP server for health checks
func StartHealthServer(healthChecker *HealthChecker, port int) {
	mux := http.NewServeMux()

	// Health check endpoints
	mux.HandleFunc("/health", healthChecker.HealthHandler)
	mux.HandleFunc("/ready", healthChecker.ReadinessHandler)
	mux.HandleFunc("/live", healthChecker.LivenessHandler)

	// Metrics endpoint (basic)
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		status := healthChecker.GetHealthStatus(r.Context())
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status.Metrics)
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logrus.WithError(err).Error("Health server failed")
		}
	}()

	logrus.WithField("port", port).Info("Health server started")
}

// Metrics collector for operational metrics
type MetricsCollector struct {
	mutex              sync.RWMutex
	streamOperations   int64
	failedOperations   int64
	connectionAttempts int64
	failedConnections  int64
	responseTimes      []time.Duration
	lastSuccessfulOp   time.Time
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		responseTimes: make([]time.Duration, 0, 100), // Keep last 100 response times
	}
}

// RecordStreamOperation records a stream operation
func (m *MetricsCollector) RecordStreamOperation(success bool, duration time.Duration) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.streamOperations++
	if !success {
		m.failedOperations++
	} else {
		m.lastSuccessfulOp = time.Now()
	}

	// Keep rolling window of response times
	if len(m.responseTimes) >= 100 {
		m.responseTimes = m.responseTimes[1:]
	}
	m.responseTimes = append(m.responseTimes, duration)
}

// RecordConnection records a connection attempt
func (m *MetricsCollector) RecordConnection(success bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.connectionAttempts++
	if !success {
		m.failedConnections++
	}
}

// GetMetrics returns current metrics
func (m *MetricsCollector) GetMetrics() *HealthMetrics {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	var avgResponseTime time.Duration
	if len(m.responseTimes) > 0 {
		var total time.Duration
		for _, rt := range m.responseTimes {
			total += rt
		}
		avgResponseTime = total / time.Duration(len(m.responseTimes))
	}

	return &HealthMetrics{
		TotalConnections:    m.connectionAttempts,
		FailedConnections:   m.failedConnections,
		StreamOperations:    m.streamOperations,
		FailedOperations:    m.failedOperations,
		AverageResponseTime: avgResponseTime,
		LastSuccessfulOp:    m.lastSuccessfulOp,
	}
}
