package main

import (
	"os"
	"strconv"
	"time"
)

// Environment represents different deployment environments
type Environment string

const (
	Development Environment = "development"
	Testing     Environment = "testing"
	Staging     Environment = "staging"
	Production  Environment = "production"
)

// ConfigManager handles configuration loading from different sources
type ConfigManager struct {
	env Environment
}

// NewConfigManager creates a new configuration manager
func NewConfigManager() *ConfigManager {
	env := Environment(getEnvOrDefault("ENVIRONMENT", "development"))
	return &ConfigManager{env: env}
}

// LoadRedisConfig loads Redis configuration based on environment
func (cm *ConfigManager) LoadRedisConfig() *RedisConfig {
	config := &RedisConfig{
		Host:     getEnvOrDefault("REDIS_HOST", "localhost"),
		Port:     getEnvIntOrDefault("REDIS_PORT", 6379),
		Username: getEnvOrDefault("REDIS_USERNAME", ""),
		UseAAD:   getEnvBoolOrDefault("REDIS_USE_AAD", false),
	}

	// Environment-specific configurations
	switch cm.env {
	case Development:
		config.MaxRetries = 1
		config.DialTimeout = 10 * time.Second
		config.ReadTimeout = 5 * time.Second
		config.WriteTimeout = 5 * time.Second
		config.PoolSize = 5
		config.MinIdleConns = 2
		config.UseAAD = false // Use password for development
		if config.Port == 6379 {
			config.Port = 6379 // Standard Redis port for local development
		}

	case Testing:
		config.MaxRetries = 2
		config.DialTimeout = 5 * time.Second
		config.ReadTimeout = 3 * time.Second
		config.WriteTimeout = 3 * time.Second
		config.PoolSize = 3
		config.MinIdleConns = 1
		config.UseAAD = false

	case Staging:
		config.MaxRetries = 3
		config.DialTimeout = 8 * time.Second
		config.ReadTimeout = 4 * time.Second
		config.WriteTimeout = 4 * time.Second
		config.PoolSize = 8
		config.MinIdleConns = 3
		config.UseAAD = true
		config.Port = 6380 // Azure Redis port

	case Production:
		config.MaxRetries = 3
		config.DialTimeout = 5 * time.Second
		config.ReadTimeout = 3 * time.Second
		config.WriteTimeout = 3 * time.Second
		config.PoolSize = 10
		config.MinIdleConns = 5
		config.UseAAD = true
		config.Port = 6380 // Azure Redis port

		// Production-specific optimizations
		config.MaxConnAge = 30 * time.Minute
		config.PoolTimeout = 4 * time.Second
		config.IdleTimeout = 5 * time.Minute
	}

	// Override with environment-specific values
	if maxRetries := getEnvIntOrDefault("REDIS_MAX_RETRIES", 0); maxRetries > 0 {
		config.MaxRetries = maxRetries
	}
	if dialTimeout := getEnvDurationOrDefault("REDIS_DIAL_TIMEOUT", 0); dialTimeout > 0 {
		config.DialTimeout = dialTimeout
	}
	if poolSize := getEnvIntOrDefault("REDIS_POOL_SIZE", 0); poolSize > 0 {
		config.PoolSize = poolSize
	}
	if readTimeout := getEnvDurationOrDefault("REDIS_READ_TIMEOUT", 0); readTimeout > 0 {
		config.ReadTimeout = readTimeout
	}
	if writeTimeout := getEnvDurationOrDefault("REDIS_WRITE_TIMEOUT", 0); writeTimeout > 0 {
		config.WriteTimeout = writeTimeout
	}
	if poolTimeout := getEnvDurationOrDefault("REDIS_POOL_TIMEOUT", 0); poolTimeout > 0 {
		config.PoolTimeout = poolTimeout
	}
	if minIdleConns := getEnvIntOrDefault("REDIS_MIN_IDLE_CONNS", 0); minIdleConns > 0 {
		config.MinIdleConns = minIdleConns
	}
	if idleTimeout := getEnvDurationOrDefault("REDIS_IDLE_TIMEOUT", 0); idleTimeout > 0 {
		config.IdleTimeout = idleTimeout
	}
	if maxConnAge := getEnvDurationOrDefault("REDIS_MAX_CONN_AGE", 0); maxConnAge > 0 {
		config.MaxConnAge = maxConnAge
	}

	return config
}

// GetEnvironment returns the current environment
func (cm *ConfigManager) GetEnvironment() Environment {
	return cm.env
}

// IsProduction returns true if running in production
func (cm *ConfigManager) IsProduction() bool {
	return cm.env == Production
}

// IsDevelopment returns true if running in development
func (cm *ConfigManager) IsDevelopment() bool {
	return cm.env == Development
}

// Helper functions for environment variable parsing

func getEnvIntOrDefault(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvBoolOrDefault(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

func getEnvDurationOrDefault(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}

// Helper function to get environment variable with default
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// Azure-specific configuration
type AzureConfig struct {
	SubscriptionID string
	ResourceGroup  string
	TenantID       string
	ClientID       string
	ClientSecret   string
}

// LoadAzureConfig loads Azure-specific configuration
func (cm *ConfigManager) LoadAzureConfig() *AzureConfig {
	return &AzureConfig{
		SubscriptionID: getEnvOrDefault("AZURE_SUBSCRIPTION_ID", ""),
		ResourceGroup:  getEnvOrDefault("AZURE_RESOURCE_GROUP", ""),
		TenantID:       getEnvOrDefault("AZURE_TENANT_ID", ""),
		ClientID:       getEnvOrDefault("AZURE_CLIENT_ID", ""),
		ClientSecret:   getEnvOrDefault("AZURE_CLIENT_SECRET", ""),
	}
}

// LoggingConfig holds logging configuration
type LoggingConfig struct {
	Level      string
	Format     string
	Output     string
	Structured bool
}

// LoadLoggingConfig loads logging configuration
func (cm *ConfigManager) LoadLoggingConfig() *LoggingConfig {
	config := &LoggingConfig{
		Level:      getEnvOrDefault("LOG_LEVEL", "info"),
		Format:     getEnvOrDefault("LOG_FORMAT", "json"),
		Output:     getEnvOrDefault("LOG_OUTPUT", "stdout"),
		Structured: getEnvBoolOrDefault("LOG_STRUCTURED", true),
	}

	// Environment-specific logging settings
	switch cm.env {
	case Development:
		config.Level = "debug"
		config.Format = "text"
		config.Structured = false

	case Testing:
		config.Level = "warn"
		config.Format = "json"
		config.Structured = true

	case Production:
		config.Level = "info"
		config.Format = "json"
		config.Structured = true
	}

	return config
}
