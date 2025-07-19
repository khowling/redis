package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"time"

	entraid "github.com/redis/go-redis-entraid"
	"github.com/redis/go-redis-entraid/identity"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/auth"
	"github.com/sirupsen/logrus"
)

// AzureRedisClient represents the Azure Redis client
type AzureRedisClient struct {
	client        redis.Cmdable // Use interface to support both single and cluster clients
	clusterClient *redis.ClusterClient
	singleClient  *redis.Client
	logger        *logrus.Logger
	config        *RedisConfig
	isCluster     bool
}

// RedisConfig holds configuration for Azure Redis connection
type RedisConfig struct {
	Host         string
	Port         int
	Username     string
	UseAAD       bool
	MaxRetries   int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolSize     int
	MinIdleConns int
	MaxConnAge   time.Duration
	PoolTimeout  time.Duration
	IdleTimeout  time.Duration
	IsCluster    bool
}

// NewRedisConfig creates a new Redis configuration with sensible defaults
func NewRedisConfig() *RedisConfig {
	host := os.Getenv("REDIS_HOST")
	port := 6380

	// Check if REDIS_ENDPOINT is provided (format: host:port)
	endpoint := os.Getenv("REDIS_ENDPOINT")
	if endpoint != "" {
		// Parse endpoint to extract host and port
		if colon := len(endpoint) - 1; colon > 0 {
			for i := len(endpoint) - 1; i >= 0; i-- {
				if endpoint[i] == ':' {
					host = endpoint[:i]
					if portStr := endpoint[i+1:]; portStr != "" {
						if p, err := fmt.Sscanf(portStr, "%d", &port); p == 1 && err == nil {
							// Port parsed successfully
						} else {
							port = 6380 // Default to Azure Redis port
						}
					}
					break
				}
			}
		}
	}

	if host == "" {
		host = "localhost"
	}

	// For localhost, use standard Redis port and disable AAD
	useAAD := true
	isCluster := false
	if host == "localhost" {
		port = 6379    // Standard Redis port for localhost
		useAAD = false // Disable Azure AD for localhost
		isCluster = false
	} else {
		// For Azure Redis Cache, check if cluster mode should be enabled
		// Default to false, can be enabled via environment variable
		if os.Getenv("REDIS_ENABLE_CLUSTER") == "true" {
			isCluster = true
		} else {
			isCluster = false // Default to single mode
		}
	}

	// Parse pool size from environment with default
	poolSize := 50
	if poolSizeStr := os.Getenv("REDIS_POOL_SIZE"); poolSizeStr != "" {
		if ps, err := fmt.Sscanf(poolSizeStr, "%d", &poolSize); ps != 1 || err != nil {
			poolSize = 50 // Default value
		}
	}

	// Parse max retries from environment with default
	maxRetries := 3
	if maxRetriesStr := os.Getenv("REDIS_MAX_RETRIES"); maxRetriesStr != "" {
		if mr, err := fmt.Sscanf(maxRetriesStr, "%d", &maxRetries); mr != 1 || err != nil {
			maxRetries = 3 // Default value
		}
	}

	// Parse dial timeout from environment with default
	dialTimeout := 5 * time.Second
	if dialTimeoutStr := os.Getenv("REDIS_DIAL_TIMEOUT"); dialTimeoutStr != "" {
		if dt, err := time.ParseDuration(dialTimeoutStr); err == nil {
			dialTimeout = dt
		}
	}

	return &RedisConfig{
		Host:         host,
		Port:         port,
		Username:     os.Getenv("REDIS_USERNAME"),
		UseAAD:       useAAD,
		MaxRetries:   maxRetries,
		DialTimeout:  dialTimeout,
		ReadTimeout:  2 * time.Second, // Reduced for faster operations
		WriteTimeout: 2 * time.Second, // Reduced for faster operations
		PoolSize:     poolSize,        // Now configurable via environment
		MinIdleConns: poolSize / 3,    // Dynamic based on pool size
		MaxConnAge:   30 * time.Minute,
		PoolTimeout:  2 * time.Second, // Reduced timeout for faster failover
		IdleTimeout:  3 * time.Minute, // Reduced idle timeout
		IsCluster:    isCluster,
	}
}

// NewAzureRedisClient creates a new Azure Redis client with AAD authentication
func NewAzureRedisClient(config *RedisConfig) (*AzureRedisClient, error) {
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel) // Reduced logging verbosity - only warnings and errors

	if config.Host == "" {
		return nil, fmt.Errorf("redis host is required")
	}

	client := &AzureRedisClient{
		logger:    logger,
		config:    config,
		isCluster: config.IsCluster,
	}

	if config.IsCluster {
		// Create cluster client
		clusterOpts := &redis.ClusterOptions{
			Addrs:           []string{fmt.Sprintf("%s:%d", config.Host, config.Port)},
			Username:        config.Username,
			MaxRetries:      config.MaxRetries,
			DialTimeout:     config.DialTimeout,
			ReadTimeout:     config.ReadTimeout,
			WriteTimeout:    config.WriteTimeout,
			PoolSize:        config.PoolSize,
			MinIdleConns:    config.MinIdleConns,
			ConnMaxLifetime: config.MaxConnAge,
			PoolTimeout:     config.PoolTimeout,
			ConnMaxIdleTime: config.IdleTimeout,
		}

		// Only use TLS for non-localhost connections
		if config.Host != "localhost" {
			clusterOpts.TLSConfig = &tls.Config{ServerName: config.Host}
		}

		// Use Azure AD authentication with managed identity if enabled
		if config.UseAAD {
			provider, err := createManagedIdentityProvider()
			if err != nil {
				return nil, fmt.Errorf("failed to create managed identity provider: %w", err)
			}
			clusterOpts.StreamingCredentialsProvider = provider
		}

		client.clusterClient = redis.NewClusterClient(clusterOpts)
		client.client = client.clusterClient
	} else {
		// Create single client
		opts := &redis.Options{
			Addr:            fmt.Sprintf("%s:%d", config.Host, config.Port),
			Username:        config.Username,
			MaxRetries:      config.MaxRetries,
			DialTimeout:     config.DialTimeout,
			ReadTimeout:     config.ReadTimeout,
			WriteTimeout:    config.WriteTimeout,
			PoolSize:        config.PoolSize,
			MinIdleConns:    config.MinIdleConns,
			ConnMaxLifetime: config.MaxConnAge,
			PoolTimeout:     config.PoolTimeout,
			ConnMaxIdleTime: config.IdleTimeout,
		}

		// Only use TLS for non-localhost connections
		if config.Host != "localhost" {
			opts.TLSConfig = &tls.Config{ServerName: config.Host}
		}

		// Use Azure AD authentication with managed identity if enabled
		if config.UseAAD {
			provider, err := createManagedIdentityProvider()
			if err != nil {
				return nil, fmt.Errorf("failed to create managed identity provider: %w", err)
			}
			opts.StreamingCredentialsProvider = provider
		}

		client.singleClient = redis.NewClient(opts)
		client.client = client.singleClient
	}

	// Test connection with retry logic
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := retryOperation(ctx, func() error {
		return client.client.Ping(ctx).Err()
	}, 3, time.Second)

	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Print connection success directly to stdout so it's always visible
	mode := "single"
	if config.IsCluster {
		mode = "cluster"
	}
	fmt.Printf("✅ Successfully connected to Redis %s at %s:%d\n", mode, config.Host, config.Port)

	return client, nil
}

// createManagedIdentityProvider creates a managed identity credentials provider
func createManagedIdentityProvider() (auth.StreamingCredentialsProvider, error) {
	// Check if we have a specific client ID for user-assigned managed identity
	clientID := os.Getenv("AZURE_CLIENT_ID")

	if clientID != "" {
		// Use user-assigned managed identity with object ID
		// Note: We're using the Object ID which should be provided in AZURE_CLIENT_ID
		// Based on the error logs, the Object ID is: 7303e014-4009-403f-b170-83ef57374e21
		return entraid.NewManagedIdentityCredentialsProvider(
			entraid.ManagedIdentityCredentialsProviderOptions{
				ManagedIdentityProviderOptions: identity.ManagedIdentityProviderOptions{
					ManagedIdentityType:  "UserAssignedObjectID",
					UserAssignedObjectID: clientID, // This should actually be the Object ID
				},
			},
		)
	} else {
		// Fall back to system-assigned managed identity
		return entraid.NewManagedIdentityCredentialsProvider(
			entraid.ManagedIdentityCredentialsProviderOptions{
				ManagedIdentityProviderOptions: identity.ManagedIdentityProviderOptions{
					ManagedIdentityType: identity.SystemAssignedIdentity,
				},
			},
		)
	}
}

// retryOperation implements exponential backoff retry logic
func retryOperation(ctx context.Context, operation func() error, maxRetries int, baseDelay time.Duration) error {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		lastErr = operation()
		if lastErr == nil {
			return nil
		}

		if attempt == maxRetries {
			break
		}

		// Exponential backoff
		delay := baseDelay * time.Duration(1<<attempt)
		time.Sleep(delay)
	}

	return lastErr
}

// GetStreamInfo returns information about a Redis stream
func (c *AzureRedisClient) GetStreamInfo(ctx context.Context, streamName string) (*redis.XInfoStream, error) {
	return c.client.XInfoStream(ctx, streamName).Result()
}

// Close closes the Redis client connection
func (c *AzureRedisClient) Close() error {
	if c.clusterClient != nil {
		return c.clusterClient.Close()
	}
	if c.singleClient != nil {
		return c.singleClient.Close()
	}
	return nil
}
