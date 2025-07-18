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
	client *redis.Client
	logger *logrus.Logger
	config *RedisConfig
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
}

// NewRedisConfig creates a new Redis configuration
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
	if host == "localhost" {
		port = 6379    // Standard Redis port for localhost
		useAAD = false // Disable Azure AD for localhost
	}

	return &RedisConfig{
		Host:         host,
		Port:         port,
		Username:     os.Getenv("REDIS_USERNAME"),
		UseAAD:       useAAD,
		MaxRetries:   3,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		PoolSize:     50,
		MinIdleConns: 10,
		MaxConnAge:   30 * time.Minute,
		PoolTimeout:  5 * time.Second,
		IdleTimeout:  5 * time.Minute,
	}
}

// NewAzureRedisClient creates a new Azure Redis client
func NewAzureRedisClient(config *RedisConfig) (*AzureRedisClient, error) {
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	if config.Host == "" {
		return nil, fmt.Errorf("redis host is required")
	}

	// Create Redis client options
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
		opts.Password = ""
	}

	client := redis.NewClient(opts)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := client.Ping(ctx).Err()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	fmt.Printf("✅ Successfully connected to Redis at %s:%d\n", config.Host, config.Port)

	return &AzureRedisClient{
		client: client,
		logger: logger,
		config: config,
	}, nil
}

// createManagedIdentityProvider creates a managed identity credentials provider
func createManagedIdentityProvider() (auth.StreamingCredentialsProvider, error) {
	clientID := os.Getenv("AZURE_CLIENT_ID")

	if clientID != "" {
		return entraid.NewManagedIdentityCredentialsProvider(
			entraid.ManagedIdentityCredentialsProviderOptions{
				ManagedIdentityProviderOptions: identity.ManagedIdentityProviderOptions{
					ManagedIdentityType:  "UserAssignedObjectID",
					UserAssignedObjectID: clientID,
				},
			},
		)
	} else {
		return entraid.NewManagedIdentityCredentialsProvider(
			entraid.ManagedIdentityCredentialsProviderOptions{
				ManagedIdentityProviderOptions: identity.ManagedIdentityProviderOptions{
					ManagedIdentityType: identity.SystemAssignedIdentity,
				},
			},
		)
	}
}

// GetStreamInfo returns information about a Redis stream
func (c *AzureRedisClient) GetStreamInfo(ctx context.Context, streamName string) (*redis.XInfoStream, error) {
	return c.client.XInfoStream(ctx, streamName).Result()
}

// Close closes the Redis client connection
func (c *AzureRedisClient) Close() error {
	return c.client.Close()
}
