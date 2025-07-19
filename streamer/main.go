package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"text/template"
	"time"

	entraid "github.com/redis/go-redis-entraid"
	"github.com/redis/go-redis-entraid/identity"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/auth"
	"github.com/sirupsen/logrus"
)

// AzureRedisClient represents the Azure Redis client with stream operations
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
	Host          string
	Port          int
	Username      string
	UseAAD        bool
	MaxRetries    int
	DialTimeout   time.Duration
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
	PoolSize      int
	MinIdleConns  int
	MaxConnAge    time.Duration
	PoolTimeout   time.Duration
	IdleTimeout   time.Duration
	IdleCheckFreq time.Duration
	IsCluster     bool
	MaxLen        int64 // Maximum messages per stream
}

// StreamMessage represents a message to be added to a Redis stream
type StreamMessage struct {
	Fields map[string]interface{}
	ID     string // Optional: if empty, Redis will auto-generate
}

// StreamAddOptions contains options for adding messages to a stream
type StreamAddOptions struct {
	MaxLen      int64
	Approximate bool
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

	// Parse stream max length from environment with default
	maxLen := int64(2000000) // Default: 2 million messages per stream
	if maxLenStr := os.Getenv("REDIS_STREAM_MAXLEN"); maxLenStr != "" {
		if ml, err := strconv.ParseInt(maxLenStr, 10, 64); err == nil && ml > 0 {
			maxLen = ml
		}
	}

	return &RedisConfig{
		Host:          host,
		Port:          port,
		Username:      os.Getenv("REDIS_USERNAME"),
		UseAAD:        useAAD,
		MaxRetries:    maxRetries,
		DialTimeout:   dialTimeout,
		ReadTimeout:   2 * time.Second, // Reduced for faster operations
		WriteTimeout:  2 * time.Second, // Reduced for faster operations
		PoolSize:      poolSize,        // Now configurable via environment
		MinIdleConns:  poolSize / 3,    // Dynamic based on pool size
		MaxConnAge:    30 * time.Minute,
		PoolTimeout:   2 * time.Second,  // Reduced timeout for faster failover
		IdleTimeout:   3 * time.Minute,  // Reduced idle timeout
		IdleCheckFreq: 30 * time.Second, // Less frequent idle checks
		IsCluster:     isCluster,
		MaxLen:        maxLen, // Configurable stream max length
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
} // AddToStream adds a message to a Redis stream with error handling and retry logic
func (c *AzureRedisClient) AddToStream(ctx context.Context, streamName string, message StreamMessage, options *StreamAddOptions) (string, error) {
	if streamName == "" {
		return "", fmt.Errorf("stream name cannot be empty")
	}

	if len(message.Fields) == 0 {
		return "", fmt.Errorf("message fields cannot be empty")
	}

	// Prepare stream add arguments
	args := &redis.XAddArgs{
		Stream: streamName,
		Values: message.Fields,
	}

	// Set message ID if provided
	if message.ID != "" {
		args.ID = message.ID
	}

	// Apply options if provided
	if options != nil {
		if options.MaxLen > 0 {
			args.MaxLen = options.MaxLen
			args.Approx = options.Approximate
		}
	}

	// Add message to stream with retry logic
	var messageID string
	err := retryOperation(ctx, func() error {
		result := c.client.XAdd(ctx, args)
		if result.Err() != nil {
			return result.Err()
		}
		messageID = result.Val()
		return nil
	}, c.config.MaxRetries, time.Second)

	if err != nil {
		c.logger.WithFields(logrus.Fields{
			"stream":    streamName,
			"messageID": message.ID,
			"error":     err,
		}).Error("Failed to add message to stream")
		return "", fmt.Errorf("failed to add message to stream %s: %w", streamName, err)
	}

	c.logger.WithFields(logrus.Fields{
		"stream":    streamName,
		"messageID": messageID,
	}).Debug("Successfully added message to stream")

	return messageID, nil
}

// AddBatchToStream adds multiple messages to a stream in a pipeline for better performance
func (c *AzureRedisClient) AddBatchToStream(ctx context.Context, streamName string, messages []StreamMessage, options *StreamAddOptions) ([]string, error) {
	if streamName == "" {
		return nil, fmt.Errorf("stream name cannot be empty")
	}

	if len(messages) == 0 {
		return nil, fmt.Errorf("messages cannot be empty")
	}

	// Use pipeline for batch operations
	pipe := c.client.Pipeline()
	var commands []*redis.StringCmd

	for _, message := range messages {
		args := &redis.XAddArgs{
			Stream: streamName,
			Values: message.Fields,
		}

		if message.ID != "" {
			args.ID = message.ID
		}

		if options != nil && options.MaxLen > 0 {
			args.MaxLen = options.MaxLen
			args.Approx = options.Approximate
		}

		commands = append(commands, pipe.XAdd(ctx, args))
	}

	// Execute pipeline with retry logic
	var messageIDs []string
	err := retryOperation(ctx, func() error {
		_, err := pipe.Exec(ctx)
		if err != nil {
			return err
		}

		messageIDs = make([]string, len(commands))
		for i, cmd := range commands {
			if cmd.Err() != nil {
				return cmd.Err()
			}
			messageIDs[i] = cmd.Val()
		}
		return nil
	}, c.config.MaxRetries, time.Second)

	if err != nil {
		c.logger.WithFields(logrus.Fields{
			"stream":       streamName,
			"messageCount": len(messages),
			"error":        err,
		}).Error("Failed to add batch messages to stream")
		return nil, fmt.Errorf("failed to add batch messages to stream %s: %w", streamName, err)
	}

	// Only log batch operations at debug level to reduce verbosity
	c.logger.WithFields(logrus.Fields{
		"stream":       streamName,
		"messageCount": len(messages),
	}).Debug("Successfully added batch messages to stream")

	return messageIDs, nil
}

// GetStreamInfo gets information about a stream
func (c *AzureRedisClient) GetStreamInfo(ctx context.Context, streamName string) (*redis.XInfoStream, error) {
	if streamName == "" {
		return nil, fmt.Errorf("stream name cannot be empty")
	}

	var info *redis.XInfoStream
	err := retryOperation(ctx, func() error {
		result := c.client.XInfoStream(ctx, streamName)
		if result.Err() != nil {
			return result.Err()
		}
		info = result.Val()
		return nil
	}, c.config.MaxRetries, time.Second)

	if err != nil {
		c.logger.WithFields(logrus.Fields{
			"stream": streamName,
			"error":  err,
		}).Error("Failed to get stream info")
		return nil, fmt.Errorf("failed to get stream info for %s: %w", streamName, err)
	}

	return info, nil
}

// Close closes the Redis client connection
func (c *AzureRedisClient) Close() error {
	c.logger.Info("Closing Redis client connection")
	if c.clusterClient != nil {
		return c.clusterClient.Close()
	}
	if c.singleClient != nil {
		return c.singleClient.Close()
	}
	return nil
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

// TradeTemplate represents the template data for trade messages
type TradeTemplate struct {
	TradeId   string
	Timestamp string
	Symbol    string // Added symbol field for random UK symbols
}

// generateTradeId generates a unique trade ID
func generateTradeId() string {
	return fmt.Sprintf("TXN-%s-%09d",
		time.Now().Format("2006-0102"),
		rand.Intn(1000000000))
}

// loadUKSymbols loads UK stock symbols from file
func loadUKSymbols(symbolsPath string) ([]string, error) {
	content, err := os.ReadFile(symbolsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read symbols file: %w", err)
	}

	// Split by lines and filter out empty lines
	lines := strings.Split(string(content), "\n")
	var symbols []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			symbols = append(symbols, line)
		}
	}

	if len(symbols) == 0 {
		return nil, fmt.Errorf("no symbols found in file")
	}

	return symbols, nil
}

// MessageTemplate holds the parsed template for reuse
type MessageTemplate struct {
	template  *template.Template
	baseData  map[string]interface{}
	ukSymbols []string // Cache of UK stock symbols
}

// loadMessageTemplate loads and parses the trade message template once
func loadMessageTemplate(templatePath string) (*MessageTemplate, error) {
	// Load UK symbols first
	ukSymbols, err := loadUKSymbols("uk_symbols.txt")
	if err != nil {
		return nil, fmt.Errorf("failed to load UK symbols: %w", err)
	}

	// Read the template file once
	templateContent, err := os.ReadFile(templatePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read template file: %w", err)
	}

	// Parse template once
	tmpl, err := template.New("trade").Parse(string(templateContent))
	if err != nil {
		return nil, fmt.Errorf("failed to parse template: %w", err)
	}

	// Parse the template with sample data to get the base structure
	sampleData := TradeTemplate{
		TradeId:   "SAMPLE",
		Timestamp: "SAMPLE",
		Symbol:    "SAMPLE",
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, sampleData); err != nil {
		return nil, fmt.Errorf("failed to execute sample template: %w", err)
	}

	// Parse JSON to get base structure
	var baseData map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &baseData); err != nil {
		return nil, fmt.Errorf("failed to parse sample JSON: %w", err)
	}

	return &MessageTemplate{
		template:  tmpl,
		baseData:  baseData,
		ukSymbols: ukSymbols,
	}, nil
}

// generateMessage creates a new message using the cached template
func (mt *MessageTemplate) generateMessage() (map[string]interface{}, error) {
	// Select random UK symbol
	randomSymbol := mt.ukSymbols[rand.Intn(len(mt.ukSymbols))]

	// Create template data with unique values
	data := TradeTemplate{
		TradeId:   generateTradeId(),
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Symbol:    randomSymbol,
	}

	// Execute template with new data
	var buf bytes.Buffer
	if err := mt.template.Execute(&buf, data); err != nil {
		return nil, fmt.Errorf("failed to execute template: %w", err)
	}

	// Parse JSON into map
	var result map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	return result, nil
}

// StreamingConfig holds configuration for high-performance streaming
type StreamingConfig struct {
	BatchSize     int           // Number of messages to batch together
	FlushInterval time.Duration // Maximum time to wait before flushing batch
	WorkerCount   int           // Number of concurrent workers
	MessageRate   int           // Messages per second (0 = unlimited)
	RunDuration   time.Duration // How long to run (0 = infinite)
}

// NewStreamingConfig creates default streaming configuration
func NewStreamingConfig() *StreamingConfig {
	return &StreamingConfig{
		BatchSize:     10,
		FlushInterval: 50 * time.Millisecond,
		WorkerCount:   4,
		MessageRate:   0, // Unlimited
		RunDuration:   0, // Infinite
	}
}

// StreamingStats holds runtime statistics
type StreamingStats struct {
	mu                sync.RWMutex
	TotalMessages     int64
	TotalBatches      int64
	MessagesPerSecond float64
	StartTime         time.Time
	Errors            int64
	StreamLength      int64
}

// Update updates the statistics
func (s *StreamingStats) Update(messages int64, batches int64, errors int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TotalMessages += messages
	s.TotalBatches += batches
	s.Errors += errors
	elapsed := time.Since(s.StartTime).Seconds()
	if elapsed > 0 {
		s.MessagesPerSecond = float64(s.TotalMessages) / elapsed
	}
}

// UpdateStreamLength updates the current stream length
func (s *StreamingStats) UpdateStreamLength(length int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.StreamLength = length
}

// GetStats returns current statistics
func (s *StreamingStats) GetStats() (int64, int64, float64, int64, int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.TotalMessages, s.TotalBatches, s.MessagesPerSecond, s.Errors, s.StreamLength
}

// StartHighPerformanceStreaming starts continuous message streaming with batching
func (c *AzureRedisClient) StartHighPerformanceStreaming(ctx context.Context, config *StreamingConfig) {
	stats := &StreamingStats{
		StartTime: time.Now(),
	}

	// Load message template once at startup
	messageTemplate, err := loadMessageTemplate("test_message.json")
	if err != nil {
		fmt.Printf("❌ Failed to load message template: %v\n", err)
		return
	}
	fmt.Printf("✅ Message template loaded successfully\n")

	// Create a cancelable context for coordinating shutdown
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create channels for coordination
	messageChan := make(chan StreamMessage, config.BatchSize*config.WorkerCount)
	doneChan := make(chan struct{})

	// Start message generator with cached template
	go c.messageGenerator(ctx, messageChan, config, stats, messageTemplate)

	// Start stream length monitor
	go c.streamLengthMonitor(ctx, stats)

	// Start batch processors
	var wg sync.WaitGroup
	for i := 0; i < config.WorkerCount; i++ {
		wg.Add(1)
		go c.batchProcessor(ctx, messageChan, &wg, config, stats, i)
	}

	// Start statistics reporter
	go c.statsReporter(ctx, stats, doneChan)

	// Handle shutdown - this will cancel the context when signal is received
	c.handleShutdown(ctx, cancel, config, &wg, doneChan)
}

// messageGenerator continuously generates messages using cached template
func (c *AzureRedisClient) messageGenerator(ctx context.Context, messageChan chan<- StreamMessage, config *StreamingConfig, stats *StreamingStats, messageTemplate *MessageTemplate) {
	defer close(messageChan) // Close channel when generator exits

	var rateLimiter <-chan time.Time
	if config.MessageRate > 0 {
		rateLimiter = time.Tick(time.Second / time.Duration(config.MessageRate))
	}

	var endTime time.Time
	if config.RunDuration > 0 {
		endTime = time.Now().Add(config.RunDuration)
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Check if we should stop based on duration
			if config.RunDuration > 0 && time.Now().After(endTime) {
				return
			}

			// Rate limiting
			if rateLimiter != nil {
				select {
				case <-rateLimiter:
				case <-ctx.Done():
					return
				}
			}

			// Generate message using cached template (much faster!)
			tradeData, err := messageTemplate.generateMessage()
			if err != nil {
				stats.Update(0, 0, 1)
				continue
			}

			message := StreamMessage{
				Fields: tradeData,
			}

			// Send to batch processor with context checking
			select {
			case messageChan <- message:
			case <-ctx.Done():
				return
			}
		}
	}
}

// batchProcessor processes messages in batches
func (c *AzureRedisClient) batchProcessor(ctx context.Context, messageChan <-chan StreamMessage, wg *sync.WaitGroup, config *StreamingConfig, stats *StreamingStats, workerID int) {
	defer wg.Done()

	batch := make([]StreamMessage, 0, config.BatchSize)
	flushTimer := time.NewTimer(config.FlushInterval)
	defer flushTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			// Context cancelled, flush remaining messages with timeout
			if len(batch) > 0 {
				// Create a shutdown timeout context
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				c.flushBatch(shutdownCtx, batch, stats, workerID)
			}
			return

		case message, ok := <-messageChan:
			if !ok {
				// Channel closed, flush remaining messages with timeout
				if len(batch) > 0 {
					// Create a shutdown timeout context
					shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					c.flushBatch(shutdownCtx, batch, stats, workerID)
				}
				return
			}

			batch = append(batch, message)

			// Flush if batch is full
			if len(batch) >= config.BatchSize {
				c.flushBatch(ctx, batch, stats, workerID)
				batch = batch[:0] // Reset batch
				if !flushTimer.Stop() {
					<-flushTimer.C
				}
				flushTimer.Reset(config.FlushInterval)
			}

		case <-flushTimer.C:
			// Flush on timer
			if len(batch) > 0 {
				c.flushBatch(ctx, batch, stats, workerID)
				batch = batch[:0] // Reset batch
			}
			flushTimer.Reset(config.FlushInterval)
		}
	}
}

// flushBatch sends a batch of messages to Redis streams with timestamp indexing for fast queries
func (c *AzureRedisClient) flushBatch(ctx context.Context, batch []StreamMessage, stats *StreamingStats, workerID int) {
	if len(batch) == 0 {
		return
	}

	// Group messages by symbol for separate streams
	symbolBatches := make(map[string][]StreamMessage)

	for _, message := range batch {
		// Extract symbol from message fields
		if symbolInterface, exists := message.Fields["symbol"]; exists {
			if symbol, ok := symbolInterface.(string); ok {
				symbolBatches[symbol] = append(symbolBatches[symbol], message)
			}
		}
	}

	// Process each symbol batch with hybrid approach (streams + timestamp index)
	totalErrors := int64(0)
	totalProcessed := int64(0)

	for symbol, symbolBatch := range symbolBatches {
		streamName := fmt.Sprintf("tick_%s", symbol)

		// Phase 1: Add messages to stream using pipeline
		streamPipe := c.client.Pipeline()
		var streamCmds []*redis.StringCmd

		for _, message := range symbolBatch {
			args := &redis.XAddArgs{
				Stream: streamName,
				Values: message.Fields,
				MaxLen: c.config.MaxLen, // Use configurable max length
				Approx: true,
			}
			streamCmds = append(streamCmds, streamPipe.XAdd(ctx, args))
		}

		// Execute stream pipeline with timeout context for shutdown
		execCtx := ctx
		if ctx.Err() != nil {
			// During shutdown, create a timeout context to allow graceful completion
			var cancel context.CancelFunc
			execCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
		}

		_, err := streamPipe.Exec(execCtx)
		if err != nil {
			totalErrors++
			// Only log as error if it's not a context cancellation during shutdown
			if ctx.Err() == nil {
				c.logger.WithFields(logrus.Fields{
					"worker": workerID,
					"stream": streamName,
					"count":  len(symbolBatch),
					"error":  err,
				}).Error("Failed to flush symbol batch to stream")
			}
			continue
		}

		// Phase 2: Add timestamp index entries for fast time-based queries
		indexPipe := c.client.Pipeline()
		indexKey := fmt.Sprintf("ts_idx_%s", symbol)
		indexEntries := 0

		for i, cmd := range streamCmds {
			if cmd.Err() == nil {
				streamID := cmd.Val()

				// Extract timestamp from message for indexing
				if timestampInterface, exists := symbolBatch[i].Fields["timestamp"]; exists {
					if timestampStr, ok := timestampInterface.(string); ok {
						if ts, err := time.Parse(time.RFC3339Nano, timestampStr); err == nil {
							// Add to sorted set: score = timestamp_millis, member = stream_id
							// Use timeout context for shutdown operations
							indexCtx := context.Background()
							if ctx.Err() != nil {
								var cancel context.CancelFunc
								indexCtx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
								defer cancel()
							}

							indexPipe.ZAdd(indexCtx, indexKey, redis.Z{
								Score:  float64(ts.UnixMilli()),
								Member: streamID,
							})
							indexEntries++

							// Set TTL on timestamp index (optional - for data retention)
							if indexEntries == 1 { // Set TTL once per batch
								indexPipe.Expire(indexCtx, indexKey, 7*24*time.Hour) // 7 days retention
							}
						}
					}
				}
			}
		}

		// Execute timestamp index pipeline (non-blocking for performance)
		if indexEntries > 0 {
			go func(pipe redis.Pipeliner, streamName string, count int, shutdownCtx context.Context) {
				// Use the shutdown context instead of Background to respect cancellation
				_, indexErr := pipe.Exec(shutdownCtx)
				if indexErr != nil {
					// Only log as warning if it's not due to context cancellation during shutdown
					if shutdownCtx.Err() == nil {
						c.logger.WithFields(logrus.Fields{
							"stream": streamName,
							"count":  count,
							"error":  indexErr,
						}).Warn("Failed to update timestamp index")
					}
				}
			}(indexPipe, streamName, indexEntries, ctx)
		}

		totalProcessed += int64(len(symbolBatch))
	}

	// Update stats with total processed and errors
	stats.Update(totalProcessed, int64(len(symbolBatches)), totalErrors)
}

// streamLengthMonitor monitors the total length across all symbol streams
func (c *AzureRedisClient) streamLengthMonitor(ctx context.Context, stats *StreamingStats) {
	ticker := time.NewTicker(5 * time.Second) // Check stream length every 5 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Check if context is cancelled before making Redis calls
			if ctx.Err() != nil {
				return
			}

			// Get list of all streams that match our pattern
			keys, err := c.client.Keys(ctx, "tick_*").Result()
			if err != nil {
				// Don't log context cancelled errors during shutdown
				if ctx.Err() == nil {
					c.logger.WithFields(logrus.Fields{
						"error": err,
					}).Warn("Failed to get stream keys")
				}
				continue
			}

			// Sum up lengths from all symbol streams
			totalLength := int64(0)
			for _, streamName := range keys {
				// Check context again before each stream info call
				if ctx.Err() != nil {
					return
				}

				streamInfo, err := c.GetStreamInfo(ctx, streamName)
				if err != nil {
					// Don't log context cancelled errors during shutdown
					if ctx.Err() == nil {
						c.logger.WithFields(logrus.Fields{
							"stream": streamName,
							"error":  err,
						}).Debug("Failed to get stream info")
					}
					continue
				}
				totalLength += streamInfo.Length
			}

			// Update stats with total length across all streams
			stats.UpdateStreamLength(totalLength)
		}
	}
}

// statsReporter reports statistics periodically
func (c *AzureRedisClient) statsReporter(ctx context.Context, stats *StreamingStats, doneChan chan<- struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// Get pod/hostname for identification
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	for {
		select {
		case <-ctx.Done():
			doneChan <- struct{}{}
			return
		case <-ticker.C:
			messages, batches, rate, errors, streamLength := stats.GetStats()

			// Get stream count for additional info - only if context is not cancelled
			streamCount := 0
			if ctx.Err() == nil {
				keys, err := c.client.Keys(ctx, "tick_*").Result()
				if err == nil {
					streamCount = len(keys)
				}
			}

			// Print statistics with pod identification and stream info
			fmt.Printf("📊 [%s] Pod: %s | Messages: %d | Batches: %d | Rate: %.2f msg/sec | Errors: %d | Streams: %d | Total Messages: %d | Uptime: %s\n",
				time.Now().Format("15:04:05"),
				hostname,
				messages, batches, rate, errors, streamCount, streamLength,
				time.Since(stats.StartTime).Round(time.Second),
			)
		}
	}
}

// handleShutdown handles graceful shutdown
func (c *AzureRedisClient) handleShutdown(ctx context.Context, cancel context.CancelFunc, config *StreamingConfig, wg *sync.WaitGroup, doneChan <-chan struct{}) {
	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal or duration completion
	select {
	case <-sigChan:
		fmt.Println("\n🛑 Shutdown signal received, stopping gracefully...")
		cancel() // Cancel the context to signal all goroutines to stop
	case <-doneChan:
		fmt.Println("📊 Statistics reporter finished")
		cancel() // Cancel the context to signal all goroutines to stop
	}

	// Give workers time to finish current batches
	fmt.Println("⏳ Waiting for workers to finish current batches...")

	// Wait for all workers to finish with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		fmt.Println("✅ All workers finished gracefully")
	case <-time.After(30 * time.Second): // Increased timeout for large batches
		fmt.Println("⚠️  Timeout waiting for workers, forcing shutdown")
	}

	fmt.Println("✅ Shutdown complete")
}

// Example usage
func main() {
	ctx := context.Background()

	// Create configuration
	config := NewRedisConfig()

	// Set your Azure Redis configuration
	// Username is only required for Azure Redis (not localhost)
	if config.UseAAD && config.Username == "" {
		log.Fatal("REDIS_USERNAME environment variable is required for Azure Redis")
	}

	// Create client
	client, err := NewAzureRedisClient(config)
	if err != nil {
		log.Fatal("Failed to create Redis client:", err)
	}
	defer client.Close()

	// Start health check server
	healthChecker := NewHealthChecker(client, 30*time.Second)
	StartHealthServer(healthChecker, 8080)
	log.Printf("🏥 Health check server started on port 8080")

	// Configure high-performance streaming for AKS
	streamingConfig := NewStreamingConfig()
	streamingConfig.BatchSize = 5000                      // 10x increase for maximum throughput
	streamingConfig.FlushInterval = 25 * time.Millisecond // Was 50ms
	streamingConfig.WorkerCount = 32                      // More workers for AKS resources
	streamingConfig.MessageRate = 0                       // Unlimited rate
	streamingConfig.RunDuration = 0                       // Run indefinitely

	log.Printf("🚀 Starting high-performance Redis streaming...")
	log.Printf("📊 Configuration:")
	log.Printf("   - Batch Size: %d messages", streamingConfig.BatchSize)
	log.Printf("   - Flush Interval: %v", streamingConfig.FlushInterval)
	log.Printf("   - Workers: %d", streamingConfig.WorkerCount)
	log.Printf("   - Rate Limit: %s", func() string {
		if streamingConfig.MessageRate == 0 {
			return "unlimited"
		}
		return fmt.Sprintf("%d msg/sec", streamingConfig.MessageRate)
	}())
	log.Printf("   - Duration: %s", func() string {
		if streamingConfig.RunDuration == 0 {
			return "infinite"
		}
		return streamingConfig.RunDuration.String()
	}())
	log.Printf("⏹️  Press Ctrl+C to stop")
	log.Printf("==========================================")

	// Start streaming (this will block until interrupted)
	client.StartHighPerformanceStreaming(ctx, streamingConfig)
}
