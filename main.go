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
	"sync"
	"syscall"
	"text/template"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// AzureRedisClient represents the Azure Redis client with stream operations
type AzureRedisClient struct {
	client *redis.Client
	logger *logrus.Logger
	config *RedisConfig
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
	if host == "" {
		host = "localhost"
	}

	// For localhost, use standard Redis port and disable AAD
	port := 6380
	useAAD := true
	if host == "localhost" {
		port = 6379    // Standard Redis port for localhost
		useAAD = false // Disable Azure AD for localhost
	}

	return &RedisConfig{
		Host:          host,
		Port:          port,
		Username:      os.Getenv("REDIS_USERNAME"),
		UseAAD:        useAAD,
		MaxRetries:    3,
		DialTimeout:   5 * time.Second,
		ReadTimeout:   3 * time.Second,
		WriteTimeout:  3 * time.Second,
		PoolSize:      10,
		MinIdleConns:  5,
		MaxConnAge:    30 * time.Minute,
		PoolTimeout:   4 * time.Second,
		IdleTimeout:   5 * time.Minute,
		IdleCheckFreq: 1 * time.Minute,
	}
}

// NewAzureRedisClient creates a new Azure Redis client with AAD authentication
func NewAzureRedisClient(config *RedisConfig) (*AzureRedisClient, error) {
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel) // Reduced logging verbosity - only warnings and errors

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

	// Use Azure AD authentication if enabled
	if config.UseAAD {
		token, err := getAzureADToken()
		if err != nil {
			return nil, fmt.Errorf("failed to get Azure AD token: %w", err)
		}
		opts.Password = token
	}

	client := redis.NewClient(opts)

	// Test connection with retry logic
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := retryOperation(ctx, func() error {
		return client.Ping(ctx).Err()
	}, 3, time.Second)

	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Print connection success directly to stdout so it's always visible
	fmt.Printf("✅ Successfully connected to Redis at %s:%d\n", config.Host, config.Port)

	return &AzureRedisClient{
		client: client,
		logger: logger,
		config: config,
	}, nil
}

// getAzureADToken retrieves an Azure AD token for Redis authentication
func getAzureADToken() (string, error) {
	// Use DefaultAzureCredential for authentication
	// This will try Managed Identity first, then fall back to other methods
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return "", fmt.Errorf("failed to create Azure credential: %w", err)
	}

	// Get token for Azure Redis scope
	tokenRequestOptions := policy.TokenRequestOptions{
		Scopes: []string{"https://redis.azure.com/.default"},
	}
	token, err := cred.GetToken(context.Background(), tokenRequestOptions)
	if err != nil {
		return "", fmt.Errorf("failed to get token: %w", err)
	}

	return token.Token, nil
}

// AddToStream adds a message to a Redis stream with error handling and retry logic
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
	return c.client.Close()
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
}

// generateTradeId generates a unique trade ID
func generateTradeId() string {
	return fmt.Sprintf("TXN-%s-%09d",
		time.Now().Format("2006-0102"),
		rand.Intn(1000000000))
}

// loadTradeTemplate loads and processes the trade message template
func loadTradeTemplate(templatePath string) (map[string]interface{}, error) {
	// Read the template file
	templateContent, err := os.ReadFile(templatePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read template file: %w", err)
	}

	// Create template data
	data := TradeTemplate{
		TradeId:   generateTradeId(),
		Timestamp: time.Now().Format(time.RFC3339Nano),
	}

	// Parse and execute template
	tmpl, err := template.New("trade").Parse(string(templateContent))
	if err != nil {
		return nil, fmt.Errorf("failed to parse template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
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

// GetStats returns current statistics
func (s *StreamingStats) GetStats() (int64, int64, float64, int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.TotalMessages, s.TotalBatches, s.MessagesPerSecond, s.Errors
}

// StartHighPerformanceStreaming starts continuous message streaming with batching
func (c *AzureRedisClient) StartHighPerformanceStreaming(ctx context.Context, config *StreamingConfig) {
	stats := &StreamingStats{
		StartTime: time.Now(),
	}

	// Create a cancelable context for coordinating shutdown
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create channels for coordination
	messageChan := make(chan StreamMessage, config.BatchSize*config.WorkerCount)
	doneChan := make(chan struct{})

	// Start message generator
	go c.messageGenerator(ctx, messageChan, config, stats)

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

// messageGenerator continuously generates messages
func (c *AzureRedisClient) messageGenerator(ctx context.Context, messageChan chan<- StreamMessage, config *StreamingConfig, stats *StreamingStats) {
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

			// Generate message
			tradeData, err := loadTradeTemplate("test_message.json")
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
			// Context cancelled, flush remaining messages gracefully
			if len(batch) > 0 {
				c.flushBatch(context.Background(), batch, stats, workerID) // Use background context for final flush
			}
			return

		case message, ok := <-messageChan:
			if !ok {
				// Channel closed, flush remaining messages and exit
				if len(batch) > 0 {
					c.flushBatch(context.Background(), batch, stats, workerID) // Use background context for final flush
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

// flushBatch sends a batch of messages to Redis
func (c *AzureRedisClient) flushBatch(ctx context.Context, batch []StreamMessage, stats *StreamingStats, workerID int) {
	if len(batch) == 0 {
		return
	}

	_, err := c.AddBatchToStream(ctx, "trade_events", batch, &StreamAddOptions{
		MaxLen:      10000,
		Approximate: true,
	})

	if err != nil {
		stats.Update(0, 0, 1)
		c.logger.WithFields(logrus.Fields{
			"worker": workerID,
			"error":  err,
		}).Error("Failed to flush batch")
	} else {
		stats.Update(int64(len(batch)), 1, 0)
	}
}

// statsReporter reports statistics periodically
func (c *AzureRedisClient) statsReporter(ctx context.Context, stats *StreamingStats, doneChan chan<- struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			doneChan <- struct{}{}
			return
		case <-ticker.C:
			messages, batches, rate, errors := stats.GetStats()
			// Print statistics directly to stdout (not through logger) to avoid log level issues
			fmt.Printf("📊 [%s] Messages: %d | Batches: %d | Rate: %.2f msg/sec | Errors: %d | Uptime: %s\n",
				time.Now().Format("15:04:05"),
				messages, batches, rate, errors,
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
	case <-time.After(10 * time.Second):
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

	// Configure high-performance streaming
	streamingConfig := NewStreamingConfig()
	streamingConfig.BatchSize = 20                        // Batch 20 messages together
	streamingConfig.FlushInterval = 25 * time.Millisecond // Flush every 25ms
	streamingConfig.WorkerCount = 8                       // 8 concurrent workers
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
