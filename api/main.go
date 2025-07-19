package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/redis/go-redis/v9"
)

// APIServer handles HTTP requests for Redis stream data
type APIServer struct {
	redisClient *AzureRedisClient
}

// StreamMessageResponse represents a Redis stream message in API response
type StreamMessageResponse struct {
	ID        string                 `json:"id"`
	Stream    string                 `json:"stream"`
	Symbol    string                 `json:"symbol"`
	Timestamp string                 `json:"timestamp"`
	Fields    map[string]interface{} `json:"fields"`
}

// QueryResponse represents the API response structure
type QueryResponse struct {
	Messages []StreamMessageResponse `json:"messages"`
	Count    int                     `json:"count"`
	Symbols  []string                `json:"symbols"`
	Query    QueryParams             `json:"query"`
}

// QueryParams represents the query parameters
type QueryParams struct {
	Symbols   []string `json:"symbols"`
	StartTime string   `json:"start_time,omitempty"`
	EndTime   string   `json:"end_time,omitempty"`
	Limit     int      `json:"limit"`
}

// ErrorResponse represents API error responses
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// NewAPIServer creates a new API server
func NewAPIServer(redisClient *AzureRedisClient) *APIServer {
	return &APIServer{
		redisClient: redisClient,
	}
}

// GetMessages handles GET /api/messages endpoint
func (s *APIServer) GetMessages(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Content-Type", "application/json")

	// Handle preflight OPTIONS request
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Parse query parameters
	query := r.URL.Query()

	// Get symbols (required)
	symbolsParam := query.Get("symbols")
	if symbolsParam == "" {
		s.writeError(w, http.StatusBadRequest, "symbols parameter is required", "Provide comma-separated list of symbols")
		return
	}
	symbols := strings.Split(symbolsParam, ",")
	for i, symbol := range symbols {
		symbols[i] = strings.TrimSpace(strings.ToUpper(symbol))
	}

	// Get optional parameters
	startTime := query.Get("start")
	endTime := query.Get("end")
	limitParam := query.Get("limit")

	// Parse limit (default: 1000, max: 100000)
	limit := 1000
	if limitParam != "" {
		if parsedLimit, err := strconv.Atoi(limitParam); err == nil {
			limit = parsedLimit
			if limit > 100000 {
				limit = 100000 // Cap at 100k messages
			}
			if limit < 1 {
				limit = 1
			}
		}
	}

	// Query Redis streams
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	messages, err := s.queryStreams(ctx, symbols, startTime, endTime, limit)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to query streams", err.Error())
		return
	}

	// Prepare response
	response := QueryResponse{
		Messages: messages,
		Count:    len(messages),
		Symbols:  symbols,
		Query: QueryParams{
			Symbols:   symbols,
			StartTime: startTime,
			EndTime:   endTime,
			Limit:     limit,
		},
	}

	// Write JSON response
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// queryStreams queries multiple Redis streams using timestamp indexes for fast time-based queries
func (s *APIServer) queryStreams(ctx context.Context, symbols []string, startTime, endTime string, limit int) ([]StreamMessageResponse, error) {
	var allMessages []StreamMessageResponse
	messagesPerSymbol := limit / len(symbols)
	if messagesPerSymbol < 1 {
		messagesPerSymbol = 1
	}

	for _, symbol := range symbols {
		streamName := fmt.Sprintf("tick_%s", symbol)
		indexKey := fmt.Sprintf("ts_idx_%s", symbol)

		// Check if stream exists
		exists, err := s.redisClient.client.Exists(ctx, streamName).Result()
		if err != nil {
			log.Printf("Error checking stream existence for %s: %v", streamName, err)
			continue
		}
		if exists == 0 {
			log.Printf("Stream %s does not exist", streamName)
			continue
		}

		var streamIDs []string

		// Use timestamp index for fast time-based queries when timestamps are provided
		if startTime != "" || endTime != "" {
			// Parse timestamps to milliseconds
			var startMs, endMs int64
			startMs = 0           // Default to beginning
			endMs = 9999999999999 // Default to far future

			if startTime != "" {
				if ts, err := parseTimestamp(startTime); err == nil {
					startMs = ts
				}
			}
			if endTime != "" {
				if ts, err := parseTimestamp(endTime); err == nil {
					endMs = ts
				}
			}

			// Use ZRANGEBYSCORE on timestamp index for O(log N) query performance
			indexResults, err := s.redisClient.client.ZRangeByScore(ctx, indexKey, &redis.ZRangeBy{
				Min:   fmt.Sprintf("%d", startMs),
				Max:   fmt.Sprintf("%d", endMs),
				Count: int64(messagesPerSymbol),
			}).Result()

			if err != nil {
				log.Printf("Error querying timestamp index %s: %v", indexKey, err)
				// Fallback to stream scan if index query fails
				streamIDs = s.fallbackStreamQuery(ctx, streamName, startTime, endTime, messagesPerSymbol)
			} else {
				streamIDs = indexResults
				log.Printf("Found %d stream IDs in timestamp range for %s using index", len(streamIDs), symbol)
			}
		} else {
			// For recent messages without time filter, get from timestamp index (newest first)
			indexResults, err := s.redisClient.client.ZRevRange(ctx, indexKey, 0, int64(messagesPerSymbol-1)).Result()
			if err != nil {
				log.Printf("Error getting recent messages from index %s: %v", indexKey, err)
				// Fallback to stream scan if index query fails
				streamIDs = s.fallbackStreamQuery(ctx, streamName, "", "", messagesPerSymbol)
			} else {
				streamIDs = indexResults
				log.Printf("Found %d recent stream IDs for %s using index", len(streamIDs), symbol)
			}
		}

		// Fetch actual messages from stream using the stream IDs from index
		if len(streamIDs) > 0 {
			messages, err := s.fetchMessagesFromStream(ctx, streamName, streamIDs, symbol)
			if err != nil {
				log.Printf("Error fetching messages from stream %s: %v", streamName, err)
				continue
			}
			allMessages = append(allMessages, messages...)
		}
	}

	// Sort by timestamp (most recent first) and limit
	if len(allMessages) > limit {
		allMessages = allMessages[:limit]
	}

	return allMessages, nil
}

// parseTimestamp parses various timestamp formats
func parseTimestamp(ts string) (int64, error) {
	// Try Unix timestamp first
	if unixTs, err := strconv.ParseInt(ts, 10, 64); err == nil {
		return unixTs * 1000, nil // Convert to milliseconds
	}

	// Try RFC3339 format
	if t, err := time.Parse(time.RFC3339, ts); err == nil {
		return t.UnixMilli(), nil
	}

	// Try RFC3339Nano format
	if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
		return t.UnixMilli(), nil
	}

	// Try common formats
	formats := []string{
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, ts); err == nil {
			return t.UnixMilli(), nil
		}
	}

	return 0, fmt.Errorf("unable to parse timestamp: %s", ts)
}

// extractTimestamp extracts timestamp from message fields
func extractTimestamp(fields map[string]interface{}) string {
	if ts, exists := fields["timestamp"]; exists {
		if tsStr, ok := ts.(string); ok {
			return tsStr
		}
	}
	return ""
}

// writeError writes an error response
func (s *APIServer) writeError(w http.ResponseWriter, statusCode int, error, message string) {
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{
		Error:   error,
		Message: message,
	})
}

// HealthCheck handles GET /health endpoint
func (s *APIServer) HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Test Redis connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := s.redisClient.client.Ping(ctx).Err()
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "unhealthy",
			"redis":  "disconnected",
			"error":  err.Error(),
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "healthy",
		"redis":  "connected",
		"time":   time.Now().Format(time.RFC3339),
	})
}

// GetStreams handles GET /api/streams endpoint
func (s *APIServer) GetStreams(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get all tick streams
	keys, err := s.redisClient.client.Keys(ctx, "tick_*").Result()
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "Failed to get streams", err.Error())
		return
	}

	// Get stream info for each
	streams := make([]map[string]interface{}, 0, len(keys))
	for _, streamName := range keys {
		info, err := s.redisClient.GetStreamInfo(ctx, streamName)
		if err != nil {
			continue
		}

		symbol := strings.TrimPrefix(streamName, "tick_")
		streams = append(streams, map[string]interface{}{
			"stream":      streamName,
			"symbol":      symbol,
			"length":      info.Length,
			"first_entry": info.FirstEntry.ID,
			"last_entry":  info.LastEntry.ID,
		})
	}

	response := map[string]interface{}{
		"streams": streams,
		"count":   len(streams),
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// fallbackStreamQuery performs traditional stream scanning when index is unavailable
func (s *APIServer) fallbackStreamQuery(ctx context.Context, streamName, startTime, endTime string, limit int) []string {
	// Build XRANGE arguments
	start := "-" // From beginning
	end := "+"   // To end

	// Parse timestamps if provided
	if startTime != "" {
		if ts, err := parseTimestamp(startTime); err == nil {
			start = fmt.Sprintf("%d-0", ts)
		}
	}
	if endTime != "" {
		if ts, err := parseTimestamp(endTime); err == nil {
			end = fmt.Sprintf("%d-0", ts)
		}
	}

	// Query stream with XRANGE or XREVRANGE
	var messages []redis.XMessage
	var err error

	if startTime == "" && endTime == "" {
		// Get most recent messages
		messages, err = s.redisClient.client.XRevRangeN(ctx, streamName, "+", "-", int64(limit)).Result()
	} else {
		// Get messages in time range
		messages, err = s.redisClient.client.XRangeN(ctx, streamName, start, end, int64(limit)).Result()
	}

	if err != nil {
		log.Printf("Error in fallback stream query %s: %v", streamName, err)
		return []string{}
	}

	// Extract stream IDs
	streamIDs := make([]string, len(messages))
	for i, msg := range messages {
		streamIDs[i] = msg.ID
	}

	return streamIDs
}

// fetchMessagesFromStream retrieves specific messages from stream by their IDs
func (s *APIServer) fetchMessagesFromStream(ctx context.Context, streamName string, streamIDs []string, symbol string) ([]StreamMessageResponse, error) {
	var messages []StreamMessageResponse

	// Use pipeline for efficient batch retrieval of multiple messages
	pipe := s.redisClient.client.Pipeline()
	var commands []*redis.XMessageSliceCmd

	// For each stream ID, query the specific message
	for _, streamID := range streamIDs {
		// XRANGE with same start and end gives us the specific message
		cmd := pipe.XRange(ctx, streamName, streamID, streamID)
		commands = append(commands, cmd)
	}

	// Execute pipeline
	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute message fetch pipeline: %w", err)
	}

	// Process results
	for _, cmd := range commands {
		if cmd.Err() != nil {
			log.Printf("Error fetching message: %v", cmd.Err())
			continue
		}

		cmdMessages := cmd.Val()
		for _, msg := range cmdMessages {
			messages = append(messages, StreamMessageResponse{
				ID:        msg.ID,
				Stream:    streamName,
				Symbol:    symbol,
				Timestamp: extractTimestamp(msg.Values),
				Fields:    msg.Values,
			})
		}
	}

	return messages, nil
}

func main() {
	// Create Redis client
	config := NewRedisConfig()
	redisClient, err := NewAzureRedisClient(config)
	if err != nil {
		log.Fatal("Failed to create Redis client:", err)
	}
	defer redisClient.Close()

	// Create API server
	apiServer := NewAPIServer(redisClient)

	// Setup routes
	r := mux.NewRouter()

	// API routes
	r.HandleFunc("/api/messages", apiServer.GetMessages).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/streams", apiServer.GetStreams).Methods("GET", "OPTIONS")
	r.HandleFunc("/health", apiServer.HealthCheck).Methods("GET")

	// Root endpoint with usage info
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"service": "Redis Stream API",
			"version": "1.0.0",
			"endpoints": map[string]string{
				"GET /api/messages": "Query messages from streams. Params: symbols (required), start, end, limit",
				"GET /api/streams":  "List all available streams",
				"GET /health":       "Health check",
			},
			"examples": map[string]string{
				"single_symbol":    "/api/messages?symbols=AAL&limit=10",
				"multiple_symbols": "/api/messages?symbols=AAL,BP,BARC&limit=50",
				"time_range":       "/api/messages?symbols=AAL&start=2024-01-01T00:00:00Z&end=2024-01-01T23:59:59Z&limit=100",
			},
		})
	}).Methods("GET")

	// Start server
	port := ":8081"
	log.Printf("🚀 Redis Stream API Server starting on port %s", port)
	log.Printf("📖 API Documentation available at: http://localhost%s", port)
	log.Printf("🏥 Health check available at: http://localhost%s/health", port)
	log.Printf("📊 Example: http://localhost%s/api/messages?symbols=AAL,BP&limit=10", port)

	log.Fatal(http.ListenAndServe(port, r))
}
