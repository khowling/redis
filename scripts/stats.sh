#!/bin/bash

# Performance statistics for Redis streaming

echo "📊 Redis Streaming Performance Statistics"
echo "========================================"

# Check if Redis is running
if ! redis-cli ping > /dev/null 2>&1; then
    echo "❌ Redis is not running or not accessible"
    exit 1
fi

echo "✅ Redis is running"

# Get stream information
stream_length=$(redis-cli XLEN trade_events)
echo "📈 Stream Length: $stream_length messages"

# Get Redis info
redis_info=$(redis-cli INFO stats | grep -E "(total_commands_processed|instantaneous_ops_per_sec|keyspace_hits|keyspace_misses)")
echo "📊 Redis Statistics:"
echo "$redis_info"

# Get memory usage
memory_info=$(redis-cli INFO memory | grep -E "(used_memory_human|used_memory_peak_human)")
echo "💾 Memory Usage:"
echo "$memory_info"

# Show some sample messages
echo "🔍 Sample Messages (last 3):"
redis-cli XREVRANGE trade_events + - COUNT 3

echo "========================================"
