#!/bin/bash

# Simple monitoring script for Redis streams

echo "🔍 Monitoring Redis Stream: trade_events"
echo "================================================"

while true; do
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Stream Info:"
    
    # Get stream length
    redis-cli -h localhost -p 6379 XLEN trade_events
    
    # Get latest messages (last 5)
    echo "Latest 5 messages:"
    redis-cli -h localhost -p 6379 XREVRANGE trade_events + - COUNT 5
    
    echo "================================================"
    sleep 2
done
