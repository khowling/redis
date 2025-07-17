#!/bin/bash

echo "🔍 Starting Redis Stream Monitor"
echo "📊 Monitoring: trade_events, user_events"
echo "⏹️  Press Ctrl+C to stop"
echo "================================"

# Get the latest message ID for each stream to start from
TRADE_LAST=$(redis-cli XINFO STREAM trade_events 2>/dev/null | grep -A1 "last-generated-id" | tail -1 | tr -d '"' || echo '$')
USER_LAST=$(redis-cli XINFO STREAM user_events 2>/dev/null | grep -A1 "last-generated-id" | tail -1 | tr -d '"' || echo '$')

echo "Starting from:"
echo "  trade_events: $TRADE_LAST"
echo "  user_events: $USER_LAST"
echo "================================"

# Continuous monitoring loop
while true; do
    # Read new messages with a 1-second timeout
    result=$(redis-cli XREAD BLOCK 1000 STREAMS trade_events user_events "$TRADE_LAST" "$USER_LAST" 2>/dev/null)
    
    if [ $? -eq 0 ] && [ ! -z "$result" ]; then
        echo "🔔 New messages received at $(date):"
        echo "$result" | sed 's/^/  /'
        echo "--------------------------------"
        
        # Update last seen IDs (simplified - you might want to parse this properly)
        TRADE_LAST=$(echo "$result" | grep -A1 "trade_events" | tail -1 | awk '{print $1}' || echo "$TRADE_LAST")
        USER_LAST=$(echo "$result" | grep -A1 "user_events" | tail -1 | awk '{print $1}' || echo "$USER_LAST")
    fi
    
    # Small delay to prevent excessive CPU usage
    sleep 0.1
done
