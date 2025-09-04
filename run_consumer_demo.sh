#!/bin/bash

echo "Starting Member 1 Consumer..."
echo "=========================================="

# Start the consumer and show output for 10 seconds, then kill it
(gradle runMember1 & echo $! > /tmp/consumer.pid) &
sleep 5

# Send a newline to stop the consumer
echo "" | gradle runMember1 2>/dev/null || true

# Clean up
if [ -f /tmp/consumer.pid ]; then
    kill $(cat /tmp/consumer.pid) 2>/dev/null || true
    rm /tmp/consumer.pid
fi

echo "Demo completed!"