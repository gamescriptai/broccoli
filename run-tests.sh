#!/bin/bash

# Start Redis server using Docker
echo "Starting Redis server using Docker..."
docker run --name test-redis -d -p 6380:6379 redis > /dev/null

# Wait for Redis server to start
sleep 2

# Run cargo tests
echo "Running cargo tests..."
cargo test

# Stop and remove Redis container
echo "Stopping Redis server..."
docker stop test-redis > /dev/null
docker rm test-redis > /dev/null