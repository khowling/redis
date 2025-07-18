#!/bin/bash

# Redis Streaming Platform Build Script

set -e

echo "🏗️  Building Redis Streaming Platform"
echo "====================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Create dist directory
echo "0. Setting up build directories..."
mkdir -p dist
print_status "Created dist/ directory"

# Build Streamer Application
echo ""
echo "1. Building Streamer Application..."
cd streamer
if go mod tidy && go build -o ../dist/redis-streamer; then
    print_status "Streamer application built successfully"
else
    print_error "Failed to build streamer application"
    exit 1
fi
cd ..

# Build API Application
echo ""
echo "2. Building API Application..."
cd api
if go mod tidy && go build -o ../dist/redis-query-api; then
    print_status "API application built successfully"
else
    print_error "Failed to build API application"
    exit 1
fi
cd ..

# Docker builds (optional)
if command -v docker &> /dev/null; then
    echo ""
    echo "3. Building Docker Images..."
    
    # Build streamer image
    if docker build -t redis-streamer:latest streamer/; then
        print_status "Streamer Docker image built successfully"
    else
        print_warning "Failed to build streamer Docker image"
    fi
    
    # Build API image
    if docker build -t redis-query-api:latest api/; then
        print_status "API Docker image built successfully"
    else
        print_warning "Failed to build API Docker image"
    fi
else
    print_warning "Docker not found, skipping Docker builds"
fi

# Summary
echo ""
echo "🎉 Build Complete!"
echo "=================="
echo "Executables created:"
echo "  • dist/redis-streamer"
echo "  • dist/redis-query-api"
echo ""
echo "To run locally:"
echo "  1. Start Redis: docker-compose up redis-local"
echo "  2. Start streamer: REDIS_HOST=localhost ./dist/redis-streamer"
echo "  3. Start API: REDIS_HOST=localhost ./dist/redis-query-api"
echo "  4. Test API: curl http://localhost:8081/health"
echo ""
echo "Docker images built (if Docker available):"
echo "  • redis-streamer:latest"
echo "  • redis-query-api:latest"
echo ""
print_status "Ready for deployment!"
