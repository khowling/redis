#!/bin/bash

# Clean script for Redis Streaming Platform

echo "🧹 Cleaning build artifacts..."

# Remove dist directory
if [ -d "dist" ]; then
    rm -rf dist/
    echo "✅ Removed dist/ directory"
else
    echo "ℹ️  No dist/ directory found"
fi

# Remove any binaries in component directories
find streamer/ api/ -name "redis-*" -type f -delete 2>/dev/null
echo "✅ Removed any stray binaries from component directories"

# Clean Go module caches in subdirectories
if [ -d "streamer" ]; then
    (cd streamer && go clean -cache -modcache 2>/dev/null || true)
fi

if [ -d "api" ]; then
    (cd api && go clean -cache -modcache 2>/dev/null || true)
fi
echo "✅ Cleaned Go module caches"

# Remove Docker images (optional)
read -p "Remove Docker images? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker rmi redis-streamer:latest redis-query-api:latest 2>/dev/null || true
    echo "✅ Removed Docker images"
fi

echo ""
echo "🎉 Cleanup complete!"
