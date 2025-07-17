.PHONY: help build run test clean fmt lint docker-build docker-run deps

# Default target
help:
	@echo "Available targets:"
	@echo "  build        - Build the application"
	@echo "  run          - Run the application"
	@echo "  test         - Run tests"
	@echo "  clean        - Clean build artifacts"
	@echo "  fmt          - Format code"
	@echo "  lint         - Run linter"
	@echo "  docker-build - Build Docker image"
	@echo "  docker-run   - Run with Docker Compose"
	@echo "  deps         - Download dependencies"

# Build the application
build:
	go build -o bin/azure-redis-client .

# Run the application
run:
	go run main.go examples.go

# Run tests
test:
	go test -v ./...

# Clean build artifacts
clean:
	rm -rf bin/
	go clean

# Format code
fmt:
	go fmt ./...

# Run linter (requires golangci-lint)
lint:
	golangci-lint run

# Build Docker image
docker-build:
	docker build -t azure-redis-client .

# Run with Docker Compose
docker-run:
	docker-compose up --build

# Stop Docker Compose
docker-stop:
	docker-compose down

# Download dependencies
deps:
	go mod download
	go mod tidy

# Update dependencies
deps-update:
	go get -u ./...
	go mod tidy

# Security scan
security:
	gosec ./...

# Generate documentation
docs:
	godoc -http=:6060

# Run examples
examples:
	go run main.go examples.go

# Build for multiple platforms
build-all:
	GOOS=linux GOARCH=amd64 go build -o bin/azure-redis-client-linux-amd64 .
	GOOS=windows GOARCH=amd64 go build -o bin/azure-redis-client-windows-amd64.exe .
	GOOS=darwin GOARCH=amd64 go build -o bin/azure-redis-client-darwin-amd64 .
	GOOS=darwin GOARCH=arm64 go build -o bin/azure-redis-client-darwin-arm64 .
