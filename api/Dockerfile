# Build stage
FROM golang:1.23-alpine AS builder

# Install required packages
RUN apk add --no-cache git ca-certificates

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the API application
RUN CGO_ENABLED=0 go build -o api-server .

# Final stage
FROM alpine:latest

# Install ca-certificates for TLS
RUN apk --no-cache add ca-certificates

# Create non-root user
RUN addgroup -g 1001 appgroup && \
    adduser -u 1001 -G appgroup -s /bin/sh -D appuser

# Set working directory
WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/api-server .

# Change ownership
RUN chown appuser:appgroup /app/api-server

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 8081

# Run the API server
CMD ["./api-server"]
