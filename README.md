# Redis Streaming Platform

A high-performance Redis streaming platform for UK stock market data with REST API access.

## 🏗️ Project Structure

```
redis/
├── streamer/          # Main streaming application (writes to Redis)
│   ├── main.go        # High-performance Redis stream writer
│   ├── config.go      # Redis configuration
│   ├── health.go      # Health monitoring
│   ├── go.mod         # Module: redis-streamer
│   ├── test_message.json
│   └── uk_symbols.txt # 87 UK stock symbols
├── api/               # REST API server (reads from Redis)
│   ├── main.go        # HTTP API server
│   ├── redis.go       # Redis client for API
│   └── go.mod         # Module: redis-query-api
├── scripts/           # Monitoring and utility scripts
├── k8s-*.yaml         # Kubernetes deployment files
├── docker-compose.yml # Docker setup
├── Dockerfile         # Container image
└── README.md          # This file
```

## 🚀 Quick Start

### 1. Start Redis Streaming (Producer)
```bash
cd streamer
REDIS_HOST=localhost go run .
```

### 2. Start API Server (Consumer)
```bash
cd api
REDIS_HOST=localhost go run .
```

### 3. Query the API
```bash
# Get stream list
curl http://localhost:8081/api/streams

# Query messages
curl "http://localhost:8081/api/messages?symbols=AAL,BP&limit=10"

# Health check
curl http://localhost:8081/health
```

## Build

Locally

```bash
go build -o redis-client .

```

test
```bash
 redis-cli --scan --pattern "tick_*" | head -10

 redis-cli --scan --pattern "tick_*" | sort | while read stream; do echo -n "$stream: "; redis-cli XLEN "$stream"; done

 total=0; count=0; for stream in $(redis-cli --scan --pattern "tick_*"); do len=$(redis-cli XLEN "$stream" | tail -1); total=$((total + len)); count=$((count + 1)); done; echo "Total streams: $count"; echo "Total messages: $total"; echo "Average per stream: $((total / count))"
```


### Deploy to Azure Aks

Build and push to Azure Container Registry (ACR uses native ARM64 builders):

We will be deploying on `Standard_F8as_v6`, the v6 cores are a great price/performance

this is the command to add the nodepool
```bash
az aks nodepool add   --cluster-name khredis   --name v6   --resource-group redis  --node-count 3   --node-vm-size Standard_F8as_v6   --enable-cluster-autoscaler   --min-count 1   --max-count 5   --node-taints workload=compute:NoSchedule   --labels workload=compute   --labels vm-type=f8as-v6
```


```bash
az acr build --registry kharc --image redis-streamer:0.1-amd64 --platform linux/amd64 .

```



## Prerequisites

- Go 1.21 or later
- Azure Redis Cache instance
- Azure AD authentication configured (Managed Identity or Service Principal)

## Installation

```bash
go mod init your-project-name
go get github.com/Azure/azure-sdk-for-go/sdk/azcore
go get github.com/Azure/azure-sdk-for-go/sdk/azidentity
go get github.com/go-redis/redis/v8
go get github.com/sirupsen/logrus
```

## Configuration

### Environment Variables

Set the following environment variables:

```bash
export REDIS_HOST="your-redis-instance.redis.cache.windows.net"
export REDIS_USERNAME="your-username"
```

### Azure AD Authentication

This client uses Azure AD authentication by default. Ensure your application has the appropriate permissions:

1. **For Managed Identity**: Assign the "Redis Cache Contributor" role to your Azure resource
2. **For Service Principal**: Create a service principal and assign the appropriate Redis permissions
3. **For Development**: Use Azure CLI authentication: `az login`

## Usage

### Basic Usage

```go
package main

import (
    "context"
    "log"
    "time"
)

func main() {
    // Create configuration
    config := NewRedisConfig()
    config.Host = "your-redis-instance.redis.cache.windows.net"
    config.Username = "your-username"

    // Create client
    client, err := NewAzureRedisClient(config)
    if err != nil {
        log.Fatal("Failed to create Redis client:", err)
    }
    defer client.Close()

    ctx := context.Background()

    // Add a message to a stream
    message := StreamMessage{
        Fields: map[string]interface{}{
            "user_id":    "12345",
            "action":     "login",
            "timestamp":  time.Now().Unix(),
            "ip_address": "192.168.1.1",
        },
    }

    messageID, err := client.AddToStream(ctx, "user_events", message, &StreamAddOptions{
        MaxLen:      1000,
        Approximate: true,
    })
    if err != nil {
        log.Printf("Error: %v", err)
        return
    }
    
    log.Printf("Message added with ID: %s", messageID)
}
```

### Batch Operations

```go
// Add multiple messages efficiently
messages := []StreamMessage{
    {
        Fields: map[string]interface{}{
            "order_id": "order_001",
            "status":   "pending",
            "amount":   99.99,
        },
    },
    {
        Fields: map[string]interface{}{
            "order_id": "order_002",
            "status":   "confirmed",
            "amount":   149.99,
        },
    },
}

messageIDs, err := client.AddBatchToStream(ctx, "orders", messages, &StreamAddOptions{
    MaxLen:      5000,
    Approximate: true,
})
```

### Stream Information

```go
// Get stream information
info, err := client.GetStreamInfo(ctx, "user_events")
if err != nil {
    log.Printf("Error: %v", err)
    return
}

fmt.Printf("Stream Length: %d\n", info.Length)
fmt.Printf("First Entry: %s\n", info.FirstEntry.ID)
fmt.Printf("Last Entry: %s\n", info.LastEntry.ID)
```

## Configuration Options

The `RedisConfig` struct supports the following options:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| Host | string | - | Redis host (required) |
| Port | int | 6380 | Redis port |
| Username | string | - | Redis username |
| UseAAD | bool | true | Use Azure AD authentication |
| MaxRetries | int | 3 | Maximum retry attempts |
| DialTimeout | time.Duration | 5s | Connection timeout |
| ReadTimeout | time.Duration | 3s | Read operation timeout |
| WriteTimeout | time.Duration | 3s | Write operation timeout |
| PoolSize | int | 10 | Connection pool size |
| MinIdleConns | int | 5 | Minimum idle connections |
| MaxConnAge | time.Duration | 30m | Maximum connection age |
| PoolTimeout | time.Duration | 4s | Pool timeout |
| IdleTimeout | time.Duration | 5m | Idle connection timeout |

## Stream Operations

### Adding Messages

The client supports three ways to add messages:

1. **Single Message**: `AddToStream(ctx, streamName, message, options)`
2. **Batch Messages**: `AddBatchToStream(ctx, streamName, messages, options)`
3. **Custom ID**: Set the `ID` field in `StreamMessage`

### Stream Options

Use `StreamAddOptions` to control stream behavior:

- `MaxLen`: Maximum number of messages to retain
- `Approximate`: Use approximate trimming for better performance

## Error Handling

The client implements comprehensive error handling:

- **Retry Logic**: Exponential backoff for transient failures
- **Context Support**: Respects context cancellation and timeouts
- **Detailed Logging**: Structured logging with error context
- **Connection Recovery**: Automatic reconnection on connection failures

## Production Considerations

### Security

- Uses Azure AD authentication by default
- Supports managed identity for secure credential management
- Enables TLS encryption for all connections
- Never hardcodes credentials in source code

### Performance

- Connection pooling for optimal resource usage
- Batch operations for high-throughput scenarios
- Configurable timeouts and retry policies
- Efficient memory usage with streaming operations

### Monitoring

- Structured logging with configurable levels
- Metrics collection for stream operations
- Error tracking and alerting capabilities
- Health check endpoints support

### Best Practices

1. **Use Managed Identity**: Preferred authentication method in Azure
2. **Configure Timeouts**: Set appropriate timeouts for your use case
3. **Monitor Stream Size**: Use `MaxLen` to prevent unbounded growth
4. **Batch Operations**: Use batch operations for high-volume scenarios
5. **Handle Errors**: Implement proper error handling and alerting
6. **Use Contexts**: Always use context for cancellation and timeouts

## Examples

Run the examples to see the client in action:

```bash
go run main.go examples.go
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License.

## Support

For issues and questions:
- Create an issue in the repository
- Check Azure Redis documentation
- Review Azure SDK for Go documentation

## References

- [Azure Redis Cache Documentation](https://docs.microsoft.com/en-us/azure/azure-cache-for-redis/)
- [Azure SDK for Go](https://github.com/Azure/azure-sdk-for-go)
- [Redis Streams Documentation](https://redis.io/docs/data-types/streams/)
- [Go Redis Client](https://github.com/go-redis/redis)



## Build

```
az acr build --registry kharc --image redis-client:0.3-arm64-native --platform linux/arm64 . 
```
