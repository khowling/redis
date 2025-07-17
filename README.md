# Azure Managed Redis Go Client for Stream Operations

A robust Go client for Azure Managed Redis that provides secure stream operations with Azure AD authentication, retry logic, and comprehensive error handling.

## Features

- **Azure AD Authentication**## Build

Build and push to Azure Container Registry (ACR uses native ARM64 builders):

```bash
# Native ARM64 build - no cross-compilation needed
az acr build --registry kharc --image redis-client:0.3-arm64-native --platform linux/arm64 .

# Alternative with version tag
az acr build --registry kharc --image redis-client:latest --platform linux/arm64 .
```s managed identity for secure authentication
- **Stream Operations**: Add single messages, batch messages, and retrieve stream information
- **Retry Logic**: Exponential backoff retry mechanism for transient failures
- **Connection Pooling**: Optimized connection management for better performance
- **Comprehensive Logging**: Structured logging with configurable levels
- **Error Handling**: Robust error handling with context-aware timeouts
- **TLS Support**: Secure connections with TLS encryption
- **Production Ready**: Includes monitoring, graceful shutdown, and production patterns

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
