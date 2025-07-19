# Azure Redis Cache - Flash A 1000 SKU

This repository contains Bicep templates and deployment scripts for creating an Azure Cache for Redis with Flash A 1000 SKU, configured for development environments with private endpoint connectivity and user-assigned managed identity.

## Overview

The Redis cache is configured with the following specifications:
- **SKU**: Premium P1000 (Flash A 1000)
- **Purpose**: Development environment (no high availability)
- **Network Access**: Private endpoint only (public access disabled)
- **Authentication**: User-assigned managed identity with access key authentication disabled
- **Security**: TLS 1.2 minimum, SSL-only connections

## Files

- `redis.bicep` - Main Bicep template for Redis cache
- `redis.parameters.json` - Parameters file with default values
- `deploy.sh` - Deployment script
- `README.md` - This documentation

## Prerequisites

1. **Azure CLI** installed and logged in
2. **Existing Private Endpoint** - You must have a private endpoint already created
3. **Existing User-Assigned Managed Identity** - You must have a managed identity created
4. **Resource Group** - Target resource group must exist
5. **Permissions** - Contributor access to the resource group

## Configuration

### Required Parameters

Before deployment, update the `redis.parameters.json` file with your actual values:

```json
{
  "privateEndpointId": {
    "value": "/subscriptions/{your-subscription-id}/resourceGroups/{your-rg}/providers/Microsoft.Network/privateEndpoints/{your-pe-name}"
  },
  "userAssignedIdentityId": {
    "value": "/subscriptions/{your-subscription-id}/resourceGroups/{your-rg}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/{your-identity-name}"
  }
}
```

### Optional Parameters

- `redisName` - Name of the Redis cache instance
- `location` - Azure region for deployment
- `tags` - Resource tags
- `minimumTlsVersion` - Minimum TLS version (1.0, 1.1, 1.2)
- `enableNonSslPort` - Enable non-SSL port 6379 (not recommended)
- `redisVersion` - Redis version ("latest" recommended)

## Deployment

### Option 1: Using the Deployment Script

1. Update the script variables:
   ```bash
   # Edit deploy.sh
   RESOURCE_GROUP_NAME="your-resource-group"
   SUBSCRIPTION_ID="your-subscription-id"
   ```

2. Run the deployment:
   ```bash
   ./deploy.sh
   ```

### Option 2: Using Azure CLI Directly

1. Validate the template:
   ```bash
   az deployment group validate \
     --resource-group your-resource-group \
     --template-file redis.bicep \
     --parameters @redis.parameters.json
   ```

2. Deploy the template:
   ```bash
   az deployment group create \
     --resource-group your-resource-group \
     --name redis-deployment \
     --template-file redis.bicep \
     --parameters @redis.parameters.json
   ```

## Flash A 1000 SKU Details

The Flash A 1000 SKU provides:
- **Memory**: 1000 GB
- **Performance**: Optimized for development scenarios
- **Storage**: Uses NVMe flash storage for cost efficiency
- **Replication**: Not available (single instance for development)
- **Clustering**: Not supported
- **Backup**: Available but simplified for development use

## Security Features

### Network Security
- **Public Access**: Disabled
- **Private Endpoint**: Required for all access
- **SSL/TLS**: Enforced with minimum TLS 1.2
- **Non-SSL Port**: Disabled by default

### Authentication
- **Access Keys**: Disabled
- **Azure AD**: Enabled
- **Managed Identity**: User-assigned identity for secure access
- **Auth Required**: Authentication is mandatory

### Redis Configuration
- **Memory Policy**: `allkeys-lru` (optimized for caching)
- **Keyspace Events**: Disabled (better performance)
- **Azure AD**: Enabled for authentication

## Outputs

The template provides the following outputs:
- `redisId` - Resource ID of the Redis cache
- `redisName` - Name of the Redis cache
- `redisHostName` - Hostname for connections
- `redisSslPort` - SSL port (6380)
- `redisPort` - Non-SSL port (6379, if enabled)
- `provisioningState` - Current provisioning status
- `connectionString` - Connection string format

## Private Endpoint Configuration

After deployment, you need to configure your existing private endpoint to connect to the Redis cache:

1. Navigate to your private endpoint in the Azure portal
2. Add a new private endpoint connection
3. Select the Redis cache as the target resource
4. Choose "redisCache" as the target sub-resource
5. Configure DNS settings if using private DNS zones

## Connection Examples

### Using .NET
```csharp
var connectionString = "your-redis-hostname:6380";
var redis = ConnectionMultiplexer.Connect(new ConfigurationOptions
{
    EndPoints = { connectionString },
    Ssl = true,
    SslProtocols = SslProtocols.Tls12,
    // Use Azure AD authentication with managed identity
    AuthenticationDatabase = 0
});
```

### Using Python
```python
import redis

r = redis.Redis(
    host='your-redis-hostname',
    port=6380,
    ssl=True,
    ssl_cert_reqs=None,
    # Configure Azure AD authentication
)
```

## Monitoring and Troubleshooting

### Key Metrics to Monitor
- Memory usage
- CPU utilization
- Connected clients
- Cache hits/misses
- Network I/O

### Common Issues
1. **Connection Timeouts**: Verify private endpoint configuration
2. **Authentication Errors**: Check managed identity permissions
3. **SSL Errors**: Ensure TLS 1.2+ is used in client applications

## Cost Optimization

For development environments:
- Flash A SKU provides cost-effective storage
- Single instance (no replication) reduces costs
- Monitor usage and scale down if needed
- Consider using scheduled scaling for dev/test workloads

## Compliance and Security

This configuration follows Azure security best practices:
- ✅ Private networking only
- ✅ Managed identity authentication
- ✅ TLS encryption in transit
- ✅ Access key authentication disabled
- ✅ Minimum TLS 1.2 enforced

## Support

For issues or questions:
1. Check Azure Redis Cache documentation
2. Review deployment logs in Azure portal
3. Use Azure diagnostics and monitoring tools
4. Contact your Azure support team if needed

## License

This project is licensed under the MIT License.
