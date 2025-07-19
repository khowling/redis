// Parameters
@description('The name of the Redis cache instance')
param redisName string

@description('The location where the Redis cache will be deployed')
param location string = resourceGroup().location

@description('The resource ID of the existing private endpoint')
param privateEndpointId string

@description('The resource ID of the existing user-assigned managed identity')
param userAssignedIdentityId string

@description('Tags to apply to the Redis cache resource')
param tags object = {}

@description('The minimum TLS version required for the Redis cache')
@allowed(['1.0', '1.1', '1.2'])
param minimumTlsVersion string = '1.2'

@description('Whether to enable non-SSL port (6379) - not recommended for production')
param enableNonSslPort bool = false

@description('Redis version to deploy')
param redisVersion string = 'latest'

// Variables
var skuConfig = {
  name: 'Premium'
  family: 'P'
  capacity: 1000  // Flash A 1000 SKU
}

// Redis Cache Resource
resource redisCache 'Microsoft.Cache/Redis@2024-11-01' = {
  name: redisName
  location: location
  tags: tags
  
  // Configure user-assigned managed identity
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${userAssignedIdentityId}': {}
    }
  }
  
  properties: {
    // SKU Configuration for Flash A 1000
    sku: skuConfig
    
    // Network and security configuration
    publicNetworkAccess: 'Disabled'  // Only accessible via private endpoint
    enableNonSslPort: enableNonSslPort
    minimumTlsVersion: minimumTlsVersion
    
    // Redis configuration
    redisVersion: redisVersion
    
    // Development configuration - no high availability
    // For Flash A SKUs, replication is not available as it's designed for development scenarios
    
    // Redis configuration settings optimized for development
    redisConfiguration: {
      'maxmemory-policy': 'allkeys-lru'  // Good for development caching scenarios
      'notify-keyspace-events': ''       // Disabled for better performance in dev
      'aad-enabled': 'true'              // Enable Azure AD authentication
    }
    
    // Disable access key authentication in favor of managed identity
    disableAccessKeyAuthentication: true
  }
}

// Private endpoint connection (referencing existing private endpoint)
// Note: The actual private endpoint connection will be established when the private endpoint
// is configured to connect to this Redis cache. This is typically done separately.

// Outputs
@description('The resource ID of the Redis cache')
output redisId string = redisCache.id

@description('The name of the Redis cache')
output redisName string = redisCache.name

@description('The hostname of the Redis cache')
output redisHostName string = redisCache.properties.hostName

@description('The SSL port of the Redis cache')
output redisSslPort int = redisCache.properties.sslPort

@description('The non-SSL port of the Redis cache (if enabled)')
output redisPort int = redisCache.properties.port

@description('The provisioning state of the Redis cache')
output provisioningState string = redisCache.properties.provisioningState

@description('Connection string for the Redis cache (using hostname and SSL port)')
output connectionString string = '${redisCache.properties.hostName}:${redisCache.properties.sslPort}'
