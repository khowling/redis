# Kubernetes Deployment Guide

This directory contains Kubernetes manifests for deploying the Redis Stream application with Azure Managed Identity support.

## Files

- `k8s-deployment.yaml` - Main deployment with service account, workload identity configuration, and environment variables
- `redis_managed_identity_example.go` - Example code showing how to use managed identity with go-redis-entraid

## Key Changes for Managed Identity

When using Azure Managed Identity with Redis, you need to:

1. **Install go-redis-entraid package**: `go get github.com/redis/go-redis-entraid`
2. **Use REDIS_ENDPOINT**: Set to `hostname:port` format (e.g., `khredis.westeurope.redis.azure.net:6380`)
3. **Set AZURE_CLIENT_ID**: Your User Managed Identity Client ID
4. **Configure Redis ACL**: Add your managed identity Object ID as a Redis user
5. **Update connection code**: Use `StreamingCredentialsProvider` instead of password auth

The main difference is in your Go code - you'll use `entraid.NewManagedIdentityCredentialsProvider()` instead of password-based authentication.

## Prerequisites

1. **Azure Kubernetes Service (AKS)** with Workload Identity enabled
2. **User Managed Identity** created in Azure
3. **Azure Redis Cache** instance with Azure AD authentication enabled
4. **Container Image** built and pushed to a container registry
5. **go-redis-entraid** package installed in your Go application

## Setup Instructions

### 1. Enable Workload Identity on AKS

If not already enabled:
```bash
az aks update \
    --resource-group <resource-group> \
    --name <cluster-name> \
    --enable-workload-identity
```

### 2. Create User Managed Identity

```bash
az identity create \
    --resource-group <resource-group> \
    --name redis-stream-app-identity
```

### 3. Enable Azure AD Authentication on Redis Cache

```bash
# Enable Azure AD authentication on your Redis cache
az redis update \
    --resource-group <resource-group> \
    --name <redis-name> \
    --enable-authentication yes

# Add the managed identity as a Redis user
az redis access-policy-assignment create \
    --resource-group <resource-group> \
    --redis-name <redis-name> \
    --access-policy-name "Data Contributor" \
    --object-id 7303e014-4009-403f-b170-83ef57374e21 \
    --object-id-alias "redis-stream-app-identity"
```

### 3. Grant Redis Access to Managed Identity

See step 3 above for enabling Azure AD authentication and adding the managed identity as a Redis user.

### 4. Install go-redis-entraid Package

Update your Go application to use the `go-redis-entraid` package:

```bash
# In your Go project directory
go get github.com/redis/go-redis-entraid
```

Update your `go.mod` file to include the required dependency, and modify your Redis connection code according to the example in `redis_managed_identity_example.go`.

### 5. Configure Environment Variables

Edit the `k8s-deployment.yaml` file and replace the following placeholders with your actual values:

**Required Configuration:**
- `REDIS_HOST`: Your Azure Redis Cache endpoint (e.g., `mycache.redis.cache.windows.net`)
- `AZURE_CLIENT_ID`: Your User Managed Identity Client ID
- `AZURE_TENANT_ID`: Your Azure Tenant ID

**Example:**
```yaml
env:
- name: REDIS_HOST
  value: "mycache.redis.cache.windows.net"
- name: AZURE_CLIENT_ID
  value: "12345678-1234-1234-1234-123456789abc"
- name: AZURE_TENANT_ID
  value: "87654321-4321-4321-4321-cba987654321"
```

**Values to Replace in k8s-deployment.yaml:**
1. ✅ `REDIS_ENDPOINT` is already set to `khredis.westeurope.redis.azure.net:6380`
2. ✅ `AZURE_CLIENT_ID` is already set to `e5833ec1-dc2b-471c-a883-a9e8544b2efa`
3. ❌ Replace `your-azure-tenant-id` with your actual Azure Tenant ID
4. ✅ `REDIS_USERNAME` is already set to the managed identity Object ID

### 6. Build and Push Container Image

```bash
# Build the image
docker build -t <your-registry>/redis-stream-app:latest .

# Push to registry
docker push <your-registry>/redis-stream-app:latest

# Update the image reference in the deployment files
```

### 7. Deploy to Kubernetes

```bash
kubectl apply -f k8s-deployment.yaml
```

### 8. Federated Identity Credential (if using Workload Identity)

Create the federated identity credential:
```bash
az identity federated-credential create \
    --name redis-stream-app-federated-id \
    --identity-name redis-stream-app-identity \
    --resource-group <resource-group> \
    --issuer https://<aks-cluster-oidc-issuer-url> \
    --subject system:serviceaccount:default:redis-stream-app-sa \
    --audience api://AzureADTokenExchange
```

## Environment Variables Reference

### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `REDIS_HOST` | Azure Redis Cache endpoint | `mycache.redis.cache.windows.net` |
| `AZURE_CLIENT_ID` | User Managed Identity Client ID | `12345678-1234-1234-1234-123456789abc` |
| `AZURE_TENANT_ID` | Azure Tenant ID | `87654321-4321-4321-4321-cba987654321` |

### Optional Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `REDIS_PORT` | Redis port | `6380` |
| `REDIS_USERNAME` | Redis username (if not using AAD) | `""` |
| `REDIS_USE_AAD` | Use Azure AD authentication | `true` |
| `ENVIRONMENT` | Application environment | `production` |
| `REDIS_MAX_RETRIES` | Connection retry count | `3` |
| `REDIS_POOL_SIZE` | Connection pool size | `10` |

## Monitoring and Troubleshooting

### Check Pod Status
```bash
kubectl get pods -l app=redis-stream-app
kubectl describe pod <pod-name>
```

### Check Logs
```bash
kubectl logs -l app=redis-stream-app -f
```

### Test Health Endpoint
```bash
kubectl port-forward service/redis-stream-app-service 8080:80
curl http://localhost:8080/health
```

### Verify Workload Identity
```bash
# Exec into the pod and check identity
kubectl exec -it <pod-name> -- printenv | grep AZURE
```

## Security Considerations

1. **Environment Variables**: While using direct environment variables is simpler, be aware that they are visible in pod specifications
2. **Enable RBAC** and restrict service account permissions
3. **Use least privilege** for managed identity role assignments
4. **Enable Pod Security Standards** in your namespace
5. **Network isolation** - consider using network policies to restrict traffic

## Production Recommendations

1. **Resource Limits**: Adjust CPU and memory limits based on your workload
2. **Replicas**: Scale the deployment based on your traffic patterns
3. **Monitoring**: Set up monitoring and alerting for the application
4. **Backup**: Ensure Redis data is properly backed up
5. **Network Policies**: Implement network policies to restrict traffic
