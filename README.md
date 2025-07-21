# Redis Streaming Platform

A high-performance Redis streaming platform for financial market data with realtime REST API access.

## 🏗️ Project Structure

```
redis/
├── streamer/          # Main streaming application (writes to Redis)
│   ├── main.go        # High-performance Redis stream writer
│   ├── config.go      # Redis configuration
│   ├── health.go      # Health monitoring
│   ├── go.mod         # Module: redis-streamer
│   ├── test_message.json
│   ├── uk_symbols.txt # 600 UK stock symbols
│   ├── k8s-*.yaml         # Kubernetes deployment files
│   └── Dockerfile         # Container image
├── api/               # REST API server (reads from Redis)
│   ├── main.go        # HTTP API server
│   ├── redis.go       # Redis client for API
│   └── go.mod         # Module: redis-query-api
│   ├── k8s-*.yaml         # Kubernetes deployment files
│   └── Dockerfile         # Container image
├── scripts/           # Monitoring and utility scripts
├── docker-compose.yml # Docker setup
└── README.md          # This file
```

## 🚀 Quick Start (localhost, requires redis installed)

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

### 4. Examine Redis keys

test
```bash
 redis-cli --scan --pattern "tick_*" | head -10

 redis-cli --scan --pattern "tick_*" | sort | while read stream; do echo -n "$stream: "; redis-cli XLEN "$stream"; done

 total=0; count=0; for stream in $(redis-cli --scan --pattern "tick_*"); do len=$(redis-cli XLEN "$stream" | tail -1); total=$((total + len)); count=$((count + 1)); done; echo "Total streams: $count"; echo "Total messages: $total"; echo "Average per stream: $((total / count))"
```


# Deploy to Azure Aks

## Prerequisites

- Go 1.21 or later
- Azure Redis Cache instance
- Azure AD authentication configured (Managed Identity or Service Principal)


### Configuration

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




### Build and push to Azure Container Registry:

We will be deploying on `Standard_F8as_v6`, the v6 cores are a great price/performance

This is the command to add the nodepool
```bash
az aks nodepool add   --cluster-name khredis   --name v6   --resource-group redis  --node-count 3   --node-vm-size Standard_F8as_v6   --enable-cluster-autoscaler   --min-count 1   --max-count 5   --node-taints workload=compute:NoSchedule   --labels workload=compute   --labels vm-type=f8as-v6
```


```bash
az acr build --registry kharc --image redis-streamer:0.1-amd64 --platform linux/amd64 ./streamer
az acr build --registry kharc --image redis-api:0.1-amd64 --platform linux/amd64 ./api
```

### Config map

Update
```
  REDIS_ENDPOINT: "khredis.westeurope.redis.azure.net:10000"  
  # Azure Managed Identity Configuration
  AZURE_CLIENT_ID: "xxx"  # User Managed Identity Object ID
  AZURE_TENANT_ID: "xxx"      # Azure Tenant ID
  REDIS_USERNAME: "xxx"   # Object ID for Redis ACL
```

```bash
kubectl apply -f ./streamer/k8s-deployment.yaml 
kubectl apply -f ./api/k8s-deployment.yaml 
```

Scale
```bash
kubectl scale deployment redis-stream-app --replicas=3
kubectl scale deployment redis-query-api --replicas=3
```

### Monitoring

```
k logs -l app=redis-stream-app -f
```

### Testing


```
curl -s 'http://9.163.170.3/api/messages?symbols=AAL,BP,WPP,TSCO,DGE,GSK&limit=100000'

curl -s 'http://9.163.170.3/api/messages?symbols=AAL,BP,WPP,TSCO,DGE,GSK&limit=1&pinStart=true'

curl -s 'http://9.163.170.3/api/messages?symbols=AAL,BP,WPP,TSCO,DGE,GSK&limit=1&pinStart=false'

ab -n 10000 -c 10  'http://9.163.170.3/api/messages?symbols=AAL,BP,WPP,TSCO,DGE,GSK&limit=10000' 
```



### Utilities

```bash
 az redisenterprise flush --name khredis --resource-group <your-resource-group>
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
