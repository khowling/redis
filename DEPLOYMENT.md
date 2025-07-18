# 🚀 Deployment Guide

This guide covers deployment options for the Redis Streaming Platform with separate streamer and API components.

## 📁 Project Structure
```
redis/
├── streamer/              # Redis Stream Producer
│   ├── main.go           # Streaming application
│   ├── Dockerfile        # Streamer container
│   ├── k8s-deployment.yaml
│   └── k8s-secrets.yaml
├── api/                  # Redis Query API
│   ├── main.go           # API server
│   ├── Dockerfile        # API container
│   ├── k8s-deployment.yaml
│   └── k8s-secrets.yaml
├── docker-compose.yml    # Local development
└── build.sh             # Build script
```

## 🏠 Local Development

### Option 1: Native Go (Recommended for Development)
```bash
# Terminal 1: Start Redis
docker-compose up redis-local

# Terminal 2: Start Streamer
cd streamer
REDIS_HOST=localhost go run .

# Terminal 3: Start API
cd api
REDIS_HOST=localhost go run .

# Terminal 4: Test API
curl http://localhost:8081/health
curl "http://localhost:8081/api/messages?symbols=AAL,BP&limit=5"
```

### Option 2: Docker Compose (Full Stack)
```bash
# Start everything
docker-compose up --build

# Services available:
# - Redis: localhost:6379
# - API: http://localhost:8081
# - Redis Commander: http://localhost:8082
```

### Option 3: Mixed (Streamer in Docker, API Native)
```bash
# Start Redis and Streamer
docker-compose up redis-local redis-streamer

# Start API natively for development
cd api && REDIS_HOST=localhost go run .
```

## ☁️ Azure Kubernetes Service (AKS)

### Prerequisites
```bash
# Ensure you're connected to the right cluster
az aks get-credentials --resource-group your-rg --name khredis

# Verify node pool exists
kubectl get nodes -l kubernetes.azure.com/agentpool=khrediscompute
```

### Deploy Streamer
```bash
cd streamer

# Create secrets (update values first)
kubectl apply -f k8s-secrets.yaml

# Deploy streamer
kubectl apply -f k8s-deployment.yaml

# Check status
kubectl get pods -l app=azure-redis-client
kubectl logs -l app=azure-redis-client -f
```

### Deploy API
```bash
cd ../api

# Create API secrets (update if different from streamer)
kubectl apply -f k8s-secrets.yaml

# Deploy API
kubectl apply -f k8s-deployment.yaml

# Check status
kubectl get pods -l app=redis-query-api
kubectl get svc redis-query-api-service

# Get external IP
kubectl get svc redis-query-api-service -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
```

### Access API
```bash
# Get external IP
API_IP=$(kubectl get svc redis-query-api-service -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

# Test endpoints
curl http://$API_IP/health
curl "http://$API_IP/api/streams"
curl "http://$API_IP/api/messages?symbols=AAL,BP&limit=10"
```

## 🐳 Docker Build & Push

### Build Images
```bash
# Build streamer
cd streamer
docker build -t your-registry/redis-streamer:latest .

# Build API
cd ../api
docker build -t your-registry/redis-query-api:latest .
```

### Push to Registry
```bash
# Azure Container Registry
az acr login --name your-acr
docker tag redis-streamer:latest your-acr.azurecr.io/redis-streamer:latest
docker tag redis-query-api:latest your-acr.azurecr.io/redis-query-api:latest
docker push your-acr.azurecr.io/redis-streamer:latest
docker push your-acr.azurecr.io/redis-query-api:latest
```

### Update K8s Manifests
```bash
# Update image references in k8s-deployment.yaml files
sed -i 's|redis-streamer:latest|your-acr.azurecr.io/redis-streamer:latest|' streamer/k8s-deployment.yaml
sed -i 's|redis-query-api:latest|your-acr.azurecr.io/redis-query-api:latest|' api/k8s-deployment.yaml
```

## 🔧 Configuration

### Environment Variables

#### Common (Both Components)
```bash
REDIS_HOST=your-redis-host
REDIS_USERNAME=your-username
AZURE_CLIENT_ID=your-managed-identity-client-id
REDIS_POOL_SIZE=50
REDIS_MAX_RETRIES=3
REDIS_DIAL_TIMEOUT=5s
```

#### Streamer Specific
```bash
REDIS_ENDPOINT=host:port  # Alternative to REDIS_HOST
BATCH_SIZE=5000          # Messages per batch
FLUSH_INTERVAL=25ms      # Batch flush interval
WORKER_COUNT=32          # Number of workers
```

#### API Specific
```bash
API_PORT=8081           # API server port
CORS_ORIGINS=*          # CORS allowed origins
READ_TIMEOUT=10s        # Redis read timeout
WRITE_TIMEOUT=10s       # Redis write timeout
```

## 🔍 Monitoring & Troubleshooting

### Check Streamer Health
```bash
# Kubernetes
kubectl logs -l app=azure-redis-client -f
kubectl exec -it deployment/azure-redis-client -- ps aux

# Docker
docker-compose logs redis-streamer -f
```

### Check API Health
```bash
# Direct health check
curl http://your-api-endpoint/health

# Kubernetes
kubectl logs -l app=redis-query-api -f
kubectl port-forward svc/redis-query-api-service 8081:80

# Docker
docker-compose logs redis-query-api -f
```

### Redis Monitoring
```bash
# Redis CLI commands
redis-cli XINFO STREAM tick_AAL
redis-cli KEYS "tick_*"
redis-cli INFO memory
redis-cli INFO stats

# Via Redis Commander
open http://localhost:8082
```

## 🚨 Common Issues

### Streamer Not Writing
```bash
# Check Redis connection
kubectl logs -l app=azure-redis-client | grep "Successfully connected"

# Check managed identity
kubectl describe pod -l app=azure-redis-client
kubectl logs -l app=azure-redis-client | grep "managed identity"
```

### API Not Responding
```bash
# Check service
kubectl get svc redis-query-api-service
kubectl describe svc redis-query-api-service

# Check pods
kubectl get pods -l app=redis-query-api
kubectl describe pod -l app=redis-query-api
```

### Performance Issues
```bash
# Check resource usage
kubectl top pods
kubectl describe nodes

# Monitor Redis
redis-cli --latency-history
redis-cli INFO stats | grep instantaneous
```

## 📊 Performance Tuning

### Streamer Optimization
- Increase `REDIS_POOL_SIZE` for higher throughput
- Adjust `BATCH_SIZE` (5000 recommended)
- Tune `WORKER_COUNT` based on CPU cores
- Monitor memory usage and adjust `GOMEMLIMIT`

### API Optimization
- Set appropriate `REDIS_POOL_SIZE` for read workload
- Configure connection timeouts for responsiveness
- Use load balancer for multiple API replicas
- Enable request/response caching if needed

## 🎯 Production Checklist

- [ ] Update all secrets with production values
- [ ] Configure proper resource limits in K8s manifests
- [ ] Set up monitoring and alerting
- [ ] Configure ingress/load balancer for API
- [ ] Set up log aggregation
- [ ] Configure backup strategy for Redis
- [ ] Test disaster recovery procedures
- [ ] Set up auto-scaling for API pods
- [ ] Configure network policies
- [ ] Set up TLS certificates for external access
