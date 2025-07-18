# 📁 Project Structure

```
redis/                          # Root directory
├── .git/                       # Git repository
├── .gitignore                  # Git ignore rules (includes dist/)
├── README.md                   # Main project documentation
├── DEPLOYMENT.md               # Comprehensive deployment guide
├── PROJECT_STRUCTURE.md        # This file
├── build.sh                    # Build script for both components
├── clean.sh                    # Clean build artifacts
├── docker-compose.yml          # Local development stack
├── Makefile                    # Build automation
├── dist/                       # Build output directory (gitignored)
│   ├── redis-streamer          # Streamer binary
│   └── redis-query-api         # API binary
├── scripts/                    # Utility scripts
│   ├── health-check.sh
│   ├── monitor.sh
│   ├── monitor_streams.sh
│   └── stats.sh
├── streamer/                   # Redis Stream Producer
│   ├── main.go                 # High-performance streaming application
│   ├── config.go               # Redis configuration
│   ├── health.go               # Health monitoring
│   ├── test_message.json       # Message template
│   ├── uk_symbols.txt          # 87 UK stock symbols
│   ├── Dockerfile              # Container image for streamer
│   ├── k8s-deployment.yaml     # Kubernetes deployment manifest
│   ├── k8s-secrets.yaml        # Kubernetes secrets
│   ├── go.mod                  # Go module (redis-streamer)
│   └── go.sum                  # Go dependencies
└── api/                        # Redis Query API
    ├── main.go                 # HTTP API server
    ├── redis.go                # Redis client for API
    ├── Dockerfile              # Container image for API
    ├── k8s-deployment.yaml     # Kubernetes deployment manifest
    ├── k8s-secrets.yaml        # Kubernetes secrets
    ├── go.mod                  # Go module (redis-query-api)
    └── go.sum                  # Go dependencies
```

## 🧹 Cleaned Up Files

**Removed redundant files:**
- ❌ `Dockerfile` (root) → Moved to `streamer/Dockerfile`
- ❌ `Dockerfile.api` (root) → Replaced with `api/Dockerfile`
- ❌ `go.mod` & `go.sum` (root) → Separate modules in subdirectories
- ❌ `api-server.go` (root) → Integrated into `api/main.go`
- ❌ `azure-redis-client` & `redis-client` → Compiled binaries removed
- ❌ `API_SUCCESS.md` → Temporary documentation removed
- ❌ `KUBERNETES_DEPLOYMENT.md` → Content merged into `DEPLOYMENT.md`
- ❌ `MANAGED_IDENTITY_INTEGRATION.md` → Content merged into `DEPLOYMENT.md`

## ✅ Clean Structure Benefits

1. **No Redundancy**: Each file has a clear purpose and location
2. **Modular Design**: Separate Go modules for streamer and API
3. **Independent Deployment**: Each component has its own deployment artifacts
4. **Clear Separation**: Producer and consumer logic is isolated
5. **Easy Navigation**: Logical file organization
6. **Build Efficiency**: No conflicting dependencies or binaries

## 🚀 Quick Commands

```bash
# Build everything
./build.sh

# Clean build artifacts
./clean.sh

# Local development with binaries
./build.sh
docker-compose up redis-local &
REDIS_HOST=localhost ./dist/redis-streamer &
REDIS_HOST=localhost ./dist/redis-query-api &

# Local development with Docker
docker-compose up --build

# Build individual components
cd streamer && go build -o ../dist/redis-streamer
cd api && go build -o ../dist/redis-query-api

# Deploy to Kubernetes
kubectl apply -f streamer/k8s-secrets.yaml
kubectl apply -f streamer/k8s-deployment.yaml
kubectl apply -f api/k8s-secrets.yaml
kubectl apply -f api/k8s-deployment.yaml
```

**Project is now clean, organized, and ready for production! 🎉**
