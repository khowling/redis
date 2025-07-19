#!/bin/bash

# Azure Redis Cache Deployment Script
# This script deploys the Redis cache using the Bicep template

set -e

# Variables
RESOURCE_GROUP="redis"
TEMPLATE_FILE="redis.bicep"
private_endpoint_name="redispe"
identity_name_id="MC_redis_khredis_westeurope/providers/Microsoft.ManagedIdentity/userAssignedIdentities/khredis-agentpool"


# Get the current subscription ID
SUBSCRIPTION_ID=$(az account show --query id -o tsv)

printf "Using Subscription ID: $SUBSCRIPTION_ID"

# Deploy the template
printf "Deploying Redis cache..."
az deployment group create \
    --resource-group "$RESOURCE_GROUP" \
    --template-file "$TEMPLATE_FILE" \
    --parameters redisName="khredis" \
                 location="westeurope" \
                 privateEndpointId="/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.Network/privateEndpoints/${private_endpoint_name}" \
                 userAssignedIdentityId="/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${identity_name_id}" \
                 tags='{"Environment":"Development","Project":"Redis Cache","Owner":"DevTeam"}' \
                 minimumTlsVersion="1.2" \
                 enableNonSslPort=false \
                 redisVersion="latest" \

