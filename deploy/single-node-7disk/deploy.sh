#!/bin/bash
# Deploy ObjectIO to remote VM
# Usage: ./deploy.sh [user@host] [ssh_key]

set -e

# Configuration
REMOTE_HOST="${1:-ubuntu@192.168.4.225}"
SSH_KEY="${2:-~/.ssh/onekey}"
DEPLOY_DIR="/opt/objectio"

SSH_CMD="ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no"
SCP_CMD="scp -i ${SSH_KEY} -o StrictHostKeyChecking=no"

echo "=== ObjectIO Deployment ==="
echo "Target: ${REMOTE_HOST}"
echo "Deploy dir: ${DEPLOY_DIR}"
echo ""

# Step 1: Setup disks on remote
echo "=== Step 1: Setting up disks ==="
${SSH_CMD} ${REMOTE_HOST} 'bash -s' < setup-disks.sh

# Step 2: Create deployment directory
echo ""
echo "=== Step 2: Creating deployment directory ==="
${SSH_CMD} ${REMOTE_HOST} "sudo mkdir -p ${DEPLOY_DIR} && sudo chown \$(whoami):\$(whoami) ${DEPLOY_DIR}"

# Step 3: Copy deployment files
echo ""
echo "=== Step 3: Copying deployment files ==="
${SCP_CMD} -r docker-compose.yml config ${REMOTE_HOST}:${DEPLOY_DIR}/

# Step 4: Copy source code for building (or use pre-built images)
echo ""
echo "=== Step 4: Copying source code for Docker build ==="
cd ../..
tar --exclude='target' --exclude='.git' --exclude='deploy' -czf /tmp/objectio-src.tar.gz .
${SCP_CMD} /tmp/objectio-src.tar.gz ${REMOTE_HOST}:/tmp/
${SSH_CMD} ${REMOTE_HOST} "mkdir -p ${DEPLOY_DIR}/src && cd ${DEPLOY_DIR}/src && tar -xzf /tmp/objectio-src.tar.gz"

# Update docker-compose to use local source
${SSH_CMD} ${REMOTE_HOST} "cd ${DEPLOY_DIR} && sed -i 's|context: \.\./\.\.|context: ./src|g' docker-compose.yml"

# Step 5: Build and start services
echo ""
echo "=== Step 5: Building Docker images ==="
${SSH_CMD} ${REMOTE_HOST} "cd ${DEPLOY_DIR} && docker compose build"

echo ""
echo "=== Step 6: Starting services ==="
${SSH_CMD} ${REMOTE_HOST} "cd ${DEPLOY_DIR} && docker compose up -d"

# Step 7: Wait for services to be healthy
echo ""
echo "=== Step 7: Waiting for services ==="
sleep 5
${SSH_CMD} ${REMOTE_HOST} "cd ${DEPLOY_DIR} && docker compose ps"

# Step 8: Show logs
echo ""
echo "=== Step 8: Service logs ==="
${SSH_CMD} ${REMOTE_HOST} "cd ${DEPLOY_DIR} && docker compose logs --tail=20"

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "S3 Endpoint: http://${REMOTE_HOST%%@*}:9000"
echo ""
echo "Test with:"
echo "  export AWS_ACCESS_KEY_ID=admin"
echo "  export AWS_SECRET_ACCESS_KEY=<see gateway logs>"
echo "  aws --endpoint-url http://192.168.4.225:9000 s3 mb s3://test"
echo "  aws --endpoint-url http://192.168.4.225:9000 s3 cp /etc/hosts s3://test/"
echo "  aws --endpoint-url http://192.168.4.225:9000 s3 ls s3://test/"
