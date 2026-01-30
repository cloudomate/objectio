#!/bin/bash
# Setup multipass VM for ObjectIO testing
# Creates a VM with 8x10GB sparse disk files

set -e

VM_NAME="objectio-node1"
VM_CPUS=4
VM_MEMORY=4G
VM_DISK=50G

echo "[INFO] Creating VM: ${VM_NAME}"
multipass launch 24.04 --name ${VM_NAME} \
    --cpus ${VM_CPUS} \
    --memory ${VM_MEMORY} \
    --disk ${VM_DISK}

echo "[INFO] Waiting for VM to be ready..."
multipass exec ${VM_NAME} -- cloud-init status --wait

echo "[INFO] Installing required packages..."
multipass exec ${VM_NAME} -- sudo apt-get update -qq
multipass exec ${VM_NAME} -- sudo apt-get install -y -qq curl build-essential

echo "[INFO] Creating 8x10GB sparse disk files..."
for i in $(seq 1 8); do
    echo "  Creating disk${i}.raw (10GB sparse)..."
    multipass exec ${VM_NAME} -- sudo truncate -s 10G /var/lib/objectio/disk${i}.raw
done

# Set permissions
multipass exec ${VM_NAME} -- sudo chown -R ubuntu:ubuntu /var/lib/objectio

echo "[INFO] Installing Rust toolchain..."
multipass exec ${VM_NAME} -- bash -c 'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y'

echo "[INFO] Verifying disk setup..."
multipass exec ${VM_NAME} -- ls -lh /var/lib/objectio/

echo "[INFO] VM setup complete!"
multipass info ${VM_NAME}
