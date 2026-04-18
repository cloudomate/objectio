#!/bin/bash
#
# ObjectIO Test VM Setup Script
# Creates a minimal Ubuntu 24.04 VM using Multipass with 8 virtual disks
#
# Usage:
#   ./setup-test-vm.sh [create|start|stop|delete|status|shell|setup-disks|disk-status|cleanup-disks]
#
set -euo pipefail

VM_NAME="objectio-test"
VM_CPUS=2
VM_MEMORY="4G"
VM_DISK="20G"
NUM_DISKS=8
DISK_SIZE_GB=10
DISK_DIR="/var/lib/objectio/disks"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_multipass() {
    if ! command -v multipass &> /dev/null; then
        log_error "Multipass is not installed. Please install it first:"
        echo "  brew install multipass"
        exit 1
    fi
}

create_vm() {
    check_multipass

    log_info "Creating VM: $VM_NAME"
    log_info "Specs: ${VM_CPUS} CPUs, ${VM_MEMORY} RAM, ${VM_DISK} disk"

    # Check if VM already exists
    if multipass list 2>/dev/null | grep -q "^${VM_NAME}"; then
        log_warn "VM $VM_NAME already exists."
        read -p "Delete and recreate? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            log_info "Deleting existing VM..."
            multipass delete "$VM_NAME" --purge || true
        else
            log_info "Keeping existing VM. Use '$0 shell' to connect."
            exit 0
        fi
    fi

    log_info "Launching VM with cloud-init configuration..."
    multipass launch 24.04 \
        --name "$VM_NAME" \
        --cpus "$VM_CPUS" \
        --memory "$VM_MEMORY" \
        --disk "$VM_DISK" \
        --cloud-init "$SCRIPT_DIR/cloud-init.yaml"

    # Wait for cloud-init to complete
    log_info "Waiting for cloud-init to complete..."
    multipass exec "$VM_NAME" -- cloud-init status --wait

    # Mount the project directory
    log_info "Mounting ObjectIO project directory..."
    PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
    multipass mount "$PROJECT_DIR" "$VM_NAME":/home/ubuntu/objectio || log_warn "Mount failed - may need to enable mounts in multipass"

    log_info "VM created successfully!"
    log_info ""
    log_info "Next steps:"
    log_info "  1. Run '$0 setup-disks' to initialize the virtual disks"
    log_info "  2. Run '$0 shell' to access the VM"
}

start_vm() {
    check_multipass
    log_info "Starting VM: $VM_NAME"
    multipass start "$VM_NAME"
    log_info "VM started"
}

stop_vm() {
    check_multipass
    log_info "Stopping VM: $VM_NAME"
    multipass stop "$VM_NAME"
    log_info "VM stopped"
}

delete_vm() {
    check_multipass
    log_info "Deleting VM: $VM_NAME"
    multipass delete "$VM_NAME" 2>/dev/null || true
    multipass purge 2>/dev/null || true
    log_info "VM deleted and purged"
}

status_vm() {
    check_multipass
    log_info "VM Status:"
    multipass list 2>/dev/null | grep -E "^Name|^${VM_NAME}" || echo "VM not found"
    echo ""
    if multipass list 2>/dev/null | grep -q "^${VM_NAME}.*Running"; then
        log_info "VM Info:"
        multipass info "$VM_NAME"
    fi
}

shell_vm() {
    check_multipass
    log_info "Opening shell to VM: $VM_NAME"
    multipass shell "$VM_NAME"
}

setup_disks() {
    check_multipass
    log_info "Setting up virtual disks in VM..."
    multipass exec "$VM_NAME" -- sudo /usr/local/bin/setup-objectio-disks.sh setup
}

disk_status() {
    check_multipass
    log_info "Checking disk status in VM..."
    multipass exec "$VM_NAME" -- sudo /usr/local/bin/setup-objectio-disks.sh status
}

cleanup_disks() {
    check_multipass
    log_info "Cleaning up virtual disks in VM..."
    multipass exec "$VM_NAME" -- sudo /usr/local/bin/setup-objectio-disks.sh cleanup
}

show_usage() {
    cat << EOF
ObjectIO Test VM Setup Script

Usage: $0 <command>

Commands:
  create        Create a new VM with Ubuntu 24.04
  start         Start the VM
  stop          Stop the VM
  delete        Delete the VM completely
  status        Show VM status
  shell         Open a shell to the VM

  setup-disks   Create and setup virtual disks inside the VM
  disk-status   Show status of virtual disks
  cleanup-disks Remove virtual disks from the VM

Examples:
  $0 create         # Create the VM
  $0 setup-disks    # Setup 8 virtual disks with superblocks
  $0 disk-status    # Check disk and loop device status
  $0 shell          # Access the VM
  $0 stop           # Stop the VM
  $0 delete         # Remove the VM

The VM will have:
  - ${VM_CPUS} CPUs, ${VM_MEMORY} RAM
  - ${NUM_DISKS} virtual disks of ${DISK_SIZE_GB}GB each (sparse files)
  - Disks located at ${DISK_DIR}/disk{0-7}.img
  - Loop devices at /dev/loop{10-17}
  - Each disk has a 4KB ObjectIO superblock

Superblock Format (first 4KB):
  - Bytes 0-7:   Magic number "OBJIO001"
  - Bytes 8-11:  Version (uint32 LE)
  - Bytes 12-15: Disk ID (uint32 LE)
  - Bytes 16-23: Creation timestamp (uint64 LE)
  - Bytes 24-4095: Reserved (zeros)
EOF
}

# Main entry point
main() {
    local command="${1:-help}"

    case "$command" in
        create)
            create_vm
            ;;
        start)
            start_vm
            ;;
        stop)
            stop_vm
            ;;
        delete)
            delete_vm
            ;;
        status)
            status_vm
            ;;
        shell)
            shell_vm
            ;;
        setup-disks)
            setup_disks
            ;;
        disk-status)
            disk_status
            ;;
        cleanup-disks)
            cleanup_disks
            ;;
        help|--help|-h)
            show_usage
            ;;
        *)
            log_error "Unknown command: $command"
            show_usage
            exit 1
            ;;
    esac
}

main "$@"
