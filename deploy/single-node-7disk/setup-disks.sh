#!/bin/bash
# Setup disks on the remote VM for ObjectIO
# Run this script on the target VM

set -e

echo "=== ObjectIO Disk Setup ==="

# Disk mappings: vdb -> disk0, vdc -> disk1, etc.
DISKS=(vdb vdc vdd vde vdf vdg vdh)

for i in "${!DISKS[@]}"; do
    DISK="${DISKS[$i]}"
    MOUNT_POINT="/mnt/disk${i}"
    DEVICE="/dev/${DISK}"

    echo "Setting up ${DEVICE} -> ${MOUNT_POINT}"

    # Create mount point
    sudo mkdir -p "${MOUNT_POINT}"

    # Check if disk is already formatted
    if ! sudo blkid "${DEVICE}" &>/dev/null; then
        echo "  Formatting ${DEVICE} as ext4..."
        sudo mkfs.ext4 -F "${DEVICE}"
    fi

    # Mount if not already mounted
    if ! mountpoint -q "${MOUNT_POINT}"; then
        echo "  Mounting ${DEVICE} at ${MOUNT_POINT}..."
        sudo mount "${DEVICE}" "${MOUNT_POINT}"
    fi

    # Add to fstab if not already there
    if ! grep -q "${DEVICE}" /etc/fstab; then
        echo "  Adding to /etc/fstab..."
        echo "${DEVICE} ${MOUNT_POINT} ext4 defaults,nofail 0 2" | sudo tee -a /etc/fstab
    fi

    # Set permissions for Docker
    sudo chown -R 1000:1000 "${MOUNT_POINT}"
    sudo chmod 755 "${MOUNT_POINT}"

    echo "  Done: ${DEVICE} -> ${MOUNT_POINT}"
done

echo ""
echo "=== Disk Setup Complete ==="
lsblk -o NAME,SIZE,TYPE,MOUNTPOINT,FSTYPE | grep -E "^(NAME|vd[b-h])"
echo ""
echo "Total usable storage:"
df -h /mnt/disk* | tail -n +2 | awk '{sum+=$2} END {print sum " GB"}'
