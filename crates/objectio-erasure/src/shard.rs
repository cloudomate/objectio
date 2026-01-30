//! Shard type for erasure coded data

use bytes::Bytes;
use objectio_common::{Checksum, ObjectId, ShardId};

/// A single shard of erasure coded data
#[derive(Clone, Debug)]
pub struct Shard {
    /// Unique identifier for this shard
    pub id: ShardId,
    /// Whether this is a parity shard (vs data shard)
    pub is_parity: bool,
    /// The shard data
    pub data: Bytes,
    /// Checksum for integrity verification
    pub checksum: Checksum,
}

impl Shard {
    /// Create a new shard
    #[must_use]
    pub fn new(id: ShardId, data: Bytes, is_parity: bool) -> Self {
        let checksum = Checksum::compute_fast(&data);
        Self {
            id,
            is_parity,
            data,
            checksum,
        }
    }

    /// Create a data shard
    #[must_use]
    pub fn data(object_id: ObjectId, stripe_id: u64, position: u8, data: Bytes) -> Self {
        Self::new(ShardId::new(object_id, stripe_id, position), data, false)
    }

    /// Create a parity shard
    #[must_use]
    pub fn parity(object_id: ObjectId, stripe_id: u64, position: u8, data: Bytes) -> Self {
        Self::new(ShardId::new(object_id, stripe_id, position), data, true)
    }

    /// Verify the shard's checksum
    #[must_use]
    pub fn verify(&self) -> bool {
        self.checksum.verify_fast(&self.data)
    }

    /// Get the size of the shard data
    #[must_use]
    pub fn size(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_creation() {
        let object_id = ObjectId::new();
        let data = Bytes::from_static(b"test data");
        let shard = Shard::data(object_id, 0, 0, data.clone());

        assert!(!shard.is_parity);
        assert_eq!(shard.data, data);
        assert!(shard.verify());
    }

    #[test]
    fn test_shard_verify() {
        let object_id = ObjectId::new();
        let data = Bytes::from_static(b"test data");
        let shard = Shard::data(object_id, 0, 0, data);

        assert!(shard.verify());
    }
}
