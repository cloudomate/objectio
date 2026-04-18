//! Quality of Service (QoS) for block storage
//!
//! Provides rate limiting and I/O scheduling to guarantee IOPS and bandwidth
//! for volumes. QoS is enforced at the block gateway level, not at OSDs.
//!
//! # Components
//!
//! - **TokenBucket**: Rate limiter using token bucket algorithm with burst support
//! - **VolumeRateLimiter**: Per-volume IOPS and bandwidth limits
//! - **IoStats**: Real-time I/O statistics collection
//!
//! # Example
//!
//! ```ignore
//! let qos = VolumeQosConfig {
//!     max_iops: 10000,
//!     min_iops: 5000,
//!     max_bandwidth_bps: 500 * 1024 * 1024, // 500 MB/s
//!     burst_iops: 15000,
//!     burst_seconds: 30,
//!     priority: Priority::High,
//! };
//!
//! let limiter = VolumeRateLimiter::new(qos);
//!
//! // Before each I/O
//! if limiter.try_acquire_iops(1) && limiter.try_acquire_bandwidth(4096) {
//!     // Proceed with I/O
//! } else {
//!     // Throttled - queue or return busy
//! }
//! ```

use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// I/O priority level for scheduling
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum Priority {
    Low = 0,
    #[default]
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// QoS configuration for a volume
#[derive(Debug, Clone)]
pub struct VolumeQosConfig {
    /// Maximum sustained IOPS (0 = unlimited)
    pub max_iops: u64,
    /// Guaranteed minimum IOPS (reserved capacity)
    pub min_iops: u64,
    /// Maximum bandwidth in bytes per second (0 = unlimited)
    pub max_bandwidth_bps: u64,
    /// Burst IOPS above max (for short periods)
    pub burst_iops: u64,
    /// How long burst can last in seconds
    pub burst_seconds: u32,
    /// I/O scheduling priority
    pub priority: Priority,
    /// Target latency SLO in microseconds (informational)
    pub target_latency_us: u64,
}

impl Default for VolumeQosConfig {
    fn default() -> Self {
        Self {
            max_iops: 0,          // Unlimited
            min_iops: 0,          // No guarantee
            max_bandwidth_bps: 0, // Unlimited
            burst_iops: 0,        // No burst
            burst_seconds: 0,
            priority: Priority::Normal,
            target_latency_us: 1000, // 1ms default target
        }
    }
}

impl VolumeQosConfig {
    /// Check if QoS limits are configured (not unlimited)
    pub fn has_limits(&self) -> bool {
        self.max_iops > 0 || self.max_bandwidth_bps > 0
    }

    /// Check if this config has guaranteed IOPS
    pub fn has_guarantee(&self) -> bool {
        self.min_iops > 0
    }
}

/// Token bucket rate limiter
///
/// Implements the token bucket algorithm for rate limiting:
/// - Tokens are added at a fixed rate (refill_rate per second)
/// - Tokens accumulate up to max_tokens (burst capacity)
/// - Each operation consumes tokens
/// - If no tokens available, operation is rejected (or queued)
#[derive(Debug)]
pub struct TokenBucket {
    /// Current token count (scaled by 1000 for sub-token precision)
    tokens: AtomicU64,
    /// Maximum tokens (burst capacity), scaled
    max_tokens: u64,
    /// Tokens added per second, scaled
    refill_rate: u64,
    /// Last refill timestamp
    last_refill: Mutex<Instant>,
    /// Scale factor for sub-token precision
    scale: u64,
}

impl TokenBucket {
    /// Create a new token bucket
    ///
    /// # Arguments
    /// * `rate` - Tokens per second (sustained rate)
    /// * `burst` - Maximum tokens (burst capacity)
    pub fn new(rate: u64, burst: u64) -> Self {
        let scale = 1000; // Allows fractional tokens
        Self {
            tokens: AtomicU64::new(burst * scale),
            max_tokens: burst * scale,
            refill_rate: rate * scale,
            last_refill: Mutex::new(Instant::now()),
            scale,
        }
    }

    /// Try to acquire tokens without blocking
    ///
    /// Returns true if tokens were acquired, false if rate limited
    pub fn try_acquire(&self, count: u64) -> bool {
        self.refill();

        let needed = count * self.scale;
        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            if current < needed {
                return false;
            }

            match self.tokens.compare_exchange_weak(
                current,
                current - needed,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(_) => continue, // Retry on contention
            }
        }
    }

    /// Get current token count
    pub fn available(&self) -> u64 {
        self.refill();
        self.tokens.load(Ordering::Relaxed) / self.scale
    }

    /// Refill tokens based on elapsed time
    fn refill(&self) {
        let mut last = self.last_refill.lock();
        let now = Instant::now();
        let elapsed = now.duration_since(*last);

        // Calculate new tokens (with microsecond precision)
        let elapsed_us = elapsed.as_micros() as u64;
        let new_tokens = (elapsed_us * self.refill_rate) / 1_000_000;

        if new_tokens > 0 {
            let current = self.tokens.load(Ordering::Relaxed);
            let new_value = (current + new_tokens).min(self.max_tokens);
            self.tokens.store(new_value, Ordering::Relaxed);
            *last = now;
        }
    }

    /// Update the rate limit dynamically
    pub fn set_rate(&mut self, rate: u64, burst: u64) {
        self.refill_rate = rate * self.scale;
        self.max_tokens = burst * self.scale;
        // Cap current tokens to new max
        let current = self.tokens.load(Ordering::Relaxed);
        if current > self.max_tokens {
            self.tokens.store(self.max_tokens, Ordering::Relaxed);
        }
    }
}

/// Per-volume rate limiter combining IOPS and bandwidth limits
#[derive(Debug)]
pub struct VolumeRateLimiter {
    /// Volume ID
    volume_id: String,
    /// IOPS token bucket
    iops_bucket: Option<TokenBucket>,
    /// Bandwidth token bucket (tokens = bytes)
    bandwidth_bucket: Option<TokenBucket>,
    /// QoS configuration
    config: VolumeQosConfig,
    /// I/O statistics
    stats: IoStats,
}

impl VolumeRateLimiter {
    /// Create a new rate limiter for a volume
    pub fn new(volume_id: String, config: VolumeQosConfig) -> Self {
        let iops_bucket = if config.max_iops > 0 {
            let burst = if config.burst_iops > 0 {
                config.burst_iops
            } else {
                config.max_iops
            };
            Some(TokenBucket::new(config.max_iops, burst))
        } else {
            None
        };

        let bandwidth_bucket = if config.max_bandwidth_bps > 0 {
            // Burst = 1 second worth of bandwidth
            Some(TokenBucket::new(
                config.max_bandwidth_bps,
                config.max_bandwidth_bps,
            ))
        } else {
            None
        };

        Self {
            volume_id,
            iops_bucket,
            bandwidth_bucket,
            config,
            stats: IoStats::new(),
        }
    }

    /// Try to acquire permission for an I/O operation
    ///
    /// Returns true if the I/O can proceed, false if throttled
    pub fn try_acquire(&self, io_size_bytes: u64) -> bool {
        // Check IOPS limit
        if let Some(ref bucket) = self.iops_bucket
            && !bucket.try_acquire(1)
        {
            self.stats.record_throttled();
            return false;
        }

        // Check bandwidth limit
        if let Some(ref bucket) = self.bandwidth_bucket
            && !bucket.try_acquire(io_size_bytes)
        {
            self.stats.record_throttled();
            return false;
        }

        true
    }

    /// Record a completed read operation
    pub fn record_read(&self, bytes: u64, latency_us: u64) {
        self.stats.record_read(bytes, latency_us);
    }

    /// Record a completed write operation
    pub fn record_write(&self, bytes: u64, latency_us: u64) {
        self.stats.record_write(bytes, latency_us);
    }

    /// Get current I/O statistics
    pub fn stats(&self) -> &IoStats {
        &self.stats
    }

    /// Get the QoS configuration
    pub fn config(&self) -> &VolumeQosConfig {
        &self.config
    }

    /// Update QoS configuration
    pub fn update_config(&mut self, config: VolumeQosConfig) {
        // Update IOPS bucket
        if config.max_iops > 0 {
            let burst = if config.burst_iops > 0 {
                config.burst_iops
            } else {
                config.max_iops
            };
            if let Some(ref mut bucket) = self.iops_bucket {
                bucket.set_rate(config.max_iops, burst);
            } else {
                self.iops_bucket = Some(TokenBucket::new(config.max_iops, burst));
            }
        } else {
            self.iops_bucket = None;
        }

        // Update bandwidth bucket
        if config.max_bandwidth_bps > 0 {
            if let Some(ref mut bucket) = self.bandwidth_bucket {
                bucket.set_rate(config.max_bandwidth_bps, config.max_bandwidth_bps);
            } else {
                self.bandwidth_bucket = Some(TokenBucket::new(
                    config.max_bandwidth_bps,
                    config.max_bandwidth_bps,
                ));
            }
        } else {
            self.bandwidth_bucket = None;
        }

        self.config = config;
    }

    /// Get volume ID
    pub fn volume_id(&self) -> &str {
        &self.volume_id
    }

    /// Calculate current IOPS utilization (0.0 - 1.0)
    pub fn iops_utilization(&self) -> f64 {
        if self.config.max_iops == 0 {
            return 0.0;
        }
        let current = self.stats.current_iops();
        (current as f64 / self.config.max_iops as f64).min(1.0)
    }
}

/// Latency histogram for percentile calculations
///
/// Uses logarithmic buckets to efficiently track latency distribution:
/// - Bucket 0: 0-10us
/// - Bucket 1: 10-20us
/// - Bucket 2: 20-50us
/// - Bucket 3: 50-100us
/// - Bucket 4: 100-200us
/// - Bucket 5: 200-500us
/// - Bucket 6: 500us-1ms
/// - Bucket 7: 1-2ms
/// - Bucket 8: 2-5ms
/// - Bucket 9: 5-10ms
/// - Bucket 10: 10-20ms
/// - Bucket 11: 20-50ms
/// - Bucket 12: 50-100ms
/// - Bucket 13: 100-200ms
/// - Bucket 14: 200-500ms
/// - Bucket 15: 500ms+
#[derive(Debug)]
pub struct LatencyHistogram {
    /// Bucket counts (16 buckets)
    buckets: [AtomicU64; 16],
    /// Total samples
    count: AtomicU64,
    /// Sum of all samples (for average)
    sum: AtomicU64,
    /// Minimum latency observed
    min: AtomicU64,
    /// Maximum latency observed
    max: AtomicU64,
}

/// Bucket boundaries in microseconds
const BUCKET_BOUNDARIES_US: [u64; 16] = [
    10,       // 0: 0-10us
    20,       // 1: 10-20us
    50,       // 2: 20-50us
    100,      // 3: 50-100us
    200,      // 4: 100-200us
    500,      // 5: 200-500us
    1_000,    // 6: 500us-1ms
    2_000,    // 7: 1-2ms
    5_000,    // 8: 2-5ms
    10_000,   // 9: 5-10ms
    20_000,   // 10: 10-20ms
    50_000,   // 11: 20-50ms
    100_000,  // 12: 50-100ms
    200_000,  // 13: 100-200ms
    500_000,  // 14: 200-500ms
    u64::MAX, // 15: 500ms+
];

impl LatencyHistogram {
    /// Create a new latency histogram
    pub fn new() -> Self {
        Self {
            buckets: Default::default(),
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            min: AtomicU64::new(u64::MAX),
            max: AtomicU64::new(0),
        }
    }

    /// Record a latency sample in microseconds
    pub fn record(&self, latency_us: u64) {
        // Find bucket
        let bucket_idx = BUCKET_BOUNDARIES_US
            .iter()
            .position(|&boundary| latency_us < boundary)
            .unwrap_or(15);

        self.buckets[bucket_idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(latency_us, Ordering::Relaxed);

        // Update min (using compare-exchange loop)
        loop {
            let current_min = self.min.load(Ordering::Relaxed);
            if latency_us >= current_min {
                break;
            }
            if self
                .min
                .compare_exchange_weak(
                    current_min,
                    latency_us,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
        }

        // Update max
        loop {
            let current_max = self.max.load(Ordering::Relaxed);
            if latency_us <= current_max {
                break;
            }
            if self
                .max
                .compare_exchange_weak(
                    current_max,
                    latency_us,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
        }
    }

    /// Get the count of samples
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Get the sum of all samples
    pub fn sum(&self) -> u64 {
        self.sum.load(Ordering::Relaxed)
    }

    /// Get the average latency in microseconds
    pub fn avg(&self) -> u64 {
        let count = self.count();
        if count == 0 {
            return 0;
        }
        self.sum() / count
    }

    /// Get the minimum latency observed
    pub fn min(&self) -> u64 {
        let min = self.min.load(Ordering::Relaxed);
        if min == u64::MAX { 0 } else { min }
    }

    /// Get the maximum latency observed
    pub fn max(&self) -> u64 {
        self.max.load(Ordering::Relaxed)
    }

    /// Get percentile latency in microseconds
    ///
    /// # Arguments
    /// * `percentile` - Percentile to calculate (0.0 to 1.0, e.g., 0.99 for p99)
    pub fn percentile(&self, percentile: f64) -> u64 {
        let total = self.count();
        if total == 0 {
            return 0;
        }

        let target = ((total as f64) * percentile).ceil() as u64;
        let mut cumulative = 0u64;

        for (i, bucket) in self.buckets.iter().enumerate() {
            cumulative += bucket.load(Ordering::Relaxed);
            if cumulative >= target {
                // Return upper bound of this bucket
                return BUCKET_BOUNDARIES_US[i];
            }
        }

        // Shouldn't reach here, but return max boundary
        BUCKET_BOUNDARIES_US[15]
    }

    /// Get p50 (median) latency in microseconds
    pub fn p50(&self) -> u64 {
        self.percentile(0.50)
    }

    /// Get p90 latency in microseconds
    pub fn p90(&self) -> u64 {
        self.percentile(0.90)
    }

    /// Get p95 latency in microseconds
    pub fn p95(&self) -> u64 {
        self.percentile(0.95)
    }

    /// Get p99 latency in microseconds
    pub fn p99(&self) -> u64 {
        self.percentile(0.99)
    }

    /// Get p999 (99.9th percentile) latency in microseconds
    pub fn p999(&self) -> u64 {
        self.percentile(0.999)
    }

    /// Get bucket counts for Prometheus histogram export
    pub fn bucket_counts(&self) -> Vec<(u64, u64)> {
        BUCKET_BOUNDARIES_US
            .iter()
            .zip(self.buckets.iter())
            .map(|(&boundary, count)| (boundary, count.load(Ordering::Relaxed)))
            .collect()
    }

    /// Reset the histogram
    pub fn reset(&self) {
        for bucket in &self.buckets {
            bucket.store(0, Ordering::Relaxed);
        }
        self.count.store(0, Ordering::Relaxed);
        self.sum.store(0, Ordering::Relaxed);
        self.min.store(u64::MAX, Ordering::Relaxed);
        self.max.store(0, Ordering::Relaxed);
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Latency percentiles snapshot
#[derive(Debug, Clone, Copy, Default)]
pub struct LatencyPercentiles {
    /// Average latency in microseconds
    pub avg: u64,
    /// Minimum latency in microseconds
    pub min: u64,
    /// Maximum latency in microseconds
    pub max: u64,
    /// 50th percentile (median)
    pub p50: u64,
    /// 90th percentile
    pub p90: u64,
    /// 95th percentile
    pub p95: u64,
    /// 99th percentile
    pub p99: u64,
    /// 99.9th percentile
    pub p999: u64,
}

impl LatencyPercentiles {
    /// Create from histogram
    pub fn from_histogram(histogram: &LatencyHistogram) -> Self {
        Self {
            avg: histogram.avg(),
            min: histogram.min(),
            max: histogram.max(),
            p50: histogram.p50(),
            p90: histogram.p90(),
            p95: histogram.p95(),
            p99: histogram.p99(),
            p999: histogram.p999(),
        }
    }
}

/// Real-time I/O statistics for a volume
#[derive(Debug)]
pub struct IoStats {
    /// Total read operations
    read_ops: AtomicU64,
    /// Total write operations
    write_ops: AtomicU64,
    /// Total bytes read
    read_bytes: AtomicU64,
    /// Total bytes written
    write_bytes: AtomicU64,
    /// Total read latency (microseconds)
    read_latency_sum: AtomicU64,
    /// Total write latency (microseconds)
    write_latency_sum: AtomicU64,
    /// Number of throttled I/Os
    throttled_ops: AtomicU64,
    /// Statistics window start time
    window_start: Mutex<Instant>,
    /// Ops in current window (for IOPS calculation)
    window_ops: AtomicU64,
    /// Read latency histogram
    read_latency_histogram: LatencyHistogram,
    /// Write latency histogram
    write_latency_histogram: LatencyHistogram,
}

impl IoStats {
    /// Create new I/O statistics tracker
    pub fn new() -> Self {
        Self {
            read_ops: AtomicU64::new(0),
            write_ops: AtomicU64::new(0),
            read_bytes: AtomicU64::new(0),
            write_bytes: AtomicU64::new(0),
            read_latency_sum: AtomicU64::new(0),
            write_latency_sum: AtomicU64::new(0),
            throttled_ops: AtomicU64::new(0),
            window_start: Mutex::new(Instant::now()),
            window_ops: AtomicU64::new(0),
            read_latency_histogram: LatencyHistogram::new(),
            write_latency_histogram: LatencyHistogram::new(),
        }
    }

    /// Record a read operation
    pub fn record_read(&self, bytes: u64, latency_us: u64) {
        self.read_ops.fetch_add(1, Ordering::Relaxed);
        self.read_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.read_latency_sum
            .fetch_add(latency_us, Ordering::Relaxed);
        self.window_ops.fetch_add(1, Ordering::Relaxed);
        self.read_latency_histogram.record(latency_us);
    }

    /// Record a write operation
    pub fn record_write(&self, bytes: u64, latency_us: u64) {
        self.write_ops.fetch_add(1, Ordering::Relaxed);
        self.write_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.write_latency_sum
            .fetch_add(latency_us, Ordering::Relaxed);
        self.window_ops.fetch_add(1, Ordering::Relaxed);
        self.write_latency_histogram.record(latency_us);
    }

    /// Record a throttled I/O
    pub fn record_throttled(&self) {
        self.throttled_ops.fetch_add(1, Ordering::Relaxed);
    }

    /// Get total read operations
    pub fn read_ops(&self) -> u64 {
        self.read_ops.load(Ordering::Relaxed)
    }

    /// Get total write operations
    pub fn write_ops(&self) -> u64 {
        self.write_ops.load(Ordering::Relaxed)
    }

    /// Get total bytes read
    pub fn read_bytes(&self) -> u64 {
        self.read_bytes.load(Ordering::Relaxed)
    }

    /// Get total bytes written
    pub fn write_bytes(&self) -> u64 {
        self.write_bytes.load(Ordering::Relaxed)
    }

    /// Get number of throttled operations
    pub fn throttled_ops(&self) -> u64 {
        self.throttled_ops.load(Ordering::Relaxed)
    }

    /// Get average read latency in microseconds
    pub fn avg_read_latency_us(&self) -> u64 {
        let ops = self.read_ops.load(Ordering::Relaxed);
        if ops == 0 {
            return 0;
        }
        self.read_latency_sum.load(Ordering::Relaxed) / ops
    }

    /// Get average write latency in microseconds
    pub fn avg_write_latency_us(&self) -> u64 {
        let ops = self.write_ops.load(Ordering::Relaxed);
        if ops == 0 {
            return 0;
        }
        self.write_latency_sum.load(Ordering::Relaxed) / ops
    }

    /// Get current IOPS (operations in the last second)
    pub fn current_iops(&self) -> u64 {
        let mut window_start = self.window_start.lock();
        let now = Instant::now();
        let elapsed = now.duration_since(*window_start);

        if elapsed >= Duration::from_secs(1) {
            // Reset window
            let ops = self.window_ops.swap(0, Ordering::Relaxed);
            *window_start = now;
            ops
        } else {
            // Extrapolate to per-second
            let ops = self.window_ops.load(Ordering::Relaxed);
            let elapsed_ms = elapsed.as_millis() as u64;
            if elapsed_ms > 0 {
                (ops * 1000) / elapsed_ms
            } else {
                0
            }
        }
    }

    /// Get read latency percentiles
    pub fn read_latency_percentiles(&self) -> LatencyPercentiles {
        LatencyPercentiles::from_histogram(&self.read_latency_histogram)
    }

    /// Get write latency percentiles
    pub fn write_latency_percentiles(&self) -> LatencyPercentiles {
        LatencyPercentiles::from_histogram(&self.write_latency_histogram)
    }

    /// Get read latency histogram for Prometheus export
    pub fn read_latency_histogram(&self) -> &LatencyHistogram {
        &self.read_latency_histogram
    }

    /// Get write latency histogram for Prometheus export
    pub fn write_latency_histogram(&self) -> &LatencyHistogram {
        &self.write_latency_histogram
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.read_ops.store(0, Ordering::Relaxed);
        self.write_ops.store(0, Ordering::Relaxed);
        self.read_bytes.store(0, Ordering::Relaxed);
        self.write_bytes.store(0, Ordering::Relaxed);
        self.read_latency_sum.store(0, Ordering::Relaxed);
        self.write_latency_sum.store(0, Ordering::Relaxed);
        self.throttled_ops.store(0, Ordering::Relaxed);
        self.window_ops.store(0, Ordering::Relaxed);
        *self.window_start.lock() = Instant::now();
        self.read_latency_histogram.reset();
        self.write_latency_histogram.reset();
    }
}

impl Default for IoStats {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_token_bucket_basic() {
        let bucket = TokenBucket::new(100, 100); // 100 tokens/sec, 100 burst

        // Should have full burst available
        assert!(bucket.try_acquire(50));
        assert!(bucket.try_acquire(50));

        // Should be empty now
        assert!(!bucket.try_acquire(1));
    }

    #[test]
    fn test_token_bucket_refill() {
        let bucket = TokenBucket::new(1000, 100); // 1000 tokens/sec

        // Drain the bucket
        assert!(bucket.try_acquire(100));
        assert!(!bucket.try_acquire(1));

        // Wait for refill (100ms = ~100 tokens at 1000/sec)
        thread::sleep(Duration::from_millis(110));

        // Should have tokens now
        assert!(bucket.try_acquire(50));
    }

    #[test]
    fn test_volume_rate_limiter() {
        let config = VolumeQosConfig {
            max_iops: 1000,
            max_bandwidth_bps: 10 * 1024 * 1024, // 10 MB/s
            ..Default::default()
        };

        let limiter = VolumeRateLimiter::new("vol-1".to_string(), config);

        // Should allow I/O within limits
        assert!(limiter.try_acquire(4096));

        // Record some I/O
        limiter.record_write(4096, 100);
        assert_eq!(limiter.stats().write_ops(), 1);
    }

    #[test]
    fn test_unlimited_qos() {
        let config = VolumeQosConfig::default(); // All zeros = unlimited
        let limiter = VolumeRateLimiter::new("vol-1".to_string(), config);

        // Should always allow
        for _ in 0..1000 {
            assert!(limiter.try_acquire(1024 * 1024));
        }
    }

    #[test]
    fn test_io_stats() {
        let stats = IoStats::new();

        stats.record_read(4096, 100);
        stats.record_read(4096, 200);
        stats.record_write(8192, 150);

        assert_eq!(stats.read_ops(), 2);
        assert_eq!(stats.write_ops(), 1);
        assert_eq!(stats.read_bytes(), 8192);
        assert_eq!(stats.write_bytes(), 8192);
        assert_eq!(stats.avg_read_latency_us(), 150); // (100+200)/2
    }

    #[test]
    fn test_latency_histogram_basic() {
        let histogram = LatencyHistogram::new();

        // Record some latencies
        histogram.record(5); // 0-10us bucket
        histogram.record(15); // 10-20us bucket
        histogram.record(150); // 100-200us bucket
        histogram.record(1500); // 1-2ms bucket

        assert_eq!(histogram.count(), 4);
        assert_eq!(histogram.min(), 5);
        assert_eq!(histogram.max(), 1500);
    }

    #[test]
    fn test_latency_histogram_percentiles() {
        let histogram = LatencyHistogram::new();

        // Record 100 samples in different buckets (boundaries use strict <)
        // 50 samples at 50us (bucket 3: 50-100us, since 50 < 50 is false)
        // 40 samples at 150us (bucket 4: 100-200us)
        // 9 samples at 5000us (bucket 9: 5-10ms, since 5000 < 5000 is false)
        // 1 sample at 50000us (bucket 12: 50-100ms, since 50000 < 50000 is false)

        for _ in 0..50 {
            histogram.record(50);
        }
        for _ in 0..40 {
            histogram.record(150);
        }
        for _ in 0..9 {
            histogram.record(5000);
        }
        histogram.record(50000);

        assert_eq!(histogram.count(), 100);

        // p50 should be in the 50us bucket (upper bound = 100us)
        assert_eq!(histogram.p50(), 100);

        // p90 should be in the 150us bucket (upper bound = 200us)
        assert_eq!(histogram.p90(), 200);

        // p99 should be in the 5000us bucket (upper bound = 10000us)
        assert_eq!(histogram.p99(), 10000);

        // p999 (99.9th) should be in the 50000us bucket (upper bound = 100000us)
        assert_eq!(histogram.p999(), 100000);
    }

    #[test]
    fn test_latency_histogram_bucket_counts() {
        let histogram = LatencyHistogram::new();

        histogram.record(5); // bucket 0
        histogram.record(5); // bucket 0
        histogram.record(150); // bucket 4

        let buckets = histogram.bucket_counts();
        assert_eq!(buckets[0].1, 2); // Two samples in bucket 0
        assert_eq!(buckets[4].1, 1); // One sample in bucket 4
    }

    #[test]
    fn test_latency_histogram_reset() {
        let histogram = LatencyHistogram::new();

        histogram.record(100);
        histogram.record(200);
        assert_eq!(histogram.count(), 2);

        histogram.reset();
        assert_eq!(histogram.count(), 0);
        assert_eq!(histogram.sum(), 0);
        assert_eq!(histogram.min(), 0);
        assert_eq!(histogram.max(), 0);
    }

    #[test]
    fn test_io_stats_percentiles() {
        let stats = IoStats::new();

        // Record 100 reads with varying latencies
        for i in 0..100 {
            stats.record_read(4096, (i + 1) * 10); // 10us to 1000us
        }

        let percentiles = stats.read_latency_percentiles();
        assert!(percentiles.p50 > 0);
        assert!(percentiles.p95 >= percentiles.p50);
        assert!(percentiles.p99 >= percentiles.p95);
    }
}
