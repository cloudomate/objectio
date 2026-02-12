//! SMART disk health monitoring
//!
//! Monitors disk health using SMART attributes from smartctl.
//! Provides health scoring and predictive failure warnings.

use std::collections::HashMap;
use std::fmt::Write;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};
use tracing::{debug, error, warn};

/// SMART attribute IDs
const ATTR_TEMPERATURE: u8 = 194;
const ATTR_REALLOCATED_SECTORS: u8 = 5;
const ATTR_PENDING_SECTORS: u8 = 197;
const ATTR_POWER_ON_HOURS: u8 = 9;
const ATTR_UNCORRECTABLE_ERRORS: u8 = 198;
const ATTR_COMMAND_TIMEOUT: u8 = 188;

/// Thresholds for health scoring
const REALLOCATED_WARN_THRESHOLD: u64 = 1;
const REALLOCATED_CRITICAL_THRESHOLD: u64 = 10;
const PENDING_WARN_THRESHOLD: u64 = 1;
const PENDING_CRITICAL_THRESHOLD: u64 = 5;
const TEMPERATURE_WARN_CELSIUS: u64 = 50;
const TEMPERATURE_CRITICAL_CELSIUS: u64 = 60;

/// SMART health status for a single disk
#[derive(Debug, Clone)]
pub struct DiskSmartHealth {
    /// Device path (e.g., /dev/sda)
    pub device: String,
    /// Temperature in Celsius
    pub temperature_celsius: Option<u64>,
    /// Reallocated sector count
    pub reallocated_sectors: u64,
    /// Current pending sector count
    pub pending_sectors: u64,
    /// Power-on hours
    pub power_on_hours: u64,
    /// Uncorrectable error count
    pub uncorrectable_errors: u64,
    /// Command timeout count
    pub command_timeout_count: u64,
    /// Overall SMART status passed
    pub smart_passed: bool,
    /// Health score (0-100, 100 = healthy)
    pub health_score: u8,
    /// Predicted days until failure (None if unknown)
    pub predicted_failure_days: Option<u32>,
    /// Last check timestamp
    pub last_check: Instant,
    /// Raw SMART attributes
    pub attributes: HashMap<u8, SmartAttribute>,
}

impl Default for DiskSmartHealth {
    fn default() -> Self {
        Self {
            device: String::new(),
            temperature_celsius: None,
            reallocated_sectors: 0,
            pending_sectors: 0,
            power_on_hours: 0,
            uncorrectable_errors: 0,
            command_timeout_count: 0,
            smart_passed: true,
            health_score: 100,
            predicted_failure_days: None,
            last_check: Instant::now(),
            attributes: HashMap::new(),
        }
    }
}

/// A single SMART attribute
#[derive(Debug, Clone)]
pub struct SmartAttribute {
    pub id: u8,
    pub name: String,
    pub value: u64,
    pub worst: u64,
    pub threshold: u64,
    pub raw_value: u64,
}

/// SMART monitoring service
#[derive(Debug)]
pub struct SmartMonitor {
    /// OSD ID for metric labels
    osd_id: String,
    /// Disk health status per device
    disks: RwLock<HashMap<String, DiskSmartHealth>>,
    /// Check interval
    check_interval: Duration,
    /// Last global check timestamp
    last_check: RwLock<Instant>,
    /// Total checks performed
    check_count: AtomicU64,
    /// Total check errors
    check_errors: AtomicU64,
}

impl SmartMonitor {
    /// Create a new SMART monitor
    pub fn new(osd_id: &str, check_interval: Duration) -> Self {
        Self {
            osd_id: osd_id.to_string(),
            disks: RwLock::new(HashMap::new()),
            check_interval,
            last_check: RwLock::new(Instant::now() - check_interval),
            check_count: AtomicU64::new(0),
            check_errors: AtomicU64::new(0),
        }
    }

    /// Check if SMART monitoring is available
    pub fn is_available() -> bool {
        Command::new("smartctl")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// Check if a device supports SMART
    pub fn device_supports_smart(device: &str) -> bool {
        Command::new("smartctl")
            .args(["-i", device])
            .output()
            .map(|o| {
                let stdout = String::from_utf8_lossy(&o.stdout);
                stdout.contains("SMART support is: Available")
                    || stdout.contains("SMART support is: Enabled")
            })
            .unwrap_or(false)
    }

    /// Check all registered disks if interval has elapsed
    pub fn check_if_needed(&self, devices: &[String]) -> bool {
        let now = Instant::now();
        let should_check = {
            let last = self.last_check.read().unwrap();
            now.duration_since(*last) >= self.check_interval
        };

        if should_check {
            self.check_disks(devices);
            *self.last_check.write().unwrap() = now;
            true
        } else {
            false
        }
    }

    /// Force check all specified disks
    pub fn check_disks(&self, devices: &[String]) {
        for device in devices {
            match self.check_disk(device) {
                Ok(health) => {
                    debug!(
                        "SMART check for {}: score={}, temp={:?}C, reallocated={}, pending={}",
                        device,
                        health.health_score,
                        health.temperature_celsius,
                        health.reallocated_sectors,
                        health.pending_sectors
                    );
                    self.disks.write().unwrap().insert(device.clone(), health);
                }
                Err(e) => {
                    warn!("SMART check failed for {}: {}", device, e);
                    self.check_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
            self.check_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Check a single disk
    pub fn check_disk(&self, device: &str) -> Result<DiskSmartHealth, String> {
        let output = Command::new("smartctl")
            .args(["-A", "-H", "-j", device])
            .output()
            .map_err(|e| format!("Failed to run smartctl: {}", e))?;

        if !output.status.success() && output.stdout.is_empty() {
            return Err(format!(
                "smartctl failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        self.parse_smart_json(device, &stdout)
    }

    /// Parse smartctl JSON output
    fn parse_smart_json(&self, device: &str, json_str: &str) -> Result<DiskSmartHealth, String> {
        // Parse JSON
        let json: serde_json::Value =
            serde_json::from_str(json_str).map_err(|e| format!("Failed to parse JSON: {}", e))?;

        let mut health = DiskSmartHealth {
            device: device.to_string(),
            last_check: Instant::now(),
            ..Default::default()
        };

        // Parse SMART overall health
        if let Some(status) = json.get("smart_status") {
            health.smart_passed = status
                .get("passed")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
        }

        // Parse SMART attributes
        if let Some(ata_smart) = json.get("ata_smart_attributes") {
            if let Some(table) = ata_smart.get("table").and_then(|t| t.as_array()) {
                for attr in table {
                    let id = attr.get("id").and_then(|v| v.as_u64()).unwrap_or(0) as u8;
                    let name = attr
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let value = attr.get("value").and_then(|v| v.as_u64()).unwrap_or(0);
                    let worst = attr.get("worst").and_then(|v| v.as_u64()).unwrap_or(0);
                    let threshold = attr.get("thresh").and_then(|v| v.as_u64()).unwrap_or(0);
                    let raw_value = attr
                        .get("raw")
                        .and_then(|r| r.get("value"))
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);

                    let smart_attr = SmartAttribute {
                        id,
                        name,
                        value,
                        worst,
                        threshold,
                        raw_value,
                    };

                    // Extract key attributes
                    match id {
                        ATTR_TEMPERATURE => {
                            // Temperature is usually in raw value, but can be modified
                            let temp = if raw_value < 100 {
                                raw_value
                            } else {
                                // Some drives store temp in lower 8 bits
                                raw_value & 0xFF
                            };
                            health.temperature_celsius = Some(temp);
                        }
                        ATTR_REALLOCATED_SECTORS => {
                            health.reallocated_sectors = raw_value;
                        }
                        ATTR_PENDING_SECTORS => {
                            health.pending_sectors = raw_value;
                        }
                        ATTR_POWER_ON_HOURS => {
                            health.power_on_hours = raw_value;
                        }
                        ATTR_UNCORRECTABLE_ERRORS => {
                            health.uncorrectable_errors = raw_value;
                        }
                        ATTR_COMMAND_TIMEOUT => {
                            health.command_timeout_count = raw_value;
                        }
                        _ => {}
                    }

                    health.attributes.insert(id, smart_attr);
                }
            }
        }

        // Also check for NVMe temperature
        if health.temperature_celsius.is_none() {
            if let Some(temp) = json.get("temperature") {
                if let Some(current) = temp.get("current").and_then(|v| v.as_u64()) {
                    health.temperature_celsius = Some(current);
                }
            }
        }

        // Calculate health score
        health.health_score = self.calculate_health_score(&health);
        health.predicted_failure_days = self.predict_failure(&health);

        Ok(health)
    }

    /// Calculate health score (0-100)
    fn calculate_health_score(&self, health: &DiskSmartHealth) -> u8 {
        let mut score: i32 = 100;

        // SMART overall status
        if !health.smart_passed {
            score -= 50;
        }

        // Reallocated sectors
        if health.reallocated_sectors >= REALLOCATED_CRITICAL_THRESHOLD {
            score -= 40;
        } else if health.reallocated_sectors >= REALLOCATED_WARN_THRESHOLD {
            score -= 20;
        }

        // Pending sectors
        if health.pending_sectors >= PENDING_CRITICAL_THRESHOLD {
            score -= 30;
        } else if health.pending_sectors >= PENDING_WARN_THRESHOLD {
            score -= 15;
        }

        // Temperature
        if let Some(temp) = health.temperature_celsius {
            if temp >= TEMPERATURE_CRITICAL_CELSIUS {
                score -= 20;
            } else if temp >= TEMPERATURE_WARN_CELSIUS {
                score -= 10;
            }
        }

        // Uncorrectable errors
        if health.uncorrectable_errors > 0 {
            score -= std::cmp::min(health.uncorrectable_errors as i32 * 5, 30);
        }

        std::cmp::max(0, score) as u8
    }

    /// Predict days until failure (simple heuristic)
    fn predict_failure(&self, health: &DiskSmartHealth) -> Option<u32> {
        // If SMART failed, predict imminent failure
        if !health.smart_passed {
            return Some(7);
        }

        // If critical reallocated sectors, predict failure soon
        if health.reallocated_sectors >= REALLOCATED_CRITICAL_THRESHOLD {
            return Some(30);
        }

        // If pending sectors are growing, predict failure
        if health.pending_sectors >= PENDING_CRITICAL_THRESHOLD {
            return Some(60);
        }

        // If warning level, predict longer timeline
        if health.reallocated_sectors >= REALLOCATED_WARN_THRESHOLD
            || health.pending_sectors >= PENDING_WARN_THRESHOLD
        {
            return Some(180);
        }

        // No predicted failure
        None
    }

    /// Get health status for a specific disk
    pub fn get_disk_health(&self, device: &str) -> Option<DiskSmartHealth> {
        self.disks.read().unwrap().get(device).cloned()
    }

    /// Get health status for all disks
    pub fn get_all_health(&self) -> HashMap<String, DiskSmartHealth> {
        self.disks.read().unwrap().clone()
    }

    /// Check if any disk has warnings
    pub fn has_warnings(&self) -> bool {
        self.disks
            .read()
            .unwrap()
            .values()
            .any(|h| h.health_score < 80 || h.predicted_failure_days.is_some())
    }

    /// Check if any disk has critical issues
    pub fn has_critical(&self) -> bool {
        self.disks
            .read()
            .unwrap()
            .values()
            .any(|h| h.health_score < 50 || !h.smart_passed)
    }

    /// Export metrics in Prometheus format
    pub fn export_prometheus(&self) -> String {
        let mut output = String::with_capacity(4 * 1024);
        let osd_id = &self.osd_id;

        let disks = self.disks.read().unwrap();

        if disks.is_empty() {
            return output;
        }

        // Temperature
        writeln!(
            output,
            "# HELP objectio_disk_temperature_celsius Disk temperature in Celsius"
        )
        .unwrap();
        writeln!(output, "# TYPE objectio_disk_temperature_celsius gauge").unwrap();
        for (device, health) in disks.iter() {
            if let Some(temp) = health.temperature_celsius {
                writeln!(
                    output,
                    "objectio_disk_temperature_celsius{{osd_id=\"{}\",device=\"{}\"}} {}",
                    osd_id, device, temp
                )
                .unwrap();
            }
        }

        // Reallocated sectors
        writeln!(
            output,
            "# HELP objectio_disk_reallocated_sectors Count of reallocated sectors"
        )
        .unwrap();
        writeln!(output, "# TYPE objectio_disk_reallocated_sectors gauge").unwrap();
        for (device, health) in disks.iter() {
            writeln!(
                output,
                "objectio_disk_reallocated_sectors{{osd_id=\"{}\",device=\"{}\"}} {}",
                osd_id, device, health.reallocated_sectors
            )
            .unwrap();
        }

        // Pending sectors
        writeln!(
            output,
            "# HELP objectio_disk_pending_sectors Count of pending sectors"
        )
        .unwrap();
        writeln!(output, "# TYPE objectio_disk_pending_sectors gauge").unwrap();
        for (device, health) in disks.iter() {
            writeln!(
                output,
                "objectio_disk_pending_sectors{{osd_id=\"{}\",device=\"{}\"}} {}",
                osd_id, device, health.pending_sectors
            )
            .unwrap();
        }

        // Power on hours
        writeln!(
            output,
            "# HELP objectio_disk_power_on_hours Disk power-on hours"
        )
        .unwrap();
        writeln!(output, "# TYPE objectio_disk_power_on_hours counter").unwrap();
        for (device, health) in disks.iter() {
            writeln!(
                output,
                "objectio_disk_power_on_hours{{osd_id=\"{}\",device=\"{}\"}} {}",
                osd_id, device, health.power_on_hours
            )
            .unwrap();
        }

        // SMART healthy (1 = healthy, 0 = unhealthy)
        writeln!(
            output,
            "# HELP objectio_disk_healthy Disk health status (1=healthy, 0=unhealthy)"
        )
        .unwrap();
        writeln!(output, "# TYPE objectio_disk_healthy gauge").unwrap();
        for (device, health) in disks.iter() {
            let healthy = if health.smart_passed && health.health_score >= 50 {
                1
            } else {
                0
            };
            writeln!(
                output,
                "objectio_disk_healthy{{osd_id=\"{}\",device=\"{}\"}} {}",
                osd_id, device, healthy
            )
            .unwrap();
        }

        // Health score
        writeln!(
            output,
            "# HELP objectio_disk_health_score Disk health score (0-100)"
        )
        .unwrap();
        writeln!(output, "# TYPE objectio_disk_health_score gauge").unwrap();
        for (device, health) in disks.iter() {
            writeln!(
                output,
                "objectio_disk_health_score{{osd_id=\"{}\",device=\"{}\"}} {}",
                osd_id, device, health.health_score
            )
            .unwrap();
        }

        // Predicted failure days (only if predicted)
        let has_predictions = disks.values().any(|h| h.predicted_failure_days.is_some());
        if has_predictions {
            writeln!(
                output,
                "# HELP objectio_disk_predicted_failure_days Predicted days until disk failure"
            )
            .unwrap();
            writeln!(output, "# TYPE objectio_disk_predicted_failure_days gauge").unwrap();
            for (device, health) in disks.iter() {
                if let Some(days) = health.predicted_failure_days {
                    writeln!(
                        output,
                        "objectio_disk_predicted_failure_days{{osd_id=\"{}\",device=\"{}\"}} {}",
                        osd_id, device, days
                    )
                    .unwrap();
                }
            }
        }

        // Check stats
        writeln!(
            output,
            "# HELP objectio_disk_smart_checks_total Total SMART checks performed"
        )
        .unwrap();
        writeln!(output, "# TYPE objectio_disk_smart_checks_total counter").unwrap();
        writeln!(
            output,
            "objectio_disk_smart_checks_total{{osd_id=\"{}\"}} {}",
            osd_id,
            self.check_count.load(Ordering::Relaxed)
        )
        .unwrap();

        writeln!(
            output,
            "# HELP objectio_disk_smart_check_errors_total Total SMART check errors"
        )
        .unwrap();
        writeln!(
            output,
            "# TYPE objectio_disk_smart_check_errors_total counter"
        )
        .unwrap();
        writeln!(
            output,
            "objectio_disk_smart_check_errors_total{{osd_id=\"{}\"}} {}",
            osd_id,
            self.check_errors.load(Ordering::Relaxed)
        )
        .unwrap();

        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_score_calculation() {
        let monitor = SmartMonitor::new("osd1", Duration::from_secs(60));

        // Healthy disk
        let healthy = DiskSmartHealth {
            smart_passed: true,
            reallocated_sectors: 0,
            pending_sectors: 0,
            temperature_celsius: Some(35),
            ..Default::default()
        };
        assert_eq!(monitor.calculate_health_score(&healthy), 100);

        // Disk with reallocated sectors
        let warning = DiskSmartHealth {
            smart_passed: true,
            reallocated_sectors: 5,
            pending_sectors: 0,
            temperature_celsius: Some(35),
            ..Default::default()
        };
        assert!(monitor.calculate_health_score(&warning) < 100);

        // Failed SMART
        let failed = DiskSmartHealth {
            smart_passed: false,
            reallocated_sectors: 0,
            pending_sectors: 0,
            temperature_celsius: Some(35),
            ..Default::default()
        };
        assert!(monitor.calculate_health_score(&failed) <= 50);
    }

    #[test]
    fn test_predict_failure() {
        let monitor = SmartMonitor::new("osd1", Duration::from_secs(60));

        // Healthy disk - no prediction
        let healthy = DiskSmartHealth {
            smart_passed: true,
            reallocated_sectors: 0,
            pending_sectors: 0,
            ..Default::default()
        };
        assert!(monitor.predict_failure(&healthy).is_none());

        // Failed SMART - imminent failure
        let failed = DiskSmartHealth {
            smart_passed: false,
            reallocated_sectors: 0,
            pending_sectors: 0,
            ..Default::default()
        };
        assert!(monitor.predict_failure(&failed).is_some());
        assert!(monitor.predict_failure(&failed).unwrap() <= 30);

        // Critical reallocated sectors
        let critical = DiskSmartHealth {
            smart_passed: true,
            reallocated_sectors: 15,
            pending_sectors: 0,
            ..Default::default()
        };
        assert!(monitor.predict_failure(&critical).is_some());
    }
}
