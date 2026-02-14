//! Systemd unit file generation

/// Generate systemd unit for Gateway service
pub fn generate_gateway_unit() -> String {
    r#"[Unit]
Description=ObjectIO S3 Gateway
Documentation=https://github.com/objectio/objectio
After=network-online.target
Wants=network-online.target
After=objectio-meta.service
Wants=objectio-meta.service

[Service]
Type=simple
User=objectio
Group=objectio
ExecStart=/usr/local/bin/objectio-gateway --config /etc/objectio/gateway.toml
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=5s
LimitNOFILE=65536
LimitNPROC=4096

# Security hardening
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
ReadWritePaths=/var/lib/objectio /var/log/objectio

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=objectio-gateway

[Install]
WantedBy=multi-user.target
"#
    .to_string()
}

/// Generate systemd unit for Metadata service
pub fn generate_meta_unit() -> String {
    r#"[Unit]
Description=ObjectIO Metadata Service
Documentation=https://github.com/objectio/objectio
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=objectio
Group=objectio
ExecStart=/usr/local/bin/objectio-meta --config /etc/objectio/meta.toml
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=5s
LimitNOFILE=65536
LimitNPROC=4096

# Allow binding to privileged ports if needed
AmbientCapabilities=CAP_NET_BIND_SERVICE

# Security hardening
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
ReadWritePaths=/var/lib/objectio /var/log/objectio

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=objectio-meta

[Install]
WantedBy=multi-user.target
"#
    .to_string()
}

/// Generate systemd unit for OSD service
pub fn generate_osd_unit() -> String {
    r#"[Unit]
Description=ObjectIO Object Storage Daemon
Documentation=https://github.com/objectio/objectio
After=network-online.target
Wants=network-online.target
After=objectio-meta.service
Wants=objectio-meta.service

[Service]
Type=simple
User=root
Group=root
ExecStart=/usr/local/bin/objectio-osd --config /etc/objectio/osd.toml
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=5s

# Resource limits
LimitNOFILE=1048576
LimitNPROC=4096
LimitMEMLOCK=infinity

# OSD needs elevated privileges for raw disk access
AmbientCapabilities=CAP_SYS_RAWIO CAP_DAC_READ_SEARCH

# Security hardening (limited due to raw disk access)
NoNewPrivileges=yes
ProtectHome=yes
PrivateTmp=yes
ReadWritePaths=/var/lib/objectio /var/log/objectio /dev

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=objectio-osd

[Install]
WantedBy=multi-user.target
"#
    .to_string()
}

/// Generate systemd unit for all services (convenience)
#[allow(dead_code)]
pub fn generate_all_units() -> Vec<(&'static str, String)> {
    vec![
        ("objectio-gateway", generate_gateway_unit()),
        ("objectio-meta", generate_meta_unit()),
        ("objectio-osd", generate_osd_unit()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gateway_unit() {
        let unit = generate_gateway_unit();
        assert!(unit.contains("[Unit]"));
        assert!(unit.contains("[Service]"));
        assert!(unit.contains("[Install]"));
        assert!(unit.contains("objectio-gateway"));
    }

    #[test]
    fn test_meta_unit() {
        let unit = generate_meta_unit();
        assert!(unit.contains("objectio-meta"));
    }

    #[test]
    fn test_osd_unit() {
        let unit = generate_osd_unit();
        assert!(unit.contains("objectio-osd"));
        assert!(unit.contains("CAP_SYS_RAWIO")); // Needs raw disk access
    }
}
