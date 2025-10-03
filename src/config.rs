use std::{net::SocketAddr, path::PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::storage::StorageConfig;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("Validation error: {0}")]
    Validation(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlazeServerConfig {
    // Server settings
    pub server: ServerConfig,

    // Storage engine configuration
    pub storage: StorageConfig,

    // Persistence settings
    pub persistence: PersistenceConfig,

    // Observability settings
    pub observability: ObservabilityConfig,

    // Security settings (optional)
    #[serde(default)]
    pub security: SecurityConfig,
}

// Server-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    // TCP server bind address
    #[serde(default = "default_bind_addr")]
    pub bind_addr: SocketAddr,

    // Connection timeout in seconds
    #[serde(default = "default_connection_timeout")]
    pub connection_timeout: u64,

    // Maximum concurrent connections
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    // TCP keepalive interval (seconds)
    #[serde(default = "default_keepalive")]
    pub keepalive_interval: Option<u64>,

    // Number of worker threads (0 = auto)
    #[serde(default)]
    pub worker_threads: usize,
}

// Pesistence configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceConfig {
    // Enable AOF (Append-Only File) logging
    #[serde(default = "default_true")]
    pub enabled: bool,

    // AOF file path
    #[serde(default = "default_aof_path")]
    pub aof_path: PathBuf,

    // Fsync policy: always(0), every_n(n), never
    #[serde(default = "default_fsync_policy")]
    pub fsync_policy: FsyncPolicy,

    // Enable snapshots
    #[serde(default = "default_true")]
    pub snapshot_enabled: bool,

    // Snapshot interval in seconds
    #[serde(default = "default_snapshot_interval")]
    pub snapshot_interval: u64,

    // Snapshot directory
    #[serde(default = "default_snapshot_dir")]
    pub snapshot_dir: PathBuf,
}

// Fsync policy for AOF
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum FsyncPolicy {
    Always,      // Fsync after every write (safest, slowest)
    EveryN(u64), // Fsync after N operations (balanced)
    Never,       // OS decides (fastest, least safe)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    // Enable metrics endpoint
    #[serde(default = "default_true")]
    pub metrics_enabled: bool,

    // Metrics server bind address
    #[serde(default = "default_metrics_addr")]
    pub metrics_addr: Option<SocketAddr>,

    // Enable health check endpoint
    #[serde(default = "default_true")]
    pub health_check_enabled: bool,

    // Health check server bind address
    #[serde(default = "default_health_addr")]
    pub health_check_addr: Option<SocketAddr>,

    // Log level: trace, debug, info, warn, error
    #[serde(default = "default_log_level")]
    pub log_level: String,

    // Log format: compact, pretty, json
    #[serde(default = "default_log_format")]
    pub log_format: String,
}

// Security configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SecurityConfig {
    // Enable TLS
    #[serde(default)]
    pub tls_enabled: bool,

    // TLS certificate path
    pub tls_cert_path: Option<PathBuf>,

    // TLS private key path
    pub tls_key_path: Option<PathBuf>,

    // Require client authentication
    #[serde(default)]
    pub require_auth: bool,

    pub auth_password: Option<String>,
}

// Default value functions
fn default_bind_addr() -> SocketAddr {
    "127.0.0.1:6379".parse().unwrap()
}

fn default_connection_timeout() -> u64 {
    300
}

fn default_max_connections() -> usize {
    1000
}

fn default_keepalive() -> Option<u64> {
    Some(60)
}

fn default_true() -> bool {
    true
}

fn default_aof_path() -> PathBuf {
    PathBuf::from("data/blazekvdb.aof")
}

fn default_fsync_policy() -> FsyncPolicy {
    FsyncPolicy::EveryN(100)
}

fn default_snapshot_interval() -> u64 {
    3600
}

fn default_snapshot_dir() -> PathBuf {
    PathBuf::from("data/snapshots")
}

fn default_metrics_addr() -> Option<SocketAddr> {
    Some("127.0.0.1:9090".parse().unwrap())
}

fn default_health_addr() -> Option<SocketAddr> {
    Some("127.0.0.1:8080".parse().unwrap())
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_format() -> String {
    "compact".to_string()
}

impl Default for BlazeServerConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                bind_addr: default_bind_addr(),
                connection_timeout: default_connection_timeout(),
                max_connections: default_max_connections(),
                keepalive_interval: default_keepalive(),
                worker_threads: 0,
            },
            storage: StorageConfig::default(),
            persistence: PersistenceConfig {
                enabled: true,
                aof_path: default_aof_path(),
                fsync_policy: default_fsync_policy(),
                snapshot_enabled: true,
                snapshot_interval: default_snapshot_interval(),
                snapshot_dir: default_snapshot_dir(),
            },
            observability: ObservabilityConfig {
                metrics_enabled: true,
                metrics_addr: default_metrics_addr(),
                health_check_enabled: true,
                health_check_addr: default_health_addr(),
                log_level: default_log_level(),
                log_format: default_log_format(),
            },
            security: SecurityConfig::default(),
        }
    }
}

impl BlazeServerConfig {
    /// Load config from JSON file
    pub fn from_json_file<P: AsRef<std::path::Path>>(path: P) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path)?;
        let config: BlazeServerConfig = serde_json::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    /// Load config from TOML file
    pub fn from_toml_file<P: AsRef<std::path::Path>>(path: P) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path)?;
        let config: BlazeServerConfig = toml::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    /// Save config to JSON file
    pub fn to_json_file<P: AsRef<std::path::Path>>(&self, path: P) -> Result<(), ConfigError> {
        let content = serde_json::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    /// Save config to TOML file
    pub fn to_toml_file<P: AsRef<std::path::Path>>(&self, path: P) -> Result<(), ConfigError> {
        let content =
            toml::to_string_pretty(self).map_err(|e| ConfigError::Validation(e.to_string()))?;
        std::fs::write(path, content)?;
        Ok(())
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), ConfigError> {
        // Validate server config
        if self.server.max_connections == 0 {
            return Err(ConfigError::Validation(
                "max_connections must be > 0".to_string(),
            ));
        }

        if self.server.connection_timeout == 0 {
            return Err(ConfigError::Validation(
                "connection_timeout must be > 0".to_string(),
            ));
        }

        // Validate persistence config
        if self.persistence.enabled && self.persistence.aof_path.as_os_str().is_empty() {
            return Err(ConfigError::Validation(
                "aof_path required when persistence enabled".to_string(),
            ));
        }

        // Validate storage config
        if self.storage.max_memory == 0 {
            return Err(ConfigError::Validation(
                "max_memory must be > 0".to_string(),
            ));
        }

        if self.storage.shard_count == 0 {
            return Err(ConfigError::Validation(
                "shard_count must be > 0".to_string(),
            ));
        }

        // Validate TLS config
        if self.security.tls_enabled {
            if self.security.tls_cert_path.is_none() {
                return Err(ConfigError::Validation(
                    "tls_cert_path required when TLS enabled".to_string(),
                ));
            }
            if self.security.tls_key_path.is_none() {
                return Err(ConfigError::Validation(
                    "tls_key_path required when TLS enabled".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Generate example config file
    pub fn example() -> Self {
        Self::default()
    }

    /// Load from environment variables (override config file)
    pub fn from_env(&mut self) {
        use std::env;

        // Server overrides
        if let Ok(addr) = env::var("KVSTORE_BIND_ADDR") {
            if let Ok(parsed) = addr.parse() {
                self.server.bind_addr = parsed;
            }
        }

        if let Ok(timeout) = env::var("KVSTORE_CONNECTION_TIMEOUT") {
            if let Ok(parsed) = timeout.parse() {
                self.server.connection_timeout = parsed;
            }
        }

        if let Ok(max_conn) = env::var("KVSTORE_MAX_CONNECTIONS") {
            if let Ok(parsed) = max_conn.parse() {
                self.server.max_connections = parsed;
            }
        }

        // Persistence overrides
        if let Ok(aof_path) = env::var("KVSTORE_AOF_PATH") {
            self.persistence.aof_path = PathBuf::from(aof_path);
        }

        if let Ok(enabled) = env::var("KVSTORE_PERSISTENCE_ENABLED") {
            self.persistence.enabled = enabled.to_lowercase() == "true";
        }

        // Logging overrides
        if let Ok(log_level) = env::var("KVSTORE_LOG_LEVEL") {
            self.observability.log_level = log_level;
        }

        // Security overrides
        if let Ok(password) = env::var("KVSTORE_AUTH_PASSWORD") {
            self.security.auth_password = Some(password);
            self.security.require_auth = true;
        }
    }
}
