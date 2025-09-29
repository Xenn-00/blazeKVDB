use std::u64;

use thiserror::Error;

pub mod engine;
pub mod persistence;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Key not found: {key}")]
    KeyNotFound { key: String },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::error::EncodeError),

    #[error("Deserialization error: {0}")]
    Deserialization(#[from] bincode::error::DecodeError),

    #[error("Persistence error: {0}")]
    Persistence(String),
}

pub type StorageResult<T> = Result<T, StorageError>;

#[async_trait::async_trait]
pub trait StorageEngine: Send + Sync {
    // Get value by key
    async fn get(&self, key: &str) -> StorageResult<Option<Vec<u8>>>;

    // Set key-value pair
    async fn set(&self, key: &str, value: Vec<u8>) -> StorageResult<()>;

    // Delete key-value pair
    async fn delete(&self, key: &str) -> StorageResult<bool>;

    // Check if key exists
    async fn exists(&self, key: &str) -> StorageResult<bool>;

    // Get all keys with prefix
    async fn scan(&self, prefix: &str) -> StorageResult<Vec<String>>;

    // Get storage statistics
    async fn stats(&self) -> StorageResult<StorageStats>;

    // Health check
    async fn health_check(&self) -> StorageResult<()>;
}

// Storage statistics structure
#[derive(Debug, Clone)]
pub struct StorageStats {
    pub total_keys: usize,
    pub memory_usage: usize,
    pub hit_rate: f64,
    pub total_operations: u64,
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub max_memory: usize,         // Max memory usage in bytes
    pub persistence_enabled: bool, // Enable AOF logging
    pub aof_path: String,          // AOF file path
    pub snapshot_interval: u64,    // Snapshot interval in seconds
    pub shard_count: usize,        // Number of shards for HashMap}
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            max_memory: 1024 * 1024 * 100, // 100 MB
            persistence_enabled: true,
            aof_path: "resplite.aof".to_string(),
            snapshot_interval: 3600, // 5 minutes
            shard_count: 16,         // 16 shards
        }
    }
}
