use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use parking_lot::RwLock;
use tracing::{debug, info, instrument};

use crate::storage::{StorageConfig, StorageEngine, StorageError, StorageResult, StorageStats};

// Shard for reduce lock contention
// why using shard rwlock? because this is easier to implement, predictable perf, lock-free between shards (diferrent shards = zero contention)
// but the tradeoff is still blocking within shard, uneven distribution of keys - some shards might be hotter, expensive range ops - must check all shards
#[derive(Debug)]
struct Shard {
    data: RwLock<HashMap<String, Vec<u8>>>,
    size: AtomicUsize, // Track memory usage per shard
}

impl Shard {
    fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            size: AtomicUsize::new(0),
        }
    }

    fn estimate_size(key: &str, value: &[u8]) -> usize {
        key.len() + value.len() + 64 // rough overhead estimate
    }
}

// High-performance in-memory storage engine
// Uses sharding RwLock HashMap to reduce contention
pub struct MemoryEngine {
    shards: Vec<Arc<Shard>>,
    shard_count: usize,
    config: StorageConfig,

    // metrics
    pub total_operations: AtomicU64,
    pub hit_count: AtomicU64,
    pub miss_count: AtomicU64,

    // memory tracking
    pub total_memory: AtomicUsize,
}

impl MemoryEngine {
    // Create new memory engine with configuration
    pub fn new(config: StorageConfig) -> Self {
        let shard_count = config.shard_count;
        let mut shards = Vec::with_capacity(shard_count);

        for _ in 0..shard_count {
            shards.push(Arc::new(Shard::new()));
        }

        info!("MemoryEngine initialized with {} shards", shard_count);

        Self {
            shards,
            shard_count,
            config,
            total_operations: AtomicU64::new(0),
            hit_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
            total_memory: AtomicUsize::new(0),
        }
    }

    // Get shard index for a key using hash
    fn get_shard_index(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shard_count
    }

    // Get shard by key
    fn get_shard(&self, key: &str) -> &Arc<Shard> {
        let index = self.get_shard_index(key);
        &self.shards[index]
    }

    // Check memory limits
    fn check_memory_limit(&self, additional_size: usize) -> StorageResult<()> {
        let current = self.total_memory.load(Ordering::Relaxed);
        if current + additional_size > self.config.max_memory {
            return Err(StorageError::Persistence(format!(
                "Memory limit exceeded: {} + {} > {}",
                current, additional_size, self.config.max_memory
            )));
        }
        Ok(())
    }

    // Update memory tracking
    fn update_memory(&self, delta: isize) {
        if delta > 0 {
            self.total_memory
                .fetch_add(delta as usize, Ordering::Relaxed);
        } else {
            self.total_memory
                .fetch_sub((-delta) as usize, Ordering::Relaxed);
        }
    }
}

#[async_trait::async_trait]
impl StorageEngine for MemoryEngine {
    #[instrument(skip(self), fields(key = %key))]
    async fn get(&self, key: &str) -> StorageResult<Option<Vec<u8>>> {
        debug!("getting key from memory engine");

        self.total_operations.fetch_add(1, Ordering::Relaxed);

        let shard = self.get_shard(key);
        let guard = shard.data.read();

        match guard.get(key) {
            Some(value) => {
                self.hit_count.fetch_add(1, Ordering::Relaxed);
                debug!("Key found in memory");
                Ok(Some(value.clone()))
            }
            None => {
                self.miss_count.fetch_add(1, Ordering::Relaxed);
                debug!("Key not found in memory");
                Ok(None)
            }
        }
    }

    #[instrument(skip(self, value), fields(key = %key, size = value.len()))]
    async fn set(&self, key: &str, value: Vec<u8>) -> StorageResult<()> {
        debug!("Setting key in memory engine");

        let size = Shard::estimate_size(key, &value);

        // Check memory limit before allocating
        self.check_memory_limit(size)?;

        let shard = self.get_shard(key);
        let mut guard = shard.data.write();

        // Check if key exists (for memory tracking)
        let old_size = if let Some(old_value) = guard.get(key) {
            Shard::estimate_size(key, old_value)
        } else {
            0
        };

        // Insert new value
        guard.insert(key.to_string(), value);

        // Update memory tracking
        let memory_delta = size as isize - old_size as isize;
        self.update_memory(memory_delta);
        shard.size.fetch_add(size, Ordering::Relaxed);
        if old_size > 0 {
            shard.size.fetch_sub(old_size, Ordering::Relaxed);
        }

        self.total_operations.fetch_add(1, Ordering::Relaxed);
        debug!("Key stored in memory, memory delta: {}", memory_delta);

        Ok(())
    }

    #[instrument(skip(self), fields(key = %key))]
    async fn delete(&self, key: &str) -> StorageResult<bool> {
        debug!("Deleting key from memory engine");

        let shard = self.get_shard(key);
        let mut guard = shard.data.write();

        match guard.remove(key) {
            Some(old_value) => {
                let size = Shard::estimate_size(key, &old_value);
                self.update_memory(-(size as isize));
                shard.size.fetch_sub(size, Ordering::Relaxed);

                self.total_operations.fetch_add(1, Ordering::Relaxed);
                debug!("Key deleted from memory");
                Ok(true)
            }
            None => {
                debug!("Key not found in memory");
                Ok(false)
            }
        }
    }

    async fn exists(&self, key: &str) -> StorageResult<bool> {
        let shard = self.get_shard(key);
        let guard = shard.data.read();
        Ok(guard.contains_key(key))
    }

    #[instrument(skip(self), fields(prefix = %prefix))]
    async fn scan(&self, prefix: &str) -> StorageResult<Vec<String>> {
        debug!("Scanning keys with prefix");

        let mut results = Vec::new();

        // Scan all shards
        for shard in &self.shards {
            let guard = shard.data.read();
            for key in guard.keys() {
                if key.starts_with(prefix) {
                    results.push(key.clone());
                }
            }
        }

        debug!("Scan found {} keys with prefix '{}'", results.len(), prefix);
        Ok(results)
    }

    async fn stats(&self) -> StorageResult<StorageStats> {
        let mut total_keys = 0;

        // Count keys across all shards
        for shard in &self.shards {
            let guard = shard.data.read();
            total_keys += guard.len();
        }

        let total_ops = self.total_operations.load(Ordering::Relaxed);
        let hits = self.hit_count.load(Ordering::Relaxed);
        let misses = self.miss_count.load(Ordering::Relaxed);

        // Hit rate should only consider hits and misses
        // because other operations (set, delete) do not contribute to hit/miss
        let total_gets = hits + misses;
        let hit_rate = if total_gets > 0 {
            hits as f64 / total_gets as f64
        } else {
            0.0
        };

        Ok(StorageStats {
            total_keys,
            memory_usage: self.total_memory.load(Ordering::Relaxed),
            hit_rate,
            total_operations: total_ops,
        })
    }

    async fn health_check(&self) -> StorageResult<()> {
        // Simple health check - try to access first shard
        let _guard = self.shards[0].data.read();
        Ok(())
    }
}
