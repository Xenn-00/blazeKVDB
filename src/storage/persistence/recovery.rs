use std::collections::HashMap;

use chrono::{DateTime, Utc};
use tracing::{error, info, instrument, warn};

use crate::storage::{
    StorageEngine, StorageResult,
    persistence::{
        aof::{AppendOnlyFile, Operation},
        snapshot::Snapshotter,
    },
};

// Recovery manager handles database recovery from persistence
pub struct RecoveryManager {
    aof: Option<AppendOnlyFile>,
    snapshotter: Option<Snapshotter>,
}

#[derive(Debug, Default, Clone)]
pub struct RecoveryStats {
    pub snapshot_loaded: bool,
    pub snapshot_timestamp: Option<DateTime<Utc>>,
    pub keys_from_snapshot: usize,
    pub aof_operations_total: usize,
    pub aof_operations_replayed: usize,
    pub final_key_count: usize,
}

impl RecoveryManager {
    pub fn new(aof: Option<AppendOnlyFile>, snapshotter: Option<Snapshotter>) -> Self {
        Self { aof, snapshotter }
    }

    // Recover database state
    // Strategy: Load snapshot (if exists) + replay AOF from snapshot timestamp
    #[instrument(skip(self, storage))]
    pub async fn recover(&self, storage: &dyn StorageEngine) -> StorageResult<RecoveryStats> {
        info!("Starting database recovery...");

        let mut stats = RecoveryStats::default();

        // Step 1: Load snapshot if available
        if let Some(ref snapshotter) = self.snapshotter {
            info!("Snapshotter is available, attempting to load...");

            match snapshotter.load_latest_snapshot().await {
                Ok(Some(snapshot)) => {
                    info!(
                        "Restoring from snapshot: {} keys",
                        snapshot.metadata.total_keys
                    );

                    let snapshot_timestamp = snapshot.metadata.timestamp;
                    stats.snapshot_loaded = true;
                    stats.keys_from_snapshot = snapshot.data.len();

                    // Restore data from snapshot
                    for (key, value) in snapshot.data {
                        storage.set(&key, value).await?;
                    }
                    info!("Snapshot restored: {} keys", stats.keys_from_snapshot);
                    stats.snapshot_timestamp = Some(snapshot_timestamp);
                }
                Ok(None) => {
                    info!("⚠️ No snapshot found, will replay full AOF");
                }
                Err(e) => {
                    error!("❌ Failed to load snapshot: {:?}", e);
                    return Err(e);
                }
            }
        } else {
            info!("⚠️ No snapshotter configured")
        }
        // Step 2: Replay AOF operations (from after snapshot timestamp)
        if let Some(ref aof) = self.aof {
            info!("Replaying AOF operations...");

            let operations = aof.read_operations().await?;
            stats.aof_operations_total = operations.len();

            for operation in operations {
                match operation {
                    Operation::Put { key, value } => {
                        storage.set(&key, value).await?;
                        stats.aof_operations_replayed += 1;
                    }
                    Operation::Delete { key } => {
                        storage.delete(&key).await?;
                        stats.aof_operations_replayed += 1
                    }
                }
            }

            info!(
                "AOF replay complete: {} operations",
                stats.aof_operations_replayed
            );
        }
        // Final stats
        let final_stats = storage.stats().await?;
        stats.final_key_count = final_stats.total_keys;

        info!("Recovery complete: {} total keys", stats.final_key_count);

        Ok(stats)
    }

    // Create snapshot from current state
    pub async fn create_snapshot(&self, storage: &dyn StorageEngine) -> StorageResult<()> {
        if let Some(ref snapshotter) = self.snapshotter {
            info!("Creating manual snapshot...");

            // Get all data from storage
            let all_keys = storage.scan("").await?;
            let mut data = HashMap::new();

            for key in all_keys {
                if let Some(value) = storage.get(&key).await? {
                    data.insert(key, value);
                }
            }

            snapshotter.create_snapshot(data).await?;

            info!("Manual snapshot created successfully");
        } else {
            warn!("Snapshot not enabled");
        }

        Ok(())
    }
}

impl RecoveryStats {
    pub fn print_summary(&self) {
        info!("📊 Recovery Statistics: ");
        if self.snapshot_loaded {
            info!("  Snapshot:");
            info!("    • Loaded: Yes");
            if let Some(ts) = self.snapshot_timestamp {
                info!("    • Timestamp: {}", ts);
            }
            info!("    • Keys restored: {}", self.keys_from_snapshot);
        } else {
            info!("  Snapshot: Not loaded");
        }

        info!("  AOF:");
        info!("    • Total operations: {}", self.aof_operations_total);
        info!("    • Replayed: {}", self.aof_operations_replayed);

        info!("  Final state:");
        info!("    • Total keys: {}", self.final_key_count);
    }
}
