use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::sync::RwLock;
use tracing::{error, info, instrument};

use crate::{
    config::{FsyncPolicy, PersistenceConfig},
    storage::{
        StorageEngine, StorageError, StorageResult,
        persistence::{
            aof::{AppendOnlyFile, Operation},
            recovery::{RecoveryManager, RecoveryStats},
            snapshot::Snapshotter,
        },
    },
};

// Manages all persistence operations (AOF + Snapshots)
pub struct PersistenceManager {
    pub aof: Option<Arc<RwLock<AppendOnlyFile>>>,
    pub snapshotter: Option<Snapshotter>,
    config: PersistenceConfig,
    storage: Arc<dyn StorageEngine>,
}

/// Persistence statistics
#[derive(Debug)]
pub struct PersistenceStats {
    pub aof_enabled: bool,
    pub aof_stats: Option<super::aof::AofStats>,
    pub snapshot_enabled: bool,
    pub snapshot_count: usize,
}

impl PersistenceManager {
    // Create new persistence manager
    pub async fn new(
        config: PersistenceConfig,
        storage: Arc<dyn StorageEngine>,
    ) -> StorageResult<Self> {
        info!("Initializing persistence manager...");

        // Initialize AOF if enabled
        let aof = if config.enabled {
            info!("Initializimng AOF at: {}", config.aof_path.display());

            let mut aof = AppendOnlyFile::new(&config.aof_path).await?;

            // Set Fsync policy
            aof.fsync_every = match config.fsync_policy {
                FsyncPolicy::Always => 0,
                FsyncPolicy::EveryN(n) => n,
                FsyncPolicy::Never => u64::MAX,
            };

            // Start background writer
            aof.start_background_writer().await;

            Some(Arc::new(RwLock::new(aof)))
        } else {
            info!("AOF disabled");
            None
        };

        // Initializer snapshotter if enabled
        let snapshotter = if config.snapshot_enabled {
            info!(
                "Initializing snapshotter at: {}",
                config.snapshot_dir.display()
            );
            let snapshotter = Snapshotter::new(&config.snapshot_dir)?;
            Some(snapshotter)
        } else {
            info!("Snapshots disabled");
            None
        };

        Ok(Self {
            aof,
            snapshotter,
            config,
            storage,
        })
    }

    // Recover database from persistence
    #[instrument(skip(self))]
    pub async fn recover(&self) -> StorageResult<RecoveryStats> {
        info!("Starting database recovery...");

        let aof_for_recovery = if let Some(ref _aof_lock) = self.aof {
            // Create new AOF instance for reading (don't interfere with writer)
            let aof_path = &self.config.aof_path;

            if aof_path.exists() {
                Some(AppendOnlyFile::new(aof_path).await?)
            } else {
                None
            }
        } else {
            None
        };

        let recovery_manager = RecoveryManager::new(aof_for_recovery, self.snapshotter.clone());

        let stats = recovery_manager.recover(self.storage.as_ref()).await?;

        stats.print_summary();

        Ok(stats)
    }

    // Log operation to AOF
    #[instrument(skip(self))]
    pub async fn log_operation(&self, operation: Operation) -> StorageResult<()> {
        if let Some(ref aof) = self.aof {
            let aof = aof.read().await;
            aof.log_operation(operation).await?;
        }
        Ok(())
    }

    // Create snapshot manually
    #[instrument(skip(self))]
    pub async fn create_snapshot(&self) -> StorageResult<()> {
        if let Some(ref snapshotter) = self.snapshotter {
            info!("Creating manual snapshot...");

            // Get all data from storage
            let all_keys = self.storage.scan("").await?;
            let mut data = HashMap::new();

            for key in all_keys {
                if let Some(value) = self.storage.get(&key).await? {
                    data.insert(key, value);
                }
            }

            let snapshot_path = snapshotter.create_snapshot(data).await?;

            info!("Snapshot created: {}", snapshot_path.display());

            // Optionally compact AOF after snapshot
            if self.config.enabled {
                info!("Compacting AOF after snapshot...");
                self.compact_aof().await?;
            }

            Ok(())
        } else {
            Err(StorageError::Persistence(
                "Snapshots not enabled".to_string(),
            ))
        }
    }

    // Compact AOF (remove redundant operations)
    async fn compact_aof(&self) -> StorageResult<()> {
        if let Some(ref aof_lock) = self.aof {
            let all_keys = self.storage.scan("").await?;
            let mut current_state = Vec::new();

            for key in all_keys {
                if let Some(value) = self.storage.get(&key).await? {
                    current_state.push((key, value));
                }
            }

            let mut aof = aof_lock.write().await;
            aof.compact(current_state.into_iter()).await?;

            info!("AOF compaction completed");
        }

        Ok(())
    }

    /// Start background snapshot task
    pub fn start_background_snapshots(self: Arc<Self>) {
        if !self.config.snapshot_enabled {
            return;
        }

        let interval = Duration::from_secs(self.config.snapshot_interval);

        info!(
            "Starting background snapshot task (interval: {:?})",
            interval
        );

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);

            loop {
                interval_timer.tick().await;

                info!("Background snapshot triggered");

                match self.create_snapshot().await {
                    Ok(_) => {
                        info!("Background snapshot completed successfully");
                    }
                    Err(e) => {
                        error!("Background snapshot failed: {}", e);
                    }
                }
            }
        });
    }

    /// Get persistence statistics
    pub async fn stats(&self) -> PersistenceStats {
        let aof_stats = if let Some(ref aof) = self.aof {
            let aof = aof.read().await;
            Some(aof.stats())
        } else {
            None
        };

        let snapshot_count = if let Some(ref snapshotter) = self.snapshotter {
            snapshotter
                .list_snapshots()
                .await
                .ok()
                .map(|s| s.len())
                .unwrap_or(0)
        } else {
            0
        };

        PersistenceStats {
            aof_enabled: self.aof.is_some(),
            aof_stats,
            snapshot_enabled: self.snapshotter.is_some(),
            snapshot_count,
        }
    }
}
