use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};
use tracing::{debug, error, info, instrument, warn};

use crate::storage::{StorageError, StorageResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub version: String,
    pub timestamp: DateTime<Utc>,
    pub total_keys: usize,
    pub total_size: usize,
    pub checksum: Option<String>,
}

// Complete database snapshot
#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub metadata: SnapshotMetadata,
    pub data: HashMap<String, Vec<u8>>,
}

impl Snapshot {
    pub fn new(data: HashMap<String, Vec<u8>>) -> Self {
        let total_size: usize = data.iter().map(|(k, v)| k.len() + v.len()).sum();

        Self {
            metadata: SnapshotMetadata {
                version: env!("CARGO_PKG_VERSION").to_string(),
                timestamp: Utc::now(),
                total_keys: data.len(),
                total_size,
                checksum: None, // TODO: Implement checksum
            },
            data,
        }
    }
}

// Manage snapshot creation and loading
#[derive(Debug, Clone)]
pub struct Snapshotter {
    snapshot_dir: PathBuf,
}

impl Snapshotter {
    // Create new snapshotter
    pub fn new<P: AsRef<Path>>(snapshot_dir: P) -> StorageResult<Self> {
        let snapshot_dir = snapshot_dir.as_ref().to_path_buf();

        // Create directory if not exists
        std::fs::create_dir_all(&snapshot_dir)?;

        info!("Snapshotter initialized at: {}", snapshot_dir.display());

        Ok(Self { snapshot_dir })
    }

    // Create snapshot from current data
    #[instrument(skip(self, data))]
    pub async fn create_snapshot(&self, data: HashMap<String, Vec<u8>>) -> StorageResult<PathBuf> {
        let snapshot = Snapshot::new(data);

        info!(
            "Creating snapshot: {} keys, {} bytes",
            snapshot.metadata.total_keys, snapshot.metadata.total_size
        );

        // Generate filename with timestamp
        let timestamp = snapshot.metadata.timestamp.format("%Y%m%d-%H%M%S");
        let filename = format!("snapshot-{}.rdb", timestamp);
        let filepath = self.snapshot_dir.join(&filename);

        // Serialize snapshot using the serde adapter so serde::Serialize is sufficient
        let serialized = bincode::serde::encode_to_vec(&snapshot, bincode::config::standard())
            .map_err(|e| StorageError::Serialization(e))?;

        debug!("Snapshot serialized: {} bytes", serialized.len());

        // Write to temporary file first (atomic write)
        let temp_path = filepath.with_extension("tmp");
        let mut file = File::create(&temp_path).await?;
        file.write_all(&serialized).await?;
        file.sync_all().await?;

        // Rename to final filename (atomic operation)
        tokio::fs::rename(&temp_path, &filepath).await?;

        info!("Snapshot create successfully: {}", filepath.display());

        // Update symlink to latest snapshot
        self.update_latest_symlink(&filepath).await?;
        // Cleanup old snapshots
        self.cleanup_old_snapshots().await?;

        Ok(filepath)
    }

    // Update symlink to latest snapshot
    async fn update_latest_symlink(&self, filepath: &Path) -> StorageResult<()> {
        let latest_path = self.snapshot_dir.join("snapshot-latest.rdb");

        // Remove old symlink if exists
        if tokio::fs::try_exists(&latest_path).await.unwrap_or(false) {
            tokio::fs::remove_file(&latest_path).await?;
        }
        // Copy instead of symlink (works on all platforms)
        tokio::fs::copy(filepath, &latest_path).await?;

        debug!("Updated latest snapshot link");

        Ok(())
    }

    // Load latest snapshot
    #[instrument(skip(self))]
    pub async fn load_latest_snapshot(&self) -> StorageResult<Option<Snapshot>> {
        let latest_path = self.snapshot_dir.join("snapshot-latest.rdb");

        if !latest_path.exists() {
            info!("No snapshot found");
            return Ok(None);
        }

        match self.load_snapshot(&latest_path).await {
            Ok(snapshot) => Ok(Some(snapshot)),
            Err(e) => {
                error!("Failed to load snapshot: {:?}", e);
                Err(e)
            }
        }
    }

    // Load specific snapshot
    #[instrument(skip(self))]
    pub async fn load_snapshot<P: AsRef<Path> + std::fmt::Debug>(
        &self,
        path: P,
    ) -> StorageResult<Snapshot> {
        let path = path.as_ref();

        info!("Loading snapshot from: {}", path.display());

        let mut file = File::open(path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        debug!("Snapshot file read: {} bytes", buffer.len());

        let (snapshot, _) =
            bincode::serde::decode_from_slice::<Snapshot, _>(&buffer, bincode::config::standard())
                .map_err(|e| StorageError::Deserialization(e))?;

        info!(
            "Snapshot loaded: {} keys from {}",
            snapshot.metadata.total_keys, snapshot.metadata.timestamp
        );

        Ok(snapshot)
    }

    // Cleanup ol snapshots (keep last N)
    async fn cleanup_old_snapshots(&self) -> StorageResult<()> {
        const KEEP_SNAPSHOTS: usize = 5;

        let mut snapshots = self.list_snapshots().await?;

        if snapshots.len() <= KEEP_SNAPSHOTS {
            return Ok(());
        }

        // Sort by timestamp (oldest first)
        snapshots.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

        // Remove oldest snapshots
        let to_remove = snapshots.len() - KEEP_SNAPSHOTS;
        for i in 0..to_remove {
            let filename = format!(
                "snapshot-{}.rdb",
                snapshots[i].timestamp.format("%Y%m%d-%H%M%S")
            );
            let filepath = self.snapshot_dir.join(filename);

            if let Err(e) = tokio::fs::remove_file(&filepath).await {
                warn!(
                    "Failed to remove old snapshot {}: {}",
                    filepath.display(),
                    e
                );
            } else {
                debug!("Removed old snapshot: {}", filepath.display());
            }
        }

        Ok(())
    }

    // List all available snapshots
    pub async fn list_snapshots(&self) -> StorageResult<Vec<SnapshotMetadata>> {
        let mut entries = tokio::fs::read_dir(&self.snapshot_dir).await?;
        let mut snapshots = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("rdb")
                && path.file_name().and_then(|s| s.to_str()) != Some("snapshot-latest.rdb")
            {
                if let Ok(snapshot) = self.load_snapshot(&path).await {
                    snapshots.push(snapshot.metadata);
                }
            }
        }

        Ok(snapshots)
    }
}
