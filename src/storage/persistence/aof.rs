use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use flume::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
};
use tracing::{debug, info, instrument, warn};

use crate::storage::{StorageError, StorageResult};

// Operations that can be logged to AOF (Append-Only File)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operation {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
    // Future: Expire, Increment, etc.
}

impl Operation {
    // Serialize operation to AOF format
    pub fn to_aof_entry(&self) -> StorageResult<String> {
        match self {
            Operation::Put { key, value } => {
                // Format: Set key value_base64
                let value_b64 =
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, value);
                Ok(format!("SET {} {}\n", key, value_b64))
            }

            Operation::Delete { key } => {
                // Format: Del key
                Ok(format!("DEL {}\n", key))
            }
        }
    }

    // Parse AOF entry back to operation
    pub fn from_aof_entry(line: &str) -> StorageResult<Self> {
        let parts: Vec<&str> = line.trim().split(' ').collect();

        match parts.get(0) {
            Some(&"SET") if parts.len() >= 3 => {
                let key = parts[1].to_string();
                let value_b64 = parts[2..].join(" "); // handle spaces in value
                let value =
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &value_b64)
                        .map_err(|e| {
                            StorageError::Persistence(format!("Base64 decode error: {}", e))
                        })?;
                Ok(Operation::Put { key, value })
            }
            Some(&"DEL") if parts.len() == 2 => {
                let key = parts[1].to_string();
                Ok(Operation::Delete { key })
            }
            _ => Err(StorageError::Persistence(format!(
                "Invalid AOF entry: {}",
                line
            ))),
        }
    }
}

// Append-Only File for persistence
// Logs all write operations for crash recovery
pub struct AppendOnlyFile {
    writer: Option<BufWriter<File>>,
    file_path: PathBuf,

    // background writer
    operation_tx: Sender<Operation>,
    operation_rx: Receiver<Operation>,

    // stats
    operation_logged: Arc<AtomicU64>,
    file_size: Arc<AtomicU64>,

    // config
    fsync_every: u64, // fsync after N operations (0 = every operation)
}

impl AppendOnlyFile {
    // Create new AOF instance
    pub async fn new<P: AsRef<Path>>(file_path: P) -> StorageResult<Self> {
        let file_path = file_path.as_ref().to_path_buf();

        // Create directory if not exists
        if let Some(parent) = file_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                StorageError::Persistence(format!(
                    "Failed to create AOF directory {:?}: {}",
                    parent, e
                ))
            })?;
        }

        // Create channel for background writing
        let (op_tx, op_rx) = flume::unbounded();

        info!("AOF initialized at {}", file_path.display());

        let mut aof = Self {
            writer: None,
            file_path,
            operation_rx: op_rx,
            operation_tx: op_tx,
            operation_logged: Arc::new(AtomicU64::new(0)),
            file_size: Arc::new(AtomicU64::new(0)),
            fsync_every: 1, // sync after every 1 operations by default
        };

        aof.open_writer().await?;
        Ok(aof)
    }

    // Open file writer
    async fn open_writer(&mut self) -> StorageResult<()> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.file_path)
            .await?;

        // Get current file size
        let metadata = file.metadata().await?;
        self.file_size.store(metadata.len(), Ordering::Relaxed);

        self.writer = Some(BufWriter::new(file));
        debug!("AOF writer opened, current size: {} bytes", metadata.len());

        Ok(())
    }

    #[instrument(skip(self), fields(op = ?operation))]
    pub async fn log_operation(&self, operation: Operation) -> StorageResult<()> {
        debug!("Queuing operation for AOF logging");

        self.operation_tx
            .send_async(operation)
            .await
            .map_err(|e| StorageError::Persistence(format!("Failed to queue operation: {}", e)))?;

        Ok(())
    }

    // Log operation sychronously (blocking)
    pub async fn log_operation_sync(&mut self, operation: Operation) -> StorageResult<()> {
        self.write_operation(&operation).await
    }

    // Write operation to disk
    async fn write_operation(&mut self, operation: &Operation) -> StorageResult<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| StorageError::Persistence("AOF writer not initialized".to_string()))?;

        let entry = operation.to_aof_entry()?;

        // Write to buffer
        writer.write_all(entry.as_bytes()).await?;

        // Fsync based on policy
        let ops_count = self.operation_logged.fetch_add(1, Ordering::Relaxed) + 1;
        if self.fsync_every == 0 || ops_count % self.fsync_every == 0 {
            writer.flush().await?;
            writer.get_mut().sync_all().await?;
            debug!("AOF fsynced after {} operations", ops_count);
        }

        self.file_size
            .fetch_add(entry.len() as u64, Ordering::Relaxed);

        Ok(())
    }

    // Start background writer task
    pub async fn start_background_writer(&mut self) {
        let mut writer = self.writer.take();
        let rx = self.operation_rx.clone();
        let file_size = self.file_size.clone();
        let operation_logged = self.operation_logged.clone();
        let fsync_every = self.fsync_every;

        tokio::spawn(async move {
            info!("AOF background writer started");

            while let Ok(operation) = rx.recv_async().await {
                if let Some(ref mut w) = writer {
                    if let Ok(entry) = operation.to_aof_entry() {
                        if let Err(e) = w.write_all(entry.as_bytes()).await {
                            warn!("Failed to write AOF entry: {}", e);
                            continue;
                        }

                        // Fsync policy
                        let ops_count = operation_logged.fetch_add(1, Ordering::Relaxed) + 1;
                        if fsync_every == 0 || ops_count % fsync_every == 0 {
                            if let Err(e) = w.flush().await {
                                warn!("Failed to flush AOF writer: {}", e);
                            }
                            if let Err(e) = w.get_mut().sync_all().await {
                                warn!("Failed to fsync AOF file: {}", e);
                            }
                        }

                        file_size.fetch_add(entry.len() as u64, Ordering::Relaxed);
                    }
                }
            }

            info!("AOF background writer stopped");
        });
    }

    // Read all operations from AOF file
    pub async fn read_operations(&self) -> StorageResult<Vec<Operation>> {
        let file = File::open(&self.file_path).await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut operations = Vec::new();

        while let Some(line) = lines.next_line().await? {
            if !line.trim().is_empty() {
                match Operation::from_aof_entry(&line) {
                    Ok(op) => operations.push(op),
                    Err(e) => {
                        warn!("Skipping invalid AOF entry: {}: {}", line, e);
                    }
                }
            }
        }

        info!("Read {} operations from AOF", operations.len());
        Ok(operations)
    }

    // Get AOF statistics
    pub fn stats(&self) -> AofStats {
        AofStats {
            operations_logged: self.operation_logged.load(Ordering::Relaxed),
            file_size_bytes: self.file_size.load(Ordering::Relaxed),
            file_path: self.file_path.clone(),
        }
    }

    // Compact AOF by rewriting with current state
    pub async fn compact(
        &mut self,
        current_keys: impl Iterator<Item = (String, Vec<u8>)>,
    ) -> StorageResult<()> {
        info!("Starting AOF compaction");

        let temp_path = self.file_path.with_extension("aof.tmp");
        let temp_file = File::create(&temp_path).await?;
        let mut temp_writer = BufWriter::new(temp_file);

        let mut compacted_opt = 0;

        // Write current state
        for (k, v) in current_keys {
            let op = Operation::Put { key: k, value: v };
            let entry = op.to_aof_entry()?;
            temp_writer.write_all(entry.as_bytes()).await?;
            compacted_opt += 1;
        }

        temp_writer.flush().await?;
        temp_writer.into_inner().sync_all().await?;

        // Replace old AOF with compacted
        tokio::fs::rename(&temp_path, &self.file_path).await?;

        self.open_writer().await?;
        info!(
            "AOF compaction completed, {} entries written",
            compacted_opt
        );
        Ok(())
    }
}

#[derive(Debug)]
pub struct AofStats {
    pub operations_logged: u64,
    pub file_size_bytes: u64,
    pub file_path: PathBuf,
}
