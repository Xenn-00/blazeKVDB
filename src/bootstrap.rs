use std::sync::Arc;

use tracing::{debug, info, instrument};

use crate::{
    commands::{Command, CommandDispatcher, CommandResponse},
    config::BlazeServerConfig,
    storage::{
        StorageEngine, StorageResult,
        engine::memory::MemoryEngine,
        persistence::{
            aof::Operation,
            manager::{PersistenceManager, PersistenceStats},
        },
    },
};

pub struct BlazeKVDB {
    storage: Arc<dyn StorageEngine>,
    persistence: Option<Arc<PersistenceManager>>,
    dispatcher: Arc<CommandDispatcher>,
}

impl BlazeKVDB {
    /// Create new KV store with configuration
    #[instrument(skip(config))]
    pub async fn new(config: BlazeServerConfig) -> StorageResult<Self> {
        info!("Initializing KV Store...");

        // 1. Initialize storage engine
        info!("Creating storage engine...");
        let storage = Arc::new(MemoryEngine::new(config.storage.clone())) as Arc<dyn StorageEngine>;

        // 2. Initialize persistence (if enabled)
        let persistence = if config.persistence.enabled {
            info!("Initializing persistence layer...");

            let persistence = Arc::new(
                PersistenceManager::new(config.persistence.clone(), storage.clone()).await?,
            );

            // 3. Recover from persistence
            info!("Recovering database state...");
            persistence.recover().await?;

            Some(persistence)
        } else {
            info!("Persistence disabled");
            None
        };

        // 4. Initialize command dispatcher
        let dispatcher = Arc::new(CommandDispatcher::new(storage.clone()));

        let store = Self {
            storage,
            persistence,
            dispatcher,
        };

        // 5. Start background tasks
        if let Some(ref persistence) = store.persistence {
            info!("Starting background snapshot task...");
            persistence.clone().start_background_snapshots();
        }

        info!("✅ KV Store initialized successfully");

        Ok(store)
    }

    /// Execute command with persistence
    #[instrument(skip(self))]
    pub async fn execute(&self, command: Command) -> CommandResponse {
        debug!("Executing command: {:?}", command);

        // Log write operations to AOF before execution
        if let Some(ref persistence) = self.persistence {
            match &command {
                Command::Set(cmd) => {
                    if let Err(e) = persistence
                        .log_operation(Operation::Put {
                            key: cmd.key.clone(),
                            value: cmd.value.clone(),
                        })
                        .await
                    {
                        return CommandResponse::Error(format!("Persistence error: {}", e));
                    }
                }
                Command::Delete(cmd) => {
                    if let Err(e) = persistence
                        .log_operation(Operation::Delete {
                            key: cmd.key.clone(),
                        })
                        .await
                    {
                        return CommandResponse::Error(format!("Persistence error: {}", e));
                    }
                }
                _ => {} // Read-only commands don't need persistence
            }
        }

        // Execute command
        self.dispatcher.execute(command).await
    }

    /// Create manual snapshot
    pub async fn snapshot(&self) -> StorageResult<()> {
        if let Some(ref persistence) = self.persistence {
            persistence.create_snapshot().await
        } else {
            Err(crate::storage::StorageError::Persistence(
                "Persistence not enabled".to_string(),
            ))
        }
    }

    /// Get storage statistics
    pub async fn storage_stats(&self) -> StorageResult<crate::storage::StorageStats> {
        self.storage.stats().await
    }

    /// Get persistence statistics
    pub async fn persistence_stats(&self) -> Option<PersistenceStats> {
        if let Some(ref persistence) = self.persistence {
            Some(persistence.stats().await)
        } else {
            None
        }
    }

    /// Get command dispatcher (for server integration)
    pub fn dispatcher(&self) -> Arc<CommandDispatcher> {
        self.dispatcher.clone()
    }

    /// Get storage engine (for direct access if needed)
    pub fn storage(&self) -> Arc<dyn StorageEngine> {
        self.storage.clone()
    }
}
