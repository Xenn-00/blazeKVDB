use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{
    commands::{CommandError, CommandHandler, CommandResponse},
    storage::StorageEngine,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatsCommand;

#[async_trait]
impl CommandHandler for StatsCommand {
    async fn execute(&self, storage: &dyn StorageEngine) -> CommandResponse {
        match storage.stats().await {
            Ok(stats) => {
                debug!("Stats retrieved successfully");
                CommandResponse::Stats {
                    total_keys: stats.total_keys,
                    memory_usage: stats.memory_usage,
                    hit_rate: stats.hit_rate,
                    total_operations: stats.total_operations,
                }
            }
            Err(e) => {
                debug!("Failed to get stats: {}", e);
                CommandResponse::Error(e.to_string())
            }
        }
    }

    fn name(&self) -> &'static str {
        "STATS"
    }

    fn validate(&self) -> Result<(), CommandError> {
        Ok(())
    }
    fn is_read_only(&self) -> bool {
        true
    }
}
