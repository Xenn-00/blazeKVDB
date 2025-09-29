use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    commands::{CommandError, CommandHandler, CommandResponse},
    storage::StorageEngine,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PingCommand;

#[async_trait]
impl CommandHandler for PingCommand {
    async fn execute(&self, storage: &dyn StorageEngine) -> CommandResponse {
        match storage.health_check().await {
            Ok(_) => CommandResponse::Pong,
            Err(e) => CommandResponse::Error(e.to_string()),
        }
    }

    fn name(&self) -> &'static str {
        "PING"
    }

    fn validate(&self) -> Result<(), CommandError> {
        Ok(())
    }
    fn is_read_only(&self) -> bool {
        true
    }
}
