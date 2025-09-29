use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use crate::{
    commands::{CommandError, CommandHandler, CommandResponse},
    storage::StorageEngine,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetCommand {
    pub key: String,
}

impl GetCommand {
    pub fn new(key: String) -> Self {
        Self { key }
    }
}

#[async_trait]
impl CommandHandler for GetCommand {
    #[instrument(skip(self, storage), fields(key = %self.key))]
    async fn execute(&self, storage: &dyn StorageEngine) -> CommandResponse {
        debug!("Executing GET command");

        match storage.get(&self.key).await {
            Ok(Some(value)) => {
                debug!("Key found, returning value of {} bytes", value.len());
                CommandResponse::Value(value)
            }
            Ok(None) => {
                debug!("Key not found");
                CommandResponse::Error("Key not found".to_string())
            }
            Err(e) => {
                debug!("Storage error: {}", e);
                CommandResponse::Error(e.to_string())
            }
        }
    }

    fn name(&self) -> &'static str {
        "GET"
    }

    fn validate(&self) -> Result<(), CommandError> {
        if self.key.is_empty() {
            return Err(CommandError::InvalidParameter(
                "Key cannot be empty".to_string(),
            ));
        }

        if self.key.len() > 512 {
            return Err(CommandError::InvalidParameter(
                "Key too long (max 512 bytes)".to_string(),
            ));
        }
        Ok(())
    }

    fn is_read_only(&self) -> bool {
        true
    }
}
