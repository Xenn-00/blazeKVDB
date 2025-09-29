use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use crate::{
    commands::{CommandError, CommandHandler, CommandResponse},
    storage::StorageEngine,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteCommand {
    pub key: String,
}

impl DeleteCommand {
    pub fn new(key: String) -> Self {
        Self { key }
    }
}

#[async_trait]
impl CommandHandler for DeleteCommand {
    #[instrument(skip(self, storage), fields(key = %self.key))]
    async fn execute(&self, storage: &dyn StorageEngine) -> CommandResponse {
        debug!("Executing DELETE command");

        match storage.delete(&self.key).await {
            Ok(deleted) => {
                debug!("Delete operation completed, deleted: {}", deleted);
                CommandResponse::Bool(deleted)
            }
            Err(e) => {
                debug!("Failed to delete key: {}", e);
                CommandResponse::Error(e.to_string())
            }
        }
    }

    fn name(&self) -> &'static str {
        "DELETE"
    }

    fn validate(&self) -> Result<(), CommandError> {
        if self.key.is_empty() {
            return Err(CommandError::MissingParameter(
                "Key cannot be empty".to_string(),
            ));
        }

        Ok(())
    }

    fn is_read_only(&self) -> bool {
        false
    }
}
