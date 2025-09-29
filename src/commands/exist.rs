use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use crate::{
    commands::{CommandError, CommandHandler, CommandResponse},
    storage::StorageEngine,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExistCommand {
    pub key: String,
}

impl ExistCommand {
    pub fn new(key: String) -> Self {
        Self { key }
    }
}

#[async_trait]
impl CommandHandler for ExistCommand {
    #[instrument(skip(self, storage), fields(key = %self.key))]
    async fn execute(&self, storage: &dyn StorageEngine) -> CommandResponse {
        debug!("Executing EXIST command");

        match storage.exists(&self.key).await {
            Ok(exist) => {
                debug!("exist operation completed, exitsted: {}", exist);
                CommandResponse::Bool(exist)
            }
            Err(e) => {
                debug!("Failed to exist key: {}", e);
                CommandResponse::Error(e.to_string())
            }
        }
    }

    fn name(&self) -> &'static str {
        "EXIST"
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
