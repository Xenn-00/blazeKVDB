use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use crate::{
    commands::{CommandError, CommandHandler, CommandResponse},
    storage::StorageEngine,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScanCommand {
    pub prefix: String,
}

impl ScanCommand {
    pub fn new(prefix: String) -> Self {
        Self { prefix }
    }
}

#[async_trait]
impl CommandHandler for ScanCommand {
    #[instrument(skip(self, storage), fields(prefix = %self.prefix))]
    async fn execute(&self, storage: &dyn StorageEngine) -> CommandResponse {
        match storage.scan(&self.prefix).await {
            Ok(keys) => {
                debug!("Scan completed, found {} keys", keys.len());
                CommandResponse::Keys(keys)
            }
            Err(e) => {
                debug!("Scan failed: {}", e);
                CommandResponse::Error(e.to_string())
            }
        }
    }

    fn name(&self) -> &'static str {
        "SCAN"
    }

    fn validate(&self) -> Result<(), CommandError> {
        Ok(())
    }

    fn is_read_only(&self) -> bool {
        true
    }
}
