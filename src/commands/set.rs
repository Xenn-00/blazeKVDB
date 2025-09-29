use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use crate::{
    commands::{CommandError, CommandHandler, CommandResponse},
    storage::StorageEngine,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetCommand {
    pub key: String,
    pub value: Vec<u8>,
    pub ttl: Option<u64>, // Future: TTl support
}

impl SetCommand {
    pub fn new(key: String, value: Vec<u8>) -> Self {
        Self {
            key,
            value,
            ttl: None,
        }
    }

    pub fn with_ttl(mut self, ttl: u64) -> Self {
        self.ttl = Some(ttl);
        self
    }
}

#[async_trait]
impl CommandHandler for SetCommand {
    #[instrument(skip(self, storage), fields(key = %self.key, size = self.value.len()))]
    async fn execute(&self, storage: &dyn StorageEngine) -> CommandResponse {
        debug!("Executing SET command");

        // TODO: Handle TTL in future versions
        if self.ttl.is_some() {
            return CommandResponse::Error("TTL not yet supported".to_string());
        }

        match storage.set(&self.key, self.value.clone()).await {
            Ok(_) => {
                debug!("Key set successfully");
                CommandResponse::Ok
            }
            Err(e) => {
                debug!("Failed to set key: {}", e);
                CommandResponse::Error(e.to_string())
            }
        }
    }

    fn name(&self) -> &'static str {
        "SET"
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

        if self.value.len() > 10 * 1024 * 1024 {
            return Err(CommandError::InvalidParameter(
                "Value too large (max 10MB)".to_string(),
            ));
        }

        Ok(())
    }

    fn is_read_only(&self) -> bool {
        false
    }

    fn complexity(&self) -> u32 {
        // Complexity based on value size
        (self.value.len() / 1024).max(1) as u32
    }
}
