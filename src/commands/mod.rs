use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    commands::{
        delete::DeleteCommand, exist::ExistCommand, get::GetCommand, ping::PingCommand,
        scan::ScanCommand, set::SetCommand, stats::StatsCommand,
    },
    storage::StorageEngine,
};

pub mod delete;
pub mod exist;
pub mod get;
pub mod ping;
pub mod scan;
pub mod set;
pub mod stats;

#[derive(Debug, Clone)]
pub struct CommandMetadata {
    pub name: &'static str,
    pub read_only: bool,
    pub estimated_complexity: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CommandResponse {
    Value(Vec<u8>),
    Ok,
    Bool(bool),
    Keys(Vec<String>),
    Stats {
        total_keys: usize,
        memory_usage: usize,
        hit_rate: f64,
        total_operations: u64,
    },
    Pong,
    Error(String),
}

#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),

    #[error("Missing required parameter: {0}")]
    MissingParameter(String),

    #[error("Storage error: {0}")]
    Storage(String),
}

#[async_trait]
pub trait CommandHandler: Send + Sync {
    // Execute the command against storage
    async fn execute(&self, storage: &dyn StorageEngine) -> CommandResponse;

    // Get command name for logging/metrics
    fn name(&self) -> &'static str;

    // Validate command parameters
    fn validate(&self) -> Result<(), CommandError>;

    // Get command metrics/metadata
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: self.name(),
            read_only: self.is_read_only(),
            estimated_complexity: self.complexity(),
        }
    }

    // Whether this command only reads data
    fn is_read_only(&self) -> bool {
        false
    }

    // Computatinal complexity estimate (for rate limiting)
    fn complexity(&self) -> u32 {
        1
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Command {
    Get(GetCommand),
    Set(SetCommand),
    Delete(DeleteCommand),
    Scan(ScanCommand),
    Exist(ExistCommand),
    Stats,
    Ping,
}

impl Command {
    // Convert to boxed command handler
    pub fn into_handler(self) -> Box<dyn CommandHandler> {
        match self {
            Command::Get(cmd) => Box::new(cmd),
            Command::Set(cmd) => Box::new(cmd),
            Command::Delete(cmd) => Box::new(cmd),
            Command::Scan(cmd) => Box::new(cmd),
            Command::Exist(cmd) => Box::new(cmd),
            Command::Stats => Box::new(StatsCommand),
            Command::Ping => Box::new(PingCommand),
        }
    }
}

// Enhanced command dispatcher with middleware support
pub struct CommandDispatcher {
    storage: Arc<dyn StorageEngine>,
    middleware: Vec<Box<dyn CommandMiddleware>>,
}

impl CommandDispatcher {
    pub fn new(storage: Arc<dyn StorageEngine>) -> Self {
        Self {
            storage,
            middleware: Vec::new(),
        }
    }

    // Add middleware (rate limiting, logging, etc.)
    pub fn with_middleware(mut self, middleware: Box<dyn CommandMiddleware>) -> Self {
        self.middleware.push(middleware);
        self
    }

    // Execute command with full middleware chain
    pub async fn execute(&self, command: Command) -> CommandResponse {
        let handler = command.into_handler();

        // Validate command
        if let Err(e) = handler.validate() {
            return CommandResponse::Error(e.to_string());
        }

        // Run pre-execution middleware
        for middleware in &self.middleware {
            if let Err(response) = middleware.before_execute(handler.as_ref()).await {
                return response;
            }
        }

        // Execute command
        let response = handler.execute(self.storage.as_ref()).await;

        // Run post-execution middleware
        for middleware in &self.middleware {
            middleware.after_execute(handler.as_ref(), &response).await;
        }

        response
    }

    pub async fn execute_batch(&self, commands: Vec<Command>) -> Vec<CommandResponse> {
        let mut responses = Vec::with_capacity(commands.len());

        for command in commands {
            let response = self.execute(command).await;
            responses.push(response);
        }

        responses
    }
}

// Middleware trait for cross-cutting concerns
#[async_trait]
pub trait CommandMiddleware: Send + Sync {
    // Called before command execution
    async fn before_execute(&self, command: &dyn CommandHandler) -> Result<(), CommandResponse>;

    // Called after command execution
    async fn after_execute(&self, command: &dyn CommandHandler, response: &CommandResponse);
}
