use thiserror::Error;

use crate::{protocol::parser::ProtocolError, storage::StorageError};

#[derive(Debug, Error)]
pub enum BlazeError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Server error: {0}")]
    Server(String),

    #[error("Configuration error: {0}")]
    Config(String),
}

pub type BlazeResult<T> = Result<T, BlazeError>;
