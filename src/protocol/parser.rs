use thiserror::Error;

use crate::commands::{
    Command, CommandResponse, delete::DeleteCommand, exist::ExistCommand, get::GetCommand,
    scan::ScanCommand, set::SetCommand,
};

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("Invalid command format: {0}")]
    InvalidFormat(String),

    #[error("Unknown command: {0}")]
    UnknownCommand(String),

    #[error("Missing arguments for command: {0}")]
    MissingArguments(String),

    #[error("Base64 decode error: {0}")]
    Base64Error(#[from] base64::DecodeError),

    #[error("JSON serialization error: {0}")]
    JsonError(#[from] serde_json::Error),
}

// Simple text-based protocol parser
// Protocol format:
// - GET key
// - SET key value_base64
// - DELETE key
// - EXIST key
// - SCAN prefix
// - STATS
// - PING

pub struct ProtocolParser;

impl ProtocolParser {
    // Parse incoming message into Command
    pub fn parse_command(message: &str) -> Result<Command, ProtocolError> {
        let message = message.trim();
        if message.is_empty() {
            return Err(ProtocolError::InvalidFormat("Empty command".to_string()));
        }

        let parts: Vec<&str> = message.split_whitespace().collect();
        let command = parts[0].to_uppercase();

        match command.as_str() {
            "GET" => {
                if parts.len() < 2 {
                    return Err(ProtocolError::MissingArguments(
                        "GET requires key".to_string(),
                    ));
                }
                Ok(Command::Get(GetCommand::new(parts[1].to_string())))
            }

            "SET" => {
                if parts.len() < 3 {
                    return Err(ProtocolError::MissingArguments(
                        "SET requires key and value".to_string(),
                    ));
                }

                let key = parts[1].to_string();

                // Handle value - could be base64 encoded or plain text
                let value = if parts.len() == 3 {
                    // Single value part - try base64 first, fallback to plain text
                    match base64::Engine::decode(
                        &base64::engine::general_purpose::STANDARD,
                        parts[2],
                    ) {
                        Ok(decoded) => decoded,
                        Err(_) => parts[2].as_bytes().to_vec(), // Plain text fallback
                    }
                } else {
                    // Multiple parts - join with spaces and treat as plain text
                    let value_str = parts[2..].join(" ");
                    value_str.as_bytes().to_vec()
                };

                Ok(Command::Set(SetCommand::new(key, value)))
            }

            "DELETE" | "DEL" => {
                if parts.len() < 2 {
                    return Err(ProtocolError::MissingArguments(
                        "DELETE requires key".to_string(),
                    ));
                }

                Ok(Command::Delete(DeleteCommand::new(parts[1].to_string())))
            }

            "EXIST" => {
                if parts.len() < 2 {
                    return Err(ProtocolError::MissingArguments(
                        "EXIST requires key".to_string(),
                    ));
                }
                Ok(Command::Exist(ExistCommand::new(parts[1].to_string())))
            }

            "SCAN" => {
                let prefix = if parts.len() >= 2 {
                    parts[1].to_string()
                } else {
                    String::new() // Empty prefix = scan all
                };

                Ok(Command::Scan(ScanCommand::new(prefix)))
            }

            "STATS" => Ok(Command::Stats),

            "PING" => Ok(Command::Ping),

            _ => Err(ProtocolError::UnknownCommand(command)),
        }
    }

    pub fn serialize_response(response: &CommandResponse) -> Result<String, ProtocolError> {
        match response {
            CommandResponse::Value(data) => {
                // Return base64 encoded value for binary safety
                let encoded =
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, data);
                Ok(format!("VALUE {}\n", encoded))
            }

            CommandResponse::Ok => Ok("OK\n".to_string()),
            CommandResponse::Bool(true) => Ok("TRUE\n".to_string()),
            CommandResponse::Bool(false) => Ok("FALSE\n".to_string()),
            CommandResponse::Keys(keys) => {
                if keys.is_empty() {
                    Ok("KEYS 0\n".to_string())
                } else {
                    let mut result = format!("KEYS {}\n", keys.len());
                    for key in keys {
                        result.push_str(&format!("{}\n", key));
                    }
                    Ok(result)
                }
            }
            CommandResponse::Stats {
                total_keys,
                memory_usage,
                hit_rate,
                total_operations,
            } => Ok(format!(
                "STATS total_keys={} memory_usage={} hit_rate={:.3} total_operations:{}\n",
                total_keys, memory_usage, hit_rate, total_operations
            )),
            CommandResponse::Pong => Ok("PONG\n".to_string()),
            CommandResponse::Error(msg) => Ok(format!("ERROR {}\n", msg)),
        }
    }
    pub fn parse_commands(buffer: &str) -> Vec<Result<Command, ProtocolError>> {
        buffer
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(Self::parse_command)
            .collect()
    }
}
