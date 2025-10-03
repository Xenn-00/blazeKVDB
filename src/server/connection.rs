use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::{
    io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpStream,
};
use tracing::{debug, error, info, instrument, warn};

use crate::{
    commands::{CommandDispatcher, CommandResponse},
    protocol::parser::ProtocolParser,
};

#[derive(Debug, Clone)]
pub struct ConnectionStats {
    pub commands_processed: u64,
    pub bytes_received: u64,
    pub bytes_sent: u64,
    pub connection_duration: Duration,
    pub last_command_time: Option<Instant>,
}

// handles individual TCP connections
pub struct ConnectionHandler {
    dispatcher: Arc<CommandDispatcher>,

    // Connection metrics
    commands_processed: AtomicU64,
    bytes_received: AtomicU64,
    bytes_sent: AtomicU64,
    connection_start: Instant,
}

impl ConnectionHandler {
    pub fn new(dispatcher: Arc<CommandDispatcher>) -> Self {
        Self {
            dispatcher,
            commands_processed: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            connection_start: Instant::now(),
        }
    }

    // handle a TCP connection
    #[instrument(skip(self, stream), fields(addr = %addr))]
    pub async fn handle_connection(&self, stream: TcpStream, addr: SocketAddr) {
        info!("New connection established");

        let mut buffer = Vec::with_capacity(4096);
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        loop {
            buffer.clear();

            // Read line from client
            match reader.read_until(b'\n', &mut buffer).await {
                Ok(0) => {
                    debug!("Client disconnected");
                    break;
                }
                Ok(bytes_read) => {
                    self.bytes_received
                        .fetch_add(bytes_read as u64, Ordering::Relaxed);

                    let message = String::from_utf8_lossy(&buffer);
                    let message = message.trim();

                    if message.is_empty() {
                        continue;
                    }

                    debug!("Received command: {}", message);
                    // Process command
                    let response = self.process_command(message).await;
                    // Send command
                    if let Err(e) = self.send_response(&mut write_half, response).await {
                        error!("Failed to send response: {}", e);
                        break;
                    }

                    self.commands_processed.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    error!("Error reading from connection: {}", e);
                    break;
                }
            }
        }

        let duration = self.connection_start.elapsed();
        let commands = self.commands_processed.load(Ordering::Relaxed);

        info!(
            "Connection closed - processed {} commands in {:?}",
            commands, duration
        )
    }

    // Process a single command
    async fn process_command(&self, message: &str) -> CommandResponse {
        match ProtocolParser::parse_command(message) {
            Ok(command) => {
                debug!("Parsed command successfully: {:?}", command);
                self.dispatcher.execute(command).await
            }
            Err(e) => {
                warn!("Failed to parse command '{}': {}", message, e);
                CommandResponse::Error(format!("Parse error: {}", e))
            }
        }
    }

    async fn send_response<W>(
        &self,
        writer: &mut W,
        response: CommandResponse,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        W: AsyncWrite + Unpin + Send,
    {
        let response_str = ProtocolParser::serialize_response(&response)?;

        writer.write_all(response_str.as_bytes()).await?;
        writer.flush().await?;

        self.bytes_sent
            .fetch_add(response_str.len() as u64, Ordering::Relaxed);

        debug!("Response sent: {} bytes", response_str.len());
        Ok(())
    }

    // Get connection statistics
    pub fn stats(&self) -> ConnectionStats {
        ConnectionStats {
            commands_processed: self.commands_processed.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            connection_duration: self.connection_start.elapsed(),
            last_command_time: Some(Instant::now()),
        }
    }
}
