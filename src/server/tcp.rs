use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio::{net::TcpListener, signal};
use tracing::{error, info, instrument};

use crate::{commands::CommandDispatcher, server::connection::ConnectionHandler};

pub struct TcpServer {
    dispatcher: Arc<CommandDispatcher>,
    bind_addr: SocketAddr,

    // Server metrics
    total_connections: Arc<AtomicUsize>,
    active_connections: Arc<AtomicUsize>,
}

#[derive(Debug, Clone)]
pub struct ServerStats {
    pub total_connections: usize,
    pub active_connections: usize,
}

impl TcpServer {
    // Create new TCP server
    pub fn new(dispatcher: Arc<CommandDispatcher>, bind_addr: SocketAddr) -> Self {
        Self {
            dispatcher,
            bind_addr,
            total_connections: AtomicUsize::new(0).into(),
            active_connections: AtomicUsize::new(0).into(),
        }
    }

    // Start the TCP server
    #[instrument(skip(self))]
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(self.bind_addr).await?;

        info!("KV Store TCP server listening on {}", self.bind_addr);

        // setup graceful shutdown
        let shutdown = self.setup_graceful_shutdown();

        tokio::select! {
            result = self.accept_connections(listener) => {
                if let Err(e) = result {
                    error!("Server error: {}", e)
                }
            }
            _ = shutdown => {
                info!("Received shutdown signal");
            }
        }

        info!("Server shutting down...");

        Ok(())
    }

    // Accept incoming connections
    pub async fn accept_connections(
        &self,
        listener: TcpListener,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    let total = self.total_connections.fetch_add(1, Ordering::Relaxed) + 1;
                    let active = self.active_connections.fetch_add(1, Ordering::Relaxed) + 1;

                    info!(
                        "Accepted connection {} from {} (active: {})",
                        total, addr, active
                    );

                    // Spawn task to handle connection
                    let dispatcher = self.dispatcher.clone();
                    let active_connections = self.active_connections.clone();

                    tokio::spawn(async move {
                        let handler = ConnectionHandler::new(dispatcher);
                        handler.handle_connection(stream, addr).await;

                        // Decrement active connection count
                        active_connections.fetch_sub(1, Ordering::Relaxed);
                    });
                }

                Err(e) => {
                    error!("failed to accept connection: {}", e);
                    // Small delay to prevent tight error loops
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
        }
    }

    async fn setup_graceful_shutdown(&self) {
        let ctrl_c = async {
            signal::ctrl_c()
                .await
                .expect("failed to install Ctrl+C handler")
        };

        #[cfg(unix)]
        let terminate = async {
            signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("failed to install signal handler")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => {
                info!("Received Ctrl+C signal");
            }
            _ = terminate => {
                info!("Received terminate signal");
            }
        }
    }

    pub fn stats(&self) -> ServerStats {
        ServerStats {
            total_connections: self.total_connections.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
        }
    }
}
