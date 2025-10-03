use std::{net::SocketAddr, sync::Arc};

use blazekvdb::{
    commands::CommandDispatcher,
    server::{connection::ConnectionHandler, tcp::TcpServer},
    storage::{StorageConfig, StorageEngine, engine::memory::MemoryEngine},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn create_test_server() -> (TcpServer, SocketAddr) {
    let config = StorageConfig::default();
    let storage = Arc::new(MemoryEngine::new(config)) as Arc<dyn StorageEngine>;
    let dispatcher = Arc::new(CommandDispatcher::new(storage));

    let addr = "127.0.0.1:0".parse().unwrap(); // let OS choose port
    let server = TcpServer::new(dispatcher, addr);

    (server, addr)
}

#[tokio::test]
async fn test_server_connection() {
    let (server, _) = create_test_server().await;

    // Start server in background
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = TcpListener::bind(bind_addr).await.unwrap();
    let actual_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        server.accept_connections(listener).await.ok();
    });

    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Connect and test basic communication
    let mut stream = TcpStream::connect(actual_addr).await.unwrap();

    // Send PING command
    stream.write_all(b"PING\n").await.unwrap();

    // Read response
    let mut buffer = [0; 1024];
    let n = stream.read(&mut buffer).await.unwrap();
    let response = String::from_utf8_lossy(&buffer[..n]);

    println!("response: {}", &response);

    assert_eq!(response, "PONG\n");
}

#[tokio::test]
async fn test_connection_stats() {
    let config = StorageConfig::default();
    let storage = Arc::new(MemoryEngine::new(config)) as Arc<dyn StorageEngine>;
    let dispatcher = Arc::new(CommandDispatcher::new(storage));

    let handler = ConnectionHandler::new(dispatcher);
    let stats = handler.stats();

    println!("stats: {:?}", &stats);

    assert_eq!(stats.commands_processed, 0);
    assert_eq!(stats.bytes_received, 0);
    assert_eq!(stats.bytes_sent, 0);
}
