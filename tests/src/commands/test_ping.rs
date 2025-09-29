use std::sync::Arc;

use blazekvdb::{
    commands::{CommandHandler, CommandResponse, ping::PingCommand},
    storage::{StorageConfig, StorageEngine, engine::memory::MemoryEngine},
};

#[tokio::test]
async fn test_ping_command() {
    let config = StorageConfig::default();
    let engine = Arc::new(MemoryEngine::new(config)) as Arc<dyn StorageEngine>;

    let response = PingCommand.execute(&*engine).await;
    assert_eq!(response, CommandResponse::Pong)
}
