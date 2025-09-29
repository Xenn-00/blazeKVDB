use std::sync::Arc;

use blazekvdb::{
    commands::{CommandHandler, CommandResponse, get::GetCommand},
    storage::{StorageConfig, StorageEngine, engine::memory::MemoryEngine},
};

#[test]
fn test_get_validation() {
    let cmd = GetCommand::new("".to_string());
    assert!(cmd.validate().is_err());
}

#[tokio::test]
async fn test_get_execute() {
    let config = StorageConfig::default();
    let engine = Arc::new(MemoryEngine::new(config)) as Arc<dyn StorageEngine>;

    engine.set("key1", b"value1".to_vec()).await.unwrap();

    let cmd = GetCommand::new("key1".to_string());
    let response = cmd.execute(&*engine).await;
    assert_eq!(response, CommandResponse::Value(b"value1".to_vec()));
}
