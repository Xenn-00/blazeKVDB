use std::sync::Arc;

use blazekvdb::{
    commands::{CommandHandler, CommandResponse, delete::DeleteCommand},
    storage::{StorageConfig, StorageEngine, engine::memory::MemoryEngine},
};

#[test]
fn test_delete_validation() {
    let cmd = DeleteCommand::new("".to_string());
    assert!(cmd.validate().is_err());
}

#[tokio::test]
async fn test_delete_execute() {
    let config = StorageConfig::default();
    let engine = Arc::new(MemoryEngine::new(config)) as Arc<dyn StorageEngine>;

    engine.set("key", b"value1".to_vec()).await.unwrap();

    let cmd = DeleteCommand::new("key".to_string());
    let response = cmd.execute(&*engine).await;
    assert_eq!(response, CommandResponse::Bool(true))
}
