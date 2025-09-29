use std::sync::Arc;

use blazekvdb::{
    commands::{CommandHandler, CommandResponse, exist::ExistCommand},
    storage::{StorageConfig, StorageEngine, engine::memory::MemoryEngine},
};

#[test]
fn test_exist_validation() {
    let cmd = ExistCommand::new("".to_string());
    assert!(cmd.validate().is_err());
}

#[tokio::test]
async fn test_exist_execute() {
    let config = StorageConfig::default();
    let engine = Arc::new(MemoryEngine::new(config)) as Arc<dyn StorageEngine>;

    engine.set("key1", b"value1".to_vec()).await.unwrap();

    let cmd = ExistCommand::new("key1".to_string());
    let response = cmd.execute(&*engine).await;
    assert_eq!(response, CommandResponse::Bool(true));
}
