use std::sync::Arc;

use blazekvdb::{
    commands::{CommandHandler, CommandResponse, set::SetCommand},
    storage::{StorageConfig, StorageEngine, engine::memory::MemoryEngine},
};

#[test]
fn test_set_validation() {
    let cmd = SetCommand::new("".to_string(), Vec::<u8>::new());
    assert!(cmd.validate().is_err());
}

#[tokio::test]
async fn test_set_execute() {
    let config = StorageConfig::default();
    let engine = Arc::new(MemoryEngine::new(config)) as Arc<dyn StorageEngine>;

    let cmd = SetCommand::new("key".to_string(), b"value1".to_vec());
    let response = cmd.execute(&*engine).await;
    assert_eq!(response, CommandResponse::Ok)
}
