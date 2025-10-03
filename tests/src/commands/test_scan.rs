use std::sync::Arc;

use blazekvdb::{
    commands::{CommandHandler, CommandResponse, scan::ScanCommand},
    storage::{StorageConfig, StorageEngine, engine::memory::MemoryEngine},
};

#[tokio::test]
async fn test_scan_execute() {
    let config = StorageConfig::default();
    let engine = Arc::new(MemoryEngine::new(config)) as Arc<dyn StorageEngine>;

    engine.set("key1", b"value1".to_vec()).await.unwrap();
    engine.set("key2", b"value1".to_vec()).await.unwrap();

    let cmd = ScanCommand::new("key".to_string());
    let mut response = cmd.execute(&*engine).await;
    if let CommandResponse::Keys(ref mut keys) = response {
        keys.sort(); // Sort the keys
    }
    assert_eq!(
        response,
        CommandResponse::Keys({
            let mut keys = vec!["key1".to_string(), "key2".to_string()];
            keys.sort();
            keys
        })
    );
}
