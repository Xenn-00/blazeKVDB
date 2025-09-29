use std::sync::Arc;

use blazekvdb::{
    commands::{CommandHandler, CommandResponse, stats::StatsCommand},
    storage::{StorageConfig, StorageEngine, engine::memory::MemoryEngine},
};

#[tokio::test]
async fn test_stats_execute() {
    let config = StorageConfig::default();
    let engine = Arc::new(MemoryEngine::new(config)) as Arc<dyn StorageEngine>;

    // Add some data first
    engine.set("key1", b"value1".to_vec()).await.unwrap();
    engine.set("key2", b"value2".to_vec()).await.unwrap();
    engine.set("key3", b"value3".to_vec()).await.unwrap();

    let response = StatsCommand.execute(&*engine).await;

    // Check that we get stats back in the correct format
    if let CommandResponse::Stats {
        total_keys,
        total_operations,
        hit_rate,
        memory_usage,
    } = response
    {
        assert!(total_keys != 0);
        assert!(total_operations != 0);
        assert!(hit_rate.is_finite());
        assert!(memory_usage != 0)
    } else {
        panic!("Expected Stats response");
    }
}
