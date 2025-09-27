use std::sync::atomic::Ordering;

use resp_lite::storage::{StorageConfig, StorageEngine, engine::memory::MemoryEngine};

#[tokio::test]
async fn test_basic_operations() {
    let config = StorageConfig::default();
    let engine = MemoryEngine::new(config);

    // Test set and get
    engine.set("key1", b"value1".to_vec()).await.unwrap();
    let result = engine.get("key1").await.unwrap();
    assert_eq!(result, Some(b"value1".to_vec()));

    // Test delete
    let deleted = engine.delete("key1").await.unwrap();
    assert!(deleted);

    let result = engine.get("key1").await.unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn test_sharding() {
    let config = StorageConfig {
        shard_count: 4,
        ..Default::default()
    };

    let engine = MemoryEngine::new(config);

    // Add keys and verify they're distributed across shards
    for i in 0..100 {
        let key = format!("key{}", i);
        let value = format!("value{}", i).into_bytes();
        engine.set(&key, value).await.unwrap();
    }

    let stats = engine.stats().await.unwrap();
    assert_eq!(stats.total_keys, 100);
}

#[tokio::test]
async fn test_memory_limit() {
    let config = StorageConfig {
        max_memory: 1024, // 1 KB
        ..Default::default()
    };

    let engine = MemoryEngine::new(config);

    // Add keys until memory limit is reached
    for i in 0..100 {
        let key = format!("key{}", i);
        let value = vec![b'a'; 50]; // 50 bytes each
        let result = engine.set(&key, value).await;

        if result.is_err() {
            break; // Stop when memory limit is hit
        }
    }

    let stats = engine.stats().await.unwrap();
    assert!(stats.memory_usage <= 1024);
}

#[tokio::test]
async fn test_memory_stats() {
    let config = StorageConfig::default();
    let engine = MemoryEngine::new(config);

    // Add some keys
    for i in 0..10 {
        let key = format!("key{}", i);
        let value = format!("value{}", i).into_bytes();
        engine.set(&key, value).await.unwrap();
    }

    // Access some keys to generate hits and misses
    for i in 0..5 {
        let key = format!("key{}", i);
        engine.get(&key).await.unwrap();
    }
    for i in 10..15 {
        let key = format!("key{}", i);
        engine.get(&key).await.unwrap();
    }

    let stats = engine.stats().await.unwrap();
    assert_eq!(stats.total_keys, 10);
    // Hit rate calculation:
    // Total GET ops = 10 (5 hits + 5 misses)
    // Hits = 5
    // Hit rate = 5/10 = 0.5
    assert_eq!(stats.hit_rate, 0.5);
}

#[tokio::test]
async fn test_memory_stats_debug() {
    let config = StorageConfig::default();
    let engine = MemoryEngine::new(config);

    // Before anything
    let initial = engine.stats().await.unwrap();
    println!(
        "Initial: ops={}, hits={}, misses={}, hit_rate={}",
        initial.total_operations,
        engine.hit_count.load(Ordering::Relaxed),
        engine.miss_count.load(Ordering::Relaxed),
        initial.hit_rate
    );

    // After puts
    for i in 0..10 {
        engine
            .set(&format!("key{}", i), format!("value{}", i).into_bytes())
            .await
            .unwrap();
    }

    let after_puts = engine.stats().await.unwrap();
    println!(
        "After PUTs: ops={}, hits={}, misses={}, hit_rate={}",
        after_puts.total_operations,
        engine.hit_count.load(Ordering::Relaxed),
        engine.miss_count.load(Ordering::Relaxed),
        after_puts.hit_rate
    );

    // After gets (hits)
    for i in 0..5 {
        engine.get(&format!("key{}", i)).await.unwrap();
    }

    // After gets (misses)
    for i in 10..15 {
        engine.get(&format!("key{}", i)).await.unwrap();
    }

    let final_stats = engine.stats().await.unwrap();
    println!(
        "Final: ops={}, hits={}, misses={}, hit_rate={}",
        final_stats.total_operations,
        engine.hit_count.load(Ordering::Relaxed),
        engine.miss_count.load(Ordering::Relaxed),
        final_stats.hit_rate
    );
}
