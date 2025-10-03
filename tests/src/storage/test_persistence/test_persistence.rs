use std::{collections::HashMap, sync::Arc};

use blazekvdb::{
    config::{FsyncPolicy, PersistenceConfig},
    storage::{
        StorageConfig, StorageEngine,
        engine::memory::MemoryEngine,
        persistence::{
            aof::{AppendOnlyFile, Operation},
            manager::PersistenceManager,
            recovery::RecoveryManager,
            snapshot::Snapshotter,
        },
    },
};
use tempfile::tempdir;

#[tokio::test]
async fn test_aof_operations() {
    let temp_dir = tempdir().unwrap();
    let aof_path = temp_dir.path().join("test.aof");

    let mut aof = AppendOnlyFile::new(&aof_path).await.unwrap();

    // Log some operations
    let op1 = Operation::Put {
        key: "key1".to_string(),
        value: b"value1".to_vec(),
    };
    let op2 = Operation::Delete {
        key: "key2".to_string(),
    };

    aof.log_operation_sync(op1.clone()).await.unwrap();
    aof.log_operation_sync(op2.clone()).await.unwrap();

    // Read back operations
    let ops = aof.read_operations().await.unwrap();
    assert_eq!(ops.len(), 2);

    // Verify operations
    match &ops[0] {
        Operation::Put { key, value } => {
            assert_eq!(key, "key1");
            assert_eq!(value, b"value1");
        }
        _ => panic!("Expected Put operation"),
    }

    match &ops[1] {
        Operation::Delete { key } => {
            assert_eq!(key, "key2");
        }
        _ => panic!("Expected Delete operation"),
    }
}

#[tokio::test]
async fn test_snapshot_creation_and_loading() {
    let temp_dir = tempdir().unwrap();
    let snapshotter = Snapshotter::new(temp_dir.path()).unwrap();

    // Create test data
    let mut data = HashMap::new();
    data.insert("key1".to_string(), b"value1".to_vec());
    data.insert("key2".to_string(), b"value2".to_vec());

    // Create snapshot
    let snapshot_path = snapshotter.create_snapshot(data.clone()).await.unwrap();
    assert!(snapshot_path.exists());

    // Load snapshot
    let loaded = snapshotter.load_snapshot(&snapshot_path).await.unwrap();
    assert_eq!(loaded.data.len(), 2);
    assert_eq!(loaded.data.get("key1").unwrap(), b"value1");
}

#[tokio::test]
async fn test_recovery_with_snapshot_and_aof() {
    let temp_dir = tempdir().unwrap();
    let aof_path = temp_dir.path().join("test.aof");
    let snapshot_dir = temp_dir.path().join("snapshots");

    // Setup
    let config = StorageConfig::default();
    let storage = Arc::new(MemoryEngine::new(config));
    let aof = AppendOnlyFile::new(&aof_path).await.unwrap();

    // Create snapshot with initial data
    storage.set("key1", b"value1".to_vec()).await.unwrap();

    let snapshotter = Snapshotter::new(&snapshot_dir).unwrap();
    let mut data = HashMap::new();
    data.insert("key1".to_string(), b"value1".to_vec());

    snapshotter.create_snapshot(data).await.unwrap();

    // Clear storage
    storage.delete("key1").await.unwrap();

    // Recovery should restore from snapshot
    let recovery = RecoveryManager::new(Some(aof), Some(snapshotter));
    let stats = recovery.recover(storage.as_ref()).await.unwrap();

    println!("stats: {:?}", stats);
    assert!(stats.snapshot_loaded);
    assert_eq!(stats.keys_from_snapshot, 1);

    // Verify data restored
    let result = storage.get("key1").await.unwrap();
    assert_eq!(result, Some(b"value1".to_vec()))
}

#[tokio::test]
async fn test_persistence_manager_initialization() {
    let temp_dir = tempdir().unwrap();

    let config = PersistenceConfig {
        enabled: true,
        aof_path: temp_dir.path().join("test.aof"),
        fsync_policy: FsyncPolicy::EveryN(100),
        snapshot_enabled: true,
        snapshot_interval: 3600,
        snapshot_dir: temp_dir.path().join("snapshots"),
    };

    let storage_config = StorageConfig::default();
    let storage = Arc::new(MemoryEngine::new(storage_config)) as Arc<dyn StorageEngine>;

    let manager = PersistenceManager::new(config, storage).await.unwrap();

    assert!(manager.aof.is_some());
    assert!(manager.snapshotter.is_some());
}

#[tokio::test]
async fn test_full_persistence_cycle() {
    let temp_dir = tempdir().unwrap();

    let config = PersistenceConfig {
        enabled: true,
        aof_path: temp_dir.path().join("test.aof"),
        fsync_policy: FsyncPolicy::Always,
        snapshot_enabled: true,
        snapshot_interval: 3600,
        snapshot_dir: temp_dir.path().join("snapshots"),
    };

    let storage_config = StorageConfig::default();
    let storage = Arc::new(MemoryEngine::new(storage_config)) as Arc<dyn StorageEngine>;

    let manager = Arc::new(
        PersistenceManager::new(config.clone(), storage.clone())
            .await
            .unwrap(),
    );

    storage.set("key1", b"value1".to_vec()).await.unwrap();
    manager
        .log_operation(Operation::Put {
            key: "key1".to_string(),
            value: b"value1".to_vec(),
        })
        .await
        .unwrap();

    storage.set("key2", b"value2".to_vec()).await.unwrap();
    manager
        .log_operation(Operation::Put {
            key: "key2".to_string(),
            value: b"value2".to_vec(),
        })
        .await
        .unwrap();

    manager.create_snapshot().await.unwrap();

    storage.delete("key1").await.unwrap();
    storage.delete("key2").await.unwrap();

    // Recover
    let new_storage =
        Arc::new(MemoryEngine::new(StorageConfig::default())) as Arc<dyn StorageEngine>;
    let new_manager = PersistenceManager::new(config, new_storage.clone())
        .await
        .unwrap();

    let stats = new_manager.recover().await.unwrap();

    // Verify recovery
    assert!(stats.snapshot_loaded || stats.aof_operations_replayed > 0);

    let value1 = new_storage.get("key1").await.unwrap();
    assert_eq!(value1, Some(b"value1".to_vec()));
}
