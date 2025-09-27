use blazekvdb::storage::persistence::aof::{AppendOnlyFile, Operation};
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
