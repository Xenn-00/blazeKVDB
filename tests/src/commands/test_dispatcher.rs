use std::sync::Arc;

use blazekvdb::{
    commands::{
        Command, CommandDispatcher, CommandResponse, delete::DeleteCommand, exist::ExistCommand,
        get::GetCommand, scan::ScanCommand, set::SetCommand,
    },
    storage::{StorageConfig, StorageEngine, engine::memory::MemoryEngine},
};

#[tokio::test]
async fn test_command_dispatcher() {
    let config = StorageConfig::default();
    let engine = Arc::new(MemoryEngine::new(config)) as Arc<dyn StorageEngine>;
    let dispatcher = CommandDispatcher::new(engine);

    let ping_response = dispatcher.execute(Command::Ping).await;
    assert_eq!(ping_response, CommandResponse::Pong);

    let set_command = SetCommand::new("key1".to_string(), b"value1".to_vec());
    let set_response = dispatcher.execute(Command::Set(set_command)).await;
    assert_eq!(set_response, CommandResponse::Ok);

    let get_command = GetCommand::new("key1".to_string());
    let get_response = dispatcher.execute(Command::Get(get_command)).await;
    assert_eq!(get_response, CommandResponse::Value(b"value1".to_vec()));

    let exist_command = ExistCommand::new("key1".to_string());
    let exist_response = dispatcher.execute(Command::Exist(exist_command)).await;
    assert_eq!(exist_response, CommandResponse::Bool(true));

    let scan_command = ScanCommand::new("key".to_string());
    let scan_response = dispatcher.execute(Command::Scan(scan_command)).await;
    assert_eq!(
        scan_response,
        CommandResponse::Keys(vec!["key1".to_string()])
    );

    let stats_response = dispatcher.execute(Command::Stats).await;
    if let CommandResponse::Stats {
        total_keys,
        memory_usage,
        hit_rate,
        total_operations,
    } = stats_response
    {
        assert!(total_keys != 0);
        assert!(total_operations != 0);
        assert!(hit_rate.is_finite());
        assert!(memory_usage != 0)
    } else {
        panic!("Expected Stats response");
    }

    let delete_command = DeleteCommand::new("key1".to_string());
    let delete_response = dispatcher.execute(Command::Delete(delete_command)).await;
    assert_eq!(delete_response, CommandResponse::Bool(true))
}

#[tokio::test]
async fn test_batch_commands() {
    let config = StorageConfig::default();
    let engine = Arc::new(MemoryEngine::new(config)) as Arc<dyn StorageEngine>;
    let dispatcher = CommandDispatcher::new(engine);

    let commands = vec![
        Command::Set(SetCommand::new("key1".to_string(), b"value1".to_vec())),
        Command::Set(SetCommand::new("key2".to_string(), b"value2".to_vec())),
        Command::Get(GetCommand::new("key1".to_string())),
        Command::Exist(ExistCommand::new("key2".to_string())),
        Command::Delete(DeleteCommand::new("key2".to_string())),
    ];

    let responses = dispatcher.execute_batch(commands).await;

    assert_eq!(responses.len(), 5);
    assert_eq!(responses[0], CommandResponse::Ok);
    assert_eq!(responses[1], CommandResponse::Ok);
    assert_eq!(responses[2], CommandResponse::Value(b"value1".to_vec()));
    assert_eq!(responses[3], CommandResponse::Bool(true));
    assert_eq!(responses[4], CommandResponse::Bool(true));
}
