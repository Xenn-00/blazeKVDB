use blazekvdb::{
    commands::{
        Command, CommandResponse, delete::DeleteCommand, exist::ExistCommand, get::GetCommand,
        scan::ScanCommand, set::SetCommand,
    },
    protocol::parser::ProtocolParser,
};

#[test]
fn test_parse_get_command() {
    let cmd = ProtocolParser::parse_command("GET mykey").unwrap();
    assert_eq!(cmd, Command::Get(GetCommand::new("mykey".to_string())))
}

#[test]
fn test_parse_set_command_plain_text() {
    let cmd = ProtocolParser::parse_command("SET mykey hello world").unwrap();
    assert_eq!(
        cmd,
        Command::Set(SetCommand::new(
            "mykey".to_string(),
            b"hello world".to_vec()
        ))
    )
}

#[test]
fn test_parse_set_command_base64() {
    // "hello" in base64 is "aGVsbG8="
    let cmd = ProtocolParser::parse_command("SET mykey aGVsbG8=").unwrap();
    assert_eq!(
        cmd,
        Command::Set(SetCommand::new("mykey".to_string(), b"hello".to_vec()))
    )
}

#[test]
fn test_parse_delete_command() {
    let cmd = ProtocolParser::parse_command("DELETE mykey").unwrap();
    assert_eq!(
        cmd,
        Command::Delete(DeleteCommand::new("mykey".to_string()))
    );

    // Test alias
    let cmd = ProtocolParser::parse_command("DEL mykey").unwrap();
    assert_eq!(
        cmd,
        Command::Delete(DeleteCommand::new("mykey".to_string()))
    );
}

#[test]
fn test_parse_exist_command() {
    let cmd = ProtocolParser::parse_command("EXIST mykey").unwrap();
    assert_eq!(cmd, Command::Exist(ExistCommand::new("mykey".to_string())));
}

#[test]
fn test_parse_scan_command() {
    let cmd = ProtocolParser::parse_command("SCAN user:").unwrap();
    assert_eq!(cmd, Command::Scan(ScanCommand::new("user:".to_string())));

    // Test scan all
    let cmd = ProtocolParser::parse_command("SCAN").unwrap();
    assert_eq!(cmd, Command::Scan(ScanCommand::new(String::new())))
}

#[test]
fn test_parse_simple_commands() {
    assert_eq!(
        ProtocolParser::parse_command("PING").unwrap(),
        Command::Ping
    );
    assert_eq!(
        ProtocolParser::parse_command("STATS").unwrap(),
        Command::Stats
    );
}

#[test]
fn test_parse_errors() {
    // Missing arguments
    assert!(ProtocolParser::parse_command("GET").is_err());
    assert!(ProtocolParser::parse_command("SET key").is_err());
    assert!(ProtocolParser::parse_command("DELETE").is_err());

    // Unknown command
    assert!(ProtocolParser::parse_command("UNKNOWN").is_err());

    // Empty command
    assert!(ProtocolParser::parse_command("").is_err());
}

#[test]
fn test_serialize_responses() {
    // Value response
    let response = CommandResponse::Value(b"hello".to_vec());
    let serialized = ProtocolParser::serialize_response(&response).unwrap();
    assert_eq!(serialized, "VALUE aGVsbG8=\n"); // "hello" in base64

    // OK response
    let response = CommandResponse::Ok;
    let serialized = ProtocolParser::serialize_response(&response).unwrap();
    assert_eq!(serialized, "OK\n");

    // Boolean responses
    let response = CommandResponse::Bool(true);
    let serialized = ProtocolParser::serialize_response(&response).unwrap();
    assert_eq!(serialized, "TRUE\n");

    let response = CommandResponse::Bool(false);
    let serialized = ProtocolParser::serialize_response(&response).unwrap();
    assert_eq!(serialized, "FALSE\n");

    // Keys response
    let response = CommandResponse::Keys(vec!["key1".to_string(), "key2".to_string()]);
    let serialized = ProtocolParser::serialize_response(&response).unwrap();
    assert_eq!(serialized, "KEYS 2\nkey1\nkey2\n");

    // Error response
    let response = CommandResponse::Error("Something went wrong".to_string());
    let serialized = ProtocolParser::serialize_response(&response).unwrap();
    assert_eq!(serialized, "ERROR Something went wrong\n");

    // Pong response
    let response = CommandResponse::Pong;
    let serialized = ProtocolParser::serialize_response(&response).unwrap();
    assert_eq!(serialized, "PONG\n");
}

#[test]
fn test_parse_multiple_commands() {
    let buffer = "GET key1\nSET key2 value2\nPING\n";
    let commands = ProtocolParser::parse_commands(buffer);

    assert_eq!(commands.len(), 3);
    assert_eq!(
        commands[0].as_ref().unwrap(),
        &Command::Get(GetCommand::new("key1".to_string()))
    );
    assert_eq!(
        commands[1].as_ref().unwrap(),
        &Command::Set(SetCommand::new("key2".to_string(), b"value2".to_vec()))
    );
    assert_eq!(commands[2].as_ref().unwrap(), &Command::Ping);
}
