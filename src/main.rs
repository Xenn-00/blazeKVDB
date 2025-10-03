use std::{path::Path, sync::Arc};

use blazekvdb::{
    bootstrap::BlazeKVDB,
    config::BlazeServerConfig,
    error::{BlazeError, BlazeResult},
    server::tcp::TcpServer,
    storage::StorageEngine,
};
use clap::Parser;
use tracing::{Level, error, info};
use tracing_subscriber::{EnvFilter, fmt};
use warp::Filter;

#[derive(Parser)]
#[command(name = "blazekvdb")]
#[command(
    about = "A high-performance key-value store without risking any security issue 😈😈 ",
    version
)]
#[command(author = "Xenn-00 <github.com/Xenn-00>")]
struct Cli {
    // Configuration file path
    #[arg(short, long, default_value = "blazekvdb.json")]
    config: String,

    // Bind address for TCP server
    #[arg(short, long)]
    bind: Option<String>,

    // Enable debug logging
    #[arg(short, long)]
    debug: bool,

    // Disable persistence
    #[arg(long)]
    no_persistence: bool,

    // Generate example config file and exit
    #[arg(long, value_name = "FILE")]
    generate_config: Option<String>,

    // Use environment variables for configuration
    #[arg(long)]
    use_env: bool,

    // Validate config and exit
    #[arg(long)]
    validate: bool,
}

#[tokio::main]
async fn main() -> BlazeResult<()> {
    // Parse CLI arguments
    let cli = Cli::parse();

    // Generate example config if requested
    if let Some(output_path) = cli.generate_config {
        return generate_config_file(&output_path);
    }

    // Load and prepare configuration
    let mut config = load_configuration(&cli)?;

    // Apply environment variable overrides if requested
    if cli.use_env {
        info!("Applying environment variable overrides");
        config.from_env();
    }

    // Apply CLI overrides
    apply_cli_overrides(&mut config, &cli)?;

    // Validate configuration
    config
        .validate()
        .map_err(|e| BlazeError::Config(format!("{}", e).into()))?;

    // If validate-only mode, exit here
    if cli.validate {
        info!("✅ Configuration is valid");
        return Ok(());
    }

    // Setup logging
    setup_logging(&config);

    // Print banner
    print_banner();

    // Print config summary
    print_config_summary(&config);

    // Initialize BlazeKVDB
    info!("Initializing BlazeKVDB...");
    let kvdb = match BlazeKVDB::new(config.clone()).await {
        Ok(db) => Arc::new(db),
        Err(e) => {
            error!("❌ Failed to initialize database: {}", e);
            return Err(e.into());
        }
    };

    let dispatcher = kvdb.dispatcher();
    let storage = kvdb.storage();

    print_startup_info(&config, &kvdb).await;

    start_auxiliary_services(&config, storage.clone()).await;

    info!("Starting TCP server...");
    let server = TcpServer::new(dispatcher, config.server.bind_addr);

    if let Err(e) = server.start().await {
        error!("❌ Server error: {}", e);
        return Err(BlazeError::Server(e.to_string()));
    }

    Ok(())
}

/// Setup logging based on configuration
fn setup_logging(config: &BlazeServerConfig) {
    let log_level = match config.observability.log_level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let env_filter = EnvFilter::from_default_env().add_directive(log_level.into());

    let subscriber = fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false);

    match config.observability.log_format.as_str() {
        "json" => subscriber.json().init(),
        "pretty" => subscriber.pretty().init(),
        _ => subscriber.compact().init(),
    }
}

fn generate_config_file(output_path: &str) -> BlazeResult<()> {
    let config = BlazeServerConfig::example();

    info!("📝 Generating example configuration...");

    if output_path.ends_with(".toml") {
        config
            .to_toml_file(output_path)
            .map_err(|e| blazekvdb::error::BlazeError::Config(format!("{}", e).into()))?;
        info!("✅ Generated TOML config: {}", output_path);
        println!("\n{}", "=".repeat(60));
        println!("Example configuration generated: {}", output_path);
        println!(
            "Edit the file and start with: cargo run -- --config {}",
            output_path
        );
        println!("{}", "=".repeat(60));
    } else {
        config
            .to_json_file(output_path)
            .map_err(|e| blazekvdb::error::BlazeError::Config(format!("{}", e).into()))?;
        info!("✅ Generated JSON config: {}", output_path);
        println!("\n{}", "=".repeat(60));
        println!("Example configuration generated: {}", output_path);
        println!(
            "Edit the file and start with: cargo run -- --config {}",
            output_path
        );
        println!("{}", "=".repeat(60));
    }

    Ok(())
}

// Load configuration from file or use details
fn load_configuration(cli: &Cli) -> BlazeResult<BlazeServerConfig> {
    let config_path = &cli.config;

    info!("📂 Loading config from: {}", config_path);

    if !Path::new(config_path).exists() {
        error!("❌ Config file not found: {}", config_path);
        return Err(blazekvdb::error::BlazeError::Config(
            format!("Config file not found: {}", config_path).into(),
        ));
    }

    let config = if config_path.ends_with(".toml") {
        BlazeServerConfig::from_toml_file(config_path)
            .map_err(|e| blazekvdb::error::BlazeError::Config(format!("{}", e).into()))?
    } else if config_path.ends_with(".json") {
        BlazeServerConfig::from_json_file(config_path)
            .map_err(|e| blazekvdb::error::BlazeError::Config(format!("{}", e).into()))?
    } else {
        // Try to detect format by content
        match BlazeServerConfig::from_toml_file(config_path) {
            Ok(cfg) => cfg,
            Err(_) => BlazeServerConfig::from_json_file(config_path)
                .map_err(|e| blazekvdb::error::BlazeError::Config(format!("{}", e).into()))?,
        }
    };

    info!("✅ Configuration loaded successfully");
    Ok(config)
}

// Apply CLI argument overrides to configuration
fn apply_cli_overrides(config: &mut BlazeServerConfig, cli: &Cli) -> BlazeResult<()> {
    // Overrides bind address
    if let Some(ref bind_addr) = cli.bind {
        info!("🔧 Overriding bind address: {}", bind_addr);
        config.server.bind_addr = bind_addr.parse().map_err(|e| {
            BlazeError::Config(format!("Invalid bind address '{}': {}", bind_addr, e))
        })?;
    }

    // Override persistence
    if cli.no_persistence {
        info!("🔧 Disabling persistence (CLI override)");
        config.persistence.enabled = false;
    }

    // Override log level
    if cli.debug {
        info!("🔧 Enabling debug logging (CLI override)");
        config.observability.log_level = "debug".to_string();
    }

    Ok(())
}

/// Print application banner
fn print_banner() {
    println!("\n{}", "=".repeat(70));
    println!(
        r#"
    ____  __                 __ ____   ____  ____
   / __ )/ /___ _____  ___  / //_/ | / / _ \/ __ )
  / __  / / __ `/_  / / _ \/ ,<  | |/ / // / __  |
 / /_/ / / /_/ / / /_/  __/ /| | |___/ // / /_/ /
/_____/_/\__,_/ /___/\___/_/ |_|____/____/_____/

    High-Performance Key-Value Database with Persistence
                    Version {}
    "#,
        env!("CARGO_PKG_VERSION")
    );
    println!("{}\n", "=".repeat(70));
}

/// Print configuration summary
fn print_config_summary(config: &BlazeServerConfig) {
    info!("📋 Configuration Summary:");
    info!("  ┌─ Server");
    info!("  │  • Bind address: {}", config.server.bind_addr);
    info!("  │  • Max connections: {}", config.server.max_connections);
    info!(
        "  │  • Connection timeout: {}s",
        config.server.connection_timeout
    );
    info!(
        "  │  • Worker threads: {}",
        if config.server.worker_threads == 0 {
            "auto".to_string()
        } else {
            config.server.worker_threads.to_string()
        }
    );

    info!("  ├─ Storage");
    info!(
        "  │  • Max memory: {} MB",
        config.storage.max_memory / 1024 / 1024
    );
    info!("  │  • Shard count: {}", config.storage.shard_count);

    info!("  ├─ Persistence");
    info!("  │  • Enabled: {}", config.persistence.enabled);
    if config.persistence.enabled {
        info!("  │  • AOF path: {}", config.persistence.aof_path.display());
        info!("  │  • Fsync policy: {:?}", config.persistence.fsync_policy);
        info!("  │  • Snapshots: {}", config.persistence.snapshot_enabled);
        if config.persistence.snapshot_enabled {
            info!(
                "  │  • Snapshot interval: {}s",
                config.persistence.snapshot_interval
            );
            info!(
                "  │  • Snapshot dir: {}",
                config.persistence.snapshot_dir.display()
            );
        }
    }

    info!("  ├─ Observability");
    info!("  │  • Log level: {}", config.observability.log_level);
    info!("  │  • Log format: {}", config.observability.log_format);
    info!("  │  • Metrics: {}", config.observability.metrics_enabled);
    info!(
        "  │  • Health checks: {}",
        config.observability.health_check_enabled
    );

    if config.security.tls_enabled || config.security.require_auth {
        info!("  └─ Security");
        info!("     • TLS: {}", config.security.tls_enabled);
        info!("     • Auth required: {}", config.security.require_auth);
    } else {
        info!("  └─ Security: Disabled");
    }
}

/// Print startup information
async fn print_startup_info(config: &BlazeServerConfig, kvdb: &BlazeKVDB) {
    // Get current stats
    if let Ok(storage_stats) = kvdb.storage_stats().await {
        info!("📊 Database Statistics:");
        info!("  • Total keys: {}", storage_stats.total_keys);
        info!(
            "  • Memory usage: {} MB",
            storage_stats.memory_usage / 1024 / 1024
        );
        info!("  • Hit rate: {:.2}%", storage_stats.hit_rate * 100.0);
        info!("  • Total operations: {}", storage_stats.total_operations);
    }

    // Persistence stats
    if let Some(persistence_stats) = kvdb.persistence_stats().await {
        if persistence_stats.aof_enabled {
            if let Some(ref aof_stats) = persistence_stats.aof_stats {
                info!("💾 Persistence Statistics:");
                info!("  • AOF operations: {}", aof_stats.operations_logged);
                info!(
                    "  • AOF size: {} MB",
                    aof_stats.file_size_bytes / 1024 / 1024
                );
                info!("  • Snapshots: {}", persistence_stats.snapshot_count);
            }
        }
    }

    println!("\n{}", "=".repeat(70));
    println!("✅ BlazeKVDB is ready to accept connections!");
    println!("{}", "=".repeat(70));
    println!("\n📡 Connection Information:");
    println!("  • TCP Server: {}", config.server.bind_addr);
    println!(
        "  • Connect with: telnet {} {}",
        config.server.bind_addr.ip(),
        config.server.bind_addr.port()
    );

    if config.observability.health_check_enabled {
        if let Some(addr) = config.observability.health_check_addr {
            println!("  • Health check: http://{}/health", addr);
            println!("  • Stats: http://{}/stats", addr);
        }
    }

    if config.observability.metrics_enabled {
        if let Some(addr) = config.observability.metrics_addr {
            println!("  • Metrics: http://{}/metrics", addr);
        }
    }

    println!("\n💡 Useful Commands:");
    println!("  • SET key value    - Store a key-value pair");
    println!("  • GET key          - Retrieve a value");
    println!("  • DELETE key       - Remove a key");
    println!("  • EXISTS key       - Check if key exists");
    println!("  • SCAN prefix      - List keys with prefix");
    println!("  • STATS            - Show database statistics");
    println!("  • SAVE             - Trigger manual snapshot");
    println!("  • PING             - Check server health");

    println!("\n{}", "=".repeat(70));
    println!("Press Ctrl+C to shutdown gracefully\n");
}

/// Start auxiliary services (health check, metrics)
async fn start_auxiliary_services(config: &BlazeServerConfig, storage: Arc<dyn StorageEngine>) {
    // Start health check server
    if config.observability.health_check_enabled {
        if let Some(health_addr) = config.observability.health_check_addr {
            info!("🏥 Starting health check server on http://{}", health_addr);
            start_health_server(health_addr, storage.clone()).await;
        }
    }

    // Start metrics server
    if config.observability.metrics_enabled {
        if let Some(metrics_addr) = config.observability.metrics_addr {
            info!(
                "📊 Starting metrics server on http://{}/metrics",
                metrics_addr
            );
            start_metrics_server(metrics_addr, storage.clone()).await;
        }
    }
}

/// Start health check HTTP server
async fn start_health_server(addr: std::net::SocketAddr, storage: Arc<dyn StorageEngine>) {
    let health = warp::path("health").map(move || {
        warp::reply::json(&serde_json::json!({
            "status": "healthy",
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "version": env!("CARGO_PKG_VERSION"),
        }))
    });

    let ready = warp::path("ready").and_then({
        let storage = storage.clone();
        move || {
            let storage = storage.clone();
            async move {
                match storage.health_check().await {
                    Ok(_) => Ok::<_, warp::Rejection>(warp::reply::json(&serde_json::json!({
                        "status": "ready",
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                    }))),
                    Err(e) => Ok::<_, warp::Rejection>(warp::reply::json(&serde_json::json!({
                        "status": "not_ready",
                        "error": e.to_string(),
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                    }))),
                }
            }
        }
    });

    let stats = warp::path("stats").and_then({
        let storage = storage.clone();
        move || {
            let storage = storage.clone();
            async move {
                match storage.stats().await {
                    Ok(stats) => Ok(warp::reply::json(&serde_json::json!({
                        "total_keys": stats.total_keys,
                        "memory_usage_bytes": stats.memory_usage,
                        "memory_usage_mb": stats.memory_usage / 1024 / 1024,
                        "hit_rate": stats.hit_rate,
                        "total_operations": stats.total_operations,
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                    }))),
                    Err(_) => Err(warp::reject()),
                }
            }
        }
    });

    let routes = health.or(ready).or(stats);

    tokio::spawn(warp::serve(routes).run(addr));
}

/// Start metrics HTTP server
async fn start_metrics_server(addr: std::net::SocketAddr, storage: Arc<dyn StorageEngine>) {
    let metrics = warp::path("metrics").and_then({
        let storage = storage.clone();
        move || {
            let storage = storage.clone();
            async move {
                match storage.stats().await {
                    Ok(stats) => {
                        let metrics_text = format!(
                            "# HELP blaze_kvdb_keys_total Total number of keys stored\n\
                                 # TYPE blaze_kvdb_keys_total gauge\n\
                                 blaze_kvdb_keys_total {}\n\
                                 \n\
                                 # HELP blaze_kvdb_memory_bytes Memory usage in bytes\n\
                                 # TYPE blaze_kvdb_memory_bytes gauge\n\
                                 blaze_kvdb_memory_bytes {}\n\
                                 \n\
                                 # HELP blaze_kvdb_hit_rate Cache hit rate\n\
                                 # TYPE blaze_kvdb_hit_rate gauge\n\
                                 blaze_kvdb_hit_rate {:.3}\n\
                                 \n\
                                 # HELP blaze_kvdb_operations_total Total operations processed\n\
                                 # TYPE blaze_kvdb_operations_total counter\n\
                                 blaze_kvdb_operations_total {}\n\
                                 \n\
                                 # HELP blaze_kvdb_up Server uptime indicator\n\
                                 # TYPE blaze_kvdb_up gauge\n\
                                 blaze_kvdb_up 1\n",
                            stats.total_keys,
                            stats.memory_usage,
                            stats.hit_rate,
                            stats.total_operations,
                        );

                        Ok::<_, warp::Rejection>(warp::reply::with_header(
                            metrics_text,
                            "content-type",
                            "text/plain; charset=utf-8",
                        ))
                    }
                    Err(_) => Ok(warp::reply::with_header(
                        "# BlazeKVDB metrics unavailable\nblaze_kvdb_up 0\n".to_string(),
                        "content-type",
                        "text/plain; charset=utf-8",
                    )),
                }
            }
        }
    });

    tokio::spawn(warp::serve(metrics).run(addr));
}
