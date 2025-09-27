use std::sync::Arc;

use resp_lite::{http::metrics, metrics::RespLiteMetrics};
use tracing::Level;

#[tokio::main]
async fn main() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let metrics = Arc::new(RespLiteMetrics::new());

    // Metrics endpoint
    let metrics_server = {
        let metrics_route = metrics::routes(metrics);

        warp::serve(metrics_route).run(([127, 0, 0, 1], 8080))
    };

    tokio::select! {
        _ = metrics_server => {}
    }

    // kv setup service would go below here
}
