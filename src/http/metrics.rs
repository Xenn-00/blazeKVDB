use std::sync::Arc;

use warp::Filter;

use crate::metrics::RespLiteMetrics;

pub fn routes(
    metrics: Arc<RespLiteMetrics>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let health_route =
        warp::path!("health").map(|| warp::reply::with_status("OK", warp::http::StatusCode::OK));

    let metrics_route = warp::path!("metrics").map(move || {
        let body = metrics.export();
        warp::reply::with_header(body, "content-type", "text/plain")
    });

    health_route.or(metrics_route)
}
