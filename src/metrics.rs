use prometheus::{
    Counter, Gauge, Histogram, HistogramOpts, Registry, TextEncoder, exponential_buckets,
};

pub struct RespLiteMetrics {
    // Counter - total number which always increase (total number of requests)
    pub requests_total: Counter,

    // Gauge - number which can go up and down (current number of active connections)
    pub active_connections: Gauge,
    pub keys_total: Gauge,

    // Histogram - latency distribution of requests
    pub request_duration: Histogram,

    registry: Registry,
}

impl RespLiteMetrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let requests_total = Counter::new("kv_requests_total", "Total requests received").unwrap();
        let active_connections = Gauge::new("kv_active_connections", "Active connections").unwrap();
        let request_duration = Histogram::with_opts(
            HistogramOpts::new("kv_request_duration_seconds", "Request duration in seconds")
                .buckets(exponential_buckets(0.001, 2.0, 10).unwrap()),
        )
        .unwrap();
        let keys_total = Gauge::new("kv_keys_total", "Total keys stored").unwrap();

        registry.register(Box::new(requests_total.clone())).unwrap();
        registry
            .register(Box::new(active_connections.clone()))
            .unwrap();
        registry
            .register(Box::new(request_duration.clone()))
            .unwrap();
        registry.register(Box::new(keys_total.clone())).unwrap();

        Self {
            requests_total,
            active_connections,
            request_duration,
            keys_total,
            registry,
        }
    }

    pub fn export(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        encoder.encode_to_string(&metric_families).unwrap()
    }
}
