//! Observability infrastructure: metrics, tracing, and diagnostics.

pub mod metrics_server;

#[allow(unused_imports)]
pub use metrics_server::{start_metrics_server, MetricsServerHandle};
