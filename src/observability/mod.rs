//! Observability infrastructure: metrics, tracing, and diagnostics.

pub mod metrics_server;

pub use metrics_server::{start_metrics_server, MetricsServerHandle};
