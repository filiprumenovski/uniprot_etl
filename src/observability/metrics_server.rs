//! Prometheus metrics HTTP server.
//!
//! Provides a lightweight HTTP server that exposes pipeline metrics in Prometheus
//! text exposition format. Runs in a separate thread with minimal overhead.

use std::sync::Arc;
use std::thread;

use anyhow::{Context, Result};
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Router,
};

use crate::metrics::Metrics;
use crate::sampler::ChannelStats;

/// Shared state for the metrics server.
#[derive(Clone)]
struct ServerState {
    metrics: Metrics,
    channel_stats: Arc<ChannelStats>,
}

/// Handle for managing the metrics server lifecycle.
pub struct MetricsServerHandle {
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    join_handle: thread::JoinHandle<()>,
}

impl MetricsServerHandle {
    /// Shutdown the metrics server gracefully.
    pub fn shutdown(self) -> Result<()> {
        // Send shutdown signal (ignore errors if receiver already dropped)
        let _ = self.shutdown_tx.send(());

        // Wait for server thread to finish
        self.join_handle
            .join()
            .map_err(|_| anyhow::anyhow!("Metrics server thread panicked"))?;

        eprintln!("[INFO] Metrics server stopped");
        Ok(())
    }
}

/// Start the Prometheus metrics HTTP server in a background thread.
///
/// # Arguments
/// * `metrics` - Shared metrics instance (Arc-based clone)
/// * `channel_stats` - Channel statistics for backpressure tracking
/// * `bind_address` - Address to bind to (e.g., "127.0.0.1:9090")
///
/// # Returns
/// A handle that can be used to shutdown the server gracefully.
///
/// # Example
/// ```ignore
/// let metrics = Metrics::new();
/// let channel_stats = Arc::new(ChannelStats::new(8));
/// let handle = start_metrics_server(metrics, channel_stats, "127.0.0.1:9090".to_string())?;
/// // Server is now running...
/// handle.shutdown()?;
/// ```
pub fn start_metrics_server(
    metrics: Metrics,
    channel_stats: Arc<ChannelStats>,
    bind_address: String,
) -> Result<MetricsServerHandle> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    let join_handle = thread::Builder::new()
        .name("metrics-server".to_string())
        .spawn(move || {
            if let Err(e) = run_server(metrics, channel_stats, bind_address, shutdown_rx) {
                eprintln!("[ERROR] Metrics server error: {}", e);
            }
        })
        .context("Failed to spawn metrics server thread")?;

    Ok(MetricsServerHandle {
        shutdown_tx,
        join_handle,
    })
}

/// Run the metrics server (blocking, runs in dedicated thread).
fn run_server(
    metrics: Metrics,
    channel_stats: Arc<ChannelStats>,
    bind_address: String,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<()> {
    // Create a minimal single-threaded tokio runtime
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("Failed to create tokio runtime for metrics server")?;

    runtime.block_on(async {
        let state = ServerState {
            metrics,
            channel_stats,
        };

        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .with_state(state);

        let listener = tokio::net::TcpListener::bind(&bind_address)
            .await
            .with_context(|| format!("Failed to bind metrics server to {}", bind_address))?;

        eprintln!("[INFO] Metrics server listening on http://{}", bind_address);

        // Run server with graceful shutdown
        let serve_future = axum::serve(listener, app)
            .with_graceful_shutdown(async {
                shutdown_rx.await.ok();
            });

        serve_future.await.context("Metrics server error")?;

        Ok(())
    })
}

/// HTTP handler for /metrics endpoint.
async fn metrics_handler(State(state): State<ServerState>) -> impl IntoResponse {
    let backpressure = state.channel_stats.average_fullness();
    let metrics_text = state.metrics.to_prometheus_string(backpressure);

    (
        StatusCode::OK,
        [("Content-Type", "text/plain; version=0.0.4")],
        metrics_text,
    )
}
