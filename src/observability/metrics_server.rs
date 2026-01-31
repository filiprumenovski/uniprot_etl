//! Prometheus metrics HTTP server.
//!
//! Provides a lightweight HTTP server that exposes pipeline metrics in Prometheus
//! text exposition format. Runs in a separate thread with minimal overhead.

use std::sync::Arc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Router};
use tower_http::cors::CorsLayer;

use crate::metrics::Metrics;
use crate::sampler::ChannelStats;

/// Shared state for the metrics server.
#[derive(Clone)]
struct ServerState {
    metrics: Metrics,
    channel_stats: Arc<ChannelStats>,
    run_id: Option<String>,
    run_start_time: f64,
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
/// let handle = start_metrics_server(metrics, channel_stats, "127.0.0.1:9090".to_string(), None)?;
/// // Server is now running...
/// handle.shutdown()?;
/// ```
pub fn start_metrics_server(
    metrics: Metrics,
    channel_stats: Arc<ChannelStats>,
    bind_address: String,
    run_id: Option<String>,
) -> Result<MetricsServerHandle> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    let join_handle = thread::Builder::new()
        .name("metrics-server".to_string())
        .spawn(move || {
            if let Err(e) = run_server(metrics, channel_stats, bind_address, run_id, shutdown_rx) {
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
    run_id: Option<String>,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<()> {
    // Create a minimal single-threaded tokio runtime
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("Failed to create tokio runtime for metrics server")?;

    runtime.block_on(async {
        let run_start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        let state = ServerState {
            metrics,
            channel_stats,
            run_id,
            run_start_time,
        };

        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .with_state(state)
            .layer(CorsLayer::permissive());

        let listener = tokio::net::TcpListener::bind(&bind_address)
            .await
            .with_context(|| format!("Failed to bind metrics server to {}", bind_address))?;

        eprintln!("[INFO] Metrics server listening on http://{}", bind_address);

        // Run server with graceful shutdown
        let serve_future = axum::serve(listener, app).with_graceful_shutdown(async {
            shutdown_rx.await.ok();
        });

        serve_future.await.context("Metrics server error")?;

        Ok(())
    })
}

/// HTTP handler for /metrics endpoint.
async fn metrics_handler(State(state): State<ServerState>) -> impl IntoResponse {
    let backpressure = state.channel_stats.average_fullness();
    let mut metrics_text = state.metrics.to_prometheus_string(backpressure);

    if let Some(ref run_id) = state.run_id {
        metrics_text = inject_run_id_labels(&metrics_text, run_id);
        metrics_text.push_str(&format!(
            "uniprot_etl_run_start_time_seconds{{run_id=\"{}\"}} {}\n",
            run_id, state.run_start_time
        ));
    }

    (
        StatusCode::OK,
        [("Content-Type", "text/plain; version=0.0.4")],
        metrics_text,
    )
}

fn inject_run_id_labels(metrics_text: &str, run_id: &str) -> String {
    let mut output = String::with_capacity(metrics_text.len() + run_id.len() * 4);
    for line in metrics_text.lines() {
        if line.is_empty() || line.starts_with('#') {
            output.push_str(line);
            output.push('\n');
            continue;
        }

        let Some(space_idx) = line.find(' ') else {
            output.push_str(line);
            output.push('\n');
            continue;
        };

        let (series, rest) = line.split_at(space_idx);
        if series.contains("run_id=") {
            output.push_str(line);
            output.push('\n');
            continue;
        }

        if let Some(brace_idx) = series.find('{') {
            if series.ends_with('}') {
                let (prefix, _) = series.split_at(series.len().saturating_sub(1));
                output.push_str(prefix);
                output.push_str(",run_id=\"");
                output.push_str(run_id);
                output.push_str("\"}");
                output.push_str(rest);
                output.push('\n');
                continue;
            } else if brace_idx < series.len() {
                output.push_str(series);
                output.push_str("{run_id=\"");
                output.push_str(run_id);
                output.push_str("\"}");
                output.push_str(rest);
                output.push('\n');
                continue;
            }
        }

        output.push_str(series);
        output.push_str("{run_id=\"");
        output.push_str(run_id);
        output.push_str("\"}");
        output.push_str(rest);
        output.push('\n');
    }

    output
}

#[cfg(test)]
mod tests {
    use super::start_metrics_server;
    use crate::metrics::Metrics;
    use crate::sampler::ChannelStats;
    use anyhow::Result;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::Arc;
    use std::time::Duration;

    fn get_free_port() -> Result<u16> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        Ok(listener.local_addr()?.port())
    }

    fn http_get(addr: &str, path: &str) -> Result<String> {
        let mut stream = TcpStream::connect(addr)?;
        stream.set_read_timeout(Some(Duration::from_millis(500)))?;
        let request = format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            path, addr
        );
        stream.write_all(request.as_bytes())?;
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        Ok(response)
    }

    #[test]
    fn metrics_server_exposes_prometheus_metrics() -> Result<()> {
        let port = get_free_port()?;
        let addr = format!("127.0.0.1:{}", port);

        let metrics = Metrics::new();
        metrics.inc_entries();
        let channel_stats = Arc::new(ChannelStats::new(8));
        let handle = start_metrics_server(
            metrics,
            channel_stats,
            addr.clone(),
            Some("test_run".to_string()),
        )?;

        let mut last_error = None;
        let mut ok = false;

        for _ in 0..20 {
            match http_get(&addr, "/metrics") {
                Ok(response) => {
                    if let Some(body) = response.split("\r\n\r\n").nth(1) {
                        if body.contains("uniprot_etl_entries_total{run_id=\"test_run\"}") {
                            ok = true;
                            break;
                        }
                    }
                    last_error = Some("metrics response missing expected content".to_string());
                }
                Err(err) => {
                    last_error = Some(err.to_string());
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        handle.shutdown()?;

        if ok {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "metrics endpoint not ready or invalid response: {}",
                last_error.unwrap_or_else(|| "unknown error".to_string())
            ))
        }
    }
}
