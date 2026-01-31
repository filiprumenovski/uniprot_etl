//! Pipeline execution commands.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use serde::Serialize;
use tauri::{AppHandle, Emitter, Manager, State};

use uniprot_etl::error::EtlError;
use uniprot_etl::sampler::ChannelStats;
use uniprot_etl::{
    run_pipeline, start_metrics_server, Metrics, PipelineArgs, RunContext, RunReport, RunStatus,
    Settings,
};

use crate::state::{AppState, PipelineHandle};

/// Progress update sent to frontend via events.
#[derive(Clone, Serialize)]
pub struct ProgressUpdate {
    pub entries_parsed: u64,
    pub entries_per_sec: f64,
    pub batches_written: u64,
    pub features_extracted: u64,
    pub isoforms_extracted: u64,
    pub ptm_mapped: u64,
    pub ptm_failed: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub elapsed_secs: f64,
    pub is_running: bool,
}

impl ProgressUpdate {
    fn from_metrics(metrics: &Metrics, is_running: bool) -> Self {
        let elapsed = metrics.elapsed_secs();
        let entries = metrics.entries();
        Self {
            entries_parsed: entries,
            entries_per_sec: if elapsed > 0.001 {
                entries as f64 / elapsed
            } else {
                0.0
            },
            batches_written: metrics.batches(),
            features_extracted: metrics.features(),
            isoforms_extracted: metrics.isoforms(),
            ptm_mapped: metrics.ptm_mapped(),
            ptm_failed: metrics.ptm_failed(),
            bytes_read: metrics.bytes_read(),
            bytes_written: metrics.bytes_written(),
            elapsed_secs: elapsed,
            is_running,
        }
    }
}

/// Start the ETL pipeline.
/// Returns immediately; progress updates sent via "pipeline:progress" events.
#[tauri::command]
pub async fn start_pipeline(
    app: AppHandle,
    state: State<'_, AppState>,
    input_path: String,
    output_path: String,
    fasta_sidecar_path: Option<String>,
    batch_size: Option<usize>,
) -> Result<String, String> {
    // Check if already running
    {
        let guard = state.pipeline.lock().map_err(|e| e.to_string())?;
        if let Some(ref handle) = *guard {
            if handle.is_running() {
                return Err("Pipeline already running".into());
            }
        }
    }

    // Build settings
    // Load defaults or from config.yaml if present, similar to CLI behavior
    let mut settings = Settings::load_from_yaml(None).unwrap_or_else(|e| {
        eprintln!(
            "[WARN] Failed to load config.yaml in GUI: {}. Using defaults.",
            e
        );
        Settings::default()
    });

    settings.storage.input_path = Some(PathBuf::from(&input_path));
    settings.storage.output_path = PathBuf::from(&output_path);
    if let Some(ref fasta) = fasta_sidecar_path {
        settings.storage.fasta_sidecar_path = Some(PathBuf::from(fasta));
    }
    if let Some(bs) = batch_size {
        settings.performance.batch_size = bs;
    }

    // Resolve paths
    let cwd = std::env::current_dir().map_err(|e| e.to_string())?;
    settings.resolve_paths(&cwd).map_err(|e| e.to_string())?;
    // Store runs outside the dev watcher to avoid restarts.
    let runs_dir = app
        .path()
        .app_data_dir()
        .map_err(|e| e.to_string())?
        .join("runs");
    settings.runs.runs_dir = runs_dir;
    // Ensure Grafana/Prometheus can reach the metrics endpoint from Docker.
    settings.observability.metrics_bind_address = "0.0.0.0:9090".to_string();

    let metrics = Metrics::new();
    let channel_stats = Arc::new(ChannelStats::new(settings.performance.channel_capacity));
    let cancel_flag = Arc::new(AtomicBool::new(false));
    let cancel_flag_for_thread = Arc::clone(&cancel_flag);
    let running_flag = Arc::new(AtomicBool::new(true));
    let running_flag_for_thread = Arc::clone(&running_flag);

    // Clone for thread
    let metrics_clone = metrics.clone();
    let app_clone = app.clone();

    // Spawn pipeline thread
    let handle = thread::spawn(move || {
        let run_context = match RunContext::new(&settings.runs.runs_dir) {
            Ok(ctx) => ctx,
            Err(e) => {
                let _ = app_clone.emit(
                    "pipeline:complete",
                    serde_json::json!({
                        "success": false,
                        "message": format!("{:#}", e)
                    }),
                );
                running_flag_for_thread.store(false, Ordering::Relaxed);
                return Err(format!("{:#}", e));
            }
        };

        if let Err(e) = settings.save_snapshot(&run_context.config_snapshot_path()) {
            let _ = app_clone.emit(
                "pipeline:complete",
                serde_json::json!({
                    "success": false,
                    "message": format!("{:#}", e)
                }),
            );
            running_flag_for_thread.store(false, Ordering::Relaxed);
            return Err(format!("{:#}", e));
        }

        let metrics_server_handle = if settings.observability.enable_metrics_server {
            match start_metrics_server(
                metrics_clone.clone(),
                Arc::clone(&channel_stats),
                settings.observability.metrics_bind_address.clone(),
                Some(run_context.run_id.clone()),
            ) {
                Ok(handle) => {
                    eprintln!(
                        "[INFO] Metrics server started on http://{}",
                        settings.observability.metrics_bind_address
                    );
                    Some(handle)
                }
                Err(e) => {
                    eprintln!("[WARN] Failed to start metrics server: {}", e);
                    None
                }
            }
        } else {
            None
        };

        let mut sampler = uniprot_etl::ResourceSampler::start(Arc::clone(&channel_stats));

        let pipeline_args = PipelineArgs {
            settings,
            metrics: metrics_clone.clone(),
            channel_stats: Some(channel_stats),
            cancel_flag: Some(cancel_flag_for_thread.clone()),
        };

        let result = run_pipeline(&pipeline_args);

        sampler.stop();

        if let Some(handle) = metrics_server_handle {
            if let Err(e) = handle.shutdown() {
                eprintln!("[WARN] Error shutting down metrics server: {}", e);
            }
        }

        let status = match &result {
            Ok(()) => RunStatus::Success,
            Err(e) => {
                let cancelled = matches!(e.downcast_ref::<EtlError>(), Some(EtlError::Cancelled));
                let message = if cancelled {
                    "Cancelled by user".to_string()
                } else {
                    format!("{:#}", e)
                };
                RunStatus::Error { message }
            }
        };

        let report = RunReport::generate(&run_context, &metrics_clone, &sampler, status.clone());
        if let Err(e) = report.save_yaml(&run_context.report_path()) {
            eprintln!("[WARN] Failed to save run report: {}", e);
        }

        let completion_message = match &status {
            RunStatus::Success => "Pipeline completed successfully".to_string(),
            RunStatus::Error { message } => message.clone(),
        };

        let _ = app_clone.emit(
            "pipeline:complete",
            serde_json::json!({
                "success": matches!(status, RunStatus::Success),
                "message": completion_message
            }),
        );

        running_flag_for_thread.store(false, Ordering::Relaxed);
        result.map_err(|e| format!("{:#}", e))
    });

    // Clone metrics for progress tracking before storing handle
    let progress_metrics = metrics.clone();
    let running_flag_for_progress = Arc::clone(&running_flag);
    let cancel_flag_for_progress = Arc::clone(&cancel_flag);

    // Store handle in state
    {
        let mut guard = state.pipeline.lock().map_err(|e| e.to_string())?;
        *guard = Some(PipelineHandle {
            cancel_flag,
            metrics,
            thread_handle: Some(handle),
        });
    }

    // Start progress emission loop using the cloned metrics
    let app_for_progress = app.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;

            let elapsed = progress_metrics.elapsed_secs();
            let entries = progress_metrics.entries();

            let is_running = running_flag_for_progress.load(Ordering::Relaxed);

            let update = ProgressUpdate {
                entries_parsed: entries,
                entries_per_sec: if elapsed > 0.001 {
                    entries as f64 / elapsed
                } else {
                    0.0
                },
                batches_written: progress_metrics.batches(),
                features_extracted: progress_metrics.features(),
                isoforms_extracted: progress_metrics.isoforms(),
                ptm_mapped: progress_metrics.ptm_mapped(),
                ptm_failed: progress_metrics.ptm_failed(),
                bytes_read: progress_metrics.bytes_read(),
                bytes_written: progress_metrics.bytes_written(),
                elapsed_secs: elapsed,
                is_running,
            };

            let _ = app_for_progress.emit("pipeline:progress", &update);

            // Stop after completion, cancellation, or 10 minutes.
            if !is_running || cancel_flag_for_progress.load(Ordering::Relaxed) || elapsed > 600.0 {
                break;
            }
        }
    });

    Ok("Pipeline started".into())
}

/// Get current metrics snapshot (polling fallback).
#[tauri::command]
pub fn get_live_metrics(state: State<'_, AppState>) -> Result<Option<ProgressUpdate>, String> {
    let guard = state.pipeline.lock().map_err(|e| e.to_string())?;
    Ok(guard.as_ref().map(|h| {
        let is_running = h.is_running();
        ProgressUpdate::from_metrics(&h.metrics, is_running)
    }))
}

/// Cancel the running pipeline.
#[tauri::command]
pub fn cancel_pipeline(state: State<'_, AppState>) -> Result<(), String> {
    let guard = state.pipeline.lock().map_err(|e| e.to_string())?;
    if let Some(ref handle) = *guard {
        handle.cancel();
    }
    Ok(())
}
