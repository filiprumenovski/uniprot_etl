//! Application state for managing pipeline lifecycle.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use uniprot_etl::Metrics;

/// Handle to a running pipeline for progress tracking and cancellation.
pub struct PipelineHandle {
    /// Flag to signal cancellation (future enhancement).
    pub cancel_flag: Arc<AtomicBool>,
    /// Metrics collector for real-time progress.
    pub metrics: Metrics,
    /// Thread handle for the running pipeline.
    pub thread_handle: Option<JoinHandle<Result<(), String>>>,
}

impl PipelineHandle {
    /// Check if the pipeline is still running.
    pub fn is_running(&self) -> bool {
        self.thread_handle
            .as_ref()
            .map(|h| !h.is_finished())
            .unwrap_or(false)
    }

    /// Request cancellation (pipeline must check this flag).
    pub fn cancel(&self) {
        self.cancel_flag.store(true, Ordering::Relaxed);
    }
}

/// Global application state shared across Tauri commands.
pub struct AppState {
    /// Currently running pipeline, if any.
    pub pipeline: Mutex<Option<PipelineHandle>>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            pipeline: Mutex::new(None),
        }
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}
