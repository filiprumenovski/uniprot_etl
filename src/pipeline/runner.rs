//! Pipeline orchestration: main entry point for library mode.
//!
//! This module provides the public API for running the ETL pipeline, supporting both
//! single-file and swarm (parallel directory) modes. It can be imported by any entry point
//! (CLI, Tauri GUI, tests, etc.) without runtime dependencies.

use anyhow::{anyhow, Result};
use crossbeam_channel::bounded;
use glob::glob;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use crate::config::Settings;
use crate::fasta::load_fasta_map;
use crate::metrics::{LocalMetricsAdapter, Metrics, MetricsCollector};
use crate::pipeline::parser::parse_entries;
use crate::pipeline::reader::create_xml_reader;
use crate::sampler::ChannelStats;
use crate::writer::parquet::write_batches;

/// Arguments for running the ETL pipeline (library API).
///
/// This struct captures all runtime parameters needed to execute the pipeline,
/// separate from Settings which is the configuration source of truth.
///
/// # Example
/// ```ignore
/// let settings = Settings::load_from_yaml(None)?;
/// let metrics = Metrics::new();
/// let channel_stats = Arc::new(ChannelStats::new(settings.performance.channel_capacity));
///
/// let args = PipelineArgs {
///     settings,
///     metrics: metrics.clone(),
///     channel_stats: Some(channel_stats),
/// };
///
/// run_pipeline(&args)?;
/// ```
pub struct PipelineArgs {
    /// Complete configuration (single source of truth)
    pub settings: Settings,

    /// Global metrics collector (Arc-cloned for progress tracking)
    pub metrics: Metrics,

    /// Optional: Channel stats for single-file mode backpressure tracking
    /// (In swarm mode, this tracks a dummy channel since per-file channels aren't monitored)
    pub channel_stats: Option<Arc<ChannelStats>>,
}

/// Run the ETL pipeline in the appropriate mode (single-file or swarm).
///
/// Automatically detects mode based on input path:
/// - Directory → Swarm mode (parallel processing of all XML files)
/// - File → Single-file mode (legacy behavior, sequential)
///
/// # Arguments
/// - `args`: Pipeline arguments with settings and metrics
///
/// # Returns
/// - `Ok(())` on success
/// - `Err(anyhow::Error)` with context on failure
///
/// # Example
/// ```ignore
/// let args = PipelineArgs { /* ... */ };
/// run_pipeline(&args)?;
/// ```
pub fn run_pipeline(args: &PipelineArgs) -> Result<()> {
    let input_path = args.settings.input_path()?;

    if input_path.is_dir() {
        // Swarm mode: parallel directory processing
        run_swarm_mode(args)
    } else {
        // Single-file mode: traditional pipeline
        run_single_file_mode(args)
    }
}

/// Run ETL pipeline on a single XML file.
///
/// This is the legacy behavior: one file → one output.
fn run_single_file_mode(args: &PipelineArgs) -> Result<()> {
    let input_path = args.settings.input_path()?;
    let output_path = &args.settings.storage.output_path;

    // Load FASTA sidecar if configured
    let sidecar_fasta = load_sidecar_fasta(&args.settings)?;

    // Process the file
    process_single_file(
        input_path,
        output_path,
        &args.settings,
        &args.metrics,
        sidecar_fasta,
    )
}

/// Run ETL pipeline in swarm mode: process directory of XML files in parallel.
///
/// Uses rayon for parallel processing with per-file LocalMetricsAdapter
/// for zero-contention metrics collection. Each file is processed independently
/// with its own bounded channel and writer thread.
fn run_swarm_mode(args: &PipelineArgs) -> Result<()> {
    let input_dir = args.settings.input_path()?;
    let output_dir = &args.settings.storage.output_path;

    // Create output directory if it doesn't exist
    std::fs::create_dir_all(output_dir)?;

    // Load sidecar FASTA once, shared across all workers (Arc clone for each thread)
    let sidecar_fasta = load_sidecar_fasta(&args.settings)?;

    // Find all XML files (*.xml and *.xml.gz)
    let files = discover_xml_files(input_dir)?;

    if files.is_empty() {
        return Err(anyhow!(
            "No XML files found in directory: {}",
            input_dir.display()
        ));
    }

    eprintln!("[INFO] Swarm mode: found {} XML files to process", files.len());

    // Track failures across parallel execution
    let failure_count = Arc::new(AtomicUsize::new(0));

    // Process files in parallel using rayon
    files.par_iter().for_each(|input_path| {
        let output_path = match derive_output_path(input_path, output_dir) {
            Ok(p) => p,
            Err(e) => {
                eprintln!(
                    "[ERROR] Failed to derive output path for {}: {}",
                    input_path.display(),
                    e
                );
                failure_count.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };

        eprintln!(
            "[INFO] Processing: {} -> {}",
            input_path.display(),
            output_path.display()
        );

        // Create thread-local metrics (zero cross-thread contention)
        // The Mutex is uncontended because each worker operates on its own LocalMetricsAdapter
        let local_metrics = LocalMetricsAdapter::new();

        if let Err(e) = process_single_file(
            input_path,
            &output_path,
            &args.settings,
            &local_metrics,
            sidecar_fasta.clone(),
        ) {
            eprintln!(
                "[ERROR] Failed to process {}: {:#}",
                input_path.display(),
                e
            );
            failure_count.fetch_add(1, Ordering::Relaxed);
        }

        // Merge local metrics into global (1 atomic op per field)
        local_metrics.merge_into(&args.metrics);
    });

    let failures = failure_count.load(Ordering::Relaxed);
    if failures > 0 {
        Err(anyhow!(
            "Swarm completed with {} file(s) failed out of {}",
            failures,
            files.len()
        ))
    } else {
        eprintln!(
            "[INFO] Swarm completed successfully: {} files processed",
            files.len()
        );
        Ok(())
    }
}

/// Process a single XML file through the ETL pipeline.
///
/// Creates its own isolated channel and writer thread for complete isolation.
/// This is the atomic unit of work in both single-file and swarm modes.
///
/// # Type Parameters
/// - `M`: Metrics collector (either Metrics or LocalMetricsAdapter)
fn process_single_file<M: MetricsCollector>(
    input_path: &Path,
    output_path: &Path,
    settings: &Settings,
    metrics: &M,
    sidecar_fasta: Option<Arc<HashMap<String, String>>>,
) -> Result<()> {
    // Create bounded channel for this file (isolated from other files)
    let (tx, rx) = bounded(settings.performance.channel_capacity);

    // Writer thread: consumes RecordBatches, writes Parquet
    let output_path_owned = output_path.to_path_buf();
    let writer_metrics = metrics.clone();
    let writer_settings = settings.clone();
    let writer_handle = thread::spawn(move || {
        write_batches(rx, &output_path_owned, &writer_metrics, &writer_settings)
    });

    // Create XML reader for this file
    let reader = create_xml_reader(input_path, settings, metrics)?;

    // Run the parser
    let parse_result = parse_entries(
        reader,
        tx,
        metrics,
        settings.performance.batch_size,
        sidecar_fasta,
    );

    // Wait for writer to finish
    let writer_result = writer_handle.join().expect("Writer thread panicked");

    // Propagate any errors
    parse_result?;
    writer_result?;

    Ok(())
}

/// Load sidecar FASTA if configured, return None otherwise.
fn load_sidecar_fasta(settings: &Settings) -> Result<Option<Arc<HashMap<String, String>>>> {
    if let Some(ref path) = settings.storage.fasta_sidecar_path {
        let map = load_fasta_map(path)?;
        Ok(Some(Arc::new(map)))
    } else {
        Ok(None)
    }
}

/// Discover all XML files (*.xml and *.xml.gz) in a directory.
fn discover_xml_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let pattern_xml = dir.join("*.xml").to_string_lossy().to_string();
    let pattern_gz = dir.join("*.xml.gz").to_string_lossy().to_string();

    let mut files: Vec<PathBuf> = Vec::new();

    for pattern in [&pattern_xml, &pattern_gz] {
        for entry in glob(pattern)? {
            match entry {
                Ok(path) => files.push(path),
                Err(e) => eprintln!("[WARN] Failed to read glob entry: {}", e),
            }
        }
    }

    Ok(files)
}

/// Derive output parquet path from input XML path.
///
/// Handles both .xml and .xml.gz extensions, stripping them to get the basename,
/// then appending .parquet in the output directory.
fn derive_output_path(input_path: &Path, output_dir: &Path) -> Result<PathBuf> {
    let file_name = input_path
        .file_name()
        .ok_or_else(|| anyhow!("Input path has no filename: {}", input_path.display()))?
        .to_string_lossy();

    // Strip .gz if present, then .xml
    let stem = file_name.strip_suffix(".gz").unwrap_or(&file_name);
    let stem = stem.strip_suffix(".xml").unwrap_or(stem);

    Ok(output_dir.join(format!("{}.parquet", stem)))
}

// Re-enable rayon for swarm mode
use rayon::prelude::*;
