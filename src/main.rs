mod cli;
mod config;
mod error;
mod fasta;
mod metrics;
mod pipeline;
mod report;
mod runs;
mod sampler;
mod schema;
mod writer;

use anyhow::{anyhow, Result};
use clap::Parser;
use crossbeam_channel::bounded;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::collections::HashMap;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::thread;

use crate::cli::Args;
use crate::config::Settings;
use crate::fasta::load_fasta_map;
use crate::metrics::{LocalMetricsAdapter, Metrics, MetricsCollector};
use crate::pipeline::parser::parse_entries;
use crate::pipeline::reader::create_xml_reader;
use crate::report::{RunReport, RunStatus};
use crate::runs::{cleanup_old_runs, RunContext};
use crate::sampler::{ChannelStats, ResourceSampler};
use crate::writer::parquet::write_batches;

/// A writer that tees output to both a file and stderr.
struct TeeWriter {
    file: BufWriter<File>,
}

impl TeeWriter {
    fn new(file: File) -> Self {
        Self {
            file: BufWriter::new(file),
        }
    }

    fn writeln(&mut self, msg: &str) {
        // Write to stderr
        eprintln!("{}", msg);
        // Write to file
        let _ = writeln!(self.file, "{}", msg);
        let _ = self.file.flush();
    }
}

/// Log macro that writes to both file and stderr via TeeWriter.
macro_rules! log {
    ($writer:expr, $($arg:tt)*) => {
        $writer.writeln(&format!($($arg)*))
    };
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Load settings from YAML, with CLI overrides
    let mut settings = Settings::load_from_yaml(args.config.as_deref())?;
    settings =
        settings.merge_with_cli(args.input, args.output, args.batch_size, args.fasta_sidecar);

    // Resolve paths relative to current working directory (project root)
    let root = env::current_dir()?;
    settings.resolve_paths(&root)?;

    // Create run context (timestamped directory, optionally overridden)
    let run_context = RunContext::new_with_run_id(&settings.runs.runs_dir, args.run_id)?;

    // Set up tee logging to both file and stderr
    let log_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(run_context.log_path())?;
    let mut logger = TeeWriter::new(log_file);

    log!(logger, "[INFO] Run ID: {}", run_context.run_id);
    log!(
        logger,
        "[INFO] Run directory: {}",
        run_context.run_dir.display()
    );

    // Save config snapshot
    settings.save_snapshot(&run_context.config_snapshot_path())?;
    log!(
        logger,
        "[INFO] Config snapshot saved to {}",
        run_context.config_snapshot_path().display()
    );

    log!(logger, "[INFO] Configuration ready");
    log!(
        logger,
        "[INFO]   Input: {}",
        settings.input_path()?.display()
    );
    log!(
        logger,
        "[INFO]   Output: {}",
        settings.storage.output_path.display()
    );
    if let Some(ref fasta) = settings.storage.fasta_sidecar_path {
        log!(logger, "[INFO]   FASTA sidecar: {}", fasta.display());
    } else {
        log!(logger, "[WARN]   FASTA sidecar: (not set)");
    }
    log!(
        logger,
        "[INFO]   Batch size: {}",
        settings.performance.batch_size
    );
    log!(
        logger,
        "[INFO]   Channel capacity: {}",
        settings.performance.channel_capacity
    );
    log!(
        logger,
        "[INFO]   Zstd level: {}",
        settings.performance.zstd_level
    );

    let metrics = Metrics::new();

    // Start a lightweight terminal progress bar that updates from Metrics
    let progress_running = Arc::new(AtomicBool::new(true));
    let progress_flag = Arc::clone(&progress_running);
    let progress_metrics = metrics.clone();
    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::with_template("[{spinner}] {msg}").unwrap());
    pb.enable_steady_tick(std::time::Duration::from_millis(200));
    let progress_handle = thread::spawn(move || {
        while progress_flag.load(Ordering::Relaxed) {
            let elapsed = progress_metrics.elapsed_secs();
            let entries = progress_metrics.entries();
            let batches = progress_metrics.batches();
            let features = progress_metrics.features();
            let isoforms = progress_metrics.isoforms();
            let ptm_mapped = progress_metrics.ptm_mapped();
            let ptm_failed = progress_metrics.ptm_failed();
            let bytes_read = progress_metrics.bytes_read();
            let bytes_written = progress_metrics.bytes_written();
            let eps = if elapsed > 0.0 {
                entries as f64 / elapsed
            } else {
                0.0
            };
            let mb_read = bytes_read as f64 / (1024.0 * 1024.0);
            let mb_written = bytes_written as f64 / (1024.0 * 1024.0);
            pb.set_message(format!(
                "rows: {} ({:.0}/s) | batches: {} | features: {} | isoforms: {} | ptm: {} mapped / {} failed | read: {:.2} MB | written: {:.2} MB",
                entries, eps, batches, features, isoforms, ptm_mapped, ptm_failed, mb_read, mb_written
            ));
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
        pb.finish_and_clear();
    });

    // Create channel stats for backpressure tracking (used in single-file mode only)
    let channel_stats = Arc::new(ChannelStats::new(settings.performance.channel_capacity));

    // Start resource sampler (background thread sampling at 1Hz)
    // Note: In swarm mode, this tracks a dummy channel; per-file channels are not monitored
    let mut sampler = ResourceSampler::start(Arc::clone(&channel_stats));

    // Detect if input is a directory (swarm mode) or a single file
    let input_path = settings.input_path()?;
    let is_directory = input_path.is_dir();

    // Run the appropriate pipeline mode
    let etl_result = if is_directory {
        log!(logger, "[INFO] Swarm mode activated: processing directory");

        // Load sidecar FASTA once, shared across all workers
        let sidecar_fasta = if let Some(ref path) = settings.storage.fasta_sidecar_path {
            let map = load_fasta_map(path)?;
            Some(Arc::new(map))
        } else {
            None
        };

        // In swarm mode, output_path is treated as a directory
        let output_dir = &settings.storage.output_path;
        run_swarm_pipeline(input_path, output_dir, &settings, &metrics, sidecar_fasta)
    } else {
        // Single file mode (legacy behavior)
        run_etl_pipeline(&settings, &metrics, &channel_stats)
    };

    // Stop the sampler
    sampler.stop();

    // Generate report (even on error)
    let status = match &etl_result {
        Ok(()) => RunStatus::Success,
        Err(e) => RunStatus::Error {
            message: format!("{:#}", e),
        },
    };

    let report = RunReport::generate(&run_context, &metrics, &sampler, status);

    // Attempt to save report
    if let Err(e) = report.save_yaml(&run_context.report_path()) {
        log!(logger, "[ERROR] Failed to save report: {}", e);
    } else {
        log!(
            logger,
            "[INFO] Report saved to {}",
            run_context.report_path().display()
        );
    }

    // Print metrics summary
    print_summary_to_tee(&metrics, &mut logger);

    // Stop and join progress bar thread
    progress_running.store(false, Ordering::Relaxed);
    let _ = progress_handle.join();

    // Cleanup old runs
    if let Err(e) = cleanup_old_runs(&settings.runs.runs_dir, settings.runs.keep_runs) {
        log!(logger, "[WARN] Failed to cleanup old runs: {}", e);
    }

    // Return the ETL result
    etl_result
}

/// Process a single XML file through the ETL pipeline.
/// Creates its own channel and writer thread for complete isolation.
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


/// Derive output parquet path from input XML path.
/// Handles both .xml and .xml.gz extensions.
fn derive_output_path(input_path: &Path, output_dir: &Path) -> Result<std::path::PathBuf> {
    let file_name = input_path
        .file_name()
        .ok_or_else(|| anyhow!("Input path has no filename: {}", input_path.display()))?
        .to_string_lossy();

    // Strip .gz if present, then .xml
    let stem = file_name
        .strip_suffix(".gz")
        .unwrap_or(&file_name);
    let stem = stem
        .strip_suffix(".xml")
        .unwrap_or(stem);

    Ok(output_dir.join(format!("{}.parquet", stem)))
}

/// Run the ETL pipeline in swarm mode: process all XML files in a directory in parallel.
fn run_swarm_pipeline(
    input_dir: &Path,
    output_dir: &Path,
    settings: &Settings,
    metrics: &Metrics,
    sidecar_fasta: Option<Arc<HashMap<String, String>>>,
) -> Result<()> {
    // Create output directory if it doesn't exist
    fs::create_dir_all(output_dir)?;

    // Find all XML files (both .xml and .xml.gz)
    let pattern_xml = input_dir.join("*.xml").to_string_lossy().to_string();
    let pattern_gz = input_dir.join("*.xml.gz").to_string_lossy().to_string();

    let mut files: Vec<std::path::PathBuf> = Vec::new();

    for pattern in [&pattern_xml, &pattern_gz] {
        for entry in glob(pattern)? {
            match entry {
                Ok(path) => files.push(path),
                Err(e) => eprintln!("[WARN] Failed to read glob entry: {}", e),
            }
        }
    }

    if files.is_empty() {
        return Err(anyhow!(
            "No XML files found in directory: {}",
            input_dir.display()
        ));
    }

    eprintln!("[INFO] Swarm mode: found {} XML files to process", files.len());

    // Track failures across parallel execution
    let failure_count = Arc::new(AtomicUsize::new(0));

    // Process files in parallel using rayon with per-file local metrics
    files.par_iter().for_each(|input_path| {
        let output_path = match derive_output_path(input_path, output_dir) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("[ERROR] Failed to derive output path for {}: {}", input_path.display(), e);
                failure_count.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };

        eprintln!("[INFO] Processing: {} -> {}", input_path.display(), output_path.display());

        // Create thread-local metrics for this file (zero cross-thread contention)
        // The Mutex is uncontended since each worker operates on its own LocalMetricsAdapter
        let local_metrics_adapter = LocalMetricsAdapter::new();

        if let Err(e) = process_single_file(
            input_path,
            &output_path,
            settings,
            &local_metrics_adapter,
            sidecar_fasta.clone(),
        ) {
            eprintln!("[ERROR] Failed to process {}: {:#}", input_path.display(), e);
            failure_count.fetch_add(1, Ordering::Relaxed);
        }

        // Merge local metrics into global (1 atomic operation per metric field)
        local_metrics_adapter.merge_into(metrics);
    });

    let failures = failure_count.load(Ordering::Relaxed);
    if failures > 0 {
        Err(anyhow!(
            "Swarm completed with {} file(s) failed out of {}",
            failures,
            files.len()
        ))
    } else {
        eprintln!("[INFO] Swarm completed successfully: {} files processed", files.len());
        Ok(())
    }
}

/// Legacy wrapper for single-file mode that maintains backwards compatibility.
fn run_etl_pipeline(
    settings: &Settings,
    metrics: &Metrics,
    _channel_stats: &Arc<ChannelStats>,
) -> Result<()> {
    let input_path = settings.input_path()?;
    let output_path = &settings.storage.output_path;

    // Load sidecar FASTA (shared for single file mode)
    let sidecar_fasta = if let Some(ref path) = settings.storage.fasta_sidecar_path {
        let map = load_fasta_map(path)?;
        Some(Arc::new(map))
    } else {
        None
    };

    process_single_file(input_path, output_path, settings, metrics, sidecar_fasta)
}

fn print_summary_to_tee(metrics: &Metrics, logger: &mut TeeWriter) {
    let elapsed = metrics.elapsed_secs();
    let entries = metrics.entries();
    let batches = metrics.batches();
    let bytes_read = metrics.bytes_read();
    let bytes_written = metrics.bytes_written();
    let features = metrics.features();
    let isoforms = metrics.isoforms();
    let ptm_attempted = metrics.ptm_attempted();
    let ptm_mapped = metrics.ptm_mapped();
    let ptm_failed = metrics.ptm_failed();

    let entries_per_sec = entries as f64 / elapsed;
    let mb_read = bytes_read as f64 / (1024.0 * 1024.0);
    let mb_written = bytes_written as f64 / (1024.0 * 1024.0);

    log!(logger, "");
    log!(logger, "=== ETL Summary ===");
    log!(logger, "Entries parsed:  {}", entries);
    log!(logger, "Batches written: {}", batches);
    log!(logger, "PTMs attempted:  {}", ptm_attempted);
    log!(logger, "PTMs mapped:     {}", ptm_mapped);
    log!(logger, "PTMs failed:     {}", ptm_failed);
    log!(logger, "Features:        {}", features);
    log!(logger, "Isoforms:        {}", isoforms);
    log!(logger, "Time elapsed:    {:.2}s", elapsed);
    log!(
        logger,
        "Throughput:      {:.0} entries/sec",
        entries_per_sec
    );
    log!(logger, "Bytes read:      {:.2} MB", mb_read);
    log!(logger, "Bytes written:   {:.2} MB", mb_written);
}
