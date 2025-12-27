mod cli;
mod config;
mod error;
mod fasta;
mod metrics;
mod observability;
mod pipeline;
mod report;
mod runs;
mod sampler;
mod schema;
mod writer;

use anyhow::Result;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use std::env;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;

use crate::cli::Args;
use crate::config::Settings;
use crate::metrics::Metrics;
use crate::observability::start_metrics_server;
use crate::pipeline::{run_pipeline, PipelineArgs};
use crate::report::{RunReport, RunStatus};
use crate::runs::{cleanup_old_runs, RunContext};
use crate::sampler::{ChannelStats, ResourceSampler};

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

    // Start Prometheus metrics server if enabled
    let metrics_server_handle = if settings.observability.enable_metrics_server {
        match start_metrics_server(
            metrics.clone(),
            Arc::clone(&channel_stats),
            settings.observability.metrics_bind_address.clone(),
        ) {
            Ok(handle) => {
                log!(
                    logger,
                    "[INFO] Metrics server started on http://{}",
                    settings.observability.metrics_bind_address
                );
                Some(handle)
            }
            Err(e) => {
                log!(logger, "[WARN] Failed to start metrics server: {}", e);
                log!(logger, "[WARN] Continuing without Prometheus metrics");
                None
            }
        }
    } else {
        log!(logger, "[INFO] Metrics server disabled by config");
        None
    };

    // Start resource sampler (background thread sampling at 1Hz)
    // Note: In swarm mode, this tracks a dummy channel; per-file channels are not monitored
    let mut sampler = ResourceSampler::start(Arc::clone(&channel_stats));

    // Run the pipeline via library API (auto-detects single-file vs swarm mode)
    let pipeline_args = PipelineArgs {
        settings: settings.clone(),
        metrics: metrics.clone(),
        channel_stats: Some(Arc::clone(&channel_stats)),
    };

    let etl_result = run_pipeline(&pipeline_args);

    // Stop the sampler
    sampler.stop();

    // Shutdown metrics server
    if let Some(handle) = metrics_server_handle {
        if let Err(e) = handle.shutdown() {
            log!(logger, "[WARN] Error shutting down metrics server: {}", e);
        }
    }

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
