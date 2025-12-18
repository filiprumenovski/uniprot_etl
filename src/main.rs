mod cli;
mod config;
mod error;
mod metrics;
mod pipeline;
mod report;
mod runs;
mod sampler;
mod schema;
mod writer;

use anyhow::Result;
use clap::Parser;
use crossbeam_channel::bounded;
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
    settings = settings.merge_with_cli(args.input, args.output, args.batch_size);

    // Resolve paths relative to current working directory (project root)
    let root = env::current_dir()?;
    settings.resolve_paths(&root)?;

    // Create run context (timestamped directory)
    let run_context = RunContext::new(&settings.runs.runs_dir)?;

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
                "entries: {} ({:.0}/s) | batches: {} | features: {} | isoforms: {} | read: {:.2} MB | written: {:.2} MB",
                entries, eps, batches, features, isoforms, mb_read, mb_written
            ));
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
        pb.finish_and_clear();
    });

    // Create channel stats for backpressure tracking
    let channel_stats = Arc::new(ChannelStats::new(settings.performance.channel_capacity));

    // Start resource sampler (background thread sampling at 1Hz)
    let mut sampler = ResourceSampler::start(Arc::clone(&channel_stats));

    // Create bounded channel with configured capacity
    let (tx, rx) = bounded(settings.performance.channel_capacity);

    // Run the ETL pipeline and capture result
    let etl_result = run_etl_pipeline(&settings, &metrics, tx, rx, &channel_stats);

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

fn run_etl_pipeline(
    settings: &Settings,
    metrics: &Metrics,
    tx: crossbeam_channel::Sender<arrow::array::RecordBatch>,
    rx: crossbeam_channel::Receiver<arrow::array::RecordBatch>,
    _channel_stats: &Arc<ChannelStats>,
) -> Result<()> {
    // Writer thread: consumes RecordBatches, writes Parquet
    let output_path = settings.storage.output_path.clone();
    let writer_metrics = metrics.clone();
    let writer_settings = settings.clone();
    let writer_handle =
        thread::spawn(move || write_batches(rx, &output_path, &writer_metrics, &writer_settings));

    // Parser thread: reads XML, produces RecordBatches
    // Pass metrics to create_xml_reader for bytes tracking
    let reader = create_xml_reader(settings.input_path()?, settings, metrics)?;

    // Run the parser
    let parse_result = parse_entries(reader, tx, metrics, settings.performance.batch_size);

    // Wait for writer to finish
    let writer_result = writer_handle.join().expect("Writer thread panicked");

    // Propagate any errors
    parse_result?;
    writer_result?;

    Ok(())
}

fn print_summary_to_tee(metrics: &Metrics, logger: &mut TeeWriter) {
    let elapsed = metrics.elapsed_secs();
    let entries = metrics.entries();
    let batches = metrics.batches();
    let bytes_read = metrics.bytes_read();
    let bytes_written = metrics.bytes_written();
    let features = metrics.features();
    let isoforms = metrics.isoforms();

    let entries_per_sec = entries as f64 / elapsed;
    let mb_read = bytes_read as f64 / (1024.0 * 1024.0);
    let mb_written = bytes_written as f64 / (1024.0 * 1024.0);

    log!(logger, "");
    log!(logger, "=== ETL Summary ===");
    log!(logger, "Entries parsed:  {}", entries);
    log!(logger, "Batches written: {}", batches);
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
