mod cli;
mod config;
mod error;
mod metrics;
mod pipeline;
mod schema;
mod writer;

use anyhow::Result;
use clap::Parser;
use crossbeam_channel::bounded;
use std::env;
use std::thread;

use crate::cli::Args;
use crate::config::Settings;
use crate::metrics::Metrics;
use crate::pipeline::parser::parse_entries;
use crate::pipeline::reader::create_xml_reader;
use crate::writer::parquet::write_batches;

fn main() -> Result<()> {
    let args = Args::parse();

    // Load settings from YAML, with CLI overrides
    let mut settings = Settings::load_from_yaml(args.config.as_deref())?;
    settings = settings.merge_with_cli(args.input, args.output, args.batch_size);

    // Resolve paths relative to current working directory (project root)
    let root = env::current_dir()?;
    settings.resolve_paths(&root)?;

    eprintln!("[INFO] Configuration ready");
    eprintln!("[INFO]   Input: {}", settings.input_path()?.display());
    eprintln!(
        "[INFO]   Output: {}",
        settings.storage.output_path.display()
    );
    eprintln!("[INFO]   Batch size: {}", settings.performance.batch_size);
    eprintln!(
        "[INFO]   Channel capacity: {}",
        settings.performance.channel_capacity
    );
    eprintln!("[INFO]   Zstd level: {}", settings.performance.zstd_level);

    let metrics = Metrics::new();

    // Create bounded channel with configured capacity
    let (tx, rx) = bounded(settings.performance.channel_capacity);

    // Writer thread: consumes RecordBatches, writes Parquet
    let output_path = settings.storage.output_path.clone();
    let writer_metrics = metrics.clone();
    let writer_settings = settings.clone();
    let writer_handle =
        thread::spawn(move || write_batches(rx, &output_path, &writer_metrics, &writer_settings));

    // Parser thread: reads XML, produces RecordBatches
    let reader = create_xml_reader(settings.input_path()?, &settings)?;
    parse_entries(reader, tx, &metrics, settings.performance.batch_size)?;

    // Wait for writer to finish
    writer_handle.join().expect("Writer thread panicked")?;

    metrics.print_summary();
    Ok(())
}
