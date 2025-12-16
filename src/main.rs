mod cli;
mod error;
mod metrics;
mod pipeline;
mod schema;
mod writer;

use anyhow::Result;
use clap::Parser;
use crossbeam_channel::bounded;
use std::thread;

use crate::cli::Args;
use crate::metrics::Metrics;
use crate::pipeline::reader::create_xml_reader;
use crate::pipeline::parser::parse_entries;
use crate::writer::parquet::write_batches;

fn main() -> Result<()> {
    let args = Args::parse();
    let metrics = Metrics::new();

    // Bounded channel for RecordBatch transfer (4-8 batches in flight)
    let (tx, rx) = bounded(8);

    // Writer thread: consumes RecordBatches, writes Parquet
    let output_path = args.output.clone();
    let writer_metrics = metrics.clone();
    let writer_handle = thread::spawn(move || {
        write_batches(rx, &output_path, &writer_metrics)
    });

    // Parser thread: reads XML, produces RecordBatches
    let reader = create_xml_reader(&args.input)?;
    parse_entries(reader, tx, &metrics, args.batch_size)?;

    // Wait for writer to finish
    writer_handle.join().expect("Writer thread panicked")?;

    metrics.print_summary();
    Ok(())
}
