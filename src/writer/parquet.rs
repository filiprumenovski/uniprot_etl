use arrow::record_batch::RecordBatch;
use crossbeam_channel::Receiver;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{WriterProperties, WriterVersion};
use std::fs::File;
use std::path::Path;

use crate::config::Settings;
use crate::metrics::MetricsCollector;
use crate::schema::schema_ref;
use anyhow::{anyhow, Result};

/// Consumes RecordBatches from the channel and writes them to a Parquet file.
pub fn write_batches<M: MetricsCollector>(
    rx: Receiver<RecordBatch>,
    output: &Path,
    metrics: &M,
    settings: &Settings,
) -> Result<()> {
    let file = File::create(output)?;
    let props = writer_properties(settings)?;
    let mut writer = ArrowWriter::try_new(file, schema_ref(), Some(props))?;

    for batch in rx {
        let batch_bytes = batch.get_array_memory_size() as u64;
        writer.write(&batch)?;
        metrics.add_bytes_written(batch_bytes);
    }

    let file_metadata = writer.close()?;
    let row_groups = file_metadata.row_groups;
    let total_bytes: i64 = row_groups.iter().map(|rg| rg.total_byte_size).sum();
    eprintln!(
        "Wrote Parquet: {} (size: {:.2} MB)",
        output.display(),
        total_bytes as f64 / (1024.0 * 1024.0)
    );

    Ok(())
}

/// Creates optimized WriterProperties for UniProt data from Settings.
fn writer_properties(settings: &Settings) -> Result<WriterProperties> {
    let zstd_level = ZstdLevel::try_new(settings.performance.zstd_level as i32)
        .map_err(|e| anyhow!("Invalid zstd_level: {}", e))?;

    Ok(WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::ZSTD(zstd_level))
        // Use dictionary encoding for string columns (good for repeated values)
        .set_column_encoding("id".into(), Encoding::PLAIN)
        .set_column_encoding("sequence".into(), Encoding::PLAIN)
        .set_dictionary_enabled(true)
        // Row group size: balance between compression and random access
        .set_max_row_group_size(settings.performance.max_row_group_size)
        .build())
}
