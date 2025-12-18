use anyhow::{anyhow, Result};
use clap::Parser;
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::array::{Array, BooleanBuilder, Int32Array, RecordBatchReader};
use arrow::compute::filter as filter_array;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{WriterProperties, WriterVersion};

/// Filter UniProt Parquet by NCBI TaxIDs (human/mouse/rat) and
/// produce three species-specific Parquet files next to the input.
#[derive(Parser, Debug)]
#[command(name = "filter_taxa")]
#[command(about = "Split UniProt Parquet into human/mouse/rat by organism_id")]
pub struct Args {
    /// Path to input Parquet file
    #[arg(short, long)]
    pub input: PathBuf,

    /// Optional output directory (defaults to input file's directory)
    #[arg(short, long)]
    pub outdir: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let input_path = args.input;
    let outdir = args.outdir.unwrap_or_else(|| PathBuf::from("data/species"));

    if !input_path.exists() {
        return Err(anyhow!("Input Parquet not found: {}", input_path.display()));
    }
    std::fs::create_dir_all(&outdir)?;

    // Build a RecordBatch reader from Parquet
    let file = File::open(&input_path)?;
    let mut rb_reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(64_000)
        .build()?;

    let schema = rb_reader.schema();

    // Prepare output writers for each species
    let props = writer_properties();
    let base = input_stem(&input_path)?;
    let human_path = outdir.join(format!("{}__human.parquet", base));
    let mouse_path = outdir.join(format!("{}__mouse.parquet", base));
    let rat_path = outdir.join(format!("{}__rat.parquet", base));

    let mut human_writer = ArrowWriter::try_new(
        File::create(&human_path)?,
        schema.clone(),
        Some(props.clone()),
    )?;
    let mut mouse_writer = ArrowWriter::try_new(
        File::create(&mouse_path)?,
        schema.clone(),
        Some(props.clone()),
    )?;
    let mut rat_writer = ArrowWriter::try_new(
        File::create(&rat_path)?,
        schema.clone(),
        Some(props.clone()),
    )?;

    // Stream through batches and route rows by organism_id
    while let Some(batch) = rb_reader.next() {
        let batch = batch?;
        let organism_idx = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == "organism_id")
            .ok_or_else(|| anyhow!("Column 'organism_id' not found in schema"))?;

        let org_col = batch.column(organism_idx);
        let org = org_col
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| anyhow!("Column 'organism_id' is not Int32"))?;

        // Build masks for each species
        let mut human_mask = BooleanBuilder::new();
        let mut mouse_mask = BooleanBuilder::new();
        let mut rat_mask = BooleanBuilder::new();

        // builders grow as needed

        for i in 0..org.len() {
            if org.is_null(i) {
                human_mask.append_value(false);
                mouse_mask.append_value(false);
                rat_mask.append_value(false);
                continue;
            }
            let v = org.value(i);
            human_mask.append_value(v == 9606);
            mouse_mask.append_value(v == 10090);
            rat_mask.append_value(v == 10116);
        }

        let human_mask = human_mask.finish();
        let mouse_mask = mouse_mask.finish();
        let rat_mask = rat_mask.finish();

        // Filter the batch per species and write if non-empty
        if let Some(filtered) = filter_batch(&batch, &human_mask)? {
            human_writer.write(&filtered)?;
        }
        if let Some(filtered) = filter_batch(&batch, &mouse_mask)? {
            mouse_writer.write(&filtered)?;
        }
        if let Some(filtered) = filter_batch(&batch, &rat_mask)? {
            rat_writer.write(&filtered)?;
        }
    }

    // Close writers to flush metadata
    human_writer.close()?;
    mouse_writer.close()?;
    rat_writer.close()?;

    eprintln!(
        "Wrote:\n  - {}\n  - {}\n  - {}",
        human_path.display(),
        mouse_path.display(),
        rat_path.display()
    );

    Ok(())
}

fn filter_batch(
    batch: &RecordBatch,
    mask: &arrow::array::BooleanArray,
) -> Result<Option<RecordBatch>> {
    // Short-circuit if no rows match
    if mask.true_count() == 0 {
        return Ok(None);
    }

    let filtered_cols = batch
        .columns()
        .iter()
        .map(|col| filter_array(col.as_ref(), mask))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let filtered = RecordBatch::try_new(batch.schema().clone(), filtered_cols)?;
    Ok(Some(filtered))
}

fn input_stem(path: &Path) -> Result<String> {
    let file_name = path
        .file_name()
        .ok_or_else(|| anyhow!("Invalid input path"))?
        .to_string_lossy()
        .into_owned();
    Ok(file_name.trim_end_matches(".parquet").to_string())
}

fn writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_column_encoding("id".into(), Encoding::PLAIN)
        .set_column_encoding("sequence".into(), Encoding::PLAIN)
        .set_dictionary_enabled(true)
        .set_max_row_group_size(100_000)
        .build()
}
