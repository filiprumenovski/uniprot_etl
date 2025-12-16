use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "uniprot_etl")]
#[command(about = "High-throughput ETL for UniProtKB/Swiss-Prot XML to Apache Parquet")]
#[command(version)]
pub struct Args {
    /// Path to input UniProt XML file (supports .xml and .xml.gz)
    #[arg(short, long)]
    pub input: PathBuf,

    /// Path to output Parquet file
    #[arg(short, long)]
    pub output: PathBuf,

    /// Batch size (number of entries per RecordBatch)
    #[arg(short, long, default_value = "10000")]
    pub batch_size: usize,
}
