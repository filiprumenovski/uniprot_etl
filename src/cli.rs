use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "uniprot_etl")]
#[command(about = "High-throughput ETL for UniProtKB/Swiss-Prot XML to Apache Parquet")]
#[command(version)]
pub struct Args {
    /// Path to config YAML file (default: config.yaml in root)
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    /// Path to input UniProt XML file (supports .xml and .xml.gz)
    /// Overrides config.yaml value if provided
    #[arg(short, long)]
    pub input: Option<PathBuf>,

    /// Path to output Parquet file
    /// Overrides config.yaml value if provided
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Batch size (number of entries per RecordBatch)
    /// Overrides config.yaml value if provided
    #[arg(short, long)]
    pub batch_size: Option<usize>,

    /// Path to isoform sidecar FASTA (varsplic.fasta, unzipped)
    /// Overrides config.yaml value if provided
    #[arg(long)]
    pub fasta_sidecar: Option<PathBuf>,

    /// Override the generated run identifier (directory name under runs/).
    ///
    /// Intended for profiling/wrappers that need a deterministic run directory.
    /// If the value does not start with "run_", it will be prefixed.
    #[arg(long)]
    pub run_id: Option<String>,
}
