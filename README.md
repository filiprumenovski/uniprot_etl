# UniProt ETL: High-Performance XML to Parquet Converter

[![JOSS Status](https://joss.theoj.org/papers/10.21105/joss.00000/status.svg)](https://joss.theoj.org/papers/10.21105/joss.00000)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**UniProt ETL** is a high-throughput, streaming engine designed to convert the massive UniProtKB/Swiss-Prot XML datasets into Apache Parquet. 

Built with Rust, it enables **commodity hardware** (e.g., a standard laptop) to parse, process, and analyze the entire UniProt database in minutes—tasks that previously required high-memory workstations or clusters.

## Statement of Need

Modern biological research increasingly relies on high-throughput proteomics, where UniProt serves as the central hub for protein sequence and functional information. However, the sheer size of UniProtKB data dumps (exceeding 100GB uncompressed XML) poses a significant computational bottleneck for bioinformaticians and proteomic researchers. Existing tools often require high-memory infrastructure or lack the ability to preserve hierarchically structured data like isoforms and feature annotations. **UniProt ETL** addresses this gap by providing a resource-efficient, streaming solution that converts these massive datasets into Apache Parquet. This enables researchers to perform performant, SQL-like analytics on commodity hardware, significantly accelerating the transition from raw data dumps to actionable biological insights.

## Why Parquet?

The traditional UniProt XML dumps are massive (>100GB uncompressed), difficult to query, and require expensive DOM parsing. 

This tool democratizes access to this data by converting it to **Apache Parquet**, offering:
- **Columnar Storage**: Perform super-fast analytics. select only the columns you need (e.g., "Get all protein sequences for organism 9606").
- **Nested Schema**: Preserves the biological hierarchy (isoforms, features, evidence) using Parquet's `LIST` and `STRUCT` types, without flattening/duplication.
- **Massive Compression**: Reduces disk footprint by ~60x (e.g., 8.8GB XML stream $\rightarrow$ 140MB Parquet).
- **Ecosystem Compatible**: Ready for DuckDB, Polars, PyArrow, Spark, and Pandas.

## Output Schema

The resulting Parquet file uses a **nested schema** to preserve the full biological context of each entry without duplication.

```text
root
├── id: string (Primary Accession)
├── sequence: string (Canonical AA Sequence)
├── organism_id: int32 (NCBI TaxID)
├── entry_name: string
├── gene_name: string
├── protein_name: string
├── isoforms: list
│   └── item: struct
│       ├── isoform_id: string
│       ├── isoform_sequence: string
│       └── isoform_note: string
├── features: list
│   └── item: struct
│       ├── feature_type: string (e.g., "chain", "domain")
│       ├── description: string
│       ├── start: int32
│       ├── end: int32
│       └── evidence_code: string (ECO codes)
├── active_sites: list
│   └── item: struct
│       ├── description: string
│       ├── start: int32
│       └── ...
└── [Enriched Columns: domains, binding_sites, natural_variants, etc.]
```

> [!TIP]
> **Community Feedback**: We are actively seeking input from the scientific community to refine this schema. If you have suggestions for improving the data model to better serve your research needs, please open an issue or discussion!

## Key Features

- **🚀 Streaming Architecture**: Uses an event-driven XML parser (`quick-xml`) to process gigabytes of data with constant, low memory usage (<1GB RAM).
- **⚡ Parallel Processing**: "Swarm Mode" utilizes all available CPU cores (`rayon`) for parsing and transformation.
    - *Tip: For best results, download the **Taxonomic Division** files (e.g., `vertebrates`, `plants`) and run in directory mode. This allows the swarm to process multiple files simultaneously.*
- **🛡️ Zero-Copy Design**: Minimizes memory allocations for maximum throughput.
- **🧬 Biological Fidelity**: Preserves all feature evidence, isoform sequences, and subcellular locations.
- **📊 Observability**: Built-in Prometheus metrics server for real-time performance monitoring.

## Isoform Resolution Algorithm

> [!NOTE]
> **Requirement**: Populating isoform sequences requires the **`varsplic.fasta`** sidecar file. The main XML dump contains only the canonical sequence; the FASTA file is essential for resolving and validating splice variant sequences during the ETL process.

A rigorous coordinate mapping system ensures that features (e.g., Active Sites, PTMs) defined on the canonical sequence are correctly projected onto alternative isoforms.

1.  **VSP-ID Scoping**: The parser captures `splice variant` features (VSP IDs) and links them to specific isoforms via "Alternative Product" comments.
2.  **Coordinate Shifting**: A per-isoform linear mapper applies insertions, deletions, and substitutions to shift downstream coordinates.
3.  **Ambiguity Handling**:
    *   **Deletions**: Features falling into deleted regions are rigorously flagged as `VspDeletionEvent`.
    *   **Indels**: Length-changing edits reject interior mappings (`VspUnresolvable`) to prevent "snapping" errors.
    *   **Heuristics**: "Phantom shifts" from metadata text (e.g., "See Ref 2") are detected and excluded from coordinate calculations.

## Performance

On a consumer-grade laptop (Apple M4, 16GB RAM):

| Metric | Result |
|--------|--------|
| **Input** | Swiss-Prot Full Pattern (590k entries) |
| **Speed** | **~24,800 entries/sec** |
| **Time** | **23.7 seconds** |
| **Peak RAM** | **~730 MB** |
| **Compression** | 8.8 GB Read $\rightarrow$ 140 MB Written |

*> See `benchmarks/` for detailed logs.*

## Build and Installation

### Prerequisites
- **Rust** (1.70+): `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- **Just** (Optional, for task running): `cargo install just`

### Build from Source

**Core CLI:**
```bash
git clone https://github.com/filiprumenovski/uniprot_etl.git
cd uniprot_etl
cargo build --release
# Binary available at: target/release/uniprot_etl
```

**GUI (Desktop App):**
```bash
# Requires Node.js and cargo-tauri
just gui-setup  # Install npm dependencies
just gui-build  # Build Tauri app
```

### Running Tests
To run the full test suite, execute:
```bash
cargo test
```

## Usage

### Downloading Data

> [!NOTE]
> UniProt ETL is designed primarily as a Command Line Interface (CLI) tool. While it is structured as a crate, the library API is internal and not currently documented for external consumption.

We provide a CLI to easily download UniProt datasets (Swiss-Prot, TrEMBL, and FASTA sidecars) directly from the FTP server:

```bash
# Download Swiss-Prot XML files (Taxonomic Divisions)
just download-sprot data/xml/sprot

# Download TrEMBL XML files (Taxonomic Divisions)
just download-trembl data/xml/trembl

# Download TrEMBL FASTA
just download-trembl-fasta data/fasta

# Download Swiss-Prot Varsplic FASTA (required for isoform resolution)
just download-sprot-varsplic data/fasta
```

> [!IMPORTANT]
> **Unzip FASTA Files**: The FASTA files are downloaded as `.gz` archives. You **must unzip them** before passing them to the `--fasta-sidecar` argument (e.g., `gunzip data/fasta/uniprot_sprot_varsplic.fasta.gz`). The XML parser, however, **natively supports** `.xml.gz` files, so you do *not* need to unzip the XML dumps.

### Basic Conversion
Convert a gzipped UniProt XML dump to Parquet:

```bash
./target/release/uniprot_etl --input uniprot_sprot.xml.gz --output uniprot.parquet
```

### With Isoform Sequences
Attach a sidecar FASTA file (e.g., `varsplic.fasta`) to populate isoform sequences:

```bash
./target/release/uniprot_etl \
  --input uniprot_sprot.xml.gz \
  --fasta-sidecar uniprot_sprot_varsplic.fasta \
  --output uniprot-isoforms.parquet
```

### Configuration
You can also use a `config.yaml` file for reproducible runs:

```yaml
storage:
  input_path: "data/uniprot_sprot.xml.gz"
  output_path: "data/output.parquet"
performance:
  batch_size: 10000
  thread_count: 8
```

Run with:
```bash
./target/release/uniprot_etl --config config.yaml
```

## Data Analysis Example

Once converted, query your data instantly using [DuckDB](https://duckdb.org/):

```sql
-- Count proteins by organism
SELECT 
    organism_id, 
    COUNT(*) as protein_count 
FROM 'uniprot.parquet' 
GROUP BY organism_id 
ORDER BY protein_count DESC 
LIMIT 5;
```

## Project Structure

```text
.
├── Cargo.toml          # Workspace configuration
├── LICENSE
├── README.md
├── benchmarks/         # Performance benchmarks and logs
│   └── 2026-01-31_swissprot_full.summary.txt
├── config.yaml         # Default configuration
├── docs/               # Architecture Decision Records (ADRs)
│   ├── adr/
│   │   ├── 0002-streaming-xml-quick-xml.md
│   │   ├── 0004-nested-parquet-schema.md
│   │   └── 0007-isoform-scoped-vsp-mapping.md
│   └── ...
├── gui/                # Desktop GUI (Tauri + Next.js)
│   ├── frontend/       # React/Next.js frontend code
│   └── src-tauri/      # Rust backend for Tauri
├── scripts/            # Helper scripts (Python)
├── src/                # Core Rust Source Code
│   ├── bin/            # Utility binaries (inspect, query, filter)
│   ├── pipeline/       # ETL Logic
│   │   ├── parser.rs   # XML Event Parser
│   │   ├── mapper.rs   # Coordinate Mapper
│   │   └── ...
│   ├── writer/         # Parquet Writer
│   ├── observability/  # Metrics Server
│   ├── schema.rs       # Arrow Schema Definition
│   ├── lib.rs
│   └── main.rs
└── tests/              # Integration Tests
    ├── biological_validation.rs
    ├── pipeline_parse.rs
    └── ...
```

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on setting up your development environment and submitting PRs.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Citation

If you use UniProt ETL in your research, please cite it as:

> [Placeholder for JOSS Citation]
