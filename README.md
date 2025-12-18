# UniProt_ETL

A high-performance, memory-efficient ETL pipeline for transforming UniProt XML dumps into columnar Parquet format. Written in Rust with zero-copy streaming, nested schema preservation, and comprehensive evidence provenance tracking.

## Quick Start

### Prerequisites
- Rust 1.70+ (via [rustup](https://rustup.rs/))
- `just` task runner (install: `brew install just` or `cargo install just`)

### Setup & Common Tasks

```bash
# Install dependencies and run dev checks (fmt, lint, test)
just dev-check

# Build release binary
cargo build --release

# Run pipeline with default config
just run --input data/raw/uniprot_sprot.xml.gz --output data/parquet/output.parquet

# Run linter
just lint

# Run tests
just test

# Clean large data files (with confirmation)
just clean-data

# Clean with dry-run to preview
just clean-data-dry

# Profile with flamegraph
just profile-flamegraph bench="flamegraph_benchmark"
```

### Configuration

Edit [config.yaml](config.yaml) to customize:
- `batch_size`: Entries per Parquet row group (default 10,000).
- `thread_count`: Parser worker threads (currently fixed at 1, future multi-threaded support).
- `channel_capacity`: Bounded channel buffer size in batches (default 8).
- `buffer_size`: I/O buffer for XML reading (default 256KB).
- `zstd_level`: Compression level 1–22 (default 3; higher = smaller but slower).

## Architecture

UniProt_ETL is built on four key architectural decisions documented in [docs/adr/](docs/adr/):

1. **[ADR-0001: Rust](docs/adr/0001-rust-memory-safety.md)** — Memory safety, high throughput, and reproducibility.
2. **[ADR-0002: Event-Driven Streaming XML](docs/adr/0002-streaming-xml-quick-xml.md)** — Constant-memory parsing with quick-xml and gzip streaming.
3. **[ADR-0003: Producer-Consumer with crossbeam](docs/adr/0003-producer-consumer-crossbeam.md)** — I/O decoupling and backpressure.
4. **[ADR-0004: Nested Parquet Schema](docs/adr/0004-nested-parquet-schema.md)** — Hierarchical fidelity and evidence preservation.

### Data Flow

```
UniProt XML (gzip)
    ↓
[Event-driven parser (main thread)]
    ├─ Accumulate entries into state
    ├─ Resolve evidence codes (ECO)
    └─ Batch & send to channel
    ↓
[Writer thread]
    ├─ Receive batches
    ├─ Build Arrow arrays
    └─ Serialize to Parquet (Zstd)
    ↓
Columnar Parquet output
```

### Schema

Output Parquet preserves UniProt's nested structure:

```
id (Utf8)
sequence (Utf8)
organism_id (Int32)
isoforms (List<{id, sequence, note}>)
features (List<{feature_type, description, start, end, evidence}>)
locations (List<{location, evidence}>)
```

Evidence codes (ECO) are semicolon-joined strings; parse downstream as needed.

## Development

### Scripts

Located in [scripts/](scripts/):
- `clean_data.sh`: Remove generated data files; see [ADR-0003](docs/adr/0003-producer-consumer-crossbeam.md#notes) for tuning.
- `fetch_uniprot.sh`: Download UniProt datasets; requires `UNIPROT_URL` env var.
- `profile_flamegraph.sh`: Build and profile benchmarks with cargo-flamegraph.

### Testing

```bash
# Unit tests
just test

# Benchmarks (requires UniProt XML; see benches/README or docs)
just bench

# Flamegraph profiling (requires cargo-flamegraph)
just profile-flamegraph bench="flamegraph_benchmark"
```

### Code Organization

```
src/
├── main.rs              # CLI orchestration
├── cli.rs               # Clap argument parsing
├── config.rs            # YAML config + Settings
├── schema.rs            # Arrow schema definition
├── metrics.rs           # Performance counters
├── error.rs             # Error types
├── lib.rs               # Public module exports
├── pipeline/
│   ├── parser.rs        # Event-driven XML loop
│   ├── state.rs         # Entry state machine
│   ├── reader.rs        # File I/O + gzip
│   ├── builders.rs      # Arrow array builders
│   ├── batcher.rs       # Batch grouping
│   └── mod.rs           # Submodule exports
├── writer/
│   ├── parquet.rs       # Parquet serialization
│   └── mod.rs           # Submodule exports
└── bin/
    └── filter_taxa.rs   # Utility: split by organism_id
```

## Contributing

### Adding New Decisions

Copy [docs/adr/template.md](docs/adr/template.md) and increment the ADR number:

```bash
cp docs/adr/template.md docs/adr/000X-my-decision.md
```

Fill in Title, Status, Context, Decision, Consequences sections. Ensure Context links technical choices to biological requirements (e.g., evidence fidelity).

### Code Style

- **Formatting:** `just fmt`
- **Linting:** `just lint` (Clippy with `-D warnings`)
- **Testing:** `just test`

Run `just dev-check` to execute all three.

## Performance

Target metrics (validated in CI):
- **Memory:** <500MB for Swiss-Prot (~550k entries).
- **Speed:** <10 minutes on commodity hardware (4-core, 8GB RAM, SSD).
- **Throughput:** ~1M entries/min after warm-up.

See [benches/](benches/) for profiling scripts and [FLAMEGRAPH_INSTRUCTIONS.md](FLAMEGRAPH_INSTRUCTIONS.md) for detailed profiling guidance.

## Troubleshooting

### Out of Memory

Reduce `batch_size` and `channel_capacity` in [config.yaml](config.yaml):

```yaml
performance:
  batch_size: 5000
  channel_capacity: 4
```

### Slow Parquet Write

Reduce `zstd_level` for faster (but less-compressed) output:

```yaml
performance:
  zstd_level: 1
```

### Large Intermediate Files

Use `just clean-data --keep-parquet` to preserve output while removing raw/tmp files.

## References

- [UniProt Format Documentation](https://www.uniprot.org/help)
- [Apache Arrow & Parquet Specs](https://arrow.apache.org/)
- [Criterion Benchmarking](https://bheisler.github.io/criterion.rs/book/)
- [quick-xml Crate](https://docs.rs/quick-xml/)

## License

[Specify your license, e.g., MIT, Apache-2.0]

## Citation

If you use UniProt_ETL in research, please cite:

```bibtex
@software{uniprot_etl,
  title={UniProt_ETL: High-Performance ETL for Bioinformatics},
  author={Your Name},
  year={2024},
  url={https://github.com/yourorg/UniProt_ETL}
}
```
