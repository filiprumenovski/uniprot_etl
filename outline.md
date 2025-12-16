# DeepFEHL Substrate Engine — Progress Summary (for Downstream Coding LLMs)

## Mission Recap
We are building a **high-throughput, zero-copy, streaming ETL engine in pure Rust** that converts **UniProtKB/Swiss-Prot XML** into a **lossless, nested Apache Parquet substrate** for DeepFEHL.

This substrate directly addresses the **precision crisis in O-GlcNAc proteomics** by preserving biological hierarchy, evidence provenance, and isoform fidelity—features destroyed by flat TSV/CSV pipelines.

---

## Key Gains So Far (Locked Decisions & Architecture)

### 1. Data Integrity Breakthroughs
- **Ghost Site Elimination**
  - Evidence codes (ECO) are preserved *per feature* in a nested structure.
  - Enables downstream filtering of low-quality artifacts (e.g. BEMAD) via simple columnar queries.
- **Albumin Test Enabled**
  - Structured parsing of *subcellular localization* allows filtering of serum contaminants (O-GalNAc).
- **Isoform Fidelity**
  - Isoform-specific sequences parsed from UniProt “alternative products”.
  - Enables correct AlphaFold / ESM-2 mapping for proteins like **Tau** and **c-Myc**.

---

### 2. Architectural Decisions (Hard Constraints Met)
- **Pure Rust Stack**
  - `quick-xml` (event-based, no DOM)
  - `arrow` + `parquet` (nested columnar)
  - `flate2` (streaming gzip)
  - `crossbeam-channel` (bounded producer–consumer)
- **Zero-DOM, Constant Memory**
  - Event-driven state machine.
  - Entry-local scratch buffers only.
  - Peak RAM capped by bounded channel + batch size.
- **Split-Disk I/O Strategy**
  - Read: external disk (`uniprot_sprot.xml.gz`)
  - Write: internal SSD (`.parquet`)
  - Prevents disk head contention, maximizes throughput.

---

### 3. Streaming Pipeline (Finalized)
External Disk (XML.gz)
→ BufReader (256KB)
→ GzDecoder
→ quick-xml Reader
→ Rust State Machine
→ Arrow Builders (Nested)
→ RecordBatch (10k entries)
→ crossbeam-channel (bounded)
→ Parquet Writer (Zstd lvl 3)
→ Internal SSD

---

### 4. Locked Arrow / Parquet Schema (MVP)
Top-level columns:
- `id: Utf8` — primary accession
- `sequence: Utf8` — AA string
- `organism_id: Int32` — NCBI TaxID

Nested columns:
- `isoforms: List<Struct>`
  - `isoform_id: Utf8`
  - `isoform_sequence: Utf8 (nullable)`
  - `isoform_note: Utf8 (nullable)`
- `features: List<Struct>`
  - `feature_type: Utf8`
  - `description: Utf8 (nullable)`
  - `start: Int32 (nullable)`
  - `end: Int32 (nullable)`
  - `evidence_code: Utf8 (nullable | semicolon-joined)`
- `location: List<Struct>`
  - `location: Utf8`
  - `evidence_code: Utf8 (nullable)`

**Why this matters:**  
The schema preserves one-to-many biological relationships while remaining fully columnar and ML-ready.

---

### 5. UniProt XML Parsing Strategy (Critical Insight)
- **Entry-local Evidence Resolution**
  - `<evidence key="X">` references are resolved via an entry-local `key → ECO` map.
  - Guarantees 100% evidence preservation for modified residues.
- **Robust Location Parsing**
  - Handles `<position>` vs `<begin>/<end>` variants.
  - Coordinates stored as `Option<Int32>` (null-safe).
- **Sequence Text Handling**
  - Sequence text may arrive in multiple XML text events → accumulated until closing tag.

---

### 6. Batching & Performance Targets
- **Batch size:** 10,000 proteins per `RecordBatch`
- **Compression:** Zstd level 3 (balanced IO vs CPU)
- **Concurrency:** Single writer thread, bounded channel (4–8 batches)
- **Target metrics:**
  - Swiss-Prot (~570k entries) in <10 minutes
  - Peak RAM <500MB
  - Pure Rust static binary

---

### 7. Implementation State
- Core architecture fully specified
- Schema finalized
- Parsing strategy locked
- Ready for incremental coding:
  1. CLI + I/O + empty pipeline
  2. Accession / sequence / organism
  3. Evidence table
  4. Features (modified residues)
  5. Subcellular location
  6. Isoforms

This spec is **coding-ready** and safe to hand off to downstream LLM agents without architectural drift.


### 8. Initial Repository Structure

uniprot_etl/
├── Cargo.toml
├── README.md
├── src/
│   ├── main.rs              # CLI entrypoint, thread orchestration
│   ├── cli.rs               # clap definitions (--input, --output)
│   ├── schema.rs            # Arrow Schema + field builders
│   ├── pipeline/
│   │   ├── mod.rs
│   │   ├── reader.rs        # split-disk I/O, gzip + buffered reader
│   │   ├── parser.rs        # quick-xml event loop + state machine
│   │   ├── state.rs         # enum State + transition logic
│   │   ├── scratch.rs       # EntryScratch + feature/location structs
│   │   ├── builders.rs      # Arrow ListBuilder / StructBuilder logic
│   │   └── batcher.rs       # 10k-entry batching → RecordBatch
│   ├── writer/
│   │   ├── mod.rs
│   │   └── parquet.rs       # ArrowWriter config (Zstd, dict encoding)
│   ├── error.rs             # thiserror / anyhow wrappers
│   └── metrics.rs           # counters, timing, throughput stats
└── benches/
    └── throughput.rs        # optional Criterion benchmark