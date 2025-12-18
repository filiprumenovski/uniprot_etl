# ADR-0006: Enrich Nomenclature Tissue and Structural Hooks for 3D Mapping

## Status

Accepted

## Context

The initial ETL captured core protein data (accession, sequence, organism_id, features, locations, isoforms) but lacked:
- "Nomenclature tissue" needed for multi-omic joins: gene and protein names.
- "Structural hooks" needed for downstream 3D tensorization: links to PDB and AlphaFoldDB.

Constraints:
- Memory footprint: maintain <500MB RAM; strictly event-driven parsing (no DOM).
- Zero-copy: reuse a single `text_buffer` in `EntryScratch` for textual capture.
- Schema fidelity: preserve biological hierarchy and avoid breaking existing column indices.

This ADR documents a non-breaking refactor that enriches metadata and prepares the schema for structural mapping.

## Decision

Append rich metadata columns to the Arrow/Parquet schema and extend the parser state machine to capture them efficiently:

- New top-level columns:
  - `entry_name: Utf8`
  - `gene_name: Utf8`
  - `protein_name: Utf8`
  - `organism_name: Utf8` (scientific)
  - `existence: Int8` (1–5 mapping, nullable when unknown)
  - `structures: List<Struct<db: Utf8, id: Utf8>>` (PDB/AlphaFoldDB)

- Parser state machine additions in `src/pipeline/state.rs`:
  - `EntryName`, `Gene`/`GeneName`, `Protein`/`RecommendedName`, `OrganismScientificName`, `ProteinExistence`
  - Mark `EntryName`, `GeneName`, `RecommendedName`, `OrganismScientificName` as text-capturing states.

- Scratch buffer in `src/pipeline/scratch.rs`:
  - Add optional fields: `entry_name`, `gene_name`, `protein_name`, `organism_scientific_name`
  - Add `existence: i8` (default 0) and `structures: Vec<StructureRef>`
  - Ensure `clear()` resets all fields for zero-copy reuse.

- Parser in `src/pipeline/parser.rs`:
  - Capture `<entry><name>` → `entry_name`
  - Capture `<gene><name type="primary">` → `gene_name`
  - Capture `<protein><recommendedName><fullName>` → `protein_name`
  - Capture `<organism><name type="scientific">` → `organism_scientific_name`
  - Map `<proteinExistence type="...">` → `existence: i8` (including self-closing tags)
  - Append structural hooks from `<dbReference type="PDB|AlphaFoldDB" id="...">`.

- Schema in `src/schema.rs`:
  - Append new fields after existing top-level columns to preserve indices used by current tests.

- Builders in `src/pipeline/builders.rs`:
  - Add corresponding builders and wire values into `append_entry()` and `finish_batch()`.

## Consequences

### Positive
- Enables high-speed joins across multi-omic datasets using gene/protein names.
- Prepares for 3D tensorization by providing direct PDB/AlphaFoldDB references.
- Maintains performance targets and avoids memory regression with event-driven, zero-copy parsing.
- Preserves existing schema indices; downstream code and tests remain stable.

### Negative
- Schema widening increases column count and Parquet size modestly.
- Some entries may lack `existence` or names; nullability is required and handled.

### Notes

- Mapping for `proteinExistence`:
  - "evidence at protein level" → 1
  - "evidence at transcript level" → 2
  - "inferred from homology" → 3
  - "predicted" → 4
  - "uncertain" → 5
  - Unknown/absent → null (internal default 0)

- Affected modules:
  - Parser: [src/pipeline/parser.rs](../../src/pipeline/parser.rs)
  - State: [src/pipeline/state.rs](../../src/pipeline/state.rs)
  - Scratch: [src/pipeline/scratch.rs](../../src/pipeline/scratch.rs)
  - Schema: [src/schema.rs](../../src/schema.rs)
  - Builders: [src/pipeline/builders.rs](../../src/pipeline/builders.rs)

- Validations:
  - Added test: [tests/pipeline_nomenclature.rs](../../tests/pipeline_nomenclature.rs)
  - Inspector tool: [src/bin/inspect_parquet.rs](../../src/bin/inspect_parquet.rs) confirms schema and sample values (e.g., TP53).

- Performance:
  - Real Swiss-Prot run confirms throughput and non-regression in entries/min.
  - Logs: check recent run in [runs/](../../runs/).

- Future:
  - Consider adding normalized views (DuckDB) for common join patterns.
  - Optionally add per-batch metrics for non-null `gene_name`/`protein_name` coverage.
