# ADR-0009: Enriched Feature Columns (Sites, Domains, Variants, Subunits, Interactions)

## Status

Accepted

## Context

The UniProt XML format contains multiple high-value annotation classes (features and comments) that are essential for downstream ML/analytics but are inconvenient to query efficiently in a generic, mixed `features` list.

Constraints and requirements for this pipeline:
- **Streaming / constant memory**: The parser must remain event-driven (quick-xml) and avoid building a DOM.
- **Evidence fidelity**: We must not filter “low confidence” annotations; instead preserve evidence provenance so downstream models can weight signals.
- **Isoform explosion**: Rows are exploded per isoform (when isoforms exist); coordinate-based annotations must be mapped to isoform coordinates using the same mapping machinery as PTMs.
- **Physical Truth Constraint**: Coordinate-based annotations must be validated against sequence length bounds to avoid emitting impossible coordinates.
- **Self-closing tags**: UniProt emits important coordinate elements as empty/self-closing tags, so extraction must handle both start/end and empty tag cases.

Related decisions and prior art:
- ADR-0002 (streaming XML)
- ADR-0004 (nested Parquet schema)
- ADR-0007 (isoform-scoped coordinate mapping)
- ADR-0008 (biological ceilings / validation behavior)

## Decision

Add eight dedicated nested Parquet columns for “high-ROI” UniProt annotations while maintaining the existing `features` column for backwards compatibility:

Category A (coordinate-based features):
- `active_sites`
- `binding_sites`
- `metal_coordinations`
- `mutagenesis_sites`
- `domains`

Category B:
- `natural_variants` (coordinate-based)
- `subunits` (comment text)
- `interactions` (interaction partners)

For each extracted record, store evidence provenance as:
- `evidence_code`: semicolon-joined resolved ECO codes
- `confidence_score`: derived numeric score used for downstream weighting

Coordinate-based features are validated against the canonical sequence and mapped per isoform row via `CoordinateMapper`.

## Consequences

### Positive
- Enables direct, typed, columnar access to frequently-used annotations without scanning heterogeneous feature bags.
- Preserves evidence and avoids hard filtering, enabling downstream ML calibration.
- Keeps streaming performance characteristics (constant memory) and supports self-closing tags.
- Maintains backwards compatibility by still populating the legacy `features` list.

### Negative
- Schema grows (more columns, more nested builders), increasing implementation surface area and requiring schema/builder ordering discipline.
- Some annotations may map imperfectly to isoforms (e.g., deletions, residue mismatches); these are emitted as mapping failures and require downstream interpretation.

### Notes

Affected modules:
- `src/schema.rs` (new columns)
- `src/pipeline/state.rs` (new parsing states)
- `src/pipeline/scratch.rs` (new scratch buffers)
- `src/pipeline/parser.rs` (feature routing, empty-tag coordinate capture, comment extraction)
- `src/pipeline/builders.rs` (new nested builders + isoform coordinate mapping)

Open questions / future refinements:
- Align interaction partner extraction more precisely to UniProt interaction comment sub-structure if needed.
- Fill optional fields like `metal` and `domain_name` when present in richer XML sub-elements.
