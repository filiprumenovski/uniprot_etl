# ADR-0010: Modular Pipelined ETL Architecture

## Status

Accepted

## Context

The UniProt XML parser had grown into a monolithic state machine (`parser.rs` >700 LOC) with a sprawling `State` enum and an oversized `EntryScratch` that mixed transient parsing data with downstream business logic. Builders contained duplicated append logic for eight feature lists, and `Batcher` blended isoform biology (VSP mapping, FASTA lookups) with batching concerns. This made changes risky, reduced cache locality, and obscured ownership boundaries between parsing, transformation, and output.

## Decision

Adopt a modular pipeline:
- Delegate XML handling to sub-handlers (`handlers/{metadata,features,comments}.rs`), eliminating the global `State` enum and keeping the main parser loop shallow.
- Split parsing output into a `ParsedEntry` with focused feature/comment collections plus narrow scratch structs.
- Introduce `EntryTransformer` (`transformer.rs`) to own isoform/VSP expansion and sidecar FASTA lookups, producing `TransformedRow`.
- Simplify `Batcher` to accept `TransformedRow` and only batch/flush.
- DRY builder logic via `FeatureListBuilder` and move PTM mapping into `builders/ptm.rs`.
- Consolidate PTM failure counters into a struct within `Metrics`.

## Consequences

### Positive
- Clear ownership boundaries: parser only parses, transformer handles biology, batcher just batches.
- Reduced duplication in feature builders with a single generic appender.
- Easier to extend handlers for new tags without touching the core loop or scratch layout.

### Negative
- Slightly higher module count and glue code may marginally increase binary size.
- More indirection (Arc<ParsedEntry>, trait-based feature append) can add small runtime overhead if not optimized away.

### Notes

- Key modules: `src/pipeline/parser.rs`, `src/pipeline/handlers/*`, `src/pipeline/transformer.rs`, `src/pipeline/builders/{mod,common,ptm}.rs`, `src/pipeline/mapper.rs`.
- External API remains unchanged (CLI/config). Downstream schemas untouched.
