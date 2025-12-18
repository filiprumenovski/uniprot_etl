# ADR-0004: Nested Arrow/Parquet Schema to Preserve Biological Hierarchy and Evidence Provenance

## Status

Accepted

## Context

UniProt entries exhibit rich nested structure:
- **Protein sequences:** each entry has one canonical sequence; may have isoforms with alternative sequences.
- **Features:** molecular function, transmembrane regions, domains, post-translational modifications; each tied to zero or more evidence codes (ECO).
- **Subcellular locations:** cellular compartments; each with evidence links.
- **Isoforms:** alternative products (splicing, truncation); each with its own sequence and evidence.

**Flatness vs. fidelity trade-off:**
- **Flat schema (e.g., one row per feature):** explodes row count and duplicates protein metadata; hard to reconstruct the entry and its evidence graph.
- **Nested schema (struct/array types):** preserves hierarchy; Parquet natively supports LIST and STRUCT types.

**Evidence provenance requirement:**
- Every feature, location, and isoform carries references to evidence codes (ECO).
- Downstream analyses rely on evidence origin to filter low-confidence predictions.
- Lossless preservation is a hard requirement for scientific reproducibility.

## Decision

Adopt a **nested Arrow/Parquet schema** with the following structure:

```rust
Entry {
  id: Utf8,
  sequence: Utf8,
  organism_id: Int32,
  isoforms: List<Isoform>,
  features: List<Feature>,
  locations: List<Location>,
}

Isoform {
  id: Utf8,
  sequence: Utf8,
  note: Utf8,
}

Feature {
  feature_type: Utf8,
  description: Utf8,
  start: UInt32,
  end: UInt32,
  evidence: Utf8,  // semicolon-joined ECO strings
}

Location {
  location: Utf8,
  evidence: Utf8,  // semicolon-joined ECO strings
}
```

**Rationale:**
- **Minimal row count:** one row per entry; isoforms/features/locations stored as nested lists.
- **Evidence links:** evidence strings are joined and stored per feature/location; can be parsed by downstream tools.
- **Compatibility:** Parquet reader libraries (Python, R, DuckDB, Spark) handle nested types correctly.
- **Lossless:** schema accurately reflects UniProt XML structure; no data aggregation or loss.

**Implementation:**
- [src/schema.rs](../../src/schema.rs): defines Arrow schema and builder boilerplate.
- [src/pipeline/builders.rs](../../src/pipeline/builders.rs): populates nested arrays and structs from parsed entries.
- [src/pipeline/state.rs](../../src/pipeline/state.rs): entry-local `evidence_map` resolves evidence references on flush.

## Consequences

### Positive
- **Hierarchical preservation:** isoforms, features, locations remain grouped by entry; downstream queries can reconstruct relationships.
- **Evidence traceability:** ECO codes are stored inline; tools can filter by evidence confidence without external lookup.
- **Efficient storage:** nested Parquet is more compact than normalized tables; fewer duplicate columns.
- **Query-friendly:** modern analytics tools (DuckDB, Spark) handle nested types natively; no manual joins needed.

### Negative
- **Nested query complexity:** some tools or analysts unfamiliar with nested Parquet may need learning curve; mitigated by clear documentation and examples.
- **Flattening overhead:** if downstream requires flat tables, explicit unnesting step needed (e.g., DuckDB `UNNEST`, pandas `explode`).
- **Schema evolution risk:** adding new nested fields requires careful backward compatibility handling; Parquet versioning mitigates but requires vigilance.

### Notes

**Schema constraints (locked):**
- `id`, `sequence`, `organism_id` are required scalars per entry.
- `isoforms`, `features`, `locations` are optional lists (may be empty).
- Evidence strings are semicolon-joined; downstream parsers should split on `;` to recover individual ECO codes.

**Evidence resolution:**
- UniProt XML stores evidence as references (`<evidence key="ECO:..."/>`) to an entry-local map.
- Parser state machine resolves references during entry flush; final Parquet stores joined ECO strings.
- See [src/pipeline/state.rs](../../src/pipeline/state.rs) for `evidence_map` logic.

**Benchmarking:**
- [benches/flamegraph_benchmark.rs](../../benches/flamegraph_benchmark.rs) profiles schema construction and Parquet write.
- Row group size (default 100k) balances Parquet metadata size and query granularity.

**Testing:**
- [tests/pipeline_parse.rs](../../tests/pipeline_parse.rs): verifies nested arrays are correctly populated.

**Future:**
- Consider Protobuf or Avro for schema versioning and cross-language interop if ecosystem expands.
- Explore DuckDB Iceberg integration for incremental updates (append-only UniProt dumps).
