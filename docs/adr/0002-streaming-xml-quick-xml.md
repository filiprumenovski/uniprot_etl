# ADR-0002: Event-Driven Streaming Parser with quick-xml for Constant Memory Usage

## Status

Accepted

## Context

UniProt XML (Swiss-Prot/TrEMBL) dumps are 400MB–1GB+ in size. **Naive DOM parsing is infeasible:**
- Loading entire XML into memory violates <500MB constraint.
- DOM construction allocates millions of temporary heap objects.
- Garbage collection (in languages that use it) introduces unpredictable pauses.

**Biological parsing needs:**
- Events must preserve whitespace-containing sequences (sequences split across XML text nodes).
- Gzip-compressed input must stream without intermediate decompression files.
- Configurable buffer sizing to balance memory footprint and I/O throughput.
- Robust handling of malformed or legacy XML (UniProt occasionally has quirks).

## Decision

Implement an **event-driven state machine** using the `quick-xml` crate for SAX-like parsing, combined with `flate2` for transparent gzip streaming.

**Key design:**
- **No DOM:** Each XML event (StartElement, EndElement, Text) is processed immediately and discarded.
- **Entry-local state:** Accumulate entry data (id, sequence, organism, features, isoforms) in a fast stack-allocated struct.
- **Gzip streaming:** [create_xml_reader](../../src/pipeline/reader.rs) auto-detects `.gz` extension and wraps `File` → `GzDecoder` → `BufReader` (configurable size from settings).
- **Whitespace handling:** quick-xml trim_text mode, but sequences reconstruct from multi-part Text events.

**Parser implementation:**
- [src/pipeline/parser.rs](../../src/pipeline/parser.rs): main event loop iterates quick-xml reader.
- [src/pipeline/state.rs](../../src/pipeline/state.rs): entry accumulation and field construction.
- [src/pipeline/scratch.rs](../../src/pipeline/scratch.rs): reusable buffers to avoid per-entry allocations.
- [src/pipeline/reader.rs](../../src/pipeline/reader.rs): file I/O and decompression setup.

## Consequences

### Positive
- **Constant memory:** Each entry occupies ~1KB; no accumulation overhead. Peak RAM <500MB for largest dumps.
- **Streaming gzip:** No temporary uncompressed files; input reads directly from compressed source.
- **Fast parsing:** Rust's regex-free state machine is ~2–3× faster than DOM-based JSON/BSON converters.
- **Fault tolerance:** Invalid XML elements are skipped; parsing continues (robust recovery).

### Negative
- **Complex state machine:** Logic spread across parser, state, and builders; harder to audit than DOM walk.
- **Sequence accumulation:** Sequences split across multiple XML Text events require manual concatenation; edge case handling needed.
- **Position tracking:** Quick-xml positions less detailed than DOM; error messages show approximate byte offsets.

### Notes

**Configuration:**
- `buffer_size` in [config.yaml](../../config.yaml): default 256KB; tune per storage device I/O characteristics.
- `batch_size`: logical grouping of entries before Parquet write; not tied to XML parsing granularity.

**Testing:**
- [tests/pipeline_parse.rs](../../tests/pipeline_parse.rs): unit tests for parser correctness on sample XML.
- [tests/pipeline_smoke.rs](../../tests/pipeline_smoke.rs): end-to-end; requires real UniProt XML.

**Evidence preservation:** Entry-local `evidence_map` in state resolves evidence references during flush (see [ADR-0004](0004-nested-parquet-schema.md)).

**Future:** Investigate SAX2 or Expat for XSLT support if schema transformations become common.
