# ADR-0007: Isoform-scoped splice-variant (VSP) coordinate mapping

## Status

Accepted

## Context

This ETL explodes UniProtKB entries into one Parquet row per isoform, while preserving a canonical “parent” identity via `parent_id`. We also attempt to map and verify PTM (post-translational modification) features from canonical coordinates onto each isoform.

During audit-driven validation on Swiss-Prot Human XML + the UniProt varsplic FASTA sidecar, we observed an unexpectedly high PTM failure rate dominated by `residue_mismatch` and `isoform_oob`, with `vsp_deletion` stuck at 0 despite entries containing splice-induced deletions.

Investigation of the input revealed that the dataset encodes isoform edit operations via:
- `<feature type="splice variant" id="VSP_...">` with a `<location>` span, and often **no `<variation>` text** (the span implicitly denotes deletion).
- Isoforms reference the splice-variant feature ids via `<comment type="alternative products"> … <isoform> … <sequence type="described" ref="VSP_..."/> …`.

Two failures fell out of this:

1) **Global application of splice edits was incorrect**

Early mapper implementations applied splice-variant features at the entry level to all isoforms. This was wrong because different isoforms reference different `VSP_...` edits; applying all edits globally causes cascading coordinate drift and excessive `vsp_deletion` events.

2) **VSP refs were not being captured reliably**

UniProt often emits isoform `<sequence .../>` as a self-closing (empty) tag. We originally collected `VSP_...` ids in the `Start` event handler only, which meant isoforms frequently had an empty `vsp_ids` list, disabling isoform-scoped mapping and keeping `vsp_deletion` telemetry at 0.

Relevant modules:
- [src/pipeline/parser.rs](src/pipeline/parser.rs)
- [src/pipeline/scratch.rs](src/pipeline/scratch.rs)
- [src/pipeline/mapper.rs](src/pipeline/mapper.rs)
- [src/pipeline/batcher.rs](src/pipeline/batcher.rs)

## Decision

We will scope splice-variant coordinate edits to the specific isoform(s) that reference them.

Concretely:

1) **Capture splice-variant feature ids**

Parse feature attributes so `FeatureScratch` retains the `id="VSP_..."`.

2) **Capture per-isoform VSP references**

In the alternative-products comment parser, collect all `VSP_...` refs encountered in isoform `<sequence ...>` tags into `IsoformScratch.vsp_ids`.

Implementation note: because UniProt uses self-closing tags (`<sequence .../>`) heavily, the parser must capture these refs in both `Start` and `Empty` event handlers.

3) **Build a mapper per isoform**

Construct `CoordinateMapper` per isoform using only the subset of splice-variant features whose `id` is in that isoform’s `vsp_ids` list.

4) **Deletion semantics for splice variants without `<variation>`**

When a `splice variant` feature has a location span but no variation text, interpret the span as a deletion for mapping purposes.

This decision is validated operationally by:
- `ptm_failed_vsp_deletion` becoming non-zero (telemetry is working and deletions are being recognized).
- A large recovery in mapped PTMs, indicating reduced coordinate drift vs global edits.

Example empirical outcome (Swiss-Prot Human run):
- Before: `ptm_failed_vsp_deletion = 0`, `ptm_mapped ≈ 81k`, high mismatch/OOB.
- After: `ptm_failed_vsp_deletion ≈ 8k`, `ptm_mapped ≈ 113k`, mismatch/OOB decreased.

## Consequences

### Positive
- Correctness: splice-variant edits are applied only to the isoforms that declare them.
- Better PTM mapping fidelity: fewer spurious coordinate shifts reduces `residue_mismatch` and `isoform_oob`.
- Telemetry integrity: `vsp_deletion` counters reflect real deletion events instead of being suppressed (missing VSP refs) or inflated (global edits).
- Extensibility: per-isoform `vsp_ids` becomes a general hook for future isoform edit types that are similarly referenced.

### Negative
- Per-isoform mapper construction adds overhead (though the observed runs remain fast and parser-bound).
- The “missing variation implies deletion” rule is a heuristic; if UniProt introduces other splice-variant encodings without `<variation>`, this may need refinement.
- Requires careful parsing of XML empty tags to avoid silently disabling scoping.

### Notes

- Affected parsing and scoping logic:
  - [src/pipeline/parser.rs](src/pipeline/parser.rs)
  - [src/pipeline/scratch.rs](src/pipeline/scratch.rs)
  - [src/pipeline/batcher.rs](src/pipeline/batcher.rs)
  - [src/pipeline/mapper.rs](src/pipeline/mapper.rs)

- Operational verification:
  - Run reports saved under `runs/run_*/report.yaml`.

- Open questions / future refinements:
  - Consider narrowing the deletion heuristic by requiring an explicit location span (begin/end) and feature type `splice variant`.
  - Consider recording per-entry (or sampled) VSP-id counts to help detect parser regressions.
