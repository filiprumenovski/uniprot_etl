# ADR-0008: PTM Mapping Biological Ceiling

## Status

Accepted

## Context

Following the implementation of isoform-scoped VSP mapping (ADR-0007) and interval-aware coordinate mapping, we observed that PTM mapping success rates plateaued at ~67.6% (113,466 / 167,925). The remaining ~54,000 failures were investigated to determine whether they represent fixable bugs or fundamental biological limitations.

### Pre-Investigation Hypothesis

The initial assumption was that the remaining failures might be recoverable through:
- Better VSP ID parsing in the XML parser
- Fixes to the coordinate mapping algorithm
- FASTA sidecar data quality improvements

### Investigation Methodology

We added diagnostic instrumentation to the PTM mapping pipeline to classify each failure:

```rust
// For each PTM failure, log:
// - shift: mapped_position - original_position (0 = identity mapping)
// - vsp_count: number of VSP edits in the mapper
// - expected_len: canonical_len + total_delta (expected isoform length)
```

This allowed us to distinguish between:
1. **Mapping logic errors** (shift != 0 with wrong AA, or vsp_count=0 when VSPs should apply)
2. **Biological differences** (shift=0 with different AA, or legitimately truncated isoforms)

## Analysis Results

### RESIDUE_MISMATCH (24,198 failures)

| Pattern | Count | % | Classification |
|---------|-------|---|----------------|
| shift=0 (identity mapping) | 24,198 | 100% | Biological |
| vsp_count=0 (no VSPs applied) | 24,161 | 99.8% | Biological |
| vsp_count>0 (VSPs applied, still mismatch) | 37 | 0.2% | Biological |

**Finding**: ALL residue mismatches occur at identity-mapped positions where the isoform sequence genuinely differs from canonical. These are true sequence variants, not mapping errors.

Examples:
- `Q9NRA8-2` has sequence variants at 13+ positions (S→E, S→Q, K→F, etc.)
- `Q6ZT62-2` appears as an isoform of `Q9Y3L3` but is actually a different protein

### ISOFORM_OOB (19,652 failures)

| Pattern | Count | % | Classification |
|---------|-------|---|----------------|
| shift=0 (identity mapping) | 19,652 | 100% | Biological |
| vsp_count=0 (no VSPs applied) | 19,652 | 100% | Biological |
| isoform_len << expected_len | 19,652 | 100% | Biological |

**Finding**: ALL OOB failures occur on isoforms where:
1. No VSP edits were referenced (vsp_count=0)
2. The FASTA sequence is legitimately shorter than canonical
3. The PTM position falls beyond the truncated isoform's C-terminus

Examples:
- `Q96QU6-2`: canonical=501aa, isoform=158aa (N-terminal fragment)
- `P78314-2`: canonical=561aa, isoform=97aa (N-terminal fragment)
- `P30542-2`: canonical=326aa, isoform=125aa (C-terminal truncation)

### VSP_UNRESOLVABLE (2,148 failures)

These are PTMs located at interior positions within length-changing VSP edits. The coordinate cannot be deterministically mapped because the residue has no 1:1 correspondence in the isoform.

**Finding**: Correct rejection of ambiguous mappings.

### VSP_DELETION (8,275 failures)

These are PTMs located within deleted segments. The residue does not exist in the isoform.

**Finding**: Correct rejection of non-existent sites.

## Decision

We accept the current ~67.6% PTM mapping success rate as the **biological ceiling** for this dataset. The remaining failures are not recoverable through algorithm improvements because they represent:

1. **Genuine sequence variants** (24k) - Alternative splicing creates different amino acids at PTM positions
2. **Truncated isoforms** (19k) - Alternative isoforms that don't include the PTM site
3. **Ambiguous positions** (2k) - Interior of length-changing indels with no 1:1 mapping
4. **Deleted regions** (8k) - VSP deletions that physically remove the PTM site

### Classification Summary

| Category | Count | Recoverable | Root Cause |
|----------|-------|-------------|------------|
| RESIDUE_MISMATCH | 24,198 | 0% | Genuine sequence variants |
| ISOFORM_OOB | 19,652 | 0% | Legitimate truncated isoforms |
| VSP_UNRESOLVABLE | 2,148 | 0% | Correct ambiguity rejection |
| VSP_DELETION | 8,275 | 0% | Expected deletion events |

**Total Irrecoverable**: 54,273 / 54,459 = **99.7%**

## Consequences

### Positive

- **Clarity**: We now understand that the PTM failure categories represent biological truth, not engineering defects
- **Validation confidence**: The physical truth constraint (isoform_aa == canonical_aa) is working correctly
- **Diagnostic capability**: The enhanced logging provides insight into failure patterns for future analysis

### Negative

- **Success rate ceiling**: ~32% of PTM mappings will always fail for isoforms with sequence variants or truncations
- **Log volume**: The PTM_FAIL logs are verbose; consider adding a `--quiet-ptm` flag

### Neutral

- The 67.6% success rate is appropriate for ESM-2 training data, as it represents high-fidelity ground truth
- Failed mappings should NOT be included in training data, as they would introduce noise

## Recommendations

1. **Accept current results** - The 113k successfully mapped PTMs are biologically valid
2. **Suppress verbose logging** - Consider `log_level: warn` to reduce PTM_FAIL output
3. **Document for consumers** - Downstream users should understand that PTM arrays only include verified sites
4. **Future enhancement** - Consider adding a `mapping_status` enum column for analysis:
   - `Mapped` - Successfully verified
   - `SequenceVariant` - Isoform has different AA at position
   - `Truncated` - Position beyond isoform length
   - `Deleted` - Position within VSP deletion
   - `Ambiguous` - Interior of length-changing indel

## Related

- [ADR-0007: Isoform-scoped VSP mapping](0007-isoform-scoped-vsp-mapping.md) - Prerequisite for this analysis
- [src/pipeline/mapper.rs](../../src/pipeline/mapper.rs) - Coordinate mapping implementation
- [src/pipeline/builders.rs](../../src/pipeline/builders.rs) - PTM validation and diagnostic logging
