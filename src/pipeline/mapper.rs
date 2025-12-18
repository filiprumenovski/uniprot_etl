use crate::pipeline::scratch::EntryScratch;
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MapFailure {
    /// The site falls inside a deleted segment.
    VspDeletionEvent,
    /// The mapped coordinate is outside isoform bounds.
    PtmOutOfBounds,
    /// The coordinate cannot be mapped deterministically.
    VspUnresolvable,
}

#[derive(Debug, Clone)]
struct VspEdit {
    begin_1based: i32,
    end_1based: i32,
    delta: i32,
    is_deletion: bool,
}

/// CoordinateMapper applies VSP-derived indel deltas to map canonical coordinates to isoform coordinates.
///
/// Contract:
/// - Input positions are 1-based XML coordinates.
/// - Output position is 1-based isoform coordinate.
/// - If a position is deleted by a VSP "Missing" event, returns `VspDeletionEvent`.
/// - If a position falls within a non-deletion indel region, returns `VspUnresolvable`.
#[derive(Debug, Clone)]
pub struct CoordinateMapper {
    edits: Vec<VspEdit>,
}

impl CoordinateMapper {
    pub fn from_entry(scratch: &EntryScratch) -> Self {
        Self::from_entry_for_vsp_ids(scratch, &[])
    }

    /// Returns the number of VSP edits in this mapper (for diagnostics).
    pub fn edit_count(&self) -> usize {
        self.edits.len()
    }

    /// Returns the total delta (sum of all edit deltas) for this mapper.
    /// Positive = net insertion, Negative = net deletion.
    pub fn total_delta(&self) -> i32 {
        self.edits.iter().map(|e| e.delta).sum()
    }

    /// Builds a mapper using only splice-variant edits referenced by the isoform.
    ///
    /// If `vsp_ids` is empty, returns an identity mapper.
    pub fn from_entry_for_vsp_ids(scratch: &EntryScratch, vsp_ids: &[String]) -> Self {
        if vsp_ids.is_empty() {
            return Self { edits: Vec::new() };
        }

        let vsp_set: HashSet<&str> = vsp_ids.iter().map(|s| s.as_str()).collect();
        let mut edits: Vec<VspEdit> = Vec::new();

        for feat in &scratch.features {
            // UniProt uses "splice variant" features (id="VSP_...") to describe
            // alternative isoform sequences. Older/other exports may use "variant sequence".
            if feat.feature_type != "splice variant" && feat.feature_type != "variant sequence" {
                continue;
            }

            // Scope edits to the isoform by VSP id.
            // If the feature has no id, we can't safely apply it.
            let Some(fid) = feat.id.as_deref() else {
                continue;
            };
            if !vsp_set.contains(fid) {
                continue;
            }

            let (Some(start), Some(end)) = (feat.start, feat.end) else {
                continue;
            };

            // UniProt coordinates are 1-based, inclusive.
            if start <= 0 || end <= 0 || end < start {
                continue;
            }

            let description = feat.description.as_deref().unwrap_or("");
            let variation = feat.variation.as_deref().unwrap_or("");

            // Ground-truth length for the replaced segment is defined by the coordinates.
            let original_len = (end - start) + 1;
            if original_len <= 0 {
                continue;
            }

            let variation_len = cleaned_aa_len(variation) as i32;

            // UniProt splice variants frequently omit <original>/<variation> entirely and
            // specify only a <location>. In practice, this encodes a deletion of the span
            // from the isoform relative to canonical.
            let is_missing = if feat.feature_type == "splice variant" && variation_len <= 0 {
                true
            } else {
                is_missing_variant(variation, description)
            };

            let new_len: i32 = if is_missing { 0 } else { variation_len };

            // If we can't infer the variation length and it's not missing, don't guess shifts.
            if !is_missing && new_len <= 0 {
                continue;
            }

            let delta = new_len - original_len;
            edits.push(VspEdit {
                begin_1based: start,
                end_1based: end,
                delta,
                is_deletion: is_missing && new_len == 0,
            });
        }

        edits.sort_by_key(|e| e.begin_1based);

        Self { edits }
    }

    /// Maps a point coordinate (1-based) from canonical to isoform.
    pub fn map_point_1based(&self, original_pos_1based: i32) -> Result<i32, MapFailure> {
        if original_pos_1based <= 0 {
            return Err(MapFailure::VspUnresolvable);
        }

        // Interval-style mapping with accumulated downstream deltas.
        // Rules:
        // - If pos < begin: unaffected by this event.
        // - If begin <= pos <= end:
        //   - Missing => Deleted
        //   - delta == 0 => map to the same coordinate (within-span substitution)
        //   - delta != 0 => map to start of the variation (begin), after applying prior shifts
        // - If pos > end: apply delta to downstream positions.
        let mut shift: i32 = 0;
        for edit in &self.edits {
            if original_pos_1based < edit.begin_1based {
                break;
            }

            if original_pos_1based > edit.end_1based {
                shift += edit.delta;
                continue;
            }

            // Inside edited span.
            if edit.is_deletion {
                return Err(MapFailure::VspDeletionEvent);
            }

            // Requirement 1: Identity mapping for substitutions (delta == 0)
            // Within-span substitution: position maps to itself with accumulated shift
            if edit.delta == 0 {
                let mapped = original_pos_1based + shift;
                return if mapped <= 0 {
                    Err(MapFailure::PtmOutOfBounds)
                } else {
                    Ok(mapped)
                };
            }

            // Requirement 2: For length-changing indels (delta != 0),
            // only the FIRST residue of the segment can be mapped deterministically.
            if original_pos_1based == edit.begin_1based {
                let mapped = edit.begin_1based + shift;
                return if mapped <= 0 {
                    Err(MapFailure::PtmOutOfBounds)
                } else {
                    Ok(mapped)
                };
            }

            // Internal residues (not at exact start) have no deterministic isoform coordinate.
            // Previously these were "snapped" to begin, causing RESIDUE_MISMATCH.
            // Now they are cleanly rejected as VspUnresolvable.
            return Err(MapFailure::VspUnresolvable);
        }

        let mapped = original_pos_1based + shift;
        if mapped <= 0 {
            return Err(MapFailure::PtmOutOfBounds);
        }
        Ok(mapped)
    }
}

/// Returns the amino acid count for a valid sequence, or 0 for descriptive notes.
///
/// A string is considered a descriptive note (returning 0) if it contains:
/// - Whitespace (spaces indicate free text like "See Ref 2")
/// - Digits (e.g., "In isoform 3")
/// - Non-amino acid characters
///
/// This prevents "phantom shifts" where metadata strings are misinterpreted as
/// amino acid sequences, causing coordinate drift and ISOFORM_OOB errors.
fn cleaned_aa_len(text: &str) -> usize {
    let trimmed = text.trim();

    // Empty string = deletion (length 0)
    if trimmed.is_empty() {
        return 0;
    }

    // Contamination check: spaces or digits indicate descriptive text
    if trimmed.contains(' ') || trimmed.bytes().any(|b| b.is_ascii_digit()) {
        return 0; // Treat as note, not sequence
    }

    // Valid AA check: all characters must be amino acid letters
    // Standard 20 + selenocysteine (U) + pyrrolysine (O) + ambiguous (X, B, Z, J)
    const VALID_AA: &[u8] = b"ACDEFGHIKLMNPQRSTUVWXYZBJOacdefghiklmnpqrstuvwxyzbjo";

    if trimmed.bytes().all(|b| VALID_AA.contains(&b)) {
        trimmed.len()
    } else {
        0 // Contains invalid characters - treat as note
    }
}

fn is_missing_variant(variation: &str, description: &str) -> bool {
    let v = variation.to_ascii_lowercase();
    let d = description.to_ascii_lowercase();
    v.contains("missing") || d.contains("missing")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::scratch::{EntryScratch, FeatureScratch};

    #[test]
    fn deletion_shifts_downstream_positions() {
        let mut scratch = EntryScratch::new();
        scratch.sequence = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".to_string();

        let vsp = FeatureScratch {
            id: Some("VSP_TEST".to_string()),
            feature_type: "variant sequence".to_string(),
            start: Some(5),
            end: Some(7),
            original: Some("EFG".to_string()),
            variation: Some("Missing".to_string()),
            ..Default::default()
        };
        scratch.features.push(vsp);

        let mapper = CoordinateMapper::from_entry_for_vsp_ids(&scratch, &["VSP_TEST".to_string()]);

        // Position 10 should shift -3.
        assert_eq!(mapper.map_point_1based(10).unwrap(), 7);
        // Position inside deletion should error.
        assert_eq!(
            mapper.map_point_1based(6),
            Err(MapFailure::VspDeletionEvent)
        );
    }

    #[test]
    fn non_missing_indel_rejects_interior() {
        let mut scratch = EntryScratch::new();
        scratch.sequence = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".to_string();

        // Replace positions 5..7 (len=3) with len=1 -> delta=-2.
        let vsp = FeatureScratch {
            id: Some("VSP_TEST".to_string()),
            feature_type: "variant sequence".to_string(),
            start: Some(5),
            end: Some(7),
            variation: Some("E".to_string()),
            ..Default::default()
        };
        scratch.features.push(vsp);
        let mapper = CoordinateMapper::from_entry_for_vsp_ids(&scratch, &["VSP_TEST".to_string()]);

        // Exact start maps through.
        assert_eq!(mapper.map_point_1based(5).unwrap(), 5);

        // Interior positions are unresolvable (not snapped to start).
        assert_eq!(
            mapper.map_point_1based(6),
            Err(MapFailure::VspUnresolvable)
        );
        assert_eq!(
            mapper.map_point_1based(7),
            Err(MapFailure::VspUnresolvable)
        );

        // Downstream still shifts by delta (-2).
        assert_eq!(mapper.map_point_1based(10).unwrap(), 8);
    }

    #[test]
    fn malformed_variation_treated_as_note() {
        // Strings with spaces/digits should return 0 length (bullshit detection)
        assert_eq!(cleaned_aa_len("See Ref 2"), 0);
        assert_eq!(cleaned_aa_len("In isoform 3"), 0);
        assert_eq!(cleaned_aa_len("123"), 0);
        assert_eq!(cleaned_aa_len("ABC DEF"), 0);

        // Valid amino acid sequences return actual length
        assert_eq!(cleaned_aa_len("ACGT"), 4);
        assert_eq!(cleaned_aa_len(""), 0);
        assert_eq!(cleaned_aa_len("X"), 1);
        assert_eq!(cleaned_aa_len("MVLSPADKTNVKAAWGKVGAHAGEYGAEALERMFLSFPTTKTYFPHFDLSH"), 51);

        // Mixed case is valid
        assert_eq!(cleaned_aa_len("AcGt"), 4);
    }

    #[test]
    fn substitution_maps_identity() {
        let mut scratch = EntryScratch::new();
        scratch.sequence = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".to_string();

        // Replace positions 5..7 (len=3) with len=3 -> delta=0 (substitution).
        let vsp = FeatureScratch {
            id: Some("VSP_TEST".to_string()),
            feature_type: "variant sequence".to_string(),
            start: Some(5),
            end: Some(7),
            variation: Some("XYZ".to_string()),
            ..Default::default()
        };
        scratch.features.push(vsp);
        let mapper = CoordinateMapper::from_entry_for_vsp_ids(&scratch, &["VSP_TEST".to_string()]);

        // All positions within substitution map 1-to-1.
        assert_eq!(mapper.map_point_1based(5).unwrap(), 5);
        assert_eq!(mapper.map_point_1based(6).unwrap(), 6);
        assert_eq!(mapper.map_point_1based(7).unwrap(), 7);

        // Downstream unchanged (delta=0).
        assert_eq!(mapper.map_point_1based(10).unwrap(), 10);
    }
}
