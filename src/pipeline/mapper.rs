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

            if edit.delta == 0 {
                let mapped = original_pos_1based + shift;
                return if mapped <= 0 {
                    Err(MapFailure::PtmOutOfBounds)
                } else {
                    Ok(mapped)
                };
            }

            let mapped = edit.begin_1based + shift;
            return if mapped <= 0 {
                Err(MapFailure::PtmOutOfBounds)
            } else {
                Ok(mapped)
            };
        }

        let mapped = original_pos_1based + shift;
        if mapped <= 0 {
            return Err(MapFailure::PtmOutOfBounds);
        }
        Ok(mapped)
    }
}

fn cleaned_aa_len(text: &str) -> usize {
    text.bytes().filter(|b| b.is_ascii_alphabetic()).count()
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
    fn non_missing_indel_maps_to_begin() {
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

        // Within-span point maps to begin.
        assert_eq!(mapper.map_point_1based(6).unwrap(), 5);
        // Downstream shifts.
        assert_eq!(mapper.map_point_1based(10).unwrap(), 8);
    }
}
