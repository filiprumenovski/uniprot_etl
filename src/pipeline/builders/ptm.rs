use arrow::array::{Float32Builder, Int32Builder, ListBuilder, StringBuilder, StructBuilder};
use std::collections::BTreeMap;

use crate::metrics::MetricsCollector;
use crate::pipeline::mapper::{CoordinateMapper, MapFailure};
use crate::pipeline::scratch::ParsedEntry;
use crate::pipeline::transformer::TransformedRow;

pub fn append_ptm_sites<M: MetricsCollector>(
    builder: &mut ListBuilder<StructBuilder>,
    metrics: &M,
    entry: &ParsedEntry,
    row: &TransformedRow,
) {
    let isoform_bytes = row.sequence.as_bytes();
    let mut sites: BTreeMap<i32, (u8, Vec<(i32, f32)>)> = BTreeMap::new();

    for feat in &entry.features.generic {
        let ft = feat.feature_type.to_ascii_lowercase();
        let is_point_ptm =
            ft == "glycosylation site" || ft == "modified residue" || ft == "cross-link";
        if !is_point_ptm {
            continue;
        }

        let (Some(start), Some(end)) = (feat.start, feat.end) else {
            continue;
        };
        if start <= 0 || end <= 0 || start != end {
            continue;
        }

        metrics.add_ptm_attempted(1);

        let Some(original_aa) = entry.canonical_aa_at_1based(start) else {
            metrics.add_ptm_failed(1);
            metrics.add_ptm_failed_canonical_oob(1);
            eprintln!(
                "[PTM_FAIL] code=CANONICAL_OOB parent_id={} id={} original_index={} mapped_index=?",
                row.parent_id, row.row_id, start
            );
            continue;
        };

        let mapped_1based = if row.row_id == row.parent_id {
            start
        } else {
            match map_point(metrics, &row.mapper, start, &row.parent_id, &row.row_id) {
                Ok(m) => m,
                Err(_) => continue,
            }
        };

        let mapped_idx0 = (mapped_1based as usize).saturating_sub(1);
        if mapped_idx0 >= isoform_bytes.len() {
            metrics.add_ptm_failed(1);
            metrics.add_ptm_failed_isoform_oob(1);
            let shift = mapped_1based - start;
            let expected_len = entry.sequence.len() as i32 + row.mapper.total_delta();
            eprintln!(
                "[PTM_FAIL] code=ISOFORM_OOB parent_id={} id={} original_index={} mapped_index={} isoform_len={} shift={} vsp_count={} expected_len={}",
                row.parent_id,
                row.row_id,
                start,
                mapped_1based,
                isoform_bytes.len(),
                shift,
                row.mapper.edit_count(),
                expected_len
            );
            continue;
        }

        let isoform_aa = isoform_bytes[mapped_idx0];

        if isoform_aa != original_aa {
            metrics.add_ptm_failed(1);
            metrics.add_ptm_failed_residue_mismatch(1);
            let shift = mapped_1based - start;
            eprintln!(
                "[PTM_FAIL] code=RESIDUE_MISMATCH parent_id={} id={} original_index={} mapped_index={} original_aa={} isoform_aa={} shift={} vsp_count={}",
                row.parent_id,
                row.row_id,
                start,
                mapped_1based,
                original_aa as char,
                isoform_aa as char,
                shift,
                row.mapper.edit_count()
            );
            continue;
        }

        let mod_type = classify_mod_type(&ft, feat.description.as_deref());
        let confidence = entry.max_confidence_for_evidence(&feat.evidence_keys);

        let entry_site = sites
            .entry(mapped_1based)
            .or_insert_with(|| (original_aa, Vec::new()));
        entry_site.1.push((mod_type, confidence));

        metrics.add_ptm_mapped(1);
    }

    let sites_struct = builder.values();
    for (site_index, (site_aa, modifications)) in sites {
        sites_struct
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_value(site_index);
        sites_struct
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value((site_aa as char).to_string());

        let mods_list = sites_struct
            .field_builder::<ListBuilder<StructBuilder>>(2)
            .unwrap();
        let mods_struct = mods_list.values();
        for (mod_type, confidence_score) in modifications {
            mods_struct
                .field_builder::<Int32Builder>(0)
                .unwrap()
                .append_value(mod_type);
            mods_struct
                .field_builder::<Float32Builder>(1)
                .unwrap()
                .append_value(confidence_score);
            mods_struct.append(true);
        }
        mods_list.append(true);

        sites_struct.append(true);
    }
    builder.append(true);
}

fn map_point<M: MetricsCollector>(
    metrics: &M,
    mapper: &CoordinateMapper,
    start: i32,
    parent_id: &str,
    row_id: &str,
) -> Result<i32, ()> {
    match mapper.map_point_1based(start) {
        Ok(m) => Ok(m),
        Err(MapFailure::VspDeletionEvent) => {
            metrics.add_ptm_failed(1);
            metrics.add_ptm_failed_vsp_deletion(1);
            eprintln!(
                "[PTM_FAIL] code=VSP_DELETION_EVENT parent_id={} id={} original_index={} mapped_index=?",
                parent_id, row_id, start
            );
            Err(())
        }
        Err(MapFailure::PtmOutOfBounds) => {
            metrics.add_ptm_failed(1);
            metrics.add_ptm_failed_mapper_oob(1);
            eprintln!(
                "[PTM_FAIL] code=MAPPER_OOB parent_id={} id={} original_index={} mapped_index=?",
                parent_id, row_id, start
            );
            Err(())
        }
        Err(MapFailure::VspUnresolvable) => {
            metrics.add_ptm_failed(1);
            metrics.add_ptm_failed_vsp_unresolvable(1);
            eprintln!(
                "[PTM_FAIL] code=VSP_UNRESOLVABLE parent_id={} id={} original_index={} mapped_index=?",
                parent_id, row_id, start
            );
            Err(())
        }
    }
}

fn classify_mod_type(feature_type_lower: &str, description: Option<&str>) -> i32 {
    let desc = description.unwrap_or("").to_ascii_lowercase();

    if feature_type_lower == "modified residue" && desc.contains("phospho") {
        1
    } else if feature_type_lower == "glycosylation site" && desc.contains("n-acetylglucosamine") {
        2
    } else {
        0
    }
}
