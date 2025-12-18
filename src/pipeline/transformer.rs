use crate::error::{EtlError, Result};
use crate::metrics::Metrics;
use crate::pipeline::mapper::CoordinateMapper;
use crate::pipeline::scratch::{IsoformScratch, ParsedEntry};
use std::collections::HashMap;
use std::sync::Arc;

/// Row material emitted by the transformer and fed into the batcher.
#[derive(Debug, Clone)]
pub struct TransformedRow {
    pub entry: Arc<ParsedEntry>,
    pub row_id: String,
    pub parent_id: String,
    pub sequence: String,
    pub mapper: CoordinateMapper,
}

pub struct EntryTransformer {
    metrics: Metrics,
    sidecar_fasta: Option<Arc<HashMap<String, String>>>,
}

impl EntryTransformer {
    pub fn new(metrics: Metrics, sidecar_fasta: Option<Arc<HashMap<String, String>>>) -> Self {
        Self {
            metrics,
            sidecar_fasta,
        }
    }

    /// Expands a parsed entry into one or more row-level records.
    pub fn transform(&self, entry: ParsedEntry) -> Result<Vec<TransformedRow>> {
        // Track per-entry metrics before expansion.
        self.metrics
            .add_features(entry.features.generic.len() as u64);
        self.metrics.add_isoforms(entry.isoforms.len() as u64);

        let shared_entry = Arc::new(entry);

        if shared_entry.isoforms.is_empty() {
            let mapper = CoordinateMapper::from_entry(&shared_entry);
            let row = TransformedRow {
                row_id: shared_entry.accession.clone(),
                parent_id: shared_entry.accession.clone(),
                sequence: shared_entry.sequence.clone(),
                mapper,
                entry: Arc::clone(&shared_entry),
            };
            return Ok(vec![row]);
        }

        let sidecar = self
            .sidecar_fasta
            .clone()
            .ok_or_else(|| EtlError::MissingField("fasta_sidecar_path is required when isoforms exist".to_string()))?;

        let mut rows = Vec::with_capacity(shared_entry.isoforms.len());
        for iso in &shared_entry.isoforms {
            let isoform_id = canonical_isoform_id(iso);
            let Some(isoform_sequence) = sidecar.get(&isoform_id) else {
                eprintln!(
                    "[WARN] code=ISOFORM_SEQ_MISSING parent_id={} id={} isoform_id={}",
                    shared_entry.parent_id, shared_entry.accession, isoform_id
                );
                continue;
            };

            let mapper = CoordinateMapper::from_entry_for_vsp_ids(&shared_entry, &iso.vsp_ids);
            rows.push(TransformedRow {
                row_id: isoform_id,
                parent_id: shared_entry.parent_id.clone(),
                sequence: isoform_sequence.clone(),
                mapper,
                entry: Arc::clone(&shared_entry),
            });
        }

        Ok(rows)
    }
}

fn canonical_isoform_id(iso: &IsoformScratch) -> String {
    match iso.isoform_sequence.as_deref() {
        Some(r) if !r.starts_with("VSP_") && r.contains('-') => {
            r.split_whitespace().next().unwrap_or(r).to_string()
        }
        _ => iso
            .isoform_id
            .split_whitespace()
            .next()
            .unwrap_or(iso.isoform_id.as_str())
            .to_string(),
    }
}
