use arrow::record_batch::RecordBatch;
use crossbeam_channel::Sender;

use crate::error::{EtlError, Result};
use crate::metrics::Metrics;
use crate::pipeline::builders::EntryBuilders;
use crate::pipeline::mapper::CoordinateMapper;
use crate::pipeline::scratch::EntryScratch;
use std::collections::HashMap;
use std::sync::Arc;

#[allow(dead_code)]
pub const DEFAULT_BATCH_SIZE: usize = 10_000;

/// Manages batching of entries into RecordBatches and sending to the writer.
pub struct Batcher {
    builders: EntryBuilders,
    batch_size: usize,
    sender: Sender<RecordBatch>,
    metrics: Metrics,
    sidecar_fasta: Option<Arc<HashMap<String, String>>>,
}

impl Batcher {
    #[allow(dead_code)]
    pub fn new(sender: Sender<RecordBatch>, metrics: Metrics) -> Self {
        Self::with_batch_size(sender, metrics, DEFAULT_BATCH_SIZE, None)
    }

    pub fn with_batch_size(
        sender: Sender<RecordBatch>,
        metrics: Metrics,
        batch_size: usize,
        sidecar_fasta: Option<Arc<HashMap<String, String>>>,
    ) -> Self {
        Self {
            builders: EntryBuilders::new(batch_size, metrics.clone()),
            batch_size,
            sender,
            metrics,
            sidecar_fasta,
        }
    }

    /// Adds an entry to the current batch. Flushes if batch is full.
    pub fn add_entry(&mut self, scratch: &EntryScratch) -> Result<()> {
        if scratch.isoforms.is_empty() {
            let mapper = CoordinateMapper::from_entry(scratch);
            // No isoforms: id == parent_id and sequence is canonical.
            self.builders.append_row(
                scratch,
                &scratch.accession,
                &scratch.accession,
                &scratch.sequence,
                &mapper,
            );
            self.metrics.inc_entries();

            if self.builders.len() >= self.batch_size {
                self.flush()?;
            }
        } else {
            let sidecar = self.sidecar_fasta.clone().ok_or(EtlError::MissingField(
                "fasta_sidecar_path is required when isoforms exist".to_string(),
            ))?;

            for iso in &scratch.isoforms {
                // Sidecar FASTA is keyed by UniProt isoform accession (e.g., Q16670-2).
                // In real UniProt XML this is often provided as <sequence ref="Q16670-2"/>.
                // In some fixtures, <isoform><id> may be a local label (e.g., ISO1), so we
                // fall back to sequence ref when it looks like an accession and is not VSP_...
                let isoform_id = match iso.isoform_sequence.as_deref() {
                    Some(r) if !r.starts_with("VSP_") && r.contains('-') => r
                        .split_whitespace()
                        .next()
                        .unwrap_or(r),
                    _ => iso
                        .isoform_id
                        .split_whitespace()
                        .next()
                        .unwrap_or(iso.isoform_id.as_str()),
                };

                let Some(isoform_sequence) = sidecar.get(isoform_id) else {
                    eprintln!(
                        "[WARN] code=ISOFORM_SEQ_MISSING parent_id={} id={} isoform_id={}",
                        scratch.parent_id, scratch.accession, isoform_id
                    );
                    continue;
                };

                // Build an isoform-scoped mapper from referenced VSP ids.
                let mapper = CoordinateMapper::from_entry_for_vsp_ids(scratch, &iso.vsp_ids);

                self.builders.append_row(
                    scratch,
                    isoform_id,
                    &scratch.parent_id,
                    isoform_sequence,
                    &mapper,
                );
                self.metrics.inc_entries();

                if self.builders.len() >= self.batch_size {
                    self.flush()?;
                }
            }

            // Note: isoforms_count is tracked from parsed XML (not rows emitted).
            self.metrics.add_isoforms(scratch.isoforms.len() as u64);
        }

        self.metrics.add_features(scratch.features.len() as u64);

        Ok(())
    }

    /// Flushes the current batch to the channel
    pub fn flush(&mut self) -> Result<()> {
        if self.builders.is_empty() {
            return Ok(());
        }

        let batch = self.builders.finish_batch()?;
        self.sender.send(batch).map_err(|_| EtlError::ChannelSend)?;
        self.metrics.inc_batches();

        Ok(())
    }

    /// Finishes batching, flushing any remaining entries
    pub fn finish(mut self) -> Result<()> {
        self.flush()
    }
}
