use arrow::record_batch::RecordBatch;
use crossbeam_channel::Sender;

use crate::error::{EtlError, Result};
use crate::metrics::MetricsCollector;
use crate::pipeline::builders::EntryBuilders;
use crate::pipeline::transformer::TransformedRow;

#[allow(dead_code)]
pub const DEFAULT_BATCH_SIZE: usize = 10_000;

/// Manages batching of entries into RecordBatches and sending to the writer.
pub struct Batcher<M: MetricsCollector> {
    builders: EntryBuilders,
    batch_size: usize,
    sender: Sender<RecordBatch>,
    metrics: M,
}

impl<M: MetricsCollector> Batcher<M> {
    pub fn with_batch_size(
        sender: Sender<RecordBatch>,
        metrics: M,
        batch_size: usize,
    ) -> Self {
        Self {
            builders: EntryBuilders::new(batch_size),
            batch_size,
            sender,
            metrics,
        }
    }

    /// Adds a pre-transformed row to the current batch. Flushes if batch is full.
    pub fn add_row(&mut self, row: TransformedRow) -> Result<()> {
        self.builders.append_row(&row, &self.metrics);
        self.metrics.inc_entries();

        if self.builders.len() >= self.batch_size {
            self.flush()?;
        }

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
