use arrow::record_batch::RecordBatch;
use crossbeam_channel::Sender;
use std::sync::Arc;

use crate::error::{EtlError, Result};
use crate::metrics::MetricsCollector;
use crate::pipeline::builders::EntryBuilders;
use crate::pipeline::transformer::TransformedRow;
use crate::sampler::ChannelStats;

#[allow(dead_code)]
pub const DEFAULT_BATCH_SIZE: usize = 10_000;

/// Manages batching of entries into RecordBatches and sending to the writer.
pub struct Batcher<M: MetricsCollector> {
    builders: EntryBuilders,
    batch_size: usize,
    sender: Sender<RecordBatch>,
    metrics: M,
    channel_stats: Option<Arc<ChannelStats>>,
}

impl<M: MetricsCollector> Batcher<M> {
    pub fn with_batch_size(
        sender: Sender<RecordBatch>,
        metrics: M,
        batch_size: usize,
        channel_stats: Option<Arc<ChannelStats>>,
    ) -> Self {
        Self {
            builders: EntryBuilders::new(batch_size),
            batch_size,
            sender,
            metrics,
            channel_stats,
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
        if let Some(stats) = &self.channel_stats {
            stats.record_fullness(self.sender.len());
        }

        Ok(())
    }

    /// Finishes batching, flushing any remaining entries
    pub fn finish(mut self) -> Result<()> {
        self.flush()
    }
}
