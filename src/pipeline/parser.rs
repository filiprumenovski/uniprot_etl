use arrow::record_batch::RecordBatch;
use crossbeam_channel::Sender;
use quick_xml::events::Event;
use quick_xml::Reader;
use std::collections::HashMap;
use std::io::BufRead;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::error::Result;
use crate::metrics::MetricsCollector;
use crate::pipeline::batcher::Batcher;
use crate::pipeline::handlers::metadata;
use crate::pipeline::scratch::EntryScratch;
use crate::pipeline::transformer::EntryTransformer;
use crate::sampler::ChannelStats;

/// Parses UniProt XML entries and sends RecordBatches to the channel.
pub fn parse_entries<R: BufRead, M: MetricsCollector>(
    mut reader: Reader<R>,
    sender: Sender<RecordBatch>,
    metrics: &M,
    batch_size: usize,
    sidecar_fasta: Option<Arc<HashMap<String, String>>>,
    channel_stats: Option<Arc<ChannelStats>>,
    cancel_flag: Option<&AtomicBool>,
) -> Result<()> {
    let mut batcher = Batcher::with_batch_size(sender, metrics.clone(), batch_size, channel_stats);
    let transformer = EntryTransformer::new(metrics.clone(), sidecar_fasta);
    let mut scratch = EntryScratch::new();
    let mut buf = Vec::with_capacity(4096);

    loop {
        if let Some(flag) = cancel_flag {
            if flag.load(Ordering::Relaxed) {
                return Err(crate::error::EtlError::Cancelled);
            }
        }
        buf.clear();
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) if e.local_name().as_ref() == b"entry" => {
                scratch.reset();
                metadata::consume_entry(&mut reader, &mut scratch, &mut buf)?;
                let entry = scratch.take_entry();
                for row in transformer.transform(entry)? {
                    batcher.add_row(row)?;
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }

    batcher.finish()?;
    Ok(())
}
