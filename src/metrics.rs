use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    start_time: Instant,
    entries_parsed: AtomicU64,
    batches_written: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    features_count: AtomicU64,
    isoforms_count: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                start_time: Instant::now(),
                entries_parsed: AtomicU64::new(0),
                batches_written: AtomicU64::new(0),
                bytes_read: AtomicU64::new(0),
                bytes_written: AtomicU64::new(0),
                features_count: AtomicU64::new(0),
                isoforms_count: AtomicU64::new(0),
            }),
        }
    }

    pub fn inc_entries(&self) {
        self.inner.entries_parsed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_batches(&self) {
        self.inner.batches_written.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_bytes_read(&self, bytes: u64) {
        self.inner.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn add_bytes_written(&self, bytes: u64) {
        self.inner.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn add_features(&self, count: u64) {
        self.inner
            .features_count
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn add_isoforms(&self, count: u64) {
        self.inner
            .isoforms_count
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn entries(&self) -> u64 {
        self.inner.entries_parsed.load(Ordering::Relaxed)
    }

    pub fn batches(&self) -> u64 {
        self.inner.batches_written.load(Ordering::Relaxed)
    }

    pub fn bytes_read(&self) -> u64 {
        self.inner.bytes_read.load(Ordering::Relaxed)
    }

    pub fn bytes_written(&self) -> u64 {
        self.inner.bytes_written.load(Ordering::Relaxed)
    }

    pub fn features(&self) -> u64 {
        self.inner.features_count.load(Ordering::Relaxed)
    }

    pub fn isoforms(&self) -> u64 {
        self.inner.isoforms_count.load(Ordering::Relaxed)
    }

    pub fn elapsed_secs(&self) -> f64 {
        self.inner.start_time.elapsed().as_secs_f64()
    }

    #[allow(dead_code)]
    pub fn print_summary(&self) {
        let elapsed = self.elapsed_secs();
        let entries = self.entries();
        let batches = self.batches();
        let bytes_read = self.inner.bytes_read.load(Ordering::Relaxed);
        let bytes_written = self.inner.bytes_written.load(Ordering::Relaxed);
        let features = self.inner.features_count.load(Ordering::Relaxed);
        let isoforms = self.inner.isoforms_count.load(Ordering::Relaxed);

        let entries_per_sec = entries as f64 / elapsed;
        let mb_read = bytes_read as f64 / (1024.0 * 1024.0);
        let mb_written = bytes_written as f64 / (1024.0 * 1024.0);

        eprintln!("\n=== ETL Summary ===");
        eprintln!("Entries parsed:  {entries}");
        eprintln!("Batches written: {batches}");
        eprintln!("Features:        {features}");
        eprintln!("Isoforms:        {isoforms}");
        eprintln!("Time elapsed:    {elapsed:.2}s");
        eprintln!("Throughput:      {entries_per_sec:.0} entries/sec");
        eprintln!("Bytes read:      {mb_read:.2} MB");
        eprintln!("Bytes written:   {mb_written:.2} MB");
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
