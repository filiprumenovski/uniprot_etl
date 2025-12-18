use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Thread-local metrics for zero-contention counting in parallel workloads.
/// Use this in worker threads, then merge into global Metrics at the end.
#[derive(Default)]
pub struct LocalMetrics {
    entries_parsed: u64,
    batches_written: u64,
    bytes_read: u64,
    bytes_written: u64,
    features_count: u64,
    isoforms_count: u64,
    ptm_attempted: u64,
    ptm_mapped: u64,
    ptm_failed: u64,
    ptm_failed_canonical_oob: u64,
    ptm_failed_vsp_deletion: u64,
    ptm_failed_mapper_oob: u64,
    ptm_failed_vsp_unresolvable: u64,
    ptm_failed_isoform_oob: u64,
    ptm_failed_residue_mismatch: u64,
}

impl LocalMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_entries(&mut self) {
        self.entries_parsed += 1;
    }

    pub fn inc_batches(&mut self) {
        self.batches_written += 1;
    }

    pub fn add_bytes_read(&mut self, bytes: u64) {
        self.bytes_read += bytes;
    }

    pub fn add_bytes_written(&mut self, bytes: u64) {
        self.bytes_written += bytes;
    }

    pub fn add_features(&mut self, count: u64) {
        self.features_count += count;
    }

    pub fn add_isoforms(&mut self, count: u64) {
        self.isoforms_count += count;
    }

    pub fn add_ptm_attempted(&mut self, count: u64) {
        self.ptm_attempted += count;
    }

    pub fn add_ptm_mapped(&mut self, count: u64) {
        self.ptm_mapped += count;
    }

    pub fn add_ptm_failed(&mut self, count: u64) {
        self.ptm_failed += count;
    }

    pub fn add_ptm_failed_canonical_oob(&mut self, count: u64) {
        self.ptm_failed_canonical_oob += count;
    }

    pub fn add_ptm_failed_vsp_deletion(&mut self, count: u64) {
        self.ptm_failed_vsp_deletion += count;
    }

    pub fn add_ptm_failed_mapper_oob(&mut self, count: u64) {
        self.ptm_failed_mapper_oob += count;
    }

    pub fn add_ptm_failed_vsp_unresolvable(&mut self, count: u64) {
        self.ptm_failed_vsp_unresolvable += count;
    }

    pub fn add_ptm_failed_isoform_oob(&mut self, count: u64) {
        self.ptm_failed_isoform_oob += count;
    }

    pub fn add_ptm_failed_residue_mismatch(&mut self, count: u64) {
        self.ptm_failed_residue_mismatch += count;
    }

    /// Merge this local metrics into a global Metrics instance (one atomic op per field)
    pub fn merge_into(&self, global: &Metrics) {
        if self.entries_parsed > 0 {
            global.inner.entries_parsed.fetch_add(self.entries_parsed, Ordering::Relaxed);
        }
        if self.batches_written > 0 {
            global.inner.batches_written.fetch_add(self.batches_written, Ordering::Relaxed);
        }
        if self.bytes_read > 0 {
            global.inner.bytes_read.fetch_add(self.bytes_read, Ordering::Relaxed);
        }
        if self.bytes_written > 0 {
            global.inner.bytes_written.fetch_add(self.bytes_written, Ordering::Relaxed);
        }
        if self.features_count > 0 {
            global.inner.features_count.fetch_add(self.features_count, Ordering::Relaxed);
        }
        if self.isoforms_count > 0 {
            global.inner.isoforms_count.fetch_add(self.isoforms_count, Ordering::Relaxed);
        }
        if self.ptm_attempted > 0 {
            global.inner.ptm_attempted.fetch_add(self.ptm_attempted, Ordering::Relaxed);
        }
        if self.ptm_mapped > 0 {
            global.inner.ptm_mapped.fetch_add(self.ptm_mapped, Ordering::Relaxed);
        }
        if self.ptm_failed > 0 {
            global.inner.ptm_failed.fetch_add(self.ptm_failed, Ordering::Relaxed);
        }
        if self.ptm_failed_canonical_oob > 0 {
            global.inner.ptm_failures.add_canonical_oob(self.ptm_failed_canonical_oob);
        }
        if self.ptm_failed_vsp_deletion > 0 {
            global.inner.ptm_failures.add_vsp_deletion(self.ptm_failed_vsp_deletion);
        }
        if self.ptm_failed_mapper_oob > 0 {
            global.inner.ptm_failures.add_mapper_oob(self.ptm_failed_mapper_oob);
        }
        if self.ptm_failed_vsp_unresolvable > 0 {
            global.inner.ptm_failures.add_vsp_unresolvable(self.ptm_failed_vsp_unresolvable);
        }
        if self.ptm_failed_isoform_oob > 0 {
            global.inner.ptm_failures.add_isoform_oob(self.ptm_failed_isoform_oob);
        }
        if self.ptm_failed_residue_mismatch > 0 {
            global.inner.ptm_failures.add_residue_mismatch(self.ptm_failed_residue_mismatch);
        }
    }
}

/// Adapter that wraps LocalMetrics in a Metrics-compatible API using a Mutex.
/// This allows LocalMetrics to be used with existing pipeline code that expects Clone.
/// The Mutex ensures thread-safety, but in practice each file processing is single-threaded,
/// so there's no actual contention (the Mutex is just to satisfy the borrow checker).
#[derive(Clone)]
pub struct LocalMetricsAdapter {
    inner: Arc<Mutex<LocalMetrics>>,
}

impl LocalMetricsAdapter {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(LocalMetrics::new())),
        }
    }

    pub fn inc_entries(&self) {
        self.inner.lock().unwrap().inc_entries();
    }

    pub fn inc_batches(&self) {
        self.inner.lock().unwrap().inc_batches();
    }

    pub fn add_bytes_read(&self, bytes: u64) {
        self.inner.lock().unwrap().add_bytes_read(bytes);
    }

    pub fn add_bytes_written(&self, bytes: u64) {
        self.inner.lock().unwrap().add_bytes_written(bytes);
    }

    pub fn add_features(&self, count: u64) {
        self.inner.lock().unwrap().add_features(count);
    }

    pub fn add_isoforms(&self, count: u64) {
        self.inner.lock().unwrap().add_isoforms(count);
    }

    pub fn add_ptm_attempted(&self, count: u64) {
        self.inner.lock().unwrap().add_ptm_attempted(count);
    }

    pub fn add_ptm_mapped(&self, count: u64) {
        self.inner.lock().unwrap().add_ptm_mapped(count);
    }

    pub fn add_ptm_failed(&self, count: u64) {
        self.inner.lock().unwrap().add_ptm_failed(count);
    }

    pub fn add_ptm_failed_canonical_oob(&self, count: u64) {
        self.inner.lock().unwrap().add_ptm_failed_canonical_oob(count);
    }

    pub fn add_ptm_failed_vsp_deletion(&self, count: u64) {
        self.inner.lock().unwrap().add_ptm_failed_vsp_deletion(count);
    }

    pub fn add_ptm_failed_mapper_oob(&self, count: u64) {
        self.inner.lock().unwrap().add_ptm_failed_mapper_oob(count);
    }

    pub fn add_ptm_failed_vsp_unresolvable(&self, count: u64) {
        self.inner.lock().unwrap().add_ptm_failed_vsp_unresolvable(count);
    }

    pub fn add_ptm_failed_isoform_oob(&self, count: u64) {
        self.inner.lock().unwrap().add_ptm_failed_isoform_oob(count);
    }

    pub fn add_ptm_failed_residue_mismatch(&self, count: u64) {
        self.inner.lock().unwrap().add_ptm_failed_residue_mismatch(count);
    }

    /// Merge the accumulated local metrics into a global Metrics instance
    pub fn merge_into(&self, global: &Metrics) {
        self.inner.lock().unwrap().merge_into(global);
    }
}

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
    ptm_attempted: AtomicU64,
    ptm_mapped: AtomicU64,
    ptm_failed: AtomicU64,
    ptm_failures: PtmFailures,
}

struct PtmFailures {
    canonical_oob: AtomicU64,
    vsp_deletion: AtomicU64,
    mapper_oob: AtomicU64,
    vsp_unresolvable: AtomicU64,
    isoform_oob: AtomicU64,
    residue_mismatch: AtomicU64,
}

impl PtmFailures {
    fn new() -> Self {
        Self {
            canonical_oob: AtomicU64::new(0),
            vsp_deletion: AtomicU64::new(0),
            mapper_oob: AtomicU64::new(0),
            vsp_unresolvable: AtomicU64::new(0),
            isoform_oob: AtomicU64::new(0),
            residue_mismatch: AtomicU64::new(0),
        }
    }

    fn add_canonical_oob(&self, count: u64) {
        self.canonical_oob.fetch_add(count, Ordering::Relaxed);
    }

    fn add_vsp_deletion(&self, count: u64) {
        self.vsp_deletion.fetch_add(count, Ordering::Relaxed);
    }

    fn add_mapper_oob(&self, count: u64) {
        self.mapper_oob.fetch_add(count, Ordering::Relaxed);
    }

    fn add_vsp_unresolvable(&self, count: u64) {
        self.vsp_unresolvable.fetch_add(count, Ordering::Relaxed);
    }

    fn add_isoform_oob(&self, count: u64) {
        self.isoform_oob.fetch_add(count, Ordering::Relaxed);
    }

    fn add_residue_mismatch(&self, count: u64) {
        self.residue_mismatch.fetch_add(count, Ordering::Relaxed);
    }

    fn canonical_oob(&self) -> u64 {
        self.canonical_oob.load(Ordering::Relaxed)
    }

    fn vsp_deletion(&self) -> u64 {
        self.vsp_deletion.load(Ordering::Relaxed)
    }

    fn mapper_oob(&self) -> u64 {
        self.mapper_oob.load(Ordering::Relaxed)
    }

    fn vsp_unresolvable(&self) -> u64 {
        self.vsp_unresolvable.load(Ordering::Relaxed)
    }

    fn isoform_oob(&self) -> u64 {
        self.isoform_oob.load(Ordering::Relaxed)
    }

    fn residue_mismatch(&self) -> u64 {
        self.residue_mismatch.load(Ordering::Relaxed)
    }
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
                ptm_attempted: AtomicU64::new(0),
                ptm_mapped: AtomicU64::new(0),
                ptm_failed: AtomicU64::new(0),
                ptm_failures: PtmFailures::new(),
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

    pub fn add_ptm_attempted(&self, count: u64) {
        self.inner.ptm_attempted.fetch_add(count, Ordering::Relaxed);
    }

    pub fn add_ptm_mapped(&self, count: u64) {
        self.inner.ptm_mapped.fetch_add(count, Ordering::Relaxed);
    }

    pub fn add_ptm_failed(&self, count: u64) {
        self.inner.ptm_failed.fetch_add(count, Ordering::Relaxed);
    }

    pub fn add_ptm_failed_canonical_oob(&self, count: u64) {
        self.inner.ptm_failures.add_canonical_oob(count);
    }

    pub fn add_ptm_failed_vsp_deletion(&self, count: u64) {
        self.inner.ptm_failures.add_vsp_deletion(count);
    }

    pub fn add_ptm_failed_mapper_oob(&self, count: u64) {
        self.inner.ptm_failures.add_mapper_oob(count);
    }

    pub fn add_ptm_failed_vsp_unresolvable(&self, count: u64) {
        self.inner.ptm_failures.add_vsp_unresolvable(count);
    }

    pub fn add_ptm_failed_isoform_oob(&self, count: u64) {
        self.inner.ptm_failures.add_isoform_oob(count);
    }

    pub fn add_ptm_failed_residue_mismatch(&self, count: u64) {
        self.inner.ptm_failures.add_residue_mismatch(count);
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

    pub fn ptm_attempted(&self) -> u64 {
        self.inner.ptm_attempted.load(Ordering::Relaxed)
    }

    pub fn ptm_mapped(&self) -> u64 {
        self.inner.ptm_mapped.load(Ordering::Relaxed)
    }

    pub fn ptm_failed(&self) -> u64 {
        self.inner.ptm_failed.load(Ordering::Relaxed)
    }

    pub fn ptm_failed_canonical_oob(&self) -> u64 {
        self.inner.ptm_failures.canonical_oob()
    }

    pub fn ptm_failed_vsp_deletion(&self) -> u64 {
        self.inner.ptm_failures.vsp_deletion()
    }

    pub fn ptm_failed_mapper_oob(&self) -> u64 {
        self.inner.ptm_failures.mapper_oob()
    }

    pub fn ptm_failed_vsp_unresolvable(&self) -> u64 {
        self.inner.ptm_failures.vsp_unresolvable()
    }

    pub fn ptm_failed_isoform_oob(&self) -> u64 {
        self.inner.ptm_failures.isoform_oob()
    }

    pub fn ptm_failed_residue_mismatch(&self) -> u64 {
        self.inner.ptm_failures.residue_mismatch()
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
        let ptm_attempted = self.inner.ptm_attempted.load(Ordering::Relaxed);
        let ptm_mapped = self.inner.ptm_mapped.load(Ordering::Relaxed);
        let ptm_failed = self.inner.ptm_failed.load(Ordering::Relaxed);
        let ptm_failed_canonical_oob = self.ptm_failed_canonical_oob();
        let ptm_failed_vsp_deletion = self.ptm_failed_vsp_deletion();
        let ptm_failed_mapper_oob = self.ptm_failed_mapper_oob();
        let ptm_failed_vsp_unresolvable = self.ptm_failed_vsp_unresolvable();
        let ptm_failed_isoform_oob = self.ptm_failed_isoform_oob();
        let ptm_failed_residue_mismatch = self.ptm_failed_residue_mismatch();

        let entries_per_sec = entries as f64 / elapsed;
        let mb_read = bytes_read as f64 / (1024.0 * 1024.0);
        let mb_written = bytes_written as f64 / (1024.0 * 1024.0);

        eprintln!("\n=== ETL Summary ===");
        eprintln!("Entries parsed:  {entries}");
        eprintln!("Batches written: {batches}");
        eprintln!("Features:        {features}");
        eprintln!("Isoforms:        {isoforms}");
        eprintln!("PTMs attempted:  {ptm_attempted}");
        eprintln!("PTMs mapped:     {ptm_mapped}");
        eprintln!("PTMs failed:     {ptm_failed}");
        eprintln!("  - canonical_oob:    {ptm_failed_canonical_oob}");
        eprintln!("  - vsp_deletion:     {ptm_failed_vsp_deletion}");
        eprintln!("  - mapper_oob:       {ptm_failed_mapper_oob}");
        eprintln!("  - vsp_unresolvable: {ptm_failed_vsp_unresolvable}");
        eprintln!("  - isoform_oob:      {ptm_failed_isoform_oob}");
        eprintln!("  - residue_mismatch: {ptm_failed_residue_mismatch}");
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
