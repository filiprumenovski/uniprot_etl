# ADR-0005: Execution Ledger and Diagnostic Benchmarking

## Status

Accepted

## Context

As the UniProt ETL pipeline processes large datasets (400MB–1GB+ XML files), operators need visibility into:

1. **Run history**: What runs were executed, when, and with what configuration
2. **Performance metrics**: Throughput rates, resource utilization, and timing
3. **Bottleneck identification**: Whether the parser or writer is the limiting factor
4. **Reproducibility**: Exact configuration used for each run

Without structured run artifacts, debugging performance issues or comparing runs requires manual data collection. The existing metrics summary printed to stderr is lost once the terminal closes.

### Requirements

- Create timestamped run directories with all artifacts
- Sample system resources (CPU, RAM) without impacting the 24k entries/sec hot path
- Track channel backpressure to diagnose producer-consumer imbalances
- Generate machine-readable YAML reports for automated analysis
- Automatic cleanup to prevent disk bloat

## Decision

Implement an **Execution Ledger** system with the following components:

### 1. Run Directory Structure

Each run creates a timestamped directory: `runs/run_{YYYYMMDD_HHMMSS}/`

Artifacts saved:
- `report.yaml` - Comprehensive run report with metrics and diagnostics
- `etl.log` - Complete log output from the run
- `config_snapshot.yaml` - Exact configuration used (for reproducibility)

### 2. Resource Sampling Architecture

Use `sysinfo` crate for lightweight system metrics collection:

- **Sampling frequency**: 1Hz (1 sample per second)
- **Metrics collected**: CPU usage, RSS memory, channel fullness
- **Implementation**: Background thread that doesn't block the hot path
- **Overhead**: <1ms per sample, negligible impact on throughput

### 3. Channel Backpressure Tracking

Monitor crossbeam-channel fullness to diagnose bottlenecks:

- **Method**: Sample `sender.len()` / `capacity` periodically
- **Heuristics**:
  - Channel >90% full → Writer Bottleneck (parser faster than writer)
  - Channel <10% full → Parser Bottleneck (writer faster than parser)
  - Otherwise → Balanced

### 4. Bytes Read Tracking

Wrap the XML reader in a `TrackedReader` that counts bytes consumed:

```rust
impl<R: BufRead> BufRead for TrackedReader<R> {
    fn consume(&mut self, amt: usize) {
        self.metrics.add_bytes_read(amt as u64);
        self.inner.consume(amt);
    }
}
```

### 5. Cleanup Policy

Configurable retention with sensible defaults:
- Default: Keep last 10 runs
- Cleanup runs on startup after successful run completion
- Sort by directory name (timestamp order) for deterministic cleanup

### 6. Tee Logging

All log output goes to both:
- `stderr` (real-time visibility)
- `{run_dir}/etl.log` (permanent record)

## Consequences

### Positive

- **Observability**: Full visibility into every ETL run with structured data
- **Debugging**: Easy comparison between runs to identify regressions
- **Automation**: YAML reports enable automated monitoring and alerting
- **Reproducibility**: Config snapshots enable exact reproduction of any run
- **Low overhead**: 1Hz sampling has negligible impact on throughput
- **Self-cleaning**: Automatic cleanup prevents unbounded disk growth

### Negative

- **Disk usage**: Each run creates ~10-50KB of artifacts (mitigated by cleanup)
- **Complexity**: Additional modules (runs, sampler, report) to maintain
- **Startup cost**: ~10ms to create run directory and start sampler
- **Orphaned threads**: Channel sampling thread is not gracefully terminated

### Notes

**Key Files:**
- `src/runs.rs` - Run directory lifecycle
- `src/sampler.rs` - Resource sampling and bottleneck diagnosis
- `src/report.rs` - Report generation and YAML serialization
- `src/config.rs` - RunsConfig section

**Configuration:**
```yaml
runs:
  runs_dir: "runs"      # Directory for run artifacts
  keep_runs: 10         # Number of runs to retain
```

**Future Improvements:**
- Add real-time progress reporting during parsing
- Support custom run names via CLI flag
- Add Prometheus-compatible metrics export
- Implement run comparison tooling
