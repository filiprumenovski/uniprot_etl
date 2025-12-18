# ADR-0003: Producer-Consumer Pipeline with crossbeam-channel for I/O Decoupling

## Status

Accepted

## Context

UniProt transformation is I/O-bound: XML parsing and Parquet serialization have different performance profiles.
- **Parsing:** CPU-intensive state machine; benefits from optimization but is latency-tolerant within a batch window.
- **Writing:** I/O-intensive; disk throughput varies; blocking on slow storage would stall parsing.

**Design goals:**
- Decouple parsing and writing to avoid blocking one on the other.
- Buffer a small number of batches (typically 4–8) in memory to smooth I/O variance.
- Graceful error propagation from writer back to parser (e.g., if Parquet write fails, stop parsing).
- Predictable memory use (bounded channel prevents unbounded accumulation).

## Decision

Implement a **producer-consumer architecture** using `crossbeam-channel` with a **bounded queue** (default 8 batches).

**Architecture:**
- **Main thread** (producer): runs the streaming XML parser, accumulates entries into batches, sends filled batches to the channel.
- **Writer thread** (consumer): receives batches, serializes to Parquet, writes to disk.
- **Synchronization:** crossbeam-channel is MPMC (multi-producer, multi-consumer) but used as SPSC here for simplicity and performance.
- **Batch size:** configurable (default 10,000 entries); batching amortizes Parquet row-group metadata overhead.

**Implementation:**
- [src/main.rs](../../src/main.rs): spawns writer thread, iterates parser, sends batches via channel.
- [src/writer/parquet.rs](../../src/writer/parquet.rs): consumer loop; receives batch, writes row group.
- [src/metrics.rs](../../src/metrics.rs): atomic counters to track batches, entries, and I/O throughput.

## Consequences

### Positive
- **Independent scaling:** Parser and writer threads can be tuned independently; e.g., if disk is slow, buffer absorbs variance.
- **Clean separation:** Parser logic does not reference disk I/O; writer does not know about XML structure.
- **Graceful backpressure:** Bounded channel blocks parser if writer lags (prevents memory exhaustion).
- **Error propagation:** Writer errors (e.g., disk full) surface cleanly; parser can halt.

### Negative
- **Thread overhead:** Two threads add context-switch cost; mitigated by high work-per-message ratio (batches of 10k entries).
- **Channel latency:** Bounded queue means parser blocks if buffer is full; trade-off between latency and resource use.
- **Synchronization complexity:** Thread-safe shared state (metrics, config) requires careful initialization; Rust's type system enforces correctness.

### Notes

**Configuration:**
- `channel_capacity` in [config.yaml](../../config.yaml): default 8 (capacity in number of batches, not entries).
- `batch_size`: default 10,000; increases row-group efficiency at cost of memory.

**Backpressure tuning:**
- If parser is faster than writer, increase `batch_size` or `channel_capacity` to reduce blocking.
- If memory is constrained, reduce both to lower peak memory.

**Metrics:**
- [src/metrics.rs](../../src/metrics.rs) tracks batches sent/received, entries processed, and I/O throughput.
- Printed at end of run for performance validation.

**Testing:**
- [benches/throughput.rs](../../benches/throughput.rs): measures end-to-end throughput with real data.
- [benches/flamegraph_benchmark.rs](../../benches/flamegraph_benchmark.rs): profiles writer + parser on 50k-entry subset.

**Future:** Consider work-stealing or rayon-style thread pools if writer I/O becomes multi-target (e.g., Parquet + Delta Lake + object store simultaneously).
