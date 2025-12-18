# ADR-0001: Selecting Rust for Memory Safety and High-Throughput Execution

## Status

Accepted

## Context

UniProt processes millions of entries with complex nested data structures (isoforms, features, subcellular locations, evidence references). The Swiss-Prot XML dump is frequently >500MB and must be transformed into a columnar Parquet format for downstream analysis.

**Requirements:**
- Strict biological fidelity: preserve evidence codes (ECO), isoform sequences, and feature coordinates losslessly.
- Memory efficiency: <500MB RAM consumption during streaming parse to support commodity machines.
- Speed: target <10 minutes for full Swiss-Prot dumps on modest hardware.
- Reliability: prevent data corruption and unsafe memory access during concurrent I/O.

**Alternatives considered:**
- Python with Pandas/PyArrow: higher memory overhead, slower row-wise transformation, risk of silent numeric precision loss.
- C++: faster but manual memory management increases maintenance burden and audit difficulty.
- Go: simpler concurrency but less ergonomic for complex schema manipulation.

## Decision

Implement UniProt_ETL in **Rust** as the primary language for the ETL pipeline.

**Rationale:**
- **Memory safety:** Rust's ownership system prevents buffer overflows, use-after-free, and data races at compile time; critical for reproducible scientific pipelines.
- **Performance:** Zero-cost abstractions, SIMD-friendly iterators, and native threading yield throughput matching C++.
- **Ecosystem:** Arrow, Parquet, and quick-xml crates provide high-quality, Rust-native bindings with proven Unicode and compression handling.
- **Scientific trust:** Compiled, type-safe binaries reduce the attack surface and enable deterministic reproducibility.

## Consequences

### Positive
- No garbage collection pauses; predictable latency profile suitable for batch ETL.
- Memory layout controllable; tight packing enables effective CPU cache use.
- Compile-time trait system facilitates correct schema evolution and refactoring.
- Excellent cross-platform tooling (cargo, rustup) for reproducible builds.

### Negative
- Steeper learning curve for team members unfamiliar with borrow-checker.
- Compile times slower than dynamic languages (mitigated by incremental builds and caching).
- Smaller ecosystem than Python for niche bioinformatics libraries (though core ETL tools well-covered).

### Notes

**Key files:**
- [src/main.rs](../../src/main.rs): orchestrates config, CLI, and thread spawning.
- [src/lib.rs](../../src/lib.rs): exposes public modules.
- [Cargo.toml](../../Cargo.toml): dependency management; critical pins for Arrow/Parquet versions.

**Performance targets:** Validate <500MB RAM and <10 min throughput in CI/benchmarks (see [benches/](../../benches/)).

**Future:** Monitor WASM compilation feasibility for in-browser preview and testing.
