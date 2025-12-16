# Flamegraph Benchmark Setup

## ✅ Quick Start

The benchmark is configured and ready to run with **50k batch size**. Your UniProt file is at:
```
/Volumes/NVMe 2TB/uniprot_sprot.xml.gz
```

## Running the Benchmark

### Standard Criterion Benchmarking (Recommended):
```bash
cargo bench --bench flamegraph_benchmark
```

**Output you'll see:**
- Throughput: ~2.15k elements/second
- Time per iteration: ~23.2 seconds
- Parquet output: ~283 MB
- HTML reports: `target/criterion/flamegraph_50k/`

## Profiling Options by Platform

### macOS (Apple Silicon / Intel)

Since full Xcode isn't available, use these alternatives:

#### Option 1: Criterion Built-in Profiling
```bash
cargo bench --bench flamegraph_benchmark
# Then view HTML report:
open target/criterion/flamegraph_50k/report/index.html
```

#### Option 2: macOS Sample Profiler (Native)
```bash
# Build the benchmark
cargo build --bench flamegraph_benchmark --release

# Find the binary
BINARY=$(ls target/release/deps/flamegraph_benchmark-* | grep -v '.d$' | head -1)

# Run and profile for N seconds (e.g., 30 seconds)
sample "$BINARY" 30 -o profile_output.txt

# View results
less profile_output.txt
```

#### Option 3: Instruments (if Xcode Command Line Tools updated)
```bash
# This requires Xcode to be fully installed
xcrun xctrace record --template "System Trace" --output flamegraph.trace -- cargo bench --bench flamegraph_benchmark

# View with:
open flamegraph.trace
```

### Linux

```bash
# Install flamegraph dependencies
sudo apt-get install linux-tools-common

# Run with flamegraph
cargo flamegraph --bench flamegraph_benchmark -o flamegraph.svg

# Open the SVG
open flamegraph.svg
```

## Configuration Details

The benchmark is set up with:
- **Batch size:** 50,000 entries per batch (configurable)
- **Sample size:** 10 iterations
- **Output:** `/tmp/output_flamegraph.parquet`
- **Throughput measurement:** Elements per second
- **Debug info:** Enabled for better profiling (`[profile.bench] debug = true`)

## Interpreting Results

### From Criterion Reports:
1. **Throughput**: Shows elements processed per second
2. **Execution Time**: Time per iteration
3. **Variability**: Standard deviation and outliers
4. **Trends**: If you run multiple times, shows performance consistency

### From Sample Profiler:
1. **Hot functions**: Listed by sample count (more samples = more CPU time)
2. **Call stacks**: Shows which functions call the hot functions
3. **Module breakdown**: Identifies bottlenecks in your code vs dependencies

## Example: Running a Profile

```bash
# Build in release mode with debug symbols
cargo build --bench flamegraph_benchmark --release

# Get the binary path
BINARY=$(ls -t target/release/deps/flamegraph_benchmark-* | grep -v '.d$' | head -1)
echo "Binary: $BINARY"

# Profile for 20 seconds
sample "$BINARY" 20 -o my_profile.txt

# See hotspots
grep "%" my_profile.txt | head -20
```

## Files Generated

- `target/criterion/flamegraph_50k/` - Criterion reports and plots
- `target/release/deps/flamegraph_benchmark-*` - Release binary
- `profile_output.txt` - Sample profiler output
- `flamegraph.svg` - Flamegraph visualization (Linux)

## Batch Size Configuration

To benchmark with different batch sizes, edit `benches/flamegraph_benchmark.rs`:
```rust
parse_entries(
    reader,
    tx,
    &metrics,
    black_box(50000),  // Change this value (e.g., 25000, 100000)
)
```

Then rebuild and run:
```bash
cargo bench --bench flamegraph_benchmark
```
