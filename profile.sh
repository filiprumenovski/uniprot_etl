#!/bin/bash

# macOS Profiling Script for UniProt ETL Benchmark
# This script profiles the benchmark using macOS native tools

BINARY="./target/release/deps/flamegraph_benchmark-$(ls target/release/deps/flamegraph_benchmark-* | grep -v '.d$' | head -1 | sed 's/.*-//')"

echo "🔍 Starting benchmark with macOS profiling..."
echo "Binary: $BINARY"

# Run with sample profiler for 30 seconds
echo "⏱️  Profiling for 30 seconds..."
sample "$BINARY" 30 -o flamegraph_profile.txt 2>/dev/null

echo "✅ Profile saved to: flamegraph_profile.txt"
echo ""
echo "📊 Top functions by sample count:"
head -50 flamegraph_profile.txt | grep -E "^\s+[0-9]+" | head -10
