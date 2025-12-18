#!/usr/bin/env bash
set -euo pipefail

# Backwards-compatible wrapper.
# Prefer: just profile-flamegraph (Linux/macOS w/ cargo-flamegraph)
# Or:     bash scripts/profile_sample.sh --binary <path>

SECONDS="${SECONDS:-30}"
RUN_ID="${RUN_ID:-}"
RUNS_DIR="${RUNS_DIR:-runs}"

BINARY="${BINARY:-}"
if [[ -z "${BINARY}" ]]; then
	BINARY=$(ls -t target/release/deps/flamegraph_benchmark-* 2>/dev/null | grep -v '\.d$' | head -1 || true)
fi

if [[ -z "${BINARY}" ]]; then
	echo "Could not locate bench binary. Build it first:" >&2
	echo "  cargo build --bench flamegraph_benchmark --release" >&2
	exit 1
fi

exec bash scripts/profile_sample.sh --binary "${BINARY}" --seconds "${SECONDS}" --runs-dir "${RUNS_DIR}" ${RUN_ID:+--run-id "${RUN_ID}"}
