#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: profile_flamegraph.sh [--bench NAME] [--output DIR] [--release] [--args "..."]

Builds and profiles a Criterion bench with cargo-flamegraph. Requires
cargo-flamegraph installed (cargo install flamegraph). On macOS you may need
sudo and to enable dtrace/Developer Mode.
EOF
}

BENCH_TARGET="${BENCH_TARGET:-flamegraph_benchmark}"
OUTPUT_DIR="${OUTPUT_DIR:-}"
RELEASE=false
EXTRA_ARGS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bench) shift; BENCH_TARGET=${1:-${BENCH_TARGET}} ;;
    --output) shift; OUTPUT_DIR=${1:-} ;;
    --release) RELEASE=true ;;
    --args) shift; EXTRA_ARGS=${1:-} ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
  shift || true
done

command -v cargo >/dev/null 2>&1 || { echo "cargo is required." >&2; exit 1; }
command -v cargo flamegraph >/dev/null 2>&1 || { echo "cargo-flamegraph is required (cargo install flamegraph)." >&2; exit 1; }

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUTPUT_DIR:-${ROOT_DIR}/target/flamegraphs}"
mkdir -p "${OUT_DIR}"

BUILD_FLAGS=()
${RELEASE} && BUILD_FLAGS+=(--release)

# Build first to fail fast if bench is missing
cargo build "${BUILD_FLAGS[@]}" --bench "${BENCH_TARGET}"

timestamp=$(date +%Y%m%d%H%M%S)
out_file="${OUT_DIR}/${BENCH_TARGET}-${timestamp}.svg"

echo "Running cargo flamegraph for bench ${BENCH_TARGET} -> ${out_file}" >&2
cargo flamegraph "${BUILD_FLAGS[@]}" --output "${out_file}" --bench "${BENCH_TARGET}" -- ${EXTRA_ARGS}

ls -lh "${out_file}"
echo "Flamegraph written to ${out_file}"