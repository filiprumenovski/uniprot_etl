#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: profile_flamegraph.sh [--bench NAME] [--run-id ID] [--runs-dir DIR] [--output DIR] [--release] [--args "..."]

Builds and profiles a Criterion bench with cargo-flamegraph. Requires
cargo-flamegraph installed (cargo install flamegraph). On macOS you may need
sudo and to enable dtrace/Developer Mode.

By default, artifacts are written under:
  runs/<run_id>/profiles/
EOF
}

BENCH_TARGET="${BENCH_TARGET:-flamegraph_benchmark}"
OUTPUT_DIR="${OUTPUT_DIR:-}"
RUN_ID=""
RUNS_DIR=""
RELEASE=false
EXTRA_ARGS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bench) shift; BENCH_TARGET=${1:-${BENCH_TARGET}} ;;
    --run-id) shift; RUN_ID=${1:-} ;;
    --runs-dir) shift; RUNS_DIR=${1:-} ;;
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

if [[ -n "${OUTPUT_DIR}" ]]; then
  OUT_DIR="${OUTPUT_DIR}"
else
  RUNS_ROOT="${RUNS_DIR:-${ROOT_DIR}/runs}"

  if [[ -z "${RUN_ID}" ]]; then
    timestamp=$(date +%Y%m%d_%H%M%S)
    RUN_ID="run_${timestamp}"
  elif [[ "${RUN_ID}" != run_* ]]; then
    RUN_ID="run_${RUN_ID}"
  fi

  # Avoid collisions if a directory already exists.
  RUN_DIR="${RUNS_ROOT}/${RUN_ID}"
  if [[ -e "${RUN_DIR}" ]]; then
    RUN_DIR="${RUN_DIR}-p$$"
  fi

  OUT_DIR="${RUN_DIR}/profiles"
fi

mkdir -p "${OUT_DIR}"

BUILD_FLAGS=()
${RELEASE} && BUILD_FLAGS+=(--release)

# Build first to fail fast if bench is missing
cargo build "${BUILD_FLAGS[@]}" --bench "${BENCH_TARGET}"

out_file="${OUT_DIR}/flamegraph-${BENCH_TARGET}.svg"

echo "Running cargo flamegraph for bench ${BENCH_TARGET} -> ${out_file}" >&2
cargo flamegraph "${BUILD_FLAGS[@]}" --output "${out_file}" --bench "${BENCH_TARGET}" -- ${EXTRA_ARGS}

meta_file="${OUT_DIR}/profile_meta.yaml"
{
  echo "kind: bench"
  echo "tool: cargo-flamegraph"
  echo "bench: ${BENCH_TARGET}"
  echo "release: ${RELEASE}"
  echo "created_at_utc: '$(date -u +%Y-%m-%dT%H:%M:%SZ)'"
  echo "cwd: '${ROOT_DIR}'"
  echo "output_svg: '${out_file}'"
  echo "extra_args: '${EXTRA_ARGS}'"
} > "${meta_file}"

ls -lh "${out_file}"
echo "Flamegraph written to ${out_file}"
echo "Metadata written to ${meta_file}"