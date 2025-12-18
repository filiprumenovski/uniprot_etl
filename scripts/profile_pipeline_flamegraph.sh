#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: profile_pipeline_flamegraph.sh [--run-id ID] [--runs-dir DIR] [--release] [--args "..."]

Profiles the ETL binary (uniprot_etl) with cargo-flamegraph and consolidates
artifacts under:
  runs/<run_id>/profiles/

The ETL process itself will also write its run artifacts into the same
runs/<run_id>/ directory via the --run-id override.
EOF
}

RUN_ID=""
RUNS_DIR=""
RELEASE=false
EXTRA_ARGS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-id) shift; RUN_ID=${1:-} ;;
    --runs-dir) shift; RUNS_DIR=${1:-} ;;
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
RUNS_ROOT="${RUNS_DIR:-${ROOT_DIR}/runs}"

if [[ -z "${RUN_ID}" ]]; then
  timestamp=$(date +%Y%m%d_%H%M%S)
  RUN_ID="run_${timestamp}"
elif [[ "${RUN_ID}" != run_* ]]; then
  RUN_ID="run_${RUN_ID}"
fi

RUN_DIR="${RUNS_ROOT}/${RUN_ID}"
if [[ -e "${RUN_DIR}" ]]; then
  RUN_DIR="${RUN_DIR}-p$$"
  RUN_ID="$(basename "${RUN_DIR}")"
fi

PROFILE_DIR="${RUN_DIR}/profiles"
mkdir -p "${PROFILE_DIR}"

BUILD_FLAGS=()
${RELEASE} && BUILD_FLAGS+=(--release)

out_file="${PROFILE_DIR}/flamegraph-uniprot_etl.svg"

echo "Running cargo flamegraph for uniprot_etl -> ${out_file}" >&2
cargo flamegraph "${BUILD_FLAGS[@]}" --output "${out_file}" --bin uniprot_etl -- \
  --run-id "${RUN_ID}" ${EXTRA_ARGS}

meta_file="${PROFILE_DIR}/profile_meta.yaml"
{
  echo "kind: pipeline"
  echo "tool: cargo-flamegraph"
  echo "bin: uniprot_etl"
  echo "run_id: ${RUN_ID}"
  echo "release: ${RELEASE}"
  echo "created_at_utc: '$(date -u +%Y-%m-%dT%H:%M:%SZ)'"
  echo "cwd: '${ROOT_DIR}'"
  echo "output_svg: '${out_file}'"
  echo "extra_args: '${EXTRA_ARGS}'"
} > "${meta_file}"

ls -lh "${out_file}"
echo "Flamegraph written to ${out_file}"
echo "Metadata written to ${meta_file}"
