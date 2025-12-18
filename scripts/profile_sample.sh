#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: profile_sample.sh --binary PATH [--seconds N] [--run-id ID] [--runs-dir DIR] [--output FILE]

Profiles a binary using macOS 'sample' and writes artifacts under:
  runs/<run_id>/profiles/

Notes:
- macOS only (requires 'sample' command).
- If --output is provided, it is used verbatim.
EOF
}

BINARY=""
SECONDS=30
RUN_ID=""
RUNS_DIR=""
OUTPUT_FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --binary) shift; BINARY=${1:-} ;;
    --seconds) shift; SECONDS=${1:-30} ;;
    --run-id) shift; RUN_ID=${1:-} ;;
    --runs-dir) shift; RUNS_DIR=${1:-} ;;
    --output) shift; OUTPUT_FILE=${1:-} ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
  shift || true
done

if [[ -z "${BINARY}" ]]; then
  echo "--binary is required" >&2
  usage
  exit 1
fi

command -v sample >/dev/null 2>&1 || { echo "macOS 'sample' command not found." >&2; exit 1; }

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -n "${OUTPUT_FILE}" ]]; then
  out_file="${OUTPUT_FILE}"
else
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
  fi

  PROFILE_DIR="${RUN_DIR}/profiles"
  mkdir -p "${PROFILE_DIR}"
  out_file="${PROFILE_DIR}/sample.txt"
fi

echo "Profiling with sample for ${SECONDS}s" >&2
sample "${BINARY}" "${SECONDS}" -o "${out_file}" 2>/dev/null

meta_file="$(dirname "${out_file}")/profile_meta.yaml"
{
  echo "kind: sample"
  echo "tool: sample"
  echo "binary: '${BINARY}'"
  echo "seconds: ${SECONDS}"
  echo "created_at_utc: '$(date -u +%Y-%m-%dT%H:%M:%SZ)'"
  echo "cwd: '${ROOT_DIR}'"
  echo "output_txt: '${out_file}'"
} > "${meta_file}"

ls -lh "${out_file}"
echo "Sample output written to ${out_file}"
echo "Metadata written to ${meta_file}"
