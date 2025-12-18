#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: clean_data.sh [--dry-run] [--force] [--keep-parquet] [--threshold-mb N]

Removes generated data under the data directory. By default deletes raw,
parquet, species, tmp, and logs. Use --keep-parquet to preserve parquet output.
Use --dry-run to preview. --force skips the confirmation prompt.
EOF
}

DRY_RUN=false
FORCE=false
KEEP_PARQUET=false
THRESHOLD_MB=${THRESHOLD_MB:-500}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true ;;
    --force) FORCE=true ;;
    --keep-parquet) KEEP_PARQUET=true ;;
    --threshold-mb) shift; THRESHOLD_MB=${1:-500} ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
  shift || true
done

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${ROOT_DIR}/data"

TARGETS=(
  "${DATA_DIR}/raw"
  "${DATA_DIR}/parquet"
  "${DATA_DIR}/species"
  "${DATA_DIR}/tmp"
  "${DATA_DIR}/logs"
)

if ${KEEP_PARQUET}; then
  TARGETS=("${DATA_DIR}/raw" "${DATA_DIR}/species" "${DATA_DIR}/tmp" "${DATA_DIR}/logs")
fi

if [[ ! -d "${DATA_DIR}" ]]; then
  echo "data directory not found at ${DATA_DIR}" >&2
  exit 1
fi

echo "Scanning for files > ${THRESHOLD_MB}MB under ${DATA_DIR}..."
find "${DATA_DIR}" -type f -size +"${THRESHOLD_MB}"M -print || true

echo "Planned removals:"
for path in "${TARGETS[@]}"; do
  if [[ -e "${path}" ]]; then
    echo "  ${path}"
  else
    echo "  (skip, not found) ${path}"
  fi
done

action="remove"
${DRY_RUN} && action="would remove"

echo
if ! ${FORCE} && ! ${DRY_RUN}; then
  read -r -p "Proceed to ${action} listed paths? [y/N] " reply
  case "${reply}" in
    [yY][eE][sS]|[yY]) ;;
    *) echo "Aborted."; exit 0 ;;
  esac
fi

for path in "${TARGETS[@]}"; do
  if [[ -d "${path}" ]]; then
    if ${DRY_RUN}; then
      echo "Would remove contents of ${path}/*"
    else
      echo "Removing contents of ${path}/*"
      rm -rf -- "${path}"/*
    fi
  elif [[ -e "${path}" ]]; then
    if ${DRY_RUN}; then
      echo "Would remove file ${path}"
    else
      echo "Removing file ${path}"
      rm -f -- "${path}"
    fi
  fi
done

# Ensure directories exist after cleanup
for path in "${TARGETS[@]}"; do
  if [[ ! -e "${path}" ]]; then
    mkdir -p "${path}"
    echo "Recreated directory ${path}"
  fi
done

echo "Done."