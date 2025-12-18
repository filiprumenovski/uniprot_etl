#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: fetch_uniprot.sh --url URL [--out-file NAME] [--gunzip]

Downloads a UniProt dataset into data/raw. Provide the source URL via --url or
UNIPROT_URL. Optionally rename via --out-file (defaults to the URL basename) and
pass --gunzip to decompress after download.
EOF
}

UNIPROT_URL=${UNIPROT_URL:-}
OUT_FILE=${OUT_FILE:-}
GUNZIP=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --url) shift; UNIPROT_URL=${1:-} ;;
    --out-file) shift; OUT_FILE=${1:-} ;;
    --gunzip) GUNZIP=true ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
  shift || true
done

if [[ -z "${UNIPROT_URL}" ]]; then
  echo "UNIPROT_URL is required (via --url or environment)." >&2
  usage
  exit 1
fi

command -v curl >/dev/null 2>&1 || { echo "curl is required." >&2; exit 1; }

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOT_DIR}/data/raw"
mkdir -p "${OUT_DIR}"

if [[ -z "${OUT_FILE}" ]]; then
  OUT_FILE="$(basename "${UNIPROT_URL}")"
fi

OUT_PATH="${OUT_DIR}/${OUT_FILE}"

echo "Downloading ${UNIPROT_URL}" >&2
curl -fL "${UNIPROT_URL}" -o "${OUT_PATH}"
ls -lh "${OUT_PATH}"

if ${GUNZIP}; then
  if [[ "${OUT_PATH}" != *.gz ]]; then
    echo "--gunzip requested but file does not end with .gz" >&2
    exit 1
  fi
  echo "Decompressing ${OUT_PATH}" >&2
  gunzip -f "${OUT_PATH}"
  OUT_PATH="${OUT_PATH%.gz}"
  ls -lh "${OUT_PATH}"
fi

echo "Done -> ${OUT_PATH}"