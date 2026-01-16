set shell := ["bash", "-cu"]
set dotenv-load := true

default:
    @just --list

fmt:
    cargo fmt

lint:
    cargo clippy --all-targets --all-features -- -D warnings

test:
    cargo test --all

dev-check:
    just fmt
    just lint
    just test

bench:
    cargo bench

run input_path:
    cargo run --release --bin uniprot_etl -- --config config.yaml --input "{{input_path}}"

run-debug input_path:
    cargo run --bin uniprot_etl -- --config config.yaml --input "{{input_path}}"

# Swarm mode: process a directory of XML files in parallel
run-swarm input_dir output_dir="data/parquet":
    cargo run --release --bin uniprot_etl -- --config config.yaml --input "{{input_dir}}" --output "{{output_dir}}"

clean-data flags="--force":
    bash scripts/clean_data.sh {{flags}}

clean-data-dry:
    bash scripts/clean_data.sh --dry-run

fetch-data url out_file="" flags="":
    UNIPROT_URL={{url}} OUT_FILE={{out_file}} bash scripts/fetch_uniprot.sh {{flags}}

profile-flamegraph bench="flamegraph_benchmark" run_id="" runs_dir="runs" flags="":
    BENCH_TARGET={{bench}} bash scripts/profile_flamegraph.sh --runs-dir {{runs_dir}} {{if run_id != "" { "--run-id " + run_id } else { "" }}} {{flags}}

profile-pipeline run_id="" runs_dir="runs" flags="":
    bash scripts/profile_pipeline_flamegraph.sh --runs-dir {{runs_dir}} {{if run_id != "" { "--run-id " + run_id } else { "" }}} {{flags}}

# === GUI Commands ===

# Check GUI prerequisites (node, npm, cargo-tauri)
gui-check:
    #!/usr/bin/env bash
    set -e
    echo "Checking GUI prerequisites..."
    if ! command -v node &> /dev/null; then
        echo "❌ Node.js not found. Install with: brew install node"
        exit 1
    fi
    echo "✓ Node.js $(node --version)"
    if ! command -v npm &> /dev/null; then
        echo "❌ npm not found. Install with: brew install node"
        exit 1
    fi
    echo "✓ npm $(npm --version)"
    if ! cargo tauri --version &> /dev/null; then
        echo "⚠ cargo-tauri not found. Installing..."
        cargo install tauri-cli
    fi
    echo "✓ cargo-tauri $(cargo tauri --version)"
    echo "All prerequisites satisfied!"

# Install Node.js via Homebrew (macOS)
gui-install-node:
    brew install node

# Install frontend dependencies
gui-install: gui-check
    cd gui/frontend && npm install

# Run the Tauri desktop app in development mode
gui-dev:
    cd gui/src-tauri && cargo tauri dev

# Build the Tauri desktop app for production
gui-build:
    cd gui/src-tauri && cargo tauri build

# Run frontend only (for development without Tauri)
gui-frontend:
    cd gui/frontend && npm run dev

# === Observability Commands ===

# Run GUI app in release mode with live Grafana dashboard embedded
gui-with-dashboard:
    #!/usr/bin/env bash
    set -e
    echo "Starting observability stack..."
    cd observability && docker compose up -d
    cd ..
    echo "Waiting for Grafana to be ready..."
    sleep 3
    echo "Building and launching GUI with embedded Grafana..."
    cd gui/src-tauri && cargo tauri dev

# Start Prometheus + Grafana stack
observability-up:
    cd observability && docker compose up -d

# Stop observability stack
observability-down:
    cd observability && docker compose down

# View observability logs
observability-logs:
    cd observability && docker compose logs -f

# Full GUI setup: install deps, start observability, launch app
gui-full: gui-install observability-up gui-dev
