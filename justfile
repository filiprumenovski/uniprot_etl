set shell := ["bash", "-cu"]
set dotenv-load := true

bin := "uniprot_etl"
config := "config.yaml"
runs_dir := "runs"
default_output := "data/parquet"

default:
    @just --list

# === Core ===
fmt:
    cargo fmt

lint:
    cargo clippy --all-targets --all-features -- -D warnings

test:
    cargo test --all

check:
    just fmt
    just lint
    just test

bench:
    cargo bench

# === Run ===
run input_path:
    cargo run --release --bin {{bin}} -- --config {{config}} --input "{{input_path}}"

run-debug input_path:
    cargo run --bin {{bin}} -- --config {{config}} --input "{{input_path}}"

run-swarm input_dir output_dir=default_output:
    cargo run --release --bin {{bin}} -- --config {{config}} --input "{{input_dir}}" --output "{{output_dir}}"

# === Data ===
download-sprot output_dir:
    python3 scripts/download_uniprot.py sprot --output-dir "{{output_dir}}"

download-trembl output_dir:
    python3 scripts/download_uniprot.py trembl --output-dir "{{output_dir}}"

download-trembl-fasta output_dir:
    python3 scripts/download_uniprot.py trembl-fasta --output-dir "{{output_dir}}"

download-sprot-varsplic output_dir:
    python3 scripts/download_uniprot.py sprot-varsplic --output-dir "{{output_dir}}"

data-clean flags="--force":
    bash scripts/clean_data.sh {{flags}}

data-clean-dry:
    bash scripts/clean_data.sh --dry-run

data-fetch url out_file="" flags="":
    UNIPROT_URL={{url}} OUT_FILE={{out_file}} bash scripts/fetch_uniprot.sh {{flags}}

# === GUI ===
gui-check:
    @command -v node >/dev/null || (echo "Node.js missing. Install: brew install node"; exit 1)
    @command -v npm >/dev/null || (echo "npm missing. Install: brew install node"; exit 1)
    @cargo tauri --version >/dev/null 2>&1 || (echo "cargo-tauri missing. Install: cargo install tauri-cli"; exit 1)
    @echo "GUI prerequisites OK"

gui-install-node:
    brew install node

gui-setup:
    cd gui/frontend && npm install

gui-dev:
    cd gui/src-tauri && cargo tauri dev

gui-build:
    cd gui/src-tauri && cargo tauri build

gui-frontend:
    cd gui/frontend && npm run dev

gui-run-release:
    cd gui/src-tauri && cargo tauri build --no-bundle
    ./target/release/uniprot-etl-gui

gui-with-dashboard: gui-run-release

gui-full: gui-check gui-setup gui-dev

# === Legacy Aliases ===
dev-check: check
clean-data: data-clean
clean-data-dry: data-clean-dry
fetch-data url out_file="" flags="":
    @just data-fetch {{url}} {{out_file}} {{flags}}
gui-install: gui-setup
