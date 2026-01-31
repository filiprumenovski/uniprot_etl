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
data-clean flags="--force":
    bash scripts/clean_data.sh {{flags}}

data-clean-dry:
    bash scripts/clean_data.sh --dry-run

data-fetch url out_file="" flags="":
    UNIPROT_URL={{url}} OUT_FILE={{out_file}} bash scripts/fetch_uniprot.sh {{flags}}

# === Profiling ===
profile-flamegraph bench="flamegraph_benchmark" run_id="" runs_dir=runs_dir flags="":
    BENCH_TARGET={{bench}} bash scripts/profile_flamegraph.sh --runs-dir {{runs_dir}} {{if run_id != "" { "--run-id " + run_id } else { "" }}} {{flags}}

profile-pipeline run_id="" runs_dir=runs_dir flags="":
    bash scripts/profile_pipeline_flamegraph.sh --runs-dir {{runs_dir}} {{if run_id != "" { "--run-id " + run_id } else { "" }}} {{flags}}

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
fetch-data: data-fetch
gui-install: gui-setup
