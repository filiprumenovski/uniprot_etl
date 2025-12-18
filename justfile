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
