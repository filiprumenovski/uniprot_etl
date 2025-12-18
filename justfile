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

run args="":
    cargo run --release --bin uniprot_etl -- {{args}}

pipeline input_path:
    cargo run --release --bin uniprot_etl -- --input "{{input_path}}"

pipeline-debug input_path:
    cargo run --bin uniprot_etl -- --input "{{input_path}}"

clean-data flags="--force":
    bash scripts/clean_data.sh {{flags}}

clean-data-dry:
    bash scripts/clean_data.sh --dry-run

fetch-data url out_file="" flags="":
    UNIPROT_URL={{url}} OUT_FILE={{out_file}} bash scripts/fetch_uniprot.sh {{flags}}

profile-flamegraph bench="flamegraph_benchmark" flags="":
    BENCH_TARGET={{bench}} bash scripts/profile_flamegraph.sh {{flags}}
