use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use crossbeam_channel::bounded;
use std::path::PathBuf;
use std::sync::Arc;
use uniprot_etl::config::Settings;
use uniprot_etl::fasta::load_fasta_map;
use uniprot_etl::metrics::Metrics;
use uniprot_etl::pipeline::parser::parse_entries;
use uniprot_etl::pipeline::reader::create_xml_reader;

fn benchmark_throughput(c: &mut Criterion) {
    let input_path = PathBuf::from("data/bench/bench_small.xml.gz");
    if !input_path.exists() {
        eprintln!("Benchmark data not found at {:?}. Run scripts/prepare_bench_data.py first.", input_path);
        return;
    }
    let sidecar_path = PathBuf::from("data/bench/bench_sidecar.fasta.gz");
    if !sidecar_path.exists() {
        eprintln!("FASTA sidecar not found at {:?}. Run scripts/prepare_bench_data.py first.", sidecar_path);
        return;
    }

    let mut group = c.benchmark_group("throughput");
    group.sample_size(10);
    // We know we extracted 10,000 entries
    group.throughput(Throughput::Elements(1_000));

    group.bench_function("parse_1k_entries", |b| {
        b.iter(|| {
             let metrics = Metrics::new();
             let (tx, rx) = bounded(1024); // Sink channel
             
             // Drain the channel in a separate thread to prevent blocking
             let drain_handle = std::thread::spawn(move || {
                 while let Ok(_) = rx.recv() {}
             });

             let settings = Settings::default();
             let reader = create_xml_reader(&input_path, &settings, &metrics)
                 .expect("Failed to create XML reader");
             let sidecar = load_fasta_map(&sidecar_path)
                 .expect("Failed to load FASTA sidecar");

             // Parse all entries in the file
             parse_entries(
                 reader,
                 tx,
                 &metrics,
                 1_000, // Batch size larger than file to process all in one go if possible
                 Some(Arc::new(sidecar)),
                 None,
                 None,
             )
             .expect("Failed to parse entries");

             drain_handle.join().unwrap();
        })
    });

    group.finish();
}

criterion_group!(benches, benchmark_throughput);
criterion_main!(benches);
