use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use crossbeam_channel::bounded;
use std::path::{Path, PathBuf};
use std::thread;
use uniprot_etl::config::Settings;
use uniprot_etl::metrics::Metrics;
use uniprot_etl::pipeline::parser::parse_entries;
use uniprot_etl::pipeline::reader::create_xml_reader;
use uniprot_etl::writer::parquet::write_batches;

fn find_uniprot_file() -> Option<PathBuf> {
    let paths = vec![
        "NVMe 2TB/uniprot_sprot.xml.gz",
        "NVMe2TB/uniprot_sprot.xml.gz",
        "/Volumes/NVMe 2TB/uniprot_sprot.xml.gz",
        "/Volumes/NVMe2TB/uniprot_sprot.xml.gz",
        "./data/uniprot_sprot.xml.gz",
    ];

    for path in paths {
        let p = Path::new(path);
        if p.exists() {
            return Some(p.to_path_buf());
        }
    }
    None
}

fn benchmark_pipeline_50k_batch(c: &mut Criterion) {
    let input_file = match find_uniprot_file() {
        Some(f) => {
            println!("Found UniProt file at: {:?}", f);
            f
        }
        None => {
            eprintln!("Warning: No UniProt data file found. Benchmark will be skipped.");
            eprintln!("Expected at one of: NVMe2TB/uniprot_sprot.xml.gz or similar");
            return;
        }
    };

    let mut group = c.benchmark_group("flamegraph_50k");

    // Set reasonable sample size for flamegraph
    group.sample_size(10);
    group.throughput(Throughput::Elements(50000));

    group.bench_function("parse_and_write_50k_batch", |b| {
        b.iter(|| {
            let metrics = Metrics::new();
            let (tx, rx) = bounded(8);

            let output_path = PathBuf::from("/tmp/output_flamegraph.parquet");
            let writer_metrics = metrics.clone();
            let settings = Settings::default();
            let writer_settings = settings.clone();

            let writer_handle = thread::spawn(move || {
                write_batches(rx, &output_path, &writer_metrics, &writer_settings)
            });

            let reader = create_xml_reader(input_file.as_path(), &settings, &metrics)
                .expect("Failed to create XML reader");

            parse_entries(
                reader,
                tx,
                &metrics,
                black_box(50000), // 50k batch size
            )
            .expect("Failed to parse entries");

            writer_handle
                .join()
                .expect("Writer thread panicked")
                .expect("Writer failed");
        })
    });

    group.finish();
}

criterion_group!(benches, benchmark_pipeline_50k_batch);
criterion_main!(benches);
