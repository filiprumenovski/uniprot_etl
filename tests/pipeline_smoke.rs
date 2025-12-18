use std::path::PathBuf;
use std::thread;

use arrow::record_batch::RecordBatch;
use crossbeam_channel::bounded;

use uniprot_etl::config::Settings;
use uniprot_etl::error::Result;
use uniprot_etl::metrics::Metrics;
use uniprot_etl::pipeline::parser::parse_entries;
use uniprot_etl::pipeline::reader::create_xml_reader;

/// Ignored by default: runs against the real UniProt file if available.
#[test]
#[ignore]
fn parses_real_uniprot_file_smoke() -> Result<()> {
    let path = PathBuf::from("NVMe2TB/uniprot_sprot.xml.gz");
    if !path.exists() {
        eprintln!("Skipping smoke test; file not found at {:?}", path);
        return Ok(());
    }

    let metrics = Metrics::new();
    let (tx, rx) = bounded::<RecordBatch>(8);
    let settings = Settings::default();

    // Drain batches in a consumer thread to avoid backpressure.
    let consumer = thread::spawn(move || {
        let mut rows = 0usize;
        for batch in rx {
            rows += batch.num_rows();
        }
        rows
    });

    let reader = create_xml_reader(&path, &settings, &metrics)?;
    parse_entries(reader, tx, &metrics, 5_000)?;

    let total_rows = consumer.join().expect("consumer thread panicked");
    assert!(total_rows > 0, "no rows parsed from real file");
    assert_eq!(metrics.entries() as usize, total_rows);

    Ok(())
}
