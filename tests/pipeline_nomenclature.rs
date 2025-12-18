use std::collections::HashMap;
use std::fs;
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{Array, Int8Array, ListArray, StringArray, StructArray};
use crossbeam_channel::unbounded;
use quick_xml::Reader;

use uniprot_etl::error::Result;
use uniprot_etl::metrics::Metrics;
use uniprot_etl::pipeline::parser::parse_entries;

#[test]
fn parses_nomenclature_and_structures_from_sample() -> Result<()> {
    // Load small sample XML (includes TP53-like fields)
    let xml_path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("data/raw/sample_uniprot.xml");
    let xml = fs::read_to_string(xml_path)?;
    let mut reader = Reader::from_reader(Cursor::new(xml.as_bytes()));
    reader.config_mut().trim_text(true);

    let metrics = Metrics::new();
    let (tx, rx) = unbounded();

    let mut sidecar = HashMap::new();
    // sample_uniprot.xml contains one isoform ref: P04637-1
    sidecar.insert("P04637-1".to_string(), "MEEPQSDPSV".to_string());
    parse_entries(reader, tx, &metrics, 16, Some(Arc::new(sidecar)))?;

    let batches: Vec<_> = rx.iter().collect();
    assert_eq!(batches.len(), 1);

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    // Column indices by name
    let schema = batch.schema();
    let idx = |name: &str| {
        schema
            .fields()
            .iter()
            .position(|f| f.name() == name)
            .expect(name)
    };

    // Top-level id remains present
    let ids = batch
        .column(idx("id"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ids.value(0), "P04637-1");

    // parent_id anchors back to canonical accession
    let parent_id = batch
        .column(idx("parent_id"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(parent_id.value(0), "P04637");

    // New name fields
    let entry_name = batch
        .column(idx("entry_name"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(entry_name.is_valid(0));
    let gene_name = batch
        .column(idx("gene_name"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(gene_name.value(0), "TP53");
    let protein_name = batch
        .column(idx("protein_name"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(protein_name.is_valid(0));
    let organism_name = batch
        .column(idx("organism_name"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(organism_name.value(0), "Homo sapiens");

    // Existence mapping
    let existence = batch
        .column(idx("existence"))
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap();
    assert!(existence.is_valid(0));
    assert_eq!(existence.value(0), 1);

    // Structural hooks list contains both PDB and AlphaFoldDB
    let structures = batch
        .column(idx("structures"))
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let struct_vals = structures.value(0);
    let struct_arr = struct_vals.as_any().downcast_ref::<StructArray>().unwrap();
    let dbs = struct_arr
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let ids_col = struct_arr
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let mut saw_pdb = false;
    let mut saw_af = false;
    for i in 0..struct_arr.len() {
        let db = dbs.value(i);
        let sid = ids_col.value(i);
        if db == "PDB" && sid == "1TUP" {
            saw_pdb = true;
        }
        if db == "AlphaFoldDB" && sid == "AF-P04637-F1" {
            saw_af = true;
        }
    }
    assert!(saw_pdb, "missing PDB structure 1TUP");
    assert!(saw_af, "missing AlphaFoldDB structure AF-P04637-F1");

    Ok(())
}
