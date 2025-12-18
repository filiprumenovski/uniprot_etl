use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{Array, Int32Array, ListArray, StringArray, StructArray};
use crossbeam_channel::unbounded;
use quick_xml::Reader;
use std::collections::HashMap;

use uniprot_etl::error::Result;
use uniprot_etl::metrics::Metrics;
use uniprot_etl::pipeline::parser::parse_entries;

#[test]
fn parses_single_entry_into_record_batch() -> Result<()> {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<uniprot>
    <entry>
        <accession>Q9TEST</accession>
        <sequence length="4">
            M T
            A K
        </sequence>
        <organism>
            <dbReference type="NCBI Taxonomy" id="9606"/>
        </organism>
        <feature type="domain" description="Kinase region" evidence="E1">
            <location>
                <begin position="2"/>
                <end position="3"/>
            </location>
        </feature>
        <comment type="subcellular location">
            <subcellularLocation>
                <location evidence="E1">Membrane</location>
            </subcellularLocation>
        </comment>
        <comment type="alternative products">
            <isoform>
                <id>ISO1</id>
                <sequence ref="Q9TEST-1"></sequence>
                <note>Primary isoform</note>
            </isoform>
        </comment>
        <evidence key="E1" type="ECO:0000255"/>
    </entry>
</uniprot>
"#;

    let mut reader = Reader::from_reader(Cursor::new(xml.as_bytes()));
    reader.config_mut().trim_text(true);

    let metrics = Metrics::new();
    let (tx, rx) = unbounded();

    let mut sidecar = HashMap::new();
    sidecar.insert("Q9TEST-1".to_string(), "MTAK".to_string());
    parse_entries(reader, tx, &metrics, 16, Some(Arc::new(sidecar)))?;

    let batches: Vec<_> = rx.iter().collect();
    assert_eq!(batches.len(), 1);

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ids.value(0), "Q9TEST-1");

    let sequences = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(sequences.value(0), "MTAK");

    // parent_id appended (schema extension)
    let schema = batch.schema();
    let parent_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == "parent_id")
        .expect("parent_id");
    let parents = batch
        .column(parent_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(parents.value(0), "Q9TEST");

    let organisms = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert!(organisms.is_valid(0));
    assert_eq!(organisms.value(0), 9606);

    let isoforms = batch
        .column(3)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(isoforms.value_length(0), 1);
    let isoform_values = isoforms.value(0);
    let isoform_struct = isoform_values
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let isoform_ids = isoform_struct
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(isoform_ids.value(0), "ISO1");
    let isoform_seq = isoform_struct
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(isoform_seq.value(0), "Q9TEST-1");
    let isoform_note = isoform_struct
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(isoform_note.value(0), "Primary isoform");

    let features = batch
        .column(4)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(features.value_length(0), 1);
    let feature_values = features.value(0);
    let feature_struct = feature_values
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let feature_types = feature_struct
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(feature_types.value(0), "domain");
    let feature_desc = feature_struct
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(feature_desc.value(0), "Kinase region");
    let feature_starts = feature_struct
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(feature_starts.value(0), 2);
    let feature_ends = feature_struct
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(feature_ends.value(0), 3);
    let feature_evidence = feature_struct
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(feature_evidence.value(0), "ECO:0000255");

    let locations = batch
        .column(5)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(locations.value_length(0), 1);
    let location_values = locations.value(0);
    let location_struct = location_values
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let location_names = location_struct
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(location_names.value(0), "Membrane");
    let location_evidence = location_struct
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(location_evidence.value(0), "ECO:0000255");

    assert_eq!(metrics.entries(), 1);
    assert_eq!(metrics.batches(), 1);

    Ok(())
}

#[test]
fn captures_subunit_comment_text_into_subunits_column() -> Result<()> {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<uniprot>
    <entry>
        <accession>Q9SUB</accession>
        <sequence length="4">MTAK</sequence>
        <comment type="subunit">
            <text evidence="E1">Homodimer.</text>
        </comment>
        <comment type="alternative products">
            <isoform>
                <id>ISO1</id>
                <sequence ref="Q9SUB-1"></sequence>
            </isoform>
        </comment>
        <evidence key="E1" type="ECO:0000269"/>
    </entry>
</uniprot>
"#;

    let mut reader = Reader::from_reader(Cursor::new(xml.as_bytes()));
    reader.config_mut().trim_text(true);

    let metrics = Metrics::new();
    let (tx, rx) = unbounded();

    let mut sidecar = HashMap::new();
    sidecar.insert("Q9SUB-1".to_string(), "MTAK".to_string());
    parse_entries(reader, tx, &metrics, 16, Some(Arc::new(sidecar)))?;

    let batches: Vec<_> = rx.iter().collect();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let schema = batch.schema();
    let subunits_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == "subunits")
        .expect("subunits");

    let subunits = batch
        .column(subunits_idx)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(subunits.value_length(0), 1);

    let subunit_values = subunits.value(0);
    let subunit_struct = subunit_values
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let texts = subunit_struct
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(texts.value(0), "Homodimer.");

    let evidence_codes = subunit_struct
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(evidence_codes.value(0), "ECO:0000269");

    Ok(())
}

#[test]
fn parses_multiple_entries_and_handles_missing_evidence() -> Result<()> {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<uniprot>
    <entry>
        <accession>Q1</accession>
        <sequence length="3">AAA</sequence>
        <organism>
            <dbReference type="NCBI Taxonomy" id="9606"/>
        </organism>
        <feature type="domain" evidence="E1">
            <location>
                <position position="1"/>
            </location>
        </feature>
        <evidence key="E1" type="ECO:0000255"/>
    </entry>
    <entry>
        <accession>Q2</accession>
        <sequence length="2">BB</sequence>
        <feature type="region">
            <location>
                <begin position="1"/>
                <end position="2"/>
            </location>
        </feature>
    </entry>
</uniprot>
"#;

    let mut reader = Reader::from_reader(Cursor::new(xml.as_bytes()));
    reader.config_mut().trim_text(true);

    let metrics = Metrics::new();
    let (tx, rx) = unbounded();

    parse_entries(reader, tx, &metrics, 16, None)?;

    let batches: Vec<_> = rx.iter().collect();
    assert_eq!(batches.len(), 1);

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);

    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ids.value(0), "Q1");
    assert_eq!(ids.value(1), "Q2");

    let organisms = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert!(organisms.is_valid(0));
    assert!(organisms.is_null(1));

    let features = batch
        .column(4)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    // Entry 0: has evidence code
    assert_eq!(features.value_length(0), 1);
    let feature_struct_0 = features.value(0);
    let feature_struct_0 = feature_struct_0
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let evidence_col_0 = feature_struct_0
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(evidence_col_0.value(0), "ECO:0000255");

    // Entry 1: no evidence attribute should yield null evidence_code
    assert_eq!(features.value_length(1), 1);
    let feature_struct_1 = features.value(1);
    let feature_struct_1 = feature_struct_1
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let evidence_col_1 = feature_struct_1
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(evidence_col_1.is_null(0));

    assert_eq!(metrics.entries(), 2);
    assert_eq!(metrics.batches(), 1);

    Ok(())
}
