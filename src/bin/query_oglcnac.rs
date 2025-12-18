use anyhow::{anyhow, Result};
use arrow::array::{Array, ListArray, RecordBatch, StringArray, StructArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;

fn main() -> Result<()> {
    let path = PathBuf::from("data/parquet/uniprot_human_super_substrate.parquet");
    if !path.exists() {
        return Err(anyhow!("Parquet file not found at {:?}", path));
    }

    println!("Querying O-GlcNAc sites from {:?}\n", path);

    let file = File::open(&path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut evidence_counts: HashMap<String, usize> = HashMap::new();
    let mut total_oglcnac_sites = 0;

    for maybe_batch in reader {
        let batch: RecordBatch = maybe_batch?;
        let schema = batch.schema();

        // Find the features column
        let features_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "features")
            .ok_or_else(|| anyhow!("features column not found"))?;

        let features_column = batch.column(features_idx);
        let features_list = features_column
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| anyhow!("features column is not a ListArray"))?;

        // Iterate through each row (protein entry)
        for row_idx in 0..batch.num_rows() {
            if features_list.is_null(row_idx) {
                continue;
            }

            let feature_array = features_list.value(row_idx);
            let feature_struct = feature_array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| anyhow!("feature array is not a StructArray"))?;

            // Get the feature_type and evidence_code columns from the struct
            let feature_types = feature_struct
                .column_by_name("feature_type")
                .ok_or_else(|| anyhow!("feature_type column not found"))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("feature_type is not a StringArray"))?;

            let evidence_codes = feature_struct
                .column_by_name("evidence_code")
                .ok_or_else(|| anyhow!("evidence_code column not found"))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("evidence_code is not a StringArray"))?;

            // Check each feature in this protein entry
            for feature_idx in 0..feature_types.len() {
                if feature_types.is_null(feature_idx) {
                    continue;
                }

                let feature_type = feature_types.value(feature_idx);

                // Look for glycosylation modifications, particularly O-GlcNAc
                // Common variations: "glycosylation site", "modified residue", etc.
                if feature_type.to_lowercase().contains("glyc")
                    || feature_type == "modified residue"
                {
                    // Check if description contains O-GlcNAc
                    let descriptions = feature_struct
                        .column_by_name("description")
                        .ok_or_else(|| anyhow!("description column not found"))?
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| anyhow!("description is not a StringArray"))?;

                    if !descriptions.is_null(feature_idx) {
                        let description = descriptions.value(feature_idx);
                        
                        // Look for O-GlcNAc or O-linked N-acetylglucosamine
                        if description.to_lowercase().contains("o-glcnac")
                            || description.to_lowercase().contains("n-acetylglucosamine")
                            || description.to_lowercase().contains("glcnac")
                        {
                            total_oglcnac_sites += 1;

                            // Get evidence code
                            let evidence = if evidence_codes.is_null(feature_idx) {
                                "Unknown".to_string()
                            } else {
                                evidence_codes.value(feature_idx).to_string()
                            };

                            *evidence_counts.entry(evidence).or_insert(0) += 1;
                        }
                    }
                }
            }
        }
    }

    println!("═══════════════════════════════════════");
    println!("O-GlcNAc Site Statistics");
    println!("═══════════════════════════════════════");
    println!("Total O-GlcNAc sites found: {}\n", total_oglcnac_sites);

    if total_oglcnac_sites == 0 {
        println!("No O-GlcNAc sites found in the dataset.");
        return Ok(());
    }

    println!("Evidence Level Breakdown:");
    println!("─────────────────────────────────────");

    // Categorize evidence codes
    let mut experimental = 0;
    let mut non_experimental = 0;
    let mut unknown = 0;

    // Sort evidence codes by count
    let mut sorted_evidence: Vec<_> = evidence_counts.iter().collect();
    sorted_evidence.sort_by(|a, b| b.1.cmp(a.1));

    for (evidence, count) in &sorted_evidence {
        let percentage = (**count as f64 / total_oglcnac_sites as f64) * 100.0;
        println!("{:30} {:6} ({:5.2}%)", evidence, count, percentage);

        // Classify evidence types based on ECO codes
        // ECO evidence codes: https://www.evidenceontology.org/
        // ECO:0000269 = Experimental evidence used in manual assertion
        // ECO:0007744 = Combinatorial evidence (high-throughput)
        // ECO:0000255 = Sequence similarity evidence
        // ECO:0000250 = Sequence orthology evidence
        // ECO:0000305 = Curator inference
        // ECO:0000312 = Imported information
        // ECO:0000303 = Non-traceable author statement
        
        let evidence_lower = evidence.to_lowercase();
        
        // If it contains ECO:0000269 or ECO:0007744, it has experimental evidence
        if evidence_lower.contains("eco:0000269") || evidence_lower.contains("eco:0007744") {
            experimental += *count;
        } else if evidence.as_str() == "Unknown" {
            unknown += *count;
        } else {
            // All other codes: ECO:0000255 (similarity), ECO:0000250 (orthology),
            // ECO:0000305 (curator inference), ECO:0000312 (imported), etc.
            non_experimental += *count;
        }
    }

    println!("─────────────────────────────────────");
    println!("\nSummary by Evidence Category:");
    println!("─────────────────────────────────────");
    println!(
        "Experimental:       {:6} ({:5.2}%)",
        experimental,
        (experimental as f64 / total_oglcnac_sites as f64) * 100.0
    );
    println!(
        "Non-Experimental:   {:6} ({:5.2}%)",
        non_experimental,
        (non_experimental as f64 / total_oglcnac_sites as f64) * 100.0
    );
    println!(
        "Unknown:            {:6} ({:5.2}%)",
        unknown,
        (unknown as f64 / total_oglcnac_sites as f64) * 100.0
    );
    println!("═══════════════════════════════════════");

    Ok(())
}
