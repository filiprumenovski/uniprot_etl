use anyhow::{anyhow, Result};
use arrow::array::{Array, Int32Array, ListArray, RecordBatch, StringArray, StructArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::PathBuf;

#[derive(Debug, Clone)]
struct SiteInfo {
    position: i32,
    amino_acid: String,
    ptm_type: String,
    evidence: String,
}

fn main() -> Result<()> {
    let path = PathBuf::from("data/parquet/uniprot_human_super_substrate.parquet");
    if !path.exists() {
        return Err(anyhow!("Parquet file not found at {:?}", path));
    }

    println!("🔄 Analyzing Yin-Yang Relationship: Phosphorylation ⚡ vs O-GlcNAc 🍬\n");

    let file = File::open(&path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut proteins_with_both = 0;
    let mut proteins_with_phospho_only = 0;
    let mut proteins_with_oglcnac_only = 0;
    
    let mut total_phospho_sites = 0;
    let mut total_oglcnac_sites = 0;
    let mut overlapping_sites = 0;
    let mut proximal_sites = 0; // Within 5 residues
    
    let mut phospho_evidence: HashMap<String, usize> = HashMap::new();
    let mut oglcnac_evidence: HashMap<String, usize> = HashMap::new();
    let mut overlap_examples: Vec<(String, i32, String)> = Vec::new();

    for maybe_batch in reader {
        let batch: RecordBatch = maybe_batch?;
        let schema = batch.schema();

        let id_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "id")
            .ok_or_else(|| anyhow!("id column not found"))?;

        let features_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "features")
            .ok_or_else(|| anyhow!("features column not found"))?;

        let ids = batch
            .column(id_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow!("id column is not a StringArray"))?;

        let features_column = batch.column(features_idx);
        let features_list = features_column
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| anyhow!("features column is not a ListArray"))?;

        for row_idx in 0..batch.num_rows() {
            if features_list.is_null(row_idx) {
                continue;
            }

            let protein_id = ids.value(row_idx);
            let mut phospho_sites: HashMap<i32, SiteInfo> = HashMap::new();
            let mut oglcnac_sites: HashMap<i32, SiteInfo> = HashMap::new();

            let feature_array = features_list.value(row_idx);
            let feature_struct = feature_array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| anyhow!("feature array is not a StructArray"))?;

            let feature_types = feature_struct
                .column_by_name("feature_type")
                .ok_or_else(|| anyhow!("feature_type column not found"))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("feature_type is not a StringArray"))?;

            let descriptions = feature_struct
                .column_by_name("description")
                .ok_or_else(|| anyhow!("description column not found"))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("description is not a StringArray"))?;

            let starts = feature_struct
                .column_by_name("start")
                .ok_or_else(|| anyhow!("start column not found"))?
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| anyhow!("start is not an Int32Array"))?;

            let evidence_codes = feature_struct
                .column_by_name("evidence_code")
                .ok_or_else(|| anyhow!("evidence_code column not found"))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("evidence_code is not a StringArray"))?;

            // Collect all phosphorylation and O-GlcNAc sites for this protein
            for feature_idx in 0..feature_types.len() {
                if feature_types.is_null(feature_idx) || descriptions.is_null(feature_idx) {
                    continue;
                }

                let feature_type = feature_types.value(feature_idx);
                let description = descriptions.value(feature_idx);
                let desc_lower = description.to_lowercase();

                let evidence = if evidence_codes.is_null(feature_idx) {
                    "Unknown".to_string()
                } else {
                    evidence_codes.value(feature_idx).to_string()
                };

                // Extract position if available
                let position = if !starts.is_null(feature_idx) {
                    starts.value(feature_idx)
                } else {
                    continue;
                };

                // Check for phosphorylation
                if feature_type == "modified residue" && 
                   (desc_lower.contains("phospho") || 
                    desc_lower.contains("phosphorylated")) {
                    phospho_sites.insert(position, SiteInfo {
                        position,
                        amino_acid: extract_amino_acid(&desc_lower),
                        ptm_type: "Phosphorylation".to_string(),
                        evidence: evidence.clone(),
                    });
                }

                // Check for O-GlcNAc
                if (feature_type.to_lowercase().contains("glyc") || feature_type == "modified residue") &&
                   (desc_lower.contains("o-glcnac") || 
                    desc_lower.contains("n-acetylglucosamine") ||
                    desc_lower.contains("glcnac")) {
                    oglcnac_sites.insert(position, SiteInfo {
                        position,
                        amino_acid: extract_amino_acid(&desc_lower),
                        ptm_type: "O-GlcNAc".to_string(),
                        evidence: evidence.clone(),
                    });
                }
            }

            // Analyze this protein's sites
            let has_phospho = !phospho_sites.is_empty();
            let has_oglcnac = !oglcnac_sites.is_empty();

            if has_phospho && has_oglcnac {
                proteins_with_both += 1;
            } else if has_phospho {
                proteins_with_phospho_only += 1;
            } else if has_oglcnac {
                proteins_with_oglcnac_only += 1;
            }

            // Count total sites and analyze overlaps
            total_phospho_sites += phospho_sites.len();
            total_oglcnac_sites += oglcnac_sites.len();

            // Track evidence
            for site in phospho_sites.values() {
                *phospho_evidence.entry(site.evidence.clone()).or_insert(0) += 1;
            }
            for site in oglcnac_sites.values() {
                *oglcnac_evidence.entry(site.evidence.clone()).or_insert(0) += 1;
            }

            // Check for exact overlaps and proximal sites
            for (pos_p, info_p) in &phospho_sites {
                if oglcnac_sites.contains_key(pos_p) {
                    overlapping_sites += 1;
                    if overlap_examples.len() < 10 {
                        overlap_examples.push((
                            protein_id.to_string(),
                            *pos_p,
                            info_p.amino_acid.clone(),
                        ));
                    }
                }

                // Check for proximal sites (within 5 residues)
                for pos_o in oglcnac_sites.keys() {
                    if pos_p != pos_o && (pos_p - pos_o).abs() <= 5 {
                        proximal_sites += 1;
                        break;
                    }
                }
            }
        }
    }

    // Print results
    println!("═══════════════════════════════════════════════════════════");
    println!("                  YIN-YANG ANALYSIS RESULTS                ");
    println!("═══════════════════════════════════════════════════════════\n");

    println!("📊 Protein Distribution:");
    println!("─────────────────────────────────────────────────────────");
    let total_proteins = proteins_with_both + proteins_with_phospho_only + proteins_with_oglcnac_only;
    println!("  Both ⚡ & 🍬:           {:6} ({:5.2}%)", 
        proteins_with_both,
        (proteins_with_both as f64 / total_proteins as f64) * 100.0);
    println!("  Phospho only ⚡:        {:6} ({:5.2}%)", 
        proteins_with_phospho_only,
        (proteins_with_phospho_only as f64 / total_proteins as f64) * 100.0);
    println!("  O-GlcNAc only 🍬:       {:6} ({:5.2}%)", 
        proteins_with_oglcnac_only,
        (proteins_with_oglcnac_only as f64 / total_proteins as f64) * 100.0);
    println!("  TOTAL:                 {:6}\n", total_proteins);

    println!("🎯 Site-Level Analysis:");
    println!("─────────────────────────────────────────────────────────");
    println!("  Total phospho sites ⚡:        {:8}", total_phospho_sites);
    println!("  Total O-GlcNAc sites 🍬:       {:8}", total_oglcnac_sites);
    println!("  Exact overlaps (same pos):     {:8} ({:5.2}%)", 
        overlapping_sites,
        (overlapping_sites as f64 / total_phospho_sites.min(total_oglcnac_sites) as f64) * 100.0);
    println!("  Proximal (±5 residues):        {:8}\n", proximal_sites);

    if !overlap_examples.is_empty() {
        println!("🔍 Example Overlapping Sites:");
        println!("─────────────────────────────────────────────────────────");
        for (protein_id, pos, aa) in overlap_examples.iter().take(10) {
            println!("  {} at position {} ({})", protein_id, pos, aa);
        }
        println!();
    }

    // Evidence comparison
    println!("⚖️  Evidence Level Comparison:");
    println!("─────────────────────────────────────────────────────────");
    
    let phospho_experimental = count_experimental(&phospho_evidence);
    let oglcnac_experimental = count_experimental(&oglcnac_evidence);
    
    println!("\n  Phosphorylation ⚡:");
    println!("    Experimental:       {:8} ({:5.2}%)", 
        phospho_experimental,
        (phospho_experimental as f64 / total_phospho_sites as f64) * 100.0);
    println!("    Non-Experimental:   {:8} ({:5.2}%)", 
        total_phospho_sites - phospho_experimental,
        ((total_phospho_sites - phospho_experimental) as f64 / total_phospho_sites as f64) * 100.0);

    println!("\n  O-GlcNAc 🍬:");
    println!("    Experimental:       {:8} ({:5.2}%)", 
        oglcnac_experimental,
        (oglcnac_experimental as f64 / total_oglcnac_sites as f64) * 100.0);
    println!("    Non-Experimental:   {:8} ({:5.2}%)", 
        total_oglcnac_sites - oglcnac_experimental,
        ((total_oglcnac_sites - oglcnac_experimental) as f64 / total_oglcnac_sites as f64) * 100.0);

    println!("\n═══════════════════════════════════════════════════════════");
    println!("🧘 Yin-Yang Balance: {} proteins show co-occurrence", proteins_with_both);
    println!("═══════════════════════════════════════════════════════════");

    Ok(())
}

fn extract_amino_acid(description: &str) -> String {
    // Try to extract amino acid from description (e.g., "Phosphoserine" -> "Ser")
    if description.contains("serine") || description.contains("ser") {
        "Ser".to_string()
    } else if description.contains("threonine") || description.contains("thr") {
        "Thr".to_string()
    } else if description.contains("tyrosine") || description.contains("tyr") {
        "Tyr".to_string()
    } else {
        "Unknown".to_string()
    }
}

fn count_experimental(evidence_map: &HashMap<String, usize>) -> usize {
    evidence_map
        .iter()
        .filter(|(evidence, _)| {
            let evidence_lower = evidence.to_lowercase();
            evidence_lower.contains("eco:0000269") || evidence_lower.contains("eco:0007744")
        })
        .map(|(_, count)| count)
        .sum()
}
