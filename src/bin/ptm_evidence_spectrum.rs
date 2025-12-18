use anyhow::{anyhow, Result};
use arrow::array::{Array, ListArray, RecordBatch, StringArray, StructArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;

#[derive(Debug)]
struct PtmStats {
    total: usize,
    experimental: usize,
    non_experimental: usize,
    unknown: usize,
}

impl PtmStats {
    fn new() -> Self {
        Self {
            total: 0,
            experimental: 0,
            non_experimental: 0,
            unknown: 0,
        }
    }

    fn experimental_ratio(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            self.experimental as f64 / self.total as f64
        }
    }
}

fn main() -> Result<()> {
    let path = PathBuf::from("data/parquet/uniprot_human_super_substrate.parquet");
    if !path.exists() {
        return Err(anyhow!("Parquet file not found at {:?}", path));
    }

    println!("Analyzing PTM evidence spectrum across modification types\n");

    let file = File::open(&path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut ptm_stats: HashMap<String, PtmStats> = HashMap::new();

    for maybe_batch in reader {
        let batch: RecordBatch = maybe_batch?;
        let schema = batch.schema();

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

        for row_idx in 0..batch.num_rows() {
            if features_list.is_null(row_idx) {
                continue;
            }

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

            let evidence_codes = feature_struct
                .column_by_name("evidence_code")
                .ok_or_else(|| anyhow!("evidence_code column not found"))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("evidence_code is not a StringArray"))?;

            for feature_idx in 0..feature_types.len() {
                if feature_types.is_null(feature_idx) {
                    continue;
                }

                let feature_type = feature_types.value(feature_idx);

                // Focus on modification types
                if feature_type == "modified residue" 
                    || feature_type == "lipidation"
                    || feature_type == "glycosylation site"
                    || feature_type == "cross-link"
                    || feature_type.contains("modification")
                {
                    if descriptions.is_null(feature_idx) {
                        continue;
                    }

                    let description = descriptions.value(feature_idx);
                    
                    // Extract the modification type from description
                    let mod_type = extract_modification_type(description);
                    
                    let stats = ptm_stats.entry(mod_type).or_insert_with(PtmStats::new);
                    stats.total += 1;

                    // Classify evidence
                    let evidence = if evidence_codes.is_null(feature_idx) {
                        "Unknown".to_string()
                    } else {
                        evidence_codes.value(feature_idx).to_string()
                    };

                    let evidence_lower = evidence.to_lowercase();
                    if evidence_lower.contains("eco:0000269") || evidence_lower.contains("eco:0007744") {
                        stats.experimental += 1;
                    } else if evidence == "Unknown" {
                        stats.unknown += 1;
                    } else {
                        stats.non_experimental += 1;
                    }
                }
            }
        }
    }

    // Filter out PTMs with too few sites (< 100) for statistical relevance
    let mut filtered_stats: Vec<(String, PtmStats)> = ptm_stats
        .into_iter()
        .filter(|(_, stats)| stats.total >= 100)
        .collect();

    // Sort by experimental ratio (descending)
    filtered_stats.sort_by(|a, b| {
        b.1.experimental_ratio()
            .partial_cmp(&a.1.experimental_ratio())
            .unwrap()
    });

    println!("═══════════════════════════════════════════════════════════════════════");
    println!("                    PTM Evidence Spectrum: Yin & Yang");
    println!("═══════════════════════════════════════════════════════════════════════");
    println!("Showing PTMs with ≥100 sites, ranked by experimental evidence ratio\n");

    // Yang (Light) - High experimental evidence
    println!("☯ YANG (Light) - Highly Experimentally Validated PTMs ☯");
    println!("───────────────────────────────────────────────────────────────────────");
    println!("{:35} {:>8} {:>10} {:>10}", "Modification Type", "Total", "Exptl %", "Non-Exp %");
    println!("───────────────────────────────────────────────────────────────────────");
    
    let top_experimental = filtered_stats.iter().take(10);
    for (mod_type, stats) in top_experimental {
        let exp_pct = stats.experimental_ratio() * 100.0;
        let non_exp_pct = (stats.non_experimental as f64 / stats.total as f64) * 100.0;
        println!(
            "{:35} {:>8} {:>9.1}% {:>9.1}%",
            truncate_string(mod_type, 35),
            stats.total,
            exp_pct,
            non_exp_pct
        );
    }

    println!("\n                              ⚖  BALANCE  ⚖\n");

    // Yin (Dark) - Low experimental evidence
    println!("☯ YIN (Dark) - Computationally Predicted PTMs ☯");
    println!("───────────────────────────────────────────────────────────────────────");
    println!("{:35} {:>8} {:>10} {:>10}", "Modification Type", "Total", "Exptl %", "Non-Exp %");
    println!("───────────────────────────────────────────────────────────────────────");
    
    let bottom_experimental = filtered_stats.iter().rev().take(10).rev();
    for (mod_type, stats) in bottom_experimental {
        let exp_pct = stats.experimental_ratio() * 100.0;
        let non_exp_pct = (stats.non_experimental as f64 / stats.total as f64) * 100.0;
        println!(
            "{:35} {:>8} {:>9.1}% {:>9.1}%",
            truncate_string(mod_type, 35),
            stats.total,
            exp_pct,
            non_exp_pct
        );
    }

    println!("═══════════════════════════════════════════════════════════════════════");
    
    // Summary statistics
    let total_sites: usize = filtered_stats.iter().map(|(_, s)| s.total).sum();
    let total_experimental: usize = filtered_stats.iter().map(|(_, s)| s.experimental).sum();
    let overall_exp_ratio = total_experimental as f64 / total_sites as f64 * 100.0;
    
    println!("\nOverall Statistics:");
    println!("  Total PTM sites analyzed: {}", total_sites);
    println!("  Total experimentally validated: {} ({:.1}%)", total_experimental, overall_exp_ratio);
    println!("  Number of PTM types (≥100 sites): {}", filtered_stats.len());

    Ok(())
}

fn extract_modification_type(description: &str) -> String {
    let desc_lower = description.to_lowercase();
    
    // Common PTM patterns
    if desc_lower.contains("phospho") {
        if desc_lower.contains("serine") {
            "Phosphoserine".to_string()
        } else if desc_lower.contains("threonine") {
            "Phosphothreonine".to_string()
        } else if desc_lower.contains("tyrosine") {
            "Phosphotyrosine".to_string()
        } else {
            "Phosphorylation".to_string()
        }
    } else if desc_lower.contains("acetyl") {
        "Acetylation".to_string()
    } else if desc_lower.contains("methyl") {
        "Methylation".to_string()
    } else if desc_lower.contains("ubiquit") {
        "Ubiquitination".to_string()
    } else if desc_lower.contains("sumoyl") {
        "Sumoylation".to_string()
    } else if desc_lower.contains("glcnac") || desc_lower.contains("n-acetylglucosamine") {
        "O-GlcNAc".to_string()
    } else if desc_lower.contains("palmitoyl") {
        "Palmitoylation".to_string()
    } else if desc_lower.contains("myristoyl") {
        "Myristoylation".to_string()
    } else if desc_lower.contains("hydroxyl") {
        "Hydroxylation".to_string()
    } else if desc_lower.contains("glycat") || desc_lower.contains("glycosyl") {
        "Glycosylation".to_string()
    } else if desc_lower.contains("nitro") {
        "Nitration".to_string()
    } else if desc_lower.contains("carboxyl") {
        "Carboxylation".to_string()
    } else if desc_lower.contains("oxidation") {
        "Oxidation".to_string()
    } else {
        // Extract first word or use full description if short
        let first_word = description.split_whitespace().next().unwrap_or(description);
        if first_word.len() <= 30 {
            first_word.to_string()
        } else {
            description[..30.min(description.len())].to_string()
        }
    }
}

fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}
