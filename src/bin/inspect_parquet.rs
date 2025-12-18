use anyhow::{anyhow, Result};
use arrow::array::RecordBatchReader;
use arrow::array::{Array, Int8Array, ListArray, StringArray, StructArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::path::PathBuf;

fn main() -> Result<()> {
    let path = PathBuf::from("data/parquet/uniprot.parquet");
    if !path.exists() {
        return Err(anyhow!("Parquet file not found at {:?}", path));
    }

    let file = File::open(&path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    println!("Schema: {:?}", reader.schema());

    for maybe_batch in reader {
        let batch: RecordBatch = maybe_batch?;
        // Locate columns by name
        let schema = batch.schema();
        let id_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "id")
            .ok_or_else(|| anyhow!("id column not found"))?;
        let gene_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "gene_name")
            .ok_or_else(|| anyhow!("gene_name column not found"))?;
        let protein_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "protein_name")
            .ok_or_else(|| anyhow!("protein_name column not found"))?;
        let organism_name_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "organism_name")
            .ok_or_else(|| anyhow!("organism_name column not found"))?;
        let existence_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "existence")
            .ok_or_else(|| anyhow!("existence column not found"))?;
        let structures_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "structures")
            .ok_or_else(|| anyhow!("structures column not found"))?;

        let ids = batch
            .column(id_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let genes = batch
            .column(gene_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let proteins = batch
            .column(protein_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let org_names = batch
            .column(organism_name_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let existence = batch
            .column(existence_idx)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        let structures = batch
            .column(structures_idx)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        for i in 0..batch.num_rows() {
            if ids.value(i) == "P04637" || ids.value(i) == "TP53" {
                // accession is P04637
                println!("Found P04637 at row {}", i);
                println!(
                    "  gene_name: {:?}",
                    if genes.is_valid(i) {
                        Some(genes.value(i))
                    } else {
                        None
                    }
                );
                println!(
                    "  protein_name: {:?}",
                    if proteins.is_valid(i) {
                        Some(proteins.value(i))
                    } else {
                        None
                    }
                );
                println!(
                    "  organism_name: {:?}",
                    if org_names.is_valid(i) {
                        Some(org_names.value(i))
                    } else {
                        None
                    }
                );
                println!(
                    "  existence: {:?}",
                    if existence.is_valid(i) {
                        Some(existence.value(i))
                    } else {
                        None
                    }
                );
                let struct_vals = structures.value(i);
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
                for j in 0..struct_arr.len() {
                    let db = dbs.value(j);
                    let sid = ids_col.value(j);
                    println!("  structure: {}:{}", db, sid);
                }
                return Ok(());
            }
        }
    }

    println!("P04637 not found in first pass; try other filters.");
    Ok(())
}
