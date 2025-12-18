use arrow::datatypes::{DataType, Field, Fields, Schema};
use std::sync::Arc;

/// Creates the Arrow schema for UniProt entries.
///
/// Top-level columns:
/// - id: Utf8 (primary accession)
/// - sequence: Utf8 (amino acid string)
/// - organism_id: Int32 (NCBI TaxID)
///
/// Nested columns:
/// - isoforms: List<Struct>
/// - features: List<Struct>
/// - location: List<Struct>
pub fn create_uniprot_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("sequence", DataType::Utf8, false),
        Field::new("organism_id", DataType::Int32, true),
        Field::new("isoforms", isoforms_list_type(), true),
        Field::new("features", features_list_type(), true),
        Field::new("location", location_list_type(), true),
        // New rich metadata columns (appended to preserve indices of existing tests)
        Field::new("entry_name", DataType::Utf8, true),
        Field::new("gene_name", DataType::Utf8, true),
        Field::new("protein_name", DataType::Utf8, true),
        Field::new("organism_name", DataType::Utf8, true),
        Field::new("existence", DataType::Int8, true),
        Field::new("structures", structures_list_type(), true),
    ])
}

/// Returns the Arc<Schema> for use with Arrow writers
pub fn schema_ref() -> Arc<Schema> {
    Arc::new(create_uniprot_schema())
}

/// Isoform struct: isoform_id, isoform_sequence, isoform_note
fn isoform_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("isoform_id", DataType::Utf8, false),
        Field::new("isoform_sequence", DataType::Utf8, true),
        Field::new("isoform_note", DataType::Utf8, true),
    ])
}

fn isoforms_list_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(isoform_struct_fields()),
        true,
    )))
}

/// Feature struct: feature_type, description, start, end, evidence_code
fn feature_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("feature_type", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("start", DataType::Int32, true),
        Field::new("end", DataType::Int32, true),
        Field::new("evidence_code", DataType::Utf8, true),
    ])
}

fn features_list_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(feature_struct_fields()),
        true,
    )))
}

/// Location struct: location, evidence_code
fn location_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("location", DataType::Utf8, false),
        Field::new("evidence_code", DataType::Utf8, true),
    ])
}

fn location_list_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(location_struct_fields()),
        true,
    )))
}

/// Structure struct: db, id
fn structure_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("db", DataType::Utf8, false),
        Field::new("id", DataType::Utf8, false),
    ])
}

fn structures_list_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(structure_struct_fields()),
        true,
    )))
}
