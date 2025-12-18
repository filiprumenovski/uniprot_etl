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
        // Rich metadata columns
        Field::new("entry_name", DataType::Utf8, true),
        Field::new("gene_name", DataType::Utf8, true),
        Field::new("protein_name", DataType::Utf8, true),
        Field::new("organism_name", DataType::Utf8, true),
        Field::new("existence", DataType::Int8, true),
        Field::new("structures", structures_list_type(), true),
        // Super-Substrate columns
        Field::new("parent_id", DataType::Utf8, false),
        Field::new("ptm_sites", ptm_sites_list_type(), true),
        // ====================================================================
        // 8 New Enriched Feature Columns (Category A & B)
        // ====================================================================
        // Category A: Coordinate-Based Features
        Field::new("active_sites", active_sites_list_type(), true),
        Field::new("binding_sites", binding_sites_list_type(), true),
        Field::new("metal_coordinations", metal_coordinations_list_type(), true),
        Field::new("mutagenesis_sites", mutagenesis_sites_list_type(), true),
        Field::new("domains", domains_list_type(), true),
        // Category B: Sequence Variants (also coordinate-based)
        Field::new("natural_variants", natural_variants_list_type(), true),
        // Category B: Text-Based Comment Features
        Field::new("subunits", subunits_list_type(), true),
        Field::new("interactions", interactions_list_type(), true),
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

/// PTM sites: List<Struct<site_index, site_aa, modifications>>
fn ptm_sites_list_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(ptm_site_struct_fields()),
        true,
    )))
}

fn ptm_site_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("site_index", DataType::Int32, false),
        Field::new("site_aa", DataType::Utf8, false),
        Field::new("modifications", ptm_modifications_list_type(), true),
    ])
}

fn ptm_modifications_list_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(ptm_modification_struct_fields()),
        true,
    )))
}

fn ptm_modification_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("mod_type", DataType::Int32, false),
        Field::new("confidence_score", DataType::Float32, false),
    ])
}
// ============================================================================
// Schema Helpers for 8 New Enriched Features
// ============================================================================

/// Active Site struct: id, description, start, end, confidence_score
fn active_sites_list_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(coordinate_feature_struct_fields("active_site")),
        true,
    )))
}

/// Binding Site struct: id, description, start, end, confidence_score
fn binding_sites_list_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(coordinate_feature_struct_fields("binding_site")),
        true,
    )))
}

/// Metal Coordination Site struct: id, description, metal, start, end, confidence_score
fn metal_coordinations_list_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(metal_coordination_struct_fields()),
        true,
    )))
}

fn metal_coordination_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("metal", DataType::Utf8, true),
        Field::new("start", DataType::Int32, true),
        Field::new("end", DataType::Int32, true),
        Field::new("evidence_code", DataType::Utf8, true),
        Field::new("confidence_score", DataType::Float32, true),
    ])
}

/// Mutagenesis Site struct: id, description, start, end, confidence_score
fn mutagenesis_sites_list_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(coordinate_feature_struct_fields("mutagenesis")),
        true,
    )))
}

/// Domain struct: id, description, domain_name, start, end, confidence_score
fn domains_list_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(domain_struct_fields()),
        true,
    )))
}

fn domain_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("domain_name", DataType::Utf8, true),
        Field::new("start", DataType::Int32, true),
        Field::new("end", DataType::Int32, true),
        Field::new("evidence_code", DataType::Utf8, true),
        Field::new("confidence_score", DataType::Float32, true),
    ])
}

/// Natural Variant struct: id, description, original, variation, start, end, confidence_score
fn natural_variants_list_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(natural_variant_struct_fields()),
        true,
    )))
}

fn natural_variant_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("original", DataType::Utf8, true),
        Field::new("variation", DataType::Utf8, true),
        Field::new("start", DataType::Int32, true),
        Field::new("end", DataType::Int32, true),
        Field::new("evidence_code", DataType::Utf8, true),
        Field::new("confidence_score", DataType::Float32, true),
    ])
}

/// Subunit comment struct: text, confidence_score
fn subunits_list_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(subunit_struct_fields()),
        true,
    )))
}

fn subunit_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("text", DataType::Utf8, false),
        Field::new("evidence_code", DataType::Utf8, true),
        Field::new("confidence_score", DataType::Float32, true),
    ])
}

/// Interaction struct: partner_id, interactant_id_1, interactant_id_2, confidence_score
fn interactions_list_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(interaction_struct_fields()),
        true,
    )))
}

fn interaction_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("interactant_id_1", DataType::Utf8, true),
        Field::new("interactant_id_2", DataType::Utf8, true),
        Field::new("evidence_code", DataType::Utf8, true),
        Field::new("confidence_score", DataType::Float32, true),
    ])
}

/// Helper for coordinate-based features with standard fields
fn coordinate_feature_struct_fields(_feature_name: &str) -> Fields {
    Fields::from(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("start", DataType::Int32, true),
        Field::new("end", DataType::Int32, true),
        Field::new("evidence_code", DataType::Utf8, true),
        Field::new("confidence_score", DataType::Float32, true),
    ])
}