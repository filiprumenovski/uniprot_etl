pub mod common;
pub mod ptm;

use std::sync::Arc;

use arrow::array::{
    ArrayBuilder, ArrayRef, Float32Builder, Int32Builder, Int8Builder, ListBuilder, StringBuilder, StructBuilder,
};
use arrow::datatypes::{DataType, Field, Fields};
use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::metrics::Metrics;
use crate::pipeline::builders::common::FeatureListBuilder;
use crate::pipeline::builders::ptm::append_ptm_sites;
use crate::pipeline::scratch::ParsedEntry;
use crate::pipeline::transformer::TransformedRow;
use crate::schema::schema_ref;

pub struct EntryBuilders {
    pub id: StringBuilder,
    pub sequence: StringBuilder,
    pub organism_id: Int32Builder,
    pub isoforms: ListBuilder<StructBuilder>,
    pub features: ListBuilder<StructBuilder>,
    pub locations: ListBuilder<StructBuilder>,
    pub entry_name: StringBuilder,
    pub gene_name: StringBuilder,
    pub protein_name: StringBuilder,
    pub organism_name: StringBuilder,
    pub existence: Int8Builder,
    pub structures: ListBuilder<StructBuilder>,
    pub parent_id: StringBuilder,
    pub ptm_sites: ListBuilder<StructBuilder>,
    pub active_sites: FeatureListBuilder,
    pub binding_sites: FeatureListBuilder,
    pub metal_coordinations: FeatureListBuilder,
    pub mutagenesis_sites: FeatureListBuilder,
    pub domains: FeatureListBuilder,
    pub natural_variants: FeatureListBuilder,
    pub subunits: ListBuilder<StructBuilder>,
    pub interactions: ListBuilder<StructBuilder>,
    capacity: usize,
    metrics: Metrics,
}

impl EntryBuilders {
    pub fn new(capacity: usize, metrics: Metrics) -> Self {
        Self {
            id: StringBuilder::with_capacity(capacity, capacity * 10),
            sequence: StringBuilder::with_capacity(capacity, capacity * 500),
            organism_id: Int32Builder::with_capacity(capacity),
            isoforms: create_isoforms_builder(capacity),
            features: create_features_builder(capacity),
            locations: create_locations_builder(capacity),
            entry_name: StringBuilder::with_capacity(capacity, capacity * 20),
            gene_name: StringBuilder::with_capacity(capacity, capacity * 20),
            protein_name: StringBuilder::with_capacity(capacity, capacity * 50),
            organism_name: StringBuilder::with_capacity(capacity, capacity * 30),
            existence: Int8Builder::with_capacity(capacity),
            structures: create_structures_builder(capacity),
            parent_id: StringBuilder::with_capacity(capacity, capacity * 10),
            ptm_sites: create_ptm_sites_builder(capacity),
            active_sites: FeatureListBuilder::new(create_coordinate_feature_builder(capacity), 0),
            binding_sites: FeatureListBuilder::new(create_coordinate_feature_builder(capacity), 0),
            metal_coordinations: FeatureListBuilder::new(create_metal_coordination_builder(capacity), 1),
            mutagenesis_sites: FeatureListBuilder::new(create_coordinate_feature_builder(capacity), 0),
            domains: FeatureListBuilder::new(create_domain_builder(capacity), 1),
            natural_variants: FeatureListBuilder::new(create_natural_variant_builder(capacity), 2),
            subunits: create_subunit_builder(capacity),
            interactions: create_interaction_builder(capacity),
            capacity,
            metrics,
        }
    }

    ///
    /// This is used for isoform "explosion": the same entry metadata is replicated,
    /// while row_id, row_sequence, and parent_id vary per row.
    pub fn append_row(&mut self, row: &TransformedRow) {
        let entry: &ParsedEntry = &row.entry;

        self.id.append_value(&row.row_id);
        self.sequence.append_value(&row.sequence);
        self.organism_id.append_option(entry.organism_id);

        self.entry_name.append_option(entry.entry_name.as_deref());
        self.gene_name.append_option(entry.gene_name.as_deref());
        self.protein_name
            .append_option(entry.protein_name.as_deref());
        self.organism_name
            .append_option(entry.organism_scientific_name.as_deref());
        if entry.existence == 0 {
            self.existence.append_null();
        } else {
            self.existence.append_value(entry.existence);
        }

        append_isoforms(&mut self.isoforms, entry);
        append_features(&mut self.features, entry);
        append_locations(&mut self.locations, entry);
        append_structures(&mut self.structures, entry);

        self.parent_id.append_value(&row.parent_id);

        // Coordinate-based features
        self.active_sites.append_features(
            entry,
            &row.sequence,
            &row.mapper,
            entry.features.active_sites.iter(),
            |_, _, _, _| {},
        );
        self.binding_sites.append_features(
            entry,
            &row.sequence,
            &row.mapper,
            entry.features.binding_sites.iter(),
            |_, _, _, _| {},
        );
        self.mutagenesis_sites.append_features(
            entry,
            &row.sequence,
            &row.mapper,
            entry.features.mutagenesis_sites.iter(),
            |_, _, _, _| {},
        );
        self.metal_coordinations.append_features(
            entry,
            &row.sequence,
            &row.mapper,
            entry.features.metal_coordinations.iter(),
            |builder, base, _, feat| {
                builder
                    .field_builder::<StringBuilder>(base)
                    .unwrap()
                    .append_option(feat.metal.as_deref());
            },
        );
        self.domains.append_features(
            entry,
            &row.sequence,
            &row.mapper,
            entry.features.domains.iter(),
            |builder, base, _, feat| {
                let domain_name = feat.domain_name.as_deref().or(feat.description.as_deref());
                builder
                    .field_builder::<StringBuilder>(base)
                    .unwrap()
                    .append_option(domain_name);
            },
        );
        self.natural_variants.append_features(
            entry,
            &row.sequence,
            &row.mapper,
            entry.features.natural_variants.iter(),
            |builder, base, _, feat| {
                builder
                    .field_builder::<StringBuilder>(base)
                    .unwrap()
                    .append_option(feat.original.as_deref());
                builder
                    .field_builder::<StringBuilder>(base + 1)
                    .unwrap()
                    .append_option(feat.variation.as_deref());
            },
        );

        // Text-based comment features
        append_subunits(&mut self.subunits, entry);
        append_interactions(&mut self.interactions, entry);

        // PTM sites (residue-centric)
        append_ptm_sites(&mut self.ptm_sites, &self.metrics, entry, row);
    }

    /// Finishes the current batch and returns a RecordBatch
    pub fn finish_batch(&mut self) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.id.finish()),
            Arc::new(self.sequence.finish()),
            Arc::new(self.organism_id.finish()),
            Arc::new(self.isoforms.finish()),
            Arc::new(self.features.finish()),
            Arc::new(self.locations.finish()),
            Arc::new(self.entry_name.finish()),
            Arc::new(self.gene_name.finish()),
            Arc::new(self.protein_name.finish()),
            Arc::new(self.organism_name.finish()),
            Arc::new(self.existence.finish()),
            Arc::new(self.structures.finish()),
            Arc::new(self.parent_id.finish()),
            Arc::new(self.ptm_sites.finish()),
            Arc::new(self.active_sites.finish()),
            Arc::new(self.binding_sites.finish()),
            Arc::new(self.metal_coordinations.finish()),
            Arc::new(self.mutagenesis_sites.finish()),
            Arc::new(self.domains.finish()),
            Arc::new(self.natural_variants.finish()),
            Arc::new(self.subunits.finish()),
            Arc::new(self.interactions.finish()),
        ];

        let batch = RecordBatch::try_new(schema_ref(), arrays)?;

        let metrics = self.metrics.clone();
        *self = Self::new(self.capacity, metrics);

        Ok(batch)
    }

    /// Returns the current number of entries in the builders
    pub fn len(&self) -> usize {
        self.id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn create_isoforms_builder(capacity: usize) -> ListBuilder<StructBuilder> {
    let fields = Fields::from(vec![
        Field::new("isoform_id", DataType::Utf8, false),
        Field::new("isoform_sequence", DataType::Utf8, true),
        Field::new("isoform_note", DataType::Utf8, true),
    ]);

    let struct_builder = StructBuilder::from_fields(fields, capacity);
    ListBuilder::new(struct_builder)
}

fn create_features_builder(capacity: usize) -> ListBuilder<StructBuilder> {
    let fields = Fields::from(vec![
        Field::new("feature_type", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("start", DataType::Int32, true),
        Field::new("end", DataType::Int32, true),
        Field::new("evidence_code", DataType::Utf8, true),
    ]);

    let struct_builder = StructBuilder::from_fields(fields, capacity);
    ListBuilder::new(struct_builder)
}

fn create_locations_builder(capacity: usize) -> ListBuilder<StructBuilder> {
    let fields = Fields::from(vec![
        Field::new("location", DataType::Utf8, false),
        Field::new("evidence_code", DataType::Utf8, true),
    ]);

    let struct_builder = StructBuilder::from_fields(fields, capacity);
    ListBuilder::new(struct_builder)
}

fn create_structures_builder(capacity: usize) -> ListBuilder<StructBuilder> {
    let fields = Fields::from(vec![
        Field::new("db", DataType::Utf8, false),
        Field::new("id", DataType::Utf8, false),
    ]);

    let struct_builder = StructBuilder::from_fields(fields, capacity);
    ListBuilder::new(struct_builder)
}

fn create_ptm_sites_builder(capacity: usize) -> ListBuilder<StructBuilder> {
    let mod_fields = Fields::from(vec![
        Field::new("mod_type", DataType::Int32, false),
        Field::new("confidence_score", DataType::Float32, false),
    ]);

    let mods_struct_builder = StructBuilder::from_fields(mod_fields.clone(), capacity);
    let mods_list_builder = ListBuilder::new(mods_struct_builder);

    let mods_list_type = DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(mod_fields),
        true,
    )));

    let site_fields = Fields::from(vec![
        Field::new("site_index", DataType::Int32, false),
        Field::new("site_aa", DataType::Utf8, false),
        Field::new("modifications", mods_list_type, true),
    ]);

    let site_struct_builder = StructBuilder::new(
        site_fields,
        vec![
            Box::new(Int32Builder::with_capacity(capacity)),
            Box::new(StringBuilder::with_capacity(capacity, capacity)),
            Box::new(mods_list_builder),
        ],
    );

    ListBuilder::new(site_struct_builder)
}

fn create_coordinate_feature_builder(capacity: usize) -> ListBuilder<StructBuilder> {
    let fields = Fields::from(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("start", DataType::Int32, true),
        Field::new("end", DataType::Int32, true),
        Field::new("evidence_code", DataType::Utf8, true),
        Field::new("confidence_score", DataType::Float32, true),
    ]);
    let struct_builder = StructBuilder::from_fields(fields, capacity);
    ListBuilder::new(struct_builder)
}

fn create_metal_coordination_builder(capacity: usize) -> ListBuilder<StructBuilder> {
    let fields = Fields::from(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("metal", DataType::Utf8, true),
        Field::new("start", DataType::Int32, true),
        Field::new("end", DataType::Int32, true),
        Field::new("evidence_code", DataType::Utf8, true),
        Field::new("confidence_score", DataType::Float32, true),
    ]);
    let struct_builder = StructBuilder::from_fields(fields, capacity);
    ListBuilder::new(struct_builder)
}

fn create_domain_builder(capacity: usize) -> ListBuilder<StructBuilder> {
    let fields = Fields::from(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("domain_name", DataType::Utf8, true),
        Field::new("start", DataType::Int32, true),
        Field::new("end", DataType::Int32, true),
        Field::new("evidence_code", DataType::Utf8, true),
        Field::new("confidence_score", DataType::Float32, true),
    ]);
    let struct_builder = StructBuilder::from_fields(fields, capacity);
    ListBuilder::new(struct_builder)
}

fn create_natural_variant_builder(capacity: usize) -> ListBuilder<StructBuilder> {
    let fields = Fields::from(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("original", DataType::Utf8, true),
        Field::new("variation", DataType::Utf8, true),
        Field::new("start", DataType::Int32, true),
        Field::new("end", DataType::Int32, true),
        Field::new("evidence_code", DataType::Utf8, true),
        Field::new("confidence_score", DataType::Float32, true),
    ]);
    let struct_builder = StructBuilder::from_fields(fields, capacity);
    ListBuilder::new(struct_builder)
}

fn create_subunit_builder(capacity: usize) -> ListBuilder<StructBuilder> {
    let fields = Fields::from(vec![
        Field::new("text", DataType::Utf8, false),
        Field::new("evidence_code", DataType::Utf8, true),
        Field::new("confidence_score", DataType::Float32, true),
    ]);
    let struct_builder = StructBuilder::from_fields(fields, capacity);
    ListBuilder::new(struct_builder)
}

fn create_interaction_builder(capacity: usize) -> ListBuilder<StructBuilder> {
    let fields = Fields::from(vec![
        Field::new("interactant_id_1", DataType::Utf8, true),
        Field::new("interactant_id_2", DataType::Utf8, true),
        Field::new("evidence_code", DataType::Utf8, true),
        Field::new("confidence_score", DataType::Float32, true),
    ]);
    let struct_builder = StructBuilder::from_fields(fields, capacity);
    ListBuilder::new(struct_builder)
}

fn append_isoforms(builder: &mut ListBuilder<StructBuilder>, entry: &ParsedEntry) {
    let isoforms_struct = builder.values();
    for iso in &entry.isoforms {
        isoforms_struct
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value(&iso.isoform_id);
        isoforms_struct
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_option(iso.isoform_sequence.as_deref());
        isoforms_struct
            .field_builder::<StringBuilder>(2)
            .unwrap()
            .append_option(iso.isoform_note.as_deref());
        isoforms_struct.append(true);
    }
    builder.append(true);
}

fn append_features(builder: &mut ListBuilder<StructBuilder>, entry: &ParsedEntry) {
    let features_struct = builder.values();
    for feat in &entry.features.generic {
        let evidence = entry.resolve_evidence(&feat.evidence_keys);
        features_struct
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value(&feat.feature_type);
        features_struct
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_option(feat.description.as_deref());
        features_struct
            .field_builder::<Int32Builder>(2)
            .unwrap()
            .append_option(feat.start);
        features_struct
            .field_builder::<Int32Builder>(3)
            .unwrap()
            .append_option(feat.end);
        features_struct
            .field_builder::<StringBuilder>(4)
            .unwrap()
            .append_option(evidence.as_deref());
        features_struct.append(true);
    }
    builder.append(true);
}

fn append_locations(builder: &mut ListBuilder<StructBuilder>, entry: &ParsedEntry) {
    let locations_struct = builder.values();
    for loc in &entry.comments.locations {
        let evidence = entry.resolve_evidence(&loc.evidence_keys);
        locations_struct
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value(&loc.location);
        locations_struct
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_option(evidence.as_deref());
        locations_struct.append(true);
    }
    builder.append(true);
}

fn append_structures(builder: &mut ListBuilder<StructBuilder>, entry: &ParsedEntry) {
    let structures_struct = builder.values();
    for s in &entry.structures {
        structures_struct
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value(&s.database);
        structures_struct
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value(&s.id);
        structures_struct.append(true);
    }
    builder.append(true);
}

fn append_subunits(builder: &mut ListBuilder<StructBuilder>, entry: &ParsedEntry) {
    let list_struct = builder.values();
    for sub in &entry.comments.subunits {
        let evidence_code = entry.resolve_evidence(&sub.evidence_keys);
        let confidence = entry.max_confidence_for_evidence(&sub.evidence_keys);
        list_struct
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value(sub.text.trim());
        list_struct
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_option(evidence_code.as_deref());
        list_struct
            .field_builder::<Float32Builder>(2)
            .unwrap()
            .append_value(confidence);
        list_struct.append(true);
    }
    builder.append(true);
}

fn append_interactions(builder: &mut ListBuilder<StructBuilder>, entry: &ParsedEntry) {
    let list_struct = builder.values();
    for inter in &entry.comments.interactions {
        let evidence_code = entry.resolve_evidence(&inter.evidence_keys);
        let confidence = entry.max_confidence_for_evidence(&inter.evidence_keys);
        list_struct
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_option(inter.interactant_id_1.as_deref());
        list_struct
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_option(inter.interactant_id_2.as_deref());
        list_struct
            .field_builder::<StringBuilder>(2)
            .unwrap()
            .append_option(evidence_code.as_deref());
        list_struct
            .field_builder::<Float32Builder>(3)
            .unwrap()
            .append_value(confidence);
        list_struct.append(true);
    }
    builder.append(true);
}
