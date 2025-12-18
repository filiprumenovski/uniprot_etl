use arrow::array::{
    ArrayBuilder, ArrayRef, Int32Builder, Int8Builder, ListBuilder, StringBuilder, StructBuilder,
};
use arrow::datatypes::{DataType, Field, Fields};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use crate::error::Result;
use crate::pipeline::scratch::EntryScratch;
use crate::schema::schema_ref;

/// Builders for constructing Arrow arrays from parsed entries.
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
    capacity: usize,
}

impl EntryBuilders {
    pub fn new(capacity: usize) -> Self {
        Self {
            id: StringBuilder::with_capacity(capacity, capacity * 10),
            sequence: StringBuilder::with_capacity(capacity, capacity * 500),
            organism_id: Int32Builder::with_capacity(capacity),
            isoforms: Self::create_isoforms_builder(capacity),
            features: Self::create_features_builder(capacity),
            locations: Self::create_locations_builder(capacity),
            entry_name: StringBuilder::with_capacity(capacity, capacity * 20),
            gene_name: StringBuilder::with_capacity(capacity, capacity * 20),
            protein_name: StringBuilder::with_capacity(capacity, capacity * 50),
            organism_name: StringBuilder::with_capacity(capacity, capacity * 30),
            existence: Int8Builder::with_capacity(capacity),
            structures: Self::create_structures_builder(capacity),
            capacity,
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

    /// Appends a parsed entry to the builders
    pub fn append_entry(&mut self, scratch: &EntryScratch) {
        // Top-level fields
        self.id.append_value(&scratch.accession);
        self.sequence.append_value(&scratch.sequence);
        self.organism_id.append_option(scratch.organism_id);

        // Rich names
        self.entry_name.append_option(scratch.entry_name.as_deref());
        self.gene_name.append_option(scratch.gene_name.as_deref());
        self.protein_name
            .append_option(scratch.protein_name.as_deref());
        self.organism_name
            .append_option(scratch.organism_scientific_name.as_deref());
        // Existence
        if scratch.existence == 0 {
            self.existence.append_null();
        } else {
            self.existence.append_value(scratch.existence);
        }

        // Isoforms
        let isoforms_struct = self.isoforms.values();
        for iso in &scratch.isoforms {
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
        self.isoforms.append(true);

        // Features
        let features_struct = self.features.values();
        for feat in &scratch.features {
            let evidence = scratch.resolve_evidence(&feat.evidence_keys);
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
        self.features.append(true);

        // Locations
        let locations_struct = self.locations.values();
        for loc in &scratch.locations {
            let evidence = scratch.resolve_evidence(&loc.evidence_keys);
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
        self.locations.append(true);

        // Structures
        let structures_struct = self.structures.values();
        for s in &scratch.structures {
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
        self.structures.append(true);
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
        ];

        let batch = RecordBatch::try_new(schema_ref(), arrays)?;

        // Reset builders for next batch
        *self = Self::new(self.capacity);

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
