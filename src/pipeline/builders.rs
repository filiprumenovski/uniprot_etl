use arrow::array::{
    ArrayBuilder, ArrayRef, Float32Builder, Int32Builder, Int8Builder, ListBuilder, StringBuilder,
    StructBuilder,
};
use arrow::datatypes::{DataType, Field, Fields};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use crate::error::Result;
use crate::metrics::Metrics;
use crate::pipeline::mapper::{CoordinateMapper, MapFailure};
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
    pub parent_id: StringBuilder,
    pub ptm_sites: ListBuilder<StructBuilder>,
    capacity: usize,
    metrics: Metrics,
}

impl EntryBuilders {
    pub fn new(capacity: usize, metrics: Metrics) -> Self {
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
            parent_id: StringBuilder::with_capacity(capacity, capacity * 10),
            ptm_sites: Self::create_ptm_sites_builder(capacity),
            capacity,
            metrics,
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
        // site_index: Int32, site_aa: Utf8, modifications: List<Struct<mod_type:Int32, confidence_score:Float32>>
        //
        // IMPORTANT: Build nested builders explicitly so downcasting via
        // `field_builder::<ListBuilder<StructBuilder>>()` works at runtime.
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

    /// Appends a single output row.
    ///
    /// This is used for isoform "explosion": the same `scratch` metadata is replicated,
    /// while `row_id`, `row_sequence`, and `parent_id` vary per row.
    pub fn append_row(
        &mut self,
        scratch: &EntryScratch,
        row_id: &str,
        parent_id: &str,
        row_sequence: &str,
        mapper: &CoordinateMapper,
    ) {
        // Top-level fields
        self.id.append_value(row_id);
        self.sequence.append_value(row_sequence);
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

        // Parent anchor
        self.parent_id.append_value(parent_id);

        // PTM sites (residue-centric)
        self.append_ptm_sites(scratch, row_id, parent_id, row_sequence, mapper);
    }

    fn append_ptm_sites(
        &mut self,
        scratch: &EntryScratch,
        row_id: &str,
        parent_id: &str,
        isoform_sequence: &str,
        mapper: &CoordinateMapper,
    ) {
        use std::collections::BTreeMap;

        let isoform_bytes = isoform_sequence.as_bytes();

        // site_index (1-based mapped), site_aa (single-char), modifications
        let mut sites: BTreeMap<i32, (u8, Vec<(i32, f32)>)> = BTreeMap::new();

        for feat in &scratch.features {
            let ft = feat.feature_type.to_ascii_lowercase();
            let is_point_ptm =
                ft == "glycosylation site" || ft == "modified residue" || ft == "cross-link";
            if !is_point_ptm {
                continue;
            }

            let (Some(start), Some(end)) = (feat.start, feat.end) else {
                continue;
            };
            if start <= 0 || end <= 0 || start != end {
                continue;
            }

            self.metrics.add_ptm_attempted(1);

            // Step 1 (Canonical control): fetch original_aa from canonical sequence BEFORE shifting.
            let Some(original_aa) = scratch.canonical_aa_at_1based(start) else {
                self.metrics.add_ptm_failed(1);
                self.metrics.add_ptm_failed_canonical_oob(1);
                eprintln!(
                    "[PTM_FAIL] code=CANONICAL_OOB parent_id={} id={} original_index={} mapped_index=?",
                    parent_id, row_id, start
                );
                continue;
            };

            // Step 2: map canonical coordinate to isoform coordinate.
            // Canonical control rule: canonical rows must use shift=0.
            let mapped_1based = if row_id == parent_id {
                start
            } else {
                match mapper.map_point_1based(start) {
                    Ok(m) => m,
                    Err(MapFailure::VspDeletionEvent) => {
                        self.metrics.add_ptm_failed(1);
                        self.metrics.add_ptm_failed_vsp_deletion(1);
                        eprintln!(
                            "[PTM_FAIL] code=VSP_DELETION_EVENT parent_id={} id={} original_index={} mapped_index=?",
                            parent_id, row_id, start
                        );
                        continue;
                    }
                    Err(MapFailure::PtmOutOfBounds) => {
                        self.metrics.add_ptm_failed(1);
                        self.metrics.add_ptm_failed_mapper_oob(1);
                        eprintln!(
                            "[PTM_FAIL] code=MAPPER_OOB parent_id={} id={} original_index={} mapped_index=?",
                            parent_id, row_id, start
                        );
                        continue;
                    }
                    Err(MapFailure::VspUnresolvable) => {
                        self.metrics.add_ptm_failed(1);
                        self.metrics.add_ptm_failed_vsp_unresolvable(1);
                        eprintln!(
                            "[PTM_FAIL] code=VSP_UNRESOLVABLE parent_id={} id={} original_index={} mapped_index=?",
                            parent_id, row_id, start
                        );
                        continue;
                    }
                }
            };

            let mapped_idx0 = (mapped_1based as usize).saturating_sub(1);
            if mapped_idx0 >= isoform_bytes.len() {
                self.metrics.add_ptm_failed(1);
                self.metrics.add_ptm_failed_isoform_oob(1);
                eprintln!(
                    "[PTM_FAIL] code=ISOFORM_OOB parent_id={} id={} original_index={} mapped_index={} isoform_len={}",
                    parent_id,
                    row_id,
                    start,
                    mapped_1based,
                    isoform_bytes.len()
                );
                continue;
            }

            let isoform_aa = isoform_bytes[mapped_idx0];

            // Step 3 (Verification): isoform[mapped] must equal canonical[original].
            if isoform_aa != original_aa {
                self.metrics.add_ptm_failed(1);
                self.metrics.add_ptm_failed_residue_mismatch(1);
                eprintln!(
                    "[PTM_FAIL] code=RESIDUE_MISMATCH parent_id={} id={} original_index={} mapped_index={} original_aa={} isoform_aa={} ",
                    parent_id,
                    row_id,
                    start,
                    mapped_1based,
                    original_aa as char,
                    isoform_aa as char
                );
                continue;
            }

            let mod_type = classify_mod_type(&ft, feat.description.as_deref());
            let confidence = scratch.max_confidence_for_evidence(&feat.evidence_keys);

            let entry = sites
                .entry(mapped_1based)
                .or_insert_with(|| (original_aa, Vec::new()));
            entry.1.push((mod_type, confidence));

            self.metrics.add_ptm_mapped(1);
        }

        let sites_struct = self.ptm_sites.values();
        for (site_index, (site_aa, modifications)) in sites {
            sites_struct
                .field_builder::<Int32Builder>(0)
                .unwrap()
                .append_value(site_index);
            sites_struct
                .field_builder::<StringBuilder>(1)
                .unwrap()
                .append_value((site_aa as char).to_string());

            let mods_list = sites_struct
                .field_builder::<ListBuilder<StructBuilder>>(2)
                .unwrap();
            let mods_struct = mods_list.values();
            for (mod_type, confidence_score) in modifications {
                mods_struct
                    .field_builder::<Int32Builder>(0)
                    .unwrap()
                    .append_value(mod_type);
                mods_struct
                    .field_builder::<Float32Builder>(1)
                    .unwrap()
                    .append_value(confidence_score);
                mods_struct.append(true);
            }
            mods_list.append(true);

            sites_struct.append(true);
        }
        self.ptm_sites.append(true);
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
        ];

        let batch = RecordBatch::try_new(schema_ref(), arrays)?;

        // Reset builders for next batch
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

fn classify_mod_type(feature_type_lower: &str, description: Option<&str>) -> i32 {
    let desc = description.unwrap_or("").to_ascii_lowercase();

    // Vocabulary mapping (extensible):
    // 1 = Phosphorylation (modified residue + contains "phospho")
    // 2 = O-GlcNAc (glycosylation site + contains "n-acetylglucosamine")
    if feature_type_lower == "modified residue" && desc.contains("phospho") {
        1
    } else if feature_type_lower == "glycosylation site" && desc.contains("n-acetylglucosamine") {
        2
    } else {
        0
    }
}
