use arrow::array::{
    ArrayBuilder, Float32Builder, Int32Builder, ListArray, ListBuilder, StringBuilder, StructBuilder,
};

use crate::pipeline::mapper::CoordinateMapper;
use crate::pipeline::scratch::{
    ActiveSiteScratch, BindingSiteScratch, DomainScratch, MetalCoordinationScratch, MutagenesisSiteScratch,
    NaturalVariantScratch, ParsedEntry,
};

pub trait MappableFeature {
    fn id(&self) -> Option<&str>;
    fn description(&self) -> Option<&str>;
    fn start(&self) -> Option<i32>;
    fn end(&self) -> Option<i32>;
    fn evidence_keys(&self) -> &[String];
}

macro_rules! impl_mappable {
    ($ty:ty) => {
        impl MappableFeature for $ty {
            fn id(&self) -> Option<&str> {
                self.id.as_deref()
            }

            fn description(&self) -> Option<&str> {
                self.description.as_deref()
            }

            fn start(&self) -> Option<i32> {
                self.start
            }

            fn end(&self) -> Option<i32> {
                self.end
            }

            fn evidence_keys(&self) -> &[String] {
                &self.evidence_keys
            }
        }
    };
}

impl_mappable!(ActiveSiteScratch);
impl_mappable!(BindingSiteScratch);
impl_mappable!(MutagenesisSiteScratch);
impl_mappable!(MetalCoordinationScratch);
impl_mappable!(DomainScratch);
impl_mappable!(NaturalVariantScratch);

/// Wrapper around Arrow list/struct builders that hides field index arithmetic.
pub struct FeatureListBuilder {
    inner: ListBuilder<StructBuilder>,
    extra_fields: usize,
}

impl FeatureListBuilder {
    pub fn new(inner: ListBuilder<StructBuilder>, extra_fields: usize) -> Self {
        Self {
            inner,
            extra_fields,
        }
    }

    /// Appends a row of coordinate-based features, mapping coordinates with the provided mapper.
    ///
    /// `write_extra` is responsible for populating any extra fields between description and start/end.
    pub fn append_features<'a, F, I>(
        &mut self,
        entry: &ParsedEntry,
        isoform_sequence: &str,
        mapper: &CoordinateMapper,
        features: I,
        mut write_extra: impl FnMut(&mut StructBuilder, usize, usize, &F),
    ) where
        F: MappableFeature + 'a,
        I: IntoIterator<Item = &'a F>,
    {
        let start_index = 2 + self.extra_fields;
        let mut struct_builder = self.inner.values();

        for feature in features {
            let (Some(start), Some(end)) = (feature.start(), feature.end()) else {
                continue;
            };
            let Some((mapped_start, mapped_end)) =
                map_range_1based(entry, isoform_sequence, mapper, start, end)
            else {
                continue;
            };

            let evidence = entry.resolve_evidence(feature.evidence_keys());
            let confidence = entry.max_confidence_for_evidence(feature.evidence_keys());

            struct_builder
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_option(feature.id());
            struct_builder
                .field_builder::<StringBuilder>(1)
                .unwrap()
                .append_option(feature.description());

            write_extra(&mut struct_builder, 2, self.extra_fields, feature);

            struct_builder
                .field_builder::<Int32Builder>(start_index)
                .unwrap()
                .append_value(mapped_start);
            struct_builder
                .field_builder::<Int32Builder>(start_index + 1)
                .unwrap()
                .append_value(mapped_end);
            struct_builder
                .field_builder::<StringBuilder>(start_index + 2)
                .unwrap()
                .append_option(evidence.as_deref());
            struct_builder
                .field_builder::<Float32Builder>(start_index + 3)
                .unwrap()
                .append_value(confidence);
            struct_builder.append(true);
        }

        self.inner.append(true);
    }

    pub fn finish(&mut self) -> ListArray {
        self.inner.finish()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn values(&mut self) -> &mut ListBuilder<StructBuilder> {
        &mut self.inner
    }
}

/// Maps canonical coordinates to isoform coordinates with bounds checking.
pub fn map_range_1based(
    entry: &ParsedEntry,
    isoform_sequence: &str,
    mapper: &CoordinateMapper,
    start: i32,
    end: i32,
) -> Option<(i32, i32)> {
    if start <= 0 || end <= 0 || end < start {
        return None;
    }

    let canonical_len = entry.sequence.len() as i32;
    if canonical_len <= 0 || end > canonical_len {
        return None;
    }

    let iso_len = isoform_sequence.len() as i32;
    if iso_len <= 0 {
        return None;
    }

    let mapped_start = mapper.map_point_1based(start).ok()?;
    let mapped_end = if end == start {
        mapped_start
    } else {
        mapper.map_point_1based(end).ok()?
    };

    if mapped_start <= 0 || mapped_end <= 0 {
        return None;
    }
    if mapped_start > iso_len || mapped_end > iso_len {
        return None;
    }
    if mapped_end < mapped_start {
        return None;
    }

    Some((mapped_start, mapped_end))
}
