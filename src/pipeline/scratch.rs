use std::collections::HashMap;

/// Per-feature scratch data
#[derive(Debug, Default, Clone)]
pub struct FeatureScratch {
    pub id: Option<String>,
    pub feature_type: String,
    pub description: Option<String>,
    pub start: Option<i32>,
    pub end: Option<i32>,
    pub evidence_keys: Vec<String>,
    pub original: Option<String>,
    pub variation: Option<String>,
}

impl FeatureScratch {
    pub fn clear(&mut self) {
        self.id = None;
        self.feature_type.clear();
        self.description = None;
        self.start = None;
        self.end = None;
        self.evidence_keys.clear();
        self.original = None;
        self.variation = None;
    }
}

// ============================================================================
// Category A: Coordinate-Based Feature Sub-Structs
// ============================================================================

/// Active Site feature (type="active site")
#[derive(Debug, Default, Clone)]
pub struct ActiveSiteScratch {
    pub id: Option<String>,
    pub description: Option<String>,
    pub start: Option<i32>,
    pub end: Option<i32>,
    pub evidence_keys: Vec<String>,
}

impl ActiveSiteScratch {
    pub fn clear(&mut self) {
        self.id = None;
        self.description = None;
        self.start = None;
        self.end = None;
        self.evidence_keys.clear();
    }
}

/// Binding Site feature (type="binding site")
#[derive(Debug, Default, Clone)]
pub struct BindingSiteScratch {
    pub id: Option<String>,
    pub description: Option<String>,
    pub start: Option<i32>,
    pub end: Option<i32>,
    pub evidence_keys: Vec<String>,
}

impl BindingSiteScratch {
    pub fn clear(&mut self) {
        self.id = None;
        self.description = None;
        self.start = None;
        self.end = None;
        self.evidence_keys.clear();
    }
}

/// Metal Ion Coordination site (type="metal ion-binding site")
#[derive(Debug, Default, Clone)]
pub struct MetalCoordinationScratch {
    pub id: Option<String>,
    pub description: Option<String>,
    pub metal: Option<String>,
    pub start: Option<i32>,
    pub end: Option<i32>,
    pub evidence_keys: Vec<String>,
}

impl MetalCoordinationScratch {
    pub fn clear(&mut self) {
        self.id = None;
        self.description = None;
        self.metal = None;
        self.start = None;
        self.end = None;
        self.evidence_keys.clear();
    }
}

/// Mutagenesis Site feature (type="mutagenesis site")
#[derive(Debug, Default, Clone)]
pub struct MutagenesisSiteScratch {
    pub id: Option<String>,
    pub description: Option<String>,
    pub start: Option<i32>,
    pub end: Option<i32>,
    pub evidence_keys: Vec<String>,
}

impl MutagenesisSiteScratch {
    pub fn clear(&mut self) {
        self.id = None;
        self.description = None;
        self.start = None;
        self.end = None;
        self.evidence_keys.clear();
    }
}

/// Domain Architecture feature (type="domain")
#[derive(Debug, Default, Clone)]
pub struct DomainScratch {
    pub id: Option<String>,
    pub description: Option<String>,
    pub domain_name: Option<String>,
    pub start: Option<i32>,
    pub end: Option<i32>,
    pub evidence_keys: Vec<String>,
}

impl DomainScratch {
    pub fn clear(&mut self) {
        self.id = None;
        self.description = None;
        self.domain_name = None;
        self.start = None;
        self.end = None;
        self.evidence_keys.clear();
    }
}

/// Natural Variant feature (type="sequence variant")
#[derive(Debug, Default, Clone)]
pub struct NaturalVariantScratch {
    pub id: Option<String>,
    pub description: Option<String>,
    pub original: Option<String>,
    pub variation: Option<String>,
    pub start: Option<i32>,
    pub end: Option<i32>,
    pub evidence_keys: Vec<String>,
}

impl NaturalVariantScratch {
    pub fn clear(&mut self) {
        self.id = None;
        self.description = None;
        self.original = None;
        self.variation = None;
        self.start = None;
        self.end = None;
        self.evidence_keys.clear();
    }
}

// ============================================================================
// Category B: Text-Based Comment Feature Sub-Structs
// ============================================================================

/// Subunit comment (type="subunit")
#[derive(Debug, Default, Clone)]
pub struct SubunitScratch {
    pub text: String,
    pub evidence_keys: Vec<String>,
}

impl SubunitScratch {
    pub fn clear(&mut self) {
        self.text.clear();
        self.evidence_keys.clear();
    }
}

/// Protein-Protein Interaction comment (type="interaction")
#[derive(Debug, Default, Clone)]
pub struct InteractionScratch {
    pub partner_id: Option<String>,
    pub partner_name: Option<String>,
    pub interactant_id_1: Option<String>,
    pub interactant_id_2: Option<String>,
    pub evidence_keys: Vec<String>,
}

impl InteractionScratch {
    pub fn clear(&mut self) {
        self.partner_id = None;
        self.partner_name = None;
        self.interactant_id_1 = None;
        self.interactant_id_2 = None;
        self.evidence_keys.clear();
    }
}

/// Per-location scratch data
#[derive(Debug, Default, Clone)]
pub struct LocationScratch {
    pub location: String,
    pub evidence_keys: Vec<String>,
}

impl LocationScratch {
    pub fn clear(&mut self) {
        self.location.clear();
        self.evidence_keys.clear();
    }
}

/// Per-isoform scratch data
#[derive(Debug, Default, Clone)]
pub struct IsoformScratch {
    pub isoform_id: String,
    pub isoform_sequence: Option<String>,
    /// UniProt "described" sequence refs (usually VSP_...) that define how this isoform differs.
    pub vsp_ids: Vec<String>,
    pub isoform_note: Option<String>,
}

impl IsoformScratch {
    pub fn clear(&mut self) {
        self.isoform_id.clear();
        self.isoform_sequence = None;
        self.vsp_ids.clear();
        self.isoform_note = None;
    }
}

/// Reference to external structural database (PDB/AlphaFoldDB)
#[derive(Debug, Default, Clone)]
pub struct StructureRef {
    pub database: String,
    pub id: String,
}

/// Tracks which feature type we're currently parsing to route coordinates correctly
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FeatureContext {
    #[default]
    Generic,
    ActiveSite,
    BindingSite,
    MetalCoordination,
    Mutagenesis,
    Domain,
    NaturalVariant,
}

/// Finalized entry representation used by downstream transformer and batcher.
#[derive(Debug, Default)]
pub struct ParsedEntry {
    pub accession: String,
    pub parent_id: String,
    pub sequence: String,
    pub organism_id: Option<i32>,

    pub entry_name: Option<String>,
    pub gene_name: Option<String>,
    pub protein_name: Option<String>,
    pub organism_scientific_name: Option<String>,
    pub existence: i8,

    pub structures: Vec<StructureRef>,
    pub evidence_map: HashMap<String, String>,

    pub features: FeatureCollections,
    pub comments: CommentCollections,
    pub isoforms: Vec<IsoformScratch>,
}

impl ParsedEntry {
    pub fn clear(&mut self) {
        self.accession.clear();
        self.parent_id.clear();
        self.sequence.clear();
        self.organism_id = None;
        self.entry_name = None;
        self.gene_name = None;
        self.protein_name = None;
        self.organism_scientific_name = None;
        self.existence = 0;
        self.structures.clear();
        self.evidence_map.clear();
        self.features.clear();
        self.comments.clear();
        self.isoforms.clear();
    }

    /// Returns the canonical amino acid at a 1-based XML coordinate.
    ///
    /// IMPORTANT: This must be called BEFORE any coordinate shifting.
    pub fn canonical_aa_at_1based(&self, pos_1based: i32) -> Option<u8> {
        if pos_1based <= 0 {
            return None;
        }
        let idx = (pos_1based as usize).saturating_sub(1);
        self.sequence.as_bytes().get(idx).copied()
    }

    /// Computes confidence score from evidence keys using MAX priority mapping.
    /// Mapping:
    /// - ECO:0000269 -> 1.0 (Experimental)
    /// - ECO:0007744 -> 0.8 (High-throughput)
    /// - ECO:0000250 -> 0.4 (Homology)
    /// - ECO:0000255 -> 0.1 (Predicted)
    /// - others/unknown/absent -> 0.1
    pub fn max_confidence_for_evidence(&self, keys: &[String]) -> f32 {
        if keys.is_empty() {
            return 0.1;
        }

        let mut best = 0.1f32;
        for key in keys {
            let Some(eco) = self.evidence_map.get(key) else {
                continue;
            };

            let score = match eco.as_str() {
                "ECO:0000269" => 1.0,
                "ECO:0007744" => 0.8,
                "ECO:0000250" => 0.4,
                "ECO:0000255" => 0.1,
                _ => 0.1,
            };
            if score > best {
                best = score;
                if (best - 1.0).abs() < f32::EPSILON {
                    break;
                }
            }
        }

        best
    }

    /// Resolves evidence keys to ECO codes (semicolon-joined)
    pub fn resolve_evidence(&self, keys: &[String]) -> Option<String> {
        if keys.is_empty() {
            return None;
        }

        let codes: Vec<&str> = keys
            .iter()
            .filter_map(|key| self.evidence_map.get(key).map(|s| s.as_str()))
            .collect();

        if codes.is_empty() {
            None
        } else {
            Some(codes.join(";"))
        }
    }
}

/// Aggregates coordinate-based feature collections.
#[derive(Debug, Default)]
pub struct FeatureCollections {
    pub generic: Vec<FeatureScratch>,
    pub active_sites: Vec<ActiveSiteScratch>,
    pub binding_sites: Vec<BindingSiteScratch>,
    pub metal_coordinations: Vec<MetalCoordinationScratch>,
    pub mutagenesis_sites: Vec<MutagenesisSiteScratch>,
    pub domains: Vec<DomainScratch>,
    pub natural_variants: Vec<NaturalVariantScratch>,
}

impl FeatureCollections {
    pub fn clear(&mut self) {
        self.generic.clear();
        self.active_sites.clear();
        self.binding_sites.clear();
        self.metal_coordinations.clear();
        self.mutagenesis_sites.clear();
        self.domains.clear();
        self.natural_variants.clear();
    }
}

/// Aggregates comment-derived collections.
#[derive(Debug, Default)]
pub struct CommentCollections {
    pub locations: Vec<LocationScratch>,
    pub subunits: Vec<SubunitScratch>,
    pub interactions: Vec<InteractionScratch>,
}

impl CommentCollections {
    pub fn clear(&mut self) {
        self.locations.clear();
        self.subunits.clear();
        self.interactions.clear();
    }
}

/// Entry-local scratch buffer for accumulating data during parsing.
/// All data is reset between entries to maintain constant memory.
#[derive(Debug, Default)]
pub struct EntryScratch {
    pub entry: ParsedEntry,
    pub text_buffer: String,
    pub has_primary_accession: bool,
    pub current_feature_context: FeatureContext,

    pub current_feature: FeatureScratch,
    pub current_active_site: ActiveSiteScratch,
    pub current_binding_site: BindingSiteScratch,
    pub current_metal_coordination: MetalCoordinationScratch,
    pub current_mutagenesis_site: MutagenesisSiteScratch,
    pub current_domain: DomainScratch,
    pub current_natural_variant: NaturalVariantScratch,

    pub current_location: LocationScratch,
    pub current_isoform: IsoformScratch,
    pub current_subunit: SubunitScratch,
    pub current_interaction: InteractionScratch,
}

impl EntryScratch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Resets all fields for the next entry
    pub fn reset(&mut self) {
        self.entry.clear();
        self.text_buffer.clear();
        self.has_primary_accession = false;
        self.current_feature_context = FeatureContext::Generic;

        self.current_feature.clear();
        self.current_active_site.clear();
        self.current_binding_site.clear();
        self.current_metal_coordination.clear();
        self.current_mutagenesis_site.clear();
        self.current_domain.clear();
        self.current_natural_variant.clear();
        self.current_location.clear();
        self.current_isoform.clear();
        self.current_subunit.clear();
        self.current_interaction.clear();
    }

    /// Moves the accumulated entry out, leaving the scratch ready for reuse.
    pub fn take_entry(&mut self) -> ParsedEntry {
        let entry = std::mem::take(&mut self.entry);
        self.reset();
        entry
    }
}
