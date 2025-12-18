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
    /// Only used for <feature type="variant sequence">.
    /// Captures <original>...</original> text.
    pub original: Option<String>,
    /// Only used for <feature type="variant sequence">.
    /// Captures <variation>...</variation> text.
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

/// Entry-local scratch buffer for accumulating data during parsing.
/// All data is reset between entries to maintain constant memory.
#[derive(Debug, Default)]
pub struct EntryScratch {
    /// Primary accession (first <accession> element)
    pub accession: String,
    /// Canonical/base accession used as parent_id for isoform rows.
    /// This is always the first <accession> encountered.
    pub parent_id: String,
    /// Full amino acid sequence
    pub sequence: String,
    /// NCBI Taxonomy ID
    pub organism_id: Option<i32>,

    /// Entry name (<entry><name>)
    pub entry_name: Option<String>,
    /// Primary gene name (<gene><name type="primary">)
    pub gene_name: Option<String>,
    /// Recommended protein name (<protein><recommendedName><fullName>)
    pub protein_name: Option<String>,
    /// Organism scientific name (<organism><name type="scientific">)
    pub organism_scientific_name: Option<String>,
    /// Protein existence (mapped 1-5; 0 unknown)
    pub existence: i8,

    /// Structural references (e.g., PDB, AlphaFoldDB)
    pub structures: Vec<StructureRef>,

    /// Entry-local evidence key -> ECO code mapping
    pub evidence_map: HashMap<String, String>,

    /// Accumulated features
    pub features: Vec<FeatureScratch>,
    /// Current feature being parsed
    pub current_feature: FeatureScratch,

    /// Accumulated subcellular locations
    pub locations: Vec<LocationScratch>,
    /// Current location being parsed
    pub current_location: LocationScratch,

    /// Accumulated isoforms
    pub isoforms: Vec<IsoformScratch>,
    /// Current isoform being parsed
    pub current_isoform: IsoformScratch,

    /// Text accumulator for multi-event text content
    pub text_buffer: String,

    /// Flag: have we captured the primary accession?
    pub has_primary_accession: bool,

    /// Tracks which feature type we're currently parsing (for coordinate mapping)
    /// Used to route coordinates to the correct feature-specific buffer while in FeatureLocation state
    pub current_feature_context: FeatureContext,

    // ========================================================================
    // Category A: Coordinate-Based Features (6 types including variant)
    // ========================================================================
    /// Accumulated active sites
    pub active_sites: Vec<ActiveSiteScratch>,
    /// Current active site being parsed
    pub current_active_site: ActiveSiteScratch,

    /// Accumulated binding sites
    pub binding_sites: Vec<BindingSiteScratch>,
    /// Current binding site being parsed
    pub current_binding_site: BindingSiteScratch,

    /// Accumulated metal coordination sites
    pub metal_coordinations: Vec<MetalCoordinationScratch>,
    /// Current metal coordination site being parsed
    pub current_metal_coordination: MetalCoordinationScratch,

    /// Accumulated mutagenesis sites
    pub mutagenesis_sites: Vec<MutagenesisSiteScratch>,
    /// Current mutagenesis site being parsed
    pub current_mutagenesis_site: MutagenesisSiteScratch,

    /// Accumulated domains
    pub domains: Vec<DomainScratch>,
    /// Current domain being parsed
    pub current_domain: DomainScratch,

    /// Accumulated natural variants
    pub natural_variants: Vec<NaturalVariantScratch>,
    /// Current natural variant being parsed
    pub current_natural_variant: NaturalVariantScratch,

    // ========================================================================
    // Category B: Text-Based Comment Features (2 types)
    // ========================================================================
    /// Accumulated subunit comments
    pub subunits: Vec<SubunitScratch>,
    /// Current subunit comment being parsed
    pub current_subunit: SubunitScratch,

    /// Accumulated PPI interactions
    pub interactions: Vec<InteractionScratch>,
    /// Current interaction being parsed
    pub current_interaction: InteractionScratch,
}

impl EntryScratch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Resets all fields for the next entry
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
        self.current_feature.clear();
        self.locations.clear();
        self.current_location.clear();
        self.isoforms.clear();
        self.current_isoform.clear();
        self.text_buffer.clear();
        self.has_primary_accession = false;

        // Clear all new category A coordinate-based features
        self.active_sites.clear();
        self.current_active_site.clear();
        self.binding_sites.clear();
        self.current_binding_site.clear();
        self.metal_coordinations.clear();
        self.current_metal_coordination.clear();
        self.mutagenesis_sites.clear();
        self.current_mutagenesis_site.clear();
        self.domains.clear();
        self.current_domain.clear();
        self.natural_variants.clear();
        self.current_natural_variant.clear();

        // Clear all category B text-based comment features
        self.subunits.clear();
        self.current_subunit.clear();
        self.interactions.clear();
        self.current_interaction.clear();

        // Reset feature context
        self.current_feature_context = FeatureContext::Generic;
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

/// Reference to external structural database (PDB/AlphaFoldDB)
#[derive(Debug, Default, Clone)]
pub struct StructureRef {
    pub database: String,
    pub id: String,
}
