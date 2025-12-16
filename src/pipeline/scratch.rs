use std::collections::HashMap;

/// Per-feature scratch data
#[derive(Debug, Default, Clone)]
pub struct FeatureScratch {
    pub feature_type: String,
    pub description: Option<String>,
    pub start: Option<i32>,
    pub end: Option<i32>,
    pub evidence_keys: Vec<String>,
}

impl FeatureScratch {
    pub fn clear(&mut self) {
        self.feature_type.clear();
        self.description = None;
        self.start = None;
        self.end = None;
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
    pub isoform_note: Option<String>,
}

impl IsoformScratch {
    pub fn clear(&mut self) {
        self.isoform_id.clear();
        self.isoform_sequence = None;
        self.isoform_note = None;
    }
}

/// Entry-local scratch buffer for accumulating data during parsing.
/// All data is reset between entries to maintain constant memory.
#[derive(Debug, Default)]
pub struct EntryScratch {
    /// Primary accession (first <accession> element)
    pub accession: String,
    /// Full amino acid sequence
    pub sequence: String,
    /// NCBI Taxonomy ID
    pub organism_id: Option<i32>,

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
}

impl EntryScratch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Resets all fields for the next entry
    pub fn clear(&mut self) {
        self.accession.clear();
        self.sequence.clear();
        self.organism_id = None;
        self.evidence_map.clear();
        self.features.clear();
        self.current_feature.clear();
        self.locations.clear();
        self.current_location.clear();
        self.isoforms.clear();
        self.current_isoform.clear();
        self.text_buffer.clear();
        self.has_primary_accession = false;
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
