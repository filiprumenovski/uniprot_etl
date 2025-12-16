/// Parser state machine for UniProt XML entry processing.
///
/// Transitions follow the UniProt XML structure:
/// Root -> Entry -> (various nested elements) -> Entry -> Root
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum State {
    #[default]
    Root,
    Entry,
    Accession,
    Sequence,
    Organism,
    OrganismDbReference,
    Feature,
    FeatureLocation,
    FeaturePosition,
    FeatureBegin,
    FeatureEnd,
    Comment,
    CommentSubcellularLocation,
    CommentLocation,
    CommentIsoform,
    CommentIsoformId,
    CommentIsoformSequence,
    CommentIsoformNote,
    Evidence,
}

impl State {
    /// Returns true if we're inside an entry element
    #[allow(dead_code)]
    pub fn in_entry(&self) -> bool {
        !matches!(self, State::Root)
    }

    /// Returns true if we're capturing text content
    pub fn captures_text(&self) -> bool {
        matches!(
            self,
            State::Accession
                | State::Sequence
                | State::CommentLocation
                | State::CommentIsoformId
                | State::CommentIsoformNote
        )
    }
}
