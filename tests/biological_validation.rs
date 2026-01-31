use uniprot_etl::pipeline::mapper::{CoordinateMapper, MapFailure};
use uniprot_etl::pipeline::scratch::{FeatureScratch, ParsedEntry};

#[test]
fn tp53_s15_canonical_control_identity_mapping() {
    // TP53 (P04637) has a known phospho-site at Ser15.
    // Canonical control: canonical mapping must be identity.
    let entry = ParsedEntry::default();
    let mapper = CoordinateMapper::from_entry(&entry);

    // XML coordinates are 1-based. Ser15 => index 14 (0-based).
    let mapped = mapper.map_point_1based(15).expect("identity map");
    assert_eq!(mapped, 15);
}

#[test]
fn tp53_s15_deletion_event_blocks_ptm() {
    // Synthetic VSP: delete positions 10..=20 ("Missing"). A PTM at 15 must be rejected via deletion event.
    // We don't need full XML parsing here to validate mapper semantics.
    let mut entry = ParsedEntry::default();
    entry.sequence = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".to_string();

    entry.features.generic.push(FeatureScratch {
        id: Some("VSP_TEST".to_string()),
        feature_type: "variant sequence".to_string(),
        start: Some(10),
        end: Some(20),
        variation: Some("Missing".to_string()),
        ..Default::default()
    });

    let mapper = CoordinateMapper::from_entry_for_vsp_ids(&entry, &["VSP_TEST".to_string()]);

    let err = mapper.map_point_1based(15).unwrap_err();
    assert!(matches!(err, MapFailure::VspDeletionEvent));
}
