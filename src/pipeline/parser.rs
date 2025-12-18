use arrow::record_batch::RecordBatch;
use crossbeam_channel::Sender;
use quick_xml::events::Event;
use quick_xml::Reader;
use std::io::BufRead;

use crate::error::Result;
use crate::metrics::Metrics;
use crate::pipeline::batcher::Batcher;
use crate::pipeline::scratch::EntryScratch;
use crate::pipeline::state::State;
use std::collections::HashMap;
use std::sync::Arc;

/// Parses UniProt XML entries and sends RecordBatches to the channel.
pub fn parse_entries<R: BufRead>(
    mut reader: Reader<R>,
    sender: Sender<RecordBatch>,
    metrics: &Metrics,
    batch_size: usize,
    sidecar_fasta: Option<Arc<HashMap<String, String>>>,
) -> Result<()> {
    let mut batcher = Batcher::with_batch_size(sender, metrics.clone(), batch_size, sidecar_fasta);
    let mut scratch = EntryScratch::new();
    let mut state = State::Root;
    let mut buf = Vec::with_capacity(4096);

    loop {
        buf.clear();
        match reader.read_event_into(&mut buf)? {
            Event::Eof => break,

            Event::Start(e) => {
                state = handle_start_tag(&e, state, &mut scratch)?;
            }

            Event::Empty(e) => {
                handle_empty_tag(&e, state, &mut scratch)?;
            }

            Event::Text(e) => {
                if state.captures_text() {
                    scratch.text_buffer.push_str(&e.unescape()?);
                }
            }

            Event::End(e) => {
                state = handle_end_tag(&e, state, &mut scratch, &mut batcher)?;
            }

            _ => {}
        }
    }

    batcher.finish()?;
    Ok(())
}

fn handle_start_tag(
    e: &quick_xml::events::BytesStart<'_>,
    state: State,
    scratch: &mut EntryScratch,
) -> Result<State> {
    let tag_name = e.local_name();
    let tag = tag_name.as_ref();

    Ok(match (state, tag) {
        (State::Protein, b"proteinExistence") => {
            if let Some(t) = get_attribute(e, b"type")? {
                scratch.existence = map_existence(&t);
            }
            State::ProteinExistence
        }
        (State::Root, b"entry") => {
            scratch.clear();
            State::Entry
        }
        // Entry-level name
        (State::Entry, b"name") => State::EntryName,
        (State::Entry, b"accession") => State::Accession,
        (State::Entry, b"sequence") => State::Sequence,
        (State::Entry, b"organism") => State::Organism,
        // Organism scientific name
        (State::Organism, b"name") => {
            if let Some(t) = get_attribute(e, b"type")? {
                if t == "scientific" {
                    State::OrganismScientificName
                } else {
                    State::Organism
                }
            } else {
                State::Organism
            }
        }
        (State::Organism, b"dbReference") => {
            // Look for NCBI Taxonomy reference
            if let Some(type_attr) = get_attribute(e, b"type")? {
                if type_attr == "NCBI Taxonomy" {
                    if let Some(id) = get_attribute(e, b"id")? {
                        scratch.organism_id = id.parse().ok();
                    }
                }
            }
            State::OrganismDbReference
        }
        // Gene primary name
        (State::Entry, b"gene") => State::Gene,
        (State::Gene, b"name") => {
            if let Some(t) = get_attribute(e, b"type")? {
                if t == "primary" {
                    State::GeneName
                } else {
                    State::Gene
                }
            } else {
                State::Gene
            }
        }
        // Protein recommended full name and existence
        (State::Entry, b"protein") => State::Protein,
        (State::Protein, b"recommendedName") => State::RecommendedName,
        (State::RecommendedName, b"fullName") => State::RecommendedName,
        // Structural hooks at entry level
        (State::Entry, b"dbReference") => {
            if let Some(db) = get_attribute(e, b"type")? {
                if db == "PDB" || db == "AlphaFoldDB" {
                    if let Some(id) = get_attribute(e, b"id")? {
                        scratch
                            .structures
                            .push(crate::pipeline::scratch::StructureRef { database: db, id });
                    }
                }
            }
            State::Entry
        }
        (State::Entry, b"feature") => {
            scratch.current_feature.clear();
            if let Some(id) = get_attribute(e, b"id")? {
                scratch.current_feature.id = Some(id);
            }
            if let Some(ft) = get_attribute(e, b"type")? {
                scratch.current_feature.feature_type = ft.clone();
                // Keep legacy generic feature capture for all types, even the ones
                // that are also extracted into dedicated enriched columns.
                if let Some(desc) = get_attribute(e, b"description")? {
                    scratch.current_feature.description = Some(desc);
                }
                if let Some(ev) = get_attribute(e, b"evidence")? {
                    scratch.current_feature.evidence_keys = parse_evidence_refs(&ev);
                }
                // Route to specialized feature states for new category A & B features
                return Ok(match ft.as_str() {
                    "active site" => {
                        scratch.current_active_site.clear();
                        scratch.current_feature_context = crate::pipeline::scratch::FeatureContext::ActiveSite;
                        if let Some(id) = get_attribute(e, b"id")? {
                            scratch.current_active_site.id = Some(id);
                        }
                        scratch.current_active_site.description = scratch.current_feature.description.clone();
                        scratch.current_active_site.evidence_keys = scratch.current_feature.evidence_keys.clone();
                        State::FeatureActiveSite
                    }
                    "binding site" => {
                        scratch.current_binding_site.clear();
                        scratch.current_feature_context = crate::pipeline::scratch::FeatureContext::BindingSite;
                        if let Some(id) = get_attribute(e, b"id")? {
                            scratch.current_binding_site.id = Some(id);
                        }
                        scratch.current_binding_site.description = scratch.current_feature.description.clone();
                        scratch.current_binding_site.evidence_keys = scratch.current_feature.evidence_keys.clone();
                        State::FeatureBindingSite
                    }
                    "metal ion-binding site" => {
                        scratch.current_metal_coordination.clear();
                        scratch.current_feature_context = crate::pipeline::scratch::FeatureContext::MetalCoordination;
                        if let Some(id) = get_attribute(e, b"id")? {
                            scratch.current_metal_coordination.id = Some(id);
                        }
                        scratch.current_metal_coordination.description = scratch.current_feature.description.clone();
                        scratch.current_metal_coordination.evidence_keys = scratch.current_feature.evidence_keys.clone();
                        State::FeatureMetalCoordination
                    }
                    "mutagenesis site" => {
                        scratch.current_mutagenesis_site.clear();
                        scratch.current_feature_context = crate::pipeline::scratch::FeatureContext::Mutagenesis;
                        if let Some(id) = get_attribute(e, b"id")? {
                            scratch.current_mutagenesis_site.id = Some(id);
                        }
                        scratch.current_mutagenesis_site.description = scratch.current_feature.description.clone();
                        scratch.current_mutagenesis_site.evidence_keys = scratch.current_feature.evidence_keys.clone();
                        State::FeatureMutagenesis
                    }
                    "domain" => {
                        scratch.current_domain.clear();
                        scratch.current_feature_context = crate::pipeline::scratch::FeatureContext::Domain;
                        if let Some(id) = get_attribute(e, b"id")? {
                            scratch.current_domain.id = Some(id);
                        }
                        scratch.current_domain.description = scratch.current_feature.description.clone();
                        scratch.current_domain.evidence_keys = scratch.current_feature.evidence_keys.clone();
                        State::FeatureDomain
                    }
                    "sequence variant" => {
                        scratch.current_natural_variant.clear();
                        scratch.current_feature_context = crate::pipeline::scratch::FeatureContext::NaturalVariant;
                        if let Some(id) = get_attribute(e, b"id")? {
                            scratch.current_natural_variant.id = Some(id);
                        }
                        scratch.current_natural_variant.description = scratch.current_feature.description.clone();
                        scratch.current_natural_variant.evidence_keys = scratch.current_feature.evidence_keys.clone();
                        State::FeatureNaturalVariant
                    }
                    _ => {
                        // Generic feature handling for other types
                        scratch.current_feature_context = crate::pipeline::scratch::FeatureContext::Generic;
                        State::Feature
                    }
                });
            }
            if let Some(desc) = get_attribute(e, b"description")? {
                scratch.current_feature.description = Some(desc);
            }
            if let Some(ev) = get_attribute(e, b"evidence")? {
                scratch.current_feature.evidence_keys = parse_evidence_refs(&ev);
            }
            State::Feature
        }
        (State::Feature, b"original") => {
            scratch.text_buffer.clear();
            State::FeatureOriginal
        }
        (State::Feature, b"variation") => {
            scratch.text_buffer.clear();
            State::FeatureVariation
        }
        // Natural variants also have <original>/<variation> but must populate current_natural_variant
        (State::FeatureNaturalVariant, b"original") => {
            scratch.text_buffer.clear();
            State::FeatureOriginal
        }
        (State::FeatureNaturalVariant, b"variation") => {
            scratch.text_buffer.clear();
            State::FeatureVariation
        }
        (State::Feature, b"location") => State::FeatureLocation,
        // Location handling for new coordinate-based features
        (State::FeatureActiveSite, b"location") => State::FeatureLocation,
        (State::FeatureBindingSite, b"location") => State::FeatureLocation,
        (State::FeatureMetalCoordination, b"location") => State::FeatureLocation,
        (State::FeatureMutagenesis, b"location") => State::FeatureLocation,
        (State::FeatureDomain, b"location") => State::FeatureLocation,
        (State::FeatureNaturalVariant, b"location") => State::FeatureLocation,
        (State::FeatureLocation, b"position") => {
            if let Some(pos) = get_attribute(e, b"position")? {
                if let Ok(p) = pos.parse() {
                    apply_coordinate_to_feature(p, CoordinateType::Position, scratch.current_feature_context, scratch);
                }
            }
            State::FeaturePosition
        }
        (State::FeatureLocation, b"begin") => {
            if let Some(pos) = get_attribute(e, b"position")? {
                if let Ok(p) = pos.parse() {
                    apply_coordinate_to_feature(p, CoordinateType::Begin, scratch.current_feature_context, scratch);
                }
            }
            State::FeatureBegin
        }
        (State::FeatureLocation, b"end") => {
            if let Some(pos) = get_attribute(e, b"position")? {
                if let Ok(p) = pos.parse() {
                    apply_coordinate_to_feature(p, CoordinateType::End, scratch.current_feature_context, scratch);
                }
            }
            State::FeatureEnd
        }
        (State::Entry, b"comment") => {
            if let Some(ct) = get_attribute(e, b"type")? {
                match ct.as_str() {
                    "subcellular location" => State::CommentSubcellularLocation,
                    "alternative products" => State::CommentIsoform,
                    "subunit" => {
                        scratch.current_subunit.clear();
                        if let Some(ev) = get_attribute(e, b"evidence")? {
                            scratch.current_subunit.evidence_keys = parse_evidence_refs(&ev);
                        }
                        State::CommentSubunit
                    }
                    "interaction" => {
                        scratch.current_interaction.clear();
                        if let Some(ev) = get_attribute(e, b"evidence")? {
                            scratch.current_interaction.evidence_keys = parse_evidence_refs(&ev);
                        }
                        State::CommentInteraction
                    }
                    _ => State::Comment,
                }
            } else {
                State::Comment
            }
        }
        (State::CommentSubcellularLocation, b"subcellularLocation") => {
            scratch.current_location.clear();
            State::CommentSubcellularLocation
        }
        (State::CommentSubcellularLocation, b"location") => {
            if let Some(ev) = get_attribute(e, b"evidence")? {
                scratch.current_location.evidence_keys = parse_evidence_refs(&ev);
            }
            State::CommentLocation
        }
        // Subunit comment captures inner <text>
        (State::CommentSubunit, b"text") => {
            scratch.text_buffer.clear();
            // Prefer evidence on <text> if present; otherwise keep comment-level evidence
            if let Some(ev) = get_attribute(e, b"evidence")? {
                scratch.current_subunit.evidence_keys = parse_evidence_refs(&ev);
            }
            State::CommentSubunitText
        }
        // Interaction comment often contains <dbReference type="UniProtKB" id="..."/> under interactants
        (State::CommentInteraction, b"dbReference") => {
            if let Some(t) = get_attribute(e, b"type")? {
                if t.starts_with("UniProtKB") {
                    if let Some(id) = get_attribute(e, b"id")? {
                        if scratch.current_interaction.interactant_id_1.is_none() {
                            scratch.current_interaction.interactant_id_1 = Some(id);
                        } else if scratch.current_interaction.interactant_id_2.is_none() {
                            scratch.current_interaction.interactant_id_2 = Some(id);
                        } else {
                            // If more than 2 interactants appear, flush current pair and start a new one
                            let keep_ev = scratch.current_interaction.evidence_keys.clone();
                            scratch
                                .interactions
                                .push(std::mem::take(&mut scratch.current_interaction));
                            scratch.current_interaction.clear();
                            scratch.current_interaction.evidence_keys = keep_ev;
                            scratch.current_interaction.interactant_id_1 = Some(id);
                        }
                    }
                }
            }
            State::CommentInteraction
        }
        (State::CommentIsoform, b"isoform") => {
            scratch.current_isoform.clear();
            State::CommentIsoform
        }
        (State::CommentIsoform, b"id") => State::CommentIsoformId,
        (State::CommentIsoform, b"sequence") => {
            if let Some(ref_attr) = get_attribute(e, b"ref")? {
                scratch.current_isoform.isoform_sequence = Some(ref_attr);
            }

            // UniProt isoforms can list one or more "described" sequence refs (VSP_...)
            // which correspond to <feature type="splice variant" id="VSP_..."> edits.
            let seq_type = get_attribute(e, b"type")?.unwrap_or_default();
            if let Some(ref_attr) = scratch.current_isoform.isoform_sequence.as_deref() {
                if seq_type == "described" || ref_attr.starts_with("VSP_") {
                    scratch.current_isoform.vsp_ids.push(ref_attr.to_string());
                }
            }
            State::CommentIsoformSequence
        }
        (State::CommentIsoform, b"note") => State::CommentIsoformNote,
        (State::Entry, b"evidence") => {
            if let Some(key) = get_attribute(e, b"key")? {
                if let Some(eco) = get_attribute(e, b"type")? {
                    scratch.evidence_map.insert(key, eco);
                }
            }
            State::Evidence
        }
        _ => state,
    })
}

fn handle_empty_tag(
    e: &quick_xml::events::BytesStart<'_>,
    state: State,
    scratch: &mut EntryScratch,
) -> Result<()> {
    let tag_name = e.local_name();
    let tag = tag_name.as_ref();

    match (state, tag) {
        // Protein existence empty tag
        (State::Protein, b"proteinExistence") => {
            if let Some(t) = get_attribute(e, b"type")? {
                scratch.existence = map_existence(&t);
            }
        }
        // Structural hooks can be self-closing
        (State::Entry, b"dbReference") => {
            if let Some(db) = get_attribute(e, b"type")? {
                if db == "PDB" || db == "AlphaFoldDB" {
                    if let Some(id) = get_attribute(e, b"id")? {
                        scratch
                            .structures
                            .push(crate::pipeline::scratch::StructureRef { database: db, id });
                    }
                }
            }
        }
        (State::Organism, b"dbReference") => {
            if let Some(type_attr) = get_attribute(e, b"type")? {
                if type_attr == "NCBI Taxonomy" {
                    if let Some(id) = get_attribute(e, b"id")? {
                        scratch.organism_id = id.parse().ok();
                    }
                }
            }
        }
        (State::FeatureLocation, b"position") => {
            if let Some(pos) = get_attribute(e, b"position")? {
                if let Ok(p) = pos.parse() {
                    apply_coordinate_to_feature(p, CoordinateType::Position, scratch.current_feature_context, scratch);
                }
            }
        }
        (State::FeatureLocation, b"begin") => {
            if let Some(pos) = get_attribute(e, b"position")? {
                if let Ok(p) = pos.parse() {
                    apply_coordinate_to_feature(p, CoordinateType::Begin, scratch.current_feature_context, scratch);
                }
            }
        }
        (State::FeatureLocation, b"end") => {
            if let Some(pos) = get_attribute(e, b"position")? {
                if let Ok(p) = pos.parse() {
                    apply_coordinate_to_feature(p, CoordinateType::End, scratch.current_feature_context, scratch);
                }
            }
        }
        // UniProt isoform <sequence .../> tags in alternative-products comments are commonly
        // self-closing. We need to capture both:
        // - displayed refs (e.g. Q9...-2) for sidecar lookup
        // - described refs (e.g. VSP_...) to scope splice-variant edits per isoform
        (State::CommentIsoform, b"sequence") => {
            let seq_type = get_attribute(e, b"type")?.unwrap_or_default();
            if let Some(ref_attr) = get_attribute(e, b"ref")? {
                if seq_type == "described" || ref_attr.starts_with("VSP_") {
                    scratch.current_isoform.vsp_ids.push(ref_attr);
                } else {
                    // Keep the most useful non-VSP ref for FASTA sidecar lookup.
                    // Avoid overwriting an existing accession-like ref with something else.
                    if scratch.current_isoform.isoform_sequence.is_none()
                        || scratch
                            .current_isoform
                            .isoform_sequence
                            .as_deref()
                            .is_some_and(|s| s.starts_with("VSP_"))
                    {
                        scratch.current_isoform.isoform_sequence = Some(ref_attr);
                    }
                }
            }
        }
        (State::Entry, b"evidence") => {
            if let Some(key) = get_attribute(e, b"key")? {
                if let Some(eco) = get_attribute(e, b"type")? {
                    scratch.evidence_map.insert(key, eco);
                }
            }
        }
        // Capture interaction partners from self-closing dbReference tags
        (State::CommentInteraction, b"dbReference") => {
            if let Some(t) = get_attribute(e, b"type")? {
                if t.starts_with("UniProtKB") {
                    if let Some(id) = get_attribute(e, b"id")? {
                        if scratch.current_interaction.interactant_id_1.is_none() {
                            scratch.current_interaction.interactant_id_1 = Some(id);
                        } else if scratch.current_interaction.interactant_id_2.is_none() {
                            scratch.current_interaction.interactant_id_2 = Some(id);
                        } else {
                            let keep_ev = scratch.current_interaction.evidence_keys.clone();
                            scratch
                                .interactions
                                .push(std::mem::take(&mut scratch.current_interaction));
                            scratch.current_interaction.clear();
                            scratch.current_interaction.evidence_keys = keep_ev;
                            scratch.current_interaction.interactant_id_1 = Some(id);
                        }
                    }
                }
            }
        }
        _ => {}
    }

    Ok(())
}

fn handle_end_tag(
    e: &quick_xml::events::BytesEnd<'_>,
    state: State,
    scratch: &mut EntryScratch,
    batcher: &mut Batcher,
) -> Result<State> {
    let tag_name = e.local_name();
    let tag = tag_name.as_ref();

    Ok(match (state, tag) {
        (State::Entry, b"entry") => {
            batcher.add_entry(scratch)?;
            State::Root
        }
        (State::EntryName, b"name") => {
            scratch.entry_name = Some(std::mem::take(&mut scratch.text_buffer));
            State::Entry
        }
        (State::Accession, b"accession") => {
            if !scratch.has_primary_accession {
                scratch.accession = std::mem::take(&mut scratch.text_buffer);
                // First accession is the canonical anchor.
                scratch.parent_id = scratch.accession.clone();
                scratch.has_primary_accession = true;
            } else {
                scratch.text_buffer.clear();
            }
            State::Entry
        }
        (State::Sequence, b"sequence") => {
            // Remove whitespace from sequence
            scratch.sequence = scratch
                .text_buffer
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect();
            scratch.text_buffer.clear();
            State::Entry
        }
        // Organism scientific name capture
        (State::OrganismScientificName, b"name") => {
            scratch.organism_scientific_name = Some(std::mem::take(&mut scratch.text_buffer));
            State::Organism
        }
        (State::OrganismDbReference, b"dbReference") => State::Organism,
        (State::Organism, b"organism") => State::Entry,
        // Gene name capture
        (State::GeneName, b"name") => {
            scratch.gene_name = Some(std::mem::take(&mut scratch.text_buffer));
            State::Gene
        }
        (State::Gene, b"gene") => State::Entry,
        // Protein recommended full name capture and existence state
        (State::RecommendedName, b"fullName") => {
            scratch.protein_name = Some(std::mem::take(&mut scratch.text_buffer));
            State::RecommendedName
        }
        (State::RecommendedName, b"recommendedName") => State::Protein,
        (State::ProteinExistence, b"proteinExistence") => State::Protein,
        (State::Protein, b"protein") => State::Entry,
        (State::Feature, b"feature") => {
            scratch
                .features
                .push(std::mem::take(&mut scratch.current_feature));
            State::Entry
        }
        (State::FeatureOriginal, b"original") => {
            if scratch.current_feature_context == crate::pipeline::scratch::FeatureContext::NaturalVariant {
                scratch.current_natural_variant.original = Some(std::mem::take(&mut scratch.text_buffer));
                State::FeatureNaturalVariant
            } else {
                scratch.current_feature.original = Some(std::mem::take(&mut scratch.text_buffer));
                State::Feature
            }
        }
        (State::FeatureVariation, b"variation") => {
            if scratch.current_feature_context == crate::pipeline::scratch::FeatureContext::NaturalVariant {
                scratch.current_natural_variant.variation = Some(std::mem::take(&mut scratch.text_buffer));
                State::FeatureNaturalVariant
            } else {
                scratch.current_feature.variation = Some(std::mem::take(&mut scratch.text_buffer));
                State::Feature
            }
        }
        (State::FeaturePosition, b"position") => State::FeatureLocation,
        (State::FeatureBegin, b"begin") => State::FeatureLocation,
        (State::FeatureEnd, b"end") => State::FeatureLocation,
        // Finalize coordinate-based features and reset context
        (State::FeatureActiveSite, b"feature") => {
            scratch
                .active_sites
                .push(std::mem::take(&mut scratch.current_active_site));
            scratch
                .features
                .push(std::mem::take(&mut scratch.current_feature));
            scratch.current_feature_context = crate::pipeline::scratch::FeatureContext::Generic;
            State::Entry
        }
        (State::FeatureBindingSite, b"feature") => {
            scratch
                .binding_sites
                .push(std::mem::take(&mut scratch.current_binding_site));
            scratch
                .features
                .push(std::mem::take(&mut scratch.current_feature));
            scratch.current_feature_context = crate::pipeline::scratch::FeatureContext::Generic;
            State::Entry
        }
        (State::FeatureMetalCoordination, b"feature") => {
            scratch
                .metal_coordinations
                .push(std::mem::take(&mut scratch.current_metal_coordination));
            scratch
                .features
                .push(std::mem::take(&mut scratch.current_feature));
            scratch.current_feature_context = crate::pipeline::scratch::FeatureContext::Generic;
            State::Entry
        }
        (State::FeatureMutagenesis, b"feature") => {
            scratch
                .mutagenesis_sites
                .push(std::mem::take(&mut scratch.current_mutagenesis_site));
            scratch
                .features
                .push(std::mem::take(&mut scratch.current_feature));
            scratch.current_feature_context = crate::pipeline::scratch::FeatureContext::Generic;
            State::Entry
        }
        (State::FeatureDomain, b"feature") => {
            scratch
                .domains
                .push(std::mem::take(&mut scratch.current_domain));
            scratch
                .features
                .push(std::mem::take(&mut scratch.current_feature));
            scratch.current_feature_context = crate::pipeline::scratch::FeatureContext::Generic;
            State::Entry
        }
        (State::FeatureNaturalVariant, b"feature") => {
            scratch
                .natural_variants
                .push(std::mem::take(&mut scratch.current_natural_variant));
            scratch
                .features
                .push(std::mem::take(&mut scratch.current_feature));
            scratch.current_feature_context = crate::pipeline::scratch::FeatureContext::Generic;
            State::Entry
        }
        (State::FeatureLocation, b"location") => {
            match scratch.current_feature_context {
                crate::pipeline::scratch::FeatureContext::ActiveSite => State::FeatureActiveSite,
                crate::pipeline::scratch::FeatureContext::BindingSite => State::FeatureBindingSite,
                crate::pipeline::scratch::FeatureContext::MetalCoordination => State::FeatureMetalCoordination,
                crate::pipeline::scratch::FeatureContext::Mutagenesis => State::FeatureMutagenesis,
                crate::pipeline::scratch::FeatureContext::Domain => State::FeatureDomain,
                crate::pipeline::scratch::FeatureContext::NaturalVariant => State::FeatureNaturalVariant,
                crate::pipeline::scratch::FeatureContext::Generic => State::Feature,
            }
        }
        // Finalize comment-based features
        (State::CommentSubunitText, b"text") => {
            scratch.current_subunit.text = std::mem::take(&mut scratch.text_buffer);
            State::CommentSubunit
        }
        (State::CommentSubunit, b"comment") => {
            if !scratch.current_subunit.text.trim().is_empty() {
                scratch
                    .subunits
                    .push(std::mem::take(&mut scratch.current_subunit));
            }
            State::Entry
        }
        (State::CommentInteraction, b"comment") => {
            if scratch.current_interaction.interactant_id_1.is_some()
                || scratch.current_interaction.interactant_id_2.is_some()
            {
                scratch
                    .interactions
                    .push(std::mem::take(&mut scratch.current_interaction));
            }
            State::Entry
        }
        (State::CommentLocation, b"location") => {
            scratch.current_location.location = std::mem::take(&mut scratch.text_buffer);
            State::CommentSubcellularLocation
        }
        (State::CommentSubcellularLocation, b"subcellularLocation") => {
            scratch
                .locations
                .push(std::mem::take(&mut scratch.current_location));
            State::CommentSubcellularLocation
        }
        (State::CommentSubcellularLocation, b"comment") => State::Entry,
        (State::Comment, b"comment") => State::Entry,
        (State::CommentIsoformId, b"id") => {
            scratch.current_isoform.isoform_id = std::mem::take(&mut scratch.text_buffer);
            State::CommentIsoform
        }
        (State::CommentIsoformSequence, b"sequence") => State::CommentIsoform,
        (State::CommentIsoformNote, b"note") => {
            scratch.current_isoform.isoform_note = Some(std::mem::take(&mut scratch.text_buffer));
            State::CommentIsoform
        }
        (State::CommentIsoform, b"isoform") => {
            scratch
                .isoforms
                .push(std::mem::take(&mut scratch.current_isoform));
            State::CommentIsoform
        }
        (State::CommentIsoform, b"comment") => State::Entry,
        (State::Evidence, b"evidence") => State::Entry,
        _ => state,
    })
}

/// Applies position coordinate to the appropriate feature buffer based on feature context
fn apply_coordinate_to_feature(
    pos: i32,
    coord_type: CoordinateType,
    context: crate::pipeline::scratch::FeatureContext,
    scratch: &mut EntryScratch,
) {
    let apply_to_generic = |scratch: &mut EntryScratch| {
        match coord_type {
            CoordinateType::Position => {
                scratch.current_feature.start = Some(pos);
                scratch.current_feature.end = Some(pos);
            }
            CoordinateType::Begin => {
                scratch.current_feature.start = Some(pos);
            }
            CoordinateType::End => {
                scratch.current_feature.end = Some(pos);
            }
        }
    };

    match context {
        crate::pipeline::scratch::FeatureContext::ActiveSite => {
            apply_to_generic(scratch);
            match coord_type {
            CoordinateType::Position => {
                scratch.current_active_site.start = Some(pos);
                scratch.current_active_site.end = Some(pos);
            }
            CoordinateType::Begin => {
                scratch.current_active_site.start = Some(pos);
            }
            CoordinateType::End => {
                scratch.current_active_site.end = Some(pos);
            }
            }
        }
        crate::pipeline::scratch::FeatureContext::BindingSite => {
            apply_to_generic(scratch);
            match coord_type {
            CoordinateType::Position => {
                scratch.current_binding_site.start = Some(pos);
                scratch.current_binding_site.end = Some(pos);
            }
            CoordinateType::Begin => {
                scratch.current_binding_site.start = Some(pos);
            }
            CoordinateType::End => {
                scratch.current_binding_site.end = Some(pos);
            }
            }
        }
        crate::pipeline::scratch::FeatureContext::MetalCoordination => {
            apply_to_generic(scratch);
            match coord_type {
            CoordinateType::Position => {
                scratch.current_metal_coordination.start = Some(pos);
                scratch.current_metal_coordination.end = Some(pos);
            }
            CoordinateType::Begin => {
                scratch.current_metal_coordination.start = Some(pos);
            }
            CoordinateType::End => {
                scratch.current_metal_coordination.end = Some(pos);
            }
            }
        }
        crate::pipeline::scratch::FeatureContext::Mutagenesis => {
            apply_to_generic(scratch);
            match coord_type {
            CoordinateType::Position => {
                scratch.current_mutagenesis_site.start = Some(pos);
                scratch.current_mutagenesis_site.end = Some(pos);
            }
            CoordinateType::Begin => {
                scratch.current_mutagenesis_site.start = Some(pos);
            }
            CoordinateType::End => {
                scratch.current_mutagenesis_site.end = Some(pos);
            }
            }
        }
        crate::pipeline::scratch::FeatureContext::Domain => {
            apply_to_generic(scratch);
            match coord_type {
            CoordinateType::Position => {
                scratch.current_domain.start = Some(pos);
                scratch.current_domain.end = Some(pos);
            }
            CoordinateType::Begin => {
                scratch.current_domain.start = Some(pos);
            }
            CoordinateType::End => {
                scratch.current_domain.end = Some(pos);
            }
            }
        }
        crate::pipeline::scratch::FeatureContext::NaturalVariant => {
            apply_to_generic(scratch);
            match coord_type {
            CoordinateType::Position => {
                scratch.current_natural_variant.start = Some(pos);
                scratch.current_natural_variant.end = Some(pos);
            }
            CoordinateType::Begin => {
                scratch.current_natural_variant.start = Some(pos);
            }
            CoordinateType::End => {
                scratch.current_natural_variant.end = Some(pos);
            }
            }
        }
        crate::pipeline::scratch::FeatureContext::Generic => {
            // Fall back to generic feature scratch for non-specialized types
            match coord_type {
                CoordinateType::Position => {
                    scratch.current_feature.start = Some(pos);
                    scratch.current_feature.end = Some(pos);
                }
                CoordinateType::Begin => {
                    scratch.current_feature.start = Some(pos);
                }
                CoordinateType::End => {
                    scratch.current_feature.end = Some(pos);
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum CoordinateType {
    Position,
    Begin,
    End,
}

/// Extracts an attribute value as a String
fn get_attribute(e: &quick_xml::events::BytesStart<'_>, name: &[u8]) -> Result<Option<String>> {
    for attr in e.attributes().flatten() {
        if attr.key.as_ref() == name {
            return Ok(Some(attr.unescape_value()?.into_owned()));
        }
    }
    Ok(None)
}

/// Parses space-separated evidence references into a Vec
fn parse_evidence_refs(refs: &str) -> Vec<String> {
    refs.split_whitespace().map(String::from).collect()
}

/// Maps UniProt proteinExistence type strings to i8 codes
fn map_existence(t: &str) -> i8 {
    match t {
        "evidence at protein level" => 1,
        "evidence at transcript level" => 2,
        "inferred from homology" => 3,
        "predicted" => 4,
        "uncertain" => 5,
        _ => 0,
    }
}
