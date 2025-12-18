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

/// Parses UniProt XML entries and sends RecordBatches to the channel.
pub fn parse_entries<R: BufRead>(
    mut reader: Reader<R>,
    sender: Sender<RecordBatch>,
    metrics: &Metrics,
    batch_size: usize,
) -> Result<()> {
    let mut batcher = Batcher::with_batch_size(sender, metrics.clone(), batch_size);
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
            if let Some(ft) = get_attribute(e, b"type")? {
                scratch.current_feature.feature_type = ft;
            }
            if let Some(desc) = get_attribute(e, b"description")? {
                scratch.current_feature.description = Some(desc);
            }
            if let Some(ev) = get_attribute(e, b"evidence")? {
                scratch.current_feature.evidence_keys = parse_evidence_refs(&ev);
            }
            State::Feature
        }
        (State::Feature, b"location") => State::FeatureLocation,
        (State::FeatureLocation, b"position") => {
            if let Some(pos) = get_attribute(e, b"position")? {
                if let Ok(p) = pos.parse() {
                    scratch.current_feature.start = Some(p);
                    scratch.current_feature.end = Some(p);
                }
            }
            State::FeaturePosition
        }
        (State::FeatureLocation, b"begin") => {
            if let Some(pos) = get_attribute(e, b"position")? {
                scratch.current_feature.start = pos.parse().ok();
            }
            State::FeatureBegin
        }
        (State::FeatureLocation, b"end") => {
            if let Some(pos) = get_attribute(e, b"position")? {
                scratch.current_feature.end = pos.parse().ok();
            }
            State::FeatureEnd
        }
        (State::Entry, b"comment") => {
            if let Some(ct) = get_attribute(e, b"type")? {
                match ct.as_str() {
                    "subcellular location" => State::CommentSubcellularLocation,
                    "alternative products" => State::CommentIsoform,
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
        (State::CommentIsoform, b"isoform") => {
            scratch.current_isoform.clear();
            State::CommentIsoform
        }
        (State::CommentIsoform, b"id") => State::CommentIsoformId,
        (State::CommentIsoform, b"sequence") => {
            if let Some(ref_attr) = get_attribute(e, b"ref")? {
                scratch.current_isoform.isoform_sequence = Some(ref_attr);
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
                    scratch.current_feature.start = Some(p);
                    scratch.current_feature.end = Some(p);
                }
            }
        }
        (State::FeatureLocation, b"begin") => {
            if let Some(pos) = get_attribute(e, b"position")? {
                scratch.current_feature.start = pos.parse().ok();
            }
        }
        (State::FeatureLocation, b"end") => {
            if let Some(pos) = get_attribute(e, b"position")? {
                scratch.current_feature.end = pos.parse().ok();
            }
        }
        (State::Entry, b"evidence") => {
            if let Some(key) = get_attribute(e, b"key")? {
                if let Some(eco) = get_attribute(e, b"type")? {
                    scratch.evidence_map.insert(key, eco);
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
        (State::FeaturePosition, b"position") => State::FeatureLocation,
        (State::FeatureBegin, b"begin") => State::FeatureLocation,
        (State::FeatureEnd, b"end") => State::FeatureLocation,
        (State::FeatureLocation, b"location") => State::Feature,
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
