use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;
use std::io::BufRead;

use crate::error::Result;
use crate::pipeline::handlers::{get_attribute, parse_evidence_refs, read_text, skip_element};
use crate::pipeline::scratch::EntryScratch;

pub fn consume_comment<R: BufRead>(
    reader: &mut Reader<R>,
    start: &BytesStart<'_>,
    scratch: &mut EntryScratch,
    buf: &mut Vec<u8>,
) -> Result<()> {
    let comment_type = get_attribute(start, b"type")?.unwrap_or_default();
    match comment_type.as_str() {
        "subcellular location" => consume_subcellular_location_comment(reader, scratch, buf),
        "alternative products" => consume_isoform_comment(reader, scratch, buf),
        "subunit" => consume_subunit_comment(reader, start, scratch, buf),
        "interaction" => consume_interaction_comment(reader, start, scratch, buf),
        _ => skip_element(reader, b"comment", buf),
    }
}

fn consume_subcellular_location_comment<R: BufRead>(
    reader: &mut Reader<R>,
    scratch: &mut EntryScratch,
    buf: &mut Vec<u8>,
) -> Result<()> {
    let mut inner = Vec::new();
    loop {
        buf.clear();
        match reader.read_event_into(buf)? {
            Event::Start(e) if e.local_name().as_ref() == b"subcellularLocation" => {
                scratch.current_location.clear();
            }
            Event::Start(e) if e.local_name().as_ref() == b"location" => {
                if let Some(ev) = get_attribute(&e, b"evidence")? {
                    scratch.current_location.evidence_keys = parse_evidence_refs(&ev);
                }
                let text = read_text(reader, b"location", &mut inner)?;
                scratch.current_location.location = text;
                scratch
                    .entry
                    .comments
                    .locations
                    .push(std::mem::take(&mut scratch.current_location));
            }
            Event::End(e) if e.local_name().as_ref() == b"comment" => return Ok(()),
            Event::Eof => return Ok(()),
            _ => {}
        }
    }
}

fn consume_isoform_comment<R: BufRead>(
    reader: &mut Reader<R>,
    scratch: &mut EntryScratch,
    buf: &mut Vec<u8>,
) -> Result<()> {
    let mut inner = Vec::new();
    loop {
        buf.clear();
        match reader.read_event_into(buf)? {
            Event::Start(e) if e.local_name().as_ref() == b"isoform" => {
                scratch.current_isoform.clear();
                consume_isoform(reader, scratch, &mut inner)?;
            }
            Event::End(e) if e.local_name().as_ref() == b"comment" => return Ok(()),
            Event::Eof => return Ok(()),
            _ => {}
        }
    }
}

fn consume_isoform<R: BufRead>(
    reader: &mut Reader<R>,
    scratch: &mut EntryScratch,
    buf: &mut Vec<u8>,
) -> Result<()> {
    let mut inner = Vec::new();
    loop {
        buf.clear();
        match reader.read_event_into(buf)? {
            Event::Start(e) => match e.local_name().as_ref() {
                b"id" => {
                    let id = read_text(reader, b"id", &mut inner)?;
                    scratch.current_isoform.isoform_id = id;
                }
                b"sequence" => {
                    capture_isoform_sequence(&e, scratch)?;
                    skip_element(reader, b"sequence", &mut inner)?;
                }
                b"note" => {
                    let note = read_text(reader, b"note", &mut inner)?;
                    scratch.current_isoform.isoform_note = Some(note);
                }
                _ => skip_element(reader, e.local_name().as_ref(), &mut inner)?,
            },
            Event::Empty(e) if e.local_name().as_ref() == b"sequence" => {
                capture_isoform_sequence(&e, scratch)?;
            }
            Event::End(e) if e.local_name().as_ref() == b"isoform" => {
                scratch
                    .entry
                    .isoforms
                    .push(std::mem::take(&mut scratch.current_isoform));
                return Ok(());
            }
            Event::Eof => return Ok(()),
            _ => {}
        }
    }
}

fn capture_isoform_sequence(e: &BytesStart<'_>, scratch: &mut EntryScratch) -> Result<()> {
    let seq_type = get_attribute(e, b"type")?.unwrap_or_default();
    if let Some(ref_attr) = get_attribute(e, b"ref")? {
        if seq_type == "described" || ref_attr.starts_with("VSP_") {
            scratch.current_isoform.vsp_ids.push(ref_attr);
        } else if scratch.current_isoform.isoform_sequence.is_none()
            || scratch
                .current_isoform
                .isoform_sequence
                .as_deref()
                .is_some_and(|s| s.starts_with("VSP_"))
        {
            scratch.current_isoform.isoform_sequence = Some(ref_attr);
        }
    }
    Ok(())
}

fn consume_subunit_comment<R: BufRead>(
    reader: &mut Reader<R>,
    start: &BytesStart<'_>,
    scratch: &mut EntryScratch,
    buf: &mut Vec<u8>,
) -> Result<()> {
    let mut inner = Vec::new();
    scratch.current_subunit.clear();
    if let Some(ev) = get_attribute(start, b"evidence")? {
        scratch.current_subunit.evidence_keys = parse_evidence_refs(&ev);
    }

    loop {
        buf.clear();
        match reader.read_event_into(buf)? {
            Event::Start(e) if e.local_name().as_ref() == b"text" => {
                if let Some(ev) = get_attribute(&e, b"evidence")? {
                    scratch.current_subunit.evidence_keys = parse_evidence_refs(&ev);
                }
                let text = read_text(reader, b"text", &mut inner)?;
                scratch.current_subunit.text = text;
            }
            Event::End(e) if e.local_name().as_ref() == b"comment" => {
                if !scratch.current_subunit.text.trim().is_empty() {
                    scratch
                        .entry
                        .comments
                        .subunits
                        .push(std::mem::take(&mut scratch.current_subunit));
                }
                return Ok(());
            }
            Event::Eof => return Ok(()),
            _ => {}
        }
    }
}

fn consume_interaction_comment<R: BufRead>(
    reader: &mut Reader<R>,
    start: &BytesStart<'_>,
    scratch: &mut EntryScratch,
    buf: &mut Vec<u8>,
) -> Result<()> {
    let mut inner = Vec::new();
    scratch.current_interaction.clear();
    if let Some(ev) = get_attribute(start, b"evidence")? {
        scratch.current_interaction.evidence_keys = parse_evidence_refs(&ev);
    }

    loop {
        buf.clear();
        match reader.read_event_into(buf)? {
            Event::Start(e) => {
                if e.local_name().as_ref() == b"dbReference" {
                    handle_interactant(&e, scratch)?;
                    skip_element(reader, b"dbReference", &mut inner)?;
                }
            }
            Event::Empty(e) if e.local_name().as_ref() == b"dbReference" => {
                handle_interactant(&e, scratch)?;
            }
            Event::End(e) if e.local_name().as_ref() == b"comment" => {
                if scratch.current_interaction.interactant_id_1.is_some()
                    || scratch.current_interaction.interactant_id_2.is_some()
                {
                    scratch
                        .entry
                        .comments
                        .interactions
                        .push(std::mem::take(&mut scratch.current_interaction));
                }
                return Ok(());
            }
            Event::Eof => return Ok(()),
            _ => {}
        }
    }
}

fn handle_interactant(e: &BytesStart<'_>, scratch: &mut EntryScratch) -> Result<()> {
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
                        .entry
                        .comments
                        .interactions
                        .push(std::mem::take(&mut scratch.current_interaction));
                    scratch.current_interaction.clear();
                    scratch.current_interaction.evidence_keys = keep_ev;
                    scratch.current_interaction.interactant_id_1 = Some(id);
                }
            }
        }
    }
    Ok(())
}
