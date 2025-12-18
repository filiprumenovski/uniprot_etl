use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;
use std::io::BufRead;

use crate::error::Result;
use crate::pipeline::handlers::{comments, features, get_attribute, read_text, skip_element};
use crate::pipeline::scratch::EntryScratch;

pub fn consume_entry<R: BufRead>(
    reader: &mut Reader<R>,
    scratch: &mut EntryScratch,
    buf: &mut Vec<u8>,
) -> Result<()> {
    let mut inner_buf = Vec::new();
    loop {
        buf.clear();
        match reader.read_event_into(buf)? {
            Event::Start(e) => match e.local_name().as_ref() {
                b"name" => handle_entry_name(reader, scratch, &mut inner_buf)?,
                b"accession" => handle_accession(reader, scratch, &mut inner_buf)?,
                b"sequence" => handle_sequence(reader, scratch, &mut inner_buf)?,
                b"organism" => consume_organism(reader, scratch, &mut inner_buf)?,
                b"gene" => consume_gene(reader, scratch, &mut inner_buf)?,
                b"protein" => consume_protein(reader, scratch, &mut inner_buf)?,
                b"dbReference" => handle_entry_db_reference(&e, scratch)?,
                b"feature" => features::consume_feature(reader, &e, scratch, &mut inner_buf)?,
                b"comment" => comments::consume_comment(reader, &e, scratch, &mut inner_buf)?,
                b"evidence" => handle_evidence(&e, scratch)?,
                _ => skip_element(reader, e.local_name().as_ref(), &mut inner_buf)?,
            },
            Event::Empty(e) => match e.local_name().as_ref() {
                b"dbReference" => handle_entry_db_reference(&e, scratch)?,
                b"evidence" => handle_evidence(&e, scratch)?,
                _ => {}
            },
            Event::End(e) if e.local_name().as_ref() == b"entry" => break,
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(())
}

fn handle_entry_name<R: BufRead>(
    reader: &mut Reader<R>,
    scratch: &mut EntryScratch,
    _buf: &mut Vec<u8>,
) -> Result<()> {
    let mut inner = Vec::new();
    let name = read_text(reader, b"name", &mut inner)?;
    scratch.entry.entry_name = Some(name);
    Ok(())
}

fn handle_accession<R: BufRead>(
    reader: &mut Reader<R>,
    scratch: &mut EntryScratch,
    _buf: &mut Vec<u8>,
) -> Result<()> {
    let mut inner = Vec::new();
    let accession = read_text(reader, b"accession", &mut inner)?;
    if !scratch.has_primary_accession {
        scratch.entry.accession = accession.clone();
        scratch.entry.parent_id = accession;
        scratch.has_primary_accession = true;
    }
    Ok(())
}

fn handle_sequence<R: BufRead>(
    reader: &mut Reader<R>,
    scratch: &mut EntryScratch,
    _buf: &mut Vec<u8>,
) -> Result<()> {
    let mut inner = Vec::new();
    let sequence_raw = read_text(reader, b"sequence", &mut inner)?;
    scratch.entry.sequence = sequence_raw.chars().filter(|c| !c.is_whitespace()).collect();
    Ok(())
}

fn consume_organism<R: BufRead>(
    reader: &mut Reader<R>,
    scratch: &mut EntryScratch,
    buf: &mut Vec<u8>,
) -> Result<()> {
    let mut inner = Vec::new();
    loop {
        buf.clear();
        match reader.read_event_into(buf)? {
            Event::Start(e) => match e.local_name().as_ref() {
                b"name" => {
                    if let Some(t) = get_attribute(&e, b"type")? {
                        if t == "scientific" {
                            let name = read_text(reader, b"name", &mut inner)?;
                            scratch.entry.organism_scientific_name = Some(name);
                        } else {
                            skip_element(reader, b"name", &mut inner)?;
                        }
                    } else {
                        skip_element(reader, b"name", &mut inner)?;
                    }
                }
                b"dbReference" => {
                    handle_organism_db_reference(&e, scratch)?;
                    skip_element(reader, b"dbReference", &mut inner)?;
                }
                _ => skip_element(reader, e.local_name().as_ref(), &mut inner)?,
            },
            Event::Empty(e) => {
                if e.local_name().as_ref() == b"dbReference" {
                    handle_organism_db_reference(&e, scratch)?;
                }
            }
            Event::End(e) if e.local_name().as_ref() == b"organism" => break,
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(())
}

fn handle_organism_db_reference(e: &BytesStart<'_>, scratch: &mut EntryScratch) -> Result<()> {
    if let Some(type_attr) = get_attribute(e, b"type")? {
        if type_attr == "NCBI Taxonomy" {
            if let Some(id) = get_attribute(e, b"id")? {
                scratch.entry.organism_id = id.parse().ok();
            }
        }
    }
    Ok(())
}

fn consume_gene<R: BufRead>(
    reader: &mut Reader<R>,
    scratch: &mut EntryScratch,
    buf: &mut Vec<u8>,
) -> Result<()> {
    let mut inner = Vec::new();
    loop {
        buf.clear();
        match reader.read_event_into(buf)? {
            Event::Start(e) if e.local_name().as_ref() == b"name" => {
                if let Some(t) = get_attribute(&e, b"type")? {
                    if t == "primary" {
                        let text = read_text(reader, b"name", &mut inner)?;
                        scratch.entry.gene_name = Some(text);
                    } else {
                        skip_element(reader, b"name", &mut inner)?;
                    }
                } else {
                    skip_element(reader, b"name", &mut inner)?;
                }
            }
            Event::End(e) if e.local_name().as_ref() == b"gene" => break,
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(())
}

fn consume_protein<R: BufRead>(
    reader: &mut Reader<R>,
    scratch: &mut EntryScratch,
    buf: &mut Vec<u8>,
) -> Result<()> {
    let mut inner = Vec::new();
    loop {
        buf.clear();
        match reader.read_event_into(buf)? {
            Event::Start(e) => match e.local_name().as_ref() {
                b"recommendedName" => consume_recommended_name(reader, scratch, &mut inner)?,
                b"proteinExistence" => {
                    handle_protein_existence(&e, scratch)?;
                    skip_element(reader, b"proteinExistence", &mut inner)?;
                }
                _ => skip_element(reader, e.local_name().as_ref(), &mut inner)?,
            },
            Event::Empty(e) if e.local_name().as_ref() == b"proteinExistence" => {
                handle_protein_existence(&e, scratch)?;
            }
            Event::End(e) if e.local_name().as_ref() == b"protein" => break,
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(())
}

fn consume_recommended_name<R: BufRead>(
    reader: &mut Reader<R>,
    scratch: &mut EntryScratch,
    buf: &mut Vec<u8>,
) -> Result<()> {
    let mut inner = Vec::new();
    loop {
        buf.clear();
        match reader.read_event_into(buf)? {
            Event::Start(e) if e.local_name().as_ref() == b"fullName" => {
                let text = read_text(reader, b"fullName", &mut inner)?;
                scratch.entry.protein_name = Some(text);
            }
            Event::End(e) if e.local_name().as_ref() == b"recommendedName" => break,
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(())
}

fn handle_protein_existence(e: &BytesStart<'_>, scratch: &mut EntryScratch) -> Result<()> {
    if let Some(t) = get_attribute(e, b"type")? {
        scratch.entry.existence = map_existence(&t);
    }
    Ok(())
}

fn handle_entry_db_reference(e: &BytesStart<'_>, scratch: &mut EntryScratch) -> Result<()> {
    if let Some(db) = get_attribute(e, b"type")? {
        if db == "PDB" || db == "AlphaFoldDB" {
            if let Some(id) = get_attribute(e, b"id")? {
                scratch.entry.structures.push(crate::pipeline::scratch::StructureRef {
                    database: db,
                    id,
                });
            }
        }
    }
    Ok(())
}

fn handle_evidence(e: &BytesStart<'_>, scratch: &mut EntryScratch) -> Result<()> {
    if let Some(key) = get_attribute(e, b"key")? {
        if let Some(eco) = get_attribute(e, b"type")? {
            scratch.entry.evidence_map.insert(key, eco);
        }
    }
    Ok(())
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
