use quick_xml::events::{BytesStart, Event};
use quick_xml::name::QName;
use quick_xml::Reader;
use std::io::BufRead;

use crate::error::Result;

pub mod comments;
pub mod features;
pub mod metadata;

/// Extracts an attribute value as a String
pub fn get_attribute(e: &BytesStart<'_>, name: &[u8]) -> Result<Option<String>> {
    for attr in e.attributes().flatten() {
        if attr.key.as_ref() == name {
            return Ok(Some(attr.unescape_value()?.into_owned()));
        }
    }
    Ok(None)
}

/// Parses space-separated evidence references into a Vec
pub fn parse_evidence_refs(refs: &str) -> Vec<String> {
    refs.split_whitespace().map(String::from).collect()
}

/// Reads text content until the matching end tag is reached.
pub fn read_text<R: BufRead>(
    reader: &mut Reader<R>,
    end_tag: &[u8],
    buf: &mut Vec<u8>,
) -> Result<String> {
    let mut text = String::new();
    loop {
        buf.clear();
        match reader.read_event_into(buf)? {
            Event::Text(e) => text.push_str(&e.unescape()?),
            Event::CData(e) => text.push_str(&String::from_utf8_lossy(e.as_ref())),
            Event::End(e) if e.local_name().as_ref() == end_tag => break,
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(text)
}

/// Skips the current element, consuming events until its end tag.
pub fn skip_element<R: BufRead>(
    reader: &mut Reader<R>,
    tag: &[u8],
    buf: &mut Vec<u8>,
) -> Result<()> {
    reader.read_to_end_into(QName(tag), buf)?;
    Ok(())
}
