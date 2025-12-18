use flate2::read::GzDecoder;
use quick_xml::Reader;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use crate::config::Settings;
use crate::error::Result;

pub type XmlReader<R> = Reader<R>;

/// Creates an XML reader from a file path.
/// Automatically detects .gz files and applies gzip decompression.
/// Uses buffer size from Settings.
pub fn create_xml_reader(path: &Path, settings: &Settings) -> Result<XmlReader<Box<dyn BufRead>>> {
    let file = File::open(path)?;
    let buf_size = settings.performance.buffer_size;

    let reader: Box<dyn BufRead> = if path.extension().map_or(false, |ext| ext == "gz") {
        // Gzipped file: File -> GzDecoder -> BufReader
        let decoder = GzDecoder::new(file);
        Box::new(BufReader::with_capacity(buf_size, decoder))
    } else {
        // Plain XML: File -> BufReader
        Box::new(BufReader::with_capacity(buf_size, file))
    };

    let mut xml_reader = Reader::from_reader(reader);
    xml_reader.config_mut().trim_text(true);

    Ok(xml_reader)
}
