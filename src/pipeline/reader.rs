use flate2::read::GzDecoder;
use quick_xml::Reader;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

use crate::config::Settings;
use crate::error::Result;
use crate::metrics::Metrics;

pub type XmlReader<R> = Reader<R>;

/// A wrapper reader that tracks bytes consumed for metrics.
pub struct TrackedReader<R> {
    inner: R,
    metrics: Metrics,
}

impl<R> TrackedReader<R> {
    pub fn new(inner: R, metrics: Metrics) -> Self {
        Self { inner, metrics }
    }
}

impl<R: Read> Read for TrackedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let bytes = self.inner.read(buf)?;
        self.metrics.add_bytes_read(bytes as u64);
        Ok(bytes)
    }
}

impl<R: BufRead> BufRead for TrackedReader<R> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.inner.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.metrics.add_bytes_read(amt as u64);
        self.inner.consume(amt);
    }
}

/// Creates an XML reader from a file path.
/// Automatically detects .gz files and applies gzip decompression.
/// Uses buffer size from Settings.
/// Tracks bytes read via the provided Metrics.
pub fn create_xml_reader(
    path: &Path,
    settings: &Settings,
    metrics: &Metrics,
) -> Result<XmlReader<TrackedReader<Box<dyn BufRead + Send>>>> {
    let file = File::open(path)?;
    let buf_size = settings.performance.buffer_size;

    let reader: Box<dyn BufRead + Send> = if path.extension().is_some_and(|ext| ext == "gz") {
        // Gzipped file: File -> GzDecoder -> BufReader
        let decoder = GzDecoder::new(file);
        Box::new(BufReader::with_capacity(buf_size, decoder))
    } else {
        // Plain XML: File -> BufReader
        Box::new(BufReader::with_capacity(buf_size, file))
    };

    let tracked_reader = TrackedReader::new(reader, metrics.clone());

    let mut xml_reader = Reader::from_reader(tracked_reader);
    xml_reader.config_mut().trim_text(true);

    Ok(xml_reader)
}
