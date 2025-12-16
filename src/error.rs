use thiserror::Error;

#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum EtlError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("XML parsing error: {0}")]
    Xml(#[from] quick_xml::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Channel send error")]
    ChannelSend,

    #[error("Invalid UTF-8 in XML: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("Invalid XML attribute: {0}")]
    InvalidAttribute(String),
}

pub type Result<T> = std::result::Result<T, EtlError>;
