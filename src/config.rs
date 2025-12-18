use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

/// Root configuration structure with versioning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    /// Configuration schema version for compatibility tracking
    pub version: String,
    /// Storage paths and directories
    pub storage: StorageConfig,
    /// Performance tuning parameters
    pub performance: PerformanceConfig,
    /// Logging configuration
    pub logging: LoggingConfig,
}

/// Storage configuration section
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Path to input UniProt XML file (supports .xml and .xml.gz)
    /// Can be relative to root or absolute
    pub input_path: Option<PathBuf>,
    /// Path to output Parquet file
    #[serde(default = "default_output_path")]
    pub output_path: PathBuf,
    /// Temporary directory for intermediate files
    #[serde(default = "default_temp_dir")]
    pub temp_dir: PathBuf,
}

/// Performance tuning configuration section
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Number of entries per RecordBatch
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Number of parser threads (currently unused, reserved for future)
    #[serde(default = "default_thread_count")]
    pub thread_count: usize,
    /// Channel capacity for bounded channel (number of batches in flight)
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
    /// Zstd compression level (1-22, recommended 1-10)
    #[serde(default = "default_zstd_level")]
    pub zstd_level: u32,
    /// Max row group size in Parquet
    #[serde(default = "default_max_row_group_size")]
    pub max_row_group_size: usize,
    /// Buffer size for reading XML (bytes)
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,
}

/// Logging configuration section
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level (debug, info, warn, error)
    #[serde(default = "default_log_level")]
    pub log_level: String,
    /// Metrics reporting interval in seconds
    #[serde(default = "default_metrics_interval")]
    pub metrics_interval_secs: u64,
}

// Default value functions
fn default_output_path() -> PathBuf {
    PathBuf::from("data/parquet/uniprot.parquet")
}

fn default_temp_dir() -> PathBuf {
    PathBuf::from("data/tmp")
}

fn default_batch_size() -> usize {
    10_000
}

fn default_thread_count() -> usize {
    1
}

fn default_channel_capacity() -> usize {
    8
}

fn default_zstd_level() -> u32 {
    3
}

fn default_max_row_group_size() -> usize {
    100_000
}

fn default_buffer_size() -> usize {
    256 * 1024 // 256KB
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_metrics_interval() -> u64 {
    5
}

impl Settings {
    /// Load settings from a YAML file. Falls back to defaults if file is missing.
    /// Fails fast with clear error message if YAML parsing fails.
    pub fn load_from_yaml(config_path: Option<&Path>) -> Result<Self> {
        let path = if let Some(p) = config_path {
            p.to_path_buf()
        } else {
            PathBuf::from("config.yaml")
        };

        // Try to read file; if it doesn't exist, return defaults
        let config_str = match fs::read_to_string(&path) {
            Ok(content) => content,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                eprintln!(
                    "[INFO] Config file not found at {:?}, using hardcoded defaults",
                    path
                );
                return Ok(Self::default());
            }
            Err(e) => return Err(e).context(format!("Failed to read config file at {:?}", path)),
        };

        // Parse YAML; fail fast with context
        let settings: Settings = serde_yaml::from_str(&config_str).context(format!(
            "Failed to parse config.yaml at {:?}: invalid YAML structure",
            path
        ))?;

        // Validate version
        if settings.version != "1.0" {
            eprintln!("[WARN] Config version mismatch: expected 1.0, got {}. Continuing with current schema.", settings.version);
        }

        eprintln!(
            "[INFO] Loaded config from {:?} (version: {})",
            path, settings.version
        );
        Ok(settings)
    }

    /// Merge CLI arguments into settings, with CLI taking precedence
    pub fn merge_with_cli(
        mut self,
        cli_input: Option<PathBuf>,
        cli_output: Option<PathBuf>,
        cli_batch_size: Option<usize>,
    ) -> Self {
        if let Some(input) = cli_input {
            self.storage.input_path = Some(input);
            eprintln!("[INFO] CLI override: input_path");
        }

        if let Some(output) = cli_output {
            self.storage.output_path = output;
            eprintln!("[INFO] CLI override: output_path");
        }

        if let Some(batch_size) = cli_batch_size {
            self.performance.batch_size = batch_size;
            eprintln!("[INFO] CLI override: batch_size");
        }

        self
    }

    /// Resolve paths relative to the project root
    pub fn resolve_paths(&mut self, root: &Path) -> Result<()> {
        self.storage.output_path = resolve_path(&self.storage.output_path, root)?;
        self.storage.temp_dir = resolve_path(&self.storage.temp_dir, root)?;

        if let Some(ref mut input_path) = self.storage.input_path {
            *input_path = resolve_path(input_path, root)?;
        }

        Ok(())
    }

    /// Get the input path; error if not set
    pub fn input_path(&self) -> Result<&Path> {
        self.storage
            .input_path
            .as_deref()
            .ok_or_else(|| anyhow!("input_path is required (set via --input or config.yaml)"))
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            version: "1.0".to_string(),
            storage: StorageConfig {
                input_path: None,
                output_path: default_output_path(),
                temp_dir: default_temp_dir(),
            },
            performance: PerformanceConfig {
                batch_size: default_batch_size(),
                thread_count: default_thread_count(),
                channel_capacity: default_channel_capacity(),
                zstd_level: default_zstd_level(),
                max_row_group_size: default_max_row_group_size(),
                buffer_size: default_buffer_size(),
            },
            logging: LoggingConfig {
                log_level: default_log_level(),
                metrics_interval_secs: default_metrics_interval(),
            },
        }
    }
}

/// Resolve a path to be either relative to root or return as-is if absolute
fn resolve_path(path: &Path, root: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(root.join(path))
    }
}
