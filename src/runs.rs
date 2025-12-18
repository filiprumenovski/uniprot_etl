//! Run directory lifecycle management.
//!
//! Creates timestamped run directories and manages cleanup of old runs.

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use std::fs;
use std::path::{Path, PathBuf};

/// Context for a single ETL run, including directory paths and timing.
pub struct RunContext {
    /// Path to the run directory (e.g., `runs/run_20250118_143022/`)
    pub run_dir: PathBuf,
    /// Unique run identifier (e.g., `run_20250118_143022`)
    pub run_id: String,
    /// UTC timestamp when the run started
    pub start_time: DateTime<Utc>,
}

impl RunContext {
    /// Create a new run context with a timestamped directory.
    ///
    /// Creates the directory structure: `{runs_dir}/run_{YYYYMMDD_HHMMSS}/`
    pub fn new(runs_dir: &Path) -> Result<Self> {
        Self::new_with_run_id(runs_dir, None)
    }

    /// Create a new run context, optionally forcing the run id.
    ///
    /// When `run_id_override` is provided, it is validated and normalized.
    /// The created directory must not already exist.
    pub fn new_with_run_id(runs_dir: &Path, run_id_override: Option<String>) -> Result<Self> {
        let start_time = Utc::now();

        let run_id = match &run_id_override {
            Some(raw) => normalize_run_id(raw)?,
            None => format!("run_{}", start_time.format("%Y%m%d_%H%M%S")),
        };

        let run_dir = runs_dir.join(&run_id);
        if run_dir.exists() {
            // If a wrapper explicitly requested a run id, allow reusing a freshly-created
            // directory intended to receive profiling artifacts.
            if run_id_override.is_some() {
                if !is_reusable_precreated_run_dir(&run_dir)? {
                    return Err(anyhow!(
                        "Run directory already exists and is not reusable: {}",
                        run_dir.display()
                    ));
                }
            } else {
                return Err(anyhow!(
                    "Run directory already exists: {}",
                    run_dir.display()
                ));
            }
        } else {
            fs::create_dir_all(&run_dir).with_context(|| {
                format!("Failed to create run directory: {}", run_dir.display())
            })?;
        }

        Ok(Self {
            run_dir,
            run_id,
            start_time,
        })
    }

    /// Path to the report.yaml file within this run directory.
    pub fn report_path(&self) -> PathBuf {
        self.run_dir.join("report.yaml")
    }

    /// Path to the etl.log file within this run directory.
    pub fn log_path(&self) -> PathBuf {
        self.run_dir.join("etl.log")
    }

    /// Path to the config_snapshot.yaml file within this run directory.
    pub fn config_snapshot_path(&self) -> PathBuf {
        self.run_dir.join("config_snapshot.yaml")
    }
}

fn normalize_run_id(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("run_id cannot be empty"));
    }

    if trimmed.contains('/') || trimmed.contains('\\') || trimmed.contains("..") {
        return Err(anyhow!("run_id contains invalid path characters"));
    }

    let normalized = if trimmed.starts_with("run_") {
        trimmed.to_string()
    } else {
        format!("run_{trimmed}")
    };

    if !normalized
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(anyhow!(
            "run_id may only contain ASCII letters/numbers, '_' and '-'"
        ));
    }

    Ok(normalized)
}

fn is_reusable_precreated_run_dir(run_dir: &Path) -> Result<bool> {
    if !run_dir.is_dir() {
        return Ok(false);
    }

    // Allow either an empty directory, or a directory containing only a `profiles/` folder.
    let entries = fs::read_dir(run_dir)
        .with_context(|| format!("Failed to read run directory: {}", run_dir.display()))?;

    let mut saw_entries = false;
    for entry in entries {
        let entry = entry.with_context(|| {
            format!("Failed to read directory entry in {}", run_dir.display())
        })?;
        saw_entries = true;

        let Some(name) = entry.file_name().to_str().map(|s| s.to_string()) else {
            return Ok(false);
        };

        if name == "profiles" {
            if !entry.path().is_dir() {
                return Ok(false);
            }
            continue;
        }

        // Any other file/dir indicates this isn't a fresh pre-created directory.
        return Ok(false);
    }

    // If we reached here, the directory was empty or only contained `profiles/`.
    Ok(true)
}

/// Clean up old run directories, keeping only the most recent `keep_count`.
///
/// Runs are sorted by directory name (which includes timestamp) and older
/// runs beyond `keep_count` are removed.
pub fn cleanup_old_runs(runs_dir: &Path, keep_count: usize) -> Result<()> {
    if !runs_dir.exists() {
        return Ok(());
    }

    let mut run_dirs: Vec<PathBuf> = fs::read_dir(runs_dir)
        .with_context(|| format!("Failed to read runs directory: {}", runs_dir.display()))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_dir()
                && path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("run_"))
                    .unwrap_or(false)
        })
        .collect();

    // Sort by name (timestamp order since format is run_YYYYMMDD_HHMMSS)
    run_dirs.sort();

    // Remove oldest runs if we have more than keep_count
    if run_dirs.len() > keep_count {
        let to_remove = run_dirs.len() - keep_count;
        for dir in run_dirs.into_iter().take(to_remove) {
            if let Err(e) = fs::remove_dir_all(&dir) {
                // Log but don't fail on cleanup errors
                eprintln!(
                    "[WARN] Failed to remove old run directory {}: {}",
                    dir.display(),
                    e
                );
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    #[test]
    fn test_run_context_creation() {
        let temp_dir = std::env::temp_dir().join("uniprot_etl_test_runs");
        let _ = fs::remove_dir_all(&temp_dir); // Clean up any previous test

        let ctx = RunContext::new(&temp_dir).unwrap();

        assert!(ctx.run_dir.exists());
        assert!(ctx.run_id.starts_with("run_"));
        assert!(ctx.report_path().ends_with("report.yaml"));
        assert!(ctx.log_path().ends_with("etl.log"));
        assert!(ctx.config_snapshot_path().ends_with("config_snapshot.yaml"));

        // Cleanup
        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_cleanup_old_runs() {
        let temp_dir = std::env::temp_dir().join("uniprot_etl_test_cleanup");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        // Create 5 fake run directories
        for i in 1..=5 {
            let run_dir = temp_dir.join(format!("run_2025010{}_120000", i));
            fs::create_dir_all(&run_dir).unwrap();
            // Add a file to make it non-empty
            File::create(run_dir.join("report.yaml")).unwrap();
        }

        // Keep only 2 runs
        cleanup_old_runs(&temp_dir, 2).unwrap();

        let remaining: Vec<_> = fs::read_dir(&temp_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();

        assert_eq!(remaining.len(), 2);

        // Cleanup
        let _ = fs::remove_dir_all(&temp_dir);
    }
}
