//! Run report generation and YAML serialization.
//!
//! Generates comprehensive reports capturing environment, performance metrics,
//! resource usage, and bottleneck diagnosis.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::fs;
use std::path::Path;
use sysinfo::System;

use crate::metrics::Metrics;
use crate::runs::RunContext;
use crate::sampler::ResourceSampler;

/// Status of an ETL run.
#[derive(Serialize, Clone, Debug)]
#[serde(tag = "status")]
pub enum RunStatus {
    Success,
    Error { message: String },
}

/// Complete report for a single ETL run.
#[derive(Serialize, Clone, Debug)]
pub struct RunReport {
    pub run_id: String,
    pub timestamp: DateTime<Utc>,
    pub duration_secs: f64,
    #[serde(flatten)]
    pub status: RunStatus,

    pub environment: EnvironmentInfo,
    pub performance: PerformanceMetrics,
    pub resources: ResourceMetrics,
    pub bottleneck: BottleneckInfo,
}

/// Environment information about the system.
#[derive(Serialize, Clone, Debug)]
pub struct EnvironmentInfo {
    pub os: String,
    pub os_version: String,
    pub cpu_model: String,
    pub cpu_cores: usize,
    pub total_memory_gb: f64,
}

/// Performance metrics from the ETL run.
#[derive(Serialize, Clone, Debug)]
pub struct PerformanceMetrics {
    pub entries_parsed: u64,
    pub entries_per_sec: f64,
    pub batches_written: u64,
    pub features_extracted: u64,
    pub isoforms_extracted: u64,
    pub ptm_attempted: u64,
    pub ptm_mapped: u64,
    pub ptm_failed: u64,
    pub ptm_failed_canonical_oob: u64,
    pub ptm_failed_vsp_deletion: u64,
    pub ptm_failed_mapper_oob: u64,
    pub ptm_failed_vsp_unresolvable: u64,
    pub ptm_failed_isoform_oob: u64,
    pub ptm_failed_residue_mismatch: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub bytes_per_sec: f64,
}

/// Resource usage metrics.
#[derive(Serialize, Clone, Debug)]
pub struct ResourceMetrics {
    pub peak_rss_mb: f64,
    pub peak_cpu_percent: f32,
    pub avg_channel_fullness_percent: f32,
}

/// Bottleneck diagnosis information.
#[derive(Serialize, Clone, Debug)]
pub struct BottleneckInfo {
    pub diagnosis: String,
    pub confidence: f32,
    pub recommendations: Vec<String>,
}

impl EnvironmentInfo {
    /// Gather environment information from the system.
    pub fn gather() -> Self {
        let mut sys = System::new_all();
        sys.refresh_all();

        let os = System::name().unwrap_or_else(|| "Unknown".to_string());
        let os_version = System::os_version().unwrap_or_else(|| "Unknown".to_string());

        let cpu_model = sys
            .cpus()
            .first()
            .map(|cpu| cpu.brand().to_string())
            .unwrap_or_else(|| "Unknown".to_string());

        let cpu_cores = sys.cpus().len();
        let total_memory_gb = sys.total_memory() as f64 / (1024.0 * 1024.0 * 1024.0);

        Self {
            os,
            os_version,
            cpu_model,
            cpu_cores,
            total_memory_gb,
        }
    }
}

impl RunReport {
    /// Generate a complete run report.
    pub fn generate(
        run_context: &RunContext,
        metrics: &Metrics,
        sampler: &ResourceSampler,
        status: RunStatus,
    ) -> Self {
        let elapsed = metrics.elapsed_secs();
        let entries = metrics.entries();
        let bytes_read = metrics.bytes_read();

        let entries_per_sec = if elapsed > 0.0 {
            entries as f64 / elapsed
        } else {
            0.0
        };

        let bytes_per_sec = if elapsed > 0.0 {
            bytes_read as f64 / elapsed
        } else {
            0.0
        };

        let high_water_marks = sampler.get_high_water_marks();
        let bottleneck_diagnosis = sampler.diagnose_bottleneck();

        Self {
            run_id: run_context.run_id.clone(),
            timestamp: run_context.start_time,
            duration_secs: elapsed,
            status,
            environment: EnvironmentInfo::gather(),
            performance: PerformanceMetrics {
                entries_parsed: entries,
                entries_per_sec,
                batches_written: metrics.batches(),
                features_extracted: metrics.features(),
                isoforms_extracted: metrics.isoforms(),
                ptm_attempted: metrics.ptm_attempted(),
                ptm_mapped: metrics.ptm_mapped(),
                ptm_failed: metrics.ptm_failed(),
                ptm_failed_canonical_oob: metrics.ptm_failed_canonical_oob(),
                ptm_failed_vsp_deletion: metrics.ptm_failed_vsp_deletion(),
                ptm_failed_mapper_oob: metrics.ptm_failed_mapper_oob(),
                ptm_failed_vsp_unresolvable: metrics.ptm_failed_vsp_unresolvable(),
                ptm_failed_isoform_oob: metrics.ptm_failed_isoform_oob(),
                ptm_failed_residue_mismatch: metrics.ptm_failed_residue_mismatch(),
                bytes_read,
                bytes_written: metrics.bytes_written(),
                bytes_per_sec,
            },
            resources: ResourceMetrics {
                peak_rss_mb: high_water_marks.peak_rss_bytes as f64 / (1024.0 * 1024.0),
                peak_cpu_percent: high_water_marks.peak_cpu_percent,
                avg_channel_fullness_percent: high_water_marks.avg_channel_fullness * 100.0,
            },
            bottleneck: BottleneckInfo {
                diagnosis: bottleneck_diagnosis.diagnosis,
                confidence: bottleneck_diagnosis.confidence,
                recommendations: bottleneck_diagnosis.recommendations,
            },
        }
    }

    /// Save the report as YAML to the specified path.
    pub fn save_yaml(&self, path: &Path) -> Result<()> {
        let yaml = serde_yaml::to_string(self).context("Failed to serialize report to YAML")?;

        fs::write(path, yaml)
            .with_context(|| format!("Failed to write report to {}", path.display()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_environment_info_gather() {
        let env_info = EnvironmentInfo::gather();
        assert!(!env_info.os.is_empty());
        assert!(env_info.cpu_cores > 0);
        assert!(env_info.total_memory_gb > 0.0);
    }

    #[test]
    fn test_run_status_serialization() {
        let success = RunStatus::Success;
        let yaml = serde_yaml::to_string(&success).unwrap();
        assert!(yaml.contains("Success"));

        let error = RunStatus::Error {
            message: "Test error".to_string(),
        };
        let yaml = serde_yaml::to_string(&error).unwrap();
        assert!(yaml.contains("Error"));
        assert!(yaml.contains("Test error"));
    }
}
