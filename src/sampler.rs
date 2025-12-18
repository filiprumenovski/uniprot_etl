//! Background resource sampling for performance diagnostics.
//!
//! Samples CPU usage, RSS memory, and channel fullness at 1Hz intervals
//! to identify performance bottlenecks without impacting the hot path.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use sysinfo::{Pid, ProcessRefreshKind, RefreshKind, System};

/// Statistics about channel usage for backpressure tracking.
pub struct ChannelStats {
    #[allow(dead_code)] // Used in tests
    capacity: usize,
    samples: Mutex<Vec<f32>>,
}

impl ChannelStats {
    /// Create a new channel stats tracker with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            samples: Mutex::new(Vec::with_capacity(1024)),
        }
    }

    /// Record the current channel length as a fullness sample.
    #[allow(dead_code)] // Used in tests
    pub fn record_fullness(&self, current_len: usize) {
        let fullness = if self.capacity > 0 {
            current_len as f32 / self.capacity as f32
        } else {
            0.0
        };
        if let Ok(mut samples) = self.samples.lock() {
            samples.push(fullness);
        }
    }

    /// Get the average channel fullness (0.0 - 1.0).
    pub fn average_fullness(&self) -> f32 {
        if let Ok(samples) = self.samples.lock() {
            if samples.is_empty() {
                return 0.0;
            }
            samples.iter().sum::<f32>() / samples.len() as f32
        } else {
            0.0
        }
    }
}

/// A single resource sample taken at a point in time.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ResourceSample {
    /// Time since sampler started
    pub elapsed: Duration,
    /// CPU usage percentage (0.0 - 100.0)
    pub cpu_percent: f32,
    /// Resident Set Size (physical RAM) in bytes
    pub rss_bytes: u64,
    /// Channel fullness at this sample (0.0 - 1.0)
    pub channel_fullness: f32,
}

/// High-water marks from resource sampling.
#[derive(Clone, Debug, Default)]
pub struct ResourceHighWaterMarks {
    /// Peak RSS in bytes
    pub peak_rss_bytes: u64,
    /// Peak CPU percentage
    pub peak_cpu_percent: f32,
    /// Average channel fullness (0.0 - 1.0)
    pub avg_channel_fullness: f32,
}

/// Bottleneck diagnosis result.
#[derive(Clone, Debug)]
pub struct BottleneckDiagnosis {
    /// Diagnosis string: "Writer Bottleneck", "Parser Bottleneck", or "Balanced"
    pub diagnosis: String,
    /// Confidence level (0.0 - 1.0)
    pub confidence: f32,
    /// Recommendations for improving performance
    pub recommendations: Vec<String>,
}

/// Background resource sampler that collects system metrics at 1Hz.
pub struct ResourceSampler {
    samples: Arc<Mutex<Vec<ResourceSample>>>,
    stop_flag: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    channel_stats: Arc<ChannelStats>,
}

impl ResourceSampler {
    /// Start the resource sampler in a background thread.
    ///
    /// Samples CPU, RSS, and channel fullness every 1 second.
    pub fn start(channel_stats: Arc<ChannelStats>) -> Self {
        let samples = Arc::new(Mutex::new(Vec::with_capacity(1024)));
        let stop_flag = Arc::new(AtomicBool::new(false));

        let samples_clone = Arc::clone(&samples);
        let stop_clone = Arc::clone(&stop_flag);
        let channel_stats_clone = Arc::clone(&channel_stats);

        let handle = thread::spawn(move || {
            Self::sampling_loop(samples_clone, stop_clone, channel_stats_clone);
        });

        Self {
            samples,
            stop_flag,
            handle: Some(handle),
            channel_stats,
        }
    }

    fn sampling_loop(
        samples: Arc<Mutex<Vec<ResourceSample>>>,
        stop_flag: Arc<AtomicBool>,
        channel_stats: Arc<ChannelStats>,
    ) {
        let pid = Pid::from_u32(std::process::id());
        let refresh_kind = RefreshKind::new()
            .with_processes(ProcessRefreshKind::new().with_cpu().with_memory());

        let mut sys = System::new_with_specifics(refresh_kind);
        let start = Instant::now();

        // Initial refresh to get baseline
        sys.refresh_processes_specifics(
            sysinfo::ProcessesToUpdate::Some(&[pid]),
            true,
            ProcessRefreshKind::new().with_cpu().with_memory(),
        );

        while !stop_flag.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(1));

            if stop_flag.load(Ordering::Relaxed) {
                break;
            }

            // Refresh process info
            sys.refresh_processes_specifics(
                sysinfo::ProcessesToUpdate::Some(&[pid]),
                true,
                ProcessRefreshKind::new().with_cpu().with_memory(),
            );

            if let Some(process) = sys.process(pid) {
                let sample = ResourceSample {
                    elapsed: start.elapsed(),
                    cpu_percent: process.cpu_usage(),
                    rss_bytes: process.memory(),
                    channel_fullness: channel_stats.average_fullness(),
                };

                if let Ok(mut samples_guard) = samples.lock() {
                    samples_guard.push(sample);
                }
            }
        }
    }

    /// Stop the sampler and wait for the background thread to finish.
    pub fn stop(&mut self) {
        self.stop_flag.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }

    /// Get high-water marks from all collected samples.
    pub fn get_high_water_marks(&self) -> ResourceHighWaterMarks {
        let samples = match self.samples.lock() {
            Ok(s) => s,
            Err(_) => return ResourceHighWaterMarks::default(),
        };

        if samples.is_empty() {
            return ResourceHighWaterMarks::default();
        }

        let peak_rss_bytes = samples.iter().map(|s| s.rss_bytes).max().unwrap_or(0);
        let peak_cpu_percent = samples
            .iter()
            .map(|s| s.cpu_percent)
            .fold(0.0f32, |a, b| a.max(b));
        let avg_channel_fullness = self.channel_stats.average_fullness();

        ResourceHighWaterMarks {
            peak_rss_bytes,
            peak_cpu_percent,
            avg_channel_fullness,
        }
    }

    /// Diagnose performance bottlenecks based on collected samples.
    ///
    /// Heuristics:
    /// - Channel >90% full → Writer Bottleneck (parser faster than writer)
    /// - Channel <10% full → Parser Bottleneck (writer faster than parser)
    /// - Otherwise → Balanced
    pub fn diagnose_bottleneck(&self) -> BottleneckDiagnosis {
        let avg_fullness = self.channel_stats.average_fullness();

        let (diagnosis, confidence, recommendations) = if avg_fullness > 0.9 {
            (
                "Writer Bottleneck".to_string(),
                0.9,
                vec![
                    "Consider increasing zstd compression level for better I/O throughput"
                        .to_string(),
                    "Check disk I/O performance".to_string(),
                    "Consider using faster storage (NVMe)".to_string(),
                ],
            )
        } else if avg_fullness < 0.1 {
            (
                "Parser Bottleneck".to_string(),
                0.9,
                vec![
                    "Parser is the limiting factor".to_string(),
                    "Consider increasing buffer_size for better read performance".to_string(),
                    "XML parsing is CPU-bound".to_string(),
                ],
            )
        } else if avg_fullness > 0.7 {
            (
                "Slight Writer Bottleneck".to_string(),
                0.6,
                vec![
                    "Writer is slightly slower than parser".to_string(),
                    "Consider reducing zstd compression level".to_string(),
                ],
            )
        } else if avg_fullness < 0.3 {
            (
                "Slight Parser Bottleneck".to_string(),
                0.6,
                vec![
                    "Parser is slightly slower than writer".to_string(),
                    "Consider increasing buffer_size".to_string(),
                ],
            )
        } else {
            (
                "Balanced".to_string(),
                0.8,
                vec!["Pipeline is well-balanced for current configuration".to_string()],
            )
        };

        BottleneckDiagnosis {
            diagnosis,
            confidence,
            recommendations,
        }
    }
}

impl Drop for ResourceSampler {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_stats() {
        let stats = ChannelStats::new(10);
        stats.record_fullness(5);
        stats.record_fullness(7);
        stats.record_fullness(3);

        let avg = stats.average_fullness();
        assert!((avg - 0.5).abs() < 0.01); // (0.5 + 0.7 + 0.3) / 3 = 0.5
    }

    #[test]
    fn test_sampler_start_stop() {
        let channel_stats = Arc::new(ChannelStats::new(8));
        let mut sampler = ResourceSampler::start(channel_stats);

        // Let it run briefly
        thread::sleep(Duration::from_millis(100));

        sampler.stop();

        // Should not panic on double stop
        sampler.stop();
    }

    #[test]
    fn test_bottleneck_diagnosis() {
        // Test high fullness -> writer bottleneck
        let stats = Arc::new(ChannelStats::new(10));
        for _ in 0..10 {
            stats.record_fullness(9); // 90% full
        }
        let sampler = ResourceSampler {
            samples: Arc::new(Mutex::new(Vec::new())),
            stop_flag: Arc::new(AtomicBool::new(true)),
            handle: None,
            channel_stats: stats,
        };
        let diagnosis = sampler.diagnose_bottleneck();
        assert!(diagnosis.diagnosis.contains("Writer"));

        // Test low fullness -> parser bottleneck
        let stats2 = Arc::new(ChannelStats::new(10));
        for _ in 0..10 {
            stats2.record_fullness(0); // 0% full
        }
        let sampler2 = ResourceSampler {
            samples: Arc::new(Mutex::new(Vec::new())),
            stop_flag: Arc::new(AtomicBool::new(true)),
            handle: None,
            channel_stats: stats2,
        };
        let diagnosis2 = sampler2.diagnose_bottleneck();
        assert!(diagnosis2.diagnosis.contains("Parser"));
    }
}
