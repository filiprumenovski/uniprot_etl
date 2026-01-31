pub mod cli;
pub mod config;
pub mod error;
pub mod fasta;
pub mod metrics;
pub mod observability;
pub mod pipeline;
pub mod report;
pub mod runs;
pub mod sampler;
pub mod schema;
pub mod writer;

// Re-export library API for external consumers (GUI, tests, etc.)
pub use config::Settings;
pub use metrics::Metrics;
pub use observability::{start_metrics_server, MetricsServerHandle};
pub use pipeline::{run_pipeline, PipelineArgs};
pub use report::{RunReport, RunStatus};
pub use runs::RunContext;
pub use sampler::ResourceSampler;
