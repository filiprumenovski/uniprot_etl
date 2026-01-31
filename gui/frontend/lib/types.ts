// Types matching Rust structs

export interface ProgressUpdate {
  entries_parsed: number;
  entries_per_sec: number;
  batches_written: number;
  features_extracted: number;
  isoforms_extracted: number;
  ptm_mapped: number;
  ptm_failed: number;
  bytes_read: number;
  bytes_written: number;
  elapsed_secs: number;
  is_running: boolean;
}

export interface RunSummary {
  run_id: string;
  timestamp: string;
  status: string;
  duration_secs: number;
  entries_parsed: number;
}

export interface RunReport {
  run_id: string;
  timestamp: string;
  duration_secs: number;
  status: "Success" | { Error: { message: string } };
  environment: {
    os: string;
    os_version: string;
    cpu_model: string;
    cpu_cores: number;
    total_memory_gb: number;
  };
  performance: {
    entries_parsed: number;
    entries_per_sec: number;
    batches_written: number;
    features_extracted: number;
    isoforms_extracted: number;
    ptm_attempted: number;
    ptm_mapped: number;
    ptm_failed: number;
    bytes_read: number;
    bytes_written: number;
    bytes_per_sec: number;
  };
  resources: {
    peak_rss_mb: number;
    peak_cpu_percent: number;
    avg_channel_fullness_percent: number;
  };
  bottleneck: {
    diagnosis: string;
    confidence: number;
    recommendations: string[];
  };
}

export interface PipelineCompleteEvent {
  success: boolean;
  message: string;
}
