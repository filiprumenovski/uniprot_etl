import { invoke } from "@tauri-apps/api/core";
import { listen, UnlistenFn } from "@tauri-apps/api/event";
import type { ProgressUpdate, RunSummary, PipelineCompleteEvent } from "./types";

// Pipeline commands
export const startPipeline = (params: {
  inputPath: string;
  outputPath: string;
  fastaSidecarPath?: string;
  batchSize?: number;
}) =>
  invoke<string>("start_pipeline", {
    inputPath: params.inputPath,
    outputPath: params.outputPath,
    fastaSidecarPath: params.fastaSidecarPath,
    batchSize: params.batchSize,
  });

export const getLiveMetrics = () =>
  invoke<ProgressUpdate | null>("get_live_metrics");

export const cancelPipeline = () => invoke<void>("cancel_pipeline");

// Runs commands
export const listRuns = (runsDir?: string) =>
  invoke<RunSummary[]>("list_runs", { runsDir });

export const getRunReport = (runId: string, runsDir?: string) =>
  invoke<Record<string, unknown>>("get_run_report", { runId, runsDir });

export const deleteRun = (runId: string, runsDir?: string) =>
  invoke<void>("delete_run", { runId, runsDir });

// Dialog commands
export const pickInputFile = () => invoke<string | null>("pick_input_file");
export const pickInputDirectory = () => invoke<string | null>("pick_input_directory");
export const pickOutputDirectory = () => invoke<string | null>("pick_output_directory");
export const pickFastaFile = () => invoke<string | null>("pick_fasta_file");

// Event listeners
export const onPipelineProgress = (
  callback: (update: ProgressUpdate) => void
): Promise<UnlistenFn> =>
  listen<ProgressUpdate>("pipeline:progress", (event) => callback(event.payload));

export const onPipelineComplete = (
  callback: (event: PipelineCompleteEvent) => void
): Promise<UnlistenFn> =>
  listen<PipelineCompleteEvent>("pipeline:complete", (event) => callback(event.payload));
