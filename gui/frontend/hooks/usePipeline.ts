"use client";

import { useEffect, useState, useCallback } from "react";
import {
  onPipelineProgress,
  onPipelineComplete,
  startPipeline,
  cancelPipeline,
} from "@/lib/tauri";
import type { ProgressUpdate, PipelineCompleteEvent } from "@/lib/types";

interface UsePipelineReturn {
  isRunning: boolean;
  progress: ProgressUpdate | null;
  lastResult: PipelineCompleteEvent | null;
  error: string | null;
  start: (params: {
    inputPath: string;
    outputPath: string;
    fastaSidecarPath?: string;
    batchSize?: number;
  }) => Promise<void>;
  cancel: () => Promise<void>;
}

export function usePipeline(): UsePipelineReturn {
  const [isRunning, setIsRunning] = useState(false);
  const [progress, setProgress] = useState<ProgressUpdate | null>(null);
  const [lastResult, setLastResult] = useState<PipelineCompleteEvent | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let unlistenProgress: (() => void) | undefined;
    let unlistenComplete: (() => void) | undefined;

    const setup = async () => {
      unlistenProgress = await onPipelineProgress((update) => {
        setProgress(update);
        setIsRunning(update.is_running);
      });

      unlistenComplete = await onPipelineComplete((result) => {
        setLastResult(result);
        setIsRunning(false);
        if (!result.success) {
          setError(result.message);
        }
      });
    };

    setup();

    return () => {
      unlistenProgress?.();
      unlistenComplete?.();
    };
  }, []);

  const start = useCallback(
    async (params: {
      inputPath: string;
      outputPath: string;
      fastaSidecarPath?: string;
      batchSize?: number;
    }) => {
      try {
        setError(null);
        setLastResult(null);
        setIsRunning(true);
        await startPipeline(params);
      } catch (e) {
        setError(String(e));
        setIsRunning(false);
      }
    },
    []
  );

  const cancel = useCallback(async () => {
    try {
      await cancelPipeline();
      setIsRunning(false);
    } catch (e) {
      setError(String(e));
    }
  }, []);

  return { isRunning, progress, lastResult, error, start, cancel };
}
