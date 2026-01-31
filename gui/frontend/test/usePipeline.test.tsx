import { act, renderHook, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { usePipeline } from "@/hooks/usePipeline";
import type { ProgressUpdate } from "@/lib/types";

const mocks = vi.hoisted(() => ({
  onPipelineProgress: vi.fn(),
  onPipelineComplete: vi.fn(),
  startPipeline: vi.fn(),
  cancelPipeline: vi.fn(),
}));

vi.mock("@/lib/tauri", () => ({
  onPipelineProgress: mocks.onPipelineProgress,
  onPipelineComplete: mocks.onPipelineComplete,
  startPipeline: mocks.startPipeline,
  cancelPipeline: mocks.cancelPipeline,
}));

const sampleProgress: ProgressUpdate = {
  entries_parsed: 10,
  entries_per_sec: 2,
  batches_written: 1,
  features_extracted: 3,
  isoforms_extracted: 4,
  ptm_mapped: 5,
  ptm_failed: 0,
  bytes_read: 100,
  bytes_written: 200,
  elapsed_secs: 1,
  is_running: true,
};

describe("usePipeline", () => {
  it("subscribes to pipeline updates and updates running state", async () => {
    let progressHandler: ((update: ProgressUpdate) => void) | undefined;
    mocks.onPipelineProgress.mockImplementation(async (handler) => {
      progressHandler = handler;
      return () => {};
    });
    mocks.onPipelineComplete.mockResolvedValue(() => {});

    const { result } = renderHook(() => usePipeline());

    await waitFor(() => expect(mocks.onPipelineProgress).toHaveBeenCalled());

    act(() => {
      progressHandler?.(sampleProgress);
    });

    expect(result.current.isRunning).toBe(true);
  });

  it("starts and cancels the pipeline", async () => {
    mocks.onPipelineProgress.mockResolvedValue(() => {});
    mocks.onPipelineComplete.mockResolvedValue(() => {});
    mocks.startPipeline.mockResolvedValue("ok");
    mocks.cancelPipeline.mockResolvedValue(undefined);

    const { result } = renderHook(() => usePipeline());

    await act(async () => {
      await result.current.start({
        inputPath: "/input",
        outputPath: "/output",
        batchSize: 10000,
      });
    });

    expect(mocks.startPipeline).toHaveBeenCalledWith({
      inputPath: "/input",
      outputPath: "/output",
      batchSize: 10000,
    });

    await act(async () => {
      await result.current.cancel();
    });

    expect(mocks.cancelPipeline).toHaveBeenCalled();
  });
});
