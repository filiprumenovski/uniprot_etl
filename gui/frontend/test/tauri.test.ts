import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  deleteRun,
  getRunReport,
  listRuns,
  onPipelineComplete,
  onPipelineProgress,
  startPipeline,
} from "@/lib/tauri";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));
vi.mock("@tauri-apps/api/event", () => ({ listen: vi.fn() }));

const invokeMock = vi.mocked(invoke);
const listenMock = vi.mocked(listen);

const sampleProgress = {
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

describe("tauri client", () => {
  beforeEach(() => {
    invokeMock.mockReset();
    listenMock.mockReset();
  });

  it("sends camelCase args for startPipeline", async () => {
    invokeMock.mockResolvedValue("ok");

    await startPipeline({
      inputPath: "/input",
      outputPath: "/output",
      fastaSidecarPath: "/isoforms.fasta",
      batchSize: 12345,
    });

    expect(invokeMock).toHaveBeenCalledWith("start_pipeline", {
      inputPath: "/input",
      outputPath: "/output",
      fastaSidecarPath: "/isoforms.fasta",
      batchSize: 12345,
    });
  });

  it("passes run identifiers in list/get/delete", async () => {
    await listRuns("/runs");
    expect(invokeMock).toHaveBeenCalledWith("list_runs", { runsDir: "/runs" });

    await getRunReport("run-1", "/runs");
    expect(invokeMock).toHaveBeenCalledWith("get_run_report", {
      runId: "run-1",
      runsDir: "/runs",
    });

    await deleteRun("run-2", "/runs");
    expect(invokeMock).toHaveBeenCalledWith("delete_run", {
      runId: "run-2",
      runsDir: "/runs",
    });
  });

  it("routes pipeline progress events", async () => {
    listenMock.mockResolvedValue(() => {});
    const onProgress = vi.fn();

    await onPipelineProgress(onProgress);

    const handler = listenMock.mock.calls[0]?.[1];
    handler?.({ payload: sampleProgress });

    expect(onProgress).toHaveBeenCalledWith(sampleProgress);
  });

  it("routes pipeline completion events", async () => {
    listenMock.mockResolvedValue(() => {});
    const onComplete = vi.fn();

    await onPipelineComplete(onComplete);

    const handler = listenMock.mock.calls[0]?.[1];
    handler?.({ payload: { success: false, message: "boom" } });

    expect(onComplete).toHaveBeenCalledWith({ success: false, message: "boom" });
  });
});
