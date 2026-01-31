import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import RunsPage from "@/app/runs/page";

const mocks = vi.hoisted(() => ({
  listRuns: vi.fn(),
  getRunReport: vi.fn(),
  deleteRun: vi.fn(),
}));

vi.mock("@/lib/tauri", () => ({
  listRuns: mocks.listRuns,
  getRunReport: mocks.getRunReport,
  deleteRun: mocks.deleteRun,
}));

const runReport = {
  run_id: "run-1",
  timestamp: "2024-01-01",
  duration_secs: 12.3,
  status: "Success",
  environment: {
    os: "macOS",
    os_version: "14.0",
    cpu_model: "M1",
    cpu_cores: 8,
    total_memory_gb: 16,
  },
  performance: {
    entries_parsed: 100,
    entries_per_sec: 50,
    batches_written: 1,
    features_extracted: 20,
    isoforms_extracted: 5,
    ptm_attempted: 1,
    ptm_mapped: 1,
    ptm_failed: 0,
    bytes_read: 1024,
    bytes_written: 2048,
    bytes_per_sec: 100,
  },
  resources: {
    peak_rss_mb: 512,
    peak_cpu_percent: 90,
    avg_channel_fullness_percent: 30,
  },
  bottleneck: {
    diagnosis: "I/O bound",
    confidence: 0.8,
    recommendations: [],
  },
};

describe("RunsPage", () => {
  beforeEach(() => {
    mocks.listRuns.mockReset();
    mocks.getRunReport.mockReset();
    mocks.deleteRun.mockReset();
  });

  it("renders empty state when no runs exist", async () => {
    mocks.listRuns.mockResolvedValueOnce([]);

    render(<RunsPage />);

    expect(screen.getByText(/loading runs/i)).toBeInTheDocument();
    expect(await screen.findByText(/no runs found/i)).toBeInTheDocument();
  });

  it("lists runs and supports refresh", async () => {
    mocks.listRuns
      .mockResolvedValueOnce([
        {
          run_id: "run-1",
          timestamp: "2024-01-01",
          status: "Success",
          duration_secs: 12.3,
          entries_parsed: 100,
        },
      ])
      .mockResolvedValueOnce([
        {
          run_id: "run-1",
          timestamp: "2024-01-01",
          status: "Success",
          duration_secs: 12.3,
          entries_parsed: 100,
        },
      ]);

    const user = userEvent.setup();
    render(<RunsPage />);

    expect(await screen.findByText("run-1")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /refresh/i }));

    await waitFor(() => expect(mocks.listRuns).toHaveBeenCalledTimes(2));
  });

  it("expands run details", async () => {
    mocks.listRuns.mockResolvedValueOnce([
      {
        run_id: "run-1",
        timestamp: "2024-01-01",
        status: "Success",
        duration_secs: 12.3,
        entries_parsed: 100,
      },
    ]);
    mocks.getRunReport.mockResolvedValueOnce(runReport);

    const user = userEvent.setup();
    render(<RunsPage />);

    expect(await screen.findByText("run-1")).toBeInTheDocument();

    await user.click(screen.getByLabelText(/expand run details/i));

    expect(await screen.findByText(/environment/i)).toBeInTheDocument();
  });
});
