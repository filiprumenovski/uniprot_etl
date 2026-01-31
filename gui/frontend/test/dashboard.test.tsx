import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import Dashboard from "@/app/page";

const mocks = vi.hoisted(() => ({
  start: vi.fn(),
  cancel: vi.fn(),
}));

vi.mock("@/hooks/usePipeline", () => ({
  usePipeline: () => ({
    isRunning: false,
    progress: null,
    lastResult: null,
    error: null,
    start: mocks.start,
    cancel: mocks.cancel,
  }),
}));

vi.mock("@/lib/tauri", () => ({
  pickInputFile: vi.fn(),
  pickInputDirectory: vi.fn(),
  pickOutputDirectory: vi.fn(),
  pickFastaFile: vi.fn(),
}));

describe("Dashboard", () => {
  beforeEach(() => {
    mocks.start.mockReset();
    mocks.cancel.mockReset();
  });

  it("enables start when input and output are set", async () => {
    const user = userEvent.setup();
    render(<Dashboard />);

    const startButton = screen.getByRole("button", { name: /start pipeline/i });
    expect(startButton).toBeDisabled();

    await user.type(
      screen.getByPlaceholderText("/path/to/xml/directory"),
      "/data/input"
    );
    await user.type(
      screen.getByPlaceholderText("/path/to/output"),
      "/data/output"
    );

    expect(startButton).toBeEnabled();

    await user.click(startButton);

    expect(mocks.start).toHaveBeenCalledWith({
      inputPath: "/data/input",
      outputPath: "/data/output",
      fastaSidecarPath: undefined,
      batchSize: 10000,
    });
  });
});
