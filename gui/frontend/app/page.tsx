"use client";

import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { MetricsDashboard } from "@/components/MetricsDashboard";
import { usePipeline } from "@/hooks/usePipeline";
import {
  pickInputFile,
  pickInputDirectory,
  pickOutputDirectory,
  pickFastaFile,
} from "@/lib/tauri";
import {
  FolderOpen,
  FileInput,
  Play,
  Square,
  Database,
  CheckCircle,
  XCircle,
  AlertCircle,
} from "lucide-react";

export default function Dashboard() {
  const { isRunning, progress, lastResult, error, start, cancel } = usePipeline();

  const [inputPath, setInputPath] = useState("");
  const [outputPath, setOutputPath] = useState("");
  const [fastaPath, setFastaPath] = useState("");
  const [batchSize, setBatchSize] = useState(10000);
  const [inputMode, setInputMode] = useState<"file" | "directory">("directory");

  const handlePickInput = async () => {
    try {
      const path = inputMode === "directory"
        ? await pickInputDirectory()
        : await pickInputFile();
      if (path) setInputPath(path);
    } catch (e) {
      console.error("Failed to pick input:", e);
    }
  };

  const handlePickOutput = async () => {
    try {
      const path = await pickOutputDirectory();
      if (path) setOutputPath(path);
    } catch (e) {
      console.error("Failed to pick output:", e);
    }
  };

  const handlePickFasta = async () => {
    try {
      const path = await pickFastaFile();
      if (path) setFastaPath(path);
    } catch (e) {
      console.error("Failed to pick FASTA:", e);
    }
  };

  const handleStart = async () => {
    if (!inputPath || !outputPath) return;
    await start({
      inputPath,
      outputPath,
      fastaSidecarPath: fastaPath || undefined,
      batchSize,
    });
  };

  const canStart = inputPath && outputPath && !isRunning;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4">
          <h2 className="text-xl font-semibold">Pipeline Dashboard</h2>
        </div>
        {isRunning && (
          <Badge variant="default" className="animate-pulse">
            Running
          </Badge>
        )}
        {lastResult?.success && !isRunning && (
          <Badge variant="success">Completed</Badge>
        )}
        {lastResult && !lastResult.success && !isRunning && (
          <Badge variant="destructive">Failed</Badge>
        )}
      </div>

      <div className="grid gap-4 md:grid-cols-2">
        {/* Input Configuration */}
        <Card>
          <CardHeader className="p-3 pb-1">
            <CardTitle className="text-sm font-medium flex items-center gap-2">
              <FileInput className="h-4 w-4" />
              Input
            </CardTitle>
          </CardHeader>
          <CardContent className="p-3 pt-0 space-y-2">
            <div className="flex gap-1">
              <Button
                variant={inputMode === "directory" ? "default" : "outline"}
                size="sm"
                className="h-6 text-xs"
                onClick={() => setInputMode("directory")}
              >
                Directory
              </Button>
              <Button
                variant={inputMode === "file" ? "default" : "outline"}
                size="sm"
                className="h-6 text-xs"
                onClick={() => setInputMode("file")}
              >
                File
              </Button>
            </div>

            <div className="flex gap-1">
              <Input
                className="h-7 text-xs"
                value={inputPath}
                onChange={(e) => setInputPath(e.target.value)}
                placeholder={inputMode === "directory" ? "Path to XML Directory..." : "Path to XML File..."}
                disabled={isRunning}
              />
              <Button
                variant="outline"
                size="sm"
                className="h-7 w-7 p-0"
                onClick={handlePickInput}
                disabled={isRunning}
                aria-label="Pick input path"
              >
                <FolderOpen className="h-3.5 w-3.5" />
              </Button>
            </div>

            <div className="flex gap-1">
              <Input
                className="h-7 text-xs"
                value={fastaPath}
                onChange={(e) => setFastaPath(e.target.value)}
                placeholder="Path to FASTA File (Optional)..."
                disabled={isRunning}
              />
              <Button
                variant="outline"
                size="sm"
                className="h-7 w-7 p-0"
                onClick={handlePickFasta}
                disabled={isRunning}
                aria-label="Pick FASTA sidecar"
              >
                <FolderOpen className="h-3.5 w-3.5" />
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* Output Configuration */}
        <Card>
          <CardHeader className="p-3 pb-1">
            <CardTitle className="text-sm font-medium flex items-center gap-2">
              <Database className="h-4 w-4" />
              Output
            </CardTitle>
          </CardHeader>
          <CardContent className="p-3 pt-0 space-y-2">
            <div className="flex gap-1">
              <Input
                className="h-7 text-xs"
                value={outputPath}
                onChange={(e) => setOutputPath(e.target.value)}
                placeholder="Output Directory..."
                disabled={isRunning}
              />
              <Button
                variant="outline"
                size="sm"
                className="h-7 w-7 p-0"
                onClick={handlePickOutput}
                disabled={isRunning}
                aria-label="Pick output directory"
              >
                <FolderOpen className="h-3.5 w-3.5" />
              </Button>
            </div>

            <div className="flex items-center gap-2">
              <Label className="text-xs whitespace-nowrap">Batch Size</Label>
              <Input
                className="h-7 text-xs w-24"
                type="number"
                value={batchSize}
                onChange={(e) => setBatchSize(parseInt(e.target.value) || 10000)}
                min={1000}
                max={100000}
                step={1000}
                disabled={isRunning}
              />
              {!isRunning ? (
                <Button
                  onClick={handleStart}
                  disabled={!canStart}
                  size="sm"
                  className="flex-1 h-7"
                >
                  <Play className="h-3.5 w-3.5 mr-1.5" />
                  Start
                </Button>
              ) : (
                <Button
                  onClick={cancel}
                  variant="destructive"
                  size="sm"
                  className="flex-1 h-7"
                >
                  <Square className="h-3.5 w-3.5 mr-1.5" />
                  Cancel
                </Button>
              )}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Live Metrics Dashboard */}
      <MetricsDashboard height={500} enabled={isRunning || !!lastResult} />

      {/* Result/Error Section */}
      {lastResult && !isRunning && (
        <Card className={lastResult.success ? "border-green-500" : "border-red-500"}>
          <CardContent className="p-4 flex items-center gap-2">
            {lastResult.success ? (
              <CheckCircle className="h-4 w-4 text-green-500 shrink-0" />
            ) : (
              <XCircle className="h-4 w-4 text-red-500 shrink-0" />
            )}
            <p className={`text-sm ${lastResult.success ? "text-green-600" : "text-red-600"}`}>
              {lastResult.message}
            </p>
          </CardContent>
        </Card>
      )}

      {error && !lastResult && (
        <Card className="border-red-500">
          <CardContent className="p-4 flex items-center gap-2">
            <AlertCircle className="h-4 w-4 text-red-500 shrink-0" />
            <p className="text-sm text-red-600">{error}</p>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
