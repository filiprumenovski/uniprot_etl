"use client";

import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { listRuns, getRunReport, deleteRun } from "@/lib/tauri";
import type { RunSummary, RunReport } from "@/lib/types";
import {
  History,
  Trash2,
  Eye,
  Clock,
  Database,
  CheckCircle,
  XCircle,
  ChevronDown,
  ChevronUp,
} from "lucide-react";

function formatDuration(secs: number): string {
  if (secs < 60) return `${secs.toFixed(1)}s`;
  const mins = Math.floor(secs / 60);
  const remainingSecs = secs % 60;
  if (mins < 60) return `${mins}m ${remainingSecs.toFixed(0)}s`;
  const hours = Math.floor(mins / 60);
  const remainingMins = mins % 60;
  return `${hours}h ${remainingMins}m`;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(2)} ${sizes[i]}`;
}

export default function RunsPage() {
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [expandedRun, setExpandedRun] = useState<string | null>(null);
  const [runDetails, setRunDetails] = useState<Record<string, RunReport>>({});

  useEffect(() => {
    loadRuns();
  }, []);

  const loadRuns = async () => {
    try {
      setLoading(true);
      const data = await listRuns();
      setRuns(data);
    } catch (e) {
      console.error("Failed to load runs:", e);
    } finally {
      setLoading(false);
    }
  };

  const handleExpand = async (runId: string) => {
    if (expandedRun === runId) {
      setExpandedRun(null);
      return;
    }

    setExpandedRun(runId);

    if (!runDetails[runId]) {
      try {
        const report = await getRunReport(runId);
        setRunDetails((prev) => ({ ...prev, [runId]: report as unknown as RunReport }));
      } catch (e) {
        console.error("Failed to load run details:", e);
      }
    }
  };

  const handleDelete = async (runId: string) => {
    if (!confirm(`Delete run ${runId}?`)) return;
    try {
      await deleteRun(runId);
      setRuns((prev) => prev.filter((r) => r.run_id !== runId));
      if (expandedRun === runId) setExpandedRun(null);
    } catch (e) {
      console.error("Failed to delete run:", e);
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold flex items-center gap-2">
            <History className="h-6 w-6" />
            Run History
          </h2>
          <p className="text-muted-foreground">
            View past pipeline executions and their reports
          </p>
        </div>
        <Button variant="outline" onClick={loadRuns} disabled={loading}>
          Refresh
        </Button>
      </div>

      {loading ? (
        <Card>
          <CardContent className="py-8 text-center text-muted-foreground">
            Loading runs...
          </CardContent>
        </Card>
      ) : runs.length === 0 ? (
        <Card>
          <CardContent className="py-8 text-center text-muted-foreground">
            No runs found. Run the pipeline to see history here.
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-4">
          {runs.map((run) => (
            <Card key={run.run_id}>
              <CardHeader className="pb-2">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <CardTitle className="text-lg font-mono">{run.run_id}</CardTitle>
                    <Badge variant={run.status === "Success" ? "success" : "destructive"}>
                      {run.status === "Success" ? (
                        <CheckCircle className="h-3 w-3 mr-1" />
                      ) : (
                        <XCircle className="h-3 w-3 mr-1" />
                      )}
                      {run.status}
                    </Badge>
                  </div>
                  <div className="flex items-center gap-2">
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => handleExpand(run.run_id)}
                      aria-label={expandedRun === run.run_id ? "Collapse run details" : "Expand run details"}
                    >
                      {expandedRun === run.run_id ? (
                        <ChevronUp className="h-4 w-4" />
                      ) : (
                        <ChevronDown className="h-4 w-4" />
                      )}
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => handleDelete(run.run_id)}
                      aria-label="Delete run"
                    >
                      <Trash2 className="h-4 w-4 text-destructive" />
                    </Button>
                  </div>
                </div>
                <CardDescription className="flex items-center gap-4 mt-2">
                  <span className="flex items-center gap-1">
                    <Clock className="h-3 w-3" />
                    {run.timestamp}
                  </span>
                  <span className="flex items-center gap-1">
                    <Database className="h-3 w-3" />
                    {run.entries_parsed.toLocaleString()} entries
                  </span>
                  <span>{formatDuration(run.duration_secs)}</span>
                </CardDescription>
              </CardHeader>

              {expandedRun === run.run_id && runDetails[run.run_id] && (
                <CardContent className="pt-4 border-t">
                  <RunReportDetails report={runDetails[run.run_id]} />
                </CardContent>
              )}
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}

function RunReportDetails({ report }: { report: RunReport }) {
  return (
    <div className="space-y-6">
      {/* Environment */}
      <div>
        <h4 className="font-semibold mb-2">Environment</h4>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
          <div>
            <p className="text-muted-foreground">OS</p>
            <p>{report.environment.os} {report.environment.os_version}</p>
          </div>
          <div>
            <p className="text-muted-foreground">CPU</p>
            <p>{report.environment.cpu_model}</p>
          </div>
          <div>
            <p className="text-muted-foreground">Cores</p>
            <p>{report.environment.cpu_cores}</p>
          </div>
          <div>
            <p className="text-muted-foreground">Memory</p>
            <p>{report.environment.total_memory_gb.toFixed(1)} GB</p>
          </div>
        </div>
      </div>

      {/* Performance */}
      <div>
        <h4 className="font-semibold mb-2">Performance</h4>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
          <div>
            <p className="text-muted-foreground">Entries Parsed</p>
            <p className="font-semibold">{report.performance.entries_parsed.toLocaleString()}</p>
          </div>
          <div>
            <p className="text-muted-foreground">Throughput</p>
            <p className="font-semibold">{report.performance.entries_per_sec.toLocaleString(undefined, { maximumFractionDigits: 0 })} /sec</p>
          </div>
          <div>
            <p className="text-muted-foreground">Bytes Read</p>
            <p className="font-semibold">{formatBytes(report.performance.bytes_read)}</p>
          </div>
          <div>
            <p className="text-muted-foreground">Bytes Written</p>
            <p className="font-semibold">{formatBytes(report.performance.bytes_written)}</p>
          </div>
          <div>
            <p className="text-muted-foreground">Features</p>
            <p>{report.performance.features_extracted.toLocaleString()}</p>
          </div>
          <div>
            <p className="text-muted-foreground">Isoforms</p>
            <p>{report.performance.isoforms_extracted.toLocaleString()}</p>
          </div>
          <div>
            <p className="text-muted-foreground">PTM Mapped</p>
            <p className="text-green-600">{report.performance.ptm_mapped.toLocaleString()}</p>
          </div>
          <div>
            <p className="text-muted-foreground">PTM Failed</p>
            <p className="text-red-600">{report.performance.ptm_failed.toLocaleString()}</p>
          </div>
        </div>
      </div>

      {/* Resources */}
      <div>
        <h4 className="font-semibold mb-2">Resource Usage</h4>
        <div className="grid grid-cols-3 gap-4 text-sm">
          <div>
            <p className="text-muted-foreground">Peak Memory</p>
            <p className="font-semibold">{report.resources.peak_rss_mb.toFixed(0)} MB</p>
          </div>
          <div>
            <p className="text-muted-foreground">Peak CPU</p>
            <p className="font-semibold">{report.resources.peak_cpu_percent.toFixed(1)}%</p>
          </div>
          <div>
            <p className="text-muted-foreground">Avg Channel Fullness</p>
            <p className="font-semibold">{report.resources.avg_channel_fullness_percent.toFixed(1)}%</p>
          </div>
        </div>
      </div>

      {/* Bottleneck Analysis */}
      <div>
        <h4 className="font-semibold mb-2">Bottleneck Analysis</h4>
        <div className="bg-muted rounded-md p-4">
          <p className="font-medium">{report.bottleneck.diagnosis}</p>
          <p className="text-sm text-muted-foreground mt-1">
            Confidence: {(report.bottleneck.confidence * 100).toFixed(0)}%
          </p>
          {report.bottleneck.recommendations.length > 0 && (
            <div className="mt-3">
              <p className="text-sm font-medium">Recommendations:</p>
              <ul className="list-disc list-inside text-sm text-muted-foreground mt-1">
                {report.bottleneck.recommendations.map((rec, i) => (
                  <li key={i}>{rec}</li>
                ))}
              </ul>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
