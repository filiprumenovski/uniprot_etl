"use client";

import { useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useMetricsPolling } from "@/hooks/useMetricsPolling";
import {
    Activity,
    Gauge,
    Database,
    FileText,
    Clock,
    Zap,
    TrendingUp,
    AlertCircle,
    CheckCircle2,
    Layers,
    GitBranch,
    Target,
    XCircle,
} from "lucide-react";
import {
    LineChart,
    Line,
    XAxis,
    YAxis,
    Tooltip,
    ResponsiveContainer,
    Area,
    AreaChart,
} from "recharts";

interface MetricsDashboardProps {
    height?: number;
    /** Whether to show charts (set false for compact mode) */
    showCharts?: boolean;
    /** Whether polling is active */
    enabled?: boolean;
}

function formatBytes(bytes: number): string {
    if (bytes === 0) return "0 B";
    const k = 1024;
    const sizes = ["B", "KB", "MB", "GB", "TB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}

function formatDuration(seconds: number): string {
    if (seconds < 60) return `${seconds.toFixed(1)}s`;
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    if (mins < 60) return `${mins}m ${secs.toFixed(0)}s`;
    const hours = Math.floor(mins / 60);
    return `${hours}h ${mins % 60}m`;
}

function formatNumber(n: number): string {
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
    return n.toLocaleString();
}

interface StatCardProps {
    title: string;
    value: string | number;
    icon: React.ReactNode;
    subtitle?: string;
    variant?: "default" | "success" | "warning" | "error";
}

function StatCard({ title, value, icon, subtitle, variant = "default" }: StatCardProps) {
    const variantStyles = {
        default: "text-foreground/90",
        success: "text-emerald-500",
        warning: "text-amber-500",
        error: "text-rose-500",
    };

    return (
        <div className="flex flex-col items-center justify-center p-3 rounded-lg bg-card/50 border border-border/40 transition-all hover:bg-muted/50">
            <div className="flex items-center gap-1.5 text-xs font-medium text-muted-foreground/80 mb-1.5">
                {icon}
                <span>{title}</span>
            </div>
            <div className={`text-2xl font-semibold tracking-tight tabular-nums ${variantStyles[variant]}`}>
                {value}
            </div>
            {subtitle && (
                <div className="text-[10px] uppercase tracking-wider text-muted-foreground/70 mt-1">{subtitle}</div>
            )}
        </div>
    );
}

export function MetricsDashboard({
    height = 600,
    showCharts = true,
    enabled = true,
}: MetricsDashboardProps) {
    const { current, timeSeries, isConnected, error } = useMetricsPolling({ enabled });

    // Format time series data for charts
    const chartData = useMemo(() => {
        return timeSeries.map((point, idx) => ({
            time: idx,
            entriesPerSec: Math.round(point.entriesPerSec),
            bytesRead: point.bytesRead,
            bytesWritten: point.bytesWritten,
        }));
    }, [timeSeries]);

    // Calculate success rate
    const successRate = useMemo(() => {
        if (!current) return 0;
        const total = current.ptmMapped + current.ptmFailed;
        if (total === 0) return 100;
        return (current.ptmMapped / total) * 100;
    }, [current]);

    return (
        <Card className="overflow-hidden">
            <CardHeader className="p-4 pb-2 border-b border-border/50">
                <CardTitle className="text-sm font-medium flex items-center justify-between">
                    <span className="flex items-center gap-2">
                        <Activity className="h-4 w-4" />
                        Live Metrics
                    </span>
                    <div className="flex items-center gap-2">
                        {isConnected ? (
                            <span className="flex items-center gap-1.5 text-xs text-green-500">
                                <CheckCircle2 className="h-3 w-3" />
                                Connected
                            </span>
                        ) : (
                            <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
                                <AlertCircle className="h-3 w-3" />
                                {error ? "Disconnected" : "Connecting..."}
                            </span>
                        )}
                    </div>
                </CardTitle>
            </CardHeader>

            <CardContent className="p-4" style={{ height }}>
                {!isConnected && !current ? (
                    <div className="h-full flex flex-col items-center justify-center text-muted-foreground">
                        <Activity className="h-8 w-8 mb-3 opacity-50" />
                        <p className="text-sm font-medium">Waiting for metrics...</p>
                        <p className="text-xs mt-1">
                            {error || "Metrics will appear when the pipeline starts"}
                        </p>
                    </div>
                ) : (
                    <div className="space-y-4 h-full flex flex-col">
                        {/* Stat Grid */}
                        <div className="grid grid-cols-3 md:grid-cols-6 gap-2">
                            <StatCard
                                title="Entries"
                                value={formatNumber(current?.entries ?? 0)}
                                icon={<FileText className="h-3 w-3" />}
                            />
                            <StatCard
                                title="Rate"
                                value={`${Math.round(current?.entriesPerSec ?? 0)}/s`}
                                icon={<Zap className="h-3 w-3" />}
                            />
                            <StatCard
                                title="Batches"
                                value={formatNumber(current?.batches ?? 0)}
                                icon={<Layers className="h-3 w-3" />}
                            />
                            <StatCard
                                title="Features"
                                value={formatNumber(current?.features ?? 0)}
                                icon={<Target className="h-3 w-3" />}
                            />
                            <StatCard
                                title="Isoforms"
                                value={formatNumber(current?.isoforms ?? 0)}
                                icon={<GitBranch className="h-3 w-3" />}
                            />
                            <StatCard
                                title="Elapsed"
                                value={formatDuration(current?.uptimeSeconds ?? 0)}
                                icon={<Clock className="h-3 w-3" />}
                            />
                        </div>

                        {/* Secondary Stats */}
                        <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
                            <StatCard
                                title="PTM Mapped"
                                value={formatNumber(current?.ptmMapped ?? 0)}
                                icon={<CheckCircle2 className="h-3 w-3" />}
                                variant="success"
                            />
                            <StatCard
                                title="PTM Failed"
                                value={formatNumber(current?.ptmFailed ?? 0)}
                                icon={<XCircle className="h-3 w-3" />}
                                variant={current?.ptmFailed ? "error" : "default"}
                            />
                            <StatCard
                                title="Success Rate"
                                value={`${successRate.toFixed(1)}%`}
                                icon={<TrendingUp className="h-3 w-3" />}
                                variant={successRate >= 95 ? "success" : successRate >= 80 ? "warning" : "error"}
                            />
                            <StatCard
                                title="Backpressure"
                                value={`${((current?.backpressureRatio ?? 0) * 100).toFixed(0)}%`}
                                icon={<Gauge className="h-3 w-3" />}
                                variant={
                                    (current?.backpressureRatio ?? 0) > 0.8
                                        ? "error"
                                        : (current?.backpressureRatio ?? 0) > 0.5
                                            ? "warning"
                                            : "default"
                                }
                            />
                        </div>

                        {/* I/O Stats */}
                        <div className="grid grid-cols-2 gap-2">
                            <StatCard
                                title="Bytes Read"
                                value={formatBytes(current?.bytesRead ?? 0)}
                                icon={<Database className="h-3 w-3" />}
                            />
                            <StatCard
                                title="Bytes Written"
                                value={formatBytes(current?.bytesWritten ?? 0)}
                                icon={<Database className="h-3 w-3" />}
                            />
                        </div>

                        {/* Charts */}
                        {showCharts && chartData.length > 1 && (
                            <div className="flex-1 grid md:grid-cols-2 gap-4 min-h-0">
                                {/* Parsing Rate Chart */}
                                <div className="rounded-lg bg-muted/30 p-3 border border-border/30">
                                    <div className="text-xs font-medium text-muted-foreground mb-2 flex items-center gap-1.5">
                                        <Zap className="h-3 w-3" />
                                        Parsing Rate (entries/sec)
                                    </div>
                                    <div className="h-32">
                                        <ResponsiveContainer width="100%" height="100%">
                                            <AreaChart data={chartData}>
                                                <defs>
                                                    <linearGradient id="colorRate" x1="0" y1="0" x2="0" y2="1">
                                                        <stop offset="5%" stopColor="hsl(var(--primary))" stopOpacity={0.3} />
                                                        <stop offset="95%" stopColor="hsl(var(--primary))" stopOpacity={0} />
                                                    </linearGradient>
                                                </defs>
                                                <XAxis dataKey="time" hide />
                                                <YAxis
                                                    hide
                                                    domain={[0, "auto"]}
                                                />
                                                <Tooltip
                                                    contentStyle={{
                                                        backgroundColor: "hsl(var(--background))",
                                                        border: "1px solid hsl(var(--border))",
                                                        borderRadius: "6px",
                                                        fontSize: "12px",
                                                    }}
                                                    formatter={(value) => [
                                                        `${(typeof value === "number" ? value : 0).toLocaleString()}/s`,
                                                        "Rate",
                                                    ]}
                                                    labelFormatter={() => ""}
                                                />
                                                <Area
                                                    type="monotone"
                                                    dataKey="entriesPerSec"
                                                    stroke="hsl(var(--primary))"
                                                    strokeWidth={2}
                                                    fill="url(#colorRate)"
                                                />
                                            </AreaChart>
                                        </ResponsiveContainer>
                                    </div>
                                </div>

                                {/* I/O Chart */}
                                <div className="rounded-lg bg-muted/30 p-3 border border-border/30">
                                    <div className="text-xs font-medium text-muted-foreground mb-2 flex items-center gap-1.5">
                                        <Database className="h-3 w-3" />
                                        I/O Bytes
                                    </div>
                                    <div className="h-32">
                                        <ResponsiveContainer width="100%" height="100%">
                                            <LineChart data={chartData}>
                                                <XAxis dataKey="time" hide />
                                                <YAxis
                                                    hide
                                                    domain={[0, "auto"]}
                                                />
                                                <Tooltip
                                                    contentStyle={{
                                                        backgroundColor: "hsl(var(--background))",
                                                        border: "1px solid hsl(var(--border))",
                                                        borderRadius: "6px",
                                                        fontSize: "12px",
                                                    }}
                                                    formatter={(value, name) => [
                                                        formatBytes(typeof value === "number" ? value : 0),
                                                        name === "bytesRead" ? "Read" : "Written",
                                                    ]}
                                                    labelFormatter={() => ""}
                                                />
                                                <Line
                                                    type="monotone"
                                                    dataKey="bytesRead"
                                                    stroke="hsl(142, 76%, 36%)"
                                                    strokeWidth={2}
                                                    dot={false}
                                                />
                                                <Line
                                                    type="monotone"
                                                    dataKey="bytesWritten"
                                                    stroke="hsl(221, 83%, 53%)"
                                                    strokeWidth={2}
                                                    dot={false}
                                                />
                                            </LineChart>
                                        </ResponsiveContainer>
                                    </div>
                                    <div className="flex justify-center gap-4 mt-1">
                                        <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
                                            <span className="w-2 h-2 rounded-full bg-green-500" />
                                            Read
                                        </span>
                                        <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
                                            <span className="w-2 h-2 rounded-full bg-blue-500" />
                                            Written
                                        </span>
                                    </div>
                                </div>
                            </div>
                        )}
                    </div>
                )}
            </CardContent>
        </Card>
    );
}
