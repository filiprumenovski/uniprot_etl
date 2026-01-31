"use client";

import { useState, useEffect, useCallback, useRef } from "react";

export interface MetricsData {
    // Counters
    entries: number;
    batches: number;
    features: number;
    isoforms: number;
    bytesRead: number;
    bytesWritten: number;
    ptmMapped: number;
    ptmFailed: number;
    // Gauges
    uptimeSeconds: number;
    backpressureRatio: number;
    // Derived
    entriesPerSec: number;
    timestamp: number;
}

export interface MetricsTimeSeries {
    timestamp: number;
    entries: number;
    entriesPerSec: number;
    bytesRead: number;
    bytesWritten: number;
}

interface UseMetricsPollingOptions {
    /** Polling interval in milliseconds (default: 1000) */
    intervalMs?: number;
    /** Maximum data points to retain for time series (default: 300 = 5 min at 1s interval) */
    maxDataPoints?: number;
    /** Metrics endpoint URL */
    metricsUrl?: string;
    /** Whether polling is active */
    enabled?: boolean;
}

interface UseMetricsPollingResult {
    /** Current metrics snapshot */
    current: MetricsData | null;
    /** Time-series data for charts */
    timeSeries: MetricsTimeSeries[];
    /** Whether we're connected to the metrics server */
    isConnected: boolean;
    /** Last error message if any */
    error: string | null;
    /** Force refresh metrics */
    refresh: () => void;
    /** Clear all historical data */
    reset: () => void;
}

const DEFAULT_METRICS: MetricsData = {
    entries: 0,
    batches: 0,
    features: 0,
    isoforms: 0,
    bytesRead: 0,
    bytesWritten: 0,
    ptmMapped: 0,
    ptmFailed: 0,
    uptimeSeconds: 0,
    backpressureRatio: 0,
    entriesPerSec: 0,
    timestamp: Date.now(),
};

/**
 * Parse Prometheus text exposition format into a key-value map.
 * Handles lines like:
 *   metric_name 123
 *   metric_name{label="value"} 456
 */
function parsePrometheusText(text: string): Record<string, number> {
    const result: Record<string, number> = {};

    for (const line of text.split("\n")) {
        // Skip empty lines and comments
        if (!line || line.startsWith("#")) continue;

        // Find the last space separating metric from value
        const lastSpace = line.lastIndexOf(" ");
        if (lastSpace === -1) continue;

        const metricPart = line.substring(0, lastSpace);
        const valuePart = line.substring(lastSpace + 1);

        // Extract metric name (strip labels)
        const braceIdx = metricPart.indexOf("{");
        const metricName = braceIdx === -1 ? metricPart : metricPart.substring(0, braceIdx);

        const value = parseFloat(valuePart);
        if (!isNaN(value)) {
            result[metricName] = value;
        }
    }

    return result;
}

/**
 * Convert parsed Prometheus metrics to our MetricsData structure.
 */
function metricsFromParsed(parsed: Record<string, number>, prevEntries: number, prevTimestamp: number): MetricsData {
    const now = Date.now();
    const entries = parsed["uniprot_etl_entries_total"] ?? 0;

    // Calculate rate (entries per second)
    const deltaTime = (now - prevTimestamp) / 1000;
    const deltaEntries = entries - prevEntries;
    const entriesPerSec = deltaTime > 0 ? deltaEntries / deltaTime : 0;

    return {
        entries,
        batches: parsed["uniprot_etl_batches_total"] ?? 0,
        features: parsed["uniprot_etl_features_total"] ?? 0,
        isoforms: parsed["uniprot_etl_isoforms_total"] ?? 0,
        bytesRead: parsed["uniprot_etl_bytes_read_total"] ?? 0,
        bytesWritten: parsed["uniprot_etl_bytes_written_total"] ?? 0,
        ptmMapped: parsed["uniprot_etl_ptm_mapped_total"] ?? 0,
        ptmFailed: parsed["uniprot_etl_ptm_failed_total"] ?? 0,
        uptimeSeconds: parsed["uniprot_etl_uptime_seconds"] ?? 0,
        backpressureRatio: parsed["uniprot_etl_pipeline_backpressure_ratio"] ?? 0,
        entriesPerSec: Math.max(0, entriesPerSec),
        timestamp: now,
    };
}

export function useMetricsPolling(options: UseMetricsPollingOptions = {}): UseMetricsPollingResult {
    const {
        intervalMs = 1000,
        maxDataPoints = 300,
        metricsUrl = "http://127.0.0.1:9090/metrics",
        enabled = true,
    } = options;

    const [current, setCurrent] = useState<MetricsData | null>(null);
    const [timeSeries, setTimeSeries] = useState<MetricsTimeSeries[]>([]);
    const [isConnected, setIsConnected] = useState(false);
    const [error, setError] = useState<string | null>(null);

    // Track previous values for rate calculation
    const prevRef = useRef<{ entries: number; timestamp: number }>({
        entries: 0,
        timestamp: Date.now(),
    });

    const fetchMetrics = useCallback(async () => {
        try {
            const response = await fetch(metricsUrl, {
                cache: "no-store",
                headers: { "Accept": "text/plain" },
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

            const text = await response.text();
            const parsed = parsePrometheusText(text);

            const metrics = metricsFromParsed(
                parsed,
                prevRef.current.entries,
                prevRef.current.timestamp
            );

            // Update previous values for next rate calculation
            prevRef.current = {
                entries: metrics.entries,
                timestamp: metrics.timestamp,
            };

            setCurrent(metrics);
            setIsConnected(true);
            setError(null);

            // Append to time series (sliding window)
            setTimeSeries((prev) => {
                const newPoint: MetricsTimeSeries = {
                    timestamp: metrics.timestamp,
                    entries: metrics.entries,
                    entriesPerSec: metrics.entriesPerSec,
                    bytesRead: metrics.bytesRead,
                    bytesWritten: metrics.bytesWritten,
                };

                const updated = [...prev, newPoint];
                // Keep only last maxDataPoints
                if (updated.length > maxDataPoints) {
                    return updated.slice(-maxDataPoints);
                }
                return updated;
            });

        } catch (err) {
            setIsConnected(false);
            setError(err instanceof Error ? err.message : "Failed to fetch metrics");
        }
    }, [metricsUrl, maxDataPoints]);

    const reset = useCallback(() => {
        setCurrent(null);
        setTimeSeries([]);
        prevRef.current = { entries: 0, timestamp: Date.now() };
    }, []);

    // Polling effect
    useEffect(() => {
        if (!enabled) return;

        // Initial fetch
        fetchMetrics();

        // Set up interval
        const intervalId = setInterval(fetchMetrics, intervalMs);

        return () => clearInterval(intervalId);
    }, [enabled, intervalMs, fetchMetrics]);

    return {
        current,
        timeSeries,
        isConnected,
        error,
        refresh: fetchMetrics,
        reset,
    };
}
