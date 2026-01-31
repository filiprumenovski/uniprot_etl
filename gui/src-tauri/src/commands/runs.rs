//! Run history management commands.

use std::fs;
use std::path::PathBuf;

use serde::Serialize;
use tauri::{AppHandle, Manager};

fn default_runs_dir(app: &AppHandle) -> PathBuf {
    app.path()
        .app_data_dir()
        .map(|dir| dir.join("runs"))
        .unwrap_or_else(|_| PathBuf::from("runs"))
}

/// Summary of a pipeline run for the history list.
#[derive(Serialize)]
pub struct RunSummary {
    pub run_id: String,
    pub timestamp: String,
    pub status: String,
    pub duration_secs: f64,
    pub entries_parsed: u64,
}

/// List all runs from the runs directory.
#[tauri::command]
pub fn list_runs(app: AppHandle, runs_dir: Option<String>) -> Result<Vec<RunSummary>, String> {
    let dir = runs_dir
        .map(PathBuf::from)
        .unwrap_or_else(|| default_runs_dir(&app));

    if !dir.exists() {
        return Ok(vec![]);
    }

    let mut runs: Vec<RunSummary> = vec![];

    for entry in fs::read_dir(&dir).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();

        if path.is_dir() {
            let report_path = path.join("report.yaml");
            if report_path.exists() {
                if let Ok(content) = fs::read_to_string(&report_path) {
                    if let Ok(report) = serde_yaml::from_str::<serde_yaml::Value>(&content) {
                        let status = if let Some(s) = report["status"].as_str() {
                            s.to_string()
                        } else if report["status"].get("Error").is_some() {
                            "Error".to_string()
                        } else {
                            "Unknown".to_string()
                        };

                        runs.push(RunSummary {
                            run_id: report["run_id"].as_str().unwrap_or("unknown").to_string(),
                            timestamp: report["timestamp"].as_str().unwrap_or("").to_string(),
                            status,
                            duration_secs: report["duration_secs"].as_f64().unwrap_or(0.0),
                            entries_parsed: report["performance"]["entries_parsed"]
                                .as_u64()
                                .unwrap_or(0),
                        });
                    }
                }
            }
        }
    }

    // Sort by timestamp descending (newest first)
    runs.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

    Ok(runs)
}

/// Get full report for a specific run.
#[tauri::command]
pub fn get_run_report(
    app: AppHandle,
    run_id: String,
    runs_dir: Option<String>,
) -> Result<serde_json::Value, String> {
    let dir = runs_dir
        .map(PathBuf::from)
        .unwrap_or_else(|| default_runs_dir(&app));

    let report_path = dir.join(&run_id).join("report.yaml");

    if !report_path.exists() {
        return Err(format!("Report not found for run: {}", run_id));
    }

    let content = fs::read_to_string(&report_path).map_err(|e| e.to_string())?;
    let yaml: serde_yaml::Value = serde_yaml::from_str(&content).map_err(|e| e.to_string())?;

    // Convert YAML to JSON for frontend
    serde_json::to_value(&yaml).map_err(|e| e.to_string())
}

/// Delete a run directory.
#[tauri::command]
pub fn delete_run(app: AppHandle, run_id: String, runs_dir: Option<String>) -> Result<(), String> {
    let dir = runs_dir
        .map(PathBuf::from)
        .unwrap_or_else(|| default_runs_dir(&app));

    let run_path = dir.join(&run_id);

    if !run_path.exists() {
        return Err(format!("Run not found: {}", run_id));
    }

    fs::remove_dir_all(&run_path).map_err(|e| e.to_string())
}
