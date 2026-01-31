//! File and folder picker dialog commands.

use tauri_plugin_dialog::DialogExt;

/// Convert FilePath to String
fn filepath_to_string(fp: tauri_plugin_dialog::FilePath) -> String {
    fp.into_path()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_default()
}

/// Open file picker for input XML file.
#[tauri::command]
pub async fn pick_input_file(app: tauri::AppHandle) -> Result<Option<String>, String> {
    let result = app
        .dialog()
        .file()
        .add_filter("UniProt XML", &["xml", "gz"])
        .set_title("Select UniProt XML File")
        .blocking_pick_file();

    Ok(result.map(filepath_to_string))
}

/// Open folder picker for input directory (swarm mode).
#[tauri::command]
pub async fn pick_input_directory(app: tauri::AppHandle) -> Result<Option<String>, String> {
    let result = app
        .dialog()
        .file()
        .set_title("Select Input Directory (Swarm Mode)")
        .blocking_pick_folder();

    Ok(result.map(filepath_to_string))
}

/// Open folder picker for output directory.
#[tauri::command]
pub async fn pick_output_directory(app: tauri::AppHandle) -> Result<Option<String>, String> {
    let result = app
        .dialog()
        .file()
        .set_title("Select Output Directory")
        .blocking_pick_folder();

    Ok(result.map(filepath_to_string))
}

/// Open file picker for FASTA sidecar file.
#[tauri::command]
pub async fn pick_fasta_file(app: tauri::AppHandle) -> Result<Option<String>, String> {
    let result = app
        .dialog()
        .file()
        .add_filter("FASTA Files", &["fasta", "fa", "gz"])
        .set_title("Select FASTA Sidecar (Optional)")
        .blocking_pick_file();

    Ok(result.map(filepath_to_string))
}
