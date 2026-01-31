//! UniProt ETL GUI library.

pub mod commands;
pub mod state;

use state::AppState;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_shell::init())
        .manage(AppState::new())
        .invoke_handler(tauri::generate_handler![
            // Pipeline commands
            commands::pipeline::start_pipeline,
            commands::pipeline::get_live_metrics,
            commands::pipeline::cancel_pipeline,
            // Dialog commands
            commands::dialogs::pick_input_file,
            commands::dialogs::pick_input_directory,
            commands::dialogs::pick_output_directory,
            commands::dialogs::pick_fasta_file,
            // Runs commands
            commands::runs::list_runs,
            commands::runs::get_run_report,
            commands::runs::delete_run,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
