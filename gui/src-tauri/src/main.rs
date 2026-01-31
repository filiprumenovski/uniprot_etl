//! UniProt ETL Desktop GUI entry point.

// Prevents additional console window on Windows in release
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    uniprot_etl_gui_lib::run()
}
