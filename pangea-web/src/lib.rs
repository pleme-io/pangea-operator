//! Pangea Web UI - Yew WASM application for infrastructure management.

mod app;
mod client;
mod components;
mod pages;
mod schema;
mod state;

use wasm_bindgen::prelude::*;

/// Entry point for the WASM application.
#[wasm_bindgen(start)]
pub fn run() {
    wasm_logger::init(wasm_logger::Config::default());
    log::info!("Pangea Web UI starting...");
    yew::Renderer::<app::App>::new().render();
}
