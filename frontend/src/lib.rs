// The release (optimised) build of the hydrate target overflows rustc's default
// type-layout query depth on the nested `<For>` closures in the /docs page.
// Only the release build reaches it — `cargo leptos build --release` fails
// without this, while `cargo leptos watch` is fine.
#![recursion_limit = "256"]

pub mod api_client;
pub mod app;
pub mod docs;
pub mod graph;
pub mod inspector;
pub mod log;

#[cfg(feature = "hydrate")]
#[wasm_bindgen::prelude::wasm_bindgen]
pub fn hydrate() {
    console_error_panic_hook::set_once();
    leptos::mount::hydrate_body(app::App);
}
