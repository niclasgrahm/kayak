// The release (optimised) build of the hydrate target overflows rustc's default
// type-layout query depth on the nested `<For>` closures in the /docs page.
// Only the release build reaches it — `cargo leptos build --release` fails
// without this, while `cargo leptos watch` is fine. Which is why it rots
// quietly: nothing in the development loop or in `just ci` compiles this
// target optimised, so the limit only ever proves too low when someone builds
// for production. It has been raised once already, as the card's log grew; if
// a release build starts failing here again, that is what has happened, and
// the number rustc names in the error is the number to put here.
#![recursion_limit = "512"]

pub mod api_client;
pub mod api_docs;
pub mod app;
pub mod docs;
pub mod embed;
pub mod form;
pub mod graph;
pub mod inspector;
pub mod log;
pub mod pretty;
pub mod rhai;
pub mod selection;
pub mod sidebar;
pub mod stats;

#[cfg(feature = "hydrate")]
#[wasm_bindgen::prelude::wasm_bindgen]
pub fn hydrate() {
    console_error_panic_hook::set_once();
    leptos::mount::hydrate_body(app::App);
}
