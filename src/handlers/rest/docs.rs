use axum::{Json, response::IntoResponse};
use streamer_core::docs::all_components;

/// The component reference, as data.
///
/// The `/docs` *page* is a Leptos route that generates the same thing in the
/// browser from the same `streamer-core` code, so this endpoint isn't what
/// renders it. It exists because the component reference is useful to things
/// that aren't a browser — a config linter, an editor completion, a test — and
/// because it's the schema reflection's HTTP-visible contract.
// axum handlers have to be async even when they do no awaiting
#[allow(clippy::unused_async)]
pub async fn get_docs() -> impl IntoResponse {
    Json(all_components())
}
