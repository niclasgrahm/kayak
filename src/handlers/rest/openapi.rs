use axum::{Json, response::Html, response::IntoResponse};

use crate::openapi;

/// This API as an `OpenAPI` 3.1 document. Documented in
/// `kayak_core::api_docs` under `Operation::GetOpenApi`.
// axum handlers have to be async even when they do no awaiting
#[allow(clippy::unused_async)]
pub async fn get_openapi() -> impl IntoResponse {
    Json(openapi::document())
}

/// The rendered reference page. Documented in `kayak_core::api_docs` under
/// `Operation::ApiReference`.
#[allow(clippy::unused_async)]
pub async fn api_reference() -> impl IntoResponse {
    Html(openapi::reference_page())
}
