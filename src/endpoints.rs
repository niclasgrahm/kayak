//! Where the documented endpoints and the running handlers meet.
//!
//! `api_router` is a fold over [`kayak_core::api_docs::endpoints`] rather than a
//! list of `.route()` calls, which is the whole point: a route that isn't in the
//! table doesn't get registered, so the documentation can't describe a server
//! that doesn't exist. The other direction is the compiler's job — [`handler_for`]
//! matches on [`Operation`], so a new table entry fails to build until it has a
//! handler.
//!
//! [`route_of`] is what keeps the *method* honest too. It takes the method from
//! the table rather than from the call site, so an entry documented as a `PUT`
//! and wired to `post(...)` isn't expressible.

use std::sync::Arc;

use axum::extract::Request;
use axum::middleware::Next;
use axum::routing::{MethodRouter, delete, get, post, put};
use axum::{Router, handler::Handler};
use kayak_core::api_docs::{ApiDoc, Method, Operation, endpoints};

use crate::handlers::{
    rest::{
        auth::{login, logout, token_login, whoami},
        connection::{create_connection, delete_connection, get_connections},
        docs::get_docs,
        dry_run::dry_run_pipeline,
        history::get_pipeline_history,
        layout::{get_layout, put_layout},
        openapi::{api_reference, get_openapi},
        pipeline::{create_pipeline, delete_pipeline, get_pipelines, ingest_messages},
        sample::sample_input,
        script::dry_run_script,
        settings::{get_settings, revert_config, save_config},
        state::{get_state_bucket, get_state_buckets},
    },
    ui::ui::events_handler,
};
use crate::state::AppState;

/// The JSON/SSE surface, without the Leptos routes. `main` merges this with the
/// frontend router; tests call it directly through `tower::ServiceExt::oneshot`,
/// which keeps them off real sockets.
///
/// Two entries can share a path (`GET` and `PUT /api/layout`) — axum merges
/// method routers registered on one path, and the table's uniqueness is per
/// path *and* method.
pub fn api_router(state: Arc<AppState>) -> Router {
    endpoints()
        .iter()
        .fold(Router::new(), |router, doc| {
            router.route(doc.path, guarded(doc, &state))
        })
        .with_state(state)
}

/// One endpoint's handler, behind the access check its table entry declares.
///
/// `route_layer` rather than `layer`, and the difference is the point: a
/// `route_layer` runs only for requests that **matched this route**, so an
/// unknown path comes back as the router's own empty 404 and never as a 401.
/// "This endpoint does not exist" and "you may not have this endpoint" staying
/// different answers is what keeps `every_documented_endpoint_is_routed_at_its_documented_method`
/// meaningful — and a 401 on every typo would be a poor way to learn the API.
///
/// The access comes off `doc`, so this is the *same fact* the reference page
/// and the `OpenAPI` spec render. There is no second table to fall out of step.
fn guarded(doc: &ApiDoc, state: &Arc<AppState>) -> MethodRouter<Arc<AppState>> {
    let access = doc.access;
    let state = Arc::clone(state);
    handler_for(doc).route_layer(axum::middleware::from_fn(
        move |request: Request, next: Next| {
            let state = Arc::clone(&state);
            async move { crate::auth::authorize(access, state.auth(), request, next).await }
        },
    ))
}

/// The handler serving one documented operation.
///
/// Exhaustive over [`Operation`] on purpose: this match is the reason a new
/// endpoint can't be documented and left unrouted.
fn handler_for(doc: &ApiDoc) -> MethodRouter<Arc<AppState>> {
    let method = doc.method;
    match doc.operation {
        Operation::ListPipelines => route_of(method, get_pipelines),
        Operation::CreatePipeline => route_of(method, create_pipeline),
        Operation::DeletePipeline => route_of(method, delete_pipeline),
        Operation::IngestMessages => route_of(method, ingest_messages),
        Operation::ListConnections => route_of(method, get_connections),
        Operation::GetPipelineHistory => route_of(method, get_pipeline_history),
        Operation::DryRunScript => route_of(method, dry_run_script),
        Operation::SampleInput => route_of(method, sample_input),
        Operation::DryRunPipeline => route_of(method, dry_run_pipeline),
        Operation::ListStateBuckets => route_of(method, get_state_buckets),
        Operation::GetStateBucket => route_of(method, get_state_bucket),
        Operation::CreateConnection => route_of(method, create_connection),
        Operation::DeleteConnection => route_of(method, delete_connection),
        Operation::GetSettings => route_of(method, get_settings),
        Operation::SaveConfig => route_of(method, save_config),
        Operation::RevertConfig => route_of(method, revert_config),
        Operation::GetLayout => route_of(method, get_layout),
        Operation::ReplaceLayout => route_of(method, put_layout),
        Operation::StreamEvents => route_of(method, events_handler),
        Operation::ListComponents => route_of(method, get_docs),
        Operation::GetOpenApi => route_of(method, get_openapi),
        Operation::ApiReference => route_of(method, api_reference),
        Operation::Login => route_of(method, login),
        Operation::TokenLogin => route_of(method, token_login),
        Operation::Logout => route_of(method, logout),
        Operation::WhoAmI => route_of(method, whoami),
    }
}

/// Register a handler under the method the table says it serves.
///
/// The indirection is what makes the documented method and the routed method
/// the same fact rather than two facts that agree today.
fn route_of<H, T>(method: Method, handler: H) -> MethodRouter<Arc<AppState>>
where
    H: Handler<T, Arc<AppState>>,
    T: 'static,
{
    match method {
        Method::Get => get(handler),
        Method::Post => post(handler),
        Method::Put => put(handler),
        Method::Delete => delete(handler),
    }
}
