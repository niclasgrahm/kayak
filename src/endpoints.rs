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

use axum::routing::{MethodRouter, delete, get, post, put};
use axum::{Router, handler::Handler};
use kayak_core::api_docs::{ApiDoc, Method, Operation, endpoints};

use crate::handlers::{
    rest::{
        connection::{create_connection, delete_connection, get_connections},
        docs::get_docs,
        layout::{get_layout, put_layout},
        openapi::{api_reference, get_openapi},
        pipeline::{create_pipeline, delete_pipeline, get_pipelines},
        settings::{get_settings, revert_config, save_config},
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
            router.route(doc.path, handler_for(doc))
        })
        .with_state(state)
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
        Operation::ListConnections => route_of(method, get_connections),
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
