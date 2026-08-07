use std::sync::Arc;

use tokio_stream::wrappers::{BroadcastStream, errors::BroadcastStreamRecvError};

use askama::Template;
use axum::{
    extract::State,
    response::sse::{Event, KeepAlive, Sse},
    response::{Html, IntoResponse},
};

use futures_util::stream::Stream;
use std::convert::Infallible;
use tokio_stream::StreamExt as _;

use crate::handlers::error::AppError;
use crate::state::AppState;

// axum handlers have to be async even when they do no awaiting
#[allow(clippy::unused_async)]
pub async fn index_handler(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, AppError> {
    #[derive(Template)]
    #[template(path = "index.html")]
    struct Tmpl {
        pipelines: Vec<String>,
    }
    let template = Tmpl {
        pipelines: state.get_pipeline_ids(),
    };
    Ok(Html(template.render()?))
}

// pub async fn topology_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
//     // returns pipelines and edges
//     todo!()
// }
pub async fn events_handler(
    State(state): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.subscribe_events();
    let stream = BroadcastStream::new(rx).map(|item| {
        let event = match item {
            // a message that won't serialize is reported to the client as an
            // error event; killing the whole SSE stream over it would be worse
            Ok(ev) => Event::default().json_data(&ev).unwrap_or_else(|e| {
                tracing::error!("failed to serialize ui event: {e}");
                Event::default().event("error").data(e.to_string())
            }),
            Err(BroadcastStreamRecvError::Lagged(n)) => {
                Event::default().event("lagged").data(n.to_string())
            }
        };
        Ok(event)
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}
