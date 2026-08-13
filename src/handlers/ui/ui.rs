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
    let (watching, rx) = state.subscribe_events();
    // The guard rides in the closure, so the watcher count falls exactly when
    // this stream is dropped — which is when the browser goes away. It cannot
    // live outside the stream: `BroadcastStream` takes the receiver by value,
    // and a guard dropped at the end of this function would tell every run loop
    // nobody is watching while the stream is still being served.
    let stream = BroadcastStream::new(rx).map(move |item| {
        let _watching = &watching;
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
