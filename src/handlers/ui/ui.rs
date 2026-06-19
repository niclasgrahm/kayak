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

use crate::state::AppState;

pub async fn index_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    #[derive(Template)]
    #[template(path = "index.html")]
    struct Tmpl {
        streamers: Vec<String>,
    }
    let streamers = state.get_streamer_ids().unwrap_or_default();
    let template = Tmpl { streamers };
    Html(template.render().unwrap())
}

// pub async fn topology_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
//     // returns nodes and edges
//     todo!()
// }
pub async fn events_handler(
    State(state): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.subscribe_events();
    let stream = BroadcastStream::new(rx).map(|item| {
        let event = match item {
            Ok(ev) => Event::default().json_data(&ev).unwrap(),
            Err(BroadcastStreamRecvError::Lagged(n)) => {
                Event::default().event("lagged").data(n.to_string())
            }
        };
        Ok(event)
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}
