use std::sync::Arc;

use askama::Template;
use axum::{
    extract::State,
    response::{Html, IntoResponse},
};

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
