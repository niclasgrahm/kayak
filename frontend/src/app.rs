use leptos::prelude::*;
use leptos_meta::*;
use leptos_use::{UseEventSourceReturn, use_event_source};
use std::collections::{HashMap, VecDeque};
use streamer_core::{StreamerDto, StreamerId, UiEvent, config::Config};

use crate::api_client::{ApiClient, ApiError};


pub fn shell(options: LeptosOptions) -> impl IntoView {
    view! {
        <!DOCTYPE html>
        <html lang="en">
            <head>
                <meta charset="utf-8" />
                <meta name="viewport" content="width=device-width,initial-scale=1" />
                <AutoReload options=options.clone() />
                <HydrationScripts options />
                <Stylesheet id="leptos" href="/pkg/streamer.css" />
                <MetaTags />
            </head>
            <body>
                <App />
            </body>
        </html>
    }
}

#[derive(Clone, Copy)]
pub struct AppState {
    pub streams: LocalResource<Result<Vec<StreamerDto>, ApiError>>,
    pub events: Signal<Option<UiEvent>>,
    pub canvas_state: CanvasState,
}

#[component]
pub fn App() -> impl IntoView {
    provide_meta_context();
    let streams = LocalResource::new(|| async move {
        ApiClient {
            base: String::new(),
        }
        .list_streams()
        .await
    });
    let UseEventSourceReturn { data, .. } =
        use_event_source::<UiEvent, codee::string::JsonSerdeCodec>("/events");

    let canvas_state = CanvasState {
        cards: RwSignal::new(HashMap::new()),
        camera: RwSignal::new(Camera {
            x: 0.0,
            y: 0.0,
            zoom: 1.0,
        }),
    };

    provide_context(AppState {
        streams,
        events: data,
        canvas_state,
    });
    view! {
        <Suspense fallback=move || view! { <p>"Loading streams..."</p> }>
            <Navbar />
            <div class="main-content">
                <Sidebar />
                <div
                    class="nodes"
                    style:background-position=move || {
                        let c = canvas_state.camera.get();
                        format!("{}px {}px", -c.x * c.zoom, -c.y * c.zoom)
                    }
                    style:background-size=move || {
                        let c = canvas_state.camera.get();
                        format!("{0}px {0}px", 24.0 * c.zoom)
                    }
                    on:wheel=move |ev| {
                        ev.prevent_default();
                        canvas_state
                            .camera
                            .update(|c| {
                                c.x += ev.delta_x() / c.zoom;
                                c.y += ev.delta_y() / c.zoom;
                            })
                    }
                >
                    {move || {
                        streams
                            .get()
                            .map(|res| match res {
                                Ok(list) => {
                                    view! {
                                        <div
                                            class="surface"
                                            style:transform=move || {
                                                let c = canvas_state.camera.get();
                                                format!(
                                                    "scale({}) translate({}px, {}px)",
                                                    c.zoom,
                                                    -c.x,
                                                    -c.y,
                                                )
                                            }
                                        >
                                            <For each=move || list.clone() key=|s| s.id.clone() let:s>
                                                <Card
                                                    streamer_id=s.id.clone()
                                                    events=data
                                                    config=s.config.clone()
                                                    geom=canvas_state.geom(&s.id)
                                                />
                                            </For>

                                        </div>
                                    }
                                        .into_any()
                                }
                                Err(err) => {

                                    view! { <p>"error: " {err.to_string()}</p> }
                                        .into_any()
                                }
                            })
                    }}
                </div>
            </div>
        </Suspense>
    }
}

#[component]
pub fn Sidebar() -> impl IntoView {
    let state = expect_context::<AppState>();
    view! {
        <div class="sidebar">
            {move || {
                state
                    .streams
                    .get()
                    .map(|res| match res {
                        Ok(list) => {
                            view! {
                                <For each=move || list.clone() key=|s| s.id.clone() let:s>
                                    <div
                                        class="tree-item"
                                        on:click=move |_| {
                                            let g = state.canvas_state.geom(&s.id).get_untracked();
                                            state
                                                .canvas_state
                                                .camera
                                                .update(|c| {
                                                    c.x = g.x - 40.0;
                                                    c.y = g.y - 40.0;
                                                });
                                        }
                                    >
                                        {s.id.clone()}
                                    </div>
                                </For>
                            }
                                .into_any()
                        }
                        Err(err) => view! { <p>"error: " {err.to_string()}</p> }.into_any(),
                    })
            }}
        </div>
    }
}

#[component]
pub fn Navbar() -> impl IntoView {
    view! {
        <aside class="navbar">
            <div>"navb"</div>
        </aside>
    }
}

#[derive(Clone, Copy, PartialEq)]
pub struct CardGeom {
    pub x: f64,
    pub y: f64,
    pub width: f64,
    pub height: f64,
}

#[derive(Clone, Copy, PartialEq)]
pub struct Camera {
    pub x: f64,
    pub y: f64,
    pub zoom: f64,
}

#[derive(Clone, Copy)]
pub struct CanvasState {
    pub cards: RwSignal<HashMap<StreamerId, RwSignal<CardGeom>>>,
    pub camera: RwSignal<Camera>,
}

impl CanvasState {
    pub fn geom(&self, id: &StreamerId) -> RwSignal<CardGeom> {
        if let Some(sig) = self.cards.with_untracked(|m| m.get(id).copied()) {
            return sig;
        }
        let i = self.cards.with_untracked(|m| m.len());
        let sig = RwSignal::new(CardGeom {
            x: 40.0 + i as f64 * 500.0,
            y: 40.0,
            width: 340.0,
            height: 0.0,
        });
        self.cards.update_untracked(|m| {
            m.insert(id.clone(), sig);
        });
        sig
    }
}
#[component]
pub fn Card(streamer_id: StreamerId, config: Config, events: Signal<Option<UiEvent>>, geom: RwSignal<CardGeom>) -> impl IntoView {
    let messages = RwSignal::new(VecDeque::<(u64, String)>::with_capacity(10));
    let next_id = RwSignal::new(0u64);
    let id = streamer_id.clone();
    
    Effect::new(move |_| {
        if let Some(ev) = events.get()
            && ev.streamer_id == id
        {
            messages.update(|log| {
                for msg in ev.batch.iter() {
                    if log.len() == 10 {
                        log.pop_front();
                    }
                    let id = next_id.get_untracked();
                    next_id.set(id + 1);
                    log.push_back((id, msg.to_string()));
                }
            });
        }
    });
    let log_ref = NodeRef::<leptos::html::Div>::new();


    Effect::new(move |_| {
        messages.track();
        if let Some(el) = log_ref.get() {
            el.set_scroll_top(el.scroll_height());
        }
    });
    // an empty pane would look like "no config"; show why it's missing instead
    let json = serde_json::to_string_pretty(&config)
        .unwrap_or_else(|e| format!("<could not render config: {e}>"));
    view! {
        <div
            class="card"
            style:left=move || format!("{}px", geom.get().x)
            style:top=move || format!("{}px", geom.get().y)
        >
            <header>{streamer_id.clone()}</header>
            <div class="config">
                <pre>{json}</pre>
            </div>
            <div class="messages" node_ref=log_ref>
                <For each=move || messages.get() key=|(i, _)| *i let:entry>
                    <div>{entry.1}</div>
                </For>
            </div>
        </div>
    }
}
