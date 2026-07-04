use leptos::prelude::*;
use leptos_meta::*;
use leptos_use::{UseEventSourceReturn, use_event_source};
use std::collections::{HashMap, VecDeque};
use streamer_core::{StreamerDto, StreamerId, UiEvent};

use crate::api_client::{ApiClient, ApiError};

fn layout(streams: &[StreamerDto]) -> HashMap<StreamerId, (f64, f64)> {
    streams
        .iter()
        .enumerate()
        .map(|(i, s)| (s.id.clone(), (40.0 + (i as f64) * 280.0, 40.0)))
        .collect()
}

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
    let UseEventSourceReturn {
        data, ready_state, ..
    } = use_event_source::<UiEvent, codee::string::JsonSerdeCodec>("/events");

    provide_context(AppState {
        streams,
        events: data,
    });
    view! {
        <Suspense fallback=move || view! { <p>"Loading streams..."</p> }>
            <Navbar />
            <div class="main-content">
                <Sidebar />
                <div class="nodes">
                    {move || {
                        streams
                            .get()
                            .map(|res| match res {
                                Ok(list) => {
                                    let positions = layout(&list);

                                    view! {
                                        <For each=move || list.clone() key=|s| s.id.clone() let:s>
                                            <Card
                                                streamer_id=s.id.clone()
                                                events=data
                                                x=positions.get(&s.id).copied().unwrap_or_default().0
                                                y=positions.get(&s.id).copied().unwrap_or_default().1
                                            />
                                        </For>
                                    }
                                        .into_any()
                                }
                                Err(err) => view! { <p>"error: " {err.to_string()}</p> }.into_any(),
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
                                    <div>{s.id.clone()}</div>
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
        <aside>
            <div>"navb"</div>
        </aside>
    }
}
#[component]
pub fn Card(streamer_id: StreamerId, events: Signal<Option<UiEvent>>, x: f64, y:f64) -> impl IntoView {
    let messages = RwSignal::new(VecDeque::<(u64, String)>::with_capacity(10));
    let next_id = RwSignal::new(0u64);
    let id = streamer_id.clone();
    
    Effect::new(move |_| {
        if let Some(ev) = events.get() {
            if ev.streamer_id == id {
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
        }
    });
    view! {
        <div class="card" style:left=format!("{x}px") style:top=format!("{y}px")>
            <header>{streamer_id.clone()}</header>
            <div>
                <For each=move || messages.get() key=|(i, _)| *i let:entry>
                    <div>{entry.1}</div>
                </For>
            </div>
        </div>
    }
}
