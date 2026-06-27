use leptos::prelude::*;
use leptos_meta::*;
use leptos_use::{UseEventSourceReturn, use_event_source};
use streamer_core::UiEvent;

use crate::api_client::ApiClient;

pub fn shell(options: LeptosOptions) -> impl IntoView {
    view! {
        <!DOCTYPE html>
        <html lang="en">
            <head>
                <meta charset="utf-8"/>
                <meta name="viewport" content="width=device-width,initial-scale=1"/>
                <AutoReload options=options.clone()/>
                <HydrationScripts options/>
                <MetaTags/>
            </head>
            <body>
                <App/>
            </body>
        </html>
    }
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

    view! {
        <h1>"Streamer App"</h1>
        <Suspense fallback=move || view! { <p>"Loading streams..."</p>}>
        {move || streams.get().map(|res| match res {
            Ok(list) => view! {
                <ul>
                    <For each=move || list.clone() key=|s| s.id.clone() let:s>
                        <li>{s.id.clone()}</li>
                    </For>
                </ul>
            }.into_any(),
            Err(err) => view! { <p>"error: " {err.to_string()}</p>}.into_any(),
        })}
        </Suspense>
        <LiveEvents/>
    }
}

#[component]
pub fn LiveEvents() -> impl IntoView {
    let UseEventSourceReturn {
        data, ready_state, ..
    } = use_event_source::<UiEvent, codee::string::JsonSerdeCodec>("/events");
    view! {
    <h2>"live events (" {move || format!("{:?}", ready_state.get())}</h2>
    {move || match data.get() {
        Some(ev) => view!{
            <p>{ev.streamer_id} " / " {ev.stage} " - " {ev.batch.len()} " msgs"</p>
        }.into_any(),
        None => view! { <p>"waiting for events..."</p>}.into_any(),
        }
    }}
}
