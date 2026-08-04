use leptos::prelude::*;
use leptos_meta::*;
use leptos_use::{
    UseElementSizeOptions, UseElementSizeReturn, UseEventSourceReturn, UseIntervalFnOptions,
    UseRafFnCallbackArgs, UseRafFnOptions, use_element_size, use_element_size_with_options,
    use_event_source, use_interval_fn_with_options, use_raf_fn_with_options,
};
use std::collections::{HashMap, VecDeque};
use streamer_core::{StreamerDto, StreamerId, UiEvent, config::Config};

use crate::api_client::{ApiClient, ApiError};
use crate::inspector;
use crate::graph::{
    CardGeom, Camera, Edge, FALLBACK_CARD_HEIGHT, PULSE_TICK_MS, PULSE_TICKS, approach, bounds,
    edge_paths, focus_camera, layout, nodes_from, pulsed_edge, tick_pulses, wheel_delta_pixels,
    zoom_at,
};

/// How hard the wheel zooms. Small, because the factor is exponential in the
/// scroll distance.
const ZOOM_SENSITIVITY: f64 = 0.0015;

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

/// Everything the canvas needs to draw itself. All of it is derived state
/// except `camera`, `measured` and `focus_request`, which are the three things
/// the user can actually change.
#[derive(Clone, Copy)]
pub struct CanvasState {
    /// Where each card sits — computed by `graph::layout`, never edited by hand.
    pub placements: RwSignal<HashMap<StreamerId, CardGeom>>,
    /// Card heights as actually rendered, fed back into the layout.
    pub measured: RwSignal<HashMap<StreamerId, f64>>,
    pub camera: RwSignal<Camera>,
    /// Size of the canvas viewport in css pixels; needed to centre a node.
    pub viewport: RwSignal<(f64, f64)>,
    /// Set by the sidebar; consumed by the animation loop.
    pub focus_request: RwSignal<Option<StreamerId>>,
    /// Where the camera is gliding to, if anywhere.
    pub focus_target: RwSignal<Option<Camera>>,
}

impl CanvasState {
    fn new() -> Self {
        Self {
            placements: RwSignal::new(HashMap::new()),
            measured: RwSignal::new(HashMap::new()),
            camera: RwSignal::new(Camera::default()),
            viewport: RwSignal::new((0.0, 0.0)),
            focus_request: RwSignal::new(None),
            focus_target: RwSignal::new(None),
        }
    }

    fn geom_of(&self, id: &StreamerId) -> Option<CardGeom> {
        self.placements.with(|p| p.get(id).copied())
    }

    /// Any direct camera control abandons an in-flight glide — otherwise the
    /// animation would fight the user's scroll.
    fn interrupt_focus(&self) {
        if self.focus_target.get_untracked().is_some() {
            self.focus_target.set(None);
        }
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
    let UseEventSourceReturn { data, .. } =
        use_event_source::<UiEvent, codee::string::JsonSerdeCodec>("/events");

    let canvas_state = CanvasState::new();
    provide_context(AppState {
        streams,
        events: data,
        canvas_state,
    });

    let canvas_ref = NodeRef::<leptos::html::Div>::new();
    let UseElementSizeReturn { width, height } = use_element_size(canvas_ref);
    Effect::new(move |_| canvas_state.viewport.set((width.get(), height.get())));

    // the graph itself: re-laid out whenever the pipeline list changes or a
    // card reports a new height
    Effect::new(move |_| {
        let Some(res) = streams.get() else {
            return;
        };
        let Ok(list) = res.as_ref() else {
            return;
        };
        let pairs: Vec<(StreamerId, Config)> = list
            .iter()
            .map(|s| (s.id.clone(), s.config.clone()))
            .collect();
        let placed = layout(&nodes_from(&pairs), &canvas_state.measured.get());
        canvas_state.placements.set(placed);
    });

    // The camera glide. It only runs while there is somewhere to go: the loop
    // pauses itself on arrival and the focus request below wakes it up again.
    //
    // The frame clock is ours rather than the one `use_raf_fn` hands us: that
    // one keeps counting across a pause, so the first frame of every glide
    // after the first would carry the whole idle time as its delta and land the
    // camera on the target immediately.
    let last_frame = RwSignal::new(Option::<f64>::None);
    let raf = use_raf_fn_with_options(
        move |UseRafFnCallbackArgs { timestamp, .. }| {
            let Some(target) = canvas_state.focus_target.get_untracked() else {
                last_frame.set(None);
                return;
            };
            let delta = last_frame
                .get_untracked()
                .map_or(0.0, |prev| timestamp - prev);
            last_frame.set(Some(timestamp));

            let (next, arrived) = approach(canvas_state.camera.get_untracked(), target, delta);
            canvas_state.camera.set(next);
            if arrived {
                canvas_state.focus_target.set(None);
                last_frame.set(None);
            }
        },
        UseRafFnOptions::default().immediate(false),
    );

    let resume = raf.resume.clone();
    let pause = raf.pause.clone();
    Effect::new(move |_| {
        if canvas_state.focus_target.get().is_some() {
            resume();
        } else {
            pause();
        }
    });

    // a click in the sidebar names a node; turn that into a camera target once
    // we know where the node ended up
    Effect::new(move |_| {
        let Some(id) = canvas_state.focus_request.get() else {
            return;
        };
        canvas_state.focus_request.set(None);
        let Some(geom) = canvas_state.geom_of(&id) else {
            return;
        };
        let target = focus_camera(
            canvas_state.camera.get_untracked(),
            geom,
            canvas_state.viewport.get_untracked(),
        );
        canvas_state.focus_target.set(Some(target));
    });

    // Frame the graph once, when the first layout lands. Without this the
    // camera sits at world 0,0 and a centred root is half off-screen.
    let framed = RwSignal::new(false);
    Effect::new(move |_| {
        let (vw, vh) = canvas_state.viewport.get();
        let placements = canvas_state.placements.get();
        if framed.get_untracked() || placements.is_empty() || vw <= 0.0 {
            return;
        }
        // the top row is what you want to see first — it's where the sources are
        let top = placements
            .values()
            .min_by(|a, b| a.y.total_cmp(&b.y).then_with(|| a.x.total_cmp(&b.x)));
        if let Some(top) = top {
            let camera = canvas_state.camera.get_untracked();
            let centred = focus_camera(camera, *top, (vw, vh));
            // centred horizontally, but with the top row just below the top
            // edge rather than in the middle of the screen
            canvas_state.camera.set(Camera {
                y: top.y - 48.0 / camera.zoom,
                ..centred
            });
            framed.set(true);
        }
    });

    // drag-to-pan state: the pointer position of the last mousemove, in css px
    let dragging = RwSignal::new(Option::<(f64, f64)>::None);

    view! {
        <Suspense fallback=move || view! { <p>"Loading streams..."</p> }>
            <Navbar />
            <div class="main-content">
                <Sidebar />
                <div
                    class="nodes"
                    class:panning=move || dragging.get().is_some()
                    node_ref=canvas_ref
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
                        canvas_state.interrupt_focus();
                        let (ox, oy) = canvas_offset(&canvas_ref, ev.client_x(), ev.client_y());
                        let delta = wheel_delta_pixels(ev.delta_y(), ev.delta_mode());
                        let factor = (-delta * ZOOM_SENSITIVITY).exp();
                        canvas_state.camera.update(|c| *c = zoom_at(*c, (ox, oy), factor));
                    }
                    on:mousedown=move |ev| {
                        if ev.button() != 0 || started_on_a_card(&ev) {
                            return;
                        }
                        canvas_state.interrupt_focus();
                        dragging.set(Some((f64::from(ev.client_x()), f64::from(ev.client_y()))));
                    }
                    on:mousemove=move |ev| {
                        let Some((last_x, last_y)) = dragging.get_untracked() else {
                            return;
                        };
                        let (x, y) = (f64::from(ev.client_x()), f64::from(ev.client_y()));
                        canvas_state
                            .camera
                            .update(|c| {
                                c.x -= (x - last_x) / c.zoom;
                                c.y -= (y - last_y) / c.zoom;
                            });
                        dragging.set(Some((x, y)));
                    }
                    on:mouseup=move |_| dragging.set(None)
                    on:mouseleave=move |_| dragging.set(None)
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
                                            <Edges streamers=list.clone() />
                                            <For each=move || list.clone() key=|s| s.id.clone() let:s>
                                                <Card
                                                    streamer_id=s.id.clone()
                                                    events=data
                                                    config=s.config.clone()
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

/// Whether a press landed inside a card. Panning from a card would make its
/// config text impossible to select, so those presses are left alone.
fn started_on_a_card(ev: &leptos::ev::MouseEvent) -> bool {
    use wasm_bindgen::JsCast;
    ev.target()
        .and_then(|t| t.dyn_into::<leptos::web_sys::Element>().ok())
        .and_then(|el| el.closest(".card").ok().flatten())
        .is_some()
}

/// Pointer position relative to the canvas' top-left corner. Falls back to the
/// raw client position if the element isn't mounted, which only happens before
/// the first paint.
fn canvas_offset(canvas: &NodeRef<leptos::html::Div>, client_x: i32, client_y: i32) -> (f64, f64) {
    canvas.get_untracked().map_or_else(
        || (f64::from(client_x), f64::from(client_y)),
        |el| {
            let rect = el.get_bounding_client_rect();
            (
                f64::from(client_x) - rect.left(),
                f64::from(client_y) - rect.top(),
            )
        },
    )
}

/// The lines between cards. One SVG for the whole graph, inside the scaled
/// surface so the edges zoom and pan with the cards; it has no size of its own
/// and simply overflows, which keeps it out of the way of pointer events.
///
/// An edge lights up for a moment when a batch crosses it. The pulse is held
/// here as a tick count per edge; the fade itself is a CSS transition, so this
/// only has to say *when* an edge is lit, not how bright it is.
#[component]
pub fn Edges(streamers: Vec<StreamerDto>) -> impl IntoView {
    let state = expect_context::<AppState>();

    // the graph shape only changes when the pipeline list does, which is the
    // one thing this component is rebuilt for
    let nodes = nodes_from(
        &streamers
            .iter()
            .map(|s| (s.id.clone(), s.config.clone()))
            .collect::<Vec<(StreamerId, Config)>>(),
    );

    let paths = {
        let nodes = nodes.clone();
        Memo::new(move |_| edge_paths(&nodes, &state.canvas_state.placements.get()))
    };
    // a zero-sized svg is never painted, so it has to span the whole graph
    let size = Memo::new(move |_| bounds(&state.canvas_state.placements.get()));

    let pulses = RwSignal::new(HashMap::<Edge, u8>::new());

    // a downstream receiving a batch is a batch having crossed the edge above it
    Effect::new(move |_| {
        let Some(event) = state.events.get() else {
            return;
        };
        let Some(edge) = pulsed_edge(&event.streamer_id, &event.stage, &nodes) else {
            return;
        };
        pulses.update(|p| {
            // a re-fired pulse restarts rather than stacking
            p.insert(edge, PULSE_TICKS);
        });
    });

    // Decay, ticking only while something is actually lit — on an idle graph
    // this costs nothing.
    let decay = use_interval_fn_with_options(
        // the "anything still lit" answer is read off the signal below instead
        move || pulses.update(|p| _ = tick_pulses(p)),
        PULSE_TICK_MS,
        UseIntervalFnOptions::default().immediate(false),
    );
    let (resume, pause) = (decay.resume.clone(), decay.pause.clone());
    Effect::new(move |_| {
        if pulses.with(HashMap::is_empty) {
            pause();
        } else {
            resume();
        }
    });

    view! {
        <svg
            class="edges"
            width=move || size.get().0
            height=move || size.get().1
        >
            // Rebuilt whole whenever the layout moves. Not a keyed `For`: the
            // natural key is the pair of ids it connects, which is exactly what
            // *doesn't* change when a card grows, so the old `d` would stick.
            {move || {
                paths
                    .get()
                    .into_iter()
                    .map(|(edge, d)| {
                        view! {
                            <path
                                d=d
                                class:active=move || pulses.with(|p| p.contains_key(&edge))
                            />
                        }
                    })
                    .collect_view()
            }}
        </svg>
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
                                            state.canvas_state.focus_request.set(Some(s.id.clone()));
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
    let state = expect_context::<AppState>();
    let zoom = move || format!("{:.0}%", state.canvas_state.camera.get().zoom * 100.0);
    view! {
        <aside class="navbar">
            <div>"navb"</div>
            <div class="zoom-level" title="scroll to zoom, drag to pan">
                {zoom}
            </div>
        </aside>
    }
}

/// The three stages of a pipeline, which are also the inspector's tabs.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Tab {
    Inputs,
    Transforms,
    Outputs,
}

/// A card's config, as a tabbed property list rather than raw JSON.
///
/// The config of a running streamer never changes, so all three tabs are built
/// once and only the selected one is rendered.
#[component]
fn Inspector(config: Config) -> impl IntoView {
    let input = inspector::input_section(&config);
    let output = inspector::output_section(&config);
    let transforms = inspector::transform_sections(&config);

    // the count belongs on the tab: a chain is the one part of a pipeline that
    // can be empty or long, and that's worth seeing without clicking
    let tabs = [
        (Tab::Inputs, "inputs".to_string()),
        (
            Tab::Transforms,
            format!("transforms ({})", transforms.len()),
        ),
        (Tab::Outputs, "outputs".to_string()),
    ];

    let tab = RwSignal::new(Tab::Inputs);

    view! {
        <div class="inspector">
            <div class="tabs">
                {tabs
                    .into_iter()
                    .map(|(which, label)| {
                        view! {
                            <button
                                class="tab"
                                class:active=move || tab.get() == which
                                on:click=move |_| tab.set(which)
                            >
                                {label}
                            </button>
                        }
                    })
                    .collect_view()}
            </div>
            <div class="pane">
                {move || match tab.get() {
                    Tab::Inputs => view! { <SectionView section=input.clone() /> }.into_any(),
                    Tab::Outputs => view! { <SectionView section=output.clone() /> }.into_any(),
                    Tab::Transforms if transforms.is_empty() => {
                        view! { <div class="empty">"no transforms"</div> }.into_any()
                    }
                    Tab::Transforms => {
                        transforms
                            .iter()
                            .cloned()
                            .enumerate()
                            .map(|(i, section)| view! { <SectionView section ordinal=i + 1 /> })
                            .collect_view()
                            .into_any()
                    }
                }}
            </div>
        </div>
    }
}

/// One component: a kind heading and its settings. `ordinal` numbers a
/// transform by its place in the chain — order is behaviour there.
#[component]
fn SectionView(
    section: inspector::Section,
    #[prop(optional, into)] ordinal: Option<usize>,
) -> impl IntoView {
    let heading = ordinal.map_or_else(
        || section.kind.clone(),
        |n| format!("{n}. {kind}", kind = section.kind),
    );
    let properties = section.properties;

    view! {
        <div class="section">
            <div class="section-kind">{heading}</div>
            {if properties.is_empty() {
                view! { <div class="empty">"no settings"</div> }.into_any()
            } else {
                properties
                    .into_iter()
                    .map(|p| {
                        let (name, value) = (p.name.clone(), p.value.clone());
                        view! {
                            <div class="property">
                                <span class="name" title=p.name>{name}</span>
                                // the full value on hover: a url or a subject
                                // will not fit in half a card
                                <span class="value" title=p.value>{value}</span>
                            </div>
                        }
                    })
                    .collect_view()
                    .into_any()
            }}
        </div>
    }
}

#[component]
pub fn Card(
    streamer_id: StreamerId,
    config: Config,
    events: Signal<Option<UiEvent>>,
) -> impl IntoView {
    let state = expect_context::<AppState>();
    let canvas = state.canvas_state;
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

    // Report our rendered height back to the layout so the row below clears us.
    // Only on a real change: the layout writes positions, and a write here on
    // every measurement would bounce between the two.
    //
    // Border box, not the default content box: the layout and the edges want
    // the height the card actually occupies on screen, borders included, or
    // every edge lands a couple of pixels inside the card it points at.
    let card_ref = NodeRef::<leptos::html::Div>::new();
    let UseElementSizeReturn {
        height: measured_height,
        ..
    } = use_element_size_with_options(
        card_ref,
        UseElementSizeOptions::default().box_(leptos::web_sys::ResizeObserverBoxOptions::BorderBox),
    );
    let measure_id = streamer_id.clone();
    Effect::new(move |_| {
        let height = measured_height.get();
        if height <= 0.0 {
            return;
        }
        let changed = canvas
            .measured
            .with_untracked(|m| m.get(&measure_id).is_none_or(|old| (old - height).abs() > 1.0));
        if changed {
            canvas.measured.update(|m| {
                m.insert(measure_id.clone(), height);
            });
        }
    });

    let position_id = streamer_id.clone();
    let position = Memo::new(move |_| {
        canvas
            .placements
            .with(|p| p.get(&position_id).copied())
            .unwrap_or(CardGeom {
                x: 0.0,
                y: 0.0,
                width: crate::graph::CARD_WIDTH,
                height: FALLBACK_CARD_HEIGHT,
            })
    });

    view! {
        <div
            class="card"
            node_ref=card_ref
            style:left=move || format!("{}px", position.get().x)
            style:top=move || format!("{}px", position.get().y)
        >
            <header>{streamer_id.clone()}</header>
            <Inspector config=config />
            <div class="messages" node_ref=log_ref>
                <For each=move || messages.get() key=|(i, _)| *i let:entry>
                    <div>{entry.1}</div>
                </For>
            </div>
        </div>
    }
}
