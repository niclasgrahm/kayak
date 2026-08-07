use kayak_core::docs::{Family, FieldType, all_components};
use kayak_core::{
    ConfigFormat, Connections, EdgeEnd, LayoutFile, PipelineDto, PipelineId, PortLayout, Side,
    UiEvent, config::Config,
};
use leptos::prelude::*;
use leptos_meta::*;
use leptos_router::components::{A, Route, Router, Routes};
use leptos_router::path;
use leptos_use::{
    UseElementSizeOptions, UseElementSizeReturn, UseEventListenerOptions, UseEventSourceReturn,
    UseIntervalFnOptions, UseRafFnCallbackArgs, UseRafFnOptions, use_element_size,
    use_element_size_with_options, use_event_listener, use_event_listener_with_options,
    use_event_source, use_interval_fn_with_options, use_raf_fn_with_options, use_window,
};
use std::collections::{HashMap, HashSet};

use crate::api_client::{ApiClient, ApiError};
use crate::docs;
use crate::form;
use crate::graph::{
    Camera, CardGeom, Channel, Edge, FALLBACK_CARD_HEIGHT, GRID, PULSE_TICK_MS, PULSE_TICKS,
    PortHandle, approach, bounds, dragged, dragged_channel, dragged_port, edge_paths, focus_camera,
    layout, pipelines_from, pulsed_edges, resized, tick_pulses, wheel_delta_pixels, zoom_at,
};
use crate::inspector;
use crate::log;
use crate::sidebar;
use crate::sidebar::{Row, SidebarMode};

/// How hard the wheel zooms. Small, because the factor is exponential in the
/// scroll distance.
const ZOOM_SENSITIVITY: f64 = 0.0015;

/// How far from the bottom of a log still counts as being at the bottom, in
/// pixels. A line-height's worth of slack: a browser's fractional scroll
/// positions mean an exact comparison drops out of follow mode on its own.
const FOLLOW_SLACK_PX: i32 = 20;

/// How wide the handle on a port is, in surface pixels. A grid cell: wide
/// enough to grab, narrow enough that two adjacent ports on a fanned-out face
/// are still two things.
const PORT_GRIP: f64 = GRID;

pub fn shell(options: LeptosOptions) -> impl IntoView {
    view! {
        <!DOCTYPE html>
        <html lang="en">
            <head>
                <meta charset="utf-8" />
                <meta name="viewport" content="width=device-width,initial-scale=1" />
                <AutoReload options=options.clone() />
                <HydrationScripts options />
                <Stylesheet id="leptos" href="/pkg/kayak.css" />
                <MetaTags />
            </head>
            <body>
                <App />
            </body>
        </html>
    }
}

/// What the canvas lets you do.
///
/// Read-only is the default and the reason the mode exists at all: the canvas
/// is a live window onto a running system, and a window should not have a
/// delete button one click from the pipeline list. Editing is a thing you opt
/// into.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    ReadOnly,
    Edit,
}

impl Mode {
    fn is_edit(self) -> bool {
        self == Self::Edit
    }
}

/// Which list the sidebar is showing.
///
/// Two tabs rather than two pages: they are the two halves of one config — the
/// pipelines and the systems they talk to — and picking one shouldn't take the
/// canvas away.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SidebarTab {
    Pipelines,
    Connections,
}

#[derive(Clone, Copy)]
pub struct AppState {
    pub pipelines: LocalResource<Result<Vec<PipelineDto>, ApiError>>,
    /// The named connections pipelines refer to. Re-fetched on the same trigger
    /// as the pipelines: adding one changes what the next form can offer.
    pub connections: LocalResource<Result<Connections, ApiError>>,
    pub events: Signal<Option<UiEvent>>,
    pub canvas_state: CanvasState,
    /// Bumped after a pipeline is created or deleted. `pipelines` and `settings`
    /// both read it, so bumping it re-fetches them — there is no `refetch()` on
    /// a `LocalResource`, and re-reading the server beats patching a local copy
    /// that could disagree with it.
    pub reload: RwSignal<u32>,
    /// Name of the config file the server is working against, if it has one
    /// yet. `None` means a save would *create* one, so the edit controls offer
    /// "create config file" rather than "save as".
    pub config_file: Signal<Option<String>>,
    /// Where a save lands on the server. Shown when there is no config file
    /// yet, because "config.json" on its own doesn't say where it will appear.
    pub save_directory: Signal<String>,
    /// The running graph has diverged from that file. Edits are live and the
    /// file is left alone, so this is the only thing standing between an
    /// afternoon's work and a restart.
    pub unsaved: Signal<bool>,
    pub mode: RwSignal<Mode>,
    /// Whether the "add pipeline" modal is open.
    pub adding: RwSignal<bool>,
    /// Whether the "add connection" modal is open.
    pub adding_connection: RwSignal<bool>,
    /// Whether the "save as" modal is open.
    pub saving: RwSignal<bool>,
    /// Which of the sidebar's two lists is showing.
    pub tab: RwSignal<SidebarTab>,
    /// How the pipeline list is arranged. Lives here rather than in
    /// `PipelineList` because the tab strip unmounts that component, and a way
    /// of looking at the graph shouldn't be forgotten by a glance at the
    /// connections.
    pub sidebar_mode: RwSignal<SidebarMode>,
    /// Wall clock, epoch millis, ticking once a second. One timer for the whole
    /// page rather than one per card: the only thing that needs it is the
    /// throughput readout on each log, and that has to fall back to zero when
    /// traffic stops — which no event will ever arrive to say.
    pub now: RwSignal<f64>,
    /// The viewer's timezone, as minutes to add to local time to get UTC —
    /// `Date`'s `getTimezoneOffset` convention, which is what fills it.
    ///
    /// Read once into a signal rather than at each call site because it can
    /// only be read on the client: server-side rendering has no `Date`, and the
    /// zone it would report is the server's anyway. Until the effect runs it is
    /// zero, so the first render of a log line is UTC.
    pub tz_offset: RwSignal<i32>,
}

impl AppState {
    /// Re-read the pipeline list, the connections and the save state from the
    /// server.
    pub fn refresh(&self) {
        self.reload.update(|n| *n = n.wrapping_add(1));
    }

    /// The connections a form can offer, as `(name, kind)` in name order.
    ///
    /// Derived here rather than in the modal because two of them want it — the
    /// list in the sidebar and the dropdown in the pipeline form — and because
    /// the answer is server state either way.
    pub fn connection_list(&self) -> Vec<(String, String)> {
        let connections = self.connections;
        let Some(res) = connections.get() else {
            return Vec::new();
        };
        let Ok(connections) = res.as_ref() else {
            return Vec::new();
        };
        connections
            .iter()
            .map(|(id, kind)| (id.clone(), kind.type_name().to_string()))
            .collect()
    }

    fn editing(&self) -> bool {
        self.mode.get().is_edit()
    }
}

/// A drag in progress: which card, what it is doing to it, and where the
/// pointer was when it started.
///
/// One signal for both gestures because they are the same gesture with a
/// different arithmetic at the end — press, move, release — and because only
/// one of them can be happening at a time.
#[derive(Clone, Copy, PartialEq)]
pub enum Grab {
    Move,
    Resize,
}

#[derive(Clone, PartialEq)]
pub struct Dragging {
    pub id: PipelineId,
    pub grab: Grab,
    /// Pointer position when the press landed, in client (screen) pixels.
    pub origin: (f64, f64),
    /// The card's geometry when the press landed. The gesture is applied to
    /// *this* rather than to the current geometry, so a drag can't accumulate
    /// rounding: every mousemove computes the same answer from the same start.
    pub start: CardGeom,
    /// Whether the card had a pinned height before the drag, so a move can
    /// leave it alone.
    pub pinned_height: Option<f64>,
}

/// A channel drag in progress: which edge's middle segment is being moved, and
/// where it started from.
#[derive(Clone, PartialEq)]
pub struct DraggingChannel {
    pub edge: Edge,
    /// The route runs vertically, so the pointer's *y* is what moves the
    /// channel and its x is ignored — a handle that slid sideways as well
    /// would just be a way to lose it.
    pub vertical: bool,
    pub origin: (f64, f64),
    /// The offset before the drag, so every mousemove computes its answer from
    /// the same starting point rather than accumulating one.
    pub start_offset: f64,
}

/// A port drag in progress: which end of which edge is being slid along its
/// card's face.
#[derive(Clone, PartialEq)]
pub struct DraggingPort {
    pub edge: Edge,
    pub end: EdgeEnd,
    pub side: Side,
    /// How long the face is, so the drag can stop at its ends.
    pub length: f64,
    pub origin: (f64, f64),
    pub start_along: f64,
}

/// Everything the canvas needs to draw itself. All of it is derived state
/// except `camera`, `measured` and `focus_request`, which are the three things
/// the user can actually change.
#[derive(Clone, Copy)]
pub struct CanvasState {
    /// Where each card sits — computed by `graph::layout` from the automatic
    /// layout and `arrangement`, never written to directly.
    pub placements: RwSignal<HashMap<PipelineId, CardGeom>>,
    /// The cards someone has dragged or resized, as loaded from — and saved
    /// back to — the server's layout file. Absent ids are laid out
    /// automatically, which is the normal state of most graphs.
    pub arrangement: RwSignal<LayoutFile>,
    /// The card drag currently in flight, if any.
    pub dragging: RwSignal<Option<Dragging>>,
    /// The edge-channel drag currently in flight, if any. Separate from
    /// `dragging` rather than another `Grab`: the two move different things and
    /// neither can start while the other is running, so nothing is shared but
    /// the mouse.
    pub dragging_channel: RwSignal<Option<DraggingChannel>>,
    /// The port drag currently in flight, if any.
    pub dragging_port: RwSignal<Option<DraggingPort>>,
    /// Card heights as actually rendered, fed back into the layout.
    pub measured: RwSignal<HashMap<PipelineId, f64>>,
    /// The card blown up to fill the canvas, if any. At most one at a time, and
    /// deliberately *not* part of `arrangement`: it is a way of looking at a
    /// card rather than a change to where the card lives, so it neither goes to
    /// the layout file nor survives a reload.
    pub maximized: RwSignal<Option<PipelineId>>,
    pub camera: RwSignal<Camera>,
    /// Size of the canvas viewport in css pixels; needed to centre a pipeline.
    pub viewport: RwSignal<(f64, f64)>,
    /// Set by the sidebar; consumed by the animation loop.
    pub focus_request: RwSignal<Option<PipelineId>>,
    /// Where the camera is gliding to, if anywhere.
    pub focus_target: RwSignal<Option<Camera>>,
}

impl CanvasState {
    fn new() -> Self {
        Self {
            placements: RwSignal::new(HashMap::new()),
            arrangement: RwSignal::new(LayoutFile::default()),
            dragging: RwSignal::new(None),
            dragging_channel: RwSignal::new(None),
            dragging_port: RwSignal::new(None),
            measured: RwSignal::new(HashMap::new()),
            maximized: RwSignal::new(None),
            camera: RwSignal::new(Camera::default()),
            viewport: RwSignal::new((0.0, 0.0)),
            focus_request: RwSignal::new(None),
            focus_target: RwSignal::new(None),
        }
    }

    fn geom_of(&self, id: &PipelineId) -> Option<CardGeom> {
        self.placements.with(|p| p.get(id).copied())
    }

    /// Any direct camera control abandons an in-flight glide — otherwise the
    /// animation would fight the user's scroll.
    fn interrupt_focus(&self) {
        if self.focus_target.get_untracked().is_some() {
            self.focus_target.set(None);
        }
    }

    /// Move the drag on by the pointer's current position.
    ///
    /// Applied to the arrangement as it happens rather than only on release,
    /// which is what makes the edges follow the card around instead of jumping
    /// to it at the end. The pointer delta is in screen pixels and the layout
    /// is in surface pixels, so it is divided by the zoom — at 50% a card has
    /// to keep up with a pointer moving twice as far.
    fn drag_to(&self, drag: &Dragging, client: (f64, f64)) {
        let zoom = self.camera.get_untracked().zoom;
        let dx = (client.0 - drag.origin.0) / zoom;
        let dy = (client.1 - drag.origin.1) / zoom;
        let pipeline = match drag.grab {
            Grab::Move => dragged(drag.start, dx, dy, drag.pinned_height),
            Grab::Resize => resized(drag.start, dx, dy),
        };
        self.arrangement.update(|a| {
            a.pipelines.insert(drag.id.clone(), pipeline);
        });
    }

    /// Move an edge's channel on by the pointer's current position.
    ///
    /// Only the axis the channel actually slides along is read; the other is
    /// the user moving the mouse, not the user moving the line.
    fn drag_channel_to(&self, drag: &DraggingChannel, client: (f64, f64)) {
        let zoom = self.camera.get_untracked().zoom;
        let delta = if drag.vertical {
            (client.1 - drag.origin.1) / zoom
        } else {
            (client.0 - drag.origin.0) / zoom
        };
        self.set_channel(
            &drag.edge,
            Some(dragged_channel(drag.start_offset, delta)),
        );
    }

    fn set_channel(&self, edge: &Edge, offset: Option<f64>) {
        self.arrangement
            .update(|a| a.set_edge_offset(&edge.from, &edge.to, offset));
    }

    /// Slide a port along the face it is attached to.
    ///
    /// Only the axis of the face is read — a port on a card's bottom edge moves
    /// left and right and nowhere else, which is the whole of what "where on
    /// this side does the line attach" can mean.
    fn drag_port_to(&self, drag: &DraggingPort, client: (f64, f64)) {
        let zoom = self.camera.get_untracked().zoom;
        let delta = if drag.side.is_vertical() {
            (client.0 - drag.origin.0) / zoom
        } else {
            (client.1 - drag.origin.1) / zoom
        };
        let along = dragged_port(drag.start_along, delta, drag.length);
        self.set_port(
            &drag.edge,
            drag.end,
            Some(PortLayout {
                side: drag.side,
                along,
            }),
        );
    }

    fn set_port(&self, edge: &Edge, end: EdgeEnd, port: Option<PortLayout>) {
        self.arrangement
            .update(|a| a.set_edge_port(&edge.from, &edge.to, end, port));
    }

    /// Blow a card up to fill the canvas, or put it back if it already is.
    ///
    /// Maximizing a second card restores the first, since only one can be the
    /// one you are looking at.
    fn toggle_maximized(&self, id: &PipelineId) {
        self.maximized.update(|current| {
            *current = if current.as_ref() == Some(id) {
                None
            } else {
                Some(id.clone())
            };
        });
    }

    /// Put a card back under the automatic layout.
    fn unpin(&self, id: &PipelineId) {
        let removed = self
            .arrangement
            .try_update(|a| a.pipelines.remove(id).is_some());
        if removed == Some(true) {
            self.save_arrangement();
        }
    }

    /// Put the whole canvas back under the automatic layout: every card someone
    /// has dragged or resized, and every edge whose route they have adjusted.
    ///
    /// The way out of an arrangement that has been made a mess of, and the same
    /// state as deleting the layout file and starting the server again. It is
    /// saved like any other arrangement change, so the file on disk goes back to
    /// empty too rather than reappearing on the next reload.
    fn reset_arrangement(&self) {
        let cleared = self.arrangement.try_update(|a| {
            let had_any = !a.is_empty();
            a.clear();
            had_any
        });
        if cleared == Some(true) {
            self.save_arrangement();
        }
    }

    fn save_arrangement(&self) {
        persist_arrangement(self.arrangement.get_untracked());
    }
}

/// Send the arrangement to the server, which writes it to the layout file.
///
/// Fire and forget, and deliberately quiet about failing: this is the position
/// of a box on a canvas, and interrupting someone mid-drag with a toast about
/// it would cost more than the arrangement is worth. It is still logged, so a
/// server refusing every save is findable rather than merely puzzling.
fn persist_arrangement(arrangement: LayoutFile) {
    leptos::task::spawn_local(async move {
        let client = ApiClient {
            base: String::new(),
        };
        if let Err(err) = client.save_layout(&arrangement).await {
            leptos::logging::warn!("could not save the canvas layout: {err}");
        }
    });
}

/// The two pages, behind a router so `/docs` is a real url that can be linked
/// to and reloaded rather than a mode the canvas is in.
#[component]
pub fn App() -> impl IntoView {
    provide_meta_context();
    view! {
        <Router>
            <Routes fallback=|| view! { <p class="empty">"no such page"</p> }>
                <Route path=path!("") view=CanvasPage />
                <Route path=path!("/docs") view=DocsPage />
            </Routes>
        </Router>
    }
}

/// The pipeline graph: every pipeline the server is running, as a pannable,
/// zoomable canvas of cards.
#[component]
pub fn CanvasPage() -> impl IntoView {
    // read *before* the async block so the resource depends on it: bumping it
    // is how an edit gets the list re-fetched
    let reload = RwSignal::new(0u32);
    let pipelines = LocalResource::new(move || {
        reload.track();
        async move {
            ApiClient {
                base: String::new(),
            }
            .list_pipelines()
            .await
        }
    });
    let connections = LocalResource::new(move || {
        reload.track();
        async move {
            ApiClient {
                base: String::new(),
            }
            .list_connections()
            .await
        }
    });
    // re-fetched on the same trigger as the list: an edit is exactly what can
    // change whether there are unsaved changes
    let settings = LocalResource::new(move || {
        reload.track();
        async move {
            ApiClient {
                base: String::new(),
            }
            .settings()
            .await
        }
    });
    let config_file = Signal::derive(move || {
        settings
            .get()
            .and_then(|res| res.as_ref().ok().and_then(|s| s.config_file.clone()))
    });
    let save_directory = Signal::derive(move || {
        settings
            .get()
            .and_then(|res| res.as_ref().ok().map(|s| s.save_directory.clone()))
            .unwrap_or_default()
    });
    // false until the answer arrives — a "unsaved changes" warning that flashes
    // up on every load would train people to ignore it
    let unsaved = Signal::derive(move || {
        settings
            .get()
            .and_then(|res| res.as_ref().ok().map(|s| s.unsaved_changes))
            .unwrap_or(false)
    });
    let UseEventSourceReturn { data, .. } =
        use_event_source::<UiEvent, codee::string::JsonSerdeCodec>("/events");

    let canvas_state = CanvasState::new();
    let state = AppState {
        pipelines,
        connections,
        events: data,
        canvas_state,
        reload,
        config_file,
        save_directory,
        unsaved,
        mode: RwSignal::new(Mode::ReadOnly),
        adding: RwSignal::new(false),
        adding_connection: RwSignal::new(false),
        saving: RwSignal::new(false),
        tab: RwSignal::new(SidebarTab::Pipelines),
        sidebar_mode: RwSignal::new(SidebarMode::default()),
        now: RwSignal::new(0.0),
        tz_offset: RwSignal::new(0),
    };
    provide_context(state);

    // Client only: an effect doesn't run during server-side rendering, which is
    // exactly the boundary — `js_sys::Date` is a browser API, and the server's
    // own zone would be the wrong answer even where it could be read.
    Effect::new(move |_| {
        #[allow(clippy::cast_possible_truncation)]
        state
            .tz_offset
            .set(js_sys::Date::new_0().get_timezone_offset() as i32);
        state.now.set(js_sys::Date::now());
    });
    use_interval_fn_with_options(
        move || state.now.set(js_sys::Date::now()),
        1_000,
        UseIntervalFnOptions::default(),
    );

    // Leaving edit mode with work that only exists in the server's memory is
    // the one way to lose it by accident, so the browser asks. It can only be a
    // generic prompt — browsers ignore the message — but the interruption is
    // the point.
    let _ = use_event_listener(use_window(), leptos::ev::beforeunload, move |ev| {
        if state.unsaved.get_untracked() {
            ev.prevent_default();
            ev.set_return_value("");
        }
    });

    let canvas_ref = NodeRef::<leptos::html::Div>::new();
    let UseElementSizeReturn { width, height } = use_element_size(canvas_ref);
    Effect::new(move |_| canvas_state.viewport.set((width.get(), height.get())));

    // the graph itself: re-laid out whenever the pipeline list changes or a
    // card reports a new height
    Effect::new(move |_| {
        let Some(res) = pipelines.get() else {
            return;
        };
        let Ok(list) = res.as_ref() else {
            return;
        };
        let pairs: Vec<(PipelineId, Config)> = list
            .iter()
            .map(|s| (s.id.clone(), s.config.clone()))
            .collect();
        let placed = layout(
            &pipelines_from(&pairs),
            &canvas_state.measured.get(),
            &canvas_state.arrangement.get(),
        );
        canvas_state.placements.set(placed);
    });

    // The arrangement is fetched once rather than on `reload`: the server only
    // ever has what this tab last sent it, and re-reading it mid-drag would
    // fight the drag.
    let arrangement = LocalResource::new(|| async move {
        ApiClient {
            base: String::new(),
        }
        .layout()
        .await
    });
    Effect::new(move |_| {
        if let Some(res) = arrangement.get()
            && let Ok(loaded) = res.as_ref()
        {
            canvas_state.arrangement.set(loaded.clone());
        }
    });

    // A drag is tracked on the window, not on the card: a fast pointer leaves
    // the card behind, and a mouseup outside it would otherwise never arrive
    // and leave the card stuck to the cursor.
    let _ = use_event_listener(use_window(), leptos::ev::mousemove, move |ev| {
        let at = (f64::from(ev.client_x()), f64::from(ev.client_y()));
        if let Some(drag) = canvas_state.dragging.get_untracked() {
            ev.prevent_default();
            canvas_state.drag_to(&drag, at);
        } else if let Some(drag) = canvas_state.dragging_channel.get_untracked() {
            ev.prevent_default();
            canvas_state.drag_channel_to(&drag, at);
        } else if let Some(drag) = canvas_state.dragging_port.get_untracked() {
            ev.prevent_default();
            canvas_state.drag_port_to(&drag, at);
        }
    });
    let _ = use_event_listener(use_window(), leptos::ev::mouseup, move |_| {
        let was_dragging = canvas_state.dragging.get_untracked().is_some()
            || canvas_state.dragging_channel.get_untracked().is_some()
            || canvas_state.dragging_port.get_untracked().is_some();
        if was_dragging {
            canvas_state.dragging.set(None);
            canvas_state.dragging_channel.set(None);
            canvas_state.dragging_port.set(None);
            // Saved on release rather than on every frame of the drag: the
            // layout file is a file on someone's disk, and a drag across the
            // canvas would otherwise rewrite it a hundred times.
            canvas_state.save_arrangement();
        }
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

    // a click in the sidebar names a pipeline; turn that into a camera target once
    // we know where the pipeline ended up
    Effect::new(move |_| {
        let Some(id) = canvas_state.focus_request.get() else {
            return;
        };
        canvas_state.focus_request.set(None);
        // Asking to be shown a pipeline means the canvas, so a card filling it
        // gets out of the way — the glide would otherwise happen behind it and
        // read as the sidebar not working. Done here rather than in the sidebar
        // because this is where a focus is acted on.
        canvas_state.maximized.set(None);
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
        <Suspense fallback=move || view! { <p>"Loading pipelines..."</p> }>
            <Navbar />
            <div class="main-content">
                <Sidebar />
                <div
                    class="pipelines"
                    class:panning=move || dragging.get().is_some()
                    node_ref=canvas_ref
                    style:background-position=move || {
                        let c = canvas_state.camera.get();
                        format!("{}px {}px", -c.x * c.zoom, -c.y * c.zoom)
                    }
                    style:background-size=move || {
                        let c = canvas_state.camera.get();
                        // the grid the cards snap to, so what is drawn is what
                        // they land on rather than a decoration that resembles it
                        format!("{0}px {0}px", GRID * c.zoom)
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
                        pipelines
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
                                            <Edges pipelines=list.clone() />
                                            <For each=move || list.clone() key=|s| s.id.clone() let:s>
                                                <Card
                                                    pipeline_id=s.id.clone()
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
            <Show when=move || state.adding.get()>
                <AddPipelineModal />
            </Show>
            <Show when=move || state.adding_connection.get()>
                <AddConnectionModal />
            </Show>
            <Show when=move || state.saving.get()>
                <SaveAsModal />
            </Show>
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
pub fn Edges(pipelines: Vec<PipelineDto>) -> impl IntoView {
    let state = expect_context::<AppState>();

    // the graph shape only changes when the pipeline list does, which is the
    // one thing this component is rebuilt for
    let pipelines = pipelines_from(
        &pipelines
            .iter()
            .map(|s| (s.id.clone(), s.config.clone()))
            .collect::<Vec<(PipelineId, Config)>>(),
    );

    let canvas = state.canvas_state;
    let paths = {
        let pipelines = pipelines.clone();
        Memo::new(move |_| {
            edge_paths(
                &pipelines,
                &canvas.placements.get(),
                &canvas.arrangement.get(),
            )
        })
    };
    // a zero-sized svg is never painted, so it has to span the whole graph
    let size = Memo::new(move |_| bounds(&state.canvas_state.placements.get()));

    let pulses = RwSignal::new(HashMap::<Edge, u8>::new());

    // a downstream receiving a batch is a batch having crossed the edge above it
    Effect::new(move |_| {
        let Some(event) = state.events.get() else {
            return;
        };
        let lit = pulsed_edges(&event, &pipelines);
        if lit.is_empty() {
            return;
        }
        pulses.update(|p| {
            for edge in lit {
                // a re-fired pulse restarts rather than stacking
                p.insert(edge, PULSE_TICKS);
            }
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
                    .map(|routed| {
                        let edge = routed.edge;
                        view! {
                            <path
                                d=routed.path
                                class:active=move || pulses.with(|p| p.contains_key(&edge))
                            />
                        }
                    })
                    .collect_view()
            }}
            // The grips go in their own layer, above every path: a channel that
            // has been dragged onto another edge would otherwise have its handle
            // painted under that edge and be hard to grab back.
            <Show when=move || state.editing()>
                {move || {
                    paths
                        .get()
                        .into_iter()
                        .map(|routed| {
                            let channel = routed
                                .channel
                                .map(|channel| {
                                    view! {
                                        <ChannelGrip
                                            edge=routed.edge.clone()
                                            channel=channel
                                            offset=routed.channel_offset
                                        />
                                    }
                                });
                            view! {
                                {channel}
                                <PortGrip
                                    edge=routed.edge.clone()
                                    end=EdgeEnd::From
                                    port=routed.from_port
                                />
                                <PortGrip edge=routed.edge end=EdgeEnd::To port=routed.to_port />
                            }
                        })
                        .collect_view()
                }}
            </Show>
        </svg>
    }
}

/// The handle on a route's middle segment.
///
/// Two lines on top of each other: a thick invisible one that catches the
/// pointer, because a 2px stroke is not something anyone can reliably hit, and a
/// visible grip that says the segment is draggable. Dragging it moves the
/// channel across the gap between the two cards, which is how a route the
/// automatic separation put somewhere unhelpful is moved out of the way.
///
/// `offset` is where the line is drawn, which is not always what the layout file
/// says: an untouched channel is placed automatically, and a drag has to carry
/// on from there rather than jump back to the half-way line on the first pixel.
#[component]
fn ChannelGrip(edge: Edge, channel: Channel, offset: f64) -> impl IntoView {
    let state = expect_context::<AppState>();
    let canvas = state.canvas_state;

    // Half the segment either side of its midpoint. The grip is the *whole*
    // segment rather than a dot on it: the line is what the eye is following,
    // so the line is what the hand should be able to take hold of.
    let half = channel.length / 2.0;
    let (x1, y1, x2, y2) = if channel.vertical {
        (
            channel.at.0 - half,
            channel.at.1,
            channel.at.0 + half,
            channel.at.1,
        )
    } else {
        (
            channel.at.0,
            channel.at.1 - half,
            channel.at.0,
            channel.at.1 + half,
        )
    };

    let stored_edge = StoredValue::new(edge);
    let start = move |ev: leptos::ev::MouseEvent| {
        if ev.button() != 0 {
            return;
        }
        ev.prevent_default();
        ev.stop_propagation();
        canvas.interrupt_focus();
        canvas.dragging_channel.set(Some(DraggingChannel {
            edge: stored_edge.get_value(),
            vertical: channel.vertical,
            origin: (f64::from(ev.client_x()), f64::from(ev.client_y())),
            start_offset: offset,
        }));
    };

    let held = Memo::new(move |_| {
        canvas.dragging_channel.with(|d| {
            d.as_ref()
                .is_some_and(|d| d.edge == stored_edge.get_value())
        })
    });

    view! {
        <g class="channel-grip" class:held=move || held.get()>
            <line
                class="hit"
                class:vertical=channel.vertical
                x1=x1
                y1=y1
                x2=x2
                y2=y2
                on:mousedown=start
                // the way back to the automatic route, same gesture as a card's
                on:dblclick=move |_| {
                    let edge = stored_edge.get_value();
                    canvas.set_channel(&edge, None);
                    canvas.save_arrangement();
                }
                // an attribute, not an svg `<title>` child: leptos_meta claims
                // `<title>` for the document's own, and the tab ends up named
                // after whichever edge rendered last
                aria-label="drag to move this line; double-click to reset it"
            />
            <line class="grip" x1=x1 y1=y1 x2=x2 y2=y2 />
        </g>
    }
}

/// The handle on the point where an edge meets a card.
///
/// Which *face* an edge uses is worked out from where the cards sit and stays
/// automatic — that part is nearly always right, and it changes as things move.
/// Where along that face it attaches is what this lets you say, by sliding it:
/// the fan-out spreads ends evenly, which is a good default and a poor answer
/// when two of the lines want to cross to get where they are going.
///
/// A short bar lying along the face rather than a dot, for the same reason the
/// channel grip spans its segment: it has to be findable and hittable without
/// being loud.
#[component]
fn PortGrip(edge: Edge, end: EdgeEnd, port: PortHandle) -> impl IntoView {
    let state = expect_context::<AppState>();
    let canvas = state.canvas_state;

    let half = PORT_GRIP / 2.0;
    let (x1, y1, x2, y2) = if port.side.is_vertical() {
        (port.at.0 - half, port.at.1, port.at.0 + half, port.at.1)
    } else {
        (port.at.0, port.at.1 - half, port.at.0, port.at.1 + half)
    };

    let stored_edge = StoredValue::new(edge);
    let start = move |ev: leptos::ev::MouseEvent| {
        if ev.button() != 0 {
            return;
        }
        ev.prevent_default();
        ev.stop_propagation();
        canvas.interrupt_focus();
        canvas.dragging_port.set(Some(DraggingPort {
            edge: stored_edge.get_value(),
            end,
            side: port.side,
            length: port.length,
            origin: (f64::from(ev.client_x()), f64::from(ev.client_y())),
            start_along: port.along,
        }));
    };

    let held = Memo::new(move |_| {
        canvas.dragging_port.with(|d| {
            d.as_ref()
                .is_some_and(|d| d.edge == stored_edge.get_value() && d.end == end)
        })
    });

    view! {
        <g
            class="port-grip"
            class:held=move || held.get()
            class:pinned=port.pinned
        >
            <line
                class="hit"
                class:vertical=port.side.is_vertical()
                x1=x1
                y1=y1
                x2=x2
                y2=y2
                on:mousedown=start
                // same gesture as everywhere else on the canvas: back to automatic
                on:dblclick=move |_| {
                    let edge = stored_edge.get_value();
                    canvas.set_port(&edge, end, None);
                    canvas.save_arrangement();
                }
                aria-label="drag along the card to move where this line connects; double-click to reset it"
            />
            <line class="grip" x1=x1 y1=y1 x2=x2 y2=y2 />
        </g>
    }
}

/// The two lists behind one tab strip: the pipelines, and the connections they
/// name.
///
/// Tabs rather than two panels stacked, because they are the two halves of one
/// config and only one of them is being worked on at a time — and rather than
/// two pages, because the canvas belongs beside both.
#[component]
pub fn Sidebar() -> impl IntoView {
    let state = expect_context::<AppState>();
    let tab = state.tab;
    let is = move |which: SidebarTab| tab.get() == which;

    view! {
        <div class="sidebar">
            <div class="sidebar-tabs">
                <button
                    class="tab"
                    class:active=move || is(SidebarTab::Pipelines)
                    on:click=move |_| tab.set(SidebarTab::Pipelines)
                >
                    "pipelines"
                </button>
                <button
                    class="tab"
                    class:active=move || is(SidebarTab::Connections)
                    on:click=move |_| tab.set(SidebarTab::Connections)
                >
                    "connections"
                </button>
            </div>
            // two `Show`s rather than one with a fallback: each list keeps its
            // own state that way, so switching tabs doesn't disarm a delete or
            // re-fetch anything
            <Show when=move || is(SidebarTab::Pipelines)>
                <PipelineList />
            </Show>
            <Show when=move || is(SidebarTab::Connections)>
                <ConnectionList />
            </Show>
        </div>
    }
}

/// The pipelines, and the two ways to change them: the `+` that opens the
/// "add pipeline" modal, and a delete on each row.
///
/// The list itself has two arrangements — a flat one in id order and a tree
/// A sidebar filter box, with a `×` to empty it.
///
/// One component for both sidebars because they filter the same way and should
/// look the same doing it — the pipeline list and the component reference.
///
/// The `×` is rendered only once there is something to clear
/// ([`sidebar::clearable`]), and clearing puts the caret back in the box: the
/// reason to abandon a search is nearly always to start another one, and having
/// to click back into the field afterwards would undo the point of the button.
/// It replaces the native `type="search"` cancel button rather than sitting
/// beside it — that one is webkit-only, so leaving it would mean two different
/// clear buttons depending on the browser.
#[component]
fn SearchBox(#[prop(into)] placeholder: String, query: RwSignal<String>) -> impl IntoView {
    let input: NodeRef<leptos::html::Input> = NodeRef::new();
    view! {
        <div class="search-box">
            <input
                node_ref=input
                class="search"
                type="search"
                placeholder=placeholder
                prop:value=move || query.get()
                on:input=move |ev| query.set(event_target_value(&ev))
            />
            // a plain closure rather than <Show>, whose children have to be
            // Send + Sync — the node ref this one captures is neither
            {move || {
                sidebar::clearable(&query.get())
                    .then(|| {
                        view! {
                            <button
                                class="search-clear"
                                type="button"
                                aria-label="clear search"
                                title="clear search"
                                on:click=move |_| {
                                    query.set(String::new());
                                    if let Some(input) = input.get_untracked() {
                                        let _ = input.focus();
                                    }
                                }
                            >
                                "×"
                            </button>
                        }
                    })
            }}
        </div>
    }
}

/// nested by upstream — and a search box over both. Which rows that comes to is
/// [`crate::sidebar`]'s problem; this only draws them.
#[component]
fn PipelineList() -> impl IntoView {
    let state = expect_context::<AppState>();
    // Deleting stops a running pipeline and can't be undone, so the button
    // arms rather than fires. One row at a time: arming a second disarms the
    // first, and clicking anywhere else in the list disarms it too.
    let armed = RwSignal::new(Option::<PipelineId>::None);
    let failure = RwSignal::new(Option::<String>::None);
    // Local to the list rather than kept in `AppState`, unlike the mode: a
    // filter is something you are doing right now, and coming back to the tab
    // to a list still narrowed by a word you typed earlier reads as a bug.
    let query = RwSignal::new(String::new());
    let mode = state.sidebar_mode;

    // The list as rows: the graph shape, the mode and the search box, all of
    // which are somebody else's pure function.
    let rows = Memo::new(move |_| {
        let Some(Ok(list)) = state.pipelines.get() else {
            return Vec::new();
        };
        let pairs: Vec<(PipelineId, Config)> = list
            .iter()
            .map(|s| (s.id.clone(), s.config.clone()))
            .collect();
        sidebar::rows(&pipelines_from(&pairs), mode.get(), &query.get())
    });
    // whether the list is empty because nothing matched or because there is
    // nothing to match — two different things to say
    let any_pipelines = Memo::new(move |_| {
        matches!(state.pipelines.get(), Some(Ok(list)) if !list.is_empty())
    });
    let load_error = Memo::new(move |_| match state.pipelines.get() {
        Some(Err(err)) => Some(err.to_string()),
        _ => None,
    });

    let delete = move |id: PipelineId| {
        armed.set(None);
        failure.set(None);
        leptos::task::spawn_local(async move {
            let result = ApiClient {
                base: String::new(),
            }
            .delete_pipeline(&id)
            .await;
            match result {
                Ok(()) => state.refresh(),
                Err(err) => failure.set(Some(err.to_string())),
            }
        });
    };

    view! {
        <div class="sidebar-list">
            <div class="sidebar-header">
                <span class="sidebar-title">"pipelines"</span>
                // available read-only too: how a list is arranged is a way of
                // reading it, not a change to anything
                <button
                    class="icon-button mode-toggle"
                    title=move || mode.get().title()
                    on:click=move |_| mode.update(|m| *m = m.toggled())
                >
                    {move || mode.get().label()}
                </button>
                <Show when=move || state.editing()>
                    <button
                        class="icon-button"
                        title="add pipeline"
                        on:click=move |_| state.adding.set(true)
                    >
                        "+"
                    </button>
                </Show>
            </div>
            // outside the list below, which is rebuilt on every keystroke: an
            // input that gets rebuilt as it is typed into loses the caret
            <SearchBox placeholder="search pipelines" query=query />
            {move || {
                failure
                    .get()
                    .map(|message| view! { <div class="sidebar-error">{message}</div> })
            }}
            {move || {
                load_error.get().map(|err| view! { <p>"error: " {err}</p> })
            }}
            // Rebuilt wholesale rather than keyed with <For>, for two reasons:
            // the same pipeline can appear twice in the tree, so there is no
            // key to be had, and filtering changes the list's contents without
            // changing the ids in it.
            {move || {
                rows.get()
                    .into_iter()
                    .map(|row| {
                        let Row { id, depth, repeat } = row;
                        let (focus_id, arm_id, delete_id, is_armed) = (
                            id.clone(),
                            id.clone(),
                            id.clone(),
                            id.clone(),
                        );
                        let armed_here = Memo::new(move |_| {
                            armed.get().as_deref() == Some(is_armed.as_str())
                        });
                        view! {
                            <div
                                class="tree-item"
                                class:repeat=repeat
                                // one level of nesting per grid step; a flat
                                // list is every row at zero, so this is the
                                // same rule in both modes
                                style:padding-left=format!("{}px", 8 + depth * 12)
                                title=repeat.then_some("also fed by this — shown in full elsewhere")
                                on:click=move |_| {
                                    armed.set(None);
                                    state.canvas_state.focus_request.set(Some(focus_id.clone()));
                                }
                            >
                                <span class="tree-label">{id.clone()}</span>
                                // A repeat is a pointer to a row that is on
                                // screen in full somewhere else, so it gets no
                                // delete: two buttons for one pipeline, arming
                                // together (the armed state is the id), would
                                // read as two pipelines.
                                {(!repeat)
                                    .then(|| {
                                        view! {
                                            // read-only means read-only: the
                                            // delete isn't disabled, it isn't there
                                            <Show when=move || state.editing()>
                                                <button
                                                    class="icon-button danger"
                                                    class:armed=move || armed_here.get()
                                                    title=move || {
                                                        if armed_here.get() {
                                                            "click again to delete"
                                                        } else {
                                                            "delete pipeline"
                                                        }
                                                    }
                                                    on:click={
                                                        let (arm_id, delete_id) = (
                                                            arm_id.clone(),
                                                            delete_id.clone(),
                                                        );
                                                        move |ev| {
                                                            // the row itself moves the camera
                                                            ev.stop_propagation();
                                                            if armed_here.get() {
                                                                delete(delete_id.clone());
                                                            } else {
                                                                armed.set(Some(arm_id.clone()));
                                                            }
                                                        }
                                                    }
                                                >
                                                    {move || if armed_here.get() { "sure?" } else { "×" }}
                                                </button>
                                            </Show>
                                        }
                                    })}
                            </div>
                        }
                    })
                    .collect_view()
            }}
            {move || {
                (rows.get().is_empty() && any_pipelines.get())
                    .then(|| {
                        view! {
                            <div class="empty">"no pipeline matches “" {move || query.get()} "”"</div>
                        }
                    })
            }}
        </div>
    }
}

/// The connections, with the same `+` and the same armed delete.
///
/// A row is a name and a kind; clicking one opens its settings in a card
/// beside it. There is no camera to move — a connection isn't on the canvas —
/// so the card *is* what a row clicks through to, and it is the only place the
/// settings can be read at all. Showing a credential there is safe for the
/// reason `Secret` exists: what is stored is the `${NAME}` reference, and that
/// is what the row renders.
#[component]
fn ConnectionList() -> impl IntoView {
    let state = expect_context::<AppState>();
    let armed = RwSignal::new(Option::<String>::None);
    let failure = RwSignal::new(Option::<String>::None);
    // which connection's card is open, and the viewport y its row was at when
    // it opened. The card is `position: fixed` because the sidebar scrolls —
    // `overflow-y: auto` computes overflow-x to `auto` too, so an absolutely
    // positioned card escaping to the right would be clipped by it instead.
    let opened = RwSignal::new(Option::<OpenConnection>::None);

    let delete = move |id: String| {
        armed.set(None);
        failure.set(None);
        // the card is about to be about a connection that no longer exists
        opened.set(None);
        leptos::task::spawn_local(async move {
            let result = ApiClient {
                base: String::new(),
            }
            .delete_connection(&id)
            .await;
            match result {
                Ok(()) => state.refresh(),
                // the useful case: "still used by a, b" — which is the list of
                // pipelines to deal with first
                Err(err) => failure.set(Some(err.to_string())),
            }
        });
    };

    // The card is pinned to a row's position at the moment it opened, so a
    // scroll would strand it beside the wrong name. The element that scrolls is
    // the sidebar, an ancestor this component doesn't own, and scroll events
    // don't bubble — so this listens on the document in the capture phase,
    // which is the one way to hear a scroll anywhere above.
    let list = NodeRef::<leptos::html::Div>::new();
    let _ = use_event_listener_with_options(
        document(),
        leptos::ev::scroll,
        move |ev| {
            // ...but every card on the canvas has a log that scrolls itself as
            // messages arrive, and capturing at the document hears all of them.
            // Only a scroll of something the list is *inside* can move the row.
            if scrolled_above(&ev, list) {
                opened.set(None);
            }
        },
        UseEventListenerOptions::default().capture(true),
    );

    view! {
        <div class="sidebar-list" node_ref=list>
            <div class="sidebar-header">
                <span class="sidebar-title">"connections"</span>
                <Show when=move || state.editing()>
                    <button
                        class="icon-button"
                        title="add connection"
                        on:click=move |_| state.adding_connection.set(true)
                    >
                        "+"
                    </button>
                </Show>
            </div>
            {move || {
                failure
                    .get()
                    .map(|message| view! { <div class="sidebar-error">{message}</div> })
            }}
            {move || {
                let list = state.connection_list();
                if list.is_empty() {
                    return view! {
                        <div class="empty">"no connections configured"</div>
                    }
                        .into_any();
                }
                list.into_iter()
                    .map(|(id, kind)| {
                        let (arm_id, delete_id, is_armed) = (id.clone(), id.clone(), id.clone());
                        let open_id = StoredValue::new(id.clone());
                        let armed_here = Memo::new(move |_| {
                            armed.get().as_deref() == Some(is_armed.as_str())
                        });
                        let open_here = Memo::new(move |_| {
                            opened.with(|o| {
                                o.as_ref().is_some_and(|o| {
                                    open_id.with_value(|id| o.id == *id)
                                })
                            })
                        });
                        view! {
                            <div
                                class="tree-item"
                                class:selected=move || open_here.get()
                                on:click=move |ev| {
                                    armed.set(None);
                                    if open_here.get() {
                                        opened.set(None);
                                    } else {
                                        opened
                                            .set(
                                                Some(
                                                    OpenConnection::at(open_id.get_value(), &ev),
                                                ),
                                            );
                                    }
                                }
                            >
                                <span class="tree-label">{id.clone()}</span>
                                <span class="section-kind">{kind}</span>
                                <Show when=move || state.editing()>
                                    <button
                                        class="icon-button danger"
                                        class:armed=move || armed_here.get()
                                        title=move || {
                                            if armed_here.get() {
                                                "click again to delete"
                                            } else {
                                                "delete connection"
                                            }
                                        }
                                        on:click={
                                            let (arm_id, delete_id) = (
                                                arm_id.clone(),
                                                delete_id.clone(),
                                            );
                                            move |ev| {
                                                ev.stop_propagation();
                                                if armed_here.get() {
                                                    delete(delete_id.clone());
                                                } else {
                                                    armed.set(Some(arm_id.clone()));
                                                }
                                            }
                                        }
                                    >
                                        {move || if armed_here.get() { "sure?" } else { "×" }}
                                    </button>
                                </Show>
                            </div>
                        }
                    })
                    .collect_view()
                    .into_any()
            }}
            {move || {
                opened.get().map(|open| view! { <ConnectionCard open=open /> })
            }}
        </div>
    }
}

/// Whether a scroll happened in something `inside` sits within — which is to
/// say, whether it moved `inside` on the screen. `Node::contains` reports true
/// of a node and itself, which is right here: a list that scrolls its own
/// content moves its rows too.
fn scrolled_above(ev: &leptos::ev::Event, inside: NodeRef<leptos::html::Div>) -> bool {
    use wasm_bindgen::JsCast;
    let Some(el) = inside.get_untracked() else {
        return false;
    };
    ev.target()
        .and_then(|t| t.dyn_into::<leptos::web_sys::Node>().ok())
        .is_some_and(|scrolled| scrolled.contains(Some(&el)))
}

/// The connection whose settings are on show, and where to put the card.
///
/// The row's position is captured when it is clicked rather than measured on
/// render: the card is `position: fixed`, and a rect read during rendering
/// would be a rect the row may not have yet.
#[derive(Clone, Debug, PartialEq)]
struct OpenConnection {
    id: String,
    /// viewport y of the row's top edge
    top: f64,
}

impl OpenConnection {
    /// Placed against the row the click landed on — `current_target`, not
    /// `target`, since the click may well have landed on the label inside it.
    fn at(id: String, ev: &leptos::ev::MouseEvent) -> Self {
        use wasm_bindgen::JsCast;
        let top = ev
            .current_target()
            .and_then(|t| t.dyn_into::<leptos::web_sys::Element>().ok())
            .map_or(0.0, |el| el.get_bounding_client_rect().top());
        Self { id, top }
    }
}

/// A connection's settings, beside the name in the list.
///
/// The card is the inspector's section renderer pointed at a connection, so a
/// new connection kind — or a new field on one — shows up here with no change:
/// the rows come from the same reflection the "add connection" form does.
#[component]
fn ConnectionCard(open: OpenConnection) -> impl IntoView {
    let state = expect_context::<AppState>();
    let id = StoredValue::new(open.id.clone());

    let section = Memo::new(move |_| {
        let connections = state.connections.get()?;
        let connections = connections.as_ref().ok()?;
        id.with_value(|id| connections.get(id))
            .map(inspector::connection_section)
    });

    view! {
        <div class="connection-card" style:top=format!("{}px", open.top)>
            <div class="connection-card-name">{open.id}</div>
            {move || match section.get() {
                Some(section) => view! { <SectionView section=section /> }.into_any(),
                // the list and the card read the same resource, so this is only
                // the moment between a delete landing and the list reloading
                None => view! { <div class="empty">"connection is gone"</div> }.into_any(),
            }}
        </div>
    }
}

/// Add a connection: a name, a kind, and that kind's settings.
///
/// The "add pipeline" modal with one component instead of a list — same
/// reflected fields, same uncontrolled boxes, same reason for both. Secrets are
/// referenced here, never entered: a field takes `${NAME}`, and what that
/// resolves to is the deployment's business and never the UI's.
#[component]
fn AddConnectionModal() -> impl IntoView {
    let state = expect_context::<AppState>();
    let docs = StoredValue::new(kayak_core::docs::connection_components());

    let id = RwSignal::new(String::new());
    let errors = RwSignal::new(Vec::<form::FormError>::new());
    let rejected = RwSignal::new(Option::<String>::None);
    let submitting = RwSignal::new(false);

    // the first kind, so the form is never in a state with no fields; there is
    // always at least one, or there would be nothing to connect to
    let draft = docs.with_value(|docs| docs.first().map(DraftSignals::new));

    let close = move || state.adding_connection.set(false);
    let _ = use_event_listener(use_window(), leptos::ev::keydown, move |ev| {
        if ev.key() == "Escape" {
            close();
        }
    });

    let submit = move || {
        let Some(draft) = draft else {
            return;
        };
        if submitting.get_untracked() {
            return;
        }
        rejected.set(None);
        let snapshot = draft.snapshot();
        let built =
            docs.with_value(|docs| form::build_connection(&id.get_untracked(), &snapshot, docs));
        let body = match built {
            Ok(body) => body,
            Err(found) => {
                errors.set(found);
                return;
            }
        };
        errors.set(Vec::new());
        submitting.set(true);
        leptos::task::spawn_local(async move {
            let result = ApiClient {
                base: String::new(),
            }
            .create_connection(&body)
            .await;
            submitting.set(false);
            match result {
                Ok(()) => {
                    state.refresh();
                    state.adding_connection.set(false);
                }
                Err(err) => rejected.set(Some(err.to_string())),
            }
        });
    };

    view! {
        <div class="modal-backdrop" on:click=move |_| close()>
            <div class="modal" on:click=move |ev| ev.stop_propagation()>
                <header>
                    <span class="modal-title">"add connection"</span>
                    <button class="icon-button" title="close" on:click=move |_| close()>
                        "×"
                    </button>
                </header>

                <div class="modal-body">
                    <div class="form-row">
                        <label for="connection-id">"name"</label>
                        <input
                            id="connection-id"
                            class="text-input"
                            placeholder="what pipelines will refer to, e.g. prod-kafka"
                            on:input=move |ev| id.set(event_target_value(&ev))
                        />
                    </div>

                    {match draft {
                        Some(draft) => {
                            view! {
                                <section class="stage">
                                    <ComponentEditor
                                        index=0
                                        draft=draft
                                        // a connection form has exactly one
                                        // component, so there is no list to
                                        // remove it from and nothing to point
                                        // at another pipeline
                                        drafts=RwSignal::new(Vec::new())
                                        errors=errors
                                        docs=docs
                                        pipelines=Signal::derive(Vec::new)
                                        connections=Signal::derive(Vec::new)
                                        removable=false
                                    />
                                </section>
                            }
                                .into_any()
                        }
                        None => view! { <div class="empty">"no connection kinds"</div> }.into_any(),
                    }}
                </div>

                <footer>
                    <div class="modal-messages">
                        {move || {
                            form::pipeline_errors(&errors.get())
                                .into_iter()
                                .map(|message| view! { <div class="form-error">{message}</div> })
                                .collect_view()
                        }}
                        {move || {
                            rejected
                                .get()
                                .map(|message| {
                                    view! { <div class="form-error">"server: " {message}</div> }
                                })
                        }}
                        // the same warning the pipeline modal carries, for the
                        // same reason: this lands in the runtime now and in the
                        // connections file only when someone saves
                        <div class="form-hint">
                            "available to new pipelines now — save the config to keep it"
                        </div>
                    </div>
                    <button class="button" on:click=move |_| close()>
                        "cancel"
                    </button>
                    <button
                        class="button primary"
                        disabled=move || submitting.get()
                        on:click=move |_| submit()
                    >
                        {move || if submitting.get() { "creating…" } else { "create" }}
                    </button>
                </footer>
            </div>
        </div>
    }
}

/// One component being configured in the modal.
///
/// The same thing [`form::ComponentDraft`] describes, but with each editable
/// part in its own signal. That split is what keeps the form usable: the field
/// list has to be rebuilt when the kind or the variant changes — different
/// components have different fields — but rebuilding it on every keystroke
/// would destroy the `<input>` being typed into and take the caret with it. So
/// the boxes are uncontrolled: they write to `values` and never read it back.
#[derive(Clone, Copy)]
struct DraftSignals {
    family: Family,
    kind: RwSignal<String>,
    variant: RwSignal<Option<String>>,
    values: RwSignal<HashMap<String, String>>,
}

impl DraftSignals {
    fn new(doc: &kayak_core::docs::ComponentDoc) -> Self {
        let draft = form::draft_of(doc);
        Self {
            family: draft.family,
            kind: RwSignal::new(draft.kind),
            variant: RwSignal::new(draft.variant),
            values: RwSignal::new(draft.values),
        }
    }

    /// What the pure form logic validates and builds from.
    fn snapshot(self) -> form::ComponentDraft {
        form::ComponentDraft {
            family: self.family,
            kind: self.kind.get_untracked(),
            variant: self.variant.get_untracked(),
            values: self.values.get_untracked(),
        }
    }
}

/// Add a pipeline: pick its components, fill in their settings, post it.
///
/// Nothing about the form is written by hand — every control comes from
/// `kayak_core::docs`, which reflects the fields out of the config schemas,
/// so a new component shows up here for the same reason it shows up on `/docs`.
/// The validation is [`crate::form`]'s, which is pure and unit tested; this
/// component only renders drafts and shows what comes back.
#[component]
fn AddPipelineModal() -> impl IntoView {
    let state = expect_context::<AppState>();
    let docs = StoredValue::new(all_components());

    let id = RwSignal::new(String::new());
    let drafts = RwSignal::new(Vec::<DraftSignals>::new());
    let errors = RwSignal::new(Vec::<form::FormError>::new());
    // the server's own answer, which says things the form can't know — a
    // duplicate id, an upstream that doesn't exist
    let rejected = RwSignal::new(Option::<String>::None);
    let submitting = RwSignal::new(false);

    // What a field that names a connection can be set to. Like the pipeline
    // list below it is server state rather than anything the schema knows, and
    // it carries the kind so a kafka field offers only kafka connections.
    let connections = Signal::derive(move || state.connection_list());
    // What a field that names another pipeline can be set to: the ids the
    // server is running right now. The one being added is not among them — it
    // doesn't exist yet, and could not feed itself if it did.
    let pipelines = Signal::derive(move || {
        let Some(res) = state.pipelines.get() else {
            return Vec::new();
        };
        let Ok(list) = res.as_ref() else {
            return Vec::new();
        };
        list.iter().map(|s| s.id.clone()).collect::<Vec<String>>()
    });

    let close = move || {
        state.adding.set(false);
    };
    // on the window rather than on the panel: a keydown only reaches an element
    // that has focus, and the panel doesn't until something in it is clicked
    let _ = use_event_listener(use_window(), leptos::ev::keydown, move |ev| {
        if ev.key() == "Escape" {
            close();
        }
    });

    let add = move |family: Family| {
        docs.with_value(|docs| {
            if let Some(first) = form::kinds_in(docs, family).first() {
                let draft = DraftSignals::new(first);
                drafts.update(|d| d.push(draft));
            }
        });
    };
    // a pipeline needs an input, so start it with one rather than with an
    // empty form and an error waiting to happen
    add(Family::Input);

    let submit = move || {
        if submitting.get_untracked() {
            return;
        }
        rejected.set(None);
        let snapshots: Vec<form::ComponentDraft> = drafts
            .get_untracked()
            .into_iter()
            .map(DraftSignals::snapshot)
            .collect();
        let built =
            docs.with_value(|docs| form::build_config(&id.get_untracked(), &snapshots, docs));
        let body = match built {
            Ok(body) => body,
            Err(found) => {
                errors.set(found);
                return;
            }
        };
        errors.set(Vec::new());
        submitting.set(true);
        leptos::task::spawn_local(async move {
            let result = ApiClient {
                base: String::new(),
            }
            .create_pipeline(&body)
            .await;
            submitting.set(false);
            match result {
                Ok(_) => {
                    state.refresh();
                    state.adding.set(false);
                }
                Err(err) => rejected.set(Some(err.to_string())),
            }
        });
    };

    view! {
        // clicking the backdrop is the same as cancelling; clicking the panel
        // must not be, hence the stopped propagation below
        <div class="modal-backdrop" on:click=move |_| close()>
            <div class="modal" on:click=move |ev| ev.stop_propagation()>
                <header>
                    <span class="modal-title">"add pipeline"</span>
                    <button class="icon-button" title="close" on:click=move |_| close()>
                        "×"
                    </button>
                </header>

                <div class="modal-body">
                    <div class="form-row">
                        <label for="pipeline-id">"id"</label>
                        <input
                            id="pipeline-id"
                            class="text-input"
                            placeholder="optional — generated if left blank"
                            on:input=move |ev| id.set(event_target_value(&ev))
                        />
                    </div>

                    <StageEditor
                        family=Family::Input
                        drafts=drafts
                        errors=errors
                        docs=docs
                        pipelines=pipelines
                        connections=connections
                    />
                    <StageEditor
                        family=Family::Transform
                        drafts=drafts
                        errors=errors
                        docs=docs
                        pipelines=pipelines
                        connections=connections
                    />
                    <StageEditor
                        family=Family::Output
                        drafts=drafts
                        errors=errors
                        docs=docs
                        pipelines=pipelines
                        connections=connections
                    />
                </div>

                <footer>
                    <div class="modal-messages">
                        {move || {
                            form::pipeline_errors(&errors.get())
                                .into_iter()
                                .map(|message| view! { <div class="form-error">{message}</div> })
                                .collect_view()
                        }}
                        {move || {
                            rejected
                                .get()
                                .map(|message| {
                                    view! { <div class="form-error">"server: " {message}</div> }
                                })
                        }}
                        // the pipeline starts running the moment this is
                        // accepted; the file is a separate, explicit step, and
                        // saying so here is what stops "create" from reading
                        // like "save"
                        <div class="form-hint">
                            {move || {
                                if state.config_file.get().is_some() {
                                    "starts running now — save the config to keep it"
                                } else {
                                    "starts running now — no config file to save it to"
                                }
                            }}
                        </div>
                    </div>
                    <button class="button" on:click=move |_| close()>
                        "cancel"
                    </button>
                    <button
                        class="button primary"
                        disabled=move || submitting.get()
                        on:click=move |_| submit()
                    >
                        {move || if submitting.get() { "creating…" } else { "create" }}
                    </button>
                </footer>
            </div>
        </div>
    }
}

/// One of the three stages, with its components and a button to add another.
#[component]
fn StageEditor(
    family: Family,
    drafts: RwSignal<Vec<DraftSignals>>,
    errors: RwSignal<Vec<form::FormError>>,
    docs: StoredValue<Vec<kayak_core::docs::ComponentDoc>>,
    /// The pipelines a field naming one can point at.
    pipelines: Signal<Vec<String>>,
    /// The connections a field naming one can point at, as `(name, kind)`.
    connections: Signal<Vec<(String, String)>>,
) -> impl IntoView {
    let add = move |_| {
        docs.with_value(|docs| {
            if let Some(first) = form::kinds_in(docs, family).first() {
                let draft = DraftSignals::new(first);
                drafts.update(|d| d.push(draft));
            }
        });
    };

    view! {
        <section class="stage">
            <div class="stage-header">
                <span class="section-kind">{family.label()}</span>
                <button class="icon-button" title=format!("add {}", form::singular(family)) on:click=add>
                    "+"
                </button>
            </div>
            // Rebuilt rather than keyed: the index *is* the identity here (it's
            // what an error names), and it shifts when a component is removed.
            {move || {
                drafts
                    .get()
                    .into_iter()
                    .enumerate()
                    .filter(|(_, draft)| draft.family == family)
                    .map(|(index, draft)| {
                        view! {
                            <ComponentEditor
                                index=index
                                draft=draft
                                drafts=drafts
                                errors=errors
                                docs=docs
                                pipelines=pipelines
                                connections=connections
                            />
                        }
                    })
                    .collect_view()
            }}
        </section>
    }
}

/// One component: which kind it is, and the fields that kind has.
#[component]
fn ComponentEditor(
    index: usize,
    draft: DraftSignals,
    drafts: RwSignal<Vec<DraftSignals>>,
    errors: RwSignal<Vec<form::FormError>>,
    docs: StoredValue<Vec<kayak_core::docs::ComponentDoc>>,
    pipelines: Signal<Vec<String>>,
    connections: Signal<Vec<(String, String)>>,
    /// Whether this component can be taken out again. False for the connection
    /// form, which has exactly one and would be nothing without it.
    #[prop(default = true)]
    removable: bool,
) -> impl IntoView {
    let family = draft.family;
    let doc =
        move || docs.with_value(|docs| form::doc_for(docs, family, &draft.kind.get()).cloned());

    // Changing the kind changes which fields exist, so whatever was typed into
    // the old ones has nowhere to go. Clearing is the honest option: keeping
    // the values would silently carry a `subject` from nats to kafka.
    let choose_kind = move |ev: leptos::ev::Event| {
        let kind = event_target_value(&ev);
        let variant = docs.with_value(|docs| {
            form::doc_for(docs, family, &kind)
                .and_then(|d| d.variants.first().map(|v| v.name.clone()))
        });
        draft.values.set(HashMap::new());
        draft.variant.set(variant);
        draft.kind.set(kind);
    };

    // by position, not by identity: the list is rebuilt whenever it changes, so
    // `index` is always this component's current place in it
    let remove = move |_| {
        drafts.update(|d| {
            if index < d.len() {
                d.remove(index);
            }
        });
    };

    view! {
        <div class="component-editor">
            <div class="component-header">
                <select class="select" on:change=choose_kind>
                    {move || {
                        let selected = draft.kind.get();
                        docs.with_value(|docs| {
                            form::kinds_in(docs, family)
                                .into_iter()
                                .map(|component| {
                                    let kind = component.kind.clone();
                                    let label = kind.clone();
                                    view! {
                                        <option value=kind.clone() selected=kind == selected>
                                            {label}
                                        </option>
                                    }
                                })
                                .collect_view()
                        })
                    }}
                </select>
                <Show when=move || removable>
                    <button class="icon-button danger" title="remove" on:click=remove>
                        "×"
                    </button>
                </Show>
            </div>

            // Rebuilt when the kind or the variant changes — which is exactly
            // when the fields are different ones. The inputs inside are
            // uncontrolled, so typing doesn't come back through here.
            {move || {
                let Some(doc) = doc() else {
                    return view! { <div class="empty">"unknown component"</div> }.into_any();
                };
                let variants = doc.variants.clone();
                let fields = form::fields_of(&doc, draft.variant.get().as_deref());
                view! {
                    <Show when={
                        let has = !variants.is_empty();
                        move || has
                    }>
                        <div class="form-row">
                            <label>"form"</label>
                            <select
                                class="select"
                                on:change=move |ev| {
                                    // the variants share field names but not
                                    // field types, so the values go too
                                    draft.values.set(HashMap::new());
                                    draft.variant.set(Some(event_target_value(&ev)));
                                }
                            >
                                {
                                    let selected = draft.variant.get();
                                    variants
                                        .clone()
                                        .into_iter()
                                        .map(|variant| {
                                            let name = variant.name.clone();
                                            let label = name.clone();
                                            view! {
                                                <option
                                                    value=name.clone()
                                                    selected=Some(name) == selected
                                                >
                                                    {label}
                                                </option>
                                            }
                                        })
                                        .collect_view()
                                }
                            </select>
                        </div>
                    </Show>
                    {if fields.is_empty() {
                        view! { <div class="empty">"no settings"</div> }.into_any()
                    } else {
                        fields
                            .into_iter()
                            .map(|field| {
                                view! {
                                    <FieldEditor
                                        field=field
                                        index=index
                                        values=draft.values
                                        errors=errors
                                        pipelines=pipelines
                                        connections=connections
                                    />
                                }
                            })
                            .collect_view()
                            .into_any()
                    }}
                }
                    .into_any()
            }}
        </div>
    }
}

/// One field: a control chosen by the field's type, and whatever the validator
/// had to say about it.
#[component]
fn FieldEditor(
    field: kayak_core::docs::FieldDoc,
    index: usize,
    values: RwSignal<HashMap<String, String>>,
    errors: RwSignal<Vec<form::FormError>>,
    pipelines: Signal<Vec<String>>,
    connections: Signal<Vec<(String, String)>>,
) -> impl IntoView {
    let name = field.name.clone();
    // read once, on purpose: the control is uncontrolled from here on, so that
    // typing into it doesn't rebuild it
    let initial = values.with_untracked(|v| v.get(&name).cloned().unwrap_or_default());
    let error_name = name.clone();
    let error = Memo::new(move |_| {
        errors.with(|errors| form::field_error(errors, index, &error_name).map(ToString::to_string))
    });

    let cleared_name = name.clone();
    let write = move |ev: leptos::ev::Event| {
        let value = event_target_value(&ev);
        values.update(|v| {
            v.insert(name.clone(), value);
        });
        errors.update(|errors| form::clear_field_error(errors, index, &cleared_name));
    };

    // A closed set of values is a dropdown; everything else is a box. The
    // placeholder carries the rendered type, which for a structured field is
    // the only hint that it wants JSON.
    let control = match &field.field_type {
        FieldType::Enum(options) => {
            let unset = initial.is_empty();
            let options = options.clone();
            // A dropdown with nothing chosen must not *look* like it has: a
            // browser shows the first option, but no change event has fired, so
            // nothing was recorded and the field would fail as "required" while
            // displaying a perfectly good value. A blank entry, selected, keeps
            // what is shown and what is stored the same thing. Optional fields
            // keep theirs for good — it is how a field gets unset again.
            let blank = !field.required || unset;
            view! {
                <select class="select" on:change=write>
                    <Show when=move || blank>
                        <option value="" selected=unset></option>
                    </Show>
                    {
                        let selected = initial.clone();
                        options
                            .clone()
                            .into_iter()
                            .map(|value| {
                                let label = value.clone();
                                view! {
                                    <option value=value.clone() selected=value == selected>
                                        {label}
                                    </option>
                                }
                            })
                            .collect_view()
                    }
                </select>
            }
            .into_any()
        }
        // The set of valid answers is the running graph, not anything the
        // schema could list, so it comes from the pipeline list rather than
        // from the field. Unlike every other control here this one reads its
        // options back: they arrive with the pipeline list, which may land
        // after the modal opened, so the chosen id is re-marked on each
        // rebuild rather than lost.
        FieldType::PipelineId => {
            let chosen = field.name.clone();
            let picked =
                move || values.with_untracked(|v| v.get(&chosen).cloned().unwrap_or_default());
            view! {
                <select class="select" on:change=write>
                    {move || {
                        let selected = picked();
                        let available = pipelines.get();
                        // a blank entry for the same reason the enum above has
                        // one, and because "there is nothing to point at yet"
                        // has to be said somewhere
                        let blank = if available.is_empty() {
                            "no other pipelines yet"
                        } else {
                            ""
                        };
                        view! {
                            <option value="" selected=selected.is_empty()>
                                {blank}
                            </option>
                            {available
                                .into_iter()
                                .map(|id| {
                                    let label = id.clone();
                                    view! {
                                        <option value=id.clone() selected=id == selected>
                                            {label}
                                        </option>
                                    }
                                })
                                .collect_view()}
                        }
                    }}
                </select>
            }
            .into_any()
        }
        // The same trick as the pipeline dropdown above, narrowed by kind: a
        // kafka input can only use a kafka connection, so offering the nats one
        // would only be a way to build a pipeline that fails.
        FieldType::Connection(kind) => {
            let kind = kind.clone();
            let chosen = field.name.clone();
            let picked =
                move || values.with_untracked(|v| v.get(&chosen).cloned().unwrap_or_default());
            view! {
                <select class="select" on:change=write>
                    {move || {
                        let selected = picked();
                        let available: Vec<String> = connections
                            .get()
                            .into_iter()
                            .filter(|(_, of_kind)| *of_kind == kind)
                            .map(|(id, _)| id)
                            .collect();
                        // "there is nothing to point at" has to be said, and
                        // said as the thing that is missing: the fix is to add
                        // a connection of *this* kind, on the other tab
                        let blank = if available.is_empty() {
                            format!("no {kind} connections yet")
                        } else {
                            String::new()
                        };
                        view! {
                            <option value="" selected=selected.is_empty()>
                                {blank}
                            </option>
                            {available
                                .into_iter()
                                .map(|id| {
                                    let label = id.clone();
                                    view! {
                                        <option value=id.clone() selected=id == selected>
                                            {label}
                                        </option>
                                    }
                                })
                                .collect_view()}
                        }
                    }}
                </select>
            }
            .into_any()
        }
        _ => view! {
            <input
                class="text-input"
                value=initial.clone()
                placeholder=field.type_name.clone()
                on:input=write
            />
        }
        .into_any(),
    };

    view! {
        <div class="form-row">
            <label title=field.description.clone().unwrap_or_default()>
                {field.name.clone()}
                <Show when={
                    let required = field.required;
                    move || required
                }>
                    <span class="required-marker" title="required">"*"</span>
                </Show>
            </label>
            <div class="form-control" class:invalid=move || error.get().is_some()>
                {control}
                {move || error.get().map(|message| view! { <div class="form-error">{message}</div> })}
            </div>
        </div>
    }
}

/// Shared by both pages. The zoom readout is canvas-only, so it's driven by an
/// optional context rather than by knowing which page it's on: `/docs` provides
/// no `AppState` and simply gets no readout.
#[component]
pub fn Navbar() -> impl IntoView {
    let state = use_context::<AppState>();
    let canvas = state.map(|state| state.canvas_state);
    let zoom = move || canvas.map(|c| format!("{:.0}%", c.camera.get().zoom * 100.0));
    view! {
        <aside class="navbar">
            <div class="brand">"kayak"</div>
            <nav class="nav-links">
                <A href="/" exact=true>"canvas"</A>
                <A href="/docs">"docs"</A>
            </nav>
            {state.map(|state| view! { <ModeControls state=state /> })}
            <div class="zoom-level" title="scroll to zoom, drag to pan">
                {zoom}
            </div>
        </aside>
    }
}

/// The read-only / edit switch, and everything that only makes sense once
/// you're editing: whether there is unsaved work, and the two ways to resolve
/// it.
///
/// It lives in the navbar rather than the sidebar because it is about the
/// session as a whole, not about any one pipeline.
#[component]
fn ModeControls(state: AppState) -> impl IntoView {
    let reverting = RwSignal::new(false);
    let armed = RwSignal::new(false);
    let failure = RwSignal::new(Option::<String>::None);

    let revert = move || {
        armed.set(false);
        failure.set(None);
        reverting.set(true);
        leptos::task::spawn_local(async move {
            let result = ApiClient {
                base: String::new(),
            }
            .revert_config()
            .await;
            reverting.set(false);
            match result {
                Ok(()) => state.refresh(),
                Err(err) => failure.set(Some(err.to_string())),
            }
        });
    };

    view! {
        <div class="mode-controls">
            {move || {
                failure.get().map(|message| view! { <span class="mode-error">{message}</span> })
            }}

            <Show when=move || state.editing() && state.unsaved.get()>
                <span class="unsaved" title="the running graph is not in the config file">
                    "unsaved changes"
                </span>
            </Show>

            <Show when=move || state.editing()>
                // Arranging the canvas is easy to make a mess of and has no
                // undo, so there is one way back: throw the arrangement away and
                // let the automatic layout have it. Disabled rather than hidden
                // when there is nothing arranged — the answer to "can I start
                // over" shouldn't be a button that isn't there.
                <button
                    class="button"
                    disabled=move || state.canvas_state.arrangement.with(LayoutFile::is_empty)
                    title="put every card and edge back where the automatic layout wants them"
                    on:click=move |_| {
                        armed.set(false);
                        state.canvas_state.reset_arrangement();
                    }
                >
                    "auto layout"
                </button>
                {move || {
                    // A server started without --config has no file to revert
                    // to, but it can still be asked to write one: the graph on
                    // the canvas is exactly what a config file describes, so
                    // "create" is a save with nothing to overwrite.
                    if state.config_file.get().is_none() {
                        return view! {
                            <button
                                class="button primary"
                                title="write these pipelines out as a config file"
                                on:click=move |_| {
                                    armed.set(false);
                                    state.saving.set(true);
                                }
                            >
                                "create config file"
                            </button>
                        }
                            .into_any();
                    }
                    view! {
                        <button
                            class="button"
                            class:armed=move || armed.get()
                            disabled=move || reverting.get()
                            title="discard every change and reload the config file"
                            on:click=move |_| {
                                if armed.get_untracked() {
                                    revert();
                                } else {
                                    armed.set(true);
                                }
                            }
                        >
                            // reverting stops every running pipeline, so it
                            // takes two clicks like a delete does
                            {move || {
                                if reverting.get() {
                                    "reverting…"
                                } else if armed.get() {
                                    "discard changes?"
                                } else {
                                    "revert"
                                }
                            }}
                        </button>
                        <button
                            class="button primary"
                            on:click=move |_| {
                                armed.set(false);
                                state.saving.set(true);
                            }
                        >
                            "save as…"
                        </button>
                    }
                        .into_any()
                }}
            </Show>

            <button
                class="button mode-toggle"
                class:active=move || state.editing()
                title=move || {
                    if state.editing() {
                        "leave edit mode"
                    } else {
                        "add and remove pipelines"
                    }
                }
                on:click=move |_| {
                    armed.set(false);
                    state
                        .mode
                        .update(|mode| {
                            *mode = if mode.is_edit() { Mode::ReadOnly } else { Mode::Edit };
                        });
                }
            >
                {move || if state.editing() { "editing" } else { "edit" }}
            </button>
        </div>
    }
}

/// Where to write the running graph, and in which format.
///
/// A file name, not a path: the server only writes into one directory, and
/// offering a directory picker for a choice that doesn't exist would be a lie.
/// Overwriting is just typing the name it already has, which the modal points
/// out rather than hides.
///
/// It does double duty as "create a config file" on a server started without
/// one, because that *is* the same act — the difference is only that there is
/// nothing to overwrite, so the modal names the directory the new file will
/// appear in and suggests `config.json` to put in it.
///
/// The format picker and the file name are one decision shown twice: the
/// selection is *derived* from the extension rather than held separately, and
/// picking a format rewrites the name to match. That way there is no state in
/// which the button says "yaml" and the file is called `config.json` — whatever
/// the user last touched, the two agree.
/// What a config file is called when nobody has said otherwise. Only a
/// suggestion — the server takes whatever name comes back.
const DEFAULT_CONFIG_NAME: &str = "config.json";

#[component]
fn SaveAsModal() -> impl IntoView {
    let state = expect_context::<AppState>();
    let current = state.config_file.get_untracked().unwrap_or_default();
    // creating rather than saving over: the name box starts on the conventional
    // one instead of empty, so the common case is one click
    let creating = current.is_empty();
    let name = RwSignal::new(if creating {
        DEFAULT_CONFIG_NAME.to_string()
    } else {
        current.clone()
    });
    let saving = RwSignal::new(false);
    let failure = RwSignal::new(Option::<String>::None);
    let loaded = StoredValue::new(current);
    let format = Memo::new(move |_| ConfigFormat::of_file_name(&name.get()));
    let choose = move |chosen: ConfigFormat| name.update(|n| *n = chosen.rename(n));

    let close = move || state.saving.set(false);
    let _ = use_event_listener(use_window(), leptos::ev::keydown, move |ev| {
        if ev.key() == "Escape" {
            close();
        }
    });

    let overwrites = move || {
        let typed = name.get();
        loaded.with_value(|loaded| !loaded.is_empty() && typed.trim() == loaded)
    };

    let submit = move || {
        if saving.get_untracked() {
            return;
        }
        failure.set(None);
        saving.set(true);
        leptos::task::spawn_local(async move {
            let result = ApiClient {
                base: String::new(),
            }
            .save_config(&name.get_untracked(), format.get_untracked())
            .await;
            saving.set(false);
            match result {
                Ok(_) => {
                    // the save is what makes "unsaved changes" go away, and
                    // that answer lives on the server
                    state.refresh();
                    state.saving.set(false);
                }
                Err(err) => failure.set(Some(err.to_string())),
            }
        });
    };

    view! {
        <div class="modal-backdrop" on:click=move |_| close()>
            <div class="modal narrow" on:click=move |ev| ev.stop_propagation()>
                <header>
                    <span class="modal-title">
                        {if creating { "create config file" } else { "save as" }}
                    </span>
                    <button class="icon-button" title="close" on:click=move |_| close()>
                        "×"
                    </button>
                </header>
                <div class="modal-body">
                    <div class="form-row">
                        <label for="save-name">"file name"</label>
                        // controlled, unlike the component fields: picking a
                        // format rewrites the name, and the box has to show it
                        <input
                            id="save-name"
                            class="text-input"
                            prop:value=move || name.get()
                            placeholder="config.json"
                            on:input=move |ev| name.set(event_target_value(&ev))
                            on:keydown=move |ev| {
                                if ev.key() == "Enter" {
                                    submit();
                                }
                            }
                        />
                    </div>
                    <div class="form-row">
                        <label>"format"</label>
                        <div class="format-picker">
                            {[ConfigFormat::Json, ConfigFormat::Yaml]
                                .map(|option| {
                                    view! {
                                        <button
                                            class="button"
                                            class:active=move || format.get() == option
                                            title=move || {
                                                format!("write the file as {option}")
                                            }
                                            on:click=move |_| choose(option)
                                        >
                                            {option.to_string()}
                                        </button>
                                    }
                                })
                                .into_iter()
                                .collect::<Vec<_>>()}
                        </div>
                    </div>
                    <p class="form-hint">
                        {move || {
                            if creating {
                                // where it lands is the question a fresh server
                                // raises and a loaded one doesn't
                                match state.save_directory.get() {
                                    dir if dir.is_empty() => {
                                        "created in the server's working directory".to_string()
                                    }
                                    dir => format!("created in {dir}"),
                                }
                            } else {
                                "written next to the config the server was started with".to_string()
                            }
                        }}
                    </p>
                </div>
                <footer>
                    <div class="modal-messages">
                        {move || {
                            failure
                                .get()
                                .map(|message| view! { <div class="form-error">{message}</div> })
                        }}
                        <Show when=overwrites>
                            <div class="form-warning">"this replaces the file you started from"</div>
                        </Show>
                    </div>
                    <button class="button" on:click=move |_| close()>
                        "cancel"
                    </button>
                    <button
                        class="button primary"
                        disabled=move || saving.get() || name.get().trim().is_empty()
                        on:click=move |_| submit()
                    >
                        {move || match (saving.get(), creating) {
                            (true, _) => "saving…",
                            (false, true) => "create",
                            (false, false) => "save",
                        }}
                    </button>
                </footer>
            </div>
        </div>
    }
}

/// The component reference: every input, transform and output kayak can build,
/// generated from the config schemas rather than written by hand.
///
/// The docs are the same on every visit, so they're built once here and only
/// re-filtered as the search box changes.
#[component]
pub fn DocsPage() -> impl IntoView {
    let all = StoredValue::new(all_components());
    let query = RwSignal::new(String::new());
    let selected = RwSignal::new(Option::<String>::None);
    let groups = Memo::new(move |_| all.with_value(|all| docs::groups(all, &query.get())));

    view! {
        <Navbar />
        <div class="main-content">
            <DocsSidebar groups=groups query=query selected=selected />
            <div class="docs-content">
                <Show
                    when=move || docs::total(&groups.get()) != 0
                    fallback=move || {
                        view! {
                            <p class="empty">
                                "no component matches “" {move || query.get()} "”"
                            </p>
                        }
                    }
                >
                    // Rebuilt wholesale on every keystroke rather than keyed
                    // with <For>: the groups are keyed by family, and a family
                    // whose *contents* changed keeps its key, so a keyed list
                    // would leave filtered-out components on screen.
                    {move || {
                        groups
                            .get()
                            .into_iter()
                            .map(|group| {
                                view! {
                                    <section class="docs-family">
                                        <h2>{group.family.label()}</h2>
                                        {group
                                            .components
                                            .into_iter()
                                            .map(|component| {
                                                view! {
                                                    <ComponentDoc component=component selected=selected />
                                                }
                                            })
                                            .collect_view()}
                                    </section>
                                }
                            })
                            .collect_view()
                    }}
                </Show>
            </div>
        </div>
    }
}

/// Search box plus the matching components, grouped by family. Clicking one
/// scrolls the reference to it — the list is short enough that jumping is
/// friendlier than replacing the page with a single component.
#[component]
fn DocsSidebar(
    groups: Memo<Vec<docs::Group>>,
    query: RwSignal<String>,
    selected: RwSignal<Option<String>>,
) -> impl IntoView {
    view! {
        <div class="sidebar docs-sidebar">
            <SearchBox placeholder="search components" query=query />
            // rebuilt rather than keyed, for the same reason as the reference
            // pane: the key is the family, and filtering changes its contents
            {move || {
                groups
                    .get()
                    .into_iter()
                    .map(|group| {
                        view! {
                            <div class="nav-group">
                                <div class="nav-group-title">{group.family.label()}</div>
                                {group
                                    .components
                                    .into_iter()
                                    .map(|component| {
                                        let anchor = docs::anchor_id(&component);
                                        let on_click = anchor.clone();
                                        view! {
                                            <div
                                                class="tree-item"
                                                class:selected=move || {
                                                    selected.get().as_deref() == Some(anchor.as_str())
                                                }
                                                on:click=move |_| {
                                                    selected.set(Some(on_click.clone()));
                                                    scroll_to(&on_click);
                                                }
                                            >
                                                {component.kind.clone()}
                                            </div>
                                        }
                                    })
                                    .collect_view()}
                            </div>
                        }
                    })
                    .collect_view()
            }}
        </div>
    }
}

/// One component's entry in the reference.
#[component]
fn ComponentDoc(
    component: kayak_core::docs::ComponentDoc,
    selected: RwSignal<Option<String>>,
) -> impl IntoView {
    let anchor = docs::anchor_id(&component);
    let is_selected = anchor.clone();
    let description = component.description.clone().unwrap_or_default();
    let has_settings = !component.fields.is_empty() || !component.variants.is_empty();
    let variants = component.variants.clone();

    view! {
        <article
            class="doc-card"
            class:selected=move || selected.get().as_deref() == Some(is_selected.as_str())
            id=anchor
        >
            <header>
                <span class="doc-kind">{component.kind.clone()}</span>
                // the tag is what actually goes in the config, so show it as
                // the json it will be
                <code class="doc-tag">{format!("\"type\": \"{}\"", component.kind)}</code>
            </header>
            <Description text=description />
            <Show when=move || has_settings fallback=|| view! { <p class="empty">"no settings"</p> }>
                <FieldTable fields=component.fields.clone() />
                <For
                    each={
                        let variants = variants.clone();
                        move || variants.clone()
                    }
                    key=|v| v.name.clone()
                    let:variant
                >
                    <div class="doc-variant">
                        <div class="section-kind">{variant.name.clone()}</div>
                        <FieldTable fields=variant.fields.clone() />
                    </div>
                </For>
            </Show>
        </article>
    }
}

/// The settings table: name, type, whether it has to be there, and what it does.
#[component]
fn FieldTable(fields: Vec<kayak_core::docs::FieldDoc>) -> impl IntoView {
    if fields.is_empty() {
        return ().into_any();
    }
    view! {
        <div class="field-table">
            <For each=move || fields.clone() key=|f| f.name.clone() let:field>
                <div class="field">
                    <code class="field-name">{field.name.clone()}</code>
                    <code class="field-type">{field.type_name.clone()}</code>
                    <span
                        class="field-requirement"
                        class:optional=move || !field.required
                    >
                        {docs::requirement_label(field.required)}
                    </span>
                    <div class="field-description">
                        <Description text=field.description.clone().unwrap_or_default() />
                    </div>
                </div>
            </For>
        </div>
    }
    .into_any()
}

/// A doc comment, as paragraphs with `code spans` picked out.
#[component]
fn Description(text: String) -> impl IntoView {
    let paragraphs = docs::rendered_description(&text);
    view! {
        <For each=move || paragraphs.clone().into_iter().enumerate() key=|(i, _)| *i let:paragraph>
            <p class="doc-description">
                <For
                    each=move || paragraph.1.clone().into_iter().enumerate()
                    key=|(i, _)| *i
                    let:segment
                >
                    {match segment.1 {
                        docs::Segment::Text(t) => view! { <span>{t}</span> }.into_any(),
                        docs::Segment::Code(c) => view! { <code>{c}</code> }.into_any(),
                    }}
                </For>
            </p>
        </For>
    }
}

/// Bring a component into view in the reference pane. A no-op anywhere there
/// isn't a document — server-side rendering, where no click can happen anyway.
fn scroll_to(anchor: &str) {
    let Some(element) = leptos::web_sys::window()
        .and_then(|w| w.document())
        .and_then(|d| d.get_element_by_id(anchor))
    else {
        return;
    };
    // the smooth part is css (`scroll-behavior` on the pane); the options
    // object that would set it here isn't in web-sys' default feature set
    element.scroll_into_view();
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
/// The config of a running pipeline never changes, so all three tabs are built
/// once and only the selected one is rendered.
#[component]
fn Inspector(config: Config) -> impl IntoView {
    let inputs = inspector::input_sections(&config);
    let outputs = inspector::output_sections(&config);
    let transforms = inspector::transform_sections(&config);

    // the count belongs on the tab: any of the three stages can now hold more
    // than one component, and how many is worth seeing without clicking
    let tabs = [
        (Tab::Inputs, inspector::tab_label("inputs", inputs.len())),
        (
            Tab::Transforms,
            inspector::tab_label("transforms", transforms.len()),
        ),
        (Tab::Outputs, inspector::tab_label("outputs", outputs.len())),
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
                    // no ordinals on inputs and outputs: they are a set, not a
                    // chain — every input is merged and every output gets every
                    // batch, so numbering them would imply an order that isn't
                    // there. a transform's position *is* behaviour, so it keeps
                    // its number.
                    Tab::Inputs => sections(&inputs, "no inputs", false),
                    Tab::Outputs => sections(&outputs, "no outputs", false),
                    Tab::Transforms => sections(&transforms, "no transforms", true),
                }}
            </div>
        </div>
    }
}

/// One tab's worth of sections, or a placeholder if the stage is empty. Empty
/// is a real state for all three now: a pipeline can have no transforms and no
/// outputs, and a config that somehow arrives with no inputs should say so
/// rather than render a blank pane.
fn sections(sections: &[inspector::Section], empty: &'static str, numbered: bool) -> AnyView {
    if sections.is_empty() {
        return view! { <div class="empty">{empty}</div> }.into_any();
    }
    sections
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, section)| {
            if numbered {
                view! { <SectionView section ordinal=i + 1 /> }.into_any()
            } else {
                view! { <SectionView section /> }.into_any()
            }
        })
        .collect_view()
        .into_any()
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

/// One row: when, which stage, and what.
///
/// Each `<span>` is written on a single line on purpose. A `{...}` on a line of
/// its own inside `view!` leaves a whitespace text node beside the value, and a
/// span holding one wraps to a second line — which turned every row of this log
/// double height, with the stage badge stranded under the timestamp.
#[component]
fn LogRow(entry: log::Entry, names: StoredValue<log::ComponentNames>) -> impl IntoView {
    let state = expect_context::<AppState>();
    let ts = entry.ts;
    let text = names.with_value(|names| {
        log::summary(&entry, names.name(entry.stage, entry.component))
    });

    view! {
        // Every class here is prefixed, and that is not just tidiness: the
        // "add pipeline" modal has a `.stage` of its own — a group of
        // components, nothing to do with this — and a badge that borrowed the
        // name silently picked up its 12px top margin.
        <div class="log-row" class:error=entry.is_error()>
            <span class="log-time">{move || log::format_time(ts, state.tz_offset.get())}</span>
            <span class="log-stage" title=entry.stage.as_str()>
                {log::stage_label(entry.stage)}
            </span>
            // the full payload on hover: a card is 18 cells wide and a batch is
            // not
            <span class="log-text" title=text.clone()>{text.clone()}</span>
        </div>
    }
}

/// One pass, collapsed to a summary until it is opened.
///
/// The header is the row you read: a batch arrived, this much left, and whether
/// anything failed on the way. What it took to get there is a click away —
/// which is the whole argument for grouping, since the alternative is reading
/// the same journey as three or four unrelated lines.
#[component]
fn PassView(
    pass: log::Pass,
    names: StoredValue<log::ComponentNames>,
    expanded: RwSignal<HashSet<u64>>,
) -> impl IntoView {
    let state = expect_context::<AppState>();
    let key = pass.key();
    let (ts, seq, gap) = (pass.ts, pass.seq, pass.gap_before);
    let (summary, errors) = (pass.summary(), pass.errors);
    let entries = StoredValue::new(pass.entries);
    let is_open = move || expanded.with(|open| open.contains(&key));

    view! {
        // A gap is not an entry, so it is not inside the pass — it is the
        // statement that some passes are missing from between them.
        <Show when=move || { gap > 0 }>
            <div class="log-gap" title="the event feed drops rather than blocks when a browser falls behind">
                {format!("⋯ {gap} passes not shown")}
            </div>
        </Show>
        <div
            class="log-pass"
            class:error=move || { errors > 0 }
            class:open=is_open
            title=seq.map_or_else(
                || "an event that belongs to no pass".to_string(),
                |seq| format!("pass {seq}"),
            )
            on:click=move |_| {
                expanded
                    .update(|open| {
                        if !open.remove(&key) {
                            open.insert(key);
                        }
                    })
            }
        >
            <span class="log-time">{move || log::format_time(ts, state.tz_offset.get())}</span>
            <span class="log-caret">{move || if is_open() { "▾" } else { "▸" }}</span>
            <span class="log-text">{summary}</span>
            <Show when=move || { errors > 0 }>
                <span class="log-errors">{format!("⚠ {errors}")}</span>
            </Show>
        </div>
        // Wrapped rather than left as siblings of the header: the rows have to
        // be addressable as "inside this pass" for the indent, and a sibling
        // selector would reach every row below, including the next pass's.
        <Show when=is_open>
            <div class="log-pass-entries">
                {move || {
                    entries
                        .get_value()
                        .into_iter()
                        .map(|entry| view! { <LogRow entry names /> })
                        .collect_view()
                }}
            </div>
        </Show>
    }
}

/// A card's log: a bar of controls over a scrolling tail of events.
///
/// The bar is part of the log rather than of the card because everything on it
/// acts on the log — which is also why the filter lives here and not in the
/// inspector's tab strip above. The two answer different questions: the tabs
/// navigate a config that never changes, this filters a stream that does.
#[component]
fn MessageLog(
    messages: RwSignal<log::Log>,
    filter: RwSignal<log::Filter>,
    names: StoredValue<log::ComponentNames>,
) -> impl IntoView {
    let state = expect_context::<AppState>();
    let body_ref = NodeRef::<leptos::html::Div>::new();

    // Flat by default: every event in the order it arrived, which is what a log
    // is expected to be. Grouping is the view you switch to when the question
    // is what one batch did rather than what just happened.
    let grouped = RwSignal::new(false);
    let expanded = RwSignal::new(HashSet::<u64>::new());

    // Whether the tail is pinned to the bottom. The log used to scroll down on
    // every event, which on a busy pipeline means no line can be read at all:
    // follow only while the reader is already at the end, and offer them the
    // way back when they aren't.
    let following = RwSignal::new(true);
    let at_bottom = move |el: &leptos::web_sys::HtmlDivElement| {
        el.scroll_height() - el.scroll_top() - el.client_height() <= FOLLOW_SLACK_PX
    };
    let scroll_to_end = move || {
        if let Some(el) = body_ref.get_untracked() {
            el.set_scroll_top(el.scroll_height());
        }
    };

    Effect::new(move |_| {
        messages.track();
        filter.track();
        if following.get_untracked() {
            scroll_to_end();
        }
    });

    // Both are memos and only one is ever read: a memo doesn't compute until
    // something reads it, so the view that isn't showing costs nothing.
    let visible_passes = Memo::new(move |_| {
        let filter = filter.get();
        messages.with(|log| log::visible_passes(log.passes(), filter))
    });
    let visible_entries = Memo::new(move |_| {
        let filter = filter.get();
        messages.with(|log| {
            log.entries()
                .iter()
                .filter(|entry| filter.matches(entry))
                .cloned()
                .collect::<Vec<_>>()
        })
    });
    let nothing_to_show = move || {
        if grouped.get() {
            visible_passes.with(Vec::is_empty)
        } else {
            visible_entries.with(Vec::is_empty)
        }
    };

    // Zero reads as "nothing is flowing", which is worth distinguishing from a
    // pipeline that has never produced anything — so the readout only appears
    // once there is a rate to report.
    let rate = Memo::new(move |_| {
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let now = state.now.get().max(0.0) as u64;
        messages.with(|log| log.per_second(now))
    });

    let chip = move |label: &'static str,
                     title: &'static str,
                     read: fn(&log::Filter) -> bool,
                     write: fn(&mut log::Filter)| {
        view! {
            <button
                class="chip"
                class:active=move || filter.with(read)
                title=title
                on:click=move |_| filter.update(write)
            >
                {label}
            </button>
        }
    };

    view! {
        <div class="log">
            <div class="log-bar">
                {chip("in", "batches as they arrived", |f| f.input, |f| f.input = !f.input)}
                {chip(
                    "out",
                    "batches as they left the transforms",
                    |f| f.output,
                    |f| f.output = !f.output,
                )}
                {chip(
                    "err",
                    "failures, at any stage",
                    |f| f.errors,
                    |f| f.errors = !f.errors,
                )}
                <span class="rate">
                    {move || {
                        let rate = rate.get();
                        (rate > 0.0).then(|| format!("{rate:.0}/s"))
                    }}
                </span>
                <button
                    class="clear"
                    title="group each batch's journey into one row, or list every event"
                    on:click=move |_| grouped.update(|g| *g = !*g)
                >
                    {move || if grouped.get() { "grouped" } else { "flat" }}
                </button>
                <button
                    class="clear"
                    title="clear this log"
                    on:click=move |_| {
                        messages.update(log::Log::clear);
                        following.set(true);
                    }
                >
                    "clear"
                </button>
            </div>
            <div
                class="log-body"
                node_ref=body_ref
                on:scroll=move |_| {
                    if let Some(el) = body_ref.get_untracked() {
                        following.set(at_bottom(&el));
                    }
                }
            >
                <Show when=nothing_to_show>
                    <div class="empty">
                        {move || {
                            if filter.get().is_empty() {
                                "everything is filtered out"
                            } else if messages.with(log::Log::is_empty) {
                                "waiting for messages…"
                            } else {
                                "nothing matches this filter"
                            }
                        }}
                    </div>
                </Show>
                <Show when=move || grouped.get()>
                    <For
                        each=move || visible_passes.get()
                        // by content, not identity: a pass grows after it is
                        // first drawn, and a stable key would freeze the row
                        key=log::Pass::render_key
                        let:pass
                    >
                        <PassView pass names expanded />
                    </For>
                </Show>
                <Show when=move || !grouped.get()>
                    <For each=move || visible_entries.get() key=|entry| entry.id let:entry>
                        <LogRow entry names />
                    </For>
                </Show>
            </div>
            // Only while the reader has scrolled away, and inside the log rather
            // than over the card: it is the log's own "you are not seeing the
            // newest line" and shouldn't cover the config above it.
            <Show when=move || !following.get()>
                <button
                    class="log-jump"
                    on:click=move |_| {
                        following.set(true);
                        scroll_to_end();
                    }
                >
                    "jump to latest"
                </button>
            </Show>
        </div>
    }
}

#[component]
pub fn Card(
    pipeline_id: PipelineId,
    config: Config,
    events: Signal<Option<UiEvent>>,
) -> impl IntoView {
    let state = expect_context::<AppState>();
    let canvas = state.canvas_state;
    let messages = RwSignal::new(log::Log::default());
    let filter = RwSignal::new(log::Filter::default());
    // What the component index on a failure means. Read once: the config of a
    // running pipeline never changes, and an edit rebuilds the card.
    let names = StoredValue::new(log::ComponentNames {
        transforms: inspector::transform_sections(&config)
            .into_iter()
            .map(|section| section.kind)
            .collect(),
        outputs: inspector::output_sections(&config)
            .into_iter()
            .map(|section| section.kind)
            .collect(),
    });
    let id = pipeline_id.clone();

    let maximized_id = pipeline_id.clone();
    let is_maximized = Memo::new(move |_| {
        canvas
            .maximized
            .with(|m| m.as_ref() == Some(&maximized_id))
    });

    Effect::new(move |_| {
        if let Some(ev) = events.get()
            && ev.pipeline_id == id
        {
            messages.update(|log| log.push(&ev));
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
    let measure_id = pipeline_id.clone();
    Effect::new(move |_| {
        let height = measured_height.get();
        if height <= 0.0 {
            return;
        }
        // A maximized card is as tall as the window, which is not a fact about
        // its content: reporting it would push every row below it apart and
        // then pull them back on restore. The last real measurement stands,
        // which is also the one the card goes back to.
        if is_maximized.get_untracked() {
            return;
        }
        let changed = canvas.measured.with_untracked(|m| {
            m.get(&measure_id)
                .is_none_or(|old| (old - height).abs() > 1.0)
        });
        if changed {
            canvas.measured.update(|m| {
                m.insert(measure_id.clone(), height);
            });
        }
    });

    let position_id = pipeline_id.clone();
    let position = Memo::new(move |_| {
        // A maximized card is placed against the viewport rather than by the
        // layout, and reads the camera so that panning and zooming leave it
        // where it is on screen. Its laid-out position is untouched underneath,
        // so restoring it is only a matter of dropping this branch — and the
        // edges, which are still routed against that position, come back to a
        // card that never moved.
        if is_maximized.get() {
            return crate::graph::maximized_geom(canvas.camera.get(), canvas.viewport.get());
        }
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

    // Whether this card's height was chosen rather than measured. It decides
    // between `height` and `min-height` below, which is the difference between
    // a card that scrolls its content and one that grows to fit it.
    let pinned_id = pipeline_id.clone();
    let pinned_height = Memo::new(move |_| {
        canvas
            .arrangement
            .with(|a| a.get(&pinned_id).and_then(|n| n.height))
    });

    // Both gestures start the same way, and both have to keep the press away
    // from the canvas — a card being dragged must not also pan the view behind
    // it. The canvas' own handler ignores presses that land on a card, so this
    // only has to stop the text selection that a drag would otherwise start.
    // The id is stored rather than captured so that this closure stays `Copy`:
    // the resize handle lives inside a `<Show>`, whose children are rebuilt
    // whenever the mode changes and so can't consume what they capture.
    let stored_id = StoredValue::new(pipeline_id.clone());
    let grab = move |ev: leptos::ev::MouseEvent, grab: Grab| {
        if ev.button() != 0 {
            return;
        }
        ev.prevent_default();
        ev.stop_propagation();
        canvas.interrupt_focus();
        canvas.dragging.set(Some(Dragging {
            id: stored_id.get_value(),
            grab,
            origin: (f64::from(ev.client_x()), f64::from(ev.client_y())),
            start: position.get_untracked(),
            pinned_height: pinned_height.get_untracked(),
        }));
    };

    let dragging_id = pipeline_id.clone();
    let is_dragging = Memo::new(move |_| {
        canvas
            .dragging
            .with(|d| d.as_ref().is_some_and(|d| d.id == dragging_id))
    });

    let reset_id = pipeline_id.clone();
    let toggle_id = StoredValue::new(pipeline_id.clone());

    // A maximized card is sized by the window rather than by its content, which
    // is the same arrangement a resized one is in: the box is given, and the log
    // takes what is left over and scrolls.
    let fixed_height = move || is_maximized.get() || pinned_height.get().is_some();

    view! {
        <div
            class="card"
            class:dragging=move || is_dragging.get()
            class:maximized=move || is_maximized.get()
            class:pinned=fixed_height
            node_ref=card_ref
            style:left=move || format!("{}px", position.get().x)
            style:top=move || format!("{}px", position.get().y)
            style:width=move || format!("{}px", position.get().width)
            // A measured card is only ever pushed *up* to the next grid line, so
            // it keeps growing with its content; a resized one is pinned to the
            // height that was asked for and its log scrolls inside it.
            style:height=move || {
                fixed_height().then(|| format!("{}px", position.get().height))
            }
            style:min-height=move || {
                if fixed_height() { None } else { Some(format!("{}px", position.get().height)) }
            }
        >
            <header
                // A maximized card is already where it is going to be: dragging
                // it would only write a position no one can see being chosen.
                class:draggable=move || state.editing() && !is_maximized.get()
                on:mousedown=move |ev| {
                    if state.editing() && !is_maximized.get() {
                        grab(ev, Grab::Move);
                    }
                }
                // double-click puts the card back under the automatic layout,
                // which is the only way back once it has been moved
                on:dblclick=move |_| {
                    if state.editing() && !is_maximized.get() {
                        canvas.unpin(&reset_id);
                    }
                }
                title=move || {
                    if state.editing() && !is_maximized.get() {
                        "drag to move, double-click to lay out automatically"
                    } else {
                        ""
                    }
                }
            >
                <span class="card-title">{pipeline_id.clone()}</span>
                // Not behind the edit-mode `<Show>`: filling the screen with a
                // card is a way of reading it, and read-only wants it most.
                <button
                    class="card-maximize"
                    // the press must not reach the drag handle underneath it
                    on:mousedown=move |ev| ev.stop_propagation()
                    on:dblclick=move |ev| ev.stop_propagation()
                    on:click=move |_| canvas.toggle_maximized(&toggle_id.get_value())
                    title=move || {
                        if is_maximized.get() { "restore" } else { "maximize" }
                    }
                    aria-label=move || {
                        if is_maximized.get() { "restore" } else { "maximize" }
                    }
                >
                    {move || if is_maximized.get() { "⤡" } else { "⤢" }}
                </button>
            </header>
            <Inspector config=config />
            <MessageLog messages filter names />
            <Show when=move || state.editing() && !is_maximized.get()>
                <div
                    class="resize-handle"
                    title="drag to resize"
                    on:mousedown=move |ev| grab(ev, Grab::Resize)
                />
            </Show>
        </div>
    }
}
