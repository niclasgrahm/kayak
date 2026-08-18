use kayak_core::api_docs::{ApiDoc, endpoints};
use kayak_core::docs::{Family, FieldType, all_components};
use kayak_core::history::ErrorSignature;
use kayak_core::schema::MessageSchema;
use kayak_core::script::{DryRunRequest, DryRunResponse};
use kayak_core::state::{BucketContents, BucketSummary};
use kayak_core::{
    AuthDto, ConfigFormat, Connections, EdgeEnd, LayoutFile, PipelineDto, PipelineId, PortLayout,
    RunStatus, Side, UiEvent, config::Config,
};
use leptos::prelude::*;
use leptos_meta::*;
use leptos_router::components::{A, Route, Router, Routes};
use leptos_router::path;
use leptos_use::{
    UseClipboardReturn, UseElementSizeOptions, UseElementSizeReturn, UseEventListenerOptions,
    UseEventSourceReturn, UseIntervalFnOptions, UseRafFnCallbackArgs, UseRafFnOptions,
    use_clipboard, use_element_size, use_element_size_with_options, use_event_listener,
    use_event_listener_with_options, use_event_source, use_interval_fn_with_options,
    use_raf_fn_with_options, use_window,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::api_client::{ApiClient, ApiError};
use crate::api_docs;
use crate::docs;
use crate::form;
use crate::graph::{
    Camera, CardGeom, Channel, DragCard, Edge, FALLBACK_CARD_HEIGHT, GRID, PULSE_TICK_MS,
    PULSE_TICKS, PortHandle, approach, bounds, dragged_all, dragged_channel, dragged_port,
    edge_paths, focus_camera, focus_zoom, layout, pipelines_from, pulsed_edges, resized,
    tick_pulses, wheel_delta_pixels, zoom_at,
};
use crate::inspector;
use crate::log;
use crate::pretty;
use crate::project;
use crate::selection::{self, Selection};
use crate::sidebar;
use crate::sidebar::{Row, SidebarMode};
use crate::stats;

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
    State,
}

/// One card's end of the feed, registered with [`Feed`] while it is mounted.
///
/// The `token` is the same trick the server's http inbox registry uses, and for
/// the same reason: the card list is rebuilt wholesale, so a card for an id can
/// be created *before* the old one for that id is cleaned up. An unconditional
/// remove on cleanup would then unregister its own successor and leave a card
/// that never updates again.
#[derive(Clone, Copy)]
pub struct Sink {
    token: u64,
    card: CardSink,
}

/// What a card offers the feed: where the events go, and whether it is looking.
///
/// A collapsed section is not fed. That is the point of the collapse — the card
/// is a window on a stream, and a window nobody is at should cost what a closed
/// one costs. What each section does when it is shut differs, though, and the
/// difference is what it would otherwise get *wrong*:
///
/// - The **log** still takes everything except the row (`Log::skip`, the same
///   call a paused log makes) and takes it *untracked*, so nothing renders. It
///   is the rate window that this is for: a log opened onto a busy pipeline
///   should read what the pipeline is doing rather than climb from zero over
///   ten seconds.
/// - The **chart** takes nothing at all, because a bar is a fact about a moment
///   and there is no honest way to draw the ones that were not watched. It
///   starts empty when it is opened, and fills from there.
#[derive(Clone, Copy)]
pub struct CardSink {
    pub log: RwSignal<log::Log>,
    /// Whether the log section is open. See [`CardSink`].
    pub log_open: RwSignal<bool>,
    pub paused: RwSignal<bool>,
    pub skipped: RwSignal<usize>,
    pub stats: RwSignal<stats::Stats>,
    /// Whether the chart section is open. See [`CardSink`].
    pub stats_open: RwSignal<bool>,
}

/// Where the `/events` feed is delivered.
///
/// Two things it is built to avoid, both measured on a nine-card canvas at nine
/// thousand events a second, where the main thread was blocked 63% of the time
/// in freezes approaching a second:
///
/// - **An effect per card.** Every card used to wake on every event to compare
///   one string, so the cost of an event scaled with the size of the graph.
///   Here one dispatcher looks the id up in a map.
/// - **A render per event.** A card's log is a two-hundred row keyed list, and
///   appending one row re-runs the whole reconciliation. Events are buffered and
///   delivered **once per animation frame** instead, so a card renders at most
///   sixty times a second however hard the feed is pushing — and all the events
///   of one frame land in a single signal write.
///
/// The server samples the feed as well ([`kayak::pipeline::UiThrottle`]), so in
/// practice this rarely has more than a handful of events to deliver. It is the
/// backstop for the case the server can't bound: a great many pipelines, each
/// well within its own budget.
#[derive(Clone, Copy)]
pub struct Feed {
    /// The cards currently mounted, by the pipeline they show.
    sinks: StoredValue<HashMap<PipelineId, Sink>>,
    /// What has arrived since the last frame.
    pending: StoredValue<Vec<UiEvent>>,
    next_token: StoredValue<u64>,
    /// One frame's events, published after they have been delivered to the
    /// cards. `Edges` drives its blink off this rather than off the raw feed,
    /// so the edge layer also re-renders per frame rather than per event.
    frame: RwSignal<Arc<Vec<UiEvent>>>,
}

impl Feed {
    fn new() -> Self {
        Self {
            sinks: StoredValue::new(HashMap::new()),
            pending: StoredValue::new(Vec::new()),
            next_token: StoredValue::new(0),
            frame: RwSignal::new(Arc::new(Vec::new())),
        }
    }

    /// Register a card's log and chart for the life of the component.
    fn register(self, id: PipelineId, card: CardSink) {
        let token = self.next_token.try_update_value(|n| {
            *n += 1;
            *n
        });
        let Some(token) = token else {
            return;
        };
        self.sinks.update_value(|sinks| {
            sinks.insert(id.clone(), Sink { token, card });
        });
        on_cleanup(move || {
            self.sinks.update_value(|sinks| {
                // ours only — see `Sink::token`
                if sinks.get(&id).is_some_and(|s| s.token == token) {
                    sinks.remove(&id);
                }
            });
        });
    }

    /// Hold an event until the next frame.
    fn queue(self, event: UiEvent) {
        self.pending.update_value(|pending| pending.push(event));
    }

    /// Deliver everything queued. Returns false when there was nothing to do,
    /// which is what lets the frame loop pause itself.
    fn deliver(self) -> bool {
        let events = self
            .pending
            .try_update_value(std::mem::take)
            .unwrap_or_default();
        if events.is_empty() {
            return false;
        }

        // Grouped first so that a card is written to once for the whole frame:
        // the point of the exercise is one render, not one render per event.
        let mut by_pipeline: HashMap<&PipelineId, Vec<&UiEvent>> = HashMap::new();
        for event in &events {
            by_pipeline
                .entry(&event.pipeline_id)
                .or_default()
                .push(event);
        }

        for (id, batch) in by_pipeline {
            let Some(sink) = self.sinks.with_value(|sinks| sinks.get(id).copied()) else {
                continue;
            };
            let sink = sink.card;

            // The chart counts before the log does and independently of it: the
            // two sections are collapsed separately, and a card showing only its
            // throughput is a reasonable way to watch a pipeline.
            if sink.stats_open.get_untracked() {
                sink.stats.update(|stats| {
                    for event in &batch {
                        stats.record(event);
                    }
                });
            }

            if !sink.log_open.get_untracked() {
                // Collapsed: the counters, none of the rows, and no
                // notification — nothing this log shows is on screen. See
                // [`CardSink`].
                sink.log.update_untracked(|log| {
                    for event in batch {
                        log.skip(event);
                    }
                });
            } else if sink.paused.get_untracked() {
                let failures = batch.iter().filter(|e| e.is_error()).count();
                sink.skipped.update(|n| *n = n.saturating_add(batch.len()));
                // A paused log keeps no rows, so nothing it shows changes —
                // *unless* a failure arrived, which the error badge has to
                // report. Notifying regardless is what made pausing a busy card
                // cost as much as leaving it running: `update` marks the signal
                // dirty whether or not the value moved, so the whole two-hundred
                // row list was reconciled again for a log that had not gained a
                // line. The rate readout stays live either way — it is a memo
                // over the once-a-second clock as well as over this.
                if failures > 0 {
                    sink.log.update(|log| {
                        for event in batch {
                            log.skip(event);
                        }
                    });
                } else {
                    sink.log.update_untracked(|log| {
                        for event in batch {
                            log.skip(event);
                        }
                    });
                }
            } else {
                sink.log.update(|log| {
                    for event in batch {
                        log.push(event);
                    }
                });
            }
        }

        self.frame.set(Arc::new(events));
        true
    }
}

#[derive(Clone, Copy)]
pub struct AppState {
    pub pipelines: LocalResource<Result<Vec<PipelineDto>, ApiError>>,
    /// The named connections pipelines refer to. Re-fetched on the same trigger
    /// as the pipelines: adding one changes what the next form can offer.
    pub connections: LocalResource<Result<Connections, ApiError>>,
    /// Bumped to re-read the state buckets, once a second while the tab that
    /// shows them is open.
    ///
    /// A bare tick rather than a `LocalResource` like the two above, and that
    /// is **not a style choice**: everything on this page — the navbar, the
    /// sidebar and the whole canvas — is inside one `<Suspense>`, and a
    /// resource read anywhere under it re-suspends the *boundary* every time it
    /// refetches. A resource polled once a second therefore tears the canvas
    /// down and rebuilds it once a second. The state tab reads its data through
    /// an effect and a plain signal instead, so a poll touches nothing but the
    /// rows it draws. Anything else that wants to poll needs the same treatment
    /// or its own suspense boundary.
    pub state_reload: RwSignal<u32>,
    pub events: Signal<Option<UiEvent>>,
    /// Where those events are delivered — see [`Feed`]. Cards register with it
    /// rather than each watching `events` themselves.
    pub feed: Feed,
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
    /// What kind of server this tab is looking at — blank, or already a
    /// project — once the resources have answered. The creator dialog and the
    /// empty canvas's hint both read it; see [`crate::project`].
    pub instance: Signal<project::Instance>,
    /// The session's "not now" to the creator dialog. Lives here rather than in
    /// the dialog because the dialog is mounted under a `<Show>`: its own state
    /// would be forgotten the moment declining unmounted it, and the dialog
    /// would come straight back.
    pub creator_dismissed: RwSignal<bool>,
    pub mode: RwSignal<Mode>,
    /// Whether this caller may change the graph at all — an admin, or anyone
    /// on a server with no accounts configured.
    ///
    /// Held beside `mode` rather than folded into it because they answer
    /// different questions: `mode` is "am I editing right now", this is "may I".
    /// [`AppState::editing`] requires both, so a reader cannot end up in edit
    /// mode however the mode signal got set — the server would refuse the calls
    /// anyway, and offering buttons that 403 is worse than not offering them.
    ///
    /// **A plain `bool`, and it has to be one.** The obvious spelling is a
    /// `Signal::derive` over the session, and that spelling crashes the app on
    /// sign-out: the derived signal would be owned by `CanvasPage` while
    /// subscribing to `session.auth`, which is the signal [`AuthGate`] disposes
    /// `CanvasPage` *from*. Signing out then notifies the readers of a value
    /// whose owner is being torn down in the same pass, and reading a disposed
    /// signal panics.
    ///
    /// Nothing is lost, because there is nothing to be reactive about: an
    /// identity can only change by going through `AuthGate`, which remounts
    /// this whole tree and recomputes it. The role is fixed for the life of the
    /// mount, so capturing it at construction is the honest shape as well as
    /// the safe one.
    pub may_edit: bool,
    /// Whether the "add pipeline" modal is open.
    pub adding: RwSignal<bool>,
    /// The pipeline the modal should open already fed by, when it was opened
    /// from a card's downstream handle rather than from the sidebar's `+`.
    ///
    /// A separate signal rather than a payload on `adding` because the modal
    /// reads it once, at construction: it is mounted under a `<Show>`, so every
    /// open is a fresh component and a seed can only ever be the current one.
    /// Always written through [`AppState::open_add`], which is what stops a
    /// stale id surviving into the next plain `+`.
    pub add_upstream: RwSignal<Option<PipelineId>>,
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

    /// Open the "add pipeline" modal, optionally with its first input already
    /// reading from `upstream`.
    ///
    /// The one way in, so that the seed is set — or cleared — on every open.
    pub fn open_add(&self, upstream: Option<PipelineId>) {
        self.add_upstream.set(upstream);
        self.adding.set(true);
    }

    fn editing(&self) -> bool {
        self.may_edit && self.mode.get().is_edit()
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
    pub grab: Grab,
    /// Pointer position when the press landed, in client (screen) pixels.
    pub origin: (f64, f64),
    /// Every card the gesture moves, with the geometry it had when the press
    /// landed — the gesture is applied to *that* rather than to the current
    /// geometry, so a drag can't accumulate rounding: every mousemove computes
    /// the same answer from the same start.
    ///
    /// More than one only for [`Grab::Move`], and only when the card grabbed was
    /// part of a selection: a resize is one card's height and width, and there
    /// is no sensible reading of resizing six cards from one corner.
    pub cards: Vec<DragCard>,
}

impl Dragging {
    fn moves(&self, id: &str) -> bool {
        self.cards.iter().any(|card| card.id == id)
    }
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
    /// The pipelines the user is currently working with — set by clicks on
    /// cards and on sidebar rows, and read by both so that the two lists agree
    /// about which ones are meant. Like `maximized` it is a way of *looking* at
    /// the graph: it never reaches the layout file and doesn't survive a reload.
    ///
    /// More than one only in edit mode, where a drag moves the whole set.
    pub selected: RwSignal<Selection>,
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
            selected: RwSignal::new(Selection::default()),
            focus_request: RwSignal::new(None),
            focus_target: RwSignal::new(None),
        }
    }

    fn geom_of(&self, id: &PipelineId) -> Option<CardGeom> {
        self.placements.with(|p| p.get(id).copied())
    }

    /// Make a pipeline the whole selection, dropping whatever else was in it.
    ///
    /// Guarded rather than a plain `set` because it is called from a mousedown
    /// on a card: `update`-style writes mark a signal dirty whether or not the
    /// value moved, so an unguarded write would re-run every card's selection
    /// memo on every click, including the ones that change nothing.
    fn select_only(&self, id: &PipelineId) {
        if !self.selected.with_untracked(|s| s.is_only(id.as_str())) {
            self.selected.set(Selection::only(id));
        }
    }

    /// Add a pipeline to the selection, or take it out again — shift-clicking a
    /// card or a sidebar row.
    fn toggle_selected(&self, id: &PipelineId) {
        self.selected.update(|s| s.toggle(id));
    }

    /// Add a whole set to the selection, keeping what was already there.
    fn select_also(&self, more: &Selection) {
        if self.selected.with_untracked(|s| s.covers(more)) {
            return;
        }
        self.selected.update(|s| s.add_all(more.ids().cloned()));
    }

    /// Whether a pipeline is selected, read untracked — the click handlers'
    /// question, not the cards'.
    fn is_selected(&self, id: &PipelineId) -> bool {
        self.selected.with_untracked(|s| s.contains(id.as_str()))
    }

    /// Drop the selection entirely. Clicking the canvas away from any card.
    fn clear_selection(&self) {
        if !self.selected.with_untracked(Selection::is_empty) {
            self.selected.set(Selection::default());
        }
    }

    /// The cards a press on `id` moves: the whole selection when the card
    /// grabbed is part of one, and otherwise just that card.
    ///
    /// A card with no placement yet — one that arrived since the last layout
    /// pass — is left out rather than dragged from a guessed position.
    fn moving_cards(&self, id: &PipelineId, start: CardGeom, pinned: Option<f64>) -> Vec<DragCard> {
        let grabbed = DragCard {
            id: id.clone(),
            start,
            pinned_height: pinned,
        };
        if !self.is_selected(id) {
            return vec![grabbed];
        }
        let others = self.selected.with_untracked(|s| {
            s.ids()
                .filter(|other| *other != id)
                .cloned()
                .collect::<Vec<_>>()
        });
        std::iter::once(grabbed)
            .chain(others.into_iter().filter_map(|other| {
                let start = self.geom_of(&other)?;
                let pinned_height = self.arrangement.with_untracked(|a| a.get(&other)?.height);
                Some(DragCard {
                    id: other,
                    start,
                    pinned_height,
                })
            }))
            .collect()
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
    ///
    /// A move can carry several cards and a resize is always one, which is why
    /// only the move arm reads the whole list.
    fn drag_to(&self, drag: &Dragging, client: (f64, f64)) {
        let zoom = self.camera.get_untracked().zoom;
        let dx = (client.0 - drag.origin.0) / zoom;
        let dy = (client.1 - drag.origin.1) / zoom;
        let moved = match drag.grab {
            Grab::Move => dragged_all(&drag.cards, dx, dy),
            Grab::Resize => drag
                .cards
                .first()
                .map(|card| (card.id.clone(), resized(card.start, dx, dy)))
                .into_iter()
                .collect(),
        };
        self.arrangement.update(|a| {
            for (id, pipeline) in moved {
                a.pipelines.insert(id, pipeline);
            }
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
        self.set_channel(&drag.edge, Some(dragged_channel(drag.start_offset, delta)));
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
        <AuthGate>
            <Router>
                <Routes fallback=|| view! { <p class="empty">"no such page"</p> }>
                    <Route path=path!("") view=CanvasPage />
                    <Route path=path!("/docs") view=DocsPage />
                </Routes>
            </Router>
        </AuthGate>
    }
}

/// Who this tab is signed in as, shared by everything that cares.
///
/// A plain signal filled by an effect rather than a `LocalResource`, and that
/// is the same decision the state tab documents: a resource read inside the
/// canvas's `<Suspense>` re-suspends the whole boundary every time it refetches,
/// and this one is re-read after every sign-in and every 401. Nothing here is
/// allowed to tear the canvas down.
#[derive(Clone, Copy)]
pub struct Session {
    /// `None` until the first answer arrives — which is a third state and has
    /// to be, since "we have not asked yet" must not draw a login page that is
    /// about to be replaced by the canvas.
    pub auth: RwSignal<Option<AuthDto>>,
}

impl Session {
    /// The first ask, and the embedding handshake: if the URL carries an
    /// `auth_token` — the host application's JWT, put there because an iframe
    /// can set nothing else — exchange it for the session cookie before
    /// anything decides between the login page and the canvas. The token is
    /// stripped from the address bar immediately, exchanged or not: it has
    /// been read, and a credential has no business outliving its one use in
    /// a place someone can copy a link from.
    ///
    /// A failed exchange falls back to [`Session::refresh`], which is what
    /// puts up the login page — the embedded case's honest answer too, since
    /// a service account can still type itself in.
    pub fn bootstrap(self) {
        let Some(token) = token_from_location() else {
            self.refresh();
            return;
        };
        strip_token_from_url();
        leptos::task::spawn_local(async move {
            let client = ApiClient {
                base: String::new(),
            };
            match client.token_login(&token).await {
                Ok(auth) => self.auth.set(Some(auth)),
                Err(_) => self.refresh(),
            }
        });
    }

    /// Ask the server again. Called on load, after signing in or out, and
    /// whenever a call comes back 401.
    pub fn refresh(self) {
        leptos::task::spawn_local(async move {
            let answer = ApiClient {
                base: String::new(),
            }
            .whoami()
            .await;
            // A network failure leaves the last answer standing rather than
            // logging the tab out: a dropped connection is not a sign-out, and
            // treating it as one would throw someone back to the login form
            // every time their laptop woke up.
            if let Ok(auth) = answer {
                self.auth.set(Some(auth));
            }
        });
    }

    /// What a call coming back 401 means: whatever this tab thought, it is
    /// signed out now. Setting it directly rather than re-asking makes the
    /// login form appear on the same frame as the failure.
    pub fn signed_out(self) {
        self.auth.update(|auth| {
            if let Some(auth) = auth {
                auth.username = None;
                auth.role = None;
            }
        });
    }

    /// Whether the caller may change the graph, with the not-yet-known state
    /// folded into the safe answer: nothing is editable until we know.
    ///
    /// **Read untracked, deliberately.** Every caller is inside the tree
    /// [`AuthGate`] mounts, and subscribing to `auth` from in there means being
    /// notified by the very change that disposes you — see
    /// [`AppState::may_edit`] for the crash that causes. An identity change
    /// remounts the tree, so a snapshot is not stale.
    pub fn may_edit(self) -> bool {
        self.auth
            .get_untracked()
            .is_some_and(|auth| auth.may_edit())
    }

    pub fn username(self) -> Option<String> {
        self.auth.get().and_then(|auth| auth.username)
    }

    pub fn role(self) -> Option<String> {
        self.auth
            .get()
            .and_then(|auth| auth.role)
            .map(|role| role.label().to_string())
    }
}

/// Nothing renders until we know whether this server wants a login, and the
/// login form renders instead of the app while it does.
///
/// The gate is above the router rather than inside each page, because "is this
/// tab signed in" is one question for the whole tab. It means `/docs` is behind
/// the login too even though the endpoints that feed it are public — the
/// reference stays fetchable by tooling, which is what that decision was for,
/// while the *app* is one app rather than a canvas you can't see beside a
/// reference you can.
#[component]
fn AuthGate(children: ChildrenFn) -> impl IntoView {
    let session = Session {
        auth: RwSignal::new(None),
    };
    provide_context(session);
    // effects only run in the browser, so the URL is there to read
    Effect::new(move |_| session.bootstrap());

    view! {
        {move || match session.auth.get() {
            // the first paint, before the server has answered: no login form,
            // no canvas, nothing that will be replaced a moment later
            None => view! { <p class="empty">"..."</p> }.into_any(),
            Some(auth) if auth.needs_login() => view! { <LoginPage /> }.into_any(),
            Some(_) => children().into_any(),
        }}
    }
}

/// The embedded token out of this tab's URL, if the host application put one
/// there. The parsing is `embed::token_in_query`, which is where the tests
/// are; this is only the window plumbing.
fn token_from_location() -> Option<String> {
    let search = leptos::web_sys::window()?.location().search().ok()?;
    crate::embed::token_in_query(&search)
}

/// Rewrite the address bar without the token, keeping everything else —
/// `replaceState`, so the tokened URL doesn't even survive as a history
/// entry to arrow back to.
fn strip_token_from_url() {
    let Some(window) = leptos::web_sys::window() else {
        return;
    };
    let location = window.location();
    let (Ok(path), Ok(search), Ok(hash)) = (location.pathname(), location.search(), location.hash())
    else {
        return;
    };
    let url = format!("{path}{}{hash}", crate::embed::query_without_token(&search));
    if let Ok(history) = window.history() {
        let _ = history.replace_state_with_url(&wasm_bindgen::JsValue::NULL, "", Some(&url));
    }
}

/// The sign-in form.
///
/// Deliberately the whole page rather than a modal over the canvas: there is no
/// canvas to put it over, since every call behind it would come back 401.
#[component]
fn LoginPage() -> impl IntoView {
    let session = expect_context::<Session>();
    let username = RwSignal::new(String::new());
    let password = RwSignal::new(String::new());
    let failure = RwSignal::new(Option::<String>::None);
    let busy = RwSignal::new(false);

    let submit = move || {
        if busy.get_untracked() {
            return;
        }
        busy.set(true);
        failure.set(None);
        let (name, secret) = (username.get_untracked(), password.get_untracked());
        leptos::task::spawn_local(async move {
            let result = ApiClient {
                base: String::new(),
            }
            .login(&name, &secret)
            .await;
            busy.set(false);
            match result {
                Ok(auth) => {
                    // the password signal is dropped with the component, but
                    // clearing it means it isn't sitting in a signal while the
                    // canvas mounts either
                    password.set(String::new());
                    session.auth.set(Some(auth));
                }
                // its own wording rather than `to_string`, so a 401 always
                // reads the same whatever the body said — see
                // `ApiError::login_message`
                Err(err) => failure.set(Some(err.login_message())),
            }
        });
    };

    // A failure is about the credentials that were *submitted*, so it stops
    // being true the moment either field is edited. Leaving it up would put
    // "wrong username or password" under a password that has since been
    // corrected, which reads as the correction having been rejected too.
    let editing = move || failure.set(None);

    view! {
        <div class="login-page">
            <form
                class="login"
                on:submit=move |ev| {
                    ev.prevent_default();
                    submit();
                }
            >
                <div class="brand">"kayak"</div>
                <label for="login-username">"username"</label>
                <input
                    id="login-username"
                    class="text-input"
                    class:invalid=move || failure.get().is_some()
                    autocomplete="username"
                    autofocus
                    prop:value=move || username.get()
                    on:input=move |ev| {
                        editing();
                        username.set(event_target_value(&ev));
                    }
                />
                <label for="login-password">"password"</label>
                <input
                    id="login-password"
                    class="text-input"
                    class:invalid=move || failure.get().is_some()
                    type="password"
                    autocomplete="current-password"
                    prop:value=move || password.get()
                    on:input=move |ev| {
                        editing();
                        password.set(event_target_value(&ev));
                    }
                />
                // `role="alert"` so it is announced when it appears rather than
                // only being findable by looking. It is the one thing on this
                // page that arrives without the user causing it to be drawn.
                {move || {
                    failure
                        .get()
                        .map(|message| {
                            view! {
                                <p class="login-error" role="alert">
                                    {message}
                                </p>
                            }
                        })
                }}
                <button class="button primary" type="submit" disabled=move || busy.get()>
                    {move || if busy.get() { "signing in..." } else { "sign in" }}
                </button>
            </form>
        </div>
    }
}

/// The pipeline graph: every pipeline the server is running, as a pannable,
/// zoomable canvas of cards.
#[component]
pub fn CanvasPage() -> impl IntoView {
    let session = expect_context::<Session>();
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
    let state_reload = RwSignal::new(0_u32);
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
    // unlike `config_file` this keeps "still loading" apart from "no file":
    // the creator dialog must not flash up before the answers arrive
    let instance = Signal::derive(move || project::Instance {
        config_file: settings
            .get()
            .and_then(|res| res.as_ref().ok().map(|s| s.config_file.clone())),
        pipelines: pipelines
            .get()
            .and_then(|res| res.as_ref().ok().map(Vec::len)),
    });
    // false until the answer arrives — a "unsaved changes" warning that flashes
    // up on every load would train people to ignore it
    let unsaved = Signal::derive(move || {
        settings
            .get()
            .and_then(|res| res.as_ref().ok().map(|s| s.unsaved_changes))
            .unwrap_or(false)
    });
    // A session can end while a tab is open — it expires, someone signs out
    // elsewhere, or the server restarts and forgets every session it had. The
    // canvas would then sit there showing a graph nobody is allowed to read, so
    // the first 401 puts the login form back. The pipeline list is the right
    // place to watch: it is re-fetched on every edit, so a session that has
    // gone is noticed at the next thing anyone does.
    Effect::new(move |_| {
        if let Some(res) = pipelines.get()
            && matches!(res.as_ref(), Err(ApiError::Unauthorized(_)))
        {
            session.signed_out();
        }
    });

    let UseEventSourceReturn { data, .. } =
        use_event_source::<UiEvent, codee::string::JsonSerdeCodec>("/events");

    let feed = Feed::new();
    let canvas_state = CanvasState::new();
    let state = AppState {
        pipelines,
        connections,
        state_reload,
        events: data,
        feed,
        canvas_state,
        reload,
        config_file,
        save_directory,
        unsaved,
        instance,
        creator_dismissed: RwSignal::new(false),
        mode: RwSignal::new(Mode::ReadOnly),
        // read once, not subscribed to — see the field's docs
        may_edit: session.may_edit(),
        adding: RwSignal::new(false),
        add_upstream: RwSignal::new(None),
        adding_connection: RwSignal::new(false),
        saving: RwSignal::new(false),
        tab: RwSignal::new(SidebarTab::Pipelines),
        sidebar_mode: RwSignal::new(SidebarMode::default()),
        now: RwSignal::new(0.0),
        tz_offset: RwSignal::new(0),
    };
    provide_context(state);

    // The feed's two halves. Events are queued as they arrive and delivered on
    // the next frame — see [`Feed`] for why that is the shape of it.
    //
    // The frame loop only runs while there is something to deliver: it pauses
    // itself the first time it finds the queue empty, so an idle graph costs no
    // frames at all. Same arrangement as the camera glide and the pulse decay.
    let frames = use_raf_fn_with_options(
        move |_| {
            if !feed.deliver() {
                // nothing left; the next event wakes us
            }
        },
        UseRafFnOptions::default().immediate(false),
    );
    let (resume_frames, pause_frames) = (frames.resume.clone(), frames.pause.clone());
    Effect::new(move |_| {
        let Some(event) = data.get() else {
            return;
        };
        feed.queue(event);
        resume_frames();
    });
    // Stopping is decided off the queue rather than inside the callback so the
    // loop keeps a frame's grace: `deliver` returning false means this frame
    // found nothing, and the next event will resume it again.
    Effect::new(move |_| {
        feed.frame.track();
        if feed.pending.with_value(Vec::is_empty) {
            pause_frames();
        }
    });

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
        // because this is where a focus is acted on, and the selection goes with
        // it for the same reason: whatever the camera is being pointed at is
        // what the user means.
        canvas_state.maximized.set(None);
        canvas_state.select_only(&id);
        let Some(geom) = canvas_state.geom_of(&id) else {
            return;
        };
        // Centring alone is not enough on a zoomed-out canvas: the card arrives
        // in the middle of the screen and is still too small to read. The glide
        // carries the zoom as well as the position — `approach` interpolates all
        // three — so the card ends up framed rather than merely centred.
        let viewport = canvas_state.viewport.get_untracked();
        let camera = canvas_state.camera.get_untracked();
        let zoom = focus_zoom(camera, geom, viewport);
        let target = focus_camera(Camera { zoom, ..camera }, geom, viewport);
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
                        // Empty canvas is the way out of a selection, and the
                        // only one that scales: with a dozen cards selected,
                        // shift-clicking each of them back off is not a gesture.
                        // The edge handles stop propagation before this, so
                        // dragging a line never counts as a click into nothing.
                        canvas_state.clear_selection();
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
                                                    config=s.config.clone()
                                                    status=s.status
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
                    // A blank server's empty canvas says so, and keeps a way
                    // back into the creator dialog after a "not now" — declining
                    // it must never be a dead end. Behind the dialog's backdrop
                    // while it is open, which is fine: the dialog covers it.
                    <Show when=move || state.instance.get().is_blank() && state.may_edit>
                        <div class="canvas-blank">
                            <p>"this server has no project yet"</p>
                            <button
                                class="button primary"
                                on:click=move |_| state.creator_dismissed.set(false)
                            >
                                "create a project"
                            </button>
                        </div>
                    </Show>
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
            <Show when=move || {
                project::offer_creator(
                    &state.instance.get(),
                    state.may_edit,
                    state.creator_dismissed.get(),
                )
            }>
                <CreateProjectModal />
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

    // A downstream receiving a batch is a batch having crossed the edge above
    // it. Driven off the delivered *frame* rather than off each event: this
    // rebuilds the whole edge layer, and doing that once per event was the
    // third of the per-event costs — see [`Feed`].
    Effect::new(move |_| {
        let frame = state.feed.frame.get();
        let mut lit = Vec::new();
        for event in frame.iter() {
            lit.extend(pulsed_edges(event, &pipelines));
        }
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
                <button
                    class="tab"
                    class:active=move || is(SidebarTab::State)
                    on:click=move |_| tab.set(SidebarTab::State)
                >
                    "state"
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
            <Show when=move || is(SidebarTab::State)>
                <StateList />
            </Show>
        </div>
    }
}

/// How often the buckets are re-read while the state tab is open.
///
/// A poll rather than a subscription, and deliberately: a bucket changes on
/// every message that reaches a `remember`, so pushing that would be the
/// `/events` firehose all over again for a readout nobody watches per-message.
/// A second is fast enough to look live and slow enough to cost nothing.
const STATE_POLL_MS: f64 = 1000.0;

/// The state buckets, and what one of them is holding.
///
/// Read-only, which is the whole family: buckets are declared in the config and
/// filled by `remember` transforms, so there is no `+` and no delete here —
/// unlike the two tabs beside it, this one is a window rather than an editor.
#[component]
fn StateList() -> impl IntoView {
    let state = expect_context::<AppState>();
    // which bucket's card is open, and the viewport y its row was at when it
    // opened — the same trick `ConnectionList` uses, and for the same reason:
    // the sidebar scrolls, so the card is `position: fixed` and pinned to where
    // the row was.
    let opened = RwSignal::new(Option::<OpenBucket>::None);

    // only while this tab is mounted, which is what makes the poll free when
    // nobody is looking at it
    let handle = set_interval_with_handle(
        move || state.state_reload.update(|n| *n = n.wrapping_add(1)),
        std::time::Duration::from_millis(STATE_POLL_MS as u64),
    );
    on_cleanup(move || {
        if let Ok(handle) = handle {
            handle.clear();
        }
    });

    let list = NodeRef::<leptos::html::Div>::new();
    let _ = use_event_listener_with_options(
        document(),
        leptos::ev::scroll,
        move |ev| {
            if scrolled_above(&ev, list) {
                opened.set(None);
            }
        },
        UseEventListenerOptions::default().capture(true),
    );

    // Fetched into a plain signal rather than held in a `LocalResource` — see
    // `AppState::state_reload` for why a polled resource would rebuild the
    // canvas. A failed read leaves the last good answer on screen: the list is
    // a readout, and blanking it because one poll of many didn't land would be
    // worse than showing a value a second old.
    let buckets = RwSignal::new(Vec::<BucketSummary>::new());
    Effect::new(move |_| {
        // a revert can change which buckets exist; the timer is what makes the
        // readout live
        state.reload.track();
        state.state_reload.track();
        leptos::task::spawn_local(async move {
            if let Ok(list) = (ApiClient {
                base: String::new(),
            })
            .list_state_buckets()
            .await
            {
                buckets.set(list);
            }
        });
    });
    let rows = move || buckets.get();

    view! {
        <div class="sidebar-header">
            <span class="sidebar-title">"buckets"</span>
        </div>
        <div node_ref=list>
            {move || {
                let buckets = rows();
                if buckets.is_empty() {
                    // the common case, and worth saying rather than showing an
                    // empty box: buckets are opt-in and most configs have none
                    return view! {
                        <div class="empty">"no state buckets declared"</div>
                    }
                        .into_any();
                }
                buckets
                    .into_iter()
                    .map(|bucket| {
                        let name = bucket.name.clone();
                        let is_open = name.clone();
                        view! {
                            <div
                                class="tree-item"
                                class:selected=move || {
                                    opened.get().is_some_and(|o| o.name == is_open)
                                }
                                on:click=move |ev| {
                                    let name = bucket.name.clone();
                                    opened
                                        .update(|open| {
                                            *open = match open.take() {
                                                Some(o) if o.name == name => None,
                                                _ => Some(OpenBucket::at(name, &ev)),
                                            };
                                        });
                                }
                            >
                                <span class="tree-label">{bucket.name.clone()}</span>
                                // how full it is, which is the one number that
                                // says whether anything is happening at all
                                <span class="bucket-keys">
                                    {format!("{}/{}", bucket.keys, bucket.max_keys)}
                                </span>
                            </div>
                        }
                    })
                    .collect_view()
                    .into_any()
            }}
        </div>
        {move || {
            opened
                .get()
                .map(|open| view! { <StateBucketCard open=open /> })
        }}
    }
}

/// Which bucket's card is open, and where to put it.
#[derive(Clone, PartialEq)]
struct OpenBucket {
    name: String,
    /// viewport y of the row's top edge
    top: f64,
}

impl OpenBucket {
    fn at(name: String, ev: &leptos::ev::MouseEvent) -> Self {
        use wasm_bindgen::JsCast;
        let top = ev
            .current_target()
            .and_then(|t| t.dyn_into::<leptos::web_sys::Element>().ok())
            .map_or(0.0, |el| el.get_bounding_client_rect().top());
        Self { name, top }
    }
}

/// What one bucket is holding, keys and values.
///
/// Its own resource rather than a slice of the summaries: the list needs a
/// count per bucket and the card needs every value of one, and asking for the
/// second to draw the first would mean pulling every bucket's contents every
/// second to render two numbers.
#[component]
fn StateBucketCard(open: OpenBucket) -> impl IntoView {
    let state = expect_context::<AppState>();
    let name = StoredValue::new(open.name.clone());

    // A signal and an effect rather than a `LocalResource`, for the reason the
    // list beside it uses one: this card is inside the page's single
    // `<Suspense>`, and a resource refetching once a second re-suspends the
    // whole boundary. See `AppState::state_reload`.
    let contents = RwSignal::new(Option::<Result<BucketContents, String>>::None);
    Effect::new(move |_| {
        // the same tick the list polls on, so the card and the count beside it
        // are never a second out of step with each other
        state.state_reload.track();
        leptos::task::spawn_local(async move {
            let read = (ApiClient {
                base: String::new(),
            })
            .state_bucket(&name.get_value())
            .await;
            contents.set(Some(read.map_err(|e| e.to_string())));
        });
    });

    view! {
        <div class="state-card" style:top=format!("{}px", open.top)>
            <div class="state-card-name">{open.name}</div>
            {move || match contents.get().as_ref().map(|r| r.as_ref()) {
                Some(Ok(contents)) if contents.entries.is_empty() => {
                    view! { <div class="empty">"nothing remembered yet"</div> }.into_any()
                }
                Some(Ok(contents)) => {
                    let truncated = contents.truncated;
                    let keys = contents.keys;
                    let entries = contents.entries.clone();
                    view! {
                        <div class="state-entries">
                            {entries
                                .into_iter()
                                .map(|entry| {
                                    view! {
                                        <div class="state-entry">
                                            // a pipeline with no `state.key`
                                            // holds one bucket-wide value under
                                            // the empty key, which would
                                            // otherwise draw a blank line
                                            <div class="state-key" class:unkeyed=entry.key.is_empty()>
                                                {if entry.key.is_empty() {
                                                    "the whole bucket".to_string()
                                                } else {
                                                    entry.key.clone()
                                                }}
                                            </div>
                                            <div class="property-list">
                                                {entry
                                                    .values
                                                    .into_iter()
                                                    .map(|(name, value)| {
                                                        view! {
                                                            <div class="property">
                                                                <span class="property-name">{name}</span>
                                                                <span class="property-value">
                                                                    {inspector::render(&value)}
                                                                </span>
                                                            </div>
                                                        }
                                                    })
                                                    .collect_view()}
                                            </div>
                                            <div class="state-updated">{entry.updated_at}</div>
                                        </div>
                                    }
                                })
                                .collect_view()}
                        </div>
                        <Show when=move || truncated>
                            // the card is a page of a bucket that may hold
                            // thousands; saying so is what stops it reading as
                            // the whole truth
                            <div class="state-truncated">
                                {format!("showing the newest of {keys} keys")}
                            </div>
                        </Show>
                    }
                        .into_any()
                }
                // the list and the card read the same server, so this is the
                // moment between a revert dropping a bucket and the list
                // noticing
                Some(Err(_)) => view! { <div class="empty">"bucket is gone"</div> }.into_any(),
                None => view! { <div class="empty">"loading..."</div> }.into_any(),
            }}
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
    // The row menu that is open, if any. One at a time, like the armed delete,
    // and pinned to the row it belongs to for the reason `ConnectionCard` is:
    // the sidebar scrolls its own content, so a menu laid out inside the list
    // would be clipped at its edge.
    let menu = RwSignal::new(Option::<RowMenu>::None);

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
    let any_pipelines =
        Memo::new(move |_| matches!(state.pipelines.get(), Some(Ok(list)) if !list.is_empty()));
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

    // Add a pipeline and everything downstream of it to the selection.
    //
    // Added rather than replacing what was selected, which is what makes two
    // branches of a fan-out reachable in two clicks; the way back is the same
    // as from any selection, a click on empty canvas. The camera is left where
    // it is on purpose — this is a gesture about a set of cards, and gliding to
    // one of them says the wrong thing about which.
    let select_children = move |id: &PipelineId| {
        menu.set(None);
        let Some(Ok(list)) = state.pipelines.get_untracked() else {
            return;
        };
        let pairs: Vec<(PipelineId, Config)> = list
            .iter()
            .map(|s| (s.id.clone(), s.config.clone()))
            .collect();
        state
            .canvas_state
            .select_also(&selection::descendants(&pipelines_from(&pairs), id));
    };

    // The menu is pinned to a row's position at the moment it opened, so a
    // scroll would strand it beside the wrong name — the same reason, and the
    // same document-level capture, as the connections card.
    let list = NodeRef::<leptos::html::Div>::new();
    let _ = use_event_listener_with_options(
        document(),
        leptos::ev::scroll,
        move |ev| {
            if scrolled_above(&ev, list) {
                menu.set(None);
            }
        },
        UseEventListenerOptions::default().capture(true),
    );

    view! {
        <div class="sidebar-list" node_ref=list>
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
                        on:click=move |_| state.open_add(None)
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
                        let (focus_id, arm_id, delete_id, is_armed, is_selected) = (
                            id.clone(),
                            id.clone(),
                            id.clone(),
                            id.clone(),
                            id.clone(),
                        );
                        let menu_id = StoredValue::new(id.clone());
                        let armed_here = Memo::new(move |_| {
                            armed.get().as_deref() == Some(is_armed.as_str())
                        });
                        // Every row for a pipeline lights up, the repeats
                        // included: a repeat is the same pipeline, and marking
                        // only the full row would make the selection look lost
                        // in tree mode.
                        let selected_here = Memo::new(move |_| {
                            state
                                .canvas_state
                                .selected
                                .with(|s| s.contains(is_selected.as_str()))
                        });
                        let menu_here = Memo::new(move |_| {
                            menu.with(|m| {
                                m.as_ref()
                                    .is_some_and(|m| menu_id.with_value(|id| m.id == *id))
                            })
                        });
                        view! {
                            <div
                                class="tree-item"
                                class:repeat=repeat
                                class:selected=move || selected_here.get()
                                // one level of nesting per grid step; a flat
                                // list is every row at zero, so this is the
                                // same rule in both modes
                                style:padding-left=format!("{}px", 8 + depth * 12)
                                title=repeat.then_some("also fed by this — shown in full elsewhere")
                                // Shift is the same gesture here as on a card
                                // and does the same thing: add this pipeline to
                                // the selection, or take it out. It moves no
                                // camera — a shift-click is about building a
                                // set, and gliding to whichever row was clicked
                                // last would fight that.
                                on:click=move |ev| {
                                    armed.set(None);
                                    menu.set(None);
                                    if ev.shift_key() && state.editing() {
                                        state.canvas_state.toggle_selected(&focus_id);
                                    } else {
                                        state
                                            .canvas_state
                                            .focus_request
                                            .set(Some(focus_id.clone()));
                                    }
                                }
                            >
                                <span class="tree-label">{id.clone()}</span>
                                // Not on a repeat row, for the reason the delete
                                // isn't: the open menu is keyed by id, so two
                                // rows for one pipeline would open together and
                                // read as two menus.
                                {(!repeat)
                                    .then(|| {
                                        view! {
                                            // Edit mode only, like the delete
                                            // beside it: what it offers is about
                                            // arranging the canvas, which
                                            // read-only doesn't do.
                                            <Show when=move || state.editing()>
                                                <button
                                                    class="icon-button row-menu-button"
                                                    class:open=move || menu_here.get()
                                                    title="more"
                                                    aria-label="more"
                                                    on:click=move |ev| {
                                                        // the row itself moves the camera
                                                        ev.stop_propagation();
                                                        armed.set(None);
                                                        if menu_here.get() {
                                                            menu.set(None);
                                                        } else {
                                                            menu.set(
                                                                Some(RowMenu::at(menu_id.get_value(), &ev)),
                                                            );
                                                        }
                                                    }
                                                >
                                                    "⋯"
                                                </button>
                                            </Show>
                                        }
                                    })}
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
            // One menu for the whole list rather than one per row: only one can
            // be open, and a row that renders its own would be clipped by the
            // sidebar it sits in.
            {move || {
                menu.get()
                    .map(|open| {
                        let id = open.id.clone();
                        view! {
                            <div class="row-menu" style:top=format!("{}px", open.top)>
                                <button
                                    class="row-menu-item"
                                    title="add this pipeline and everything downstream of it to the selection"
                                    on:click=move |ev| {
                                        ev.stop_propagation();
                                        select_children(&id);
                                    }
                                >
                                    "select children"
                                </button>
                            </div>
                        }
                    })
            }}
        </div>
    }
}

/// The pipeline whose row menu is open, and where to put it.
///
/// Positioned like [`OpenConnection`] and for the same reason — the sidebar
/// scrolls, so the menu is `position: fixed` and takes the row's viewport
/// position at the moment it was asked for.
#[derive(Clone, Debug, PartialEq)]
struct RowMenu {
    id: PipelineId,
    /// viewport y of the button that opened it
    top: f64,
}

impl RowMenu {
    /// Placed against the button rather than the row: `current_target` is the
    /// button this is wired to, and hanging the menu off it is what makes the
    /// two read as one control.
    fn at(id: PipelineId, ev: &leptos::ev::MouseEvent) -> Self {
        use wasm_bindgen::JsCast;
        let top = ev
            .current_target()
            .and_then(|t| t.dyn_into::<leptos::web_sys::Element>().ok())
            .map_or(0.0, |el| el.get_bounding_client_rect().bottom());
        Self { id, top }
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
        Self::from_draft(form::draft_of(doc))
    }

    /// The same, from a draft that already has something in it — what a modal
    /// opened from a card's downstream handle starts with.
    fn from_draft(draft: form::ComponentDraft) -> Self {
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
    // What sampling the input found, by draft position. Empty until something
    // is sampled; see [`SampleSchemas`].
    let schemas = SampleSchemas(RwSignal::new(Vec::<MessageSchema>::new()));
    provide_context(schemas);
    // What the panel beside the form is showing. Provided here rather than
    // held by the component that fills it, because the button that fetches and
    // the panel that shows are two levels apart.
    provide_context(SampleTarget(RwSignal::new(Option::<SampleView>::None)));
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
    // A pipeline needs an input, so start it with one rather than with an
    // empty form and an error waiting to happen. Opened from a card's
    // downstream handle, that one input is already reading from that card;
    // read untracked, because a seed is a starting point and not something the
    // form should be dragged back to.
    let seeded = state
        .add_upstream
        .get_untracked()
        .and_then(|upstream| docs.with_value(|docs| form::draft_fed_by(docs, &upstream)));
    match seeded {
        Some(draft) => drafts.update(|d| d.push(DraftSignals::from_draft(draft))),
        None => add(Family::Input),
    }

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
            // the form and the sample side by side: the panel is a sibling of
            // the modal box rather than part of it, so it can be as wide as a
            // message needs without the form's controls stretching to match
            <div class="modal-with-panel" on:click=move |ev| ev.stop_propagation()>
            <div class="modal">
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
            <SamplePanel />
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

/// What a sample of real messages showed at each component of the draft, by
/// that component's position in the flat draft list — the same index
/// [`form::FormError`] keys a field error by.
///
/// Ambient rather than a prop for the reason [`AppState`] is ambient in the
/// navbar: it is threaded through [`FieldEditor`], which renders *itself* for a
/// nested field, so a prop would have to be carried through every level of
/// that recursion to reach a `field` box three deep in a column mapping.
///
/// Empty until something samples the input, which is the state every form
/// starts in and the one it stays in for a stream nobody has fetched from: an
/// empty list of suggestions renders as an ordinary text box, which is exactly
/// what a message-field box was before any of this existed.
#[derive(Clone, Copy)]
struct SampleSchemas(RwSignal<Vec<MessageSchema>>);

impl SampleSchemas {
    /// What the component at `index` sees, if anything has been sampled.
    fn at(self, index: usize) -> Option<MessageSchema> {
        self.0.with(|schemas| schemas.get(index).cloned())
    }

    /// Records what the component at `index` sees, growing the list if the
    /// components before it have not been sampled.
    fn set_at(self, index: usize, schema: MessageSchema) {
        self.0.update(|schemas| {
            if schemas.len() <= index {
                schemas.resize(index + 1, MessageSchema::default());
            }
            schemas[index] = schema;
        });
    }

    /// Forgets everything. Called when the draft is rearranged: the entries
    /// are keyed by position, so a removed component makes every schema after
    /// it a statement about a different component.
    fn clear(self) {
        self.0.set(Vec::new());
    }

    /// The id of the `<datalist>` holding that component's field names.
    /// Per component rather than one for the form: what a transform sees is
    /// what the transforms in front of it left behind, so the answer differs
    /// down the chain.
    fn list_id(index: usize) -> String {
        format!("message-fields-{index}")
    }
}

/// What the panel beside the form is showing, if anything.
///
/// One at a time and keyed by the component it came from: two inputs are two
/// streams, and a panel showing both merged would say something about a
/// message shape that no message has.
#[derive(Clone, Copy)]
struct SampleTarget(RwSignal<Option<SampleView>>);

impl SampleTarget {
    fn show(self, index: usize, kind: &str, state: SampleState) {
        self.0.set(Some(SampleView {
            index,
            kind: kind.to_string(),
            state,
            stages: Vec::new(),
            chain_note: None,
        }));
    }

    /// Puts what the chain did under the messages the input produced, if the
    /// panel is still showing the component that was sampled. It may not be:
    /// a dry run is a second round trip, and the kind may have been changed
    /// while it was out.
    fn show_chain(self, index: usize, stages: Vec<StageView>, note: Option<String>) {
        self.0.update(|open| {
            if let Some(view) = open.as_mut().filter(|view| view.index == index) {
                view.stages = stages;
                view.chain_note = note;
            }
        });
    }

    /// Closes the panel if it is showing this component — what changing a
    /// component's kind or taking it out has to do, since what it holds is
    /// then about something that no longer exists.
    fn forget(self, index: usize) {
        if self.0.with_untracked(|open| open.as_ref().is_some_and(|v| v.index == index)) {
            self.0.set(None);
        }
    }
}

#[derive(Clone)]
struct SampleView {
    index: usize,
    kind: String,
    state: SampleState,
    /// What each transform in the draft did to those messages, once the dry
    /// run has answered. Empty while it is out, and for a draft with no
    /// transforms — which is the same picture, correctly.
    stages: Vec<StageView>,
    /// Why there are no stages, when there is a reason worth giving: a
    /// transform that isn't filled in yet, or a chain that broke.
    chain_note: Option<String>,
}

/// One transform's worth of the panel: what it handed on.
#[derive(Clone)]
struct StageView {
    title: String,
    messages: Vec<serde_json::Value>,
    /// Set when the stage handed on nothing, saying which kind of nothing it
    /// was — dropped, or still held.
    empty: Option<String>,
}

#[derive(Clone)]
enum SampleState {
    /// The request is out. A state of its own because sampling waits on a real
    /// stream — several seconds of nothing is the *expected* case for a quiet
    /// subject, and a button that looked idle through it would be pressed
    /// again.
    Fetching,
    Sampled {
        messages: Vec<serde_json::Value>,
        notes: Vec<String>,
    },
    /// The input could not be built, or reading it failed. Not an error dialog:
    /// it belongs in the panel, next to where the messages would have been,
    /// because it is the answer to the same question.
    Failed(String),
}

/// Puts the sampled messages through the draft's transforms and records what
/// every component of the draft ends up seeing.
///
/// The second half of the sample and the reason it is worth taking: an input's
/// fields are only the fields the *first* transform sees, and a column mapping
/// three stages later is written against something else entirely. Running the
/// chain is the only way to know what that something else is.
///
/// The schemas are recorded by what each component **sees**, not by what it
/// produces: a transform is configured against its input, so the box asking
/// for a field name should offer the fields that will reach it. Which is also
/// why every output gets the last stage's answer.
async fn run_chain(
    sampled_at: usize,
    messages: Vec<serde_json::Value>,
    drafts: RwSignal<Vec<DraftSignals>>,
    docs: StoredValue<Vec<kayak_core::docs::ComponentDoc>>,
    target: SampleTarget,
    schemas: Option<SampleSchemas>,
) {
    let snapshots: Vec<form::ComponentDraft> = drafts
        .get_untracked()
        .into_iter()
        .map(DraftSignals::snapshot)
        .collect();
    let chain = match docs.with_value(|docs| form::transform_chain(&snapshots, docs)) {
        Ok(chain) => chain,
        // The ordinary state of a form someone is still filling in, so it is a
        // note rather than an error: the messages above it are still the
        // answer to what was asked.
        Err(unbuildable) => {
            let which = unbuildable
                .iter()
                .map(|at| snapshots.get(*at).map_or("?", |d| d.kind.as_str()))
                .collect::<Vec<_>>()
                .join(", ");
            target.show_chain(
                sampled_at,
                Vec::new(),
                Some(format!("not run: {which} is not filled in yet")),
            );
            return;
        }
    };

    let transforms = match serde_json::from_value(serde_json::Value::Array(
        chain.iter().map(|(_, value)| value.clone()).collect(),
    )) {
        Ok(transforms) => transforms,
        Err(err) => {
            target.show_chain(sampled_at, Vec::new(), Some(err.to_string()));
            return;
        }
    };

    let answer = ApiClient {
        base: String::new(),
    }
    .dry_run_pipeline(&kayak_core::dry_run::PipelineDryRunRequest {
        messages: messages.clone(),
        transforms,
        state: None,
        buckets: std::collections::BTreeMap::new(),
    })
    .await;

    let (results, note) = match answer {
        Ok(kayak_core::dry_run::PipelineDryRunResponse::Ran { stages, .. }) => (stages, None),
        Ok(kayak_core::dry_run::PipelineDryRunResponse::Failed {
            stages,
            kind,
            message,
            ..
        }) => (stages, Some(format!("{kind} failed: {message}"))),
        Err(err) => {
            target.show_chain(sampled_at, Vec::new(), Some(err.to_string()));
            return;
        }
    };

    // What arrives at the next component, threaded down the draft. It starts
    // as the sample, which is exactly what the first transform is handed.
    let mut arriving = messages;
    let mut views = Vec::with_capacity(results.len());
    for (position, stage) in results.iter().enumerate() {
        if let (Some(schemas), Some((at, _))) = (schemas, chain.get(position)) {
            schemas.set_at(*at, kayak_core::schema::infer(&arriving));
        }
        arriving = stage.messages();
        views.push(StageView {
            title: format!("{} · {}", position + 1, stage.kind),
            empty: arriving
                .is_empty()
                .then(|| "handed on nothing".to_string()),
            messages: arriving.clone(),
        });
    }
    // Every output sees what came out of the last transform — or the sample
    // itself, for a pipeline that transforms nothing.
    if let Some(schemas) = schemas {
        let final_schema = kayak_core::schema::infer(&arriving);
        for (at, draft) in snapshots.iter().enumerate() {
            if draft.family == Family::Output {
                schemas.set_at(at, final_schema.clone());
            }
        }
    }
    target.show_chain(sampled_at, views, note);
}

/// The panel beside the add-pipeline form: what the input being configured is
/// actually carrying.
///
/// It sits *outside* the modal's own box rather than inside its body, and that
/// is the point of it: the form is a column of narrow controls and a message
/// is a block of JSON, so putting one under the other would push whatever is
/// being configured off the screen at the moment it is being checked.
#[component]
fn SamplePanel() -> impl IntoView {
    let target = use_context::<SampleTarget>();
    move || {
        let view = target?.0.get()?;
        Some(view! {
            <aside class="sample-panel">
                <header>
                    <span class="modal-title">{format!("sample · {}", view.kind)}</span>
                    <button
                        class="icon-button"
                        title="close"
                        on:click=move |_| {
                            if let Some(target) = target {
                                target.0.set(None);
                            }
                        }
                    >
                        "×"
                    </button>
                </header>
                <div class="sample-body">
                    {sample_body(view.state)}
                    {view
                        .chain_note
                        .map(|note| view! { <div class="form-hint">{note}</div> })}
                    {view
                        .stages
                        .into_iter()
                        .map(|stage| {
                            view! {
                                <div class="sample-stage">{stage.title}</div>
                                {match stage.empty {
                                    // said rather than left blank: a stage with
                                    // nothing under it is indistinguishable
                                    // from one that hasn't been drawn
                                    Some(why) => view! { <div class="empty">{why}</div> }.into_any(),
                                    None => {
                                        stage
                                            .messages
                                            .into_iter()
                                            .map(|message| {
                                                let rendered = pretty::render(&message.to_string());
                                                view! { <MessageBody message=rendered /> }
                                            })
                                            .collect_view()
                                            .into_any()
                                    }
                                }}
                            }
                        })
                        .collect_view()}
                </div>
            </aside>
        })
    }
}

/// What the panel holds, by what the sample came back as.
fn sample_body(state: SampleState) -> AnyView {
    match state {
        SampleState::Fetching => view! {
            <div class="empty">"waiting for a message…"</div>
        }
        .into_any(),
        SampleState::Failed(message) => view! {
            <div class="form-error">{message}</div>
        }
        .into_any(),
        // Nothing arriving is an answer, and the panel says which answer it is:
        // the stream is quiet, not the button broken. None of these inputs can
        // replay what was published before the sample started, which is the
        // part a user has no way to guess.
        SampleState::Sampled { messages, notes } if messages.is_empty() => view! {
            <div class="empty">"nothing arrived. A sample only sees messages published while it waits."</div>
            {notes_view(notes)}
        }
        .into_any(),
        SampleState::Sampled { messages, notes } => view! {
            {notes_view(notes)}
            <div class="sample-count">
                {if messages.len() == 1 {
                    "1 message".to_string()
                } else {
                    format!("{} messages", messages.len())
                }}
            </div>
            {messages
                .into_iter()
                .map(|message| {
                    // through the log's renderer, so a payload looks the same
                    // here as it will on the card once this pipeline is running
                    let rendered = pretty::render(&message.to_string());
                    view! { <MessageBody message=rendered /> }
                })
                .collect_view()}
        }
        .into_any(),
    }
}

/// What the sample did differently from the pipeline it stands in for, from
/// the server's own `notes` — never rephrased here, so what the endpoint says
/// and what the panel says cannot drift apart.
fn notes_view(notes: Vec<String>) -> AnyView {
    notes
        .into_iter()
        .map(|note| view! { <div class="form-hint">{note}</div> })
        .collect_view()
        .into_any()
}

/// The suggestions behind one component's message-field boxes.
///
/// A `<datalist>` and not a `<select>`, which is the whole design of this
/// control: a sample is a handful of messages, so a field it did not happen to
/// carry is still a field, and the box has to accept one. The list only makes
/// the common case a click instead of a retype.
#[component]
fn MessageFieldOptions(index: usize) -> impl IntoView {
    let schemas = use_context::<SampleSchemas>();
    move || {
        let schema = schemas?.at(index)?;
        Some(view! {
            <datalist id=SampleSchemas::list_id(index)>
                {schema
                    .fields
                    .iter()
                    .map(|field| {
                        // the label carries what the sample knew: the type, and
                        // a value to recognise the field by. A field the sample
                        // disagreed about says so rather than picking one.
                        let mut label = field
                            .types
                            .iter()
                            .map(|t| t.as_str())
                            .collect::<Vec<_>>()
                            .join(" | ");
                        if let Some(example) = &field.example {
                            label.push_str(&format!(" · {example}"));
                        }
                        view! { <option value=field.path.clone() label=label></option> }
                    })
                    .collect_view()}
            </datalist>
        })
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
    // Ambient like the field suggestions and for the same reason: only the
    // add-pipeline form has a panel to show a sample in, and the connection
    // form reuses this component without one.
    let sample_target = use_context::<SampleTarget>();

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
        // whatever was sampled came out of the component this no longer is
        if let Some(target) = sample_target {
            target.forget(index);
        }
    };

    // by position, not by identity: the list is rebuilt whenever it changes, so
    // `index` is always this component's current place in it
    let remove = move |_| {
        drafts.update(|d| {
            if index < d.len() {
                d.remove(index);
            }
        });
        // both are keyed by position, so a removal moves every entry after
        // this one onto a different component
        if let Some(target) = sample_target {
            target.forget(index);
        }
        if let Some(schemas) = use_context::<SampleSchemas>() {
            schemas.clear();
        }
    };

    // Fetch a few real messages from this input, without creating anything.
    // The same validation the submit does, on this component alone: an input
    // that can't be built into a config can't be sampled either, and the
    // boxes light up exactly as they would on submit.
    let fetch = move |_: leptos::ev::MouseEvent| {
        let (Some(target), Some(doc)) = (sample_target, doc()) else {
            return;
        };
        let snapshot = draft.snapshot();
        match form::component_json(&doc, &snapshot) {
            Err(found) => {
                errors.update(|errors| {
                    errors.retain(|error| !form::is_field_error_for(error, index));
                    errors.extend(found.into_iter().map(|(field, message)| {
                        form::FormError::Field {
                            component: index,
                            field,
                            message,
                        }
                    }));
                });
            }
            Ok(body) => {
                errors.update(|errors| {
                    errors.retain(|error| !form::is_field_error_for(error, index));
                });
                let kind = snapshot.kind.clone();
                let input = match serde_json::from_value(body) {
                    Ok(input) => input,
                    // the form built something the config types don't accept.
                    // Shown in the panel rather than swallowed: it is a real
                    // answer about this input, and one worth a bug report.
                    Err(err) => {
                        target.show(index, &kind, SampleState::Failed(err.to_string()));
                        return;
                    }
                };
                target.show(index, &kind, SampleState::Fetching);
                let schemas = use_context::<SampleSchemas>();
                leptos::task::spawn_local(async move {
                    let request = kayak_core::sample::SampleRequest {
                        input,
                        max_messages: None,
                        timeout_ms: None,
                    };
                    let answer = ApiClient {
                        base: String::new(),
                    }
                    .sample_input(&request)
                    .await;
                    let state = match answer {
                        Ok(kayak_core::sample::SampleResponse::Sampled {
                            messages, notes, ..
                        }) => {
                            if let Some(schemas) = schemas {
                                schemas.set_at(index, kayak_core::schema::infer(&messages));
                            }
                            SampleState::Sampled {
                                messages: messages.clone(),
                                notes,
                            }
                        }
                        Ok(kayak_core::sample::SampleResponse::Failed { message, .. }) => {
                            SampleState::Failed(message)
                        }
                        Err(err) => SampleState::Failed(err.to_string()),
                    };
                    let sampled = match &state {
                        SampleState::Sampled { messages, .. } => Some(messages.clone()),
                        _ => None,
                    };
                    target.show(index, &kind, state);
                    // and straight on to what the rest of the draft does with
                    // them: a second round trip, so the messages are on screen
                    // while it is out
                    if let Some(messages) = sampled {
                        run_chain(index, messages, drafts, docs, target, schemas).await;
                    }
                });
            }
        }
    };

    view! {
        <div class="component-editor">
            <MessageFieldOptions index=index />
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
                // Only an input has anything to fetch, and only the form with
                // a panel beside it has anywhere to put it. A kind that cannot
                // be sampled keeps the button and disables it, carrying the
                // reason: "there is nothing here" would read as a bug, and the
                // reason says what to do instead.
                {move || {
                    let target = sample_target?;
                    if family != Family::Input {
                        return None;
                    }
                    let sampling = kayak_core::sample::for_input(&draft.kind.get())?;
                    let refused = !sampling.allowed();
                    let title = sampling
                        .note()
                        .map_or_else(
                            || "fetch a few real messages from this input".to_string(),
                            ToString::to_string,
                        );
                    let fetching = move || {
                        target
                            .0
                            .with(|open| {
                                open.as_ref()
                                    .is_some_and(|view| {
                                        view.index == index
                                            && matches!(view.state, SampleState::Fetching)
                                    })
                            })
                    };
                    Some(
                        view! {
                            <button
                                class="button"
                                title=title
                                disabled=move || refused || fetching()
                                on:click=fetch
                            >
                                {move || if fetching() { "fetching…" } else { "fetch messages" }}
                            </button>
                        },
                    )
                }}
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
                                        prefix=String::new()
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
///
/// It renders itself for a field with a shape of its own, which is why it
/// returns an [`AnyView`] rather than an opaque `impl IntoView` — a component
/// that contains itself can't have a return type defined in terms of its own.
/// `prefix` is where in the draft this field sits: empty at the top, and the
/// path of the object or variant it belongs to further down.
#[component]
fn FieldEditor(
    field: kayak_core::docs::FieldDoc,
    prefix: String,
    index: usize,
    values: RwSignal<HashMap<String, String>>,
    errors: RwSignal<Vec<form::FormError>>,
    pipelines: Signal<Vec<String>>,
    connections: Signal<Vec<(String, String)>>,
) -> AnyView {
    let name = form::path(&prefix, &field.name);
    // read once, on purpose: the control is uncontrolled from here on, so that
    // typing into it doesn't rebuild it
    let initial = values.with_untracked(|v| v.get(&name).cloned().unwrap_or_default());
    let error_name = name.clone();
    let error = Memo::new(move |_| {
        errors.with(|errors| form::field_error(errors, index, &error_name).map(ToString::to_string))
    });

    // the path this field sits at, kept back from the closures below because
    // the nested controls need it: it is the prefix of everything inside it
    let at = name.clone();
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
            let chosen = at.clone();
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
            let chosen = at.clone();
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
        // A value with fields of its own: the fields, laid out in place under
        // this one's name. Nothing conditional about it — every one of them is
        // always there — so it is the union arm below without the choice.
        FieldType::Object(fields) => nested(
            fields.clone(),
            at.clone(),
            index,
            values,
            errors,
            pipelines,
            connections,
        ),
        // The one field with no fixed number of boxes. Rows are added and taken
        // away, and each of them is whatever the element type asks for — an
        // aggregation's three boxes, or one box for a field name — so nothing
        // here knows what it is rendering rows *of*.
        //
        // Like the union below, this control reads its own value back: the row
        // count is exactly what has to rebuild the list, and it is held in a
        // local signal so a keystroke inside a row can't reach it.
        FieldType::List(element) => {
            let element = (**element).clone();
            let at = at.clone();
            let rows = RwSignal::new(values.with_untracked(|v| form::list_len(v, &at)));
            let add = {
                let at = at.clone();
                move |_| {
                    let len = values
                        .try_update(|v| form::push_list_element(v, &at))
                        .unwrap_or_default();
                    rows.set(len);
                }
            };
            let list_at = at.clone();
            view! {
                <div class="form-list">
                    {move || {
                        let count = rows.get();
                        (0..count)
                            .map(|row| {
                                let mut field = element.clone();
                                field.name = row.to_string();
                                let at = list_at.clone();
                                // taking a row out shifts the ones below it
                                // down, so their messages are about boxes that
                                // have moved and go with them
                                let remove = move |_| {
                                    let left = values
                                        .try_update(|v| form::remove_list_element(v, &at, row))
                                        .unwrap_or_default();
                                    errors
                                        .update(|errors| {
                                            form::clear_list_errors(errors, index, &at)
                                        });
                                    rows.set(left);
                                };
                                view! {
                                    <div class="form-list-row">
                                        <div class="form-list-body">
                                            <FieldEditor
                                                field=field
                                                prefix=list_at.clone()
                                                index=index
                                                values=values
                                                errors=errors
                                                pipelines=pipelines
                                                connections=connections
                                            />
                                        </div>
                                        <button
                                            class="icon-button danger"
                                            title="remove"
                                            on:click=remove
                                        >
                                            "×"
                                        </button>
                                    </div>
                                }
                            })
                            .collect_view()
                    }}
                    <button class="button" on:click=add>
                        "+ add"
                    </button>
                </div>
            }
            .into_any()
        }
        // The conditional one. Which boxes belong here is not known until the
        // tag is picked, so the tag is a dropdown and the rest of the form is
        // derived from it — the same shape the component's own `variants`
        // selector has, one level down.
        //
        // This is the one control whose choice is *read back*: everything else
        // here is uncontrolled because rebuilding a box destroys what is being
        // typed into it, but rebuilding is the entire point of this one. The
        // signal is local and holds only the tag, so a keystroke in a nested
        // box doesn't reach it.
        FieldType::Union(union) => {
            let tag_at = form::path(&at, &union.tag);
            let chosen = RwSignal::new(
                values.with_untracked(|v| v.get(&tag_at).cloned().unwrap_or_default()),
            );
            let unset = chosen.get_untracked().is_empty();
            // same bargain as the enum control: a blank entry so that nothing
            // chosen doesn't look like the first variant chosen
            let blank = !field.required || unset;
            let variants = union.variants.clone();
            let options = variants.clone();
            let tag_error = {
                let tag_at = tag_at.clone();
                Memo::new(move |_| {
                    errors.with(|errors| {
                        form::field_error(errors, index, &tag_at).map(ToString::to_string)
                    })
                })
            };
            let pick = move |ev: leptos::ev::Event| {
                let value = event_target_value(&ev);
                values.update(|v| {
                    v.insert(tag_at.clone(), value.clone());
                });
                errors.update(|errors| form::clear_field_error(errors, index, &tag_at));
                chosen.set(value);
            };
            let prefix = at.clone();
            view! {
                <select class="select" class:invalid=move || tag_error.get().is_some() on:change=pick>
                    <Show when=move || blank>
                        <option value="" selected=unset></option>
                    </Show>
                    {
                        let selected = chosen.get_untracked();
                        options
                            .into_iter()
                            .map(|variant| {
                                let name = variant.name.clone();
                                let label = name.clone();
                                view! {
                                    <option value=name.clone() selected=name == selected>
                                        {label}
                                    </option>
                                }
                            })
                            .collect_view()
                    }
                </select>
                {move || tag_error.get().map(|message| view! { <div class="form-error">{message}</div> })}
                {
                    let variants = variants.clone();
                    let prefix = prefix.clone();
                    move || {
                        let picked = chosen.get();
                        variants
                            .iter()
                            .find(|variant| variant.name == picked)
                            .map(|variant| {
                                nested(
                                    variant.fields.clone(),
                                    prefix.clone(),
                                    index,
                                    values,
                                    errors,
                                    pipelines,
                                    connections,
                                )
                            })
                    }
                }
            }
            .into_any()
        }
        // Two values is a closed set like any other, and the parser takes the
        // words `true` and `false` — so it is the enum control with the two
        // values written out, rather than a checkbox. A checkbox has nowhere to
        // put "not set", which is what an omitted optional boolean is.
        FieldType::Boolean => {
            let unset = initial.is_empty();
            let blank = !field.required || unset;
            view! {
                <select class="select" on:change=write>
                    <Show when=move || blank>
                        <option value="" selected=unset></option>
                    </Show>
                    {
                        let selected = initial.clone();
                        ["true", "false"]
                            .into_iter()
                            .map(|value| {
                                view! {
                                    <option value=value selected=value == selected>
                                        {value}
                                    </option>
                                }
                            })
                            .collect_view()
                    }
                </select>
            }
            .into_any()
        }
        // A field of the *messages*, so a box with a list rather than a
        // dropdown: what a sample carried is a suggestion, never the set of
        // legal answers. See [`SampleSchemas`]. With nothing sampled the list
        // is empty and this is the plain text box it has always been.
        FieldType::MessageField => view! {
            <input
                class="text-input"
                list=SampleSchemas::list_id(index)
                value=initial.clone()
                placeholder="field path"
                on:input=write
            />
        }
        .into_any(),
        // Code, so not a one-line box: a syntax-highlighted editor with a
        // gutter, and a pane that runs it. See [`ScriptEditor`].
        FieldType::Script(_) => view! {
            <ScriptEditor at=at.clone() initial=initial.clone() values=values errors=errors index=index />
        }
        .into_any(),
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
    .into_any()
}

/// The fields inside a field, rendered under its path.
///
/// One indented block of the same rows, which is what keeps a nested field
/// looking like — and behaving as — the fields around it: its own errors, its
/// own required markers, and a control chosen by its own type, however deep.
fn nested(
    fields: Vec<kayak_core::docs::FieldDoc>,
    prefix: String,
    index: usize,
    values: RwSignal<HashMap<String, String>>,
    errors: RwSignal<Vec<form::FormError>>,
    pipelines: Signal<Vec<String>>,
    connections: Signal<Vec<(String, String)>>,
) -> AnyView {
    if fields.is_empty() {
        return ().into_any();
    }
    view! {
        <div class="form-nested">
            {fields
                .into_iter()
                .map(|field| {
                    view! {
                        <FieldEditor
                            field=field
                            prefix=prefix.clone()
                            index=index
                            values=values
                            errors=errors
                            pipelines=pipelines
                            connections=connections
                        />
                    }
                })
                .collect_view()}
        </div>
    }
    .into_any()
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
            <Account />
            <div class="zoom-level" title="scroll to zoom, drag to pan">
                {zoom}
            </div>
        </aside>
    }
}

/// Who you are signed in as, and the way out.
///
/// Absent entirely on a server with no accounts — there is nobody to be, and a
/// "signed in as nobody" chip would be noise on the setup nearly everybody
/// runs locally. The role is shown beside the name because it is the answer to
/// "why can't I see the edit button", which is otherwise a mystery.
#[component]
fn Account() -> impl IntoView {
    let session = use_context::<Session>();
    let username = move || session.and_then(Session::username);
    let role = move || session.and_then(Session::role);

    let sign_out = move || {
        leptos::task::spawn_local(async move {
            // The answer doesn't matter: the server drops the session on its
            // side and clears the cookie, and if the call fails the session is
            // no more valid than it was. Either way this tab is done with it.
            let _ = ApiClient {
                base: String::new(),
            }
            .logout()
            .await;
            if let Some(session) = session {
                session.signed_out();
            }
        });
    };

    view! {
        <Show when=move || username().is_some()>
            <div class="account">
                <span class="account-name" title=move || {
                    role().map_or_else(String::new, |role| format!("signed in as {role}"))
                }>{username}</span>
                <button
                    class="button subtle"
                    title="sign out"
                    on:click=move |_| sign_out()
                >
                    "sign out"
                </button>
            </div>
        </Show>
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

            // Hidden rather than disabled for a reader, which is the rule the
            // rest of the canvas follows: read-only really is read-only, and a
            // greyed-out button is an invitation to go and ask why. The server
            // refuses the calls regardless — this only keeps the UI from
            // offering what it would refuse.
            <Show when=move || state.may_edit>
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
            </Show>
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
            let file = name.get_untracked();
            let result = ApiClient {
                base: String::new(),
            }
            // replacing the named file is what "save as" is; the warning above
            // the button is the whole of the ceremony
            .save_config(&file, ConfigFormat::of_file_name(&file), true)
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
                    <ConfigNameFields id="save-name" name=name on_enter=Callback::new(move |()| submit()) />
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

/// A config file's name and the format it is written in, edited together.
///
/// The two are one decision shown twice: the format selection is *derived*
/// from the extension rather than held separately, and picking a format
/// rewrites the name to match, so there is no state in which the button says
/// "yaml" and the file is called `config.json`. Shared by [`SaveAsModal`] and
/// [`CreateProjectModal`], which are the same act with different framing —
/// both read the format back off the name at submit time.
#[component]
fn ConfigNameFields(
    /// Unique per dialog, so each label's `for` points at its own box.
    id: &'static str,
    name: RwSignal<String>,
    on_enter: Callback<()>,
) -> impl IntoView {
    let format = Memo::new(move |_| ConfigFormat::of_file_name(&name.get()));
    let choose = move |chosen: ConfigFormat| name.update(|n| *n = chosen.rename(n));

    view! {
        <div class="form-row">
            <label for=id>"file name"</label>
            // controlled, unlike the component fields: picking a format
            // rewrites the name, and the box has to show it
            <input
                id=id
                class="text-input"
                prop:value=move || name.get()
                placeholder="config.json"
                on:input=move |ev| name.set(event_target_value(&ev))
                on:keydown=move |ev| {
                    if ev.key() == "Enter" {
                        on_enter.run(());
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
                                title=move || format!("write the file as {option}")
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
    }
}

/// The greeting on a blank server: no config file, nothing running.
///
/// Creating a project is exactly [`SaveAsModal`]'s "create config file" — the
/// same endpoint, the same adoption — offered up front rather than two clicks
/// into edit mode, because a blank canvas with no explanation is the worst
/// first minute a new instance can have. On success it opens edit mode, the
/// way creating a project anywhere opens the editor.
///
/// Declining is "not now", never "no": it only sets
/// [`AppState::creator_dismissed`], and the empty canvas behind keeps a way
/// back in. When the dialog is offered at all is [`project::offer_creator`]'s
/// decision, tested there.
#[component]
fn CreateProjectModal() -> impl IntoView {
    let state = expect_context::<AppState>();
    let name = RwSignal::new(DEFAULT_CONFIG_NAME.to_string());
    let creating = RwSignal::new(false);
    let failure = RwSignal::new(Option::<String>::None);

    let dismiss = move || state.creator_dismissed.set(true);
    let _ = use_event_listener(use_window(), leptos::ev::keydown, move |ev| {
        if ev.key() == "Escape" {
            dismiss();
        }
    });

    let submit = move || {
        if creating.get_untracked() {
            return;
        }
        failure.set(None);
        creating.set(true);
        leptos::task::spawn_local(async move {
            let file = name.get_untracked();
            let result = ApiClient {
                base: String::new(),
            }
            // never overwrite: this dialog suggests a name into a directory
            // its user has usually never seen, so an existing file has to come
            // back as a refusal rather than as a silent replacement
            .save_config(&file, ConfigFormat::of_file_name(&file), false)
            .await;
            creating.set(false);
            match result {
                Ok(_) => {
                    // the refresh is what actually retires this dialog — the
                    // server now names a config file, so the instance is no
                    // longer blank; the dismissal only covers the moment until
                    // that answer lands
                    state.creator_dismissed.set(true);
                    state.mode.set(Mode::Edit);
                    state.refresh();
                }
                Err(err) => failure.set(Some(err.to_string())),
            }
        });
    };

    view! {
        <div class="modal-backdrop" on:click=move |_| dismiss()>
            <div class="modal narrow" on:click=move |ev| ev.stop_propagation()>
                <header>
                    <span class="modal-title">"create a new project"</span>
                    <button class="icon-button" title="not now" on:click=move |_| dismiss()>
                        "×"
                    </button>
                </header>
                <div class="modal-body">
                    <p class="modal-intro">
                        "this server has no project yet — no config file the graph is \
                         saved to. pipelines built on the canvas run immediately either \
                         way; the project is what lets them survive a restart."
                    </p>
                    <ConfigNameFields
                        id="project-name"
                        name=name
                        on_enter=Callback::new(move |()| submit())
                    />
                    <p class="form-hint">
                        {move || match state.save_directory.get() {
                            dir if dir.is_empty() => {
                                "created in the server's working directory".to_string()
                            }
                            dir => format!("created in {dir}"),
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
                    </div>
                    <button class="button" on:click=move |_| dismiss()>
                        "not now"
                    </button>
                    <button
                        class="button primary"
                        disabled=move || creating.get() || name.get().trim().is_empty()
                        on:click=move |_| submit()
                    >
                        {move || if creating.get() { "creating…" } else { "create project" }}
                    </button>
                </footer>
            </div>
        </div>
    }
}

/// Which reference the `/docs` page is showing.
///
/// A tab rather than a second route because the two are the same question asked
/// at two levels — "what can I build" and "how do I ask for it" — and someone
/// reading one wants the other a click away, not a URL away.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DocsTab {
    Components,
    /// What a pipeline can remember between batches. Its own tab rather than a
    /// fifth family on the components tab, because a bucket is not a component:
    /// nothing builds one into a pipeline, and the half of it worth reading is
    /// the rules about sharing rather than the two fields it takes.
    State,
    Http,
}

/// The reference: every component kayak can build, and every request its API
/// serves. Both generated rather than written by hand — the components by
/// reflecting over the config schemas, the endpoints from the table the router
/// itself is built from.
///
/// The docs are the same on every visit, so they're built once here and only
/// re-filtered as the search box changes.
#[component]
pub fn DocsPage() -> impl IntoView {
    let all = StoredValue::new(all_components());
    let all_endpoints = StoredValue::new(endpoints());
    let state_docs = StoredValue::new(kayak_core::docs::state_docs());
    let tab = RwSignal::new(DocsTab::Components);
    // one query per tab: a search for "nats" and a search for "409" are not the
    // same search, and carrying one into the other tab would show an empty page
    let query = RwSignal::new(String::new());
    let api_query = RwSignal::new(String::new());
    // which entry the sidebar has jumped to, per tab for the same reason the
    // queries are
    let selected = RwSignal::new(Option::<String>::None);
    let api_selected = RwSignal::new(Option::<String>::None);
    let state_selected = RwSignal::new(Option::<String>::None);
    let groups = Memo::new(move |_| all.with_value(|all| docs::groups(all, &query.get())));
    let api_groups =
        Memo::new(move |_| all_endpoints.with_value(|all| api_docs::groups(all, &api_query.get())));
    let is = move |which: DocsTab| tab.get() == which;

    view! {
        <Navbar />
        <div class="main-content">
            <div class="sidebar docs-sidebar">
                <div class="sidebar-tabs">
                    <button
                        class="tab"
                        class:active=move || is(DocsTab::Components)
                        on:click=move |_| tab.set(DocsTab::Components)
                    >
                        "components"
                    </button>
                    <button
                        class="tab"
                        class:active=move || is(DocsTab::State)
                        on:click=move |_| tab.set(DocsTab::State)
                    >
                        "state"
                    </button>
                    <button
                        class="tab"
                        class:active=move || is(DocsTab::Http)
                        on:click=move |_| tab.set(DocsTab::Http)
                    >
                        "http api"
                    </button>
                </div>
                // two `Show`s rather than one with a fallback, same as the
                // canvas sidebar: each keeps its own search and scroll position
                <Show when=move || is(DocsTab::Components)>
                    <DocsSidebar groups=groups query=query selected=selected />
                </Show>
                <Show when=move || is(DocsTab::State)>
                    <StateSidebar docs=state_docs selected=state_selected />
                </Show>
                <Show when=move || is(DocsTab::Http)>
                    <ApiSidebar groups=api_groups query=api_query selected=api_selected />
                </Show>
            </div>
            <Show when=move || is(DocsTab::Components)>
                <div class="docs-content">
                    <Show
                        when=move || docs::total(&groups.get()) != 0
                        fallback=move || {
                            view! {
                                <p class="empty">
                                    "no component matches \u{201c}" {move || query.get()} "\u{201d}"
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
            </Show>
            <Show when=move || is(DocsTab::State)>
                <div class="docs-content">
                    <StateReference docs=state_docs selected=state_selected />
                </div>
            </Show>
            <Show when=move || is(DocsTab::Http)>
                <div class="docs-content">
                    <ApiIntro />
                    <Show
                        when=move || api_docs::total(&api_groups.get()) != 0
                        fallback=move || {
                            view! {
                                <p class="empty">
                                    "no endpoint matches \u{201c}" {move || api_query.get()} "\u{201d}"
                                </p>
                            }
                        }
                    >
                        // rebuilt rather than keyed, for the same reason as the
                        // component reference above
                        {move || {
                            api_groups
                                .get()
                                .into_iter()
                                .map(|group| {
                                    view! {
                                        <section class="docs-family">
                                            <h2>{group.tag.label()}</h2>
                                            <p class="doc-description">{group.tag.description()}</p>
                                            {group
                                                .endpoints
                                                .into_iter()
                                                .map(|endpoint| {
                                                    view! {
                                                        <EndpointDoc endpoint=endpoint selected=api_selected />
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
            </Show>
        </div>
    }
}

/// The line above the endpoint list, pointing at the two things this page is a
/// readable summary *of*: the spec, and the renderer that does the full job.
#[component]
fn ApiIntro() -> impl IntoView {
    view! {
        <section class="api-intro">
            <p class="doc-description">
                "Everything below is generated from the same table the server builds its \
                 routes from, so it describes the server you are talking to. The full \
                 reference, with schemas and a request panel, is at "
                <a href="/api/reference">"/api/reference"</a>
                "; the machine-readable spec is at "
                <a href="/api/openapi.json">"/api/openapi.json"</a>
                " \u{2014} point a client generator or a contract test at that one."
            </p>
        </section>
    }
}

/// Search box plus the matching endpoints, grouped by tag.
#[component]
fn ApiSidebar(
    groups: Memo<Vec<api_docs::Group>>,
    query: RwSignal<String>,
    selected: RwSignal<Option<String>>,
) -> impl IntoView {
    view! {
        <SearchBox placeholder="search endpoints" query=query />
        // rebuilt rather than keyed, for the same reason as the reference pane
        {move || {
            groups
                .get()
                .into_iter()
                .map(|group| {
                    view! {
                        <div class="nav-group">
                            <div class="nav-group-title">{group.tag.label()}</div>
                            {group
                                .endpoints
                                .into_iter()
                                .map(|endpoint| {
                                    let anchor = endpoint.anchor_id();
                                    let on_click = anchor.clone();
                                    let method = endpoint.method;
                                    view! {
                                        <div
                                            class="tree-item endpoint-item"
                                            class:selected=move || {
                                                selected.get().as_deref() == Some(anchor.as_str())
                                            }
                                            on:click=move |_| {
                                                selected.set(Some(on_click.clone()));
                                                scroll_to(&on_click);
                                            }
                                        >
                                            <span class=format!(
                                                "method-badge {}",
                                                api_docs::method_class(method),
                                            )>{method.label()}</span>
                                            <code class="endpoint-path">{endpoint.path}</code>
                                        </div>
                                    }
                                })
                                .collect_view()}
                        </div>
                    }
                })
                .collect_view()
        }}
    }
}

/// One endpoint's entry in the reference.
#[component]
fn EndpointDoc(endpoint: ApiDoc, selected: RwSignal<Option<String>>) -> impl IntoView {
    let anchor = endpoint.anchor_id();
    let is_selected = anchor.clone();
    let method = endpoint.method;
    let params = endpoint.params.clone();
    let responses = endpoint.responses.clone();
    let request = endpoint.request;

    view! {
        <article
            class="doc-card endpoint-card"
            class:selected=move || selected.get().as_deref() == Some(is_selected.as_str())
            id=anchor
        >
            <header>
                <span class=format!(
                    "method-badge {}",
                    api_docs::method_class(method),
                )>{method.label()}</span>
                <code class="endpoint-path">{endpoint.path}</code>
                <code class="doc-tag">{endpoint.operation_id()}</code>
                // Who may call it, from the same table entry the router builds
                // its middleware from — so this badge cannot describe a server
                // that would answer differently.
                <span
                    class=format!("access-badge access-{}", endpoint.access.label())
                    title=endpoint.access.description()
                >{endpoint.access.label()}</span>
            </header>
            <p class="endpoint-summary">{endpoint.summary}</p>
            <Description text=endpoint.description.to_string() />

            {(!params.is_empty())
                .then(|| {
                    view! {
                        <div class="section-kind">"path parameters"</div>
                        <div class="field-table">
                            {params
                                .into_iter()
                                .map(|param| {
                                    view! {
                                        <div class="field">
                                            <code class="field-name">{param.name}</code>
                                            <code class="field-type">"string"</code>
                                            <span class="field-requirement">"required"</span>
                                            <div class="field-description">
                                                <Description text=param.description.to_string() />
                                            </div>
                                        </div>
                                    }
                                })
                                .collect_view()}
                        </div>
                    }
                })}

            {request
                .map(|request| {
                    view! {
                        <div class="section-kind">"request body"</div>
                        <div class="field-table">
                            <div class="field">
                                <code class="field-name">"body"</code>
                                <code class="field-type">{request.body.type_name()}</code>
                                <span class="field-requirement">"required"</span>
                                <div class="field-description">
                                    <Description text=request.description.to_string() />
                                </div>
                            </div>
                        </div>
                    }
                })}

            <div class="section-kind">"responses"</div>
            <div class="field-table">
                {responses
                    .into_iter()
                    .map(|response| {
                        view! {
                            <div class="field">
                                <code class=format!(
                                    "field-name status {}",
                                    api_docs::status_class(response.status),
                                )>{response.status}</code>
                                <code class="field-type">{response.body.type_name()}</code>
                                <span class="field-requirement optional"></span>
                                <div class="field-description">
                                    <Description text=response.description.to_string() />
                                </div>
                            </div>
                        }
                    })
                    .collect_view()}
            </div>
        </article>
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
        <>
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
        </>
    }
}

/// The state tab's sidebar: the sections, in reading order.
///
/// No search box, unlike the other two: this tab is four sections rather than a
/// list of forty things, and a box that filtered nothing would only ask to be
/// typed into.
#[component]
fn StateSidebar(
    docs: StoredValue<Vec<kayak_core::docs::StateDoc>>,
    selected: RwSignal<Option<String>>,
) -> impl IntoView {
    let sections = docs.with_value(|docs| docs::state_sections(docs));
    view! {
        <div class="nav-group">
            <div class="nav-group-title">"state"</div>
            <For each=move || sections.clone() key=|s| s.anchor.clone() let:section>
                {
                    let anchor = section.anchor.clone();
                    let on_click = section.anchor.clone();
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
                            {section.title.clone()}
                        </div>
                    }
                }
            </For>
        </div>
    }
}

/// What a pipeline can remember between batches.
///
/// Part written and part generated, and the split is the one the rest of the
/// page makes: the two config shapes are reflected out of `kayak_core::state`,
/// so a bound that grows a field shows up here on its own. The prose around
/// them is what a schema cannot say — why buckets are global, and what sharing
/// one costs.
#[component]
fn StateReference(
    docs: StoredValue<Vec<kayak_core::docs::StateDoc>>,
    selected: RwSignal<Option<String>>,
) -> impl IntoView {
    let shapes = docs.get_value();
    view! {
        <section class="docs-family" id=docs::STATE_OVERVIEW>
            <h2>"how state works"</h2>
            <p class="doc-description">
                "A "
                <strong>"state bucket"</strong>
                " is what a pipeline can remember between batches: a keyed store of values \
                 taken off messages that have gone past, readable by messages that come \
                 later. Buckets are declared once at the top of the config file, under "
                <code>"state"</code>
                ", and the pipelines that use one name it \u{2014} deliberately the shape \
                 connections already have."
            </p>
            <p class="doc-description">
                "Global rather than per-pipeline because of the case that makes state worth \
                 having at all: one pipeline remembers the recipe currently running on each \
                 machine, and six unrelated pipelines stamp it onto their output. \
                 Per-pipeline state can only answer that with six copies of the same work."
            </p>
            <p class="doc-description doc-warning">
                "The rule that is not enforced is the one to know. Two pipelines sharing a \
                 bucket are two run loops with no ordering between them, so a reader can see \
                 the value from before or after a given write depending on nothing it can \
                 observe. Ordering-sensitive correlation belongs in "
                <em>"one"</em>
                " pipeline; sharing is only for state whose value does not change on the \
                 timescale of a message. A recipe that updates hourly is safe to share. A \
                 unit id that changes every cycle is not."
            </p>
            <p class="doc-description">
                "Buckets live in memory. Their contents survive a revert \u{2014} unless that \
                 bucket's declaration changed, since what it holds may not satisfy the new \
                 limits \u{2014} and they do not survive a restart. Every bucket is bounded \
                 and there is no spelling of \u{201c}unbounded\u{201d}: past "
                <code>"max_keys"</code>
                " the least recently written key is dropped, and "
                <code>"idle_timeout_secs"</code>
                " forgets a key that has not been written for a while. Expiry is applied when \
                 a bucket is next touched rather than by a sweeper, so an idle bucket keeps \
                 its keys until something writes to it again."
            </p>
            <pre class="doc-example">
                {r#"{
  "state": {
    "machine_state": { "max_keys": 5000, "idle_timeout_secs": 3600 }
  },
  "pipelines": [
    {
      "id": "machine_cycles",
      "state": { "bucket": "machine_state", "key": "_meta.machine_id" },
      "inputs": [ ... ],
      "transforms": [ ... ],
      "outputs": [ ... ]
    }
  ]
}"#}
            </pre>
            <p class="doc-description">
                "A config file that declares no buckets stays the bare array of pipelines it \
                 always was; the two-key document above is only needed once there is \
                 something to put under "
                <code>"state"</code>
                "."
            </p>
        </section>
        <section class="docs-family">
            <h2>"declaring state"</h2>
            {shapes
                .into_iter()
                .map(|shape| view! { <StateShapeDoc shape=shape selected=selected /> })
                .collect_view()}
        </section>
        <section class="docs-family" id=docs::STATE_TRANSFORMS>
            <h2>"remember and recall"</h2>
            <p class="doc-description">
                "Nothing reads or writes a bucket except two transforms, and there are two \
                 rather than one because "
                <em>"chain order is the semantics"</em>
                ": "
                <code>"remember"</code>
                " puts what a message carries into the bucket, "
                <code>"recall"</code>
                " puts what the bucket holds onto a message, and which of them comes first \
                 decides whether a message sees its own value."
            </p>
            <p class="doc-description">
                <code>"remember"</code>
                " is a tap \u{2014} it passes its batch on unchanged. "
                <code>"recall"</code>
                " writes to the top level, so a "
                <code>"reducer"</code>
                " downstream can group by a recalled name without knowing where it came from, \
                 and it defaults "
                <code>"on_missing"</code>
                " to "
                <code>"skip"</code>
                ": every stateful pipeline has a warm-up in which nothing has been remembered \
                 yet, and failing on that would fail them all at startup."
            </p>
            <p class="doc-description">
                "Both need a "
                <code>"state"</code>
                " on their pipeline and fail to build without one. Their settings are in the \
                 components tab, under transforms."
            </p>
        </section>
        <section class="docs-family" id=docs::STATE_INSPECTING>
            <h2>"inspecting a bucket"</h2>
            <p class="doc-description">
                "The canvas sidebar's "
                <strong>"state"</strong>
                " tab lists every declared bucket with the number of keys it is holding, and \
                 a click opens a card of the keys themselves. It is a window rather than an \
                 editor, and has no "
                <code>"+"</code>
                " or delete: a bucket is part of the graph's logic, so it is declared in the \
                 config file like the pipelines that use it."
            </p>
            <p class="doc-description">
                "It polls "
                <code>"GET /api/state"</code>
                " once a second while it is open \u{2014} a bucket changes with every message, \
                 so pushing it would be a firehose for a readout nobody watches per-message. "
                <code>"GET /api/state/{bucket}"</code>
                " is one bucket's contents, a page of keys at a time. Both are in the http \
                 api tab."
            </p>
        </section>
    }
}

/// One of the two config shapes, under the same furniture a component gets: the
/// path it goes at in place of a `type` tag, its doc comment, its fields.
#[component]
fn StateShapeDoc(
    shape: kayak_core::docs::StateDoc,
    selected: RwSignal<Option<String>>,
) -> impl IntoView {
    let anchor = docs::state_anchor_id(&shape);
    let is_selected = anchor.clone();
    let description = shape.description.clone().unwrap_or_default();

    view! {
        <article
            class="doc-card"
            class:selected=move || selected.get().as_deref() == Some(is_selected.as_str())
            id=anchor
        >
            <header>
                <span class="doc-kind">{shape.title.clone()}</span>
                // where it goes in the file, in place of the `"type": "..."` a
                // component's header carries: neither shape has a tag, and the
                // path is the equivalent thing you actually type
                <code class="doc-tag">{shape.path.clone()}</code>
            </header>
            <Description text=description />
            <FieldTable fields=shape.fields.clone() />
        </article>
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
            <MetadataTable metadata=component.metadata.clone() />
        </article>
    }
}

/// What an input attaches to a message when its `envelope` is set.
///
/// Its own section rather than more rows in the settings table, because these
/// are not settings: nothing here is written in a config, and the names are what
/// a *transform* downstream will address — `_meta.subject` — rather than what an
/// input accepts. Empty for everything but an input, which is what makes this a
/// no-op on the other three families.
#[component]
fn MetadataTable(metadata: Vec<kayak_core::metadata::MetaFieldDoc>) -> impl IntoView {
    if metadata.is_empty() {
        return ().into_any();
    }
    view! {
        <div class="doc-metadata">
            <div class="section-kind">"metadata"</div>
            <p class="doc-metadata-note">
                "attached under the "
                <code>"envelope"</code>
                "'s field, e.g. "
                <code>"_meta.received_at"</code>
            </p>
            <div class="field-table">
                <For each=move || metadata.clone() key=|f| f.name.clone() let:field>
                    <div class="field">
                        <code class="field-name">{field.name.clone()}</code>
                        <div class="field-description">
                            <Description text=field.description.clone() />
                        </div>
                    </div>
                </For>
            </div>
        </div>
    }
    .into_any()
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
fn LogRow(
    entry: log::Entry,
    names: StoredValue<log::ComponentNames>,
    /// Which rows are showing their payload, by entry id. Held by the log
    /// rather than the row so it survives the flat/grouped switch and the
    /// wholesale rebuild an update causes — the same reason the open passes
    /// are kept there.
    expanded: RwSignal<HashSet<u64>>,
    /// Stop the log. Expanding is reading, and a row that scrolls away while
    /// being read is the thing this feature exists to fix, so opening one
    /// pauses. Handed down as a callback because pausing is also what the
    /// bar's button does, counter reset included.
    pause: Callback<()>,
) -> impl IntoView {
    let state = expect_context::<AppState>();
    let (id, ts, stage) = (entry.id, entry.ts, entry.stage);
    let is_error = entry.is_error();
    let text =
        names.with_value(|names| log::summary(&entry, names.name(entry.stage, entry.component)));
    let is_open = move || expanded.with(|open| open.contains(&id));

    // The payload is *not* laid out here: this component is built for every
    // visible row on every update, and pretty-printing two hundred batches to
    // show none of them is exactly the kind of per-row work the feed's whole
    // design is about avoiding. `<Show>` builds its children when it opens, so
    // the box below is where the work happens and it happens once.
    let kind = StoredValue::new(entry.kind);

    view! {
        // Every class here is prefixed, and that is not just tidiness: the
        // "add pipeline" modal has a `.stage` of its own — a group of
        // components, nothing to do with this — and a badge that borrowed the
        // name silently picked up its 12px top margin.
        <div class="log-row" class:error=is_error class:open=is_open>
            // Always in the DOM and only ever changing opacity: a strip that
            // appeared on hover would have to be laid out on hover too, and a
            // column that comes and goes moves every payload sideways as the
            // pointer crosses the log.
            <button
                class="log-expand"
                title=move || if is_open() { "collapse" } else { "expand this message" }
                on:click=move |ev| {
                    ev.stop_propagation();
                    let opening = !is_open();
                    expanded.update(|open| { if !open.remove(&id) { open.insert(id); } });
                    if opening {
                        pause.run(());
                    }
                }
            >
                {move || if is_open() { "▾" } else { "▸" }}
            </button>
            <span class="log-time">{move || log::format_time(ts, state.tz_offset.get())}</span>
            <span class="log-stage" title=stage.as_str()>{log::stage_label(stage)}</span>
            // the full payload on hover: a card is 18 cells wide and a batch is
            // not
            <span class="log-text" title=text.clone()>{text.clone()}</span>
        </div>
        <Show when=is_open>
            <ExpandedEntry kind />
        </Show>
    }
}

/// What one row holds, opened out: every message of the batch, laid out and
/// coloured, each of them copyable on its own.
///
/// Every message rather than the first, because the row above already shows the
/// first — a batch of five hundred is summarised as `500 msgs · {first}`, and
/// an expansion that showed the same message again would answer a question
/// nobody had. What it can show is bounded by what the feed carries
/// (`kayak_core::MESSAGES_PER_BATCH`), and it says so when the batch was wider.
///
/// Built only while it is open: `<Show>` unmounts it on collapse, which is what
/// keeps [`crate::pretty`] off the path of an ordinary log update.
#[component]
fn ExpandedEntry(kind: StoredValue<log::EntryKind>) -> impl IntoView {
    let UseClipboardReturn { copy, copied, .. } = use_clipboard();
    let copy_all = copy.clone();

    let (messages, dropped) = kind.with_value(|kind| match kind {
        log::EntryKind::Batch { messages, dropped } => (pretty::render_all(messages), *dropped),
        // an error has no batch, and its text is the one thing a row truncates
        // that there is nowhere else to read
        log::EntryKind::Error(message) => (vec![pretty::Rendered::Plain(message.clone())], 0),
    });
    let shown = messages.len();
    let all_text = pretty::all_to_text(&messages);

    view! {
        // A press here must not reach the canvas behind the card: the box is
        // several rows tall and is dragged across while text in it is selected.
        <div class="log-expanded" on:mousedown=move |ev: leptos::ev::MouseEvent| ev.stop_propagation()>
            <div class="log-expanded-bar">
                <span class="log-expanded-count">
                    {if dropped > 0 {
                        format!("{shown} of {} messages", shown + dropped)
                    } else if shown == 1 {
                        "1 message".to_string()
                    } else {
                        format!("{shown} messages")
                    }}
                </span>
                <button
                    class="clear"
                    title="copy this message to the clipboard"
                    on:click=move |ev| {
                        ev.stop_propagation();
                        copy_all(&all_text);
                    }
                >
                    {move || if copied.get() { "copied" } else { "copy" }}
                </button>
            </div>
            {messages
                .into_iter()
                .enumerate()
                .map(|(i, message)| {
                    let copy_one = copy.clone();
                    let text = message.to_text();
                    view! {
                        <div class="log-message">
                            // Only numbered when there is more than one: an
                            // ordinal on a batch of one is a column of noise.
                            <Show when=move || { shown > 1 }>
                                <div class="log-message-bar">
                                    <span class="log-message-index">{format!("{}", i + 1)}</span>
                                    <button
                                        class="clear"
                                        title="copy this one message"
                                        on:click={
                                            let copy_one = copy_one.clone();
                                            let text = text.clone();
                                            move |ev: leptos::ev::MouseEvent| {
                                                ev.stop_propagation();
                                                copy_one(&text);
                                            }
                                        }
                                    >
                                        "copy"
                                    </button>
                                </div>
                            </Show>
                            <MessageBody message />
                        </div>
                    }
                })
                .collect_view()}
            // What the feed left behind, said where it is noticed: the box
            // showing twenty of a five-hundred message batch without saying so
            // would read as a pipeline that moved twenty.
            <Show when=move || { dropped > 0 }>
                <div class="log-expanded-note">
                    {format!("{dropped} more not carried by the event feed")}
                </div>
            </Show>
        </div>
    }
}

/// One message: laid-out JSON, or the text as it stands when it isn't JSON.
#[component]
fn MessageBody(message: pretty::Rendered) -> impl IntoView {
    match message {
        pretty::Rendered::Json(lines) => view! {
            <pre class="log-json">
                {lines
                    .into_iter()
                    .map(|line| {
                        let indent = line.indent();
                        view! {
                            <div class="json-line">
                                <span class="json-indent">{indent}</span>
                                {line
                                    .spans
                                    .into_iter()
                                    .map(|span| view! { <span class=span.kind.class()>{span.text}</span> })
                                    .collect_view()}
                            </div>
                        }
                    })
                    .collect_view()}
            </pre>
        }
        .into_any(),
        // `pre` on both arms: a truncated payload is still a payload, and
        // reflowing it as prose loses whatever structure survived the cut.
        pretty::Rendered::Plain(text) => {
            view! { <pre class="log-json plain">{text}</pre> }.into_any()
        }
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
    /// Which passes are open, by [`log::Pass::key`].
    expanded: RwSignal<HashSet<u64>>,
    /// Which *rows* are showing their payload, by entry id. A different set
    /// from `expanded` and deliberately so: opening a pass is navigating the
    /// log, opening a row is reading one message, and closing the pass over a
    /// row that was being read shouldn't throw that away.
    rows: RwSignal<HashSet<u64>>,
    pause: Callback<()>,
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
                        .map(|entry| view! { <LogRow entry names expanded=rows pause /> })
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
    /// Whether new events are being kept. Owned by the card, which is where the
    /// stream arrives.
    paused: RwSignal<bool>,
    /// How many events have gone past since it was paused.
    skipped: RwSignal<usize>,
) -> impl IntoView {
    let state = expect_context::<AppState>();
    let body_ref = NodeRef::<leptos::html::Div>::new();
    let UseClipboardReturn { copy, copied, .. } = use_clipboard();

    // Flat by default: every event in the order it arrived, which is what a log
    // is expected to be. Grouping is the view you switch to when the question
    // is what one batch did rather than what just happened.
    let grouped = RwSignal::new(false);
    let expanded = RwSignal::new(HashSet::<u64>::new());
    // Which rows are showing their payload. Kept here rather than on the row so
    // it outlives the rebuild every update causes, and so it is one set across
    // both views — a row opened in the flat view is still open when the log is
    // switched to grouped.
    let expanded_rows = RwSignal::new(HashSet::<u64>::new());

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

    // Following the tail costs a **forced synchronous layout**: reading
    // `scroll_height` makes the browser lay the pane out there and then, and a
    // log holds a couple of hundred rows. Doing it inline on every update was
    // worth about a quarter of the main thread's blocked time under load, so it
    // is deferred to the next frame — by which point the rows this update added
    // are in the DOM anyway, which is the moment the measurement is actually
    // wanted.
    //
    // Coalesced on a flag rather than queued: several updates landing before
    // the frame runs should scroll to the bottom once, not once each.
    let scroll_queued = StoredValue::new(false);
    let follow_the_tail = move || {
        if scroll_queued.get_value() {
            return;
        }
        scroll_queued.set_value(true);
        request_animation_frame(move || {
            scroll_queued.set_value(false);
            scroll_to_end();
        });
    };

    Effect::new(move |_| {
        messages.track();
        filter.track();
        if following.get_untracked() {
            follow_the_tail();
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

    // Stopping the log, however it was asked for: the bar's button, or a row
    // being opened to read it. Idempotent, because expanding a second row on an
    // already-paused log must not reset the counter of what has gone past.
    let pause_log = Callback::new(move |()| {
        if !paused.get_untracked() {
            paused.set(true);
            skipped.set(0);
        }
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
                // Pausing stops the log, not the pipeline — which is why it
                // says what went past rather than pretending nothing did.
                <button
                    class="clear"
                    class:active=move || paused.get()
                    title=move || {
                        if paused.get() {
                            format!(
                                "resume — the pipeline kept running, {} events went past",
                                skipped.get(),
                            )
                        } else {
                            "stop keeping new events, so this log can be read".to_string()
                        }
                    }
                    on:click=move |_| {
                        paused.update(|p| *p = !*p);
                        if paused.get_untracked() {
                            skipped.set(0);
                        } else {
                            // catch up with what did arrive, which is where the
                            // reader was before they paused
                            following.set(true);
                            scroll_to_end();
                        }
                    }
                >
                    {move || if paused.get() { "paused" } else { "pause" }}
                </button>
                // What the filter left, not everything the card holds: the copy
                // of a log you are reading is the log you are reading.
                <button
                    class="clear"
                    title="copy these lines to the clipboard"
                    on:click=move |_| {
                        let text = names
                            .with_value(|names| {
                                log::as_text(
                                    &visible_entries.get_untracked(),
                                    names,
                                    state.tz_offset.get_untracked(),
                                )
                            });
                        copy(&text);
                    }
                >
                    {move || if copied.get() { "copied" } else { "copy" }}
                </button>
                <button
                    class="clear"
                    title="clear this log"
                    on:click=move |_| {
                        messages.update(log::Log::clear);
                        // the rows those ids belonged to are gone; a set that
                        // outlived them would re-open whichever entries happen
                        // to be handed the same ids next
                        expanded_rows.update(HashSet::clear);
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
                // A wheel over a log that has somewhere to scroll is that log's,
                // and the canvas never hears it. The canvas' handler is a
                // `prevent_default` that zooms, so leaving the event to bubble
                // is what stopped the browser scrolling this pane at all.
                //
                // Whether there is anything to scroll is the whole test, rather
                // than which way the wheel went: chaining on to the zoom at the
                // end of a log turns overscrolling one card into a lurch of the
                // whole canvas. A log with nothing to scroll isn't a scrollable
                // pane at all, so that one falls through and zooms.
                on:wheel=move |ev| {
                    if let Some(el) = body_ref.get_untracked()
                        && el.scroll_height() > el.client_height()
                    {
                        ev.stop_propagation();
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
                        <PassView pass names expanded rows=expanded_rows pause=pause_log />
                    </For>
                </Show>
                <Show when=move || !grouped.get()>
                    <For each=move || visible_entries.get() key=|entry| entry.id let:entry>
                        <LogRow entry names expanded=expanded_rows pause=pause_log />
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

/// One of a card's three collapsible parts: a heading that toggles it, and the
/// part itself when it is open.
///
/// The body is genuinely unmounted rather than hidden, which is what makes the
/// collapse worth having: a shut log is not a two-hundred row list with
/// `display: none` on it, and a shut chart runs no memo over the clock. What the
/// feed does about it is the other half — see [`CardSink`].
///
/// `children` is a `ChildrenFn` because `<Show>` rebuilds them every time it
/// opens; a section body therefore has to be buildable more than once, which is
/// why the card stores its config rather than moving it in.
#[component]
fn CardSection(
    /// What the heading says, and what the toggle announces.
    name: &'static str,
    /// Distinguishes the three in CSS — a resized card gives its spare height to
    /// the log and to nothing else.
    class: &'static str,
    open: RwSignal<bool>,
    children: ChildrenFn,
) -> impl IntoView {
    view! {
        <div class=format!("card-section {class}") class:open=move || open.get()>
            <button
                class="section-head"
                // the heading is inside the card, so a press on it must not
                // reach the canvas' pan handler behind it
                on:mousedown=move |ev| ev.stop_propagation()
                on:click=move |_| open.update(|o| *o = !*o)
                title=move || {
                    if open.get() {
                        format!("hide {name}")
                    } else {
                        format!("show {name}")
                    }
                }
            >
                <span class="chevron">{move || if open.get() { "▾" } else { "▸" }}</span>
                <span class="section-name">{name}</span>
            </button>
            <Show when=move || open.get()>{children()}</Show>
        </div>
    }
}

/// A card's throughput: one bar pair per time unit, rolling right to left.
///
/// The whole chart is **two `<path>` elements**, one per series, and that is
/// what makes it cheap enough to redraw on every card once a second: a frame is
/// two attribute writes rather than a hundred and twenty elements reconciled.
/// The `viewBox` is a fixed 100×100 at `preserveAspectRatio: none`, so a
/// maximized card gets a wider chart rather than a scaled-up one.
#[component]
fn ThroughputChart(stats: RwSignal<stats::Stats>, reseed: RwSignal<u32>) -> impl IntoView {
    let state = expect_context::<AppState>();

    // Redrawn on the clock as well as on the events, which is what makes it
    // *roll*: a pipeline that has stopped sends nothing, and a chart that only
    // moved when something arrived would freeze with the last burst pinned to
    // the right-hand edge. One memo, read three times below.
    let bars = Memo::new(move |_| {
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let now = state.now.get().max(0.0) as u64;
        stats.with(|stats| stats.bars(now))
    });
    let peak = Memo::new(move |_| stats::Stats::peak(&bars.read()));
    // Bars are scaled against the rounded ceiling, not the raw peak, so they
    // agree with the gridlines drawn over them — see `stats::axis_ceiling`.
    let ceiling = Memo::new(move |_| stats::axis_ceiling(peak.get()));
    let paths = Memo::new(move |_| stats::bar_paths(&bars.read(), ceiling.get()));
    // Failures get their own peak and their own ceiling. Sharing the
    // throughput's would draw three failures next to fifty thousand messages as
    // nothing at all, which is the one reading the strip exists to prevent.
    let error_peak = Memo::new(move |_| stats::Stats::error_peak(&bars.read()));
    let error_ceiling = Memo::new(move |_| stats::axis_ceiling(error_peak.get()));
    let error_path = Memo::new(move |_| stats::error_path(&bars.read(), error_ceiling.get()));

    view! {
        <div class="chart">
            <div class="chart-bar">
                <span class="series in">"in"</span>
                <span class="series out">"out"</span>
                // Only keyed when there is something to key: a permanent
                // "err" legend on a healthy pipeline reads as a series that is
                // sitting at zero, which is a different claim from a pipeline
                // that has not failed.
                <Show when=move || { error_peak.get() > 0 }>
                    <span class="series err">"err"</span>
                </Show>
                <span class="units">
                    {stats::Unit::ALL
                        .into_iter()
                        .map(|unit| {
                            view! {
                                <button
                                    class="chip"
                                    class:active=move || stats.with(|s| s.unit() == unit)
                                    title=unit.window_label()
                                    on:click=move |_| {
                                        stats.update(|s| s.set_unit(unit));
                                        // `set_unit` empties the chart — see
                                        // its docs on why it can't re-bucket —
                                        // so this is the other moment there is
                                        // something for the server to seed.
                                        reseed.update(|n| *n = n.wrapping_add(1));
                                    }
                                >
                                    {unit.label()}
                                </button>
                            }
                        })
                        .collect_view()}
                </span>
            </div>
            <div class="chart-plot">
                <svg
                    class="chart-svg"
                    viewBox="0 0 100 100"
                    preserveAspectRatio="none"
                    // Labelled rather than titled: `leptos_meta` claims `<title>`
                    // for the document's, so an SVG one renames the browser tab.
                    aria-label="messages in and out per time unit"
                >
                    <path class="in" d=move || paths.get().0 />
                    <path class="out" d=move || paths.get().1 />
                </svg>
                // The axis is HTML laid over the plot, not SVG inside it, and
                // that is forced rather than chosen: the chart is drawn at
                // `preserveAspectRatio: none`, so an SVG `<text>` would be
                // stretched horizontally by whatever the card's width happens
                // to be. The lines could live in the path; the labels cannot,
                // and splitting them would be two coordinate systems to keep in
                // step.
                <div class="chart-axis" aria-hidden="true">
                    {move || {
                        let exact = peak.get();
                        stats::axis_marks(ceiling.get())
                            .into_iter()
                            .map(|(percent, label)| {
                                view! {
                                    <div
                                        class="axis-mark"
                                        style:top=format!("{percent}%")
                                        // the rounded number is the readable
                                        // one; the exact peak is still the
                                        // answer to "how high did it actually
                                        // get", so it lives on the hover
                                        title=format!("highest in this window: {exact}")
                                    >
                                        <span class="axis-label">{label}</span>
                                    </div>
                                }
                            })
                            .collect_view()
                    }}
                </div>
                <Show when=move || peak.get() == 0>
                    <div class="empty">"waiting for messages…"</div>
                </Show>
            </div>
            // Failures, on a strip of their own beneath the plot. Always
            // present so that opening a card never shifts the layout under the
            // pointer, and empty — just its baseline — until something fails.
            <div class="chart-errors" class:quiet=move || error_peak.get() == 0>
                <svg
                    class="chart-svg"
                    viewBox="0 0 100 100"
                    preserveAspectRatio="none"
                    aria-label="failures per time unit"
                >
                    <path class="err" d=move || error_path.get() />
                </svg>
                <Show when=move || { error_peak.get() > 0 }>
                    <span
                        class="error-peak"
                        title="the most failures in any one unit of this window"
                    >
                        {move || stats::compact(error_peak.get())}
                    </span>
                </Show>
            </div>
        </div>
    }
}

/// What has failed, from the server's record rather than from the live feed.
///
/// This is the half of a card that answers the question the log cannot: the
/// event stream is only produced while a browser is attached, so a pipeline
/// that broke at 02:14 and was still broken at 08:00 has nothing in its log and
/// everything here. Aggregated by the server to one row per distinct failure —
/// a broker that was down for six hours is one row with a tally, not two
/// million lines to scroll.
///
/// Silent when there is nothing to say: an empty list renders nothing at all
/// rather than a reassuring "no failures", because history can be turned off
/// and an empty state that looks the same either way would be a lie in the
/// direction that matters.
#[component]
fn FailureHistory(
    failures: RwSignal<Vec<ErrorSignature>>,
    names: StoredValue<log::ComponentNames>,
) -> impl IntoView {
    let state = expect_context::<AppState>();
    view! {
        <Show when=move || !failures.read().is_empty()>
            <div class="failures">
                <div class="failures-head">"failures on record"</div>
                <ul class="failure-list">
                    {move || {
                        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
                        let now = state.now.get().max(0.0) as u64;
                        let tz = state.tz_offset.get();
                        failures
                            .get()
                            .into_iter()
                            .map(|failure| {
                                let component = names
                                    .with_value(|names| {
                                        names
                                            .name(failure.stage, failure.component)
                                            .map(ToOwned::to_owned)
                                    })
                                    .map_or_else(
                                        || failure.stage.as_str().to_string(),
                                        |name| format!("{} {name}", failure.stage),
                                    );
                                // The whole story on hover: a row shows the
                                // latest occurrence because that is what says
                                // "still happening", and the first one is what
                                // says when it started.
                                let detail = format!(
                                    "{} occurrence(s)\nfirst {}\nlast {}",
                                    failure.count,
                                    log::format_time(failure.first_seen, tz),
                                    log::format_time(failure.last_seen, tz),
                                );
                                view! {
                                    <li class="failure" title=detail>
                                        <span class="failure-when">
                                            {log::format_time(failure.last_seen, tz)}
                                        </span>
                                        <span class="failure-age">
                                            {log::format_age(now, failure.last_seen)}
                                        </span>
                                        <span class="failure-where">{component}</span>
                                        <span class="failure-text">{failure.message}</span>
                                        // Only when it happened more than once:
                                        // a "×1" on every row is noise, and the
                                        // number is here to make a repeat stand
                                        // out.
                                        <Show when={
                                            let count = failure.count;
                                            move || count > 1
                                        }>
                                            <span class="failure-count">
                                                {format!("×{}", failure.count)}
                                            </span>
                                        </Show>
                                    </li>
                                }
                            })
                            .collect_view()
                    }}
                </ul>
            </div>
        </Show>
    }
}

#[component]
pub fn Card(pipeline_id: PipelineId, config: Config, status: RunStatus) -> impl IntoView {
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
    // Stored rather than moved, because the config section is behind a `<Show>`
    // and its children are rebuilt every time it is opened — the config of a
    // running pipeline never changes, so this is the same one every time.
    let config = StoredValue::new(config);
    let id = pipeline_id.clone();

    let maximized_id = pipeline_id.clone();
    let is_maximized =
        Memo::new(move |_| canvas.maximized.with(|m| m.as_ref() == Some(&maximized_id)));

    // Paused here rather than in `MessageLog`, because this is where the stream
    // reaches the log and pausing it anywhere further down would only be
    // hiding rows that had already been kept.
    let paused = RwSignal::new(false);
    // How much went past while paused — the answer to "is it stuck, or am I
    // just not watching". Counted here for the same reason.
    let skipped = RwSignal::new(0_usize);
    let stats = RwSignal::new(stats::Stats::default());

    // Which of the three sections are open. In memory and only here: which part
    // of a card someone is reading is a property of this browser tab, like
    // `maximized` and unlike the arrangement — writing it to the layout file
    // would commit one reader's habits to the repository.
    //
    // The log starts shut because it is the expensive one and the one you go
    // looking for; the config and the chart are what a card is *for* at a
    // glance.
    let config_open = RwSignal::new(true);
    let stats_open = RwSignal::new(true);
    let log_open = RwSignal::new(false);

    // A collapsed chart is not fed, so what it holds is a picture of a window
    // with a hole in it — and a hole in a bar chart reads as an idle pipeline,
    // which is the one thing it must never say by accident. Emptying it on the
    // way down is what makes "this is what has happened since you opened it"
    // true, and it gives the memory back for as long as nobody is looking.
    Effect::new(move |_| {
        if !stats_open.get() {
            stats.update(stats::Stats::clear);
        }
    });

    // Bumped whenever the chart has just been emptied and could be seeded from
    // the server's record: opening it, and changing the unit. A nonce rather
    // than tracking `stats` itself, which changes on every batch and would turn
    // one fetch into a request per event.
    let reseed = RwSignal::new(0_u32);
    // What has failed, aggregated by the server — one entry per distinct
    // failure with a first-seen and a tally. This is the half of a card that
    // survives nobody being here to watch: the log is fed by `/events`, which
    // is only produced while a browser is attached, so a pipeline that broke at
    // 02:14 has nothing to show in it at 08:00.
    let failures = RwSignal::new(Vec::<ErrorSignature>::new());
    let history_id = StoredValue::new(pipeline_id.clone());
    Effect::new(move |_| {
        reseed.track();
        if !stats_open.get() {
            return;
        }
        // Untracked: the unit is read to *choose a ring*, and subscribing to
        // the stats here would refetch on every message the card counts.
        let unit = stats.with_untracked(stats::Stats::unit);
        let id = history_id.get_value();
        leptos::task::spawn_local(async move {
            let Ok(history) = (ApiClient {
                base: String::new(),
            })
            .pipeline_history(&id, unit.resolution())
            .await
            else {
                // A server too old to have the endpoint, or one with history
                // turned off. Neither is worth a message on the card: the chart
                // simply fills from the live feed as it always did.
                return;
            };
            // `backfill` refuses a chart that is already counting, so a slow
            // response arriving after the feed has delivered something is a
            // no-op rather than a double count.
            stats.update(|stats| stats.backfill(history.bucket_secs, &history.buckets));
            failures.set(history.errors);
        });
    });

    // The card doesn't watch the feed; the feed writes to the card. That is the
    // difference between one map lookup per event and one effect per card per
    // event — see [`Feed`]. The registration lasts as long as the component.
    state.feed.register(
        id,
        CardSink {
            log: messages,
            log_open,
            paused,
            skipped,
            stats,
            stats_open,
        },
    );

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

    // What a press on the card does to the selection, wherever on the card it
    // lands. Three cases, and the middle one is the one that makes dragging a
    // group possible at all:
    //
    // - shift adds this card to the selection, or takes it back out;
    // - a press on a card that is *already* selected leaves the selection
    //   alone, so a group can be grabbed by any of its members — collapsing to
    //   the one pressed would make it impossible to drag the rest;
    // - anything else means this card and nothing else.
    //
    // Shift is edit-mode only. Read-only selects to look at one pipeline, and a
    // set of them there would be a state with nothing to do.
    let press_selects = move |ev: &leptos::ev::MouseEvent| {
        let id = stored_id.get_value();
        if ev.shift_key() && state.editing() {
            // A card's config and log are selectable text — that is deliberate,
            // it is where a broker url gets copied out of. Shift-click there is
            // the browser's "extend the selection to here" as well as ours, and
            // the two happening at once paints half the card blue. Ours wins.
            ev.prevent_default();
            canvas.toggle_selected(&id);
        } else if !canvas.is_selected(&id) {
            canvas.select_only(&id);
        }
    };

    let grab = move |ev: leptos::ev::MouseEvent, grab: Grab| {
        if ev.button() != 0 {
            return;
        }
        ev.prevent_default();
        // the press must not reach the card's own selection handler *and* the
        // canvas behind it; selecting is done here instead, so grabbing a card
        // selects it exactly as clicking into it does
        ev.stop_propagation();
        press_selects(&ev);
        // A shift-press that took this card *out* of the selection has said
        // what it meant; dragging the card it just deselected would undo the
        // gesture in the same movement.
        if !canvas.is_selected(&stored_id.get_value()) {
            return;
        }
        canvas.interrupt_focus();
        let start = position.get_untracked();
        let pinned = pinned_height.get_untracked();
        let cards = match grab {
            // A resize is one card's own height and width: there is no reading
            // of dragging six of them from one corner.
            Grab::Resize => vec![DragCard {
                id: stored_id.get_value(),
                start,
                pinned_height: pinned,
            }],
            Grab::Move => canvas.moving_cards(&stored_id.get_value(), start, pinned),
        };
        canvas.dragging.set(Some(Dragging {
            grab,
            origin: (f64::from(ev.client_x()), f64::from(ev.client_y())),
            cards,
        }));
    };

    let selected_id = pipeline_id.clone();
    let is_selected = Memo::new(move |_| canvas.selected.with(|s| s.contains(selected_id.as_str())));

    let dragging_id = pipeline_id.clone();
    let is_dragging = Memo::new(move |_| {
        canvas
            .dragging
            .with(|d| d.as_ref().is_some_and(|d| d.moves(&dragging_id)))
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
            class:selected=move || is_selected.get()
            node_ref=card_ref
            // Anywhere on the card, in both modes: a press is how you say which
            // pipeline you mean, and it has to work while reading the log as
            // well as on the title bar. Mousedown rather than click so that a
            // drag — which never produces a click — selects too; the header's
            // own handler stops propagation and calls this itself. The camera
            // is deliberately *not* moved: this card is already on screen, and
            // gliding it to the middle under the pointer would be a fight.
            on:mousedown=move |ev| {
                if ev.button() == 0 {
                    press_selects(&ev);
                }
            }
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
                // Only when there is something to say. A pipeline that is
                // running is the overwhelming majority and a badge reading
                // "running" on every card would be nine words of chrome
                // saying nothing — while a pipeline whose run loop has ended
                // looks, without this, exactly like a quiet one.
                <Show when=move || !status.is_running()>
                    <span
                        class="card-status"
                        class:starting=move || status == RunStatus::Starting
                        title=match status {
                            RunStatus::Starting => {
                                "an output is not ready yet — retrying, and the pipeline will \
                                 start on its own once it connects"
                            }
                            RunStatus::Stopped => "this pipeline's run loop was stopped",
                            _ => {
                                "this pipeline's run loop has ended — its input died, and \
                                 nothing will come out of it until it is rebuilt"
                            }
                        }
                    >
                        {status.as_str()}
                    </span>
                </Show>
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
            <CardSection name="config" class="section-config" open=config_open>
                <Inspector config=config.get_value() />
            </CardSection>
            <CardSection name="stats" class="section-stats" open=stats_open>
                <ThroughputChart stats reseed />
                <FailureHistory failures names />
            </CardSection>
            <CardSection name="logs" class="section-logs" open=log_open>
                <MessageLog messages filter names paused skipped />
            </CardSection>
            // Downstream is *down*, so the handle for it sits on the bottom
            // edge — the face `sides_between` will route the new card's edge
            // out of. Left rather than centre because the log's "jump to
            // latest" already lives in the middle of that edge.
            <Show when=move || state.editing() && !is_maximized.get()>
                <button
                    class="card-spawn"
                    title="add a pipeline fed by this one"
                    aria-label="add a pipeline fed by this one"
                    // the press must not reach the canvas behind the card
                    on:mousedown=move |ev| ev.stop_propagation()
                    on:click=move |ev| {
                        ev.stop_propagation();
                        state.open_add(Some(stored_id.get_value()));
                    }
                >
                    "+"
                </button>
            </Show>
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

/// The control a [`FieldType::Script`] field renders as.
///
/// Three things stacked in one box — a line gutter, the highlighted source, and
/// the textarea somebody actually types into — plus a pane that runs the script
/// against real messages. The layering is the part worth understanding:
///
/// **The `<textarea>` is on top and transparent, and the highlighted spans sit
/// behind it.** There is no way to colour text inside a textarea, and a
/// `contenteditable` div is the other approach — one that reimplements
/// selection, undo and IME badly. So the box keeps the browser's own text
/// editing whole, and the colour comes from a `<pre>` underneath holding
/// character-for-character the same text. That is why
/// [`crate::rhai::highlight`] must reproduce its input exactly: a dropped space
/// slides the colours out from under the caret.
///
/// **The textarea is still uncontrolled**, the same rule every other box in
/// this form follows — its value is set once, and rebuilding it while somebody
/// types would destroy what they were typing. The overlay is a separate signal
/// that `on:input` writes, so the colours update on every keystroke without the
/// textarea being touched at all.
#[component]
fn ScriptEditor(
    /// The path this field sits at, which is what the draft is keyed by.
    at: String,
    /// What was in the box when it was built. Read once, for the reason above.
    initial: String,
    values: RwSignal<HashMap<String, String>>,
    errors: RwSignal<Vec<form::FormError>>,
    index: usize,
) -> impl IntoView {
    // What the overlay renders. Separate from the draft map so a keystroke
    // touches one signal rather than the whole form's.
    let source = RwSignal::new(initial.clone());
    let editor = NodeRef::<leptos::html::Textarea>::new();

    let write_at = at.clone();
    let cleared = at.clone();
    let write = move |ev: leptos::ev::Event| {
        let value = event_target_value(&ev);
        values.update(|v| {
            v.insert(write_at.clone(), value.clone());
        });
        errors.update(|errors| form::clear_field_error(errors, index, &cleared));
        source.set(value);
    };

    // Tab is a character here, not a way out of the field. That is a real
    // accessibility trade — it takes the keyboard exit away — so shift-tab is
    // left alone as the way out, and escape still closes the modal.
    let indent = move |ev: leptos::ev::KeyboardEvent| {
        if ev.key() != "Tab" || ev.shift_key() {
            return;
        }
        ev.prevent_default();
        let Some(area) = editor.get() else { return };
        let text = area.value();
        let start = area.selection_start().ok().flatten().unwrap_or(0) as usize;
        let end = area.selection_end().ok().flatten().unwrap_or(0) as usize;
        // byte offsets from a textarea are UTF-16 code units; clamp through
        // char_indices so a multi-byte character can't split the string
        let split = |at: usize| {
            text.char_indices()
                .nth(at)
                .map_or(text.len(), |(byte, _)| byte)
        };
        let (start, end) = (split(start), split(end));
        let mut next = String::with_capacity(text.len() + 2);
        next.push_str(&text[..start]);
        next.push_str("  ");
        next.push_str(&text[end..]);
        area.set_value(&next);
        let caret = text[..start].chars().count() + 2;
        let _ = area.set_selection_start(Some(caret as u32));
        let _ = area.set_selection_end(Some(caret as u32));
        values.update(|v| {
            v.insert(at.clone(), next.clone());
        });
        source.set(next);
    };

    view! {
        <div class="script-editor">
            <div class="script-surface">
                <div class="script-gutter" aria-hidden="true">
                    {move || {
                        let count = source.get().lines().count().max(1);
                        (1..=count)
                            .map(|n| view! { <div>{n}</div> })
                            .collect_view()
                    }}
                </div>
                <div class="script-code">
                    // aria-hidden because the textarea over it carries the same
                    // text and is the thing a screen reader should read; two
                    // copies would be read twice
                    <pre class="script-highlight" aria-hidden="true">
                        {move || {
                            crate::rhai::highlight(&source.get())
                                .into_iter()
                                .map(|line| {
                                    view! {
                                        <div class="script-line">
                                            {line
                                                .into_iter()
                                                .map(|span| {
                                                    view! {
                                                        <span class=span.kind.class()>{span.text}</span>
                                                    }
                                                })
                                                .collect_view()}
                                        </div>
                                    }
                                })
                                .collect_view()
                        }}
                    </pre>
                    <textarea
                        node_ref=editor
                        class="script-input"
                        spellcheck="false"
                        autocapitalize="off"
                        aria-label="rhai script"
                        prop:value=initial
                        on:input=write
                        on:keydown=indent
                    />
                </div>
            </div>
            <TryIt source=source />
        </div>
    }
}

/// The pane under the script editor: run this script over some messages and see
/// what comes out.
///
/// This is what makes writing a script in the UI viable at all. Every other
/// component in this form is declarative — a filled-in form either builds or
/// says which box is wrong — while a script is code, and the only way to find
/// out what code does is to run it. Without this the loop is "create the
/// pipeline, watch the card, delete the pipeline".
///
/// It goes through `POST /api/scripts/dry-run` rather than running rhai in the
/// browser. rhai does compile to wasm and the frontend is already wasm, so it
/// was available — but that would be a second interpreter at a second version
/// with a second set of features, and a script that passes here and fails on
/// the server is the worst failure this feature can have.
#[component]
fn TryIt(source: RwSignal<String>) -> impl IntoView {
    // The messages to run over, as text: this is a scratch pad, so what is in
    // it is whatever somebody pasted out of a card's log — which may well not
    // be valid JSON yet.
    let messages = RwSignal::new("[{\"value\": 1}]".to_string());
    let outcome = RwSignal::new(None::<Result<DryRunResponse, String>>);
    let running = RwSignal::new(false);
    let open = RwSignal::new(false);

    let run = move |_| {
        let parsed = serde_json::from_str::<Vec<serde_json::Value>>(&messages.get_untracked());
        let messages = match parsed {
            Ok(messages) => messages,
            // Caught here rather than sent, because the server would answer
            // this with a deserialization error about the whole request body
            // and the fault is in one box of it.
            Err(err) => {
                outcome.set(Some(Err(format!("the messages are not a JSON array: {err}"))));
                return;
            }
        };
        let request = DryRunRequest {
            source: kayak_core::script::ScriptSource::Inline {
                code: source.get_untracked(),
            },
            scope: kayak_core::script::ScriptScope::Message,
            max_operations: None,
            messages,
            state: std::collections::BTreeMap::new(),
        };
        running.set(true);
        leptos::task::spawn_local(async move {
            let client = ApiClient {
                base: String::new(),
            };
            let result = client
                .dry_run_script(&request)
                .await
                .map_err(|err| err.to_string());
            outcome.set(Some(result));
            running.set(false);
        });
    };

    view! {
        <div class="try-it">
            <button
                class="try-it-toggle"
                type="button"
                on:click=move |_| open.update(|o| *o = !*o)
            >
                {move || if open.get() { "▾ try it" } else { "▸ try it" }}
            </button>
            <Show when=move || open.get()>
                <div class="try-it-body">
                    <label class="try-it-label">"messages"</label>
                    <textarea
                        class="try-it-input"
                        spellcheck="false"
                        prop:value=messages.get_untracked()
                        on:input=move |ev| messages.set(event_target_value(&ev))
                    />
                    <button class="button" type="button" on:click=run disabled=move || running.get()>
                        {move || if running.get() { "running…" } else { "run" }}
                    </button>
                    {move || outcome.get().map(|result| match result {
                        Err(message) => view! {
                            <div class="try-it-error">{message}</div>
                        }
                        .into_any(),
                        Ok(DryRunResponse::Failed { stage, message, line, column }) => {
                            let where_ = match (line, column) {
                                (Some(line), Some(column)) => {
                                    format!(" (line {line}, column {column})")
                                }
                                (Some(line), None) => format!(" (line {line})"),
                                _ => String::new(),
                            };
                            let stage = match stage {
                                kayak_core::script::DryRunStage::Compile => "does not compile",
                                kayak_core::script::DryRunStage::Runtime => "failed while running",
                            };
                            view! {
                                <div class="try-it-error">
                                    <strong>{stage}</strong>
                                    ": "
                                    {message}
                                    {where_}
                                </div>
                            }
                            .into_any()
                        }
                        Ok(DryRunResponse::Emitted { batches, warnings, .. }) => {
                            let total: usize = batches.iter().map(Vec::len).sum();
                            view! {
                                <div class="try-it-result">
                                    <div class="try-it-summary">
                                        {format!(
                                            "{total} message{} in {} batch{}",
                                            if total == 1 { "" } else { "s" },
                                            batches.len(),
                                            if batches.len() == 1 { "" } else { "es" },
                                        )}
                                    </div>
                                    {warnings
                                        .into_iter()
                                        .map(|text| view! { <div class="try-it-warning">{text}</div> })
                                        .collect_view()}
                                    {batches
                                        .into_iter()
                                        .map(|batch| {
                                            let text = serde_json::to_string_pretty(&batch)
                                                .unwrap_or_default();
                                            view! { <pre class="try-it-batch">{text}</pre> }
                                        })
                                        .collect_view()}
                                </div>
                            }
                            .into_any()
                        }
                    })}
                </div>
            </Show>
        </div>
    }
}
