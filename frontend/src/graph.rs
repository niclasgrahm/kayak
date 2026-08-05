//! The canvas' geometry: where cards sit, where the edges between them run, and
//! how the camera moves.
//!
//! Everything here is a pure function over plain data, deliberately: it is the
//! only part of the frontend that can be unit-tested without a browser, and it
//! is also the part most likely to be wrong (depths, centring, zoom anchoring).
//! The Leptos components in `app.rs` do nothing but feed these functions and
//! render the result.

use std::collections::{BTreeMap, HashMap, HashSet};

use streamer_core::{EventPayload, StreamerId, UiEvent, config::Config, stage};

/// Cards are a fixed width; only their height varies with the config they show.
pub const CARD_WIDTH: f64 = 340.0;
/// Used until a card has been rendered and measured.
pub const FALLBACK_CARD_HEIGHT: f64 = 260.0;
const H_GAP: f64 = 60.0;
const V_GAP: f64 = 90.0;

pub const MIN_ZOOM: f64 = 0.2;
pub const MAX_ZOOM: f64 = 2.5;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CardGeom {
    pub x: f64,
    pub y: f64,
    pub width: f64,
    pub height: f64,
}

impl CardGeom {
    #[must_use]
    pub fn centre(&self) -> (f64, f64) {
        (self.x + self.width / 2.0, self.y + self.height / 2.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Camera {
    /// World coordinate shown at the top-left of the viewport.
    pub x: f64,
    pub y: f64,
    pub zoom: f64,
}

impl Default for Camera {
    fn default() -> Self {
        Self {
            x: 0.0,
            y: 0.0,
            zoom: 1.0,
        }
    }
}

/// An edge runs from a parent's bottom edge to its child's top edge.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Edge {
    pub from: StreamerId,
    pub to: StreamerId,
}

/// How many ticks an edge stays lit. The visible fade is a CSS transition on
/// the way out, so this only has to be long enough to be seen starting.
pub const PULSE_TICKS: u8 = 3;
/// How long a tick is, in milliseconds. Ticks rather than timestamps because
/// that keeps the decay a pure function with no clock in it.
pub const PULSE_TICK_MS: u64 = 50;

/// The edges a UI event lights up.
///
/// A batch crossing an edge is observed at the *receiving* end: a downstream
/// pipeline logging an `input` event has just been handed a batch by the
/// pipeline above it. An output event is not enough on its own — a streamer
/// emits to its output whether or not anything is subscribed. A failure moves
/// no data and so lights nothing, whatever stage it came from.
///
/// A node with several upstreams lights *all* of its incoming edges, because
/// the event says a batch arrived and not which input carried it. Attributing
/// it would need the input's index on the event; until then over-lighting beats
/// picking one edge and being wrong about it.
#[must_use]
pub fn pulsed_edges(event: &UiEvent, nodes: &[(StreamerId, Vec<StreamerId>)]) -> Vec<Edge> {
    if event.stage != stage::INPUT || !matches!(event.payload, EventPayload::Batch(_)) {
        return Vec::new();
    }
    // a root's inputs come from outside the graph, so no edge lights up
    nodes
        .iter()
        .find(|(id, _)| *id == event.streamer_id)
        .map(|(_, parents)| {
            parents
                .iter()
                .map(|parent| Edge {
                    from: parent.clone(),
                    to: event.streamer_id.clone(),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Age every pulse by one tick, dropping the ones that have burnt out. Returns
/// whether anything is still lit — the caller stops ticking when nothing is.
pub fn tick_pulses(pulses: &mut HashMap<Edge, u8>) -> bool {
    pulses.retain(|_, remaining| {
        *remaining = remaining.saturating_sub(1);
        *remaining > 0
    });
    !pulses.is_empty()
}

/// A streamer's parents in the graph are whatever its `streamer` inputs name as
/// upstream. A pipeline with none of those is a root; a pipeline can also mix
/// them, being fed by another pipeline *and* by NATS.
///
/// The answer comes from `Config` itself, so the canvas and the server's
/// config-file writer read the graph the same way.
#[must_use]
pub fn upstreams_of(config: &Config) -> Vec<&StreamerId> {
    config.upstreams()
}

/// `(id, upstreams)` pairs — the only thing the layout needs to know about a
/// pipeline. An upstream that isn't in the list is dropped, so a dangling
/// reference lays out as a root rather than vanishing; a node naming the same
/// upstream twice gets one edge, not two.
#[must_use]
pub fn nodes_from(streamers: &[(StreamerId, Config)]) -> Vec<(StreamerId, Vec<StreamerId>)> {
    let known: HashSet<&StreamerId> = streamers.iter().map(|(id, _)| id).collect();
    streamers
        .iter()
        .map(|(id, config)| {
            let mut seen = HashSet::new();
            let parents = upstreams_of(config)
                .into_iter()
                .filter(|up| known.contains(up))
                .filter(|up| seen.insert((*up).clone()))
                .cloned()
                .collect();
            (id.clone(), parents)
        })
        .collect()
}

/// Depth of every node, where a root is 0 and a node with parents sits one row
/// below its *deepest* parent — so every edge points downwards, which is what
/// makes the drawing readable once a node can have several parents.
///
/// A node with no parents, or one sitting in a cycle (which the server
/// shouldn't allow, but we don't get to assume), is treated as a root rather
/// than recursing forever.
fn depths(nodes: &[(StreamerId, Vec<StreamerId>)]) -> HashMap<StreamerId, usize> {
    let parents: HashMap<&StreamerId, &Vec<StreamerId>> =
        nodes.iter().map(|(id, parents)| (id, parents)).collect();

    fn depth_of<'a>(
        id: &'a StreamerId,
        parents: &HashMap<&'a StreamerId, &'a Vec<StreamerId>>,
        resolved: &mut HashMap<StreamerId, usize>,
        // the path being walked right now; a node reappearing on it is a cycle
        on_path: &mut HashSet<StreamerId>,
    ) -> usize {
        if let Some(known) = resolved.get(id) {
            return *known;
        }
        if !on_path.insert(id.clone()) {
            return 0;
        }
        let depth = parents
            .get(id)
            .map(|ps| {
                ps.iter()
                    .map(|p| depth_of(p, parents, resolved, on_path) + 1)
                    .max()
                    .unwrap_or(0)
            })
            .unwrap_or(0);
        on_path.remove(id);
        resolved.insert(id.clone(), depth);
        depth
    }

    let mut resolved = HashMap::new();
    for (id, _) in nodes {
        depth_of(id, &parents, &mut resolved, &mut HashSet::new());
    }
    resolved
}

/// Lay the graph out top-to-bottom: one row per depth, rows ordered so that
/// children follow their parent's order, and every row centred against the
/// widest one.
///
/// `heights` holds measured card heights; anything not measured yet falls back
/// to [`FALLBACK_CARD_HEIGHT`], so a first render is roughly right and settles
/// once the cards report their real size.
#[must_use]
pub fn layout(
    nodes: &[(StreamerId, Vec<StreamerId>)],
    heights: &HashMap<StreamerId, f64>,
) -> HashMap<StreamerId, CardGeom> {
    let depths = depths(nodes);

    let mut rows: BTreeMap<usize, Vec<StreamerId>> = BTreeMap::new();
    for (id, _) in nodes {
        let depth = depths.get(id).copied().unwrap_or(0);
        rows.entry(depth).or_default().push(id.clone());
    }

    let parents: HashMap<&StreamerId, &Vec<StreamerId>> =
        nodes.iter().map(|(id, parents)| (id, parents)).collect();

    // position within its own row, used to order the row below
    let mut order: HashMap<StreamerId, usize> = HashMap::new();
    let mut row_widths: Vec<(usize, f64)> = Vec::new();

    for (depth, ids) in &mut rows {
        ids.sort_by(|a, b| {
            // a node with several parents sits under the leftmost of them,
            // which keeps its edges from crossing the whole row
            let key = |id: &StreamerId| {
                parents
                    .get(id)
                    .and_then(|ps| ps.iter().filter_map(|p| order.get(p)).min())
                    .copied()
                    // roots sort after placed children, then by id so the
                    // layout is stable no matter what order the API returned
                    .unwrap_or(usize::MAX)
            };
            key(a).cmp(&key(b)).then_with(|| a.cmp(b))
        });
        for (i, id) in ids.iter().enumerate() {
            order.insert(id.clone(), i);
        }
        let count = ids.len() as f64;
        row_widths.push((*depth, count * CARD_WIDTH + (count - 1.0).max(0.0) * H_GAP));
    }

    let widest = row_widths
        .iter()
        .map(|(_, w)| *w)
        .fold(0.0_f64, f64::max);

    let mut out = HashMap::new();
    let mut y = 0.0;
    for (depth, ids) in &rows {
        let row_width = row_widths
            .iter()
            .find(|(d, _)| d == depth)
            .map_or(0.0, |(_, w)| *w);
        let left = (widest - row_width) / 2.0;

        let mut tallest = 0.0_f64;
        for (i, id) in ids.iter().enumerate() {
            let height = heights
                .get(id)
                .copied()
                .filter(|h| *h > 0.0)
                .unwrap_or(FALLBACK_CARD_HEIGHT);
            tallest = tallest.max(height);
            out.insert(
                id.clone(),
                CardGeom {
                    x: left + i as f64 * (CARD_WIDTH + H_GAP),
                    y,
                    width: CARD_WIDTH,
                    height,
                },
            );
        }
        y += tallest + V_GAP;
    }
    out
}

/// Size of the box that contains every card, measured from the surface origin.
///
/// The edge overlay needs this: a zero-sized `<svg>` isn't painted at all, even
/// with `overflow: visible`, so it has to be given the graph's real extent.
#[must_use]
pub fn bounds(placements: &HashMap<StreamerId, CardGeom>) -> (f64, f64) {
    placements.values().fold((0.0_f64, 0.0_f64), |(w, h), g| {
        (w.max(g.x + g.width), h.max(g.y + g.height))
    })
}

/// Every edge that has both ends placed, with its SVG path, in a stable order.
///
/// This is recomputed from `placements`, so it follows cards as they grow: a
/// card that gets taller moves its own bottom edge and pushes every row below
/// it down, and both ends of the affected edges have to move with it.
#[must_use]
pub fn edge_paths(
    nodes: &[(StreamerId, Vec<StreamerId>)],
    placements: &HashMap<StreamerId, CardGeom>,
) -> Vec<(Edge, String)> {
    edges(nodes)
        .into_iter()
        .filter_map(|e| {
            let from = placements.get(&e.from)?;
            let to = placements.get(&e.to)?;
            let path = edge_path(*from, *to);
            Some((e, path))
        })
        .collect()
}

/// Parent → child pairs, in a stable order.
#[must_use]
pub fn edges(nodes: &[(StreamerId, Vec<StreamerId>)]) -> Vec<Edge> {
    let mut edges: Vec<Edge> = nodes
        .iter()
        .flat_map(|(id, parents)| {
            parents.iter().map(|p| Edge {
                from: p.clone(),
                to: id.clone(),
            })
        })
        .collect();
    edges.sort_by(|a, b| a.from.cmp(&b.from).then_with(|| a.to.cmp(&b.to)));
    edges
}

/// An SVG cubic bezier from a parent's bottom edge down to its child's top
/// edge. The control points are pulled vertically so the curve leaves and
/// enters straight down, which reads as a flow direction.
#[must_use]
pub fn edge_path(from: CardGeom, to: CardGeom) -> String {
    let (x1, y1) = (from.x + from.width / 2.0, from.y + from.height);
    let (x2, y2) = (to.x + to.width / 2.0, to.y);
    let bend = ((y2 - y1).abs() * 0.5).clamp(30.0, 120.0);
    format!("M {x1} {y1} C {x1} {}, {x2} {}, {x2} {y2}", y1 + bend, y2 - bend)
}

/// Where the camera has to sit for `target` to be centred in a viewport of
/// `(width, height)` css pixels, at the camera's current zoom.
#[must_use]
pub fn focus_camera(camera: Camera, target: CardGeom, viewport: (f64, f64)) -> Camera {
    let (cx, cy) = target.centre();
    Camera {
        x: cx - (viewport.0 / 2.0) / camera.zoom,
        y: cy - (viewport.1 / 2.0) / camera.zoom,
        zoom: camera.zoom,
    }
}

/// Zoom about a cursor position given in viewport pixels, keeping whatever is
/// under the cursor exactly where it is.
#[must_use]
pub fn zoom_at(camera: Camera, cursor: (f64, f64), factor: f64) -> Camera {
    let zoom = (camera.zoom * factor).clamp(MIN_ZOOM, MAX_ZOOM);
    if (zoom - camera.zoom).abs() < f64::EPSILON {
        return camera;
    }
    // the world point under the cursor is `camera + cursor / zoom`; solve for
    // the camera that keeps it there at the new zoom
    Camera {
        x: camera.x + cursor.0 / camera.zoom - cursor.0 / zoom,
        y: camera.y + cursor.1 / camera.zoom - cursor.1 / zoom,
        zoom,
    }
}

/// Wheel deltas come in pixels, lines or pages depending on the device; put
/// them all back into pixels before they mean anything.
#[must_use]
pub fn wheel_delta_pixels(delta: f64, delta_mode: u32) -> f64 {
    match delta_mode {
        1 => delta * 16.0,  // lines
        2 => delta * 400.0, // pages
        _ => delta,
    }
}

/// Time constant of the camera glide: each `FOCUS_TAU_MS` covers ~63% of the
/// remaining distance. The tail is what you actually feel, so the total is
/// several times this — about 350ms for a screen-sized move at 45ms.
const FOCUS_TAU_MS: f64 = 45.0;

/// Longest frame the glide will ease over, ~two frames at 60Hz.
///
/// A `delta_ms` is not always frame-sized: an animation-frame loop that has been
/// parked, a backgrounded tab, or a slow paint can hand us hundreds of
/// milliseconds, and easing over that in one step covers the whole distance at
/// once — the camera teleports instead of gliding. Clamping costs nothing when
/// frames are normal.
const MAX_FRAME_MS: f64 = 32.0;

/// One frame of camera movement toward `target`. Exponential easing, scaled by
/// the frame's own duration so the glide takes the same wall time on a 60Hz and
/// a 120Hz display. Returns the new camera and whether it has arrived.
#[must_use]
pub fn approach(camera: Camera, target: Camera, delta_ms: f64) -> (Camera, bool) {
    // 1 - e^(-dt/tau): the fraction of the remaining distance to cover
    let k = 1.0 - (-delta_ms.clamp(0.0, MAX_FRAME_MS) / FOCUS_TAU_MS).exp();
    let next = Camera {
        x: camera.x + (target.x - camera.x) * k,
        y: camera.y + (target.y - camera.y) * k,
        zoom: camera.zoom + (target.zoom - camera.zoom) * k,
    };

    // within half a pixel there is nothing left to see; snap and stop so the
    // animation frame loop can go back to sleep
    let arrived = (target.x - next.x).abs() < 0.5
        && (target.y - next.y).abs() < 0.5
        && (target.zoom - next.zoom).abs() < 0.001;
    if arrived { (target, true) } else { (next, false) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use streamer_core::config::InputKind;

    fn node(id: &str, parent: Option<&str>) -> (StreamerId, Vec<StreamerId>) {
        (
            id.to_string(),
            parent.map(ToString::to_string).into_iter().collect(),
        )
    }

    /// A node fed by several upstreams at once.
    fn node_with(id: &str, parents: &[&str]) -> (StreamerId, Vec<StreamerId>) {
        (
            id.to_string(),
            parents.iter().map(ToString::to_string).collect(),
        )
    }

    fn no_heights() -> HashMap<StreamerId, f64> {
        HashMap::new()
    }

    fn geom(placed: &HashMap<StreamerId, CardGeom>, id: &str) -> CardGeom {
        match placed.get(id) {
            Some(g) => *g,
            None => panic!("'{id}' was not placed; got {:?}", placed.keys()),
        }
    }

    #[test]
    fn a_single_root_sits_at_the_origin() {
        let placed = layout(&[node("a", None)], &no_heights());
        assert_eq!(geom(&placed, "a").x, 0.0);
        assert_eq!(geom(&placed, "a").y, 0.0);
    }

    /// The whole point of the hierarchy: a child is strictly below its parent.
    #[test]
    fn a_child_is_placed_below_its_parent() {
        let placed = layout(&[node("a", None), node("b", Some("a"))], &no_heights());
        assert!(
            geom(&placed, "b").y > geom(&placed, "a").y + geom(&placed, "a").height,
            "child overlaps or sits above its parent: {placed:?}"
        );
    }

    /// Depth is transitive — a grandchild goes two rows down, not one.
    #[test]
    fn depth_accumulates_down_a_chain() {
        let placed = layout(
            &[node("a", None), node("b", Some("a")), node("c", Some("b"))],
            &no_heights(),
        );
        assert!(geom(&placed, "a").y < geom(&placed, "b").y);
        assert!(geom(&placed, "b").y < geom(&placed, "c").y);
    }

    /// The config.json shape: one source feeding three aggregators. They share a
    /// row, don't overlap, and the parent sits centred over them.
    #[test]
    fn siblings_share_a_row_and_are_centred_under_their_parent() {
        let placed = layout(
            &[
                node("source", None),
                node("a", Some("source")),
                node("b", Some("source")),
                node("c", Some("source")),
            ],
            &no_heights(),
        );

        let row: Vec<f64> = ["a", "b", "c"].iter().map(|id| geom(&placed, id).y).collect();
        assert!(row.windows(2).all(|w| w[0] == w[1]), "siblings not on one row");

        let mut xs: Vec<f64> = ["a", "b", "c"].iter().map(|id| geom(&placed, id).x).collect();
        xs.sort_by(f64::total_cmp);
        assert!(
            xs.windows(2).all(|w| w[1] - w[0] >= CARD_WIDTH),
            "siblings overlap: {xs:?}"
        );

        let parent_centre = geom(&placed, "source").centre().0;
        let row_centre = (xs[0] + xs[2] + CARD_WIDTH) / 2.0;
        assert!(
            (parent_centre - row_centre).abs() < 1.0,
            "parent at {parent_centre} is not centred over its children at {row_centre}"
        );
    }

    /// The API returns streamers in HashMap order, so the layout must not
    /// depend on the order it gets them in — cards would shuffle on refresh.
    #[test]
    fn the_layout_is_independent_of_input_order() {
        let forward = [
            node("source", None),
            node("a", Some("source")),
            node("b", Some("source")),
        ];
        let mut reversed = forward.clone();
        reversed.reverse();

        assert_eq!(
            layout(&forward, &no_heights()),
            layout(&reversed, &no_heights())
        );
    }

    /// Measured heights push the next row further down, so tall cards don't
    /// overlap the row below.
    #[test]
    fn a_measured_row_height_pushes_the_next_row_down() {
        let nodes = [node("a", None), node("b", Some("a"))];
        let tall = HashMap::from([("a".to_string(), 900.0)]);

        let default_y = geom(&layout(&nodes, &no_heights()), "b").y;
        let pushed_y = geom(&layout(&nodes, &tall), "b").y;
        assert!(
            pushed_y > default_y,
            "a 900px card did not push the row below it down"
        );
    }

    /// A dangling upstream is possible — the upstream may have been deleted —
    /// and must lay out as a root rather than disappearing.
    #[test]
    fn an_unknown_upstream_lays_out_as_a_root() {
        let streamers = vec![(
            "orphan".to_string(),
            config_of(vec![upstream_input("was-deleted")]),
        )];
        let nodes = nodes_from(&streamers);
        assert_eq!(nodes, vec![node("orphan", None)]);

        let placed = layout(&nodes, &no_heights());
        assert_eq!(geom(&placed, "orphan").y, 0.0);
    }

    /// With several parents a node has to sit below the deepest of them, or an
    /// edge would run upwards from a parent in a lower row.
    #[test]
    fn a_node_sits_below_its_deepest_parent() {
        let nodes = [
            node("root", None),
            node("mid", Some("root")),
            // fed by both the root and the row below it
            node_with("joined", &["root", "mid"]),
        ];
        let placed = layout(&nodes, &no_heights());

        let (root_y, mid_y, joined_y) = (
            geom(&placed, "root").y,
            geom(&placed, "mid").y,
            geom(&placed, "joined").y,
        );
        assert!(mid_y > root_y, "mid should be a row below root");
        assert!(
            joined_y > mid_y,
            "a node fed by root and mid should sit below mid, not beside it"
        );
    }

    /// Both edges into a join have to be drawn; only drawing the first would
    /// hide half the graph's shape.
    #[test]
    fn a_join_draws_one_edge_per_parent() {
        let nodes = [node("a", None), node("b", None), node_with("c", &["a", "b"])];
        assert_eq!(
            edges(&nodes),
            vec![
                Edge {
                    from: "a".to_string(),
                    to: "c".to_string()
                },
                Edge {
                    from: "b".to_string(),
                    to: "c".to_string()
                },
            ]
        );
        assert_eq!(edge_paths(&nodes, &layout(&nodes, &no_heights())).len(), 2);
    }

    /// The server shouldn't be able to produce a cycle, but the layout runs on
    /// whatever the API returned and must not hang if one shows up.
    #[test]
    fn a_cycle_does_not_hang_the_layout() {
        let placed = layout(&[node("a", Some("b")), node("b", Some("a"))], &no_heights());
        assert_eq!(placed.len(), 2, "both nodes should still be placed");
    }

    /// The edge overlay is sized from this; if it under-reports, edges get
    /// clipped, and a zero size means nothing is drawn at all.
    #[test]
    fn bounds_cover_every_card() {
        let placed = layout(
            &[
                node("source", None),
                node("a", Some("source")),
                node("b", Some("source")),
            ],
            &no_heights(),
        );
        let (w, h) = bounds(&placed);

        for (id, geom) in &placed {
            assert!(geom.x + geom.width <= w, "'{id}' sticks out to the right");
            assert!(geom.y + geom.height <= h, "'{id}' sticks out below");
        }
        assert!(w > 0.0 && h > 0.0);
    }

    #[test]
    fn bounds_of_an_empty_graph_are_zero() {
        assert_eq!(bounds(&HashMap::new()), (0.0, 0.0));
    }

    #[test]
    fn edges_connect_every_child_to_its_parent() {
        let nodes = [
            node("source", None),
            node("a", Some("source")),
            node("b", Some("source")),
        ];
        assert_eq!(
            edges(&nodes),
            vec![
                Edge {
                    from: "source".to_string(),
                    to: "a".to_string()
                },
                Edge {
                    from: "source".to_string(),
                    to: "b".to_string()
                },
            ]
        );
    }

    /// An edge leaves the parent's bottom edge and arrives at the child's top
    /// edge — not at either card's origin.
    #[test]
    fn an_edge_path_runs_from_the_parent_bottom_to_the_child_top() {
        let from = CardGeom {
            x: 0.0,
            y: 0.0,
            width: 100.0,
            height: 50.0,
        };
        let to = CardGeom {
            x: 200.0,
            y: 300.0,
            width: 100.0,
            height: 50.0,
        };
        let path = edge_path(from, to);
        assert!(path.starts_with("M 50 50 "), "unexpected start: {path}");
        assert!(path.ends_with("250 300"), "unexpected end: {path}");
    }

    /// The whole point of `edge_paths`: an edge is a function of where the
    /// cards *currently* are, so a card that changes height has to redraw both
    /// the edge above it and the ones below it.
    #[test]
    fn edge_paths_follow_a_card_that_changes_height() {
        let nodes = [
            node("source", None),
            node("a", Some("source")),
            node("b", Some("a")),
        ];

        let small = edge_paths(&nodes, &layout(&nodes, &no_heights()));
        let grown = edge_paths(
            &nodes,
            &layout(&nodes, &HashMap::from([("a".to_string(), 600.0)])),
        );

        assert_eq!(small.len(), 2, "expected one path per parent/child pair");
        assert_ne!(
            small, grown,
            "the edges did not move when card 'a' grew to 600px"
        );
    }

    /// An edge with an unplaced end has no coordinates to draw between; it must
    /// be dropped rather than drawn from the origin.
    #[test]
    fn an_edge_to_an_unplaced_card_is_skipped() {
        let nodes = [node("a", None), node("b", Some("a"))];
        assert!(edge_paths(&nodes, &HashMap::new()).is_empty());
    }

    fn batch_event(streamer_id: &str, stage: &str) -> UiEvent {
        UiEvent::batch(
            streamer_id.to_string(),
            stage,
            std::sync::Arc::new(Vec::new()),
        )
    }

    /// A batch arriving at a downstream pipeline is a batch that crossed the
    /// edge above it, which is the whole signal the blink is built on.
    #[test]
    fn an_input_event_lights_the_edge_from_its_upstream() {
        let nodes = [node("a", None), node("b", Some("a"))];
        assert_eq!(
            pulsed_edges(&batch_event("b", stage::INPUT), &nodes),
            vec![Edge {
                from: "a".to_string(),
                to: "b".to_string()
            }]
        );
    }

    /// The event says a batch arrived, not which input brought it, so a node
    /// with two upstreams lights both edges rather than guessing.
    #[test]
    fn an_input_event_lights_every_incoming_edge() {
        let nodes = [
            node("a", None),
            node("b", None),
            node_with("c", &["a", "b"]),
        ];
        assert_eq!(
            pulsed_edges(&batch_event("c", stage::INPUT), &nodes),
            vec![
                Edge {
                    from: "a".to_string(),
                    to: "c".to_string()
                },
                Edge {
                    from: "b".to_string(),
                    to: "c".to_string()
                },
            ]
        );
    }

    /// A streamer emits to its output whether or not anything is listening, so
    /// an output event says nothing about an edge.
    #[test]
    fn an_output_event_lights_nothing() {
        let nodes = [node("a", None), node("b", Some("a"))];
        assert!(pulsed_edges(&batch_event("b", stage::OUTPUT), &nodes).is_empty());
    }

    /// A failure at the input stage is the *absence* of a batch, so it must not
    /// light the edge an arriving batch would have.
    #[test]
    fn an_error_event_lights_nothing() {
        let nodes = [node("a", None), node("b", Some("a"))];
        let failed = UiEvent::error("b".to_string(), stage::INPUT, &"upstream went away");
        assert!(pulsed_edges(&failed, &nodes).is_empty());
    }

    /// A root is fed from outside the graph — NATS, a timer — and has no edge
    /// coming into it to light up.
    #[test]
    fn an_input_event_on_a_root_lights_nothing() {
        let nodes = [node("a", None), node("b", Some("a"))];
        assert!(pulsed_edges(&batch_event("a", stage::INPUT), &nodes).is_empty());
        // and a pipeline that isn't on the canvas at all can't light anything
        assert!(pulsed_edges(&batch_event("ghost", stage::INPUT), &nodes).is_empty());
    }

    /// Pulses have to burn out on their own, or an edge that saw one batch
    /// stays lit forever.
    #[test]
    fn a_pulse_burns_out_after_its_ticks() {
        let edge = Edge {
            from: "a".to_string(),
            to: "b".to_string(),
        };
        let mut pulses = HashMap::from([(edge.clone(), PULSE_TICKS)]);

        for tick in 1..PULSE_TICKS {
            assert!(
                tick_pulses(&mut pulses),
                "the pulse went out after {tick} of {PULSE_TICKS} ticks"
            );
        }
        assert!(!tick_pulses(&mut pulses), "the pulse never went out");
        assert!(pulses.is_empty());
    }

    /// Each edge decays on its own clock: a busy edge must not keep a quiet one
    /// alight, and a re-fired pulse restarts.
    #[test]
    fn pulses_decay_independently() {
        let busy = Edge {
            from: "a".to_string(),
            to: "busy".to_string(),
        };
        let quiet = Edge {
            from: "a".to_string(),
            to: "quiet".to_string(),
        };
        let mut pulses = HashMap::from([(busy.clone(), PULSE_TICKS), (quiet.clone(), 1)]);

        tick_pulses(&mut pulses);
        assert!(pulses.contains_key(&busy));
        assert!(!pulses.contains_key(&quiet), "the quiet edge stayed lit");
    }

    /// A long frame — a parked raf loop, a backgrounded tab — must not eat the
    /// entire glide in one step. This is what makes a second focus click glide
    /// rather than jump.
    #[test]
    fn a_long_frame_does_not_teleport_the_camera() {
        let target = Camera {
            x: 2000.0,
            ..Camera::default()
        };
        let (next, arrived) = approach(Camera::default(), target, 5_000.0);

        assert!(!arrived, "a single 5s frame ended the glide");
        assert!(
            next.x < target.x * 0.6,
            "one frame covered {:.0}px of a 2000px move",
            next.x
        );
    }

    #[test]
    fn focusing_centres_the_card_in_the_viewport() {
        let camera = Camera::default();
        let card = CardGeom {
            x: 1000.0,
            y: 500.0,
            width: 340.0,
            height: 200.0,
        };
        let focused = focus_camera(camera, card, (800.0, 600.0));

        // the card's centre must land at the viewport's centre
        let (cx, cy) = card.centre();
        assert_eq!((cx - focused.x) * focused.zoom, 400.0);
        assert_eq!((cy - focused.y) * focused.zoom, 300.0);
    }

    /// Zoom must not drift: whatever is under the cursor stays under it.
    #[test]
    fn zooming_keeps_the_point_under_the_cursor_fixed() {
        let camera = Camera {
            x: 120.0,
            y: 80.0,
            zoom: 1.0,
        };
        let cursor = (300.0, 200.0);
        let world_before = (
            camera.x + cursor.0 / camera.zoom,
            camera.y + cursor.1 / camera.zoom,
        );

        for factor in [1.2, 0.5, 2.0] {
            let zoomed = zoom_at(camera, cursor, factor);
            let world_after = (
                zoomed.x + cursor.0 / zoomed.zoom,
                zoomed.y + cursor.1 / zoomed.zoom,
            );
            assert!(
                (world_before.0 - world_after.0).abs() < 1e-9
                    && (world_before.1 - world_after.1).abs() < 1e-9,
                "the canvas drifted under the cursor at factor {factor}"
            );
        }
    }

    #[test]
    fn zoom_is_clamped_and_leaves_the_camera_alone_at_the_limits() {
        let camera = Camera::default();
        assert_eq!(zoom_at(camera, (0.0, 0.0), 100.0).zoom, MAX_ZOOM);
        assert_eq!(zoom_at(camera, (0.0, 0.0), 0.001).zoom, MIN_ZOOM);

        let maxed = Camera {
            zoom: MAX_ZOOM,
            ..camera
        };
        assert_eq!(
            zoom_at(maxed, (300.0, 300.0), 2.0),
            maxed,
            "zooming past the limit should be a no-op, not a pan"
        );
    }

    #[test]
    fn wheel_deltas_are_normalised_to_pixels() {
        assert_eq!(wheel_delta_pixels(120.0, 0), 120.0);
        assert_eq!(wheel_delta_pixels(3.0, 1), 48.0);
        assert_eq!(wheel_delta_pixels(1.0, 2), 400.0);
    }

    /// The pan has to actually finish, and land exactly on the target rather
    /// than creeping toward it forever.
    #[test]
    fn the_camera_reaches_its_target_and_reports_arrival() {
        let target = Camera {
            x: 2000.0,
            y: -500.0,
            zoom: 1.0,
        };
        let mut camera = Camera::default();

        let mut frames = 0;
        loop {
            let (next, arrived) = approach(camera, target, 16.0);
            camera = next;
            frames += 1;
            if arrived {
                break;
            }
            assert!(frames < 600, "camera never arrived");
        }

        assert_eq!(camera, target);
        // ~400ms at 60fps: long enough to read as a camera move, short enough
        // not to feel sluggish. A feel check, not a hard requirement.
        assert!(
            (10..=30).contains(&frames),
            "focus animation took {frames} frames, which will feel wrong"
        );
    }

    /// Frame-rate independence: the same wall-clock time gets the camera to
    /// roughly the same place whether frames are 8ms or 16ms apart.
    #[test]
    fn the_glide_covers_the_same_ground_at_any_frame_rate() {
        let target = Camera {
            x: 1000.0,
            ..Camera::default()
        };

        let mut slow = Camera::default();
        for _ in 0..10 {
            (slow, _) = approach(slow, target, 16.0);
        }
        let mut fast = Camera::default();
        for _ in 0..20 {
            (fast, _) = approach(fast, target, 8.0);
        }

        assert!(
            (slow.x - fast.x).abs() < 1.0,
            "60Hz reached {} but 120Hz reached {}",
            slow.x,
            fast.x
        );
    }

    #[test]
    fn a_streamer_input_is_the_only_kind_with_an_upstream() {
        assert_eq!(
            upstreams_of(&config_of(vec![upstream_input("p1")])),
            vec![&"p1".to_string()]
        );
        assert!(upstreams_of(&config_of(vec![dummy_input()])).is_empty());
    }

    /// A pipeline fed by two others has two parents; one fed by another pipeline
    /// *and* by NATS has one, because only the pipeline is on the canvas.
    #[test]
    fn a_pipeline_reports_every_upstream_it_names() {
        assert_eq!(
            upstreams_of(&config_of(vec![
                upstream_input("p1"),
                upstream_input("p2"),
            ])),
            vec![&"p1".to_string(), &"p2".to_string()]
        );
        assert_eq!(
            upstreams_of(&config_of(vec![upstream_input("p1"), dummy_input()])),
            vec![&"p1".to_string()]
        );
    }

    /// Two inputs naming the same upstream are one relationship, and the canvas
    /// would otherwise draw the edge twice and pulse it twice.
    #[test]
    fn the_same_upstream_named_twice_yields_one_edge() {
        let streamers = vec![
            ("p1".to_string(), config_of(vec![dummy_input()])),
            (
                "child".to_string(),
                config_of(vec![upstream_input("p1"), upstream_input("p1")]),
            ),
        ];
        assert_eq!(
            nodes_from(&streamers),
            vec![
                ("p1".to_string(), vec![]),
                ("child".to_string(), vec!["p1".to_string()]),
            ]
        );
    }

    fn upstream_input(upstream: &str) -> streamer_core::config::InputConfig {
        use streamer_core::config::StreamerConfig;
        streamer_core::config::InputConfig {
            kind: InputKind::Streamer(StreamerConfig {
                upstream: upstream.to_string(),
            }),
            buffer: None,
        }
    }

    fn dummy_input() -> streamer_core::config::InputConfig {
        use streamer_core::config::DummyConfig;
        streamer_core::config::InputConfig {
            kind: InputKind::Dummy(DummyConfig { duration: 1 }),
            buffer: None,
        }
    }

    fn config_of(inputs: Vec<streamer_core::config::InputConfig>) -> Config {
        use streamer_core::config::{OutputConfig, OutputKind, StdoutOutputConfig};
        Config {
            id: None,
            inputs,
            transforms: vec![],
            outputs: vec![OutputConfig {
                kind: OutputKind::Stdout(StdoutOutputConfig {}),
            }],
        }
    }
}
