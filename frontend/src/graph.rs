//! The canvas' geometry: where cards sit, where the edges between them run, and
//! how the camera moves.
//!
//! Everything here is a pure function over plain data, deliberately: it is the
//! only part of the frontend that can be unit-tested without a browser, and it
//! is also the part most likely to be wrong (depths, centring, zoom anchoring).
//! The Leptos components in `app.rs` do nothing but feed these functions and
//! render the result.

use std::collections::{BTreeMap, HashMap, HashSet};

use streamer_core::{StreamerId, config::Config, config::InputKind};

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
#[derive(Clone, Debug, PartialEq)]
pub struct Edge {
    pub from: StreamerId,
    pub to: StreamerId,
}

/// A streamer's parent in the graph is whatever its `streamer` input names as
/// upstream. Every other input kind is a root.
#[must_use]
pub fn upstream_of(config: &Config) -> Option<&StreamerId> {
    match &config.input.kind {
        InputKind::Streamer(c) => Some(&c.upstream),
        InputKind::Dummy(_) | InputKind::Nats(_) => None,
    }
}

/// `(id, upstream)` pairs — the only thing the layout needs to know about a
/// pipeline. An upstream that isn't in the list is ignored, so a dangling
/// reference lays out as a root rather than vanishing.
#[must_use]
pub fn nodes_from(streamers: &[(StreamerId, Config)]) -> Vec<(StreamerId, Option<StreamerId>)> {
    let known: HashSet<&StreamerId> = streamers.iter().map(|(id, _)| id).collect();
    streamers
        .iter()
        .map(|(id, config)| {
            let parent = upstream_of(config)
                .filter(|up| known.contains(up))
                .cloned();
            (id.clone(), parent)
        })
        .collect()
}

/// Depth of every node, where a root is 0. A node whose parent is missing —
/// or that sits in a cycle, which the server shouldn't allow but we don't get
/// to assume — is treated as a root rather than recursing forever.
fn depths(nodes: &[(StreamerId, Option<StreamerId>)]) -> HashMap<StreamerId, usize> {
    let parents: HashMap<&StreamerId, &Option<StreamerId>> =
        nodes.iter().map(|(id, parent)| (id, parent)).collect();

    let mut depths = HashMap::new();
    for (id, _) in nodes {
        let mut chain = vec![id];
        let mut seen: HashSet<&StreamerId> = HashSet::from([id]);
        let mut cursor = id;

        // walk up to a root (or to something already resolved), then unwind
        let base = loop {
            match parents.get(cursor).and_then(|p| p.as_ref()) {
                Some(parent) if !seen.insert(parent) => break 0, // cycle
                Some(parent) => {
                    if let Some(known) = depths.get(parent) {
                        break known + 1;
                    }
                    chain.push(parent);
                    cursor = parent;
                }
                None => break 0,
            }
        };

        for (steps_up, node) in chain.iter().enumerate() {
            depths.insert((*node).clone(), base + (chain.len() - 1 - steps_up));
        }
    }
    depths
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
    nodes: &[(StreamerId, Option<StreamerId>)],
    heights: &HashMap<StreamerId, f64>,
) -> HashMap<StreamerId, CardGeom> {
    let depths = depths(nodes);

    let mut rows: BTreeMap<usize, Vec<StreamerId>> = BTreeMap::new();
    for (id, _) in nodes {
        let depth = depths.get(id).copied().unwrap_or(0);
        rows.entry(depth).or_default().push(id.clone());
    }

    let parents: HashMap<&StreamerId, &Option<StreamerId>> =
        nodes.iter().map(|(id, parent)| (id, parent)).collect();

    // position within its own row, used to order the row below
    let mut order: HashMap<StreamerId, usize> = HashMap::new();
    let mut row_widths: Vec<(usize, f64)> = Vec::new();

    for (depth, ids) in &mut rows {
        ids.sort_by(|a, b| {
            let key = |id: &StreamerId| {
                parents
                    .get(id)
                    .and_then(|p| p.as_ref())
                    .and_then(|p| order.get(p))
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

/// The SVG path of every edge that has both ends placed, in a stable order.
///
/// This is recomputed from `placements`, so it follows cards as they grow: a
/// card that gets taller moves its own bottom edge and pushes every row below
/// it down, and both ends of the affected edges have to move with it.
#[must_use]
pub fn edge_paths(
    nodes: &[(StreamerId, Option<StreamerId>)],
    placements: &HashMap<StreamerId, CardGeom>,
) -> Vec<String> {
    edges(nodes)
        .into_iter()
        .filter_map(|e| {
            let from = placements.get(&e.from)?;
            let to = placements.get(&e.to)?;
            Some(edge_path(*from, *to))
        })
        .collect()
}

/// Parent → child pairs, in a stable order.
#[must_use]
pub fn edges(nodes: &[(StreamerId, Option<StreamerId>)]) -> Vec<Edge> {
    let mut edges: Vec<Edge> = nodes
        .iter()
        .filter_map(|(id, parent)| {
            parent.as_ref().map(|p| Edge {
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

    fn node(id: &str, parent: Option<&str>) -> (StreamerId, Option<StreamerId>) {
        (id.to_string(), parent.map(ToString::to_string))
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
            config_with_upstream("was-deleted"),
        )];
        let nodes = nodes_from(&streamers);
        assert_eq!(nodes, vec![node("orphan", None)]);

        let placed = layout(&nodes, &no_heights());
        assert_eq!(geom(&placed, "orphan").y, 0.0);
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
            upstream_of(&config_with_upstream("p1")),
            Some(&"p1".to_string())
        );
        assert_eq!(upstream_of(&dummy_config()), None);
    }

    fn config_with_upstream(upstream: &str) -> Config {
        use streamer_core::config::{
            OutputConfig, OutputKind, StdoutOutputConfig, StreamerConfig,
        };
        Config {
            id: None,
            input: streamer_core::config::InputConfig {
                kind: InputKind::Streamer(StreamerConfig {
                    upstream: upstream.to_string(),
                }),
                buffer: None,
            },
            transforms: vec![],
            output: OutputConfig {
                kind: OutputKind::Stdout(StdoutOutputConfig {}),
            },
        }
    }

    fn dummy_config() -> Config {
        use streamer_core::config::{
            DummyConfig, OutputConfig, OutputKind, StdoutOutputConfig,
        };
        Config {
            id: None,
            input: streamer_core::config::InputConfig {
                kind: InputKind::Dummy(DummyConfig { duration: 1 }),
                buffer: None,
            },
            transforms: vec![],
            output: OutputConfig {
                kind: OutputKind::Stdout(StdoutOutputConfig {}),
            },
        }
    }
}
