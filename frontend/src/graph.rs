//! The canvas' geometry: where cards sit, where the edges between them run, and
//! how the camera moves.
//!
//! Everything here is a pure function over plain data, deliberately: it is the
//! only part of the frontend that can be unit-tested without a browser, and it
//! is also the part most likely to be wrong (depths, centring, zoom anchoring).
//! The Leptos components in `app.rs` do nothing but feed these functions and
//! render the result.

use std::collections::{BTreeMap, HashMap, HashSet};

use kayak_core::{
    EdgeEnd, EventPayload, LayoutFile, PipelineId, PipelineLayout, PortLayout, Side, Stage, UiEvent,
    config::Config,
};

/// The canvas' grid, in surface pixels.
///
/// It used to be decoration. It is now the unit everything is measured in:
/// cards are placed and sized in multiples of it, ports sit on its lines, and
/// edges run along them. That is what makes the drawing look deliberate rather
/// than merely tidy — a route only reads as "along the grid" if the things it
/// connects are on the grid too.
pub const GRID: f64 = 20.0;

/// Default card width — 18 grid cells, so a card's centre line lands on a grid
/// line rather than half way between two.
pub const CARD_WIDTH: f64 = 18.0 * GRID;
/// Used until a card has been rendered and measured.
pub const FALLBACK_CARD_HEIGHT: f64 = 13.0 * GRID;
/// Small enough to be worth dragging to, large enough that the header and one
/// row of the inspector still fit.
pub const MIN_CARD_WIDTH: f64 = 9.0 * GRID;
pub const MIN_CARD_HEIGHT: f64 = 5.0 * GRID;
const H_GAP: f64 = 4.0 * GRID;
const V_GAP: f64 = 5.0 * GRID;

pub const MIN_ZOOM: f64 = 0.2;
pub const MAX_ZOOM: f64 = 2.5;

/// The nearest grid line to `value`.
#[must_use]
pub fn snap(value: f64) -> f64 {
    (value / GRID).round() * GRID
}

/// The next grid line at or after `value`.
///
/// Used for heights, which are *measured* rather than chosen: a card has to be
/// at least as tall as its content, so rounding down would clip it. Rounding up
/// is also idempotent, which is what keeps the measure → lay out → render loop
/// from oscillating between two heights.
#[must_use]
pub fn snap_up(value: f64) -> f64 {
    (value / GRID).ceil() * GRID
}

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
    pub from: PipelineId,
    pub to: PipelineId,
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
/// pipeline above it. An output event is not enough on its own — a pipeline
/// emits to its output whether or not anything is subscribed. A failure moves
/// no data and so lights nothing, whatever stage it came from.
///
/// A pipeline with several upstreams lights *all* of its incoming edges, because
/// the event says a batch arrived and not which input carried it. Attributing
/// it would need the input's index on the event; until then over-lighting beats
/// picking one edge and being wrong about it.
#[must_use]
pub fn pulsed_edges(event: &UiEvent, pipelines: &[(PipelineId, Vec<PipelineId>)]) -> Vec<Edge> {
    if event.stage != Stage::Input || !matches!(event.payload, EventPayload::Batch(_)) {
        return Vec::new();
    }
    // a root's inputs come from outside the graph, so no edge lights up
    pipelines
        .iter()
        .find(|(id, _)| *id == event.pipeline_id)
        .map(|(_, parents)| {
            parents
                .iter()
                .map(|parent| Edge {
                    from: parent.clone(),
                    to: event.pipeline_id.clone(),
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

/// A pipeline's parents in the graph are whatever its `pipeline` inputs name as
/// upstream. A pipeline with none of those is a root; a pipeline can also mix
/// them, being fed by another pipeline *and* by NATS.
///
/// The answer comes from `Config` itself, so the canvas and the server's
/// config-file writer read the graph the same way.
#[must_use]
pub fn upstreams_of(config: &Config) -> Vec<&PipelineId> {
    config.upstreams()
}

/// `(id, upstreams)` pairs — the only thing the layout needs to know about a
/// pipeline. An upstream that isn't in the list is dropped, so a dangling
/// reference lays out as a root rather than vanishing; a pipeline naming the same
/// upstream twice gets one edge, not two.
#[must_use]
pub fn pipelines_from(pipelines: &[(PipelineId, Config)]) -> Vec<(PipelineId, Vec<PipelineId>)> {
    let known: HashSet<&PipelineId> = pipelines.iter().map(|(id, _)| id).collect();
    pipelines
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

/// Depth of every pipeline, where a root is 0 and a pipeline with parents sits one row
/// below its *deepest* parent — so every edge points downwards, which is what
/// makes the drawing readable once a pipeline can have several parents.
///
/// A pipeline with no parents, or one sitting in a cycle (which the server
/// shouldn't allow, but we don't get to assume), is treated as a root rather
/// than recursing forever.
fn depths(pipelines: &[(PipelineId, Vec<PipelineId>)]) -> HashMap<PipelineId, usize> {
    let parents: HashMap<&PipelineId, &Vec<PipelineId>> = pipelines
        .iter()
        .map(|(id, parents)| (id, parents))
        .collect();

    fn depth_of<'a>(
        id: &'a PipelineId,
        parents: &HashMap<&'a PipelineId, &'a Vec<PipelineId>>,
        resolved: &mut HashMap<PipelineId, usize>,
        // the path being walked right now; a pipeline reappearing on it is a cycle
        on_path: &mut HashSet<PipelineId>,
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
    for (id, _) in pipelines {
        depth_of(id, &parents, &mut resolved, &mut HashSet::new());
    }
    resolved
}

/// The height a card occupies, given what it has reported measuring and what
/// the user may have pinned it to.
///
/// The two are different in kind: a measurement is "this is how much content
/// there is", a pinned height is "this is how tall I want it". A pinned height
/// wins and the content scrolls, which is the only way a resize handle can mean
/// anything on a card whose content decides its own size.
fn height_of(
    id: &PipelineId,
    heights: &HashMap<PipelineId, f64>,
    placed: Option<PipelineLayout>,
) -> f64 {
    if let Some(pinned) = placed.and_then(|p| p.height) {
        return snap_up(pinned.max(MIN_CARD_HEIGHT));
    }
    heights
        .get(id)
        .copied()
        .filter(|h| *h > 0.0)
        .map_or(FALLBACK_CARD_HEIGHT, snap_up)
}

/// Lay the graph out top-to-bottom: one row per depth, rows ordered so that
/// children follow their parent's order, and every row centred against the
/// widest one.
///
/// `heights` holds measured card heights; anything not measured yet falls back
/// to [`FALLBACK_CARD_HEIGHT`], so a first render is roughly right and settles
/// once the cards report their real size.
///
/// `placed` is what the user has arranged by hand, and it simply overwrites the
/// automatic answer for the pipelines it names. Notably it does *not* take those
/// pipelines out of the automatic flow: the row a pinned card came from keeps its
/// slot, so dragging one card doesn't rearrange every other card on the canvas.
/// Moving a card on top of another is then possible — and is the user's
/// business, in the same way that it is in every other editor with a canvas.
#[must_use]
pub fn layout(
    pipelines: &[(PipelineId, Vec<PipelineId>)],
    heights: &HashMap<PipelineId, f64>,
    placed: &LayoutFile,
) -> HashMap<PipelineId, CardGeom> {
    let mut out = auto_layout(pipelines, heights, placed);
    for (id, geom) in &mut out {
        if let Some(pinned) = placed.get(id) {
            geom.x = snap(pinned.x);
            geom.y = snap(pinned.y);
            geom.width = snap(pinned.width.max(MIN_CARD_WIDTH));
        }
    }
    out
}

/// The automatic layout, which is what every pipeline gets until it is dragged.
fn auto_layout(
    pipelines: &[(PipelineId, Vec<PipelineId>)],
    heights: &HashMap<PipelineId, f64>,
    placed: &LayoutFile,
) -> HashMap<PipelineId, CardGeom> {
    let depths = depths(pipelines);

    let mut rows: BTreeMap<usize, Vec<PipelineId>> = BTreeMap::new();
    for (id, _) in pipelines {
        let depth = depths.get(id).copied().unwrap_or(0);
        rows.entry(depth).or_default().push(id.clone());
    }

    let parents: HashMap<&PipelineId, &Vec<PipelineId>> = pipelines
        .iter()
        .map(|(id, parents)| (id, parents))
        .collect();

    // position within its own row, used to order the row below
    let mut order: HashMap<PipelineId, usize> = HashMap::new();
    let mut row_widths: Vec<(usize, f64)> = Vec::new();

    for (depth, ids) in &mut rows {
        ids.sort_by(|a, b| {
            // a pipeline with several parents sits under the leftmost of them,
            // which keeps its edges from crossing the whole row
            let key = |id: &PipelineId| {
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

    let widest = row_widths.iter().map(|(_, w)| *w).fold(0.0_f64, f64::max);

    let mut out = HashMap::new();
    let mut y = 0.0;
    for (depth, ids) in &rows {
        let row_width = row_widths
            .iter()
            .find(|(d, _)| d == depth)
            .map_or(0.0, |(_, w)| *w);
        // snapped, not merely centred: a row that starts half a cell off the
        // grid puts every port on it half a cell off too, and the edges then
        // jog on their way out of every card in the row
        let left = snap((widest - row_width) / 2.0);

        let mut tallest = 0.0_f64;
        for (i, id) in ids.iter().enumerate() {
            let height = height_of(id, heights, placed.get(id));
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
///
/// Padded by a cell: a route leaves the bottom-most card by a stub before it
/// turns, and the svg is what would clip it.
#[must_use]
pub fn bounds(placements: &HashMap<PipelineId, CardGeom>) -> (f64, f64) {
    let (w, h) = placements.values().fold((0.0_f64, 0.0_f64), |(w, h), g| {
        (w.max(g.x + g.width), h.max(g.y + g.height))
    });
    if w <= 0.0 && h <= 0.0 {
        return (0.0, 0.0);
    }
    (w + GRID, h + GRID)
}

/// Where a card lands after being dragged by `(dx, dy)` surface pixels, snapped
/// to the grid.
///
/// Snapping the *result* rather than the delta is what makes a card that was
/// already off-grid — one from an older layout file, say — come back onto it
/// the first time it is touched.
#[must_use]
pub fn dragged(geom: CardGeom, dx: f64, dy: f64, pinned_height: Option<f64>) -> PipelineLayout {
    PipelineLayout {
        x: snap(geom.x + dx),
        y: snap(geom.y + dy),
        width: snap(geom.width),
        height: pinned_height,
    }
}

/// Where a card's bottom-right corner lands after being dragged by `(dx, dy)`.
///
/// Resizing always pins the height, including when the card was previously
/// sized by its content: the gesture means "be this tall", and a card that
/// sprang back to its content height the moment it was let go would read as the
/// handle not working.
#[must_use]
pub fn resized(geom: CardGeom, dx: f64, dy: f64) -> PipelineLayout {
    PipelineLayout {
        x: snap(geom.x),
        y: snap(geom.y),
        width: snap((geom.width + dx).max(MIN_CARD_WIDTH)),
        height: Some(snap((geom.height + dy).max(MIN_CARD_HEIGHT))),
    }
}

/// The direction a route travels on its way *out* of a face.
fn outward(side: Side) -> (f64, f64) {
    match side {
        Side::Top => (0.0, -1.0),
        Side::Right => (1.0, 0.0),
        Side::Bottom => (0.0, 1.0),
        Side::Left => (-1.0, 0.0),
    }
}

/// How much clear space a route needs on a side before it will use it. Three
/// cells: one for the stub out of each card, one for the channel between them.
const CLEARANCE: f64 = 3.0 * GRID;

/// How far a route runs straight out of a card before it is allowed to turn.
/// Without it a line leaving the bottom of a card and immediately turning right
/// looks like it is coming out of the corner.
const STUB: f64 = GRID;

/// The faces an edge should leave and arrive by.
///
/// **Vertical wins whenever it is available**, because the graph is a flow and
/// down the page is what the flow means. A child one row below its parent reads
/// as being fed by it whether it sits directly underneath or right across the
/// canvas, so it is joined bottom-to-top either way; joining it side to side
/// merely because it happened to be further away horizontally than vertically
/// turns a row of siblings into a row of lines that all arrive sideways.
///
/// So the side faces are what you get when the cards are *level* with each
/// other — their vertical extents overlap, so there is no room to route between
/// them — which is exactly when a sideways line is the one that reads right. A
/// child dragged *above* its parent leaves by the top, and the line reads as
/// running backwards, which it does.
///
/// A face is only used when there is room in front of it: the whole point is
/// straight lines, and a line squeezed through a two-pixel gap is not one. When
/// nothing is clear (cards overlapping, or touching) it falls back to
/// bottom-to-top, which is at least the direction the graph flows in.
#[must_use]
pub fn sides_between(from: CardGeom, to: CardGeom) -> (Side, Side) {
    let below = to.y - (from.y + from.height);
    let above = from.y - (to.y + to.height);
    let right = to.x - (from.x + from.width);
    let left = from.x - (to.x + to.width);

    if below >= CLEARANCE {
        (Side::Bottom, Side::Top)
    } else if right >= CLEARANCE {
        (Side::Right, Side::Left)
    } else if left >= CLEARANCE {
        (Side::Left, Side::Right)
    } else if above >= CLEARANCE {
        (Side::Top, Side::Bottom)
    } else {
        (Side::Bottom, Side::Top)
    }
}

/// How long a face is: a card's width across the top and bottom, its height up
/// the sides.
#[must_use]
pub fn face_length(geom: CardGeom, side: Side) -> f64 {
    if side.is_vertical() {
        geom.width
    } else {
        geom.height
    }
}

/// A position along a face that is actually on the card, with a cell of margin
/// at each corner. A route attaching at the very corner reads as coming out of
/// the card's edge rather than its face.
#[must_use]
pub fn clamp_along(along: f64, length: f64) -> f64 {
    let (lo, hi) = (GRID, length - GRID);
    snap(along).clamp(lo.min(hi), hi.max(lo))
}

/// The point `along` a face, in surface coordinates.
///
/// `along` is measured from the face's start — its left end across the top and
/// bottom, its top end up the sides — so it is a property of the *card*, not of
/// where the card happens to be, and survives the card being dragged.
#[must_use]
pub fn port_at(geom: CardGeom, side: Side, along: f64) -> (f64, f64) {
    let along = clamp_along(along, face_length(geom, side));
    match side {
        Side::Top => (geom.x + along, geom.y),
        Side::Bottom => (geom.x + along, geom.y + geom.height),
        Side::Left => (geom.x, geom.y + along),
        Side::Right => (geom.x + geom.width, geom.y + along),
    }
}

/// Where on a face a route attaches when nobody has said: the `index`-th of
/// `count` edges sharing it, spread evenly.
///
/// Sharing matters more than it sounds. A source with seven children would
/// otherwise have seven routes leaving the same point, and the fan-out — the
/// most informative thing about that part of the graph — would be invisible
/// until the lines had travelled a whole channel apart.
#[must_use]
pub fn auto_along(geom: CardGeom, side: Side, index: usize, count: usize) -> f64 {
    let fraction = (index + 1) as f64 / (count + 1) as f64;
    clamp_along(face_length(geom, side) * fraction, face_length(geom, side))
}

/// Where a port ends up after being dragged `delta` surface pixels along its
/// face. Snapped and kept on the card, like every other gesture on the canvas.
#[must_use]
pub fn dragged_port(start_along: f64, delta: f64, length: f64) -> f64 {
    clamp_along(start_along + delta, length)
}

/// The middle segment of a route: the part that runs *between* the two cards
/// rather than out of one of them, and the only part free to be anywhere.
///
/// That freedom is what makes it the thing to grab. Every other segment is
/// pinned to a port, so the shape a crowded graph needs — this route passing
/// above that one instead of along the same line — is entirely a question of
/// where the channels sit.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Channel {
    /// Midpoint of the segment, which is where its handle goes.
    pub at: (f64, f64),
    /// How long the segment is, so the handle can span it rather than being a
    /// dot somewhere on it.
    pub length: f64,
    /// The route runs vertically, so the channel is a horizontal bar that
    /// slides up and down. (A horizontal route's channel is the mirror image.)
    pub vertical: bool,
}

/// One route: the corners it turns, and the segment in the middle that can be
/// dragged.
#[derive(Clone, Debug, PartialEq)]
pub struct Route {
    pub points: Vec<(f64, f64)>,
    /// `None` when the route has no middle segment to speak of — a straight
    /// line between two aligned cards, or an L-shaped one between faces at
    /// right angles. Dragging either would have nothing to move.
    pub channel: Option<Channel>,
}

/// The route from a port on `from` to a port on `to`.
///
/// Always three segments at most: out of the card, along a channel between the
/// two, and into the other card. Every segment is axis-aligned, and the channel
/// is snapped to a grid line — that is the whole trick, and it is why two routes
/// running between the same pair of rows share a channel instead of each finding
/// their own diagonal.
///
/// `offset` moves that channel off the half-way line, and is what the user is
/// changing when they drag it. It is clamped to the gap between the two stubs:
/// past either end the route would have to double back on itself, so there is
/// nothing there worth reaching.
///
/// The corner case this does *not* solve is a channel running straight through
/// a third card — which is now something the user can drag their way out of,
/// rather than something only an obstacle-aware router could fix.
#[must_use]
pub fn route(
    from: (f64, f64),
    from_side: Side,
    to: (f64, f64),
    to_side: Side,
    offset: f64,
) -> Route {
    let step = |p: (f64, f64), side: Side| {
        let (dx, dy) = outward(side);
        (p.0 + dx * STUB, p.1 + dy * STUB)
    };
    let a = step(from, from_side);
    let b = step(to, to_side);

    let mut points = vec![from, a];
    let mut channel = None;
    if from_side.is_vertical() == to_side.is_vertical() {
        let vertical = from_side.is_vertical();
        let (start, end) = if vertical { (a.1, b.1) } else { (a.0, b.0) };
        let along = channel_at(start, end, offset);
        // A channel with nothing either side of it is a straight line, and
        // moving it would change nothing — so there is nothing to grab.
        let (across_from, across_to) = if vertical { (a.0, b.0) } else { (a.1, b.1) };
        if vertical {
            points.push((a.0, along));
            points.push((b.0, along));
        } else {
            points.push((along, a.1));
            points.push((along, b.1));
        }
        if !near(across_from, across_to) {
            let mid = (across_from + across_to) / 2.0;
            channel = Some(Channel {
                at: if vertical { (mid, along) } else { (along, mid) },
                length: (across_to - across_from).abs(),
                vertical,
            });
        }
    } else if from_side.is_vertical() {
        // one turn is enough when the faces are at right angles to each other
        points.push((b.0, a.1));
    } else {
        points.push((a.0, b.1));
    }
    points.push(b);
    points.push(to);

    Route {
        points: simplify(points),
        channel,
    }
}

/// Where a channel sits between two stubs: half way, plus however far it has
/// been dragged, on a grid line, and never outside the gap.
fn channel_at(start: f64, end: f64, offset: f64) -> f64 {
    let (lo, hi) = (start.min(end), start.max(end));
    snap((start + end) / 2.0 + offset).clamp(lo, hi)
}

/// How far apart the automatic separation puts two channels that would
/// otherwise lie on the same line: one cell, the unit everything else on the
/// canvas moves in, and far enough that two 2px lines read as two lines.
const CHANNEL_STEP: f64 = GRID;

/// How far off the half-way line the automatic separation is willing to go
/// before it gives up and leaves an edge where it was. Six cells is more than
/// the gap between two rows holds — [`route`] clamps long before this — so it is
/// a bound on the search rather than on the answer.
const MAX_CHANNEL_STEPS: u32 = 6;

/// The offsets tried when a channel is placed automatically: the half-way line
/// first, then a cell either side of it, then two, and so on. Nearest-first, so
/// an edge only moves as far as it has to and a graph with room to spare looks
/// exactly as it did before there was anything to separate.
fn candidate_offsets() -> impl Iterator<Item = f64> {
    std::iter::once(0.0).chain(
        (1..=MAX_CHANNEL_STEPS).flat_map(|step| {
            let d = f64::from(step) * CHANNEL_STEP;
            [d, -d]
        }),
    )
}

/// A channel as the separator sees it: the line it lies on, and the stretch of
/// that line it covers.
///
/// `line` is a y for a horizontal bar and an x for a vertical one, which is why
/// the orientation is carried and compared — the same number means different
/// places on the two.
#[derive(Clone, Copy)]
struct Segment {
    vertical: bool,
    line: f64,
    lo: f64,
    hi: f64,
}

impl Segment {
    fn of(channel: Channel) -> Self {
        let (line, across) = if channel.vertical {
            (channel.at.1, channel.at.0)
        } else {
            (channel.at.0, channel.at.1)
        };
        Self {
            vertical: channel.vertical,
            line,
            lo: across - channel.length / 2.0,
            hi: across + channel.length / 2.0,
        }
    }

    /// Whether the two would be drawn on top of each other. Touching end to end
    /// doesn't count: that is two lines meeting at a point, which is what a
    /// shared channel between adjacent fans looks like and is fine.
    fn overlaps(&self, other: &Self) -> bool {
        self.vertical == other.vertical
            && near(self.line, other.line)
            && self.lo < other.hi - 0.01
            && other.lo < self.hi - 0.01
    }
}

/// Where a channel ends up after being dragged by `delta` surface pixels.
///
/// Snapped, like everything else on the canvas, and snapped on the *result* so
/// a channel that was pulled to an odd place by an older layout comes back onto
/// the grid the first time it is touched. [`route`] does the clamping, because
/// only it knows where the two cards are.
#[must_use]
pub fn dragged_channel(start_offset: f64, delta: f64) -> f64 {
    snap(start_offset + delta)
}

/// Drop the points a route doesn't need: repeats, and corners where the line
/// carries straight on. Both occur constantly — an edge between two cards that
/// happen to line up is a single straight segment described by six points — and
/// a "corner" with nothing to turn puts a rounded joint in the middle of a
/// straight line.
fn simplify(points: Vec<(f64, f64)>) -> Vec<(f64, f64)> {
    let mut out: Vec<(f64, f64)> = Vec::with_capacity(points.len());
    for p in points {
        if out.last().is_some_and(|last| close(*last, p)) {
            continue;
        }
        // a middle point on a straight run: replace it rather than keep it
        if out.len() >= 2 {
            let (a, b) = (out[out.len() - 2], out[out.len() - 1]);
            let straight = (near(a.0, b.0) && near(b.0, p.0)) || (near(a.1, b.1) && near(b.1, p.1));
            if straight {
                out.pop();
            }
        }
        out.push(p);
    }
    out
}

fn near(a: f64, b: f64) -> bool {
    (a - b).abs() < 0.01
}

fn close(a: (f64, f64), b: (f64, f64)) -> bool {
    near(a.0, b.0) && near(a.1, b.1)
}

/// Radius of the bend at a corner. Small enough that the route still reads as
/// following the grid, large enough not to look like an aliasing artefact.
const CORNER: f64 = 8.0;

/// An SVG path through `points`, with the corners rounded off.
///
/// The rounding is cosmetic and is allowed to eat into the segments either side
/// of a corner, so it is clamped to half the shorter of them — otherwise two
/// corners a few pixels apart would each round past the other and the line
/// would double back.
#[must_use]
pub fn rounded_path(points: &[(f64, f64)], radius: f64) -> String {
    let Some(first) = points.first() else {
        return String::new();
    };
    let mut d = format!("M {} {}", round(first.0), round(first.1));
    for i in 1..points.len().saturating_sub(1) {
        let (prev, corner, next) = (points[i - 1], points[i], points[i + 1]);
        let r = radius
            .min(distance(prev, corner) / 2.0)
            .min(distance(corner, next) / 2.0);
        if r < 0.5 {
            d.push_str(&format!(" L {} {}", round(corner.0), round(corner.1)));
            continue;
        }
        let enter = along(corner, prev, r);
        let leave = along(corner, next, r);
        d.push_str(&format!(
            " L {} {} Q {} {} {} {}",
            round(enter.0),
            round(enter.1),
            round(corner.0),
            round(corner.1),
            round(leave.0),
            round(leave.1)
        ));
    }
    if points.len() > 1 {
        let last = points[points.len() - 1];
        d.push_str(&format!(" L {} {}", round(last.0), round(last.1)));
    }
    d
}

/// The point `distance` away from `from` in the direction of `towards`.
fn along(from: (f64, f64), towards: (f64, f64), distance: f64) -> (f64, f64) {
    let (dx, dy) = (towards.0 - from.0, towards.1 - from.1);
    let length = dx.hypot(dy);
    if length < f64::EPSILON {
        return from;
    }
    (
        from.0 + dx / length * distance,
        from.1 + dy / length * distance,
    )
}

fn distance(a: (f64, f64), b: (f64, f64)) -> f64 {
    (b.0 - a.0).hypot(b.1 - a.1)
}

/// Path data is read by nobody and diffed by nothing, but a fractional pixel in
/// it is still noise; two decimals is more than a screen can show.
fn round(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

/// One drawn edge: which pipelines it connects, the path to paint, and the
/// channel the user can grab to move it out of the way of another.
#[derive(Clone, Debug, PartialEq)]
pub struct EdgePath {
    pub edge: Edge,
    pub path: String,
    pub channel: Option<Channel>,
    /// The offset the channel was actually drawn at, which is the stored one
    /// where there is one and the automatically chosen one otherwise. A drag
    /// starts from here rather than from the layout file, so grabbing a line
    /// that was separated automatically moves it from where it is rather than
    /// snapping it back to the half-way line first.
    pub channel_offset: f64,
    pub from_port: PortHandle,
    pub to_port: PortHandle,
}

/// Every edge that has both ends placed, with its SVG path, in a stable order.
///
/// This is recomputed from `placements`, so it follows cards as they move and
/// grow: a card that gets taller moves its own bottom edge and pushes every row
/// below it down, and a card that is dragged to the side of its parent changes
/// which faces its edges use.
///
/// The three passes are what the ports and the channels need. Which face an
/// edge uses depends only on the pair of cards, but *where* on that face it
/// attaches depends on how many other edges chose the same one — so every edge
/// is assigned a face first, and only then a place on it. Only once both ends
/// are known is there a channel to place, and placing one takes knowing where
/// all the others went ([`channel_offsets`]).
#[must_use]
pub fn edge_paths(
    pipelines: &[(PipelineId, Vec<PipelineId>)],
    placements: &HashMap<PipelineId, CardGeom>,
    arrangement: &LayoutFile,
) -> Vec<EdgePath> {
    let assigned: Vec<Assigned> = edges(pipelines)
        .into_iter()
        .filter_map(|edge| {
            let from = *placements.get(&edge.from)?;
            let to = *placements.get(&edge.to)?;
            let (from_side, to_side) = sides_between(from, to);
            // A pinned position only means something on the face it was
            // measured against: move a card and an edge that left by the bottom
            // may now leave by the side, where the old number is meaningless.
            let adjustment = arrangement.edge(&edge.from, &edge.to);
            let pinned = |end, side| {
                adjustment
                    .port(end)
                    .filter(|p: &PortLayout| p.side == side)
                    .map(|p| p.along)
            };
            Some(Assigned {
                edge,
                from,
                from_side,
                to,
                to_side,
                pinned_from: pinned(EdgeEnd::From, from_side),
                pinned_to: pinned(EdgeEnd::To, to_side),
            })
        })
        .collect();

    let slots = slots_on_faces(&assigned);

    let ports: Vec<(PortHandle, PortHandle)> = assigned
        .iter()
        .zip(&slots)
        .map(|(a, (out_slot, in_slot))| {
            let end = |geom: CardGeom, side: Side, pinned: Option<f64>, slot: &Slot| {
                let along =
                    pinned.unwrap_or_else(|| auto_along(geom, side, slot.index, slot.count));
                PortHandle {
                    at: port_at(geom, side, along),
                    side,
                    along: clamp_along(along, face_length(geom, side)),
                    length: face_length(geom, side),
                    pinned: pinned.is_some(),
                }
            };
            (
                end(a.from, a.from_side, a.pinned_from, out_slot),
                end(a.to, a.to_side, a.pinned_to, in_slot),
            )
        })
        .collect();

    let offsets = channel_offsets(&assigned, &ports, arrangement);

    assigned
        .into_iter()
        .zip(ports)
        .zip(offsets)
        .map(|((a, (from_port, to_port)), channel_offset)| {
            let routed = route(
                from_port.at,
                a.from_side,
                to_port.at,
                a.to_side,
                channel_offset,
            );
            EdgePath {
                path: rounded_path(&routed.points, CORNER),
                channel: routed.channel,
                channel_offset,
                from_port,
                to_port,
                edge: a.edge,
            }
        })
        .collect()
}

/// The offset every edge's channel is drawn at: the one the user stored where
/// there is one, and otherwise the nearest line to half way that no other
/// channel is already lying along.
///
/// This is what keeps a fan-out from being a single thick line. Every route
/// between the same two rows would otherwise take the same half-way line, and a
/// dozen of them would be drawn on top of each other with only their stubs
/// showing — the graph's shape hidden in exactly the place it is most worth
/// seeing. Dragging them apart by hand worked, and still does; doing it by hand
/// on every graph did not.
///
/// Two rules of thumb decide the order. **Stored offsets are laid down first**,
/// because they cannot move: an automatic channel gets out of the user's way
/// rather than the other way round. **The rest go left to right**, which is the
/// greedy order that packs intervals onto the fewest lines, and — because the
/// search starts at half way and works outwards — keeps every line as close to
/// where it would have been as the crowd allows.
///
/// Vertical stubs are not considered, only the middle segments: the ports are
/// already fanned out across their faces, so two stubs coincide only when two
/// ports do, and that is [`slots_on_faces`]' business.
fn channel_offsets(
    assigned: &[Assigned],
    ports: &[(PortHandle, PortHandle)],
    arrangement: &LayoutFile,
) -> Vec<f64> {
    let segment = |i: usize, offset: f64| {
        let a = &assigned[i];
        let (from, to) = &ports[i];
        route(from.at, a.from_side, to.at, a.to_side, offset)
            .channel
            .map(Segment::of)
    };
    let stored: Vec<Option<f64>> = assigned
        .iter()
        .map(|a| arrangement.edge_offset(&a.edge.from, &a.edge.to))
        .collect();

    let mut order: Vec<usize> = (0..assigned.len()).collect();
    order.sort_by(|&a, &b| {
        // an edge with no middle segment has nothing to place and sorts last,
        // where it costs nothing
        let start = |i: usize| segment(i, 0.0).map_or(f64::MAX, |s| s.lo);
        stored[a]
            .is_none()
            .cmp(&stored[b].is_none())
            .then_with(|| start(a).total_cmp(&start(b)))
            .then_with(|| a.cmp(&b))
    });

    let mut taken: Vec<Segment> = Vec::new();
    let mut out = vec![0.0; assigned.len()];
    for i in order {
        let chosen = stored[i].or_else(|| {
            candidate_offsets().find(|offset| {
                segment(i, *offset).is_none_or(|s| !taken.iter().any(|t| t.overlaps(&s)))
            })
        });
        // nothing within reach was free: leave the edge where it wanted to be
        // and let it overlap, which is at least where the user will look for it
        let chosen = chosen.unwrap_or(0.0);
        out[i] = chosen;
        if let Some(s) = segment(i, chosen) {
            taken.push(s);
        }
    }
    out
}

/// Where an edge attaches to a card, as drawn.
///
/// Carries what a drag on it needs — the face, how far along it is now, and how
/// long the face is — so the handle can move it without knowing anything about
/// the card behind it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PortHandle {
    pub at: (f64, f64),
    pub side: Side,
    pub along: f64,
    pub length: f64,
    /// Whether this position was chosen by hand rather than by the fan-out.
    pub pinned: bool,
}

/// An edge that knows which faces it will use, but not yet where on them.
struct Assigned {
    edge: Edge,
    from: CardGeom,
    from_side: Side,
    to: CardGeom,
    to_side: Side,
    /// A hand-placed position on the face this edge actually ended up using.
    /// A stored one for a *different* face is dropped here rather than
    /// carried — see [`PortLayout`].
    pinned_from: Option<f64>,
    pinned_to: Option<f64>,
}

/// Where an edge attaches on a face: the `index`-th of the `count` edges
/// sharing it.
#[derive(Clone, Copy)]
struct Slot {
    index: usize,
    count: usize,
}

impl Default for Slot {
    /// The only edge on its face, which is what an edge gets if nothing else
    /// claims one — and a `count` of zero would divide by it.
    fn default() -> Self {
        Self { index: 0, count: 1 }
    }
}

/// For each edge in `assigned`, in the same order: its place on the face it
/// leaves by, and its place on the face it arrives at.
///
/// Edges on a face are ordered by where the *other* end sits along that face's
/// axis, so the fan under a source comes out in the same left-to-right order as
/// the children it feeds and the lines don't cross each other on their way out.
///
/// **A hand-placed end still takes up its slot here**, even though the slot's
/// position is then thrown away for it. That is what keeps pinning one edge
/// from moving any other: drop it from the count instead and the remaining ends
/// re-spread over the whole face, so nudging one line would shift its siblings
/// out from under the user. It costs an unused gap in the fan, which is a much
/// smaller surprise. Two ports can then coincide — a pinned one dragged onto an
/// automatic one — and the fix for that is another drag.
fn slots_on_faces(assigned: &[Assigned]) -> Vec<(Slot, Slot)> {
    /// One end of one edge, as the face it lands on sees it: where the edge's
    /// other end sits along this face's axis (which is what the ends are sorted
    /// by), which edge it belongs to, and whether it is that edge's outgoing end.
    struct Landing {
        across: f64,
        edge: usize,
        outgoing: bool,
    }

    let mut faces: BTreeMap<(&PipelineId, Side), Vec<Landing>> = BTreeMap::new();
    // where along the face's own axis the other card sits
    let across = |side: Side, other: CardGeom| {
        if side.is_vertical() {
            other.centre().0
        } else {
            other.centre().1
        }
    };
    for (edge, a) in assigned.iter().enumerate() {
        faces
            .entry((&a.edge.from, a.from_side))
            .or_default()
            .push(Landing {
                across: across(a.from_side, a.to),
                edge,
                outgoing: true,
            });
        faces
            .entry((&a.edge.to, a.to_side))
            .or_default()
            .push(Landing {
                across: across(a.to_side, a.from),
                edge,
                outgoing: false,
            });
    }

    let mut out = vec![(Slot::default(), Slot::default()); assigned.len()];
    for mut using in faces.into_values() {
        // ties broken by edge index, which `edges` already sorted by id
        using.sort_by(|a, b| {
            a.across
                .total_cmp(&b.across)
                .then_with(|| a.edge.cmp(&b.edge))
        });
        let count = using.len();
        for (index, landing) in using.into_iter().enumerate() {
            let slot = Slot { index, count };
            if landing.outgoing {
                out[landing.edge].0 = slot;
            } else {
                out[landing.edge].1 = slot;
            }
        }
    }
    out
}

/// Parent → child pairs, in a stable order.
#[must_use]
pub fn edges(pipelines: &[(PipelineId, Vec<PipelineId>)]) -> Vec<Edge> {
    let mut edges: Vec<Edge> = pipelines
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

/// The card geometry, in surface pixels, that exactly covers the canvas
/// viewport at the camera's current position and zoom.
///
/// A maximized card stays inside the transformed surface rather than being
/// lifted out of it, so it keeps its place among its neighbours and there is no
/// second coordinate system to keep in step. The surface is scaled by `zoom`, so
/// covering a viewport of `w` screen pixels takes `w / zoom` surface pixels —
/// which is also why this answer is not snapped to the grid: it is measured
/// against the window rather than against the layout.
#[must_use]
pub fn maximized_geom(camera: Camera, viewport: (f64, f64)) -> CardGeom {
    CardGeom {
        x: camera.x,
        y: camera.y,
        width: viewport.0 / camera.zoom,
        height: viewport.1 / camera.zoom,
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
    if arrived {
        (target, true)
    } else {
        (next, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kayak_core::config::InputKind;

    fn pipeline(id: &str, parent: Option<&str>) -> (PipelineId, Vec<PipelineId>) {
        (
            id.to_string(),
            parent.map(ToString::to_string).into_iter().collect(),
        )
    }

    /// A pipeline fed by several upstreams at once.
    fn pipeline_with(id: &str, parents: &[&str]) -> (PipelineId, Vec<PipelineId>) {
        (
            id.to_string(),
            parents.iter().map(ToString::to_string).collect(),
        )
    }

    fn no_heights() -> HashMap<PipelineId, f64> {
        HashMap::new()
    }

    /// The automatic layout, which is what these tests are about — hand-placed
    /// pipelines get their own tests below.
    fn lay(
        pipelines: &[(PipelineId, Vec<PipelineId>)],
        heights: &HashMap<PipelineId, f64>,
    ) -> HashMap<PipelineId, CardGeom> {
        layout(pipelines, heights, &LayoutFile::default())
    }

    /// An automatically placed port: the `index`-th of `count` on a face.
    /// Hand-placed ones go through `port_at` and get their own tests below.
    fn port(geom: CardGeom, side: Side, index: usize, count: usize) -> (f64, f64) {
        port_at(geom, side, auto_along(geom, side, index, count))
    }

    /// A route with the channel where the layout puts it, which is what most of
    /// these tests are about — dragged channels get their own tests below.
    fn auto_route(
        from: (f64, f64),
        from_side: Side,
        to: (f64, f64),
        to_side: Side,
    ) -> Vec<(f64, f64)> {
        route(from, from_side, to, to_side, 0.0).points
    }

    fn card(x: f64, y: f64, width: f64, height: f64) -> CardGeom {
        CardGeom {
            x,
            y,
            width,
            height,
        }
    }

    /// Every segment of a route has to be horizontal or vertical — that is the
    /// entire claim the feature makes, and the one thing a diagonal would break.
    fn assert_axis_aligned(points: &[(f64, f64)]) {
        for pair in points.windows(2) {
            let (a, b) = (pair[0], pair[1]);
            assert!(
                near(a.0, b.0) || near(a.1, b.1),
                "diagonal segment {a:?} -> {b:?} in {points:?}"
            );
        }
    }

    fn on_grid(value: f64) -> bool {
        near(value % GRID, 0.0) || near((value % GRID).abs(), GRID)
    }

    fn geom(placed: &HashMap<PipelineId, CardGeom>, id: &str) -> CardGeom {
        match placed.get(id) {
            Some(g) => *g,
            None => panic!("'{id}' was not placed; got {:?}", placed.keys()),
        }
    }

    #[test]
    fn a_single_root_sits_at_the_origin() {
        let placed = lay(&[pipeline("a", None)], &no_heights());
        assert_eq!(geom(&placed, "a").x, 0.0);
        assert_eq!(geom(&placed, "a").y, 0.0);
    }

    /// The whole point of the hierarchy: a child is strictly below its parent.
    #[test]
    fn a_child_is_placed_below_its_parent() {
        let placed = lay(
            &[pipeline("a", None), pipeline("b", Some("a"))],
            &no_heights(),
        );
        assert!(
            geom(&placed, "b").y > geom(&placed, "a").y + geom(&placed, "a").height,
            "child overlaps or sits above its parent: {placed:?}"
        );
    }

    /// Depth is transitive — a grandchild goes two rows down, not one.
    #[test]
    fn depth_accumulates_down_a_chain() {
        let placed = lay(
            &[
                pipeline("a", None),
                pipeline("b", Some("a")),
                pipeline("c", Some("b")),
            ],
            &no_heights(),
        );
        assert!(geom(&placed, "a").y < geom(&placed, "b").y);
        assert!(geom(&placed, "b").y < geom(&placed, "c").y);
    }

    /// The config.json shape: one source feeding three aggregators. They share a
    /// row, don't overlap, and the parent sits centred over them.
    #[test]
    fn siblings_share_a_row_and_are_centred_under_their_parent() {
        let placed = lay(
            &[
                pipeline("source", None),
                pipeline("a", Some("source")),
                pipeline("b", Some("source")),
                pipeline("c", Some("source")),
            ],
            &no_heights(),
        );

        let row: Vec<f64> = ["a", "b", "c"]
            .iter()
            .map(|id| geom(&placed, id).y)
            .collect();
        assert!(
            row.windows(2).all(|w| w[0] == w[1]),
            "siblings not on one row"
        );

        let mut xs: Vec<f64> = ["a", "b", "c"]
            .iter()
            .map(|id| geom(&placed, id).x)
            .collect();
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

    /// The API returns pipelines in HashMap order, so the layout must not
    /// depend on the order it gets them in — cards would shuffle on refresh.
    #[test]
    fn the_layout_is_independent_of_input_order() {
        let forward = [
            pipeline("source", None),
            pipeline("a", Some("source")),
            pipeline("b", Some("source")),
        ];
        let mut reversed = forward.clone();
        reversed.reverse();

        assert_eq!(lay(&forward, &no_heights()), lay(&reversed, &no_heights()));
    }

    /// Measured heights push the next row further down, so tall cards don't
    /// overlap the row below.
    #[test]
    fn a_measured_row_height_pushes_the_next_row_down() {
        let pipelines = [pipeline("a", None), pipeline("b", Some("a"))];
        let tall = HashMap::from([("a".to_string(), 900.0)]);

        let default_y = geom(&lay(&pipelines, &no_heights()), "b").y;
        let pushed_y = geom(&lay(&pipelines, &tall), "b").y;
        assert!(
            pushed_y > default_y,
            "a 900px card did not push the row below it down"
        );
    }

    /// A dangling upstream is possible — the upstream may have been deleted —
    /// and must lay out as a root rather than disappearing.
    #[test]
    fn an_unknown_upstream_lays_out_as_a_root() {
        let pipelines = vec![(
            "orphan".to_string(),
            config_of(vec![upstream_input("was-deleted")]),
        )];
        let pipelines = pipelines_from(&pipelines);
        assert_eq!(pipelines, vec![pipeline("orphan", None)]);

        let placed = lay(&pipelines, &no_heights());
        assert_eq!(geom(&placed, "orphan").y, 0.0);
    }

    /// With several parents a pipeline has to sit below the deepest of them, or an
    /// edge would run upwards from a parent in a lower row.
    #[test]
    fn a_pipeline_sits_below_its_deepest_parent() {
        let pipelines = [
            pipeline("root", None),
            pipeline("mid", Some("root")),
            // fed by both the root and the row below it
            pipeline_with("joined", &["root", "mid"]),
        ];
        let placed = lay(&pipelines, &no_heights());

        let (root_y, mid_y, joined_y) = (
            geom(&placed, "root").y,
            geom(&placed, "mid").y,
            geom(&placed, "joined").y,
        );
        assert!(mid_y > root_y, "mid should be a row below root");
        assert!(
            joined_y > mid_y,
            "a pipeline fed by root and mid should sit below mid, not beside it"
        );
    }

    /// Both edges into a join have to be drawn; only drawing the first would
    /// hide half the graph's shape.
    #[test]
    fn a_join_draws_one_edge_per_parent() {
        let pipelines = [
            pipeline("a", None),
            pipeline("b", None),
            pipeline_with("c", &["a", "b"]),
        ];
        assert_eq!(
            edges(&pipelines),
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
        assert_eq!(
            edge_paths(
                &pipelines,
                &lay(&pipelines, &no_heights()),
                &LayoutFile::default()
            )
            .len(),
            2
        );
    }

    /// The server shouldn't be able to produce a cycle, but the layout runs on
    /// whatever the API returned and must not hang if one shows up.
    #[test]
    fn a_cycle_does_not_hang_the_layout() {
        let placed = lay(
            &[pipeline("a", Some("b")), pipeline("b", Some("a"))],
            &no_heights(),
        );
        assert_eq!(placed.len(), 2, "both pipelines should still be placed");
    }

    /// The edge overlay is sized from this; if it under-reports, edges get
    /// clipped, and a zero size means nothing is drawn at all.
    #[test]
    fn bounds_cover_every_card() {
        let placed = lay(
            &[
                pipeline("source", None),
                pipeline("a", Some("source")),
                pipeline("b", Some("source")),
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

    fn arranged(entries: &[(&str, PipelineLayout)]) -> LayoutFile {
        let mut file = LayoutFile::default();
        for (id, pipeline) in entries {
            file.pipelines.insert((*id).to_string(), *pipeline);
        }
        file
    }

    fn at(x: f64, y: f64) -> PipelineLayout {
        PipelineLayout {
            x,
            y,
            width: CARD_WIDTH,
            height: None,
        }
    }

    /// The point of the layout file: a card that was dragged stays where it was
    /// put, however the automatic layout would have placed it.
    #[test]
    fn a_hand_placed_card_overrides_the_automatic_layout() {
        let pipelines = [pipeline("a", None), pipeline("b", Some("a"))];
        let placed = layout(
            &pipelines,
            &no_heights(),
            &arranged(&[("b", at(900.0, 40.0))]),
        );

        assert_eq!(geom(&placed, "b").x, 900.0);
        assert_eq!(geom(&placed, "b").y, 40.0);
        // ...and the card nobody touched is where it always was
        assert_eq!(
            geom(&placed, "a"),
            geom(&lay(&pipelines, &no_heights()), "a")
        );
    }

    /// Moving one card must not shuffle every other card on the canvas: the row
    /// a pinned card came out of keeps its slot.
    #[test]
    fn pinning_one_card_leaves_its_siblings_alone() {
        let pipelines = [
            pipeline("root", None),
            pipeline("a", Some("root")),
            pipeline("b", Some("root")),
        ];
        let before = lay(&pipelines, &no_heights());
        let after = layout(
            &pipelines,
            &no_heights(),
            &arranged(&[("a", at(2000.0, 2000.0))]),
        );

        assert_eq!(geom(&after, "b"), geom(&before, "b"));
        assert_eq!(geom(&after, "root"), geom(&before, "root"));
    }

    /// A layout file is hand-editable and survives changes to the grid, so what
    /// is in it may be off-grid. The canvas still puts the card on the grid —
    /// otherwise one stale entry knocks every route through it off the lines.
    #[test]
    fn an_off_grid_entry_is_snapped_when_it_is_used() {
        let placed = layout(
            &[pipeline("a", None)],
            &no_heights(),
            &arranged(&[("a", at(103.0, 217.0))]),
        );
        assert_eq!(geom(&placed, "a").x, 100.0);
        assert_eq!(geom(&placed, "a").y, 220.0);
    }

    /// A pinned height wins over the measured one — that is what a resize
    /// handle means — and a card with no pinned height still follows its
    /// content.
    #[test]
    fn a_pinned_height_beats_the_measured_one() {
        let pipelines = [pipeline("a", None)];
        let measured = HashMap::from([("a".to_string(), 400.0)]);
        assert_eq!(geom(&lay(&pipelines, &measured), "a").height, 400.0);

        let pinned = arranged(&[(
            "a",
            PipelineLayout {
                height: Some(160.0),
                ..at(0.0, 0.0)
            },
        )]);
        assert_eq!(
            geom(&layout(&pipelines, &measured, &pinned), "a").height,
            160.0
        );
    }

    /// Heights come from measuring content, so they arrive at arbitrary pixel
    /// values; a card whose bottom edge is off-grid puts every port on its
    /// sides off-grid too. Rounding *up* is what keeps the content fitting.
    #[test]
    fn a_measured_height_is_rounded_up_onto_the_grid() {
        let pipelines = [pipeline("a", None)];
        let height = geom(
            &lay(&pipelines, &HashMap::from([("a".to_string(), 253.0)])),
            "a",
        )
        .height;
        assert_eq!(height, 260.0);
        assert!(height >= 253.0, "the card would be clipped");
    }

    /// The measure → lay out → render loop feeds the height it computes back
    /// into itself, so rounding has to be idempotent or the card oscillates.
    #[test]
    fn rounding_a_height_that_is_already_on_the_grid_changes_nothing() {
        let pipelines = [pipeline("a", None)];
        let once = geom(
            &lay(&pipelines, &HashMap::from([("a".to_string(), 253.0)])),
            "a",
        )
        .height;
        let twice = geom(
            &lay(&pipelines, &HashMap::from([("a".to_string(), once)])),
            "a",
        )
        .height;
        assert_eq!(once, twice);
    }

    /// A drag lands on the grid whatever the pointer did, and — the reason the
    /// result is snapped rather than the delta — a card that started off-grid
    /// comes back onto it.
    #[test]
    fn dragging_snaps_the_card_to_the_grid() {
        let geom = card(103.0, 217.0, CARD_WIDTH, 200.0);
        let moved = dragged(geom, 4.0, -3.0, None);
        assert_eq!((moved.x, moved.y), (100.0, 220.0));
        assert_eq!(moved.height, None, "a drag must not pin the height");

        // a drag big enough to matter moves by whole cells
        let moved = dragged(card(100.0, 100.0, CARD_WIDTH, 200.0), 63.0, 0.0, None);
        assert_eq!(moved.x, 160.0);
    }

    /// A drag on a card that was already resized keeps that height: moving
    /// something is not resizing it.
    #[test]
    fn dragging_keeps_a_height_that_was_already_pinned() {
        let moved = dragged(card(0.0, 0.0, CARD_WIDTH, 300.0), 20.0, 20.0, Some(300.0));
        assert_eq!(moved.height, Some(300.0));
    }

    /// Resizing pins the height even when the card had been sized by its
    /// content, or letting go would spring it back and read as a broken handle.
    #[test]
    fn resizing_snaps_and_pins_both_dimensions() {
        let resized = resized(card(0.0, 0.0, CARD_WIDTH, 200.0), 33.0, -47.0);
        assert_eq!(resized.width, CARD_WIDTH + 40.0);
        assert_eq!(resized.height, Some(160.0));
    }

    /// A card can't be dragged out of existence: past the minimum it stops
    /// rather than inverting.
    #[test]
    fn a_card_cannot_be_resized_below_its_minimum() {
        let resized = resized(card(0.0, 0.0, CARD_WIDTH, 200.0), -5000.0, -5000.0);
        assert_eq!(resized.width, MIN_CARD_WIDTH);
        assert_eq!(resized.height, Some(MIN_CARD_HEIGHT));
    }

    /// A pinned width has to survive the round trip through the layout, or a
    /// resized card would snap back to the default on the next re-layout.
    #[test]
    fn a_pinned_width_is_used_by_the_layout() {
        let wide = arranged(&[(
            "a",
            PipelineLayout {
                width: 500.0,
                ..at(0.0, 0.0)
            },
        )]);
        let placed = layout(&[pipeline("a", None)], &no_heights(), &wide);
        assert_eq!(geom(&placed, "a").width, 500.0);
    }

    #[test]
    fn edges_connect_every_child_to_its_parent() {
        let pipelines = [
            pipeline("source", None),
            pipeline("a", Some("source")),
            pipeline("b", Some("source")),
        ];
        assert_eq!(
            edges(&pipelines),
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
    fn a_route_runs_from_the_parent_bottom_to_the_child_top() {
        let from = card(0.0, 0.0, 100.0, 60.0);
        let to = card(200.0, 300.0, 100.0, 60.0);
        assert_eq!(sides_between(from, to), (Side::Bottom, Side::Top));

        let points = auto_route(
            port(from, Side::Bottom, 0, 1),
            Side::Bottom,
            port(to, Side::Top, 0, 1),
            Side::Top,
        );
        assert_eq!(points.first().map(|p| p.1), Some(60.0), "{points:?}");
        assert_eq!(points.last().map(|p| p.1), Some(300.0), "{points:?}");
        assert_axis_aligned(&points);
    }

    /// The point of the whole exercise: no diagonals, whatever the two cards
    /// are doing relative to each other.
    #[test]
    fn every_route_is_made_of_horizontal_and_vertical_segments() {
        let from = card(200.0, 200.0, 360.0, 200.0);
        for to in [
            card(200.0, 600.0, 360.0, 200.0),  // directly below
            card(900.0, 640.0, 360.0, 200.0),  // below and to the right
            card(-500.0, 620.0, 360.0, 200.0), // below and to the left
            card(900.0, 200.0, 360.0, 200.0),  // directly to the right
            card(-500.0, 240.0, 360.0, 200.0), // to the left
            card(200.0, -300.0, 360.0, 200.0), // above: a backwards edge
            card(220.0, 220.0, 360.0, 200.0),  // overlapping
            card(560.0, 400.0, 360.0, 200.0),  // corner to corner, no clearance
        ] {
            let (from_side, to_side) = sides_between(from, to);
            let points = auto_route(
                port(from, from_side, 0, 1),
                from_side,
                port(to, to_side, 0, 1),
                to_side,
            );
            assert!(points.len() >= 2, "no route to {to:?}");
            assert_axis_aligned(&points);
        }
    }

    /// A card dragged *level* with its parent is joined side to side. Coming
    /// out of the bottom and looping round would be both longer and harder to
    /// read.
    #[test]
    fn cards_side_by_side_are_joined_through_their_sides() {
        let from = card(0.0, 0.0, 360.0, 200.0);
        assert_eq!(
            sides_between(from, card(600.0, 0.0, 360.0, 200.0)),
            (Side::Right, Side::Left)
        );
        assert_eq!(
            sides_between(from, card(-600.0, 40.0, 360.0, 200.0)),
            (Side::Left, Side::Right)
        );
        // and a child dragged above its parent leaves by the top
        assert_eq!(
            sides_between(from, card(0.0, -600.0, 360.0, 200.0)),
            (Side::Top, Side::Bottom)
        );
    }

    /// The graph is a flow, so a child a row below its parent is joined
    /// downwards whether it sits underneath or right across the canvas.
    /// Choosing sideways here — because the horizontal distance happens to be
    /// larger — makes a row of siblings into a row of lines arriving sideways,
    /// which is what a fan-out should *not* look like.
    #[test]
    fn a_child_a_row_below_is_joined_downwards_however_far_to_the_side_it_is() {
        let from = card(0.0, 0.0, 360.0, 200.0);
        for to in [
            card(0.0, 400.0, 360.0, 200.0),     // straight below
            card(2000.0, 400.0, 360.0, 200.0),  // below, far to the right
            card(-2000.0, 400.0, 360.0, 200.0), // below, far to the left
        ] {
            assert_eq!(
                sides_between(from, to),
                (Side::Bottom, Side::Top),
                "a child at {to:?} was not joined downwards"
            );
        }
    }

    /// A face is only used if a route can actually get through it. A child a
    /// few pixels to the right is not "beside" its parent in any useful sense.
    #[test]
    fn a_face_with_no_room_in_front_of_it_is_not_used() {
        let from = card(0.0, 0.0, 360.0, 200.0);
        // overlapping vertically, and only a couple of pixels of horizontal gap
        let cramped = card(362.0, 20.0, 360.0, 200.0);
        assert_eq!(sides_between(from, cramped), (Side::Bottom, Side::Top));
    }

    /// Both ends of a route sit on a grid line, and so does the channel it runs
    /// along in between. Everything else here follows from that.
    #[test]
    fn ports_and_channels_land_on_grid_lines() {
        let from = card(0.0, 0.0, CARD_WIDTH, 200.0);
        let to = card(CARD_WIDTH + 200.0, 500.0, CARD_WIDTH, 200.0);
        let points = auto_route(
            port(from, Side::Bottom, 1, 3),
            Side::Bottom,
            port(to, Side::Top, 0, 2),
            Side::Top,
        );
        for (x, y) in &points {
            assert!(on_grid(*x) && on_grid(*y), "{:?} is off the grid", (x, y));
        }
    }

    /// A source feeding seven children is the shape `config.json` is built
    /// around: if every route left the same point, the fan-out would be
    /// invisible for the first hundred pixels.
    #[test]
    fn edges_sharing_a_face_leave_from_different_points() {
        let geom = card(0.0, 0.0, CARD_WIDTH, 200.0);
        let ports: HashSet<i64> = (0..7)
            .map(|i| (port(geom, Side::Bottom, i, 7).0 * 100.0) as i64)
            .collect();
        assert_eq!(ports.len(), 7, "ports collided: {ports:?}");
    }

    /// ...and they stay on the card they belong to, however many there are.
    #[test]
    fn a_crowded_face_keeps_its_ports_on_the_card() {
        let geom = card(0.0, 0.0, CARD_WIDTH, 200.0);
        for i in 0..40 {
            let (x, y) = port(geom, Side::Bottom, i, 40);
            assert!(
                (geom.x..=geom.x + geom.width).contains(&x),
                "port {i} at x={x} is off the card"
            );
            assert_eq!(y, geom.y + geom.height);
        }
    }

    /// Two cards that line up want one straight line, not a straight line
    /// described as three segments with two corners rounded in the middle of it.
    #[test]
    fn an_aligned_pair_is_joined_by_a_single_segment() {
        let from = card(0.0, 0.0, CARD_WIDTH, 200.0);
        let to = card(0.0, 500.0, CARD_WIDTH, 200.0);
        let points = auto_route(
            port(from, Side::Bottom, 0, 1),
            Side::Bottom,
            port(to, Side::Top, 0, 1),
            Side::Top,
        );
        assert_eq!(points, vec![(180.0, 200.0), (180.0, 500.0)]);
        assert_eq!(rounded_path(&points, CORNER), "M 180 200 L 180 500");
    }

    /// A corner is rounded by cutting into the segments either side of it, so a
    /// short segment has to shrink the radius rather than overshoot — two
    /// corners that each ate past the other would draw a line that doubles back.
    #[test]
    fn a_corner_radius_never_exceeds_the_segments_it_joins() {
        let tight = [(0.0, 0.0), (4.0, 0.0), (4.0, 4.0)];
        let path = rounded_path(&tight, CORNER);
        assert!(path.starts_with("M 0 0"), "{path}");
        // the bend is at most half of the 4px segments, so nothing runs past x=4
        for token in path
            .split_whitespace()
            .filter_map(|t| t.parse::<f64>().ok())
        {
            assert!(
                (0.0..=4.0).contains(&token),
                "{token} is outside the route: {path}"
            );
        }
    }

    #[test]
    fn a_route_with_no_points_has_no_path() {
        assert_eq!(rounded_path(&[], CORNER), "");
    }

    /// Two cards one above the other, offset sideways: the classic three-segment
    /// route, and the middle segment is the one the user gets to grab.
    fn stacked() -> (CardGeom, CardGeom) {
        (
            card(0.0, 0.0, CARD_WIDTH, 200.0),
            card(600.0, 600.0, CARD_WIDTH, 200.0),
        )
    }

    fn stacked_route(offset: f64) -> Route {
        let (from, to) = stacked();
        route(
            port(from, Side::Bottom, 0, 1),
            Side::Bottom,
            port(to, Side::Top, 0, 1),
            Side::Top,
            offset,
        )
    }

    /// The point of the whole feature: dragging the channel moves the middle
    /// segment, and nothing else about the route changes — both ends stay on
    /// the ports they belong to.
    #[test]
    fn an_offset_moves_the_channel_and_leaves_the_ends_alone() {
        let (automatic, pulled) = (stacked_route(0.0), stacked_route(-120.0));

        let channel = |r: &Route| r.channel.expect("a three-segment route has a channel").at.1;
        assert_eq!(channel(&pulled), channel(&automatic) - 120.0);

        assert_eq!(pulled.points.first(), automatic.points.first());
        assert_eq!(pulled.points.last(), automatic.points.last());
        assert_axis_aligned(&pulled.points);
    }

    /// Two routes between the same pair of rows are exactly the case this
    /// exists for: give one of them an offset and they no longer lie along the
    /// same line.
    #[test]
    fn two_routes_can_be_separated_by_offsetting_one_of_them() {
        let together = (stacked_route(0.0), stacked_route(0.0));
        assert_eq!(together.0.points, together.1.points, "same route, twice");

        let apart = (stacked_route(0.0), stacked_route(60.0));
        assert_ne!(apart.0.points, apart.1.points);
    }

    /// Dragged past either card the route would have to double back on itself,
    /// so the channel stops at the gap it lives in. Nothing is lost by it —
    /// there is nothing useful out there.
    #[test]
    fn a_channel_cannot_be_dragged_out_of_the_gap_between_the_cards() {
        let (from, to) = stacked();
        for offset in [-10_000.0, 10_000.0] {
            let routed = stacked_route(offset);
            let y = routed.channel.expect("a channel").at.1;
            assert!(
                (from.y + from.height..=to.y).contains(&y),
                "an offset of {offset} put the channel at {y}, outside the gap"
            );
            assert_axis_aligned(&routed.points);
        }
    }

    /// The channel is still on the grid after being dragged — an off-grid one
    /// would undo the thing that makes the routes look deliberate.
    #[test]
    fn a_dragged_channel_stays_on_the_grid() {
        for offset in [7.0, -13.0, 41.0] {
            let y = stacked_route(offset).channel.expect("a channel").at.1;
            assert!(on_grid(y), "a channel at {y} is off the grid");
        }
        // and the drag itself snaps, from wherever it started
        assert_eq!(dragged_channel(0.0, 33.0), 40.0);
        assert_eq!(dragged_channel(40.0, -7.0), 40.0);
        assert_eq!(dragged_channel(13.0, 0.0), 20.0);
    }

    /// The handle spans the segment rather than sitting at one end of it: the
    /// line is what the eye follows, so the line is what the hand should get.
    #[test]
    fn the_channel_handle_covers_the_middle_segment() {
        let routed = stacked_route(0.0);
        let channel = routed.channel.expect("a channel");
        assert!(channel.vertical, "a bottom-to-top route runs vertically");

        // the segment between the two turns, which is exactly what it spans
        let turns: Vec<(f64, f64)> = routed
            .points
            .iter()
            .filter(|p| near(p.1, channel.at.1))
            .copied()
            .collect();
        assert_eq!(turns.len(), 2, "expected two corners on the channel");
        assert_eq!(channel.length, (turns[1].0 - turns[0].0).abs());
        assert_eq!(channel.at.0, (turns[0].0 + turns[1].0) / 2.0);
    }

    /// A route between two cards that line up is one straight segment; there is
    /// no middle to move, and offering a handle that does nothing would be
    /// worse than offering none.
    #[test]
    fn a_straight_route_has_no_channel_to_grab() {
        let from = card(0.0, 0.0, CARD_WIDTH, 200.0);
        let to = card(0.0, 600.0, CARD_WIDTH, 200.0);
        let routed = route(
            port(from, Side::Bottom, 0, 1),
            Side::Bottom,
            port(to, Side::Top, 0, 1),
            Side::Top,
            0.0,
        );
        assert_eq!(routed.channel, None);
    }

    /// Nor does an L-shaped one, where the faces are at right angles: it has
    /// two segments and both are anchored to a port.
    #[test]
    fn a_right_angled_route_has_no_channel_to_grab() {
        let from = card(0.0, 0.0, CARD_WIDTH, 200.0);
        let to = card(900.0, 600.0, CARD_WIDTH, 200.0);
        let routed = route(
            port(from, Side::Bottom, 0, 1),
            Side::Bottom,
            port(to, Side::Left, 0, 1),
            Side::Left,
            0.0,
        );
        assert_eq!(routed.channel, None);
    }

    /// A horizontal route is the mirror image: its channel is a vertical bar
    /// that slides left and right.
    #[test]
    fn a_side_to_side_route_has_a_channel_that_slides_sideways() {
        let from = card(0.0, 0.0, CARD_WIDTH, 200.0);
        // level with each other, so there is no room to route below
        let to = card(900.0, 100.0, CARD_WIDTH, 200.0);
        assert_eq!(sides_between(from, to), (Side::Right, Side::Left));

        let at = |offset| {
            route(
                port(from, Side::Right, 0, 1),
                Side::Right,
                port(to, Side::Left, 0, 1),
                Side::Left,
                offset,
            )
            .channel
            .expect("a channel")
        };
        assert!(!at(0.0).vertical);
        assert_eq!(at(-100.0).at.0, at(0.0).at.0 - 100.0);
        // ...and it is the x that moves, not the y
        assert_eq!(at(-100.0).at.1, at(0.0).at.1);
    }

    /// A fan-out: one source feeding three children, which is the shape where
    /// pinning one port could plausibly disturb the others.
    struct Fan {
        pipelines: Vec<(PipelineId, Vec<PipelineId>)>,
        placed: HashMap<PipelineId, CardGeom>,
    }

    fn fan() -> Fan {
        let pipelines = vec![
            pipeline("root", None),
            pipeline("a", Some("root")),
            pipeline("b", Some("root")),
            pipeline("c", Some("root")),
        ];
        let placed = lay(&pipelines, &no_heights());
        Fan { pipelines, placed }
    }

    fn drawn(paths: &[EdgePath], to: &str) -> EdgePath {
        paths
            .iter()
            .find(|p| p.edge.to == to)
            .cloned()
            .expect("edge not drawn")
    }

    /// The point of the feature: an edge attaches where it was told to, and the
    /// route starts from there.
    #[test]
    fn a_pinned_port_puts_the_edge_where_it_was_told() {
        let Fan { pipelines, placed } = fan();
        let mut arrangement = LayoutFile::default();
        arrangement.set_edge_port(
            "root",
            "a",
            EdgeEnd::From,
            Some(PortLayout {
                side: Side::Bottom,
                along: 40.0,
            }),
        );

        let pinned = drawn(&edge_paths(&pipelines, &placed, &arrangement), "a");
        let root = placed["root"];
        assert!(pinned.from_port.pinned);
        assert_eq!(pinned.from_port.at, (root.x + 40.0, root.y + root.height));
        assert!(
            pinned
                .path
                .starts_with(&format!("M {} {}", root.x + 40.0, root.y + root.height)),
            "the route does not start at the pinned port: {}",
            pinned.path
        );
    }

    /// Pinning one edge's port must not shuffle its siblings: the fan-out
    /// spreads the *remaining* ends over the face as if the pinned one weren't
    /// there, rather than counting it and squeezing up around a gap.
    #[test]
    fn pinning_one_port_does_not_move_the_others() {
        let Fan { pipelines, placed } = fan();
        let automatic = edge_paths(&pipelines, &placed, &LayoutFile::default());

        let mut arrangement = LayoutFile::default();
        arrangement.set_edge_port(
            "root",
            "b",
            EdgeEnd::From,
            Some(PortLayout {
                side: Side::Bottom,
                along: 20.0,
            }),
        );
        let adjusted = edge_paths(&pipelines, &placed, &arrangement);

        // 'b' moved to where it was put...
        assert_ne!(
            drawn(&automatic, "b").from_port.at,
            drawn(&adjusted, "b").from_port.at
        );
        // ...and the other two ends are exactly where they were
        for sibling in ["a", "c"] {
            assert_eq!(
                drawn(&automatic, sibling).from_port.at,
                drawn(&adjusted, sibling).from_port.at,
                "'{sibling}' moved when 'b' was pinned"
            );
        }
    }

    /// A pinned position is measured along one face, and the router is free to
    /// change its mind about which face to use. When it does, the old number
    /// means nothing and the edge goes back to being placed automatically —
    /// which is self-healing, and needs no cleanup pass over the file.
    #[test]
    fn a_pinned_port_on_a_face_the_route_no_longer_uses_is_ignored() {
        let Fan { pipelines, placed } = fan();
        // pinned on the left face; the route actually leaves by the bottom
        let mut stale = LayoutFile::default();
        stale.set_edge_port(
            "root",
            "a",
            EdgeEnd::From,
            Some(PortLayout {
                side: Side::Left,
                along: 40.0,
            }),
        );

        let automatic = drawn(
            &edge_paths(&pipelines, &placed, &LayoutFile::default()),
            "a",
        );
        let with_stale = drawn(&edge_paths(&pipelines, &placed, &stale), "a");
        assert_eq!(with_stale.from_port.at, automatic.from_port.at);
        assert!(!with_stale.from_port.pinned);
    }

    /// Both ends are adjustable, and independently: the arriving end of an edge
    /// is as likely to be the confusing one as the leaving end.
    #[test]
    fn the_arriving_end_can_be_pinned_too() {
        let Fan { pipelines, placed } = fan();
        let mut arrangement = LayoutFile::default();
        arrangement.set_edge_port(
            "root",
            "a",
            EdgeEnd::To,
            Some(PortLayout {
                side: Side::Top,
                along: 300.0,
            }),
        );

        let edge = drawn(&edge_paths(&pipelines, &placed, &arrangement), "a");
        let child = placed["a"];
        assert_eq!(edge.to_port.at, (child.x + 300.0, child.y));
        assert!(edge.to_port.pinned);
        // the leaving end is untouched
        assert!(!edge.from_port.pinned);
    }

    /// A port can't be dragged off its card, or past the corner where the line
    /// would stop reading as attached to that face at all.
    #[test]
    fn a_port_stays_on_its_face() {
        let geom = card(0.0, 0.0, CARD_WIDTH, 200.0);
        for along in [-500.0, 0.0, 5_000.0] {
            let (x, y) = port_at(geom, Side::Bottom, along);
            assert!(
                (geom.x + GRID..=geom.x + geom.width - GRID).contains(&x),
                "an `along` of {along} put the port at x={x}"
            );
            assert_eq!(y, geom.y + geom.height);
        }
        // ...and a drag lands on the grid, wherever the pointer stopped
        assert_eq!(dragged_port(60.0, 13.0, CARD_WIDTH), 80.0);
        assert_eq!(dragged_port(60.0, -9_000.0, CARD_WIDTH), GRID);
        assert_eq!(dragged_port(60.0, 9_000.0, CARD_WIDTH), CARD_WIDTH - GRID);
    }

    /// The handle has to report the face it is on and how long that face is, or
    /// a drag has nothing to clamp against.
    #[test]
    fn a_port_handle_describes_the_face_it_is_on() {
        let Fan { pipelines, placed } = fan();
        let edge = drawn(
            &edge_paths(&pipelines, &placed, &LayoutFile::default()),
            "a",
        );

        assert_eq!(edge.from_port.side, Side::Bottom);
        assert_eq!(edge.from_port.length, placed["root"].width);
        assert_eq!(edge.to_port.side, Side::Top);
        assert_eq!(edge.to_port.length, placed["a"].width);
        // and `along` is where it says it is
        assert_eq!(
            edge.from_port.at,
            port_at(placed["root"], Side::Bottom, edge.from_port.along)
        );
    }

    /// The layout file reaches the routes: an adjusted edge is drawn where it
    /// was put, exactly, and the automatic separation works around it rather
    /// than over it.
    #[test]
    fn an_adjusted_edge_is_drawn_where_it_was_put() {
        let pipelines = [
            pipeline("root", None),
            pipeline("a", Some("root")),
            pipeline("b", Some("root")),
        ];
        let placed = lay(&pipelines, &no_heights());

        let automatic = edge_paths(&pipelines, &placed, &LayoutFile::default());
        let mut arrangement = LayoutFile::default();
        arrangement.set_edge_offset("root", "a", Some(60.0));
        let adjusted = edge_paths(&pipelines, &placed, &arrangement);

        assert_ne!(drawn(&automatic, "a").path, drawn(&adjusted, "a").path);
        assert_eq!(drawn(&adjusted, "a").channel_offset, 60.0);
        assert_no_overlapping_channels(&adjusted);
    }

    /// Every channel the automatic separation places, as the separator sees
    /// them — which is what "these two lines are drawn on top of each other"
    /// means precisely.
    fn channel_segments(paths: &[EdgePath]) -> Vec<(&Edge, Segment)> {
        paths
            .iter()
            .filter_map(|p| p.channel.map(|c| (&p.edge, Segment::of(c))))
            .collect()
    }

    fn assert_no_overlapping_channels(paths: &[EdgePath]) {
        let segments = channel_segments(paths);
        for (i, (edge, segment)) in segments.iter().enumerate() {
            for (other_edge, other) in &segments[i + 1..] {
                assert!(
                    !segment.overlaps(other),
                    "{}→{} and {}→{} are drawn along the same line",
                    edge.from,
                    edge.to,
                    other_edge.from,
                    other_edge.to
                );
            }
        }
    }

    /// The default this change is about: a fan-out is several routes between the
    /// same two rows, every one of them wanting the same half-way line. Left to
    /// themselves they are drawn as one thick line with a few stubs coming out
    /// of it — so they are spread onto lines of their own.
    #[test]
    fn a_fan_out_does_not_draw_its_routes_on_top_of_each_other() {
        let pipelines: Vec<_> = std::iter::once(pipeline("root", None))
            .chain(["a", "b", "c", "d", "e"].map(|id| pipeline(id, Some("root"))))
            .collect();
        let placed = lay(&pipelines, &no_heights());

        let paths = edge_paths(&pipelines, &placed, &LayoutFile::default());
        assert_eq!(paths.len(), 5);
        assert_no_overlapping_channels(&paths);
        // and each of them is still on the grid. The child directly under the
        // root is a straight line with no channel at all, which is why this
        // filters rather than unwraps.
        for (edge, segment) in channel_segments(&paths) {
            assert!(
                on_grid(segment.line),
                "the channel of {}→{} is off the grid at {}",
                edge.from,
                edge.to,
                segment.line
            );
        }
    }

    /// A join is the same problem upside down, and the same answer: several
    /// parents feeding one child, arriving on lines of their own.
    ///
    /// Placed by hand rather than laid out, because the automatic layout centres
    /// a child under its parents and the routes then leave in opposite
    /// directions. Parents stacked to one side of their child is the shape where
    /// they all run the same way along the same line.
    #[test]
    fn a_join_does_not_draw_its_routes_on_top_of_each_other() {
        let pipelines = vec![
            pipeline("a", None),
            pipeline("b", None),
            pipeline("c", None),
            pipeline_with("sink", &["a", "b", "c"]),
        ];
        let placed = HashMap::from([
            ("a".to_string(), card(0.0, 0.0, CARD_WIDTH, 200.0)),
            ("b".to_string(), card(400.0, 0.0, CARD_WIDTH, 200.0)),
            ("c".to_string(), card(800.0, 0.0, CARD_WIDTH, 200.0)),
            ("sink".to_string(), card(2000.0, 600.0, CARD_WIDTH, 200.0)),
        ]);

        assert_no_overlapping_channels(&edge_paths(&pipelines, &placed, &LayoutFile::default()));
    }

    /// Separation is a last resort, not a style: an edge with the half-way line
    /// to itself stays on it, so a graph with room to spare is drawn exactly as
    /// it was before there was anything to separate.
    #[test]
    fn a_channel_with_nothing_in_its_way_stays_on_the_half_way_line() {
        let pipelines = vec![
            pipeline("left", None),
            pipeline("left-child", Some("left")),
            pipeline("right", None),
            pipeline("right-child", Some("right")),
        ];
        // two pairs, far enough apart that the two channels share a line but
        // not a stretch of it
        let placed = HashMap::from([
            ("left".to_string(), card(0.0, 0.0, CARD_WIDTH, 200.0)),
            (
                "left-child".to_string(),
                card(400.0, 600.0, CARD_WIDTH, 200.0),
            ),
            ("right".to_string(), card(4000.0, 0.0, CARD_WIDTH, 200.0)),
            (
                "right-child".to_string(),
                card(4400.0, 600.0, CARD_WIDTH, 200.0),
            ),
        ]);

        let paths = edge_paths(&pipelines, &placed, &LayoutFile::default());
        for path in &paths {
            assert_eq!(
                path.channel_offset, 0.0,
                "{}→{} was moved off the half-way line with nothing in its way",
                path.edge.from, path.edge.to
            );
        }
    }

    /// Where the line is drawn is what a drag on it has to start from. An
    /// automatically separated channel reports the offset it was placed at, so
    /// grabbing it carries on from there instead of snapping back to half way.
    #[test]
    fn a_route_reports_the_offset_it_was_drawn_at() {
        let pipelines: Vec<_> = std::iter::once(pipeline("root", None))
            .chain(["a", "b", "c"].map(|id| pipeline(id, Some("root"))))
            .collect();
        let placed = lay(&pipelines, &no_heights());

        for path in edge_paths(&pipelines, &placed, &LayoutFile::default()) {
            let Some(channel) = path.channel else { continue };
            let expected = route(
                path.from_port.at,
                path.from_port.side,
                path.to_port.at,
                path.to_port.side,
                path.channel_offset,
            );
            assert_eq!(expected.channel, Some(channel));
        }
    }

    /// The whole point of `edge_paths`: an edge is a function of where the
    /// cards *currently* are, so a card that changes height has to redraw both
    /// the edge above it and the ones below it.
    #[test]
    fn edge_paths_follow_a_card_that_changes_height() {
        let pipelines = [
            pipeline("source", None),
            pipeline("a", Some("source")),
            pipeline("b", Some("a")),
        ];

        let small = edge_paths(
            &pipelines,
            &lay(&pipelines, &no_heights()),
            &LayoutFile::default(),
        );
        let grown = edge_paths(
            &pipelines,
            &lay(&pipelines, &HashMap::from([("a".to_string(), 600.0)])),
            &LayoutFile::default(),
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
        let pipelines = [pipeline("a", None), pipeline("b", Some("a"))];
        assert!(edge_paths(&pipelines, &HashMap::new(), &LayoutFile::default()).is_empty());
    }

    fn batch_event(pipeline_id: &str, stage: Stage) -> UiEvent {
        UiEvent::batch(pipeline_id.to_string(), stage, &Vec::new(), 0)
    }

    /// A batch arriving at a downstream pipeline is a batch that crossed the
    /// edge above it, which is the whole signal the blink is built on.
    #[test]
    fn an_input_event_lights_the_edge_from_its_upstream() {
        let pipelines = [pipeline("a", None), pipeline("b", Some("a"))];
        assert_eq!(
            pulsed_edges(&batch_event("b", Stage::Input), &pipelines),
            vec![Edge {
                from: "a".to_string(),
                to: "b".to_string()
            }]
        );
    }

    /// The event says a batch arrived, not which input brought it, so a pipeline
    /// with two upstreams lights both edges rather than guessing.
    #[test]
    fn an_input_event_lights_every_incoming_edge() {
        let pipelines = [
            pipeline("a", None),
            pipeline("b", None),
            pipeline_with("c", &["a", "b"]),
        ];
        assert_eq!(
            pulsed_edges(&batch_event("c", Stage::Input), &pipelines),
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

    /// A pipeline emits to its output whether or not anything is listening, so
    /// an output event says nothing about an edge.
    #[test]
    fn an_output_event_lights_nothing() {
        let pipelines = [pipeline("a", None), pipeline("b", Some("a"))];
        assert!(pulsed_edges(&batch_event("b", Stage::Output), &pipelines).is_empty());
    }

    /// A failure at the input stage is the *absence* of a batch, so it must not
    /// light the edge an arriving batch would have.
    #[test]
    fn an_error_event_lights_nothing() {
        let pipelines = [pipeline("a", None), pipeline("b", Some("a"))];
        let failed = UiEvent::error("b".to_string(), Stage::Input, &"upstream went away");
        assert!(pulsed_edges(&failed, &pipelines).is_empty());
    }

    /// A root is fed from outside the graph — NATS, a timer — and has no edge
    /// coming into it to light up.
    #[test]
    fn an_input_event_on_a_root_lights_nothing() {
        let pipelines = [pipeline("a", None), pipeline("b", Some("a"))];
        assert!(pulsed_edges(&batch_event("a", Stage::Input), &pipelines).is_empty());
        // and a pipeline that isn't on the canvas at all can't light anything
        assert!(pulsed_edges(&batch_event("ghost", Stage::Input), &pipelines).is_empty());
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

    /// The geometry is in surface pixels, so it has to be divided by the zoom
    /// the surface is scaled by: at 200% a viewport of 800px is 400 surface
    /// pixels wide, and a card given 800 would hang half a screen off the right.
    #[test]
    fn a_maximized_card_covers_the_viewport_at_any_zoom() {
        for zoom in [0.5, 1.0, 2.0] {
            let camera = Camera {
                x: 120.0,
                y: -40.0,
                zoom,
            };
            let geom = maximized_geom(camera, (800.0, 600.0));
            assert_eq!(
                geom,
                CardGeom {
                    x: 120.0,
                    y: -40.0,
                    width: 800.0 / zoom,
                    height: 600.0 / zoom,
                },
                "at zoom {zoom}"
            );
        }
    }

    /// It is glued to the viewport rather than to the graph: panning moves the
    /// card with the camera, so it stays where it was put on screen.
    #[test]
    fn a_maximized_card_follows_the_camera() {
        let viewport = (800.0, 600.0);
        let before = maximized_geom(Camera::default(), viewport);
        let after = maximized_geom(
            Camera {
                x: 300.0,
                y: 200.0,
                ..Camera::default()
            },
            viewport,
        );
        assert_eq!(after.x - before.x, 300.0);
        assert_eq!(after.y - before.y, 200.0);
        assert_eq!((after.width, after.height), (before.width, before.height));
    }

    #[test]
    fn a_pipeline_input_is_the_only_kind_with_an_upstream() {
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
            upstreams_of(&config_of(
                vec![upstream_input("p1"), upstream_input("p2"),]
            )),
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
        let pipelines = vec![
            ("p1".to_string(), config_of(vec![dummy_input()])),
            (
                "child".to_string(),
                config_of(vec![upstream_input("p1"), upstream_input("p1")]),
            ),
        ];
        assert_eq!(
            pipelines_from(&pipelines),
            vec![
                ("p1".to_string(), vec![]),
                ("child".to_string(), vec!["p1".to_string()]),
            ]
        );
    }

    fn upstream_input(upstream: &str) -> kayak_core::config::InputConfig {
        use kayak_core::config::PipelineConfig;
        kayak_core::config::InputConfig {
            kind: InputKind::Pipeline(PipelineConfig {
                upstream: upstream.to_string(),
            }),
            buffer: None,
        }
    }

    fn dummy_input() -> kayak_core::config::InputConfig {
        use kayak_core::config::DummyConfig;
        kayak_core::config::InputConfig {
            kind: InputKind::Dummy(DummyConfig { duration: 1, payload: None, amplitude: None, period: None }),
            buffer: None,
        }
    }

    fn config_of(inputs: Vec<kayak_core::config::InputConfig>) -> Config {
        use kayak_core::config::{OutputConfig, OutputKind, StdoutOutputConfig};
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
