//! Where the user has dragged things to on the canvas.
//!
//! This is *not* configuration. Nothing here changes what the server runs: a
//! pipeline with a position and a pipeline without one build the same runtime,
//! and a layout naming a pipeline that no longer exists is ignored rather than
//! being an error. That separation is the whole reason it lives in its own file
//! on disk — a config file describes a system, and a config file with pixel
//! coordinates in it stops being reviewable.
//!
//! It is still meant to be committed, though, which is why the file is written
//! deterministically (a `BTreeMap`, so ids come out in one order) and why an
//! entry only exists once someone has actually moved the pipeline. A graph nobody
//! has arranged by hand has an empty layout file, or none at all, and the
//! canvas lays it out automatically.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::PipelineId;

/// Bumped if the shape below ever changes incompatibly. A file from the future
/// is still read — every field is optional or defaulted — so this exists to be
/// *reported*, not to gate parsing.
pub const LAYOUT_VERSION: u32 = 1;

/// One card's place on the canvas, in surface coordinates (the same space the
/// automatic layout works in — pixels at zoom 1, origin at the top-left of the
/// graph).
///
/// `height` is optional because the two ways a card gets its height are
/// different things: normally it is *measured* from the content it renders, and
/// only an explicit resize pins it. `None` means "however tall it needs to be",
/// which is what you want a card to go back to when its config grows.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
pub struct PipelineLayout {
    pub x: f64,
    pub y: f64,
    pub width: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<f64>,
}

/// A face of a card. Which one an edge uses is worked out from where the two
/// cards sit and is never stored — but *where along it* the edge attaches can
/// be, and a stored position only means anything together with the face it was
/// measured on.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Top,
    Right,
    Bottom,
    Left,
}

impl Side {
    /// Whether a route through this face runs vertically. The two axes are
    /// mirror images of each other, so most of the geometry is written once and
    /// asks this.
    #[must_use]
    pub fn is_vertical(self) -> bool {
        matches!(self, Self::Top | Self::Bottom)
    }
}

/// Which end of an edge something belongs to. The two ends are handled
/// identically everywhere, so they are a value rather than two code paths.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeEnd {
    From,
    To,
}

/// Where an edge attaches to a card, once someone has said so by hand.
///
/// `along` is measured from the start of the face — its left end for a top or
/// bottom face, its top end for a left or right one — rather than as a fraction
/// of it. A distance is what the drag meant: "attach a card's width in from the
/// corner" stays put when the card is made taller, where a fraction would slide.
///
/// `side` is carried because the automatic router can change its mind: move a
/// card and an edge that left by the bottom may now leave by the side, and a
/// position measured along the old face means nothing on the new one. When they
/// disagree the stored position is ignored and the edge goes back to being
/// placed automatically, which is self-healing and needs no cleanup pass.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
pub struct PortLayout {
    pub side: Side,
    pub along: f64,
}

/// One edge's routing, where the automatic answer wasn't the readable one.
///
/// An edge between two cards runs out of one, along a channel half way between
/// them, and into the other. `offset` moves that channel off the half-way line
/// — pulled towards one card or the other — which is how two routes that would
/// otherwise share a channel and lie on top of each other get separated. The
/// two ports say where on their cards the edge attaches.
///
/// The offset is *relative* rather than an absolute coordinate, because cards
/// move: storing "20 above the middle" keeps the adjustment meaningful after
/// either end is dragged, where storing a y would eventually put the channel
/// outside the gap it was meant to sit in.
///
/// Listed rather than keyed by a joined id, so nothing has to be escaped and no
/// id character is special.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Default)]
pub struct EdgeAdjustment {
    #[serde(default, skip_serializing_if = "is_zero")]
    pub offset: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_port: Option<PortLayout>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_port: Option<PortLayout>,
}

impl EdgeAdjustment {
    /// Nothing has been adjusted, so there is nothing worth writing down.
    #[must_use]
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }

    #[must_use]
    pub fn port(&self, end: EdgeEnd) -> Option<PortLayout> {
        match end {
            EdgeEnd::From => self.from_port,
            EdgeEnd::To => self.to_port,
        }
    }

    fn port_mut(&mut self, end: EdgeEnd) -> &mut Option<PortLayout> {
        match end {
            EdgeEnd::From => &mut self.from_port,
            EdgeEnd::To => &mut self.to_port,
        }
    }
}

fn is_zero(value: &f64) -> bool {
    *value == 0.0
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct EdgeLayout {
    pub from: PipelineId,
    pub to: PipelineId,
    #[serde(flatten)]
    pub adjustment: EdgeAdjustment,
}

/// The whole file: every pipeline someone has placed by hand, and every edge whose
/// route they have adjusted.
///
/// Absent ids are the normal case, not a gap to be filled — the canvas lays
/// those out itself. A pipeline is added here the first time it is dragged and
/// removed when the arrangement is reset.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LayoutFile {
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default)]
    pub pipelines: BTreeMap<PipelineId, PipelineLayout>,
    /// Kept sorted by `(from, to)` — see [`LayoutFile::set_edge_offset`] — so
    /// the file stays diffable.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub edges: Vec<EdgeLayout>,
}

fn default_version() -> u32 {
    LAYOUT_VERSION
}

impl Default for LayoutFile {
    fn default() -> Self {
        Self {
            version: LAYOUT_VERSION,
            pipelines: BTreeMap::new(),
            edges: Vec::new(),
        }
    }
}

impl LayoutFile {
    #[must_use]
    pub fn get(&self, id: &str) -> Option<PipelineLayout> {
        self.pipelines.get(id).copied()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pipelines.is_empty() && self.edges.is_empty()
    }

    /// Everything that has been adjusted about one edge. All defaults for an
    /// edge nobody has touched, which is most of them.
    #[must_use]
    pub fn edge(&self, from: &str, to: &str) -> EdgeAdjustment {
        self.find_edge(from, to)
            .map_or_else(EdgeAdjustment::default, |i| self.edges[i].adjustment)
    }

    /// How far this edge's channel has been pulled off the half-way line.
    #[must_use]
    pub fn edge_offset(&self, from: &str, to: &str) -> f64 {
        self.edge(from, to).offset
    }

    /// Move an edge's channel.
    pub fn set_edge_offset(&mut self, from: &str, to: &str, offset: f64) {
        self.adjust_edge(from, to, |a| a.offset = offset);
    }

    /// Pin where an edge attaches to one of its cards, or — with `None` — put
    /// that end back to being placed automatically.
    pub fn set_edge_port(&mut self, from: &str, to: &str, end: EdgeEnd, port: Option<PortLayout>) {
        self.adjust_edge(from, to, |a| *a.port_mut(end) = port);
    }

    /// Change one edge's adjustments, creating the entry if it needs one and
    /// **removing it again once nothing is adjusted**.
    ///
    /// That last part is the reason this is one function rather than three:
    /// putting a channel back where it started, or unpinning the last pinned
    /// port, is undoing an adjustment, and a committed file full of no-op
    /// entries would be noise in every diff.
    fn adjust_edge(&mut self, from: &str, to: &str, change: impl FnOnce(&mut EdgeAdjustment)) {
        let mut adjustment = self.edge(from, to);
        change(&mut adjustment);

        match (self.find_edge(from, to), adjustment.is_default()) {
            (Some(i), true) => {
                self.edges.remove(i);
            }
            (Some(i), false) => self.edges[i].adjustment = adjustment,
            (None, true) => {}
            (None, false) => {
                let edge = EdgeLayout {
                    from: from.to_string(),
                    to: to.to_string(),
                    adjustment,
                };
                // kept sorted rather than appended, so the file is a function
                // of the arrangement and not of the order it was made in
                let at = self
                    .edges
                    .partition_point(|e| (&e.from, &e.to) < (&edge.from, &edge.to));
                self.edges.insert(at, edge);
            }
        }
    }

    fn find_edge(&self, from: &str, to: &str) -> Option<usize> {
        self.edges.iter().position(|e| e.from == from && e.to == to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The file is committed and diffed, so it has to be stable and readable:
    /// ids in one order, no `"height": null` noise on cards nobody resized.
    #[test]
    fn a_layout_round_trips_and_omits_an_unset_height() -> serde_json::Result<()> {
        let mut layout = LayoutFile::default();
        layout.pipelines.insert(
            "zed".to_string(),
            PipelineLayout {
                x: 40.0,
                y: 80.0,
                width: 360.0,
                height: None,
            },
        );
        layout.pipelines.insert(
            "alpha".to_string(),
            PipelineLayout {
                x: 0.0,
                y: 0.0,
                width: 360.0,
                height: Some(300.0),
            },
        );

        let json = serde_json::to_string_pretty(&layout)?;
        assert!(
            !json.contains("null"),
            "an unset height was written: {json}"
        );
        assert!(
            json.find("alpha") < json.find("zed"),
            "ids are not in a stable order: {json}"
        );
        assert_eq!(serde_json::from_str::<LayoutFile>(&json)?, layout);
        Ok(())
    }

    /// The list is kept in one order however the adjustments were made, same
    /// reason as the pipelines: the file is committed, and a diff should mean the
    /// arrangement changed.
    #[test]
    fn adjusted_edges_are_stored_in_a_stable_order() {
        let mut layout = LayoutFile::default();
        layout.set_edge_offset("root", "zed", 40.0);
        layout.set_edge_offset("alpha", "child", -20.0);
        layout.set_edge_offset("root", "abel", 60.0);

        let order: Vec<(&str, &str)> = layout
            .edges
            .iter()
            .map(|e| (e.from.as_str(), e.to.as_str()))
            .collect();
        assert_eq!(
            order,
            [("alpha", "child"), ("root", "abel"), ("root", "zed")]
        );
    }

    /// Adjusting the same edge twice replaces the offset, and putting it back
    /// where it started removes the entry — an undone adjustment shouldn't
    /// leave a no-op behind in a committed file.
    #[test]
    fn readjusting_an_edge_replaces_it_and_zero_removes_it() {
        let mut layout = LayoutFile::default();
        layout.set_edge_offset("a", "b", 40.0);
        layout.set_edge_offset("a", "b", 80.0);
        assert_eq!(layout.edges.len(), 1);
        assert_eq!(layout.edge_offset("a", "b"), 80.0);

        layout.set_edge_offset("a", "b", 0.0);
        assert!(layout.edges.is_empty());
        assert_eq!(layout.edge_offset("a", "b"), 0.0);
        // and an edge nobody has touched is simply the automatic route
        assert_eq!(layout.edge_offset("never", "touched"), 0.0);
    }

    fn port(side: Side, along: f64) -> PortLayout {
        PortLayout { side, along }
    }

    /// The two adjustments live on one entry and are independent: pinning a
    /// port must not move the channel, and vice versa.
    #[test]
    fn an_edge_carries_its_channel_and_its_two_ports_independently() {
        let mut layout = LayoutFile::default();
        layout.set_edge_offset("a", "b", 40.0);
        layout.set_edge_port("a", "b", EdgeEnd::From, Some(port(Side::Bottom, 60.0)));
        layout.set_edge_port("a", "b", EdgeEnd::To, Some(port(Side::Top, 120.0)));

        assert_eq!(layout.edges.len(), 1, "one entry per edge");
        let edge = layout.edge("a", "b");
        assert_eq!(edge.offset, 40.0);
        assert_eq!(edge.port(EdgeEnd::From), Some(port(Side::Bottom, 60.0)));
        assert_eq!(edge.port(EdgeEnd::To), Some(port(Side::Top, 120.0)));
    }

    /// The entry only goes away once *everything* about the edge is back to
    /// automatic — undoing one adjustment must not silently drop the other.
    #[test]
    fn an_edge_entry_survives_until_every_adjustment_is_undone() {
        let mut layout = LayoutFile::default();
        layout.set_edge_offset("a", "b", 40.0);
        layout.set_edge_port("a", "b", EdgeEnd::From, Some(port(Side::Bottom, 60.0)));

        layout.set_edge_offset("a", "b", 0.0);
        assert_eq!(
            layout.edges.len(),
            1,
            "the pinned port was dropped with the offset"
        );
        assert_eq!(
            layout.edge("a", "b").port(EdgeEnd::From),
            Some(port(Side::Bottom, 60.0))
        );

        layout.set_edge_port("a", "b", EdgeEnd::From, None);
        assert!(layout.edges.is_empty(), "the empty entry was left behind");
    }

    /// A face is part of a pinned port, and it is written in the file as a name
    /// rather than a number — the file is read by people.
    #[test]
    fn a_pinned_port_names_its_face() -> serde_json::Result<()> {
        let mut layout = LayoutFile::default();
        layout.set_edge_port("a", "b", EdgeEnd::To, Some(port(Side::Left, 40.0)));

        let json = serde_json::to_string(&layout)?;
        assert!(json.contains(r#""side":"left""#), "got: {json}");
        assert_eq!(serde_json::from_str::<LayoutFile>(&json)?, layout);
        Ok(())
    }

    /// Edges are addressed by both ends, and direction matters: `a → b` and
    /// `b → a` are different edges that could both exist.
    #[test]
    fn edges_are_distinguished_by_both_ends_and_direction() {
        let mut layout = LayoutFile::default();
        layout.set_edge_offset("a", "b", 40.0);
        layout.set_edge_offset("b", "a", -40.0);
        layout.set_edge_offset("a", "c", 20.0);

        assert_eq!(layout.edge_offset("a", "b"), 40.0);
        assert_eq!(layout.edge_offset("b", "a"), -40.0);
        assert_eq!(layout.edge_offset("a", "c"), 20.0);
    }

    /// A hand-written or truncated file shouldn't need every key: the missing
    /// parts have obvious answers, and refusing to parse would cost someone
    /// their arrangement over a formatting slip.
    #[test]
    fn a_minimal_file_parses() -> serde_json::Result<()> {
        let parsed: LayoutFile = serde_json::from_str("{}")?;
        assert_eq!(parsed, LayoutFile::default());

        let parsed: LayoutFile =
            serde_json::from_str(r#"{"pipelines":{"a":{"x":1,"y":2,"width":3}}}"#)?;
        assert_eq!(parsed.version, LAYOUT_VERSION);
        assert_eq!(
            parsed.get("a"),
            Some(PipelineLayout {
                x: 1.0,
                y: 2.0,
                width: 3.0,
                height: None
            })
        );
        Ok(())
    }
}
