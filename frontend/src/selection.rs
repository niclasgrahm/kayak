//! Which pipelines the canvas is working with, and how a click changes that.
//!
//! A set rather than an `Option<PipelineId>` because in edit mode a graph is
//! arranged in handfuls: dragging twenty cards into place one at a time is the
//! thing this exists to stop. Read-only never grows a selection past one — the
//! only reason to name a pipeline there is to look at it — so nothing outside
//! edit mode calls [`Selection::toggle`].
//!
//! Pure and unit-tested here, same convention as `graph.rs` and `sidebar.rs`;
//! `app.rs` holds it in a signal and does the clicking.

use std::collections::BTreeSet;

use kayak_core::PipelineId;

/// The selected pipelines.
///
/// A `BTreeSet` rather than a `Vec`: a pipeline is selected or it isn't, and
/// clicking the same card twice must not put it in twice. The order it iterates
/// in is the id order, which is arbitrary but stable — nothing downstream cares
/// which card of a group a drag started from, since they all move by the same
/// delta.
#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub struct Selection(BTreeSet<PipelineId>);

impl Selection {
    /// The selection holding exactly `id`, which is what a plain click makes.
    #[must_use]
    pub fn only(id: &PipelineId) -> Self {
        Self(std::iter::once(id.clone()).collect())
    }

    #[must_use]
    pub fn of<I: IntoIterator<Item = PipelineId>>(ids: I) -> Self {
        Self(ids.into_iter().collect())
    }

    #[must_use]
    pub fn contains(&self, id: &str) -> bool {
        self.0.contains(id)
    }

    /// Whether this is exactly `id` and nothing else.
    ///
    /// The guard on the "select only this one" write: `update`-style writes mark
    /// a signal dirty whether or not the value moved, and a mousedown on the
    /// card that is already the whole selection would otherwise re-run every
    /// card's memo for nothing.
    #[must_use]
    pub fn is_only(&self, id: &str) -> bool {
        self.0.len() == 1 && self.contains(id)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn ids(&self) -> impl Iterator<Item = &PipelineId> {
        self.0.iter()
    }

    /// Add `id` if it isn't there, take it out if it is — what shift-clicking a
    /// card does.
    pub fn toggle(&mut self, id: &PipelineId) {
        if !self.0.remove(id) {
            self.0.insert(id.clone());
        }
    }

    /// Add every one of `ids`, keeping what is already selected.
    pub fn add_all<I: IntoIterator<Item = PipelineId>>(&mut self, ids: I) {
        self.0.extend(ids);
    }

    /// Whether everything in `other` is already selected.
    ///
    /// The guard on the adding write, for the reason [`Selection::is_only`] is
    /// the guard on the replacing one: "select children" of a branch that is
    /// already selected must not mark the signal dirty.
    #[must_use]
    pub fn covers(&self, other: &Self) -> bool {
        other.0.is_subset(&self.0)
    }
}

/// A pipeline and everything downstream of it, as a selection.
///
/// `pipelines` is the `(id, upstreams)` shape [`crate::graph::pipelines_from`]
/// produces, so the sidebar's "select children" and the canvas agree about what
/// feeds what.
///
/// The whole subtree rather than the direct children: the gesture is for moving
/// a branch of the graph out of the way, and a branch is not one row deep. The
/// walk carries a visited set, which makes it terminate on the cycle the server
/// shouldn't allow but that we don't get to assume — the same assumption
/// [`crate::sidebar`] declines to make.
#[must_use]
pub fn descendants(pipelines: &[(PipelineId, Vec<PipelineId>)], root: &PipelineId) -> Selection {
    let mut selected: BTreeSet<PipelineId> = std::iter::once(root.clone()).collect();
    let mut frontier = vec![root.clone()];
    while let Some(id) = frontier.pop() {
        for (child, parents) in pipelines {
            if parents.contains(&id) && selected.insert(child.clone()) {
                frontier.push(child.clone());
            }
        }
    }
    Selection(selected)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(s: &str) -> PipelineId {
        s.to_string()
    }

    fn ids(selection: &Selection) -> Vec<&str> {
        selection.ids().map(String::as_str).collect()
    }

    #[test]
    fn a_plain_click_selects_one() {
        let selection = Selection::only(&id("a"));
        assert!(selection.is_only("a"));
        assert!(!selection.contains("b"));
    }

    #[test]
    fn toggling_adds_then_removes() {
        let mut selection = Selection::only(&id("a"));
        selection.toggle(&id("b"));
        assert_eq!(ids(&selection), vec!["a", "b"]);
        assert!(!selection.is_only("a"));
        selection.toggle(&id("a"));
        assert_eq!(ids(&selection), vec!["b"]);
        selection.toggle(&id("b"));
        assert!(selection.is_empty());
    }

    #[test]
    fn adding_keeps_what_was_selected_and_says_when_there_is_nothing_to_do() {
        let mut selection = Selection::only(&id("a"));
        assert!(!selection.covers(&Selection::of([id("a"), id("b")])));
        selection.add_all([id("a"), id("b")]);
        assert_eq!(ids(&selection), vec!["a", "b"]);
        // already all there: nothing to write
        assert!(selection.covers(&Selection::of([id("a"), id("b")])));
    }

    #[test]
    fn children_are_the_whole_subtree() {
        let graph = vec![
            (id("root"), vec![]),
            (id("mid"), vec![id("root")]),
            (id("leaf"), vec![id("mid")]),
            (id("other"), vec![]),
        ];
        assert_eq!(
            ids(&descendants(&graph, &id("root"))),
            vec!["leaf", "mid", "root"]
        );
        assert_eq!(ids(&descendants(&graph, &id("mid"))), vec!["leaf", "mid"]);
        // a leaf selects itself and nothing else
        assert_eq!(ids(&descendants(&graph, &id("leaf"))), vec!["leaf"]);
    }

    #[test]
    fn a_pipeline_with_two_upstreams_is_included_once() {
        let graph = vec![
            (id("a"), vec![]),
            (id("b"), vec![id("a")]),
            (id("join"), vec![id("a"), id("b")]),
        ];
        assert_eq!(ids(&descendants(&graph, &id("a"))), vec!["a", "b", "join"]);
    }

    #[test]
    fn a_cycle_terminates() {
        let graph = vec![
            (id("a"), vec![id("b")]),
            (id("b"), vec![id("a")]),
            (id("out"), vec![id("b")]),
        ];
        assert_eq!(ids(&descendants(&graph, &id("a"))), vec!["a", "b", "out"]);
    }
}
