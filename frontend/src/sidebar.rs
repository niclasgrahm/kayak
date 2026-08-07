//! The sidebar's pipeline list: which rows it shows, in what order, and which
//! ones a search leaves standing.
//!
//! Pure functions over `(id, parents)` pairs — the same shape
//! [`crate::graph::pipelines_from`] produces for the canvas — so the two views
//! of the graph are derived from one description of it. Unit-tested here, same
//! convention as `graph.rs`, `docs.rs` and `form.rs`.

use std::collections::{HashMap, HashSet};

use kayak_core::PipelineId;

/// How the pipeline list is arranged.
///
/// Two views of the same set rather than two sets: the flat list answers "what
/// is there", the tree answers "what feeds what". Neither is right for both
/// questions, which is why this is a toggle and not a decision.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum SidebarMode {
    /// Every pipeline once, in id order.
    #[default]
    Flat,
    /// Roots first, each pipeline nested under the upstreams that feed it.
    Tree,
}

impl SidebarMode {
    #[must_use]
    pub fn toggled(self) -> Self {
        match self {
            Self::Flat => Self::Tree,
            Self::Tree => Self::Flat,
        }
    }

    /// What the mode is called on the button that switches it.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Flat => "flat",
            Self::Tree => "tree",
        }
    }

    /// The button's tooltip: what is showing, and what clicking does.
    #[must_use]
    pub fn title(self) -> &'static str {
        match self {
            Self::Flat => "flat list — click to group by upstream",
            Self::Tree => "grouped by upstream — click for a flat list",
        }
    }
}

/// One line in the list.
///
/// `depth` is the nesting depth in the tree, not the pipeline's depth in the
/// graph — a repeat sits one below whichever parent it is being shown under.
/// In [`SidebarMode::Flat`] it is always zero.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Row {
    pub id: PipelineId,
    pub depth: usize,
    /// This pipeline is fed by more than one upstream and is being shown under
    /// one of the others: a pointer to a row that appears in full elsewhere.
    /// Its children are not repeated under it.
    pub repeat: bool,
}

/// The rows to draw, for a mode and a search box.
#[must_use]
pub fn rows(pipelines: &[(PipelineId, Vec<PipelineId>)], mode: SidebarMode, query: &str) -> Vec<Row> {
    let rows = match mode {
        SidebarMode::Flat => flat(pipelines),
        SidebarMode::Tree => tree(pipelines),
    };
    filter(rows, query)
}

/// Every pipeline once, in id order.
///
/// Sorted here rather than taken as the server sends it: `GET /api/pipelines`
/// walks a `HashMap`, so its order is arbitrary and changes between reloads.
fn flat(pipelines: &[(PipelineId, Vec<PipelineId>)]) -> Vec<Row> {
    let mut ids: Vec<&PipelineId> = pipelines.iter().map(|(id, _)| id).collect();
    ids.sort_unstable();
    ids.into_iter()
        .map(|id| Row {
            id: id.clone(),
            depth: 0,
            repeat: false,
        })
        .collect()
}

/// The graph as a tree: roots in id order, each pipeline nested under every
/// upstream that feeds it.
///
/// The graph is a DAG, not a tree — a pipeline can have several upstreams — so
/// one of them has to host the real subtree and the rest get a repeat. The host
/// is the *deepest* parent, which is the one the canvas draws the card below,
/// so the two views agree about where a pipeline lives; ties go to the lower id
/// so the answer doesn't depend on config order.
///
/// A pipeline the walk never reaches — one in a cycle, which the server
/// shouldn't allow but we don't get to assume — is appended as a root rather
/// than dropped: a list that silently omits a pipeline is worse than one that
/// draws a cycle badly.
fn tree(pipelines: &[(PipelineId, Vec<PipelineId>)]) -> Vec<Row> {
    let depths = depths(pipelines);
    let host = hosts(pipelines, &depths);

    let mut children: HashMap<&PipelineId, Vec<&PipelineId>> = HashMap::new();
    let mut roots: Vec<&PipelineId> = Vec::new();
    for (id, parents) in pipelines {
        if parents.is_empty() {
            roots.push(id);
        }
        for parent in parents {
            children.entry(parent).or_default().push(id);
        }
    }
    roots.sort_unstable();
    for kids in children.values_mut() {
        kids.sort_unstable();
        kids.dedup();
    }

    let mut rows = Vec::new();
    let mut emitted: HashSet<&PipelineId> = HashSet::new();
    for root in roots {
        walk(root, 0, &children, &host, &mut emitted, &mut rows);
    }
    // whatever the walk couldn't reach, in id order
    let mut stranded: Vec<&PipelineId> = pipelines
        .iter()
        .map(|(id, _)| id)
        .filter(|id| !emitted.contains(id))
        .collect();
    stranded.sort_unstable();
    for id in stranded {
        walk(id, 0, &children, &host, &mut emitted, &mut rows);
    }
    rows
}

fn walk<'a>(
    id: &'a PipelineId,
    depth: usize,
    children: &HashMap<&'a PipelineId, Vec<&'a PipelineId>>,
    host: &HashMap<&'a PipelineId, &'a PipelineId>,
    emitted: &mut HashSet<&'a PipelineId>,
    rows: &mut Vec<Row>,
) {
    // A pipeline already on screen in full is a repeat wherever it turns up
    // again, and stops there: recursing would draw its whole subtree twice, and
    // in a cycle would not stop at all.
    let repeat = !emitted.insert(id);
    rows.push(Row {
        id: id.clone(),
        depth,
        repeat,
    });
    if repeat {
        return;
    }
    for child in children.get(id).into_iter().flatten().copied() {
        // shown here in full only if this is the parent that hosts it
        if host.get(child).is_some_and(|h| *h == id) {
            walk(child, depth + 1, children, host, emitted, rows);
        } else {
            rows.push(Row {
                id: child.clone(),
                depth: depth + 1,
                repeat: true,
            });
        }
    }
}

/// The parent each pipeline is shown under in full: the deepest one, ties by id.
fn hosts<'a>(
    pipelines: &'a [(PipelineId, Vec<PipelineId>)],
    depths: &HashMap<&PipelineId, usize>,
) -> HashMap<&'a PipelineId, &'a PipelineId> {
    pipelines
        .iter()
        .filter_map(|(id, parents)| {
            let host = parents
                .iter()
                .max_by_key(|p| (depths.get(p).copied().unwrap_or(0), std::cmp::Reverse(*p)))?;
            Some((id, host))
        })
        .collect()
}

/// Graph depth per pipeline — a root is 0, anything else is one below its
/// deepest parent. Cycle-safe, for the same reason `graph::depths` is: a
/// pipeline that reappears on the path being walked is treated as a root.
fn depths(pipelines: &[(PipelineId, Vec<PipelineId>)]) -> HashMap<&PipelineId, usize> {
    let parents: HashMap<&PipelineId, &Vec<PipelineId>> = pipelines
        .iter()
        .map(|(id, parents)| (id, parents))
        .collect();

    fn depth_of<'a>(
        id: &'a PipelineId,
        parents: &HashMap<&'a PipelineId, &'a Vec<PipelineId>>,
        resolved: &mut HashMap<&'a PipelineId, usize>,
        on_path: &mut HashSet<&'a PipelineId>,
    ) -> usize {
        if let Some(known) = resolved.get(id) {
            return *known;
        }
        if !on_path.insert(id) {
            return 0;
        }
        let depth = parents
            .get(id)
            .map(|ps| {
                ps.iter()
                    .filter_map(|p| parents.get_key_value(p).map(|(k, _)| *k))
                    .map(|p| depth_of(p, parents, resolved, on_path) + 1)
                    .max()
                    .unwrap_or(0)
            })
            .unwrap_or(0);
        on_path.remove(id);
        resolved.insert(id, depth);
        depth
    }

    let mut resolved = HashMap::new();
    for (id, _) in pipelines {
        depth_of(id, &parents, &mut resolved, &mut HashSet::new());
    }
    resolved
}

/// Whether the search box has anything worth offering to clear.
///
/// Untrimmed on purpose, unlike [`matches`]: a box holding only spaces filters
/// nothing, but it does hold text, and a clear button that vanishes while the
/// caret still has something to backspace over reads as a bug. The two rules
/// answer different questions — "does this narrow the list" and "is there
/// anything in the box".
#[must_use]
pub fn clearable(query: &str) -> bool {
    !query.is_empty()
}

/// Whether a row's id matches what was typed: case-insensitive substring, the
/// same rule the component reference uses.
#[must_use]
pub fn matches(id: &str, query: &str) -> bool {
    let query = query.trim().to_lowercase();
    query.is_empty() || id.to_lowercase().contains(&query)
}

/// Drop the rows that don't match, **keeping the ancestors of the ones that
/// do**: a match indented under nothing would lose the one thing the tree is
/// for. An ancestor kept for context can itself not match, which is fine — it
/// is there to say where the match sits.
///
/// The rows are in pre-order, so walking backwards makes the first row shallower
/// than a kept one exactly its parent.
fn filter(rows: Vec<Row>, query: &str) -> Vec<Row> {
    if query.trim().is_empty() {
        return rows;
    }
    let mut keep = vec![false; rows.len()];
    // the depth an ancestor still has to be shallower than to be worth keeping
    let mut wanted: Option<usize> = None;
    for (i, row) in rows.iter().enumerate().rev() {
        let needed = wanted.is_some_and(|w| row.depth < w);
        if matches(&row.id, query) || needed {
            keep[i] = true;
            wanted = (row.depth > 0).then_some(row.depth);
        }
    }
    rows.into_iter()
        .zip(keep)
        .filter_map(|(row, keep)| keep.then_some(row))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pipeline(id: &str, parents: &[&str]) -> (PipelineId, Vec<PipelineId>) {
        (
            id.to_string(),
            parents.iter().map(ToString::to_string).collect(),
        )
    }

    /// `id` at each row, with `>` per level of nesting and `~` marking a repeat
    /// — the shape of the list, in the shape of the list.
    fn shape(rows: &[Row]) -> Vec<String> {
        rows.iter()
            .map(|r| {
                format!(
                    "{}{}{}",
                    ">".repeat(r.depth),
                    if r.repeat { "~" } else { "" },
                    r.id
                )
            })
            .collect()
    }

    fn tree_of(pipelines: &[(PipelineId, Vec<PipelineId>)], query: &str) -> Vec<String> {
        shape(&rows(pipelines, SidebarMode::Tree, query))
    }

    #[test]
    fn a_flat_list_is_every_pipeline_once_in_id_order() {
        let pipelines = [
            pipeline("zulu", &[]),
            pipeline("alpha", &["zulu"]),
            pipeline("mike", &["zulu"]),
        ];
        assert_eq!(
            shape(&rows(&pipelines, SidebarMode::Flat, "")),
            ["alpha", "mike", "zulu"]
        );
    }

    #[test]
    fn a_tree_nests_children_under_their_upstream() {
        let pipelines = [
            pipeline("root", &[]),
            pipeline("child", &["root"]),
            pipeline("grandchild", &["child"]),
        ];
        assert_eq!(tree_of(&pipelines, ""), ["root", ">child", ">>grandchild"]);
    }

    #[test]
    fn roots_and_siblings_come_out_in_id_order() {
        let pipelines = [
            pipeline("b-root", &[]),
            pipeline("a-root", &[]),
            pipeline("z-child", &["a-root"]),
            pipeline("a-child", &["a-root"]),
        ];
        assert_eq!(
            tree_of(&pipelines, ""),
            ["a-root", ">a-child", ">z-child", "b-root"]
        );
    }

    /// The DAG case the whole design is about: `joined` is fed by two
    /// pipelines, so it appears under both — in full under one of them, as a
    /// pointer under the other.
    ///
    /// Note the repeat comes *before* the full row here: siblings are in id
    /// order whatever kind of row they are, because "where is `joined` in this
    /// list" should be answerable from the id alone.
    #[test]
    fn a_pipeline_with_two_upstreams_appears_under_both() {
        let pipelines = [
            pipeline("root", &[]),
            pipeline("mid", &["root"]),
            pipeline("joined", &["root", "mid"]),
            pipeline("tail", &["joined"]),
        ];
        assert_eq!(
            tree_of(&pipelines, ""),
            ["root", ">~joined", ">mid", ">>joined", ">>>tail"]
        );
    }

    /// Which parent hosts the full subtree isn't arbitrary: it is the deepest
    /// one, so the sidebar agrees with the canvas about where a card sits.
    #[test]
    fn the_deepest_parent_hosts_the_subtree() {
        let pipelines = [
            pipeline("root", &[]),
            pipeline("deep", &["root"]),
            pipeline("deeper", &["deep"]),
            // fed by a root and by something three rows down
            pipeline("joined", &["root", "deeper"]),
        ];
        let rows = tree_of(&pipelines, "");
        assert!(
            rows.contains(&">>>joined".to_string()),
            "the subtree should hang off the deepest parent: {rows:?}"
        );
        assert!(rows.contains(&">~joined".to_string()));
    }

    /// A repeat is a pointer, not a second copy: its children stay where the
    /// full row is.
    #[test]
    fn a_repeat_does_not_repeat_its_children() {
        let pipelines = [
            pipeline("a", &[]),
            pipeline("b", &[]),
            pipeline("joined", &["a", "b"]),
            pipeline("leaf", &["joined"]),
        ];
        let rows = tree_of(&pipelines, "");
        assert_eq!(rows.iter().filter(|r| r.ends_with("leaf")).count(), 1);
    }

    /// The server shouldn't allow a cycle, but a list that quietly drops
    /// pipelines would be worse than one that draws it oddly.
    #[test]
    fn a_cycle_still_lists_every_pipeline() {
        let pipelines = [
            pipeline("a", &["b"]),
            pipeline("b", &["a"]),
            pipeline("outside", &[]),
        ];
        let listed: HashSet<PipelineId> = rows(&pipelines, SidebarMode::Tree, "")
            .into_iter()
            .map(|r| r.id)
            .collect();
        assert_eq!(
            listed,
            ["a", "b", "outside"]
                .iter()
                .map(ToString::to_string)
                .collect()
        );
    }

    #[test]
    fn a_search_keeps_only_matching_rows_in_a_flat_list() {
        let pipelines = [
            pipeline("orders", &[]),
            pipeline("order-errors", &[]),
            pipeline("clicks", &[]),
        ];
        assert_eq!(
            shape(&rows(&pipelines, SidebarMode::Flat, "ORDER")),
            ["order-errors", "orders"]
        );
    }

    /// A match indented under nothing says nothing about where it sits, so the
    /// chain above it is kept even though those rows don't match.
    #[test]
    fn a_search_in_the_tree_keeps_the_ancestors_of_a_match() {
        let pipelines = [
            pipeline("root", &[]),
            pipeline("middle", &["root"]),
            pipeline("target", &["middle"]),
            pipeline("elsewhere", &[]),
        ];
        assert_eq!(
            tree_of(&pipelines, "target"),
            ["root", ">middle", ">>target"]
        );
    }

    /// The other half of that rule: an ancestor is kept, a *descendant* of a
    /// match is not. Matching a root would otherwise show the whole graph.
    #[test]
    fn a_search_in_the_tree_drops_the_children_of_a_match() {
        let pipelines = [
            pipeline("root", &[]),
            pipeline("child", &["root"]),
        ];
        assert_eq!(tree_of(&pipelines, "root"), ["root"]);
    }

    #[test]
    fn a_search_matching_nothing_leaves_no_rows() {
        let pipelines = [pipeline("orders", &[]), pipeline("clicks", &["orders"])];
        assert!(rows(&pipelines, SidebarMode::Tree, "nope").is_empty());
        assert!(rows(&pipelines, SidebarMode::Flat, "nope").is_empty());
    }

    #[test]
    fn whitespace_is_not_a_search() {
        let pipelines = [pipeline("orders", &[]), pipeline("clicks", &["orders"])];
        assert_eq!(rows(&pipelines, SidebarMode::Tree, "  ").len(), 2);
    }

    /// The clear button appears exactly when there is text to clear, and an
    /// empty box must not carry a control that would do nothing.
    #[test]
    fn a_box_with_text_in_it_offers_to_clear() {
        assert!(!clearable(""));
        assert!(clearable("orders"));
    }

    /// Spaces filter nothing but they *are* text — the button has to be there,
    /// even though `matches` treats the same query as no query at all.
    #[test]
    fn whitespace_is_not_a_search_but_is_still_clearable() {
        assert!(clearable("  "));
        assert!(matches("anything", "  "));
    }

    #[test]
    fn the_toggle_goes_both_ways() {
        assert_eq!(SidebarMode::Flat.toggled(), SidebarMode::Tree);
        assert_eq!(SidebarMode::Tree.toggled(), SidebarMode::Flat);
    }
}
