//! What gets measured, as data.
//!
//! The suite is a fixed list rather than a set of flags with defaults, and that
//! is the point of the crate: a baseline is only worth keeping if the run that
//! produced it and the run six months later asked the same questions. Adding a
//! scenario is fine and costs nothing — a baseline simply has no entry for it
//! and the report says so. *Changing* one is what breaks comparability, so
//! change the name at the same time and let the old row age out.
//!
//! Every scenario is one number the run loop can be asked for, and each exists
//! to isolate a different cost. Nothing here touches a network, a filesystem or
//! a broker: the components are the test doubles, so what a run measures is the
//! run loop, the merge, the transform chain and the fan-out, and nothing that
//! varies with what else is on the machine.

/// The reference scenario every ratio is taken against. One pipeline, a batch
/// of a hundred, no transforms, nobody watching — the shape closest to "the run
/// loop doing its job", with the per-message and per-batch costs both visible
/// but neither dominating.
pub const REFERENCE: &str = "batch100";

/// The transform chain a scenario puts between its input and its output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Chain {
    /// Nothing at all — the batch goes from the input straight to the outputs.
    None,
    /// `n` `filter` transforms, each of which passes every message. Passing
    /// rather than dropping on purpose: a filter that dropped its batch would
    /// measure a run loop with no outputs and no fan-out to do, which is a
    /// smaller thing than the one being asked about.
    Filter(usize),
    /// One `map` with a single `copy` of a nested field to the top level —
    /// which is one `fields::get` down a path and one `fields::set`, the two
    /// halves of the machinery every transform addresses fields through.
    Map,
}

impl Chain {
    /// How many transforms this chain builds.
    ///
    /// Named `count` rather than `len` so that the type doesn't owe anyone an
    /// `is_empty` — a chain of zero transforms is a perfectly ordinary
    /// scenario and nothing needs to ask about it separately.
    #[must_use]
    pub fn count(self) -> usize {
        match self {
            Self::None => 0,
            Self::Filter(n) => n,
            Self::Map => 1,
        }
    }
}

/// One question, and the graph that answers it.
#[derive(Clone, Debug)]
pub struct Scenario {
    /// The key a baseline files this result under. Stable for the life of the
    /// scenario — see the module docs.
    pub name: &'static str,
    /// What this run is for, printed above the table.
    pub description: &'static str,
    /// How many independent root pipelines run at once.
    pub pipelines: usize,
    /// Messages in each batch the input produces.
    pub batch_size: usize,
    /// What sits between the input and the outputs.
    pub chain: Chain,
    /// How many pipelines deep each root's chain goes, counting the root. `1`
    /// is a root on its own; `3` puts two `pipeline`-input hops below it, which
    /// is what makes the cost of a hop visible.
    pub depth: usize,
    /// Whether a task is draining the `/events` broadcast for the duration.
    ///
    /// This is the one scenario knob that is about the *server* rather than the
    /// pipeline: the run loop's reporting is gated on `receiver_count() > 0`,
    /// so a browser attaching changes what every pipeline on the box costs.
    /// That gate was worth 46% of throughput before the feed was throttled, and
    /// this is the row that keeps the number honest instead of remembered.
    pub watched: bool,
}

impl Scenario {
    const fn new(name: &'static str, description: &'static str) -> Self {
        Self {
            name,
            description,
            pipelines: 1,
            batch_size: 100,
            chain: Chain::None,
            depth: 1,
            watched: false,
        }
    }

    const fn pipelines(mut self, n: usize) -> Self {
        self.pipelines = n;
        self
    }

    const fn batch(mut self, n: usize) -> Self {
        self.batch_size = n;
        self
    }

    const fn chain(mut self, chain: Chain) -> Self {
        self.chain = chain;
        self
    }

    const fn depth(mut self, n: usize) -> Self {
        self.depth = n;
        self
    }

    const fn watched(mut self) -> Self {
        self.watched = true;
        self
    }

    /// Every pipeline this scenario builds, roots and hops together. What the
    /// server is actually running, as against what it is being fed.
    #[must_use]
    pub fn total_pipelines(&self) -> usize {
        self.pipelines * self.depth
    }
}

/// The suite, in the order it runs and the order it prints.
///
/// Ordered cheapest-first so a `--filter`ed run of the early rows is quick and
/// so a sweep that is going to fall over does it late, with the earlier numbers
/// already on screen.
#[must_use]
pub fn suite() -> Vec<Scenario> {
    vec![
        Scenario::new("batch1", "one message per batch — the per-pass floor").batch(1),
        Scenario::new("batch10", "ten per batch").batch(10),
        Scenario::new(REFERENCE, "a hundred per batch — the reference"),
        Scenario::new("batch1000", "a thousand per batch — per-message cost dominates")
            .batch(1000),
        Scenario::new("filter1", "one filter in the chain").chain(Chain::Filter(1)),
        Scenario::new("filter5", "five filters — the marginal cost of one, times five")
            .chain(Chain::Filter(5)),
        Scenario::new("map1", "one map, copying a nested field to the top level")
            .chain(Chain::Map),
        Scenario::new("pipelines10", "ten pipelines at once").pipelines(10),
        Scenario::new("pipelines100", "a hundred pipelines at once").pipelines(100),
        Scenario::new("pipelines1000", "a thousand pipelines at once").pipelines(1000),
        Scenario::new("depth3", "three pipelines deep — two `pipeline`-input hops").depth(3),
        Scenario::new("watched", "the reference, with a browser attached to /events").watched(),
    ]
}

/// The suite, or the subset whose names contain `needle`.
#[must_use]
pub fn filtered(needle: Option<&str>) -> Vec<Scenario> {
    match needle {
        None => suite(),
        Some(n) => suite().into_iter().filter(|s| s.name.contains(n)).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::{Chain, REFERENCE, filtered, suite};
    use std::collections::HashSet;

    /// A baseline files results by name, so two scenarios sharing one would
    /// have the second quietly overwrite the first's recorded number.
    #[test]
    fn scenario_names_are_unique() {
        let mut seen = HashSet::new();
        for s in suite() {
            assert!(seen.insert(s.name), "two scenarios are called '{}'", s.name);
        }
    }

    /// Every ratio in the report divides by the reference, so a suite without
    /// it produces a report of blanks.
    #[test]
    fn the_reference_scenario_is_in_the_suite() {
        assert!(suite().iter().any(|s| s.name == REFERENCE));
    }

    /// A scenario with no pipelines has nothing to measure and would divide by
    /// zero on its way to saying so.
    #[test]
    fn every_scenario_runs_at_least_one_pipeline_of_at_least_one_message() {
        for s in suite() {
            assert!(s.pipelines >= 1, "{} runs no pipelines", s.name);
            assert!(s.depth >= 1, "{} is zero pipelines deep", s.name);
            assert!(s.batch_size >= 1, "{} has empty batches", s.name);
        }
    }

    #[test]
    fn chain_length_matches_the_variant() {
        assert_eq!(Chain::None.count(), 0);
        assert_eq!(Chain::Filter(5).count(), 5);
        assert_eq!(Chain::Map.count(), 1);
    }

    #[test]
    fn total_pipelines_counts_the_hops_below_each_root() {
        let deep = suite()
            .into_iter()
            .find(|s| s.name == "depth3")
            .unwrap_or_else(|| panic!("the suite has no 'depth3'"));
        assert_eq!(deep.total_pipelines(), 3);
    }

    #[test]
    fn filtering_selects_by_substring_and_none_means_everything() {
        assert_eq!(filtered(None).len(), suite().len());
        let batches = filtered(Some("batch"));
        assert!(batches.len() >= 4, "expected the batch sweep, got {batches:?}");
        assert!(batches.iter().all(|s| s.name.contains("batch")));
        assert!(filtered(Some("no such scenario")).is_empty());
    }
}
