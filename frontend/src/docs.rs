//! Arranging the generated component reference for the `/docs` page.
//!
//! The docs themselves come from `kayak_core::docs`, which reflects them out
//! of the config schemas. This module only decides what the page shows: which
//! components survive the search box, how they're grouped, and what each one is
//! called in the URL. Like `graph` and `inspector`, it's pure and unit-tested —
//! the Leptos components in `app.rs` just render what it returns.

use kayak_core::docs::{ComponentDoc, Family};

/// The families in pipeline order, which is the order the sidebar lists them.
/// Connections come last: they are what the components above them refer to,
/// so the page reads as a pipeline first and its plumbing after.
pub const FAMILIES: [Family; 4] = [
    Family::Input,
    Family::Transform,
    Family::Output,
    Family::Connection,
];

/// A family and the components in it that matched the search. Families with no
/// matches are dropped, so an empty sidebar means "nothing matched" rather than
/// three empty headings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Group {
    pub family: Family,
    pub components: Vec<ComponentDoc>,
}

/// A stable per-component id, for the sidebar's scroll-to and for linking
/// someone straight at a component.
///
/// The family has to be part of it: `nats` is both an input and an output, so
/// the kind alone isn't unique.
#[must_use]
pub fn anchor_id(component: &ComponentDoc) -> String {
    format!("{}-{}", family_slug(component.family), component.kind)
}

fn family_slug(family: Family) -> &'static str {
    match family {
        Family::Input => "input",
        Family::Transform => "transform",
        Family::Output => "output",
        Family::Connection => "connection",
    }
}

/// The components matching `query`, grouped by family and in declaration order
/// within each.
#[must_use]
pub fn groups(components: &[ComponentDoc], query: &str) -> Vec<Group> {
    FAMILIES
        .iter()
        .filter_map(|family| {
            let matching: Vec<ComponentDoc> = components
                .iter()
                .filter(|c| c.family == *family && c.matches(query))
                .cloned()
                .collect();
            (!matching.is_empty()).then_some(Group {
                family: *family,
                components: matching,
            })
        })
        .collect()
}

/// How many components matched — what the page says when nothing did, and what
/// the heading counts.
#[must_use]
pub fn total(groups: &[Group]) -> usize {
    groups.iter().map(|g| g.components.len()).sum()
}

/// `true` / `false` reads like a value, not a requirement, in a table of
/// settings.
#[must_use]
pub fn requirement_label(required: bool) -> &'static str {
    if required { "required" } else { "optional" }
}

/// A run of description text, so `like this` in a doc comment renders as code
/// rather than as three literal backticks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Segment {
    Text(String),
    Code(String),
}

/// A doc comment, ready to render: paragraphs of segments.
///
/// Doc comments are hard-wrapped at whatever width the source file uses, and
/// those line breaks aren't meant to be seen — but blank lines are. So single
/// newlines collapse into spaces and blank lines start a new paragraph, which
/// is how the comment reads in the editor too.
#[must_use]
pub fn rendered_description(description: &str) -> Vec<Vec<Segment>> {
    description
        .split("\n\n")
        .map(|paragraph| paragraph.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|paragraph| !paragraph.is_empty())
        .map(|paragraph| segments(&paragraph))
        .collect()
}

/// Split on backticks: the odd-numbered pieces are the code spans. An unpaired
/// backtick leaves its tail as plain text rather than swallowing the rest of
/// the paragraph.
fn segments(paragraph: &str) -> Vec<Segment> {
    let pieces: Vec<&str> = paragraph.split('`').collect();
    if pieces.len().is_multiple_of(2) {
        // an odd number of backticks: nothing here is a code span, and dropping
        // the stray backtick would be editing the text
        return vec![Segment::Text(paragraph.to_string())];
    }
    pieces
        .into_iter()
        .enumerate()
        .filter(|(_, piece)| !piece.is_empty())
        .map(|(i, piece)| {
            if i % 2 == 1 {
                Segment::Code(piece.to_string())
            } else {
                Segment::Text(piece.to_string())
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use kayak_core::docs::all_components;

    fn kinds(groups: &[Group]) -> Vec<String> {
        groups
            .iter()
            .flat_map(|g| g.components.iter().map(|c| c.kind.clone()))
            .collect()
    }

    #[test]
    fn an_empty_query_keeps_every_component() {
        let all = all_components();
        let groups = groups(&all, "");
        assert_eq!(total(&groups), all.len());
    }

    /// Inputs, then transforms, then outputs — the order a pipeline reads in,
    /// not the order the config enums happen to be declared in the file.
    #[test]
    fn groups_come_back_in_pipeline_order() {
        let groups = groups(&all_components(), "");
        let families: Vec<Family> = groups.iter().map(|g| g.family).collect();
        assert_eq!(families, FAMILIES.to_vec());
    }

    #[test]
    fn a_query_narrows_the_list_to_matching_components() {
        let groups = groups(&all_components(), "reducer");
        assert_eq!(kinds(&groups), ["reducer"]);
    }

    /// A query that matches nothing must not leave three empty headings behind.
    #[test]
    fn a_family_with_no_matches_is_dropped_entirely() {
        let matched = groups(&all_components(), "reducer");
        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].family, Family::Transform);

        // a term no component kind, field or description contains
        let nothing = groups(&all_components(), "quicksilver");
        assert!(nothing.is_empty());
        assert_eq!(total(&nothing), 0);
    }

    /// `nats` names an input, an output *and* the connection they share, so a
    /// search for it has to find all three — and they need different anchors.
    ///
    /// Other components can match too (a description mentioning nats is a hit),
    /// which is the search working; what is pinned here is that the kind itself
    /// appears once per family.
    #[test]
    fn a_kind_that_exists_in_several_families_is_listed_in_each() {
        let groups = groups(&all_components(), "nats");

        let anchors: Vec<String> = groups
            .iter()
            .flat_map(|g| g.components.iter())
            .filter(|c| c.kind == "nats")
            .map(anchor_id)
            .collect();
        assert_eq!(anchors, ["input-nats", "output-nats", "connection-nats"]);
    }

    #[test]
    fn anchors_are_unique_across_every_component() {
        let all = all_components();
        let mut anchors: Vec<String> = all.iter().map(anchor_id).collect();
        let count = anchors.len();
        anchors.sort_unstable();
        anchors.dedup();
        assert_eq!(anchors.len(), count, "two components share an anchor");
    }

    #[test]
    fn a_field_is_labelled_required_or_optional() {
        assert_eq!(requirement_label(true), "required");
        assert_eq!(requirement_label(false), "optional");
    }

    /// A doc comment's line breaks are an artefact of the source file's width;
    /// its blank lines are not.
    #[test]
    fn a_doc_comment_is_unwrapped_into_paragraphs() {
        let rendered = rendered_description("one line\nwrapped here\n\nsecond paragraph\n");
        assert_eq!(
            rendered,
            vec![
                vec![Segment::Text("one line wrapped here".to_string())],
                vec![Segment::Text("second paragraph".to_string())],
            ]
        );
    }

    #[test]
    fn backticks_become_code_spans() {
        assert_eq!(
            rendered_description("set `type` to `nats` now"),
            vec![vec![
                Segment::Text("set ".to_string()),
                Segment::Code("type".to_string()),
                Segment::Text(" to ".to_string()),
                Segment::Code("nats".to_string()),
                Segment::Text(" now".to_string()),
            ]]
        );
    }

    /// Dropping the stray backtick would be quietly editing someone's docs.
    #[test]
    fn an_unpaired_backtick_is_left_as_text() {
        assert_eq!(
            rendered_description("a ` b"),
            vec![vec![Segment::Text("a ` b".to_string())]]
        );
    }

    #[test]
    fn an_empty_description_renders_nothing() {
        assert!(rendered_description("").is_empty());
        assert!(rendered_description("\n\n  \n").is_empty());
    }

    /// The real doc comments have to survive the round trip — this is what the
    /// page actually renders.
    #[test]
    fn every_real_description_renders_at_least_one_paragraph() {
        for component in all_components() {
            let description = component.description.clone().unwrap_or_default();
            assert!(
                !rendered_description(&description).is_empty(),
                "'{}' rendered an empty description",
                component.kind
            );
        }
    }
}
