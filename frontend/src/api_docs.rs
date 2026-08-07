//! Arranging the HTTP reference for the `/docs` page.
//!
//! The same shape as [`crate::docs`], one level up: that module arranges the
//! components a pipeline is built from, this one the endpoints the server
//! serves. Both are pure and unit-tested, and both leave the rendering to
//! `app.rs`.
//!
//! The endpoints themselves come from `kayak_core::api_docs`, which is also
//! what the router is built from and what `/api/openapi.json` is generated
//! from — so this page can't describe a server that doesn't exist.
//!
//! Prose is rendered by [`crate::docs::rendered_description`] rather than by a
//! copy of it: an endpoint's description is written in the same doc-comment
//! style as a component's, and should render the same way.

use kayak_core::api_docs::{ApiDoc, Method, TAGS, Tag};

/// A tag and the endpoints in it that matched the search. Tags with no matches
/// are dropped, so an empty sidebar means "nothing matched" rather than six
/// empty headings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Group {
    pub tag: Tag,
    pub endpoints: Vec<ApiDoc>,
}

/// The endpoints matching `query`, grouped by tag and in table order within
/// each.
#[must_use]
pub fn groups(endpoints: &[ApiDoc], query: &str) -> Vec<Group> {
    TAGS.iter()
        .filter_map(|tag| {
            let matching: Vec<ApiDoc> = endpoints
                .iter()
                .filter(|e| e.tag == *tag && e.matches(query))
                .cloned()
                .collect();
            (!matching.is_empty()).then_some(Group {
                tag: *tag,
                endpoints: matching,
            })
        })
        .collect()
}

/// How many endpoints matched — what the page says when nothing did.
#[must_use]
pub fn total(groups: &[Group]) -> usize {
    groups.iter().map(|g| g.endpoints.len()).sum()
}

/// The modifier class for a method badge. A colour per method is the one
/// convention every API reference shares, and breaking it would cost a reader
/// more than a bespoke palette would gain.
#[must_use]
pub fn method_class(method: Method) -> &'static str {
    match method {
        Method::Get => "get",
        Method::Post => "post",
        Method::Put => "put",
        Method::Delete => "delete",
    }
}

/// The modifier class for a status code, so a table of responses reads at a
/// glance: what you asked for, or what went wrong.
#[must_use]
pub fn status_class(status: u16) -> &'static str {
    if status < 300 { "success" } else { "failure" }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kayak_core::api_docs::endpoints;

    fn ids(groups: &[Group]) -> Vec<&'static str> {
        groups
            .iter()
            .flat_map(|g| g.endpoints.iter().map(ApiDoc::operation_id))
            .collect()
    }

    #[test]
    fn an_empty_query_keeps_every_endpoint() {
        let all = endpoints();
        assert_eq!(total(&groups(&all, "")), all.len());
    }

    /// Pipelines first, then what they are built from, then the file, then the
    /// API talking about itself — the order the table declares, not the order
    /// the routes happen to be registered in.
    #[test]
    fn groups_come_back_in_tag_order() {
        let groups = groups(&endpoints(), "");
        let tags: Vec<Tag> = groups.iter().map(|g| g.tag).collect();
        assert_eq!(tags, TAGS.to_vec());
    }

    #[test]
    fn a_query_narrows_the_list_to_matching_endpoints() {
        let groups = groups(&endpoints(), "/api/connections");
        assert_eq!(
            ids(&groups),
            ["listConnections", "createConnection", "deleteConnection"]
        );
    }

    /// A query that matches nothing must not leave six empty headings behind.
    #[test]
    fn a_tag_with_no_matches_is_dropped_entirely() {
        let nothing = groups(&endpoints(), "quicksilver");
        assert!(nothing.is_empty());
        assert_eq!(total(&nothing), 0);
    }

    /// Both entries on one path have to survive the grouping — the page lists
    /// them as two endpoints, because they are.
    #[test]
    fn two_methods_on_one_path_are_two_entries() {
        let groups = groups(&endpoints(), "layout");
        let layout: Vec<&'static str> = groups
            .iter()
            .flat_map(|g| g.endpoints.iter())
            .filter(|e| e.path == "/api/layout")
            .map(ApiDoc::operation_id)
            .collect();
        assert_eq!(layout, ["getLayout", "replaceLayout"]);
    }

    #[test]
    fn a_status_is_classed_by_whether_it_is_a_failure() {
        assert_eq!(status_class(200), "success");
        assert_eq!(status_class(204), "success");
        assert_eq!(status_class(404), "failure");
        assert_eq!(status_class(500), "failure");
    }

    /// The page renders every description through the component reference's
    /// renderer, so an endpoint whose prose comes back empty renders a blank.
    #[test]
    fn every_endpoint_description_renders_at_least_one_paragraph() {
        for endpoint in endpoints() {
            assert!(
                !crate::docs::rendered_description(endpoint.description).is_empty(),
                "'{}' rendered an empty description",
                endpoint.operation_id()
            );
        }
    }
}
