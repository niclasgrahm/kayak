//! The `OpenAPI` 3.1 document, generated from the endpoint table.
//!
//! Nothing here decides *what* the API is — that is
//! [`kayak_core::api_docs::endpoints`], the same table `api_router` is built
//! from. This module only renders it in the shape a renderer, a client
//! generator or a contract test expects, which is why it lives in the server
//! crate rather than in core: the frontend reads the table directly and has no
//! use for the spec.
//!
//! The one piece of real work is the schemas. `schemars` 1.x emits JSON Schema
//! 2020-12, which `OpenAPI` 3.1 embeds unchanged, so there is no translation to
//! do — but each generated schema is a *root*, carrying its shared definitions
//! in its own `$defs`. Those are hoisted into one `components/schemas` and the
//! `$ref`s rewritten to match, which is the only reason this is more than a
//! `serde_json::json!` literal.

use kayak_core::api_docs::{ApiDoc, Body, TAGS, endpoints, schemas};
use serde_json::{Map, Value, json};

/// Where a hoisted definition ends up, and what a rewritten `$ref` points at.
const COMPONENTS: &str = "#/components/schemas/";

/// What `schemars` calls the same thing in a generated root schema.
const DEFS: &str = "#/$defs/";

/// The whole document.
#[must_use]
pub fn document() -> Value {
    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "kayak",
            "version": env!("CARGO_PKG_VERSION"),
            "description":
                "Graph-based stream processing: an HTTP API over configurable \
                 `input → transforms → output` pipelines.\n\n\
                 Edits through this API apply to the running graph immediately and \
                 write nothing to disk — the config file is a load source and a save \
                 target, never a mirror of the runtime. `POST /api/config/save` is \
                 the only thing that writes it, and `POST /api/config/revert` is the \
                 only undo.",
        },
        "tags": tags(),
        "paths": paths(),
        "components": { "schemas": components() },
    })
}

fn tags() -> Value {
    Value::Array(
        TAGS.iter()
            .map(|tag| json!({ "name": tag.label(), "description": tag.description() }))
            .collect(),
    )
}

/// Endpoints keyed by path, then by method — two entries sharing a path (`GET`
/// and `PUT /api/layout`) become two keys of one path item, which is the same
/// merge `api_router` relies on axum for.
fn paths() -> Value {
    let mut paths = Map::new();
    for endpoint in endpoints() {
        let item = paths
            .entry(endpoint.path.to_string())
            .or_insert_with(|| Value::Object(Map::new()));
        if let Some(item) = item.as_object_mut() {
            item.insert(endpoint.method.key().to_string(), operation(&endpoint));
        }
    }
    Value::Object(paths)
}

fn operation(endpoint: &ApiDoc) -> Value {
    let mut operation = Map::new();
    operation.insert("operationId".into(), json!(endpoint.operation_id()));
    operation.insert("summary".into(), json!(endpoint.summary));
    operation.insert("description".into(), json!(endpoint.description));
    operation.insert("tags".into(), json!([endpoint.tag.label()]));

    if !endpoint.params.is_empty() {
        operation.insert(
            "parameters".into(),
            Value::Array(
                endpoint
                    .params
                    .iter()
                    .map(|param| {
                        json!({
                            "name": param.name,
                            "in": "path",
                            // a path parameter that could be left out would be a
                            // different path
                            "required": true,
                            "description": param.description,
                            "schema": { "type": "string" },
                        })
                    })
                    .collect(),
            ),
        );
    }

    if let Some(request) = endpoint.request {
        operation.insert(
            "requestBody".into(),
            json!({
                "required": true,
                "description": request.description,
                "content": content(request.body),
            }),
        );
    }

    let mut responses = Map::new();
    for response in &endpoint.responses {
        let mut body = Map::new();
        body.insert("description".into(), json!(response.description));
        if let Some(content) = content(response.body) {
            body.insert("content".into(), content);
        }
        responses.insert(response.status.to_string(), Value::Object(body));
    }
    operation.insert("responses".into(), Value::Object(responses));

    Value::Object(operation)
}

/// A body as an `OpenAPI` `content` map, or `None` for the bodyless ones.
///
/// The event stream is the lossy case: `OpenAPI` can say the response is
/// `text/event-stream` but has no way to say what the *events* in it are, so
/// the schema is a string and [`Tag::Events`]' prose carries the rest. That's
/// `AsyncAPI`'s job, and pretending otherwise here would describe a JSON body
/// that clients would then try to parse in one piece.
fn content(body: Body) -> Option<Value> {
    let content_type = body.content_type()?;
    let schema = match body {
        Body::None => return None,
        Body::Json(name) => json!({ "$ref": format!("{COMPONENTS}{name}") }),
        Body::JsonArray(name) => json!({
            "type": "array",
            "items": { "$ref": format!("{COMPONENTS}{name}") },
        }),
        Body::EventStream(name) => json!({
            "type": "string",
            "description": format!(
                "A stream of SSE frames whose `data:` field is one `{name}` as JSON.",
            ),
        }),
        Body::Html => json!({ "type": "string" }),
    };
    Some(json!({ content_type: { "schema": schema } }))
}

/// Every schema the paths refer to, in one flat map.
///
/// Hoisting first and inserting the named roots second is deliberate: a root's
/// own definition is the authoritative one, and it is also the one carrying the
/// title. The two spellings agree — `schemars` generates a type the same way
/// wherever it meets it — so this is about which copy wins, not about a
/// conflict.
fn components() -> Map<String, Value> {
    let generated = schemas();
    let mut components = Map::new();

    for schema in generated.values() {
        for (name, definition) in defs_of(schema) {
            components
                .entry(name)
                .or_insert_with(|| rewrite_refs(definition));
        }
    }
    for (name, schema) in generated {
        components.insert((*name).to_string(), rewrite_refs(bare(&schema)));
    }
    components
}

/// A root schema's `$defs`, as owned pairs.
fn defs_of(schema: &Value) -> Vec<(String, Value)> {
    schema["$defs"]
        .as_object()
        .map(|defs| {
            defs.iter()
                .map(|(name, definition)| (name.clone(), definition.clone()))
                .collect()
        })
        .unwrap_or_default()
}

/// A root schema without the two keys that only make sense on a root: its
/// `$defs` (hoisted into `components/schemas` instead) and its `$schema`
/// dialect declaration (which the document declares once, by being `OpenAPI` 3.1).
fn bare(schema: &Value) -> Value {
    let mut schema = schema.clone();
    if let Some(object) = schema.as_object_mut() {
        object.remove("$defs");
        object.remove("$schema");
    }
    schema
}

/// Repoint every `#/$defs/X` at `#/components/schemas/X`, at any depth.
///
/// Only the *value of a `$ref` key* is touched — a description that happens to
/// contain the text is left alone, which matters because these descriptions are
/// doc comments and can say anything.
fn rewrite_refs(schema: Value) -> Value {
    match schema {
        Value::Object(object) => Value::Object(
            object
                .into_iter()
                .map(|(key, value)| {
                    let value = match (key.as_str(), &value) {
                        ("$ref", Value::String(reference)) => match reference.strip_prefix(DEFS) {
                            Some(name) => json!(format!("{COMPONENTS}{name}")),
                            None => value,
                        },
                        _ => rewrite_refs(value),
                    };
                    (key, value)
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.into_iter().map(rewrite_refs).collect()),
        other => other,
    }
}

/// The page that renders [`document`].
///
/// Scalar rather than `ReDoc` (which is what most generated reference sites use)
/// for one reason: it has a request panel, and an API reference you can fire a
/// request from against the server serving it is worth more on a dev tool than
/// a prettier static page. The bundle is vendored under `assets/` rather than
/// loaded from a CDN — `just dev` has to work with no network.
///
/// The spec URL is relative, so the page works on whatever host and port the
/// server was started on without anything being configured.
#[must_use]
pub fn reference_page() -> String {
    // `data-url` is relative on purpose — see above. `darkMode` only sets the
    // *initial* theme; the renderer keeps its own toggle, which is the right
    // split — matching the rest of kayak is a sensible default, not a rule
    // someone reading a reference should be held to.
    r#"<!doctype html>
<html>
  <head>
    <title>kayak API reference</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
  </head>
  <body>
    <script
      id="api-reference"
      data-url="/api/openapi.json"
      data-configuration='{"darkMode":true}'
    ></script>
    <script src="/scalar.js"></script>
  </body>
</html>
"#
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every `$ref` in the document, at any depth.
    fn refs(value: &Value, found: &mut Vec<String>) {
        match value {
            Value::Object(object) => {
                for (key, value) in object {
                    match (key.as_str(), value) {
                        ("$ref", Value::String(reference)) => found.push(reference.clone()),
                        _ => refs(value, found),
                    }
                }
            }
            Value::Array(items) => items.iter().for_each(|item| refs(item, found)),
            _ => {}
        }
    }

    #[test]
    fn the_document_declares_itself_as_openapi_3_1() {
        let document = document();
        assert_eq!(document["openapi"], "3.1.0");
        assert_eq!(document["info"]["title"], "kayak");
        assert_eq!(document["info"]["version"], env!("CARGO_PKG_VERSION"));
    }

    /// The table is the routes, so anything missing from `paths` is an endpoint
    /// the server serves and the spec denies.
    #[test]
    fn every_endpoint_appears_under_its_path_and_method() {
        let document = document();
        for endpoint in endpoints() {
            let operation = &document["paths"][endpoint.path][endpoint.method.key()];
            assert!(
                operation.is_object(),
                "{} {} is missing from the document",
                endpoint.method.label(),
                endpoint.path
            );
            assert_eq!(operation["operationId"], endpoint.operation_id());
            assert_eq!(operation["summary"], endpoint.summary);
            assert_eq!(operation["tags"][0], endpoint.tag.label());
        }
    }

    /// Two entries on one path have to become two keys of one path item rather
    /// than the second overwriting the first.
    #[test]
    fn two_methods_on_one_path_share_a_path_item() {
        let document = document();
        let item = &document["paths"]["/api/layout"];
        assert_eq!(item["get"]["operationId"], "getLayout");
        assert_eq!(item["put"]["operationId"], "replaceLayout");
    }

    /// A dangling `$ref` renders as an empty box in every renderer there is,
    /// and is the failure mode hoisting `$defs` exists to avoid.
    #[test]
    fn every_ref_resolves_into_components() {
        let document = document();
        let mut found = Vec::new();
        refs(&document, &mut found);
        assert!(!found.is_empty(), "a document with no refs proves nothing");

        for reference in found {
            let Some(name) = reference.strip_prefix(COMPONENTS) else {
                panic!("'{reference}' does not point into components/schemas")
            };
            assert!(
                document["components"]["schemas"][name].is_object(),
                "'{reference}' points at a schema that isn't there"
            );
        }
    }

    /// `$defs` is a root-schema keyword. Leaving one behind means a renderer
    /// looks for definitions in a place the refs no longer point at.
    #[test]
    fn no_component_keeps_its_own_defs_or_dialect() {
        let document = document();
        let schemas = match document["components"]["schemas"].as_object() {
            Some(schemas) => schemas.clone(),
            None => panic!("the document has no components/schemas"),
        };
        for (name, schema) in schemas {
            assert!(schema["$defs"].is_null(), "'{name}' kept its own $defs");
            assert!(schema["$schema"].is_null(), "'{name}' kept its own $schema");
        }
    }

    /// The nested types a component config is built from have to survive the
    /// hoist — `Config` refers to `InputConfig`, which refers to `Secret` and to
    /// the per-component configs.
    ///
    /// `InputKind` is deliberately not in the list: it is `#[serde(flatten)]`ed
    /// into `InputConfig`, so it is inlined rather than being a definition of
    /// its own — which is also exactly how the wire format reads.
    #[test]
    fn shared_definitions_are_hoisted_out_of_the_roots_that_carried_them() {
        let document = document();
        let schemas = &document["components"]["schemas"];
        for name in ["Config", "InputConfig", "OutputConfig", "NatsConfig", "Secret"] {
            assert!(
                schemas[name].is_object(),
                "'{name}' did not make it into components/schemas"
            );
        }
    }

    /// A description that mentions `#/$defs/` in prose is not a reference, and
    /// rewriting it would be editing someone's doc comment.
    #[test]
    fn only_ref_values_are_rewritten() {
        let rewritten = rewrite_refs(json!({
            "description": "looks like #/$defs/Config but isn't",
            "properties": { "a": { "$ref": "#/$defs/Config" } },
            "list": [{ "$ref": "#/$defs/Secret" }],
        }));
        assert_eq!(
            rewritten["description"],
            "looks like #/$defs/Config but isn't"
        );
        assert_eq!(
            rewritten["properties"]["a"]["$ref"],
            "#/components/schemas/Config"
        );
        assert_eq!(rewritten["list"][0]["$ref"], "#/components/schemas/Secret");
    }

    /// A 204 documents no content at all; a 200 documents the media type its
    /// body actually arrives as.
    #[test]
    fn bodies_become_content_of_the_right_media_type() {
        let document = document();
        let paths = &document["paths"];

        let deleted = &paths["/api/pipelines/{pipeline_id}"]["delete"]["responses"]["204"];
        assert!(deleted["content"].is_null());

        let listed = &paths["/api/pipelines"]["get"]["responses"]["200"]["content"];
        assert_eq!(listed["application/json"]["schema"]["type"], "array");
        assert_eq!(
            listed["application/json"]["schema"]["items"]["$ref"],
            "#/components/schemas/PipelineDto"
        );

        let events = &paths["/events"]["get"]["responses"]["200"]["content"];
        assert!(events["text/event-stream"].is_object());

        let reference = &paths["/api/reference"]["get"]["responses"]["200"]["content"];
        assert!(reference["text/html"].is_object());
    }

    /// A path parameter the spec doesn't mark required makes a generated client
    /// build a request to a URL with a literal `{id}` in it.
    #[test]
    fn path_parameters_are_documented_as_required() {
        let document = document();
        let parameters =
            &document["paths"]["/api/connections/{connection_id}"]["delete"]["parameters"][0];
        assert_eq!(parameters["name"], "connection_id");
        assert_eq!(parameters["in"], "path");
        assert_eq!(parameters["required"], true);
    }

    /// Errors are documented, not just the happy path — the reason the table
    /// carries them at all.
    #[test]
    fn failure_responses_are_documented_with_the_shared_error_body() {
        let document = document();
        let conflict = &document["paths"]["/api/connections/{connection_id}"]["delete"]
            ["responses"]["409"];
        assert_eq!(
            conflict["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/ApiError"
        );
    }

    /// The renderer has to reach both the spec and its own bundle, and both are
    /// served by this server — a CDN link would break `just dev` offline.
    #[test]
    fn the_reference_page_loads_the_spec_and_the_bundle_from_this_server() {
        let page = reference_page();
        assert!(page.contains(r#"data-url="/api/openapi.json""#));
        assert!(page.contains(r#"src="/scalar.js""#));
        // the rest of kayak is dark; the reference opening light reads as a
        // different application
        assert!(page.contains(r#""darkMode":true"#));
        assert!(!page.contains("http://"), "the page reaches off this server");
        assert!(!page.contains("https://"), "the page reaches off this server");
    }
}
