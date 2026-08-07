//! Component documentation, reflected out of the config schemas.
//!
//! Every component's config derives `JsonSchema`, and `schemars` carries the
//! doc comments through as `description`. That makes the config types the single
//! source of truth for the reference docs: a new component, a new field or a
//! reworded doc comment shows up on `/docs` without anyone editing a template.
//! The rule that keeps it that way is that nothing here knows the name of any
//! particular component.
//!
//! This module is pure — schema in, [`ComponentDoc`] out — and lives in
//! `kayak-core` so both consumers can use it: the Leptos `/docs` page
//! renders it, and `GET /api/docs` serves it as JSON.

use crate::config::{InputConfig, InputKind, OutputKind, TransformKind};
use crate::connections::ConnectionKind;
use schemars::schema_for;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Which plugin point a component plugs into. Also the grouping the docs
/// sidebar uses, which is why it's ordered the way a pipeline reads.
///
/// A connection isn't a stage of a pipeline, but it is configured the same way
/// — a tagged struct with doc-commented fields — so it documents itself and
/// generates its form through exactly this machinery.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Family {
    Input,
    Transform,
    Output,
    Connection,
}

impl Family {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Input => "inputs",
            Self::Transform => "transforms",
            Self::Output => "outputs",
            Self::Connection => "connections",
        }
    }
}

/// What a field actually accepts, as something a form can be built from.
///
/// [`FieldDoc::type_name`] is the same information rendered for a human to
/// read; this is the machine-readable half, and it is what the "add pipeline"
/// modal picks a widget and a validation rule from. Keeping it here rather
/// than in the frontend is the same bargain as the rest of this module: the
/// config schema stays the single source of truth, so a new field gets a
/// working form control without anyone editing the UI.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    Text,
    Integer,
    Number,
    Boolean,
    /// A closed set of accepted values — rendered as a dropdown, not a box.
    Enum(Vec<String>),
    /// The id of another pipeline. A string on the wire, but the set of valid
    /// answers is the graph the server is currently running, which only the UI
    /// knows — so it renders as a dropdown of the pipelines that exist rather
    /// than as a box to retype an id into.
    PipelineId,
    /// The name of a configured connection, of the kind carried here (`kafka`,
    /// `nats`, ...). Like [`FieldType::PipelineId`] it is a string on the wire
    /// whose valid answers are server state, so it renders as a dropdown — of
    /// the connections of *that kind*, since a nats connection is no use to a
    /// kafka input.
    Connection(String),
    /// Something with a shape of its own (an object, a list, a tagged union
    /// like an input's `buffer`). There is no general widget for those, so the
    /// form takes them as literal JSON.
    Json,
}

/// One configurable field of a component.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldDoc {
    /// The wire name — what actually goes in the JSON.
    pub name: String,
    /// A rendered type, e.g. `string`, `integer`, or `sum | avg | min | max`
    /// for a field that only accepts certain values.
    pub type_name: String,
    /// The same thing in a form a UI can dispatch on.
    pub field_type: FieldType,
    /// The field's doc comment, if it has one.
    pub description: Option<String>,
    /// Required fields have to appear in the JSON; optional ones may be omitted.
    pub required: bool,
}

/// A component config that is a tagged enum rather than a flat struct — the
/// `filter` transform, whose fields depend on which kind of filter it is.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VariantDoc {
    pub name: String,
    pub fields: Vec<FieldDoc>,
}

/// One component: everything `/docs` shows about it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComponentDoc {
    /// The `type` tag that selects this component in a config file.
    pub kind: String,
    pub family: Family,
    /// The config struct's doc comment, if it has one.
    pub description: Option<String>,
    pub fields: Vec<FieldDoc>,
    /// Empty for all but the enum-shaped components; when it isn't, the fields
    /// live on the variants instead.
    pub variants: Vec<VariantDoc>,
}

impl ComponentDoc {
    /// Whether this component matches a search box query. Matching the field
    /// names too is the point: "how do I set a subject" should find `nats`
    /// without the user knowing that's where it lives.
    #[must_use]
    pub fn matches(&self, query: &str) -> bool {
        let query = query.trim().to_lowercase();
        if query.is_empty() {
            return true;
        }
        let in_fields = |fields: &[FieldDoc]| {
            fields
                .iter()
                .any(|f| f.name.to_lowercase().contains(&query))
        };
        self.kind.to_lowercase().contains(&query)
            || self.family.label().contains(&query)
            || self
                .description
                .as_ref()
                .is_some_and(|d| d.to_lowercase().contains(&query))
            || in_fields(&self.fields)
            || self
                .variants
                .iter()
                .any(|v| v.name.to_lowercase().contains(&query) || in_fields(&v.fields))
    }
}

/// Every component kayak can build, in the order the config enums declare them.
///
/// The three schemas are generated here rather than passed in so that callers
/// can't document a stale one.
#[must_use]
pub fn all_components() -> Vec<ComponentDoc> {
    // a schema that can't be turned into a Value would mean schemars produced
    // something non-serialisable, which can't happen; an empty family is the
    // harmless reading of it either way
    let of = |schema: Value, family: Family| components_of(&schema, family);

    // `buffer` sits on `InputConfig` alongside the flattened `InputKind`, so
    // every input accepts it and none of them declare it. Documenting it per
    // input is what the wire format actually looks like.
    let shared = shared_input_fields();
    let mut docs = of(json_schema_of_input(), Family::Input);
    for component in &mut docs {
        component.fields.extend(shared.iter().cloned());
    }

    docs.extend(of(json_schema_of_transform(), Family::Transform));
    docs.extend(of(json_schema_of_output(), Family::Output));
    docs.extend(of(json_schema_of_connection(), Family::Connection));
    docs
}

/// Just the connections, for the places that offer *those* rather than
/// pipeline components — the "add connection" form. A filter over
/// [`all_components`] rather than a second reflection, so the two can't drift.
#[must_use]
pub fn connection_components() -> Vec<ComponentDoc> {
    all_components()
        .into_iter()
        .filter(|c| c.family == Family::Connection)
        .collect()
}

/// The fields every input has, whatever its kind — the ones `InputConfig`
/// declares itself rather than getting from the flattened `InputKind`.
fn shared_input_fields() -> Vec<FieldDoc> {
    let schema = serde_json::to_value(schema_for!(InputConfig)).unwrap_or(Value::Null);
    fields_of(&schema, &schema)
}

fn json_schema_of_input() -> Value {
    serde_json::to_value(schema_for!(InputKind)).unwrap_or(Value::Null)
}

fn json_schema_of_transform() -> Value {
    serde_json::to_value(schema_for!(TransformKind)).unwrap_or(Value::Null)
}

fn json_schema_of_output() -> Value {
    serde_json::to_value(schema_for!(OutputKind)).unwrap_or(Value::Null)
}

fn json_schema_of_connection() -> Value {
    serde_json::to_value(schema_for!(ConnectionKind)).unwrap_or(Value::Null)
}

/// Read the components out of one `#[serde(tag = "type")]` enum's schema.
///
/// The `oneOf` list — not `$defs` — is what's walked, because it's the only
/// place that pairs a `type` tag with a config struct. `$defs` also holds shared
/// field types like `Secret`, which are not components.
#[must_use]
pub fn components_of(schema: &Value, family: Family) -> Vec<ComponentDoc> {
    let Some(variants) = schema["oneOf"].as_array() else {
        return Vec::new();
    };

    variants
        .iter()
        .filter_map(|variant| {
            let kind = variant["properties"]["type"]["const"].as_str()?;
            let def = resolve_ref(schema, variant)?;
            Some(ComponentDoc {
                kind: kind.to_string(),
                family,
                description: description_of(def),
                fields: fields_of(schema, def),
                variants: variants_of(schema, def),
            })
        })
        .collect()
}

/// Follow a `$ref` into `$defs`. Anything else is already the schema it needs
/// to be — an inline field type, say.
fn resolve_ref<'a>(root: &'a Value, schema: &'a Value) -> Option<&'a Value> {
    match schema["$ref"].as_str() {
        Some(reference) => {
            let name = reference.strip_prefix("#/$defs/")?;
            root["$defs"].get(name)
        }
        None => Some(schema),
    }
}

fn description_of(schema: &Value) -> Option<String> {
    schema["description"].as_str().map(ToString::to_string)
}

/// Fields in a useful order: required ones first, in the order the struct
/// declares them, then optional ones alphabetically.
///
/// The order matters because it's the order someone writes the JSON in, and
/// schemars gives `properties` alphabetically — but `required` happens to be in
/// declaration order, which is a better default for the fields that must be
/// there. Optional fields fall back to alphabetical, which is at least stable.
fn fields_of(root: &Value, def: &Value) -> Vec<FieldDoc> {
    let Some(properties) = def["properties"].as_object() else {
        return Vec::new();
    };
    let required: Vec<&str> = def["required"]
        .as_array()
        .map(|r| r.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();

    let field = |name: &str, schema: &Value| FieldDoc {
        name: name.to_string(),
        type_name: type_name_of(root, schema),
        field_type: field_type_of(root, schema),
        // a doc comment on the field wins over the one on its type: `urls` is
        // better described as "the nats url" than as all of `Secret`'s docs
        description: description_of(schema)
            .or_else(|| resolve_ref(root, schema).and_then(description_of)),
        required: required.contains(&name),
    };

    let mut fields: Vec<FieldDoc> = required
        .iter()
        .filter_map(|name| properties.get(*name).map(|s| field(name, s)))
        .collect();
    fields.extend(
        properties
            .iter()
            .filter(|(name, _)| !required.contains(&name.as_str()))
            .map(|(name, schema)| field(name, schema)),
    );
    fields
}

/// The variants of an enum-shaped component config, e.g. the `filter`
/// transform's `Numeric` and `String`. Each is an object with exactly one
/// property, named for the variant.
fn variants_of(root: &Value, def: &Value) -> Vec<VariantDoc> {
    let Some(variants) = def["oneOf"].as_array() else {
        return Vec::new();
    };
    variants
        .iter()
        .filter_map(|variant| {
            let (name, body) = variant["properties"].as_object()?.iter().next()?;
            Some(VariantDoc {
                name: name.clone(),
                fields: fields_of(root, body),
            })
        })
        .collect()
}

/// A human-readable type for a field.
///
/// Anything with a closed set of values renders as that set — `sum | avg | min
/// | max` says more than "string", and it's exactly what a config file has to
/// contain. That covers both plain string enums and the tagged-object kind like
/// a buffer's `static | tumbling`.
fn type_name_of(root: &Value, schema: &Value) -> String {
    if is_pipeline_id(schema) {
        return "pipeline id".to_string();
    }
    if let Some(kind) = connection_kind(schema) {
        return format!("{kind} connection");
    }
    // how an `Option<T>` arrives: T or null. The null half is already said by
    // the field being optional, so it would only add noise here.
    if let Some(branches) = schema["anyOf"].as_array() {
        let mut real = branches.iter().filter(|b| b["type"] != "null");
        if let (Some(only), None) = (real.next(), real.next()) {
            return type_name_of(root, only);
        }
    }
    if let Some(values) = schema["enum"].as_array() {
        return join_values(values.iter().filter_map(Value::as_str));
    }
    if let Some(variants) = schema["oneOf"].as_array() {
        let tags = variants
            .iter()
            .filter_map(|v| v["properties"]["type"]["const"].as_str());
        let joined = join_values(tags);
        if !joined.is_empty() {
            return joined;
        }
    }
    // a `$ref` carries no type of its own; the definition it points at does
    if schema["$ref"].is_string() {
        return resolve_ref(root, schema)
            .map_or_else(|| "object".to_string(), |def| type_name_of(root, def));
    }
    scalar_type_of(schema).unwrap_or("object").to_string()
}

/// The `type` keyword, as one name.
///
/// It is usually a string, but an optional scalar arrives as `["integer",
/// "null"]` — schemars only reaches for the `anyOf` form when the inner type is
/// a `$ref`. The null half says the same thing the `required` list already
/// does, so it's dropped here and both spellings come out alike.
fn scalar_type_of(schema: &Value) -> Option<&str> {
    match &schema["type"] {
        Value::String(name) => Some(name),
        Value::Array(names) => {
            let mut real = names
                .iter()
                .filter_map(Value::as_str)
                .filter(|n| *n != "null");
            match (real.next(), real.next()) {
                (Some(only), None) => Some(only),
                _ => None,
            }
        }
        _ => None,
    }
}

/// The schema extension a config field carries when its value is the id of
/// another pipeline: `#[schemars(extend("x-pipeline-id" = true))]`.
///
/// A marker on the schema rather than a field name here, because the rule that
/// nothing in this module knows the name of any component applies just as much
/// to the names of their fields. Any component that grows a reference to
/// another pipeline gets the dropdown by saying so where the field is declared.
const PIPELINE_ID_MARKER: &str = "x-pipeline-id";

fn is_pipeline_id(schema: &Value) -> bool {
    schema[PIPELINE_ID_MARKER] == Value::Bool(true)
}

/// The same idea for connections, one step further: the marker carries the
/// *kind* of connection the field wants (`#[schemars(extend("x-connection" =
/// "kafka"))]`), because "any connection" isn't a useful answer — a kafka input
/// can only use a kafka connection, and the form should offer those and no
/// others.
const CONNECTION_MARKER: &str = "x-connection";

fn connection_kind(schema: &Value) -> Option<&str> {
    schema[CONNECTION_MARKER].as_str()
}

fn join_values<'a>(values: impl Iterator<Item = &'a str>) -> String {
    values.collect::<Vec<_>>().join(" | ")
}

/// The same walk as [`type_name_of`], answering "what widget is this" instead
/// of "what do I call this".
///
/// The two deliberately disagree in one place: a tagged union like a buffer's
/// `static | tumbling` reads well as a type name, but there is no single
/// control that edits it, so it comes back as [`FieldType::Json`].
fn field_type_of(root: &Value, schema: &Value) -> FieldType {
    if is_pipeline_id(schema) {
        return FieldType::PipelineId;
    }
    if let Some(kind) = connection_kind(schema) {
        return FieldType::Connection(kind.to_string());
    }
    // `Option<T>` is T here too: whether it may be omitted is the `required`
    // flag's job, not the widget's
    if let Some(branches) = schema["anyOf"].as_array() {
        let mut real = branches.iter().filter(|b| b["type"] != "null");
        if let (Some(only), None) = (real.next(), real.next()) {
            return field_type_of(root, only);
        }
    }
    if let Some(values) = schema["enum"].as_array() {
        let values: Vec<String> = values
            .iter()
            .filter_map(Value::as_str)
            .map(ToString::to_string)
            .collect();
        // an `enum` of anything but plain strings isn't a dropdown
        if !values.is_empty() {
            return FieldType::Enum(values);
        }
    }
    if schema["$ref"].is_string() {
        return resolve_ref(root, schema).map_or(FieldType::Json, |def| field_type_of(root, def));
    }
    match scalar_type_of(schema) {
        Some("string") => FieldType::Text,
        Some("integer") => FieldType::Integer,
        Some("number") => FieldType::Number,
        Some("boolean") => FieldType::Boolean,
        _ => FieldType::Json,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The first component with this tag, which is the input one where a tag is
    /// shared. `nats` names an input, an output *and* a connection, so anything
    /// asking about one of the others has to say which — see [`in_family`].
    fn component(kind: &str) -> ComponentDoc {
        match all_components().into_iter().find(|c| c.kind == kind) {
            Some(c) => c,
            None => panic!("no component documented for '{kind}'"),
        }
    }

    fn in_family(kind: &str, family: Family) -> ComponentDoc {
        match all_components()
            .into_iter()
            .find(|c| c.kind == kind && c.family == family)
        {
            Some(c) => c,
            None => panic!("no {} component documented for '{kind}'", family.label()),
        }
    }

    fn field<'a>(component: &'a ComponentDoc, name: &str) -> &'a FieldDoc {
        match component.fields.iter().find(|f| f.name == name) {
            Some(f) => f,
            None => panic!("'{}' has no field '{name}'", component.kind),
        }
    }

    fn field_names(component: &ComponentDoc) -> Vec<&str> {
        component.fields.iter().map(|f| f.name.as_str()).collect()
    }

    /// The reflection walks `oneOf` and can silently drop a variant it doesn't
    /// understand, which would quietly undocument a component.
    #[test]
    fn every_declared_component_is_documented() {
        let declared: Vec<String> = [
            json_schema_of_input(),
            json_schema_of_transform(),
            json_schema_of_output(),
            json_schema_of_connection(),
        ]
        .iter()
        .flat_map(|schema| {
            schema["oneOf"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|v| v["properties"]["type"]["const"].as_str().map(String::from))
        })
        .collect();

        let documented: Vec<String> = all_components().iter().map(|c| c.kind.clone()).collect();
        assert_eq!(documented, declared);
        assert!(
            !documented.is_empty(),
            "no components were documented at all"
        );
    }

    /// The docs are only as good as the doc comments, so an undocumented
    /// component should fail here rather than render as a blank page.
    #[test]
    fn every_component_has_a_description_from_its_doc_comment() {
        for component in all_components() {
            assert!(
                component
                    .description
                    .as_ref()
                    .is_some_and(|d| !d.trim().is_empty()),
                "component '{}' has no doc comment",
                component.kind
            );
        }
    }

    #[test]
    fn required_and_optional_fields_are_distinguished() {
        let nats = component("nats");
        assert!(field(&nats, "connection").required);
        assert!(field(&nats, "subject").required);
        // `buffer` is an Option, and optional on the wire
        assert!(!field(&nats, "buffer").required);
    }

    /// Required first in declaration order, then optional — the order someone
    /// writing the config would fill them in.
    #[test]
    fn required_fields_come_first_in_declaration_order() {
        assert_eq!(
            field_names(&component("nats")),
            ["connection", "subject", "buffer"]
        );
    }

    #[test]
    fn field_descriptions_come_from_field_doc_comments() {
        let urls = field(&in_family("nats", Family::Connection), "urls").clone();
        let description = urls.description.unwrap_or_default();
        assert!(
            description.contains("${NAME}"),
            "expected the field's own doc comment, got: {description}"
        );
        // ...and not the (much longer) doc comment on the `Secret` type it uses
        assert!(
            !description.contains("wasm"),
            "the field doc comment should win over its type's: {description}"
        );
    }

    /// A field that only accepts certain values should say which, since that's
    /// exactly what the config file has to contain.
    #[test]
    fn a_field_with_a_closed_set_of_values_lists_them() {
        assert_eq!(
            field(&component("reducer"), "function").type_name,
            "sum | avg | min | max"
        );
    }

    /// An `Option<T>` is documented as T — the "or null" half is already said by
    /// the field being optional.
    #[test]
    fn an_optional_field_is_named_by_its_inner_type() {
        assert_eq!(
            field(&component("dummy"), "buffer").type_name,
            "static | tumbling"
        );
    }

    /// `buffer` lives on `InputConfig`, not on any `InputKind`, but every input
    /// accepts it — so every input has to document it.
    #[test]
    fn every_input_documents_the_shared_buffer_option() {
        let inputs: Vec<ComponentDoc> = all_components()
            .into_iter()
            .filter(|c| c.family == Family::Input)
            .collect();
        assert!(inputs.len() > 1, "expected several input kinds");
        for input in inputs {
            assert!(
                input.fields.iter().any(|f| f.name == "buffer"),
                "input '{}' doesn't document the buffer option",
                input.kind
            );
        }
    }

    /// The `filter` transform's fields depend on which filter it is, so they're
    /// documented per variant rather than as one flat list.
    #[test]
    fn an_enum_shaped_component_documents_its_variants() {
        let filter = component("filter");
        assert!(
            filter.fields.is_empty(),
            "filter's fields belong to its variants"
        );
        let names: Vec<&str> = filter.variants.iter().map(|v| v.name.as_str()).collect();
        assert_eq!(names, ["Numeric", "String"]);

        let numeric = &filter.variants[0];
        assert_eq!(
            numeric
                .fields
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>(),
            ["field", "operator", "value"]
        );
        assert_eq!(
            numeric.fields[1].type_name,
            "GreaterThan | LessThan | EqualTo"
        );
        assert_eq!(numeric.fields[2].type_name, "number");
    }

    /// The form in the UI is generated from these, so a field that comes back
    /// as the wrong kind gets the wrong widget and the wrong validation.
    #[test]
    fn a_field_carries_a_machine_readable_type_beside_its_rendered_one() {
        assert_eq!(
            field(&component("nats"), "subject").field_type,
            FieldType::Text
        );
        // a `Secret` is transparently a string, so it edits like one
        assert_eq!(
            field(&in_family("nats", Family::Connection), "urls").field_type,
            FieldType::Text
        );
        assert_eq!(
            field(&component("dummy"), "duration").field_type,
            FieldType::Integer
        );
        let filter = component("filter");
        let numeric = &filter.variants[0];
        match numeric.fields.iter().find(|f| f.name == "value") {
            Some(f) => assert_eq!(f.field_type, FieldType::Number),
            None => panic!("the numeric filter has no 'value' field"),
        }
    }

    /// A field naming another pipeline is a string on the wire and would
    /// otherwise be indistinguishable from one, since `PipelineId` *is* a
    /// `String`. The marker on its schema is what lets a form offer the
    /// pipelines that exist instead of a box to retype an id into.
    #[test]
    fn a_field_that_names_another_pipeline_says_so() {
        let pipeline = component("pipeline");
        let upstream = field(&pipeline, "upstream");
        assert_eq!(upstream.field_type, FieldType::PipelineId);
        assert_eq!(upstream.type_name, "pipeline id");
        // and nothing else claims to be one
        assert_eq!(
            field(&component("nats"), "subject").field_type,
            FieldType::Text
        );
    }

    /// A connection is configured exactly like a component, so it documents
    /// itself through the same reflection — and the `/docs` page and the "add
    /// connection" form both come out of this with nothing written by hand.
    #[test]
    fn connections_are_documented_as_their_own_family() {
        let kinds: Vec<String> = connection_components()
            .into_iter()
            .map(|c| c.kind)
            .collect();
        assert_eq!(kinds, ["kafka", "nats", "postgres", "file"]);

        // a file connection is the odd one out — a directory rather than a
        // server — and documents itself through the same machinery regardless
        let files = in_family("file", Family::Connection);
        assert_eq!(field_names(&files), ["root"]);

        let kafka = in_family("kafka", Family::Connection);
        assert_eq!(field_names(&kafka), ["brokers"]);
        // ...and it is not the kafka *input*, which has moved its brokers here
        assert!(!field_names(&component("kafka")).contains(&"brokers"));
    }

    /// The connection reference is a `String` like any other, so without the
    /// marker the form would render a box to retype a name into. The kind rides
    /// along because "any connection" is the wrong set to offer.
    #[test]
    fn a_field_that_names_a_connection_says_which_kind_it_wants() {
        let kafka_input = component("kafka");
        let connection = field(&kafka_input, "connection");
        assert_eq!(
            connection.field_type,
            FieldType::Connection("kafka".to_string())
        );
        assert_eq!(connection.type_name, "kafka connection");

        // each component asks for the kind it can actually use
        assert_eq!(
            field(&in_family("postgres", Family::Output), "connection").field_type,
            FieldType::Connection("postgres".to_string())
        );
        assert_eq!(
            field(&in_family("nats", Family::Output), "connection").field_type,
            FieldType::Connection("nats".to_string())
        );
    }

    /// A closed set of values is a dropdown, and it has to carry the values —
    /// the rendered `sum | avg | min | max` is for reading, not for parsing.
    #[test]
    fn a_field_with_a_closed_set_of_values_carries_them() {
        assert_eq!(
            field(&component("reducer"), "function").field_type,
            FieldType::Enum(vec![
                "sum".to_string(),
                "avg".to_string(),
                "min".to_string(),
                "max".to_string()
            ])
        );
        // ...through an Option, too
        assert_eq!(
            field(&component("kafka"), "start_at").field_type,
            FieldType::Enum(vec!["earliest".to_string(), "latest".to_string()])
        );
    }

    /// `buffer` is a tagged union of two different shapes. It renders as
    /// `static | tumbling`, but no single control edits it, so the form takes
    /// it as JSON rather than offering a dropdown that can't work.
    #[test]
    fn a_structured_field_is_typed_as_json_even_when_it_names_its_variants() {
        let dummy = component("dummy");
        let buffer = field(&dummy, "buffer");
        assert_eq!(buffer.type_name, "static | tumbling");
        assert_eq!(buffer.field_type, FieldType::Json);
    }

    /// `Option<u16>` is an integer that may be omitted, not a nullable oddity.
    #[test]
    fn an_optional_scalar_is_typed_by_its_inner_type() {
        let postgres = in_family("postgres", Family::Connection);
        let port = field(&postgres, "port");
        assert!(!port.required);
        assert_eq!(port.field_type, FieldType::Integer);
        // schemars spells this one `["integer", "null"]` rather than as an
        // `anyOf`, and the rendered name has to survive that too
        assert_eq!(port.type_name, "integer");
    }

    #[test]
    fn a_component_without_settings_documents_no_fields() {
        let stdout = component("stdout");
        assert!(stdout.fields.is_empty() && stdout.variants.is_empty());
        assert!(stdout.description.is_some(), "it should still be described");
    }

    /// `Secret` and the operator enums live in `$defs` next to the components;
    /// documenting them as components would invent things that can't be built.
    #[test]
    fn shared_field_types_are_not_mistaken_for_components() {
        let kinds: Vec<String> = all_components().into_iter().map(|c| c.kind).collect();
        for not_a_component in ["Secret", "ReduceFnKind", "NumericFilterOperatorKind"] {
            assert!(
                !kinds.iter().any(|k| k == not_a_component),
                "'{not_a_component}' was documented as a component"
            );
        }
    }

    #[test]
    fn search_matches_a_component_by_kind_family_or_field_name() {
        let nats = component("nats");
        assert!(nats.matches("nat"), "should match a partial kind");
        assert!(nats.matches("NATS"), "should be case-insensitive");
        assert!(nats.matches("subject"), "should match a field name");
        assert!(nats.matches("inputs"), "should match the family");
        assert!(nats.matches(""), "an empty query matches everything");
        assert!(nats.matches("   "), "a blank query matches everything");
        assert!(!nats.matches("kafka"));
    }

    #[test]
    fn search_matches_a_variant_field_of_an_enum_shaped_component() {
        let filter = component("filter");
        assert!(
            filter.matches("operator"),
            "should reach into variant fields"
        );
        assert!(filter.matches("numeric"), "should match a variant name");
    }
}
