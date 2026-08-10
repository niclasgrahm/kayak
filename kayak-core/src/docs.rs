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
use crate::metadata::MetaFieldDoc;
use crate::state::{PipelineState, StateBucketConfig};
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Which plugin point a component plugs into. Also the grouping the docs
/// sidebar uses, which is why it's ordered the way a pipeline reads.
///
/// A connection isn't a stage of a pipeline, but it is configured the same way
/// — a tagged struct with doc-commented fields — so it documents itself and
/// generates its form through exactly this machinery.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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
    /// A value that is one of several shapes, tagged by one of its own
    /// properties — an input's `buffer`, which is `{"type": "static", "size":
    /// 10}` or `{"type": "tumbling", "window_seconds": 30}`.
    ///
    /// This is the field-level twin of [`ComponentDoc::variants`], and it is
    /// what makes a form conditional: which fields a value has depends on which
    /// variant was picked, so the tag is chosen first and the rest of the form
    /// follows from it.
    Union(UnionDoc),
    /// A value that is an object with a fixed set of fields of its own — a file
    /// output's `rotate`. Not a choice, just a level of nesting, so the form
    /// renders its fields inline.
    Object(Vec<FieldDoc>),
    /// A value that is a list of some other type — a reducer's `aggregations`.
    /// Unlike every other field type this one has no fixed number of boxes, so
    /// the form renders rows that can be added and taken away, each of them
    /// whatever the element type asks for.
    ///
    /// The element is carried as a whole [`FieldDoc`] because that is what a
    /// control is chosen from, and its [`FieldDoc::name`] is empty: a list
    /// element has no name of its own, it has a position, which only the form
    /// rendering it knows. It is always required — a row that is there is a
    /// value that will be sent.
    List(Box<FieldDoc>),
    /// Something with a shape of its own that is none of the above — a union
    /// tagged in a spelling this module doesn't read, say. There is no general
    /// widget for those, so the form takes them as literal JSON.
    Json,
}

/// A tagged union as a form can render it: pick the tag, then fill in whatever
/// that variant asks for.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct UnionDoc {
    /// The property that says which variant this is — `type`, for every union
    /// in the config today. Carried rather than assumed, because it is what
    /// goes on the wire beside the variant's own fields.
    pub tag: String,
    /// The variants, named by their tag *value* (`static`, `tumbling`) rather
    /// than by a Rust variant name. The tag itself is not among any variant's
    /// fields — it is the choice, not a thing to fill in.
    pub variants: Vec<VariantDoc>,
}

/// One configurable field of a component.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct VariantDoc {
    pub name: String,
    pub fields: Vec<FieldDoc>,
}

/// One component: everything `/docs` shows about it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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
    /// What this input attaches to a message when its `envelope` is set —
    /// empty for every family but [`Family::Input`], and declared in
    /// [`crate::metadata`] rather than reflected, since a schema cannot know
    /// what a nats subscription knows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metadata: Vec<MetaFieldDoc>,
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
                .metadata
                .iter()
                .any(|m| m.name.to_lowercase().contains(&query))
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

/// One of the config shapes state is declared with, as the reference shows it.
///
/// Not a [`ComponentDoc`]: neither of these is a component. A bucket is not
/// built into a pipeline and has no `type` tag to select it, so giving it a
/// [`Family`] would put it in the "add pipeline" form's list of things a
/// pipeline can be made of, where it does not belong. What it shares with a
/// component is the only part worth sharing — the fields are reflected out of
/// the config type, so the doc comments on [`crate::state`] are the docs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StateDoc {
    /// What the shape is called, from its `schemars(title = ...)` — "state
    /// bucket", "pipeline state".
    pub title: String,
    /// Where it goes in the config file, as a path — `state.<name>`, or the
    /// `state` key of one pipeline. The schema can't know this; it is the one
    /// hand-written part, and it is what tells the two shapes apart on the page.
    pub path: String,
    /// The config struct's doc comment.
    pub description: Option<String>,
    pub fields: Vec<FieldDoc>,
}

/// The two halves of declaring state: the bucket, and a pipeline's binding to
/// one.
///
/// Generated here rather than written on the page for the reason every other
/// table on `/docs` is: a bound that changes or a field that grows must not
/// leave the reference behind.
#[must_use]
pub fn state_docs() -> Vec<StateDoc> {
    vec![
        state_doc(
            &serde_json::to_value(schema_for!(StateBucketConfig)).unwrap_or(Value::Null),
            "state.<name>",
        ),
        state_doc(
            &serde_json::to_value(schema_for!(PipelineState)).unwrap_or(Value::Null),
            "pipelines[].state",
        ),
    ]
}

fn state_doc(schema: &Value, path: &str) -> StateDoc {
    StateDoc {
        title: schema["title"].as_str().unwrap_or(path).to_string(),
        path: path.to_string(),
        description: description_of(schema),
        fields: fields_of(schema, schema),
    }
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
                metadata: if family == Family::Input {
                    crate::metadata::for_input(kind).unwrap_or_default()
                } else {
                    Vec::new()
                },
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
    fields_at(root, def, 0)
}

/// How far into a field's own shape the reflection will go before giving up and
/// calling it [`FieldType::Json`].
///
/// The bound is here because a schema *may* refer to itself — a config type
/// that contains its own kind — and this walk follows `$ref`s, so without a
/// floor such a type would recurse until the stack ran out. Degrading to a JSON
/// box is the same answer this module already gives for a shape it can't
/// render.
///
/// It is a stack guard and not a statement about how deep a config *should*
/// nest, which is why raising it is the right answer when something legitimate
/// reaches it. The deepest thing in the config today is a `map`'s
/// `mappings[].concat.parts[].value` — a list of a union containing a list of a
/// union — at five, so this is one clear of it. Anything that made a JSON box
/// appear where a real control belongs fails
/// [`no_component_field_needs_raw_json`](tests::no_component_field_needs_raw_json).
const MAX_NESTING: usize = 6;

fn fields_at(root: &Value, def: &Value, depth: usize) -> Vec<FieldDoc> {
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
        field_type: field_type_at(root, schema, depth),
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
    if let Some(values) = string_values_of(schema) {
        return join_values(values.iter().map(String::as_str));
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
    if scalar_type_of(schema) == Some("array") {
        return format!("list of {}", element_name_of(root, &schema["items"]));
    }
    scalar_type_of(schema).unwrap_or("object").to_string()
}

/// What to call the elements of a list.
///
/// A config struct's `#[schemars(title = ...)]` is preferred over the type name
/// it would otherwise get, because "list of object" says nothing and "list of
/// aggregation" says the whole thing. Falls back to the ordinary name for
/// elements that have no title — a list of plain strings.
fn element_name_of(root: &Value, items: &Value) -> String {
    resolve_ref(root, items)
        .and_then(|def| def["title"].as_str())
        .map_or_else(|| type_name_of(root, items), ToString::to_string)
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

/// The values of a closed set of strings, in either of the two spellings
/// schemars uses for one.
///
/// A plain unit-variant enum comes out as `"enum": ["a", "b"]` — but the moment
/// a single variant carries a doc comment there is a description to hang
/// somewhere, and schemars switches to `"oneOf": [{"const": "a", "description":
/// ...}, ...]` instead. The two say exactly the same thing about what the field
/// accepts, and recognising only the first is what quietly turned documenting a
/// variant into downgrading its dropdown to a JSON box.
///
/// Every branch has to be a string constant, which is what keeps this off the
/// tagged unions (a buffer's `static | tumbling`): those have a whole config
/// struct behind each tag, so there is no one value to pick.
fn string_values_of(schema: &Value) -> Option<Vec<String>> {
    // every branch or nothing: a set with one value that isn't a plain string
    // is not a set of plain strings, and half a dropdown is worse than none
    let all_of = |values: &[Value], read: fn(&Value) -> Option<&str>| {
        let found: Vec<String> = values
            .iter()
            .filter_map(|v| read(v).map(ToString::to_string))
            .collect();
        (!found.is_empty() && found.len() == values.len()).then_some(found)
    };
    if let Some(values) = schema["enum"].as_array() {
        return all_of(values, Value::as_str);
    }
    all_of(schema["oneOf"].as_array()?, |v| v["const"].as_str())
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
fn field_type_at(root: &Value, schema: &Value, depth: usize) -> FieldType {
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
            return field_type_at(root, only, depth);
        }
    }
    if let Some(values) = string_values_of(schema) {
        return FieldType::Enum(values);
    }
    if schema["$ref"].is_string() {
        return resolve_ref(root, schema)
            .map_or(FieldType::Json, |def| field_type_at(root, def, depth));
    }
    // past here a field has a shape of its own, and describing it means
    // describing its fields — which is the walk that has to be bounded
    if depth >= MAX_NESTING {
        return FieldType::Json;
    }
    if let Some(union) = union_at(root, schema, depth + 1) {
        return FieldType::Union(union);
    }
    match scalar_type_of(schema) {
        Some("string") => FieldType::Text,
        Some("integer") => FieldType::Integer,
        Some("number") => FieldType::Number,
        Some("boolean") => FieldType::Boolean,
        // an object with fields of its own is those fields, laid out in place.
        // One with none is something this module has no better word for.
        Some("object") => match fields_at(root, schema, depth + 1) {
            fields if fields.is_empty() => FieldType::Json,
            fields => FieldType::Object(fields),
        },
        Some("array") => element_at(root, &schema["items"], depth + 1)
            .map_or(FieldType::Json, |element| FieldType::List(Box::new(element))),
        _ => FieldType::Json,
    }
}

/// One element of a list, as the field a control is chosen from.
///
/// It has no name — a position is all a list element has — and it is always
/// required, because a row that exists is a value that will be sent. A list
/// whose elements this module can't render is not a list a form can offer, so
/// it degrades to [`FieldType::Json`] whole rather than to rows of JSON boxes.
fn element_at(root: &Value, items: &Value, depth: usize) -> Option<FieldDoc> {
    if items.is_null() {
        return None;
    }
    let field_type = field_type_at(root, items, depth);
    if field_type == FieldType::Json {
        return None;
    }
    Some(FieldDoc {
        name: String::new(),
        type_name: element_name_of(root, items),
        field_type,
        description: description_of(items)
            .or_else(|| resolve_ref(root, items).and_then(description_of)),
        required: true,
    })
}

/// A field whose value is a tagged union, if it is one: every branch of a
/// `oneOf` carrying the same tag property with a different constant value.
///
/// This is the *internally* tagged spelling (`{"type": "static", "size": 10}`),
/// which is what `#[serde(tag = "type")]` produces and what every union in the
/// config uses. The externally tagged one (`{"Numeric": {...}}`) is a component
/// config's own shape and is read by [`variants_of`] instead; a field spelled
/// that way falls back to JSON, which is the honest answer until one exists.
fn union_at(root: &Value, def: &Value, depth: usize) -> Option<UnionDoc> {
    let branches = def["oneOf"].as_array()?;
    let tag = tag_of(branches)?;
    let variants = branches
        .iter()
        .map(|branch| {
            Some(VariantDoc {
                name: branch["properties"][&tag]["const"].as_str()?.to_string(),
                // the tag is the choice itself, not one of the things the
                // chosen variant asks for
                fields: fields_at(root, branch, depth)
                    .into_iter()
                    .filter(|f| f.name != tag)
                    .collect(),
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(UnionDoc { tag, variants })
}

/// The property that tags a `oneOf`: the one that is a string constant in every
/// branch. Found rather than assumed to be `type`, so a union tagged by any
/// other name reads the same way.
fn tag_of(branches: &[Value]) -> Option<String> {
    let is_tag = |name: &str| {
        branches
            .iter()
            .all(|branch| branch["properties"][name]["const"].is_string())
    };
    branches
        .first()?["properties"]
        .as_object()?
        .keys()
        .find(|name| is_tag(name))
        .cloned()
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

    /// The state reference is generated like everything else on the page, so a
    /// bound that grows a field must not leave it behind. It also has to stay
    /// *out* of the component list — a bucket is not something a pipeline is
    /// made of, and the "add pipeline" form offers whatever is in there.
    #[test]
    fn state_documents_the_bucket_and_the_pipeline_binding() {
        let docs = state_docs();
        let titles: Vec<&str> = docs.iter().map(|d| d.title.as_str()).collect();
        assert_eq!(titles, ["state bucket", "pipeline state"]);
        assert_eq!(docs[0].path, "state.<name>");

        for doc in &docs {
            assert!(
                doc.description.is_some(),
                "'{}' is documented with no description",
                doc.title
            );
        }

        let bucket: Vec<&str> = docs[0].fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(bucket, ["idle_timeout_secs", "max_keys"]);
        // both bounds are optional because both default — there is no spelling
        // of "unbounded", which is the property the page is there to say
        assert!(docs[0].fields.iter().all(|f| !f.required));

        let binding = &docs[1];
        assert_eq!(
            binding
                .fields
                .iter()
                .map(|f| (f.name.as_str(), f.required))
                .collect::<Vec<_>>(),
            [("bucket", true), ("key", false)]
        );

        assert!(
            all_components().iter().all(|c| c.kind != "state"),
            "a state bucket is not a component"
        );
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
            // `buffer` and `envelope` are `InputConfig`'s rather than the nats
            // input's, and are appended after the kind's own fields
            ["connection", "subject", "max_batch", "buffer", "envelope"]
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
            field(&component("dummy"), "payload").type_name,
            "number | text"
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

    /// Metadata is *declared* in `crate::metadata` rather than reflected out of
    /// the schema — a schema cannot know that a nats subscription knows the
    /// subject. This is what makes the declaration compulsory: an input added
    /// without an arm in `metadata::for_input` fails here rather than shipping
    /// with an empty "metadata" section on `/docs`.
    #[test]
    fn every_input_declares_its_metadata() {
        let inputs: Vec<ComponentDoc> = all_components()
            .into_iter()
            .filter(|c| c.family == Family::Input)
            .collect();
        assert!(inputs.len() > 1, "expected several input kinds");
        for input in inputs {
            assert!(
                crate::metadata::for_input(&input.kind).is_some(),
                "input '{}' hasn't declared what metadata it attaches — add an \
                 arm to metadata::for_input, even if it is an empty one",
                input.kind
            );
            // even an input with nothing of its own to say carries the common
            // fields, so an empty list here means the arm above is a lie
            for name in ["pipeline", "input", "received_at"] {
                assert!(
                    input.metadata.iter().any(|m| m.name == name),
                    "input '{}' doesn't document the common '{name}' metadata field",
                    input.kind
                );
            }
        }
    }

    /// Only inputs have any: a transform or an output is not where a message
    /// comes from.
    #[test]
    fn nothing_but_an_input_declares_metadata() {
        for component in all_components() {
            if component.family != Family::Input {
                assert!(
                    component.metadata.is_empty(),
                    "{} '{}' declares metadata",
                    component.family.label(),
                    component.kind
                );
            }
        }
    }

    /// `envelope` lives beside `buffer` on `InputConfig`, so the same rule
    /// applies: every input accepts it and so every input documents it.
    #[test]
    fn every_input_documents_the_shared_envelope_option() {
        for input in all_components()
            .into_iter()
            .filter(|c| c.family == Family::Input)
        {
            assert!(
                input.fields.iter().any(|f| f.name == "envelope"),
                "input '{}' doesn't document the envelope option",
                input.kind
            );
        }
    }


    /// The envelope is a *choice of shapes*, so the modal has to offer the
    /// choice and then the fields it implies — not a JSON box. Same walk the
    /// `buffer` option gets, one family up.
    #[test]
    fn the_envelope_option_is_a_union_of_its_two_shapes() {
        let nats = component("nats");
        let envelope = field(&nats, "envelope");
        let FieldType::Union(union) = &envelope.field_type else {
            panic!("envelope is {:?}, not a union", envelope.field_type);
        };
        assert_eq!(union.tag, "type");
        let names: Vec<&str> = union.variants.iter().map(|v| v.name.as_str()).collect();
        assert_eq!(names, ["merge", "wrap"]);
    }

    /// The deepest shape in the config, and the one that set [`MAX_NESTING`]:
    /// a `map`'s mappings are a **list of a union**, and one of those variants
    /// carries a **list of a union** of its own. Every level has to survive the
    /// walk, or the form renders a JSON box where a control belongs — which is
    /// exactly what happened when the bound was one level short of this.
    #[test]
    fn a_maps_mappings_are_walked_all_the_way_down() {
        let map = component("map");
        let mappings = field(&map, "mappings");
        let FieldType::List(element) = &mappings.field_type else {
            panic!("mappings is {:?}, not a list", mappings.field_type);
        };
        let FieldType::Union(union) = &element.field_type else {
            panic!("a mapping is {:?}, not a union", element.field_type);
        };
        assert_eq!(union.tag, "type");
        let names: Vec<&str> = union.variants.iter().map(|v| v.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "copy",
                "constant",
                "coalesce",
                "cast",
                "concat",
                "arithmetic",
                "drop"
            ]
        );

        // the second level: concat's parts, and the literal text inside one
        let concat = union
            .variants
            .iter()
            .find(|v| v.name == "concat")
            .unwrap_or_else(|| panic!("no concat variant"));
        let parts = concat
            .fields
            .iter()
            .find(|f| f.name == "parts")
            .unwrap_or_else(|| panic!("concat has no parts field"));
        let FieldType::List(part) = &parts.field_type else {
            panic!("parts is {:?}, not a list", parts.field_type);
        };
        let FieldType::Union(part) = &part.field_type else {
            panic!("a part is {:?}, not a union", part.field_type);
        };
        let literal = part
            .variants
            .iter()
            .find(|v| v.name == "value")
            .unwrap_or_else(|| panic!("no literal part variant"));
        assert_eq!(literal.fields[0].field_type, FieldType::Text);
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
        assert_eq!(kinds, ["kafka", "nats", "postgres", "file", "s3"]);

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
            field(&component("dummy"), "payload").field_type,
            FieldType::Enum(vec!["number".to_string(), "text".to_string()])
        );
        // ...through an Option, too
        assert_eq!(
            field(&component("kafka"), "start_at").field_type,
            FieldType::Enum(vec!["earliest".to_string(), "latest".to_string()])
        );
    }

    /// A doc comment on a variant makes schemars spell the same closed set as
    /// `oneOf` of `const`s rather than as an `enum` array. Both are dropdowns:
    /// documenting a variant must not be what takes the dropdown away.
    #[test]
    fn a_closed_set_is_recognised_in_either_spelling_schemars_uses() {
        // `KafkaStartAt`'s variants carry no doc comments — the `enum` array
        assert_eq_documented(
            field(&component("kafka"), "start_at"),
            "earliest | latest",
            &["earliest", "latest"],
        );
        // `DummyPayload`'s do — `oneOf` of consts, through an `Option` as well
        assert_eq_documented(
            field(&component("dummy"), "payload"),
            "number | text",
            &["number", "text"],
        );
        // and an output's, to pin that it isn't an input-only accident
        assert_eq_documented(
            field(&in_family("file", Family::Output), "format"),
            "ndjson | json_array",
            &["ndjson", "json_array"],
        );
    }

    fn assert_eq_documented(field: &FieldDoc, type_name: &str, values: &[&str]) {
        assert_eq!(field.type_name, type_name, "field '{}'", field.name);
        assert_eq!(
            field.field_type,
            FieldType::Enum(values.iter().map(ToString::to_string).collect()),
            "field '{}' would not render as a dropdown",
            field.name
        );
    }

    /// The http transform's method is a closed set too, and was a `String` for
    /// long enough to be worth pinning as one.
    #[test]
    fn the_http_verb_is_a_closed_set_rather_than_free_text() {
        let http = in_family("http", Family::Transform);
        let verb = field(&http, "verb");
        assert_eq!(
            verb.field_type,
            FieldType::Enum(
                ["GET", "POST", "PUT", "PATCH", "DELETE"]
                    .iter()
                    .map(ToString::to_string)
                    .collect()
            )
        );
    }

    /// `buffer` is a tagged union: which fields it has depends on which kind of
    /// buffer it is. That's the whole of what a form needs to render it as a
    /// choice followed by the fields that choice implies.
    #[test]
    fn a_tagged_union_field_carries_its_tag_and_its_variants() {
        let dummy = component("dummy");
        let buffer = field(&dummy, "buffer");
        assert_eq!(buffer.type_name, "static | tumbling");
        let FieldType::Union(union) = &buffer.field_type else {
            panic!("buffer is a choice of shapes, not a {:?}", buffer.field_type);
        };
        assert_eq!(union.tag, "type");
        let names: Vec<&str> = union.variants.iter().map(|v| v.name.as_str()).collect();
        assert_eq!(names, ["static", "tumbling"]);

        // the fields differ per variant — which is why the choice has to come
        // first — and the tag is not among them, since it *is* the choice
        let fields_of = |name: &str| {
            union
                .variants
                .iter()
                .find(|v| v.name == name)
                .map(|v| {
                    v.fields
                        .iter()
                        .map(|f| (f.name.clone(), f.field_type.clone(), f.required))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };
        assert_eq!(
            fields_of("static"),
            [("size".to_string(), FieldType::Integer, true)]
        );
        assert_eq!(
            fields_of("tumbling"),
            [("window_seconds".to_string(), FieldType::Integer, true)]
        );
    }

    /// `rotate` is nesting without a choice: an object with fields of its own,
    /// which the form can lay out in place rather than take as literal JSON.
    #[test]
    fn a_nested_object_field_carries_its_own_fields() {
        let file = in_family("file", Family::Output);
        let rotate = field(&file, "rotate");
        let FieldType::Object(fields) = &rotate.field_type else {
            panic!("rotate has fields of its own, not a {:?}", rotate.field_type);
        };
        let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["interval_secs", "max_rows"]);
        assert!(
            fields.iter().all(|f| f.field_type == FieldType::Integer),
            "both triggers are counts"
        );
        assert!(
            fields.iter().all(|f| !f.required),
            "either trigger alone is a rotation"
        );
        assert!(
            fields[0].description.is_some(),
            "a nested field is documented like any other"
        );
    }

    /// A list is the one field with no fixed number of boxes, so the form is
    /// told what *one* of them looks like and renders as many as it is given.
    #[test]
    fn a_list_field_carries_the_shape_of_one_element() {
        let reducer = in_family("reducer", Family::Transform);

        let group_by = field(&reducer, "group_by");
        let FieldType::List(element) = &group_by.field_type else {
            panic!("group_by is a list, not a {:?}", group_by.field_type);
        };
        assert_eq!(element.field_type, FieldType::Text);
        assert_eq!(group_by.type_name, "list of string");
        assert!(!group_by.required, "reducing the whole batch is the default");

        let aggregations = field(&reducer, "aggregations");
        let FieldType::List(element) = &aggregations.field_type else {
            panic!(
                "aggregations is a list, not a {:?}",
                aggregations.field_type
            );
        };
        // named for the struct's title rather than "object", which says nothing
        assert_eq!(aggregations.type_name, "list of aggregation");
        assert!(aggregations.required);
        // an element has a position, not a name — the form supplies that
        assert!(element.name.is_empty());
        assert!(element.required, "a row that is there will be sent");

        let FieldType::Object(fields) = &element.field_type else {
            panic!("an aggregation has fields, not a {:?}", element.field_type);
        };
        let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["function", "as", "field"]);
        assert!(
            matches!(fields[0].field_type, FieldType::Enum(_)),
            "the function is a closed set, so it is a dropdown"
        );
        assert!(
            !fields[2].required,
            "`count` needs no field, so the element's does not"
        );
    }

    /// Every field of every component has to be something a form can render.
    /// A `Json` box is the honest fallback, but it is also the one a user has
    /// to hand-write, so nothing is allowed to fall back to it unnoticed.
    #[test]
    fn no_component_field_needs_raw_json() {
        let mut json_fields = Vec::new();
        for component in all_components() {
            collect_json_fields(&component.fields, &component.kind, &mut json_fields);
            for variant in &component.variants {
                let prefix = format!("{}.{}", component.kind, variant.name);
                collect_json_fields(&variant.fields, &prefix, &mut json_fields);
            }
        }
        assert!(
            json_fields.is_empty(),
            "these fields can only be filled in as raw JSON: {json_fields:?}"
        );
    }

    /// The whole way down, rather than the one level the config used to nest:
    /// a list of objects is two, and the point of the check is that *nothing*
    /// falls back to a JSON box unnoticed — least of all something buried.
    fn collect_json_fields(fields: &[FieldDoc], prefix: &str, found: &mut Vec<String>) {
        for field in fields {
            // a list element has no name; it is described by where it sits
            let at = if field.name.is_empty() {
                format!("{prefix}[]")
            } else {
                format!("{prefix}.{}", field.name)
            };
            match &field.field_type {
                FieldType::Json => found.push(at),
                FieldType::Object(fields) => collect_json_fields(fields, &at, found),
                FieldType::List(element) => {
                    collect_json_fields(std::slice::from_ref(element), &at, found);
                }
                FieldType::Union(union) => {
                    for variant in &union.variants {
                        collect_json_fields(
                            &variant.fields,
                            &format!("{at}.{}", variant.name),
                            found,
                        );
                    }
                }
                _ => {}
            }
        }
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

    /// The mapping is a list of forms rather than a JSON box, which is the
    /// whole reason the element is reflected as a `FieldDoc` — see
    /// `FieldType::List`.
    #[test]
    fn a_postgres_columns_field_is_a_list_of_described_columns() {
        let postgres = all_components()
            .into_iter()
            .find(|c| c.kind == "postgres" && matches!(c.family, Family::Output))
            .unwrap_or_else(|| panic!("the postgres output should be documented"));
        let columns = postgres
            .fields
            .iter()
            .find(|f| f.name == "columns")
            .unwrap_or_else(|| panic!("the postgres output should document its columns"));
        let FieldType::List(element) = &columns.field_type else {
            panic!("columns should be a list, not {:?}", columns.field_type);
        };
        let FieldType::Object(fields) = &element.field_type else {
            panic!("a column should be an object, not {:?}", element.field_type);
        };
        let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
        assert!(names.contains(&"name"), "{names:?}");
        assert!(names.contains(&"type"), "{names:?}");
        assert!(names.contains(&"field"), "{names:?}");
        // the type is a closed set, so the form gets a dropdown for it
        let column_type = fields
            .iter()
            .find(|f| f.name == "type")
            .unwrap_or_else(|| panic!("a column should document its type"));
        let FieldType::Enum(values) = &column_type.field_type else {
            panic!("a column type should be a closed set, not {:?}", column_type.field_type);
        };
        assert!(values.iter().any(|v| v == "float"), "{values:?}");
    }
}
