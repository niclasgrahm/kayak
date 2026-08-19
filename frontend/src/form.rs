//! Building a pipeline config from what someone typed into the "add pipeline"
//! modal.
//!
//! The form is not written by hand. `kayak_core::docs` already reflects
//! every component's fields out of its JSON schema — name, type, required —
//! and this turns that into a draft the UI can edit and, when it validates,
//! into the JSON that `POST /api/pipelines` takes. So a new component, or a new
//! field on one, gets a working form control and the right validation without
//! anyone touching the frontend, exactly as it gets a docs entry.
//!
//! Everything here is pure: raw strings in, `serde_json::Value` or a list of
//! errors out. `app.rs` only renders drafts and shows what comes back, which is
//! what keeps the interesting half testable without a browser.
//!
//! The validation is deliberately the *same* validation the server does, not a
//! weaker preview of it: a config this module accepts is one serde will accept.
//! It exists to say which box is wrong, in place, rather than to be the only
//! thing standing between the user and a bad config — the server still rejects
//! what it must, and a 422 is shown as-is.

use std::collections::HashMap;

use kayak_core::docs::{ComponentDoc, Family, FieldDoc, FieldType};
use kayak_core::schema::MessageSchema;
use serde_json::{Map, Value};

/// One component being filled in.
///
/// The values are raw strings — what an `<input>` holds — and stay that way
/// until the form is submitted. Parsing as you type would fight the user: a
/// half-typed `1.` is not a number yet, and neither is an empty box someone is
/// about to fill in.
///
/// A field with a shape of its own is stored flat, under a dotted [`path`]:
/// `buffer.type`, `buffer.size`. Flat rather than nested because that is what
/// makes the nesting free — one map of boxes to their contents, one error keyed
/// the same way, and no widget has to know how deep it sits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComponentDraft {
    pub family: Family,
    /// The `type` tag: which component this is.
    pub kind: String,
    /// Which form of an enum-shaped component is being configured — the
    /// `filter` transform's `Numeric` or `String`. `None` for everything else.
    pub variant: Option<String>,
    /// Raw field text, keyed by wire name. A field with no entry is untouched,
    /// which is the same as empty.
    pub values: HashMap<String, String>,
}

impl ComponentDraft {
    /// The raw text in one field.
    #[must_use]
    pub fn value(&self, field: &str) -> &str {
        self.values.get(field).map_or("", String::as_str)
    }
}

/// What's wrong with a form, in a shape the UI can put next to the thing that's
/// wrong rather than in a list at the bottom.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FormError {
    /// A problem with one field of the component at `component` (an index into
    /// the draft list, so two `nats` inputs stay distinguishable).
    Field {
        component: usize,
        field: String,
        message: String,
    },
    /// A problem with the pipeline as a whole — its id, or what it's missing.
    Pipeline(String),
}

/// The message for one field, if it has one.
#[must_use]
pub fn field_error<'a>(errors: &'a [FormError], component: usize, field: &str) -> Option<&'a str> {
    errors.iter().find_map(|e| match e {
        FormError::Field {
            component: at,
            field: name,
            message,
        } if *at == component && name == field => Some(message.as_str()),
        _ => None,
    })
}

/// Drop one field's message, because it has just been edited.
///
/// A "required" sitting under a box that now has something in it is worse than
/// no message at all: it says the form is still wrong when it isn't. The next
/// submit re-derives every message anyway, so removing one early can only ever
/// be temporary.
pub fn clear_field_error(errors: &mut Vec<FormError>, component: usize, field: &str) {
    errors.retain(|e| {
        !matches!(
            e,
            FormError::Field { component: at, field: name, .. } if *at == component && name == field
        )
    });
}

/// Whether this message is about a field of the component at `component`.
///
/// The counterpart to [`clear_field_error`] for the case where *every* message
/// about one component goes at once — re-validating that component on its own,
/// which is what sampling its input does. Clearing the lot would take the other
/// components' messages down with it, and those have not been re-derived.
#[must_use]
pub fn is_field_error_for(error: &FormError, component: usize) -> bool {
    matches!(error, FormError::Field { component: at, .. } if *at == component)
}

/// The messages that aren't about any one field.
#[must_use]
pub fn pipeline_errors(errors: &[FormError]) -> Vec<String> {
    errors
        .iter()
        .filter_map(|e| match e {
            FormError::Pipeline(message) => Some(message.clone()),
            FormError::Field { .. } => None,
        })
        .collect()
}

/// Every component of one family, in schema order — the dropdown's contents.
#[must_use]
pub fn kinds_in(docs: &[ComponentDoc], family: Family) -> Vec<&ComponentDoc> {
    docs.iter().filter(|c| c.family == family).collect()
}

/// The documentation for one component, or `None` if nothing by that name
/// exists — which can only happen if the UI is older than the server.
#[must_use]
pub fn doc_for<'a>(
    docs: &'a [ComponentDoc],
    family: Family,
    kind: &str,
) -> Option<&'a ComponentDoc> {
    docs.iter().find(|c| c.family == family && c.kind == kind)
}

/// A blank draft of a component: nothing filled in, and the first variant
/// preselected for the enum-shaped ones so the form is never in a state with no
/// fields at all.
#[must_use]
pub fn draft_of(doc: &ComponentDoc) -> ComponentDraft {
    ComponentDraft {
        family: doc.family,
        kind: doc.kind.clone(),
        variant: doc.variants.first().map(|v| v.name.clone()),
        values: HashMap::new(),
    }
}

/// A draft of the input that reads from an existing pipeline, already pointed
/// at `upstream` — what the "add a pipeline fed by this one" affordance on a
/// card opens the modal with.
///
/// Found by looking for an input with a [`FieldType::PipelineId`] field rather
/// than by naming the `pipeline` input, for the reason `docs.rs` looks for the
/// marker and not the field name: any input that grows a reference to another
/// pipeline should be usable here without this function learning about it.
/// Only top-level fields are considered — a nested reference would be a field
/// of some other value, not the thing this input is fed by.
///
/// `None` if no input has such a field, which the current schema always does;
/// the caller falls back to a blank form.
#[must_use]
pub fn draft_fed_by(docs: &[ComponentDoc], upstream: &str) -> Option<ComponentDraft> {
    let (doc, field) = kinds_in(docs, Family::Input).into_iter().find_map(|doc| {
        let field = doc
            .fields
            .iter()
            .find(|f| f.field_type == FieldType::PipelineId)?;
        Some((doc, field))
    })?;
    let mut draft = draft_of(doc);
    draft
        .values
        .insert(field.name.clone(), upstream.to_string());
    Some(draft)
}

/// The fields to render for a draft: a component's own, or — when it is
/// enum-shaped, like `filter` — the ones belonging to the selected variant.
#[must_use]
pub fn fields_of(doc: &ComponentDoc, variant: Option<&str>) -> Vec<FieldDoc> {
    if doc.variants.is_empty() {
        return doc.fields.clone();
    }
    let selected = variant.or_else(|| doc.variants.first().map(|v| v.name.as_str()));
    doc.variants
        .iter()
        .find(|v| Some(v.name.as_str()) == selected)
        .map(|v| v.fields.clone())
        .unwrap_or_default()
}

/// Where one field's text lives in a draft: its name, prefixed by the path of
/// whatever it is nested inside.
///
/// The top level has no prefix, so an ordinary field is stored under its own
/// name and nothing about the flat case changes.
#[must_use]
pub fn path(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}.{name}")
    }
}

/// How many rows a list field may hold.
///
/// The count is read out of the same string map everything else lives in, so it
/// is a number someone could in principle have put anything in. A ceiling keeps
/// a stray value from asking the renderer for a million rows; it is far above
/// any real aggregation list.
pub const MAX_LIST_ROWS: usize = 64;

/// How many rows a list field currently has.
///
/// A list's own path holds its length rather than a value — `aggregations` is
/// `"3"`, and the rows themselves live at `aggregations.0`, `aggregations.1`
/// and so on. Storing the count is what lets a row exist while it is still
/// empty: counting the keys that happen to be filled in would make a freshly
/// added row disappear until something was typed into it.
#[must_use]
pub fn list_len(values: &HashMap<String, String>, at: &str) -> usize {
    values
        .get(at)
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(0)
        .min(MAX_LIST_ROWS)
}

/// The sub-fields of a list row that a sample can answer, found by their
/// **type** rather than by their name.
///
/// Same rule [`draft_fed_by`] follows and that `kayak_core::docs` follows one
/// level up: this module knows the name of no component and no component's
/// fields. A row that maps a message field onto something is recognisable
/// without knowing it is a column — it has a [`FieldType::MessageField`] in it
/// — and the two other things a sample knows are then found the same way.
struct RowRoles {
    /// where the field's path goes.
    field: String,
    /// where a name for it goes: the first *required* text box in the row,
    /// which is what a row that names its target has. Absent is fine — the
    /// row is filled in as far as it can be.
    name: Option<String>,
    /// where a type goes, with the values that dropdown accepts. A suggestion
    /// is only written if it is one of them, so a dropdown that turns out to
    /// be about something else is left alone rather than filled with nonsense.
    types: Option<(String, Vec<String>)>,
}

impl RowRoles {
    /// The roles in one list element, or `None` if it is not a row that maps a
    /// message field at all.
    fn of(element: &FieldDoc) -> Option<Self> {
        let FieldType::Object(fields) = &element.field_type else {
            return None;
        };
        let field = fields
            .iter()
            .find(|f| f.field_type == FieldType::MessageField)?
            .name
            .clone();
        let name = fields
            .iter()
            .find(|f| f.required && f.field_type == FieldType::Text)
            .map(|f| f.name.clone());
        let types = fields.iter().find_map(|f| match &f.field_type {
            FieldType::Enum(values) => Some((f.name.clone(), values.clone())),
            _ => None,
        });
        Some(Self { field, name, types })
    }
}

/// Whether a list of rows can be filled in from a sample at all — what the
/// "fill from sample" button asks before it draws itself.
#[must_use]
pub fn can_fill_from_sample(element: &FieldDoc) -> bool {
    RowRoles::of(element).is_some()
}

/// Adds a row per field the sample carried, and says how many rows there are
/// now.
///
/// Three rules, and they are the difference between a button that saves typing
/// and one that has to be undone:
///
/// - **A field already mapped is skipped**, so pressing it twice adds nothing
///   and it never overwrites a row someone has edited. Appending rather than
///   replacing is what makes it safe to press at all.
/// - **A path with children of its own is skipped.** A message carrying
///   `sensor.id` offers both `sensor` and `sensor.id`; mapping both would
///   store the same data twice, once as JSON and once as a column.
/// - **Nullability is not written down.** The sample knows whether a field was
///   ever missing, and that is deliberately not turned into `nullable: false`:
///   five messages cannot prove a field is always there, and a column declared
///   `NOT NULL` on that evidence is a pipeline that fails at three in the
///   morning. The default — nullable — is the honest reading, and the sample's
///   answer is shown in the field list instead.
///
/// A field the sample disagreed about (a number in one message, a string in
/// the next) gets its row with the **type box left empty**: there is no honest
/// suggestion, and a required box nobody has answered is exactly what that
/// should look like.
pub fn fill_from_sample(
    values: &mut HashMap<String, String>,
    at: &str,
    element: &FieldDoc,
    schema: &MessageSchema,
) -> usize {
    let Some(roles) = RowRoles::of(element) else {
        return list_len(values, at);
    };
    let mut len = list_len(values, at);
    let mapped: Vec<String> = (0..len)
        .filter_map(|row| {
            let row_at = path(at, &row.to_string());
            values
                .get(&path(&row_at, &roles.field))
                .filter(|value| !value.trim().is_empty())
                .cloned()
        })
        .collect();

    for field in &schema.fields {
        if len >= MAX_LIST_ROWS {
            break;
        }
        let prefix = format!("{}.", field.path);
        if schema.fields.iter().any(|other| other.path.starts_with(&prefix)) {
            continue;
        }
        if mapped.iter().any(|already| already == &field.path) {
            continue;
        }
        let row_at = path(at, &len.to_string());
        values.insert(path(&row_at, &roles.field), field.path.clone());
        if let Some(name) = &roles.name {
            values.insert(path(&row_at, name), column_name(&field.path));
        }
        if let Some((box_name, accepted)) = &roles.types
            && let Some(suggestion) = field.suggested_column()
            && accepted.iter().any(|value| value == suggestion.as_str())
        {
            values.insert(path(&row_at, box_name), suggestion.as_str().to_string());
        }
        len += 1;
        values.insert(at.to_string(), len.to_string());
    }
    len
}

/// A field path as a name for the thing it lands in.
///
/// The whole path rather than its last segment, because two paths can share a
/// leaf — `sensor.id` and `device.id` are different fields and must not become
/// one column. Everything a name may not contain becomes an underscore, and a
/// name that would start with a digit gets one in front: it ends up in an SQL
/// identifier, which the output checks and would otherwise refuse.
fn column_name(path: &str) -> String {
    let mut name: String = path
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    if name.starts_with(|c: char| c.is_ascii_digit()) {
        name.insert(0, '_');
    }
    name
}

/// Add a row to the end of a list, and say how many there are now.
pub fn push_list_element(values: &mut HashMap<String, String>, at: &str) -> usize {
    let len = (list_len(values, at) + 1).min(MAX_LIST_ROWS);
    values.insert(at.to_string(), len.to_string());
    len
}

/// Take one row out of a list, and say how many are left.
///
/// The rows after it shift down, because a row's *position* is its identity
/// here: everything about row 2 is stored under keys beginning `at.2`, so
/// removing row 1 without moving row 2 down would leave a gap the renderer
/// reads as an empty row and a set of orphaned keys under it.
pub fn remove_list_element(values: &mut HashMap<String, String>, at: &str, index: usize) -> usize {
    let len = list_len(values, at);
    if index >= len {
        return len;
    }
    for row in index..len - 1 {
        move_keys_under(values, &path(at, &(row + 1).to_string()), &path(at, &row.to_string()));
    }
    drop_keys_under(values, &path(at, &(len - 1).to_string()));
    values.insert(at.to_string(), (len - 1).to_string());
    len - 1
}

/// Every key belonging to one row, moved to another row's path.
fn move_keys_under(values: &mut HashMap<String, String>, from: &str, to: &str) {
    for (key, value) in take_keys_under(values, from) {
        let suffix = key.split_at(from.len()).1;
        values.insert(format!("{to}{suffix}"), value);
    }
}

fn drop_keys_under(values: &mut HashMap<String, String>, at: &str) {
    take_keys_under(values, at);
}

/// A row's own value and everything nested inside it — `aggregations.1` and
/// `aggregations.1.function` alike, and nothing from `aggregations.10`.
fn take_keys_under(values: &mut HashMap<String, String>, at: &str) -> Vec<(String, String)> {
    let taken: Vec<String> = values
        .keys()
        .filter(|key| key.as_str() == at || key.starts_with(&format!("{at}.")))
        .cloned()
        .collect();
    taken
        .into_iter()
        .filter_map(|key| values.remove(&key).map(|value| (key, value)))
        .collect()
}

/// Drop every message belonging to a list, because its rows have just moved.
///
/// The alternative would be shifting the messages along with the values, which
/// is more work to say something less true: the rows below the removed one are
/// different rows now, and the next submit re-derives every message anyway.
pub fn clear_list_errors(errors: &mut Vec<FormError>, component: usize, at: &str) {
    let under = format!("{at}.");
    errors.retain(|e| {
        !matches!(
            e,
            FormError::Field { component: which, field, .. }
                if *which == component && (field == at || field.starts_with(&under))
        )
    });
}

/// Turn one field's raw text into the JSON value it stands for.
///
/// `Ok(None)` means "leave it out" — an empty optional field, which is not the
/// same as an empty string: writing `"subject": ""` where the field was meant
/// to be absent is a different config.
///
/// This answers for one box. A field with a shape of its own is spread over
/// several of them, so it is [`value_at`] that builds those — what arrives here
/// for one of those is the literal JSON a caller has, which is the same
/// fallback the form used to offer for all of them.
pub fn parse_field(field: &FieldDoc, raw: &str) -> Result<Option<Value>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return if field.required {
            Err("required".to_string())
        } else {
            Ok(None)
        };
    }
    let value = match &field.field_type {
        // not trimmed: a leading space in a password or a subject is a
        // legitimate, if unlikely, thing to want
        FieldType::Text => Value::String(raw.to_string()),
        FieldType::Integer => Value::from(
            trimmed
                .parse::<i64>()
                .map_err(|_| format!("'{trimmed}' is not a whole number"))?,
        ),
        FieldType::Number => Value::from(
            trimmed
                .parse::<f64>()
                .map_err(|_| format!("'{trimmed}' is not a number"))?,
        ),
        FieldType::Boolean => Value::Bool(
            trimmed
                .parse::<bool>()
                .map_err(|_| format!("'{trimmed}' is not true or false"))?,
        ),
        // an id: it goes in a URL path, so surrounding whitespace can only be a
        // slip of the keyboard. A connection name is the same kind of thing —
        // picked from a dropdown of what exists, sent as the plain name
        // and a message field is trimmed for the same reason: it is a path
        // read by `fields::get`, where a stray space is a segment that will
        // never match rather than a field anyone meant
        FieldType::PipelineId | FieldType::Connection(_) | FieldType::MessageField => {
            Value::String(trimmed.to_string())
        }
        // code, sent exactly as it was typed. Not trimmed for a stronger reason
        // than the text arm's: leading whitespace is *indentation*, and a
        // trailing newline is what every editor leaves behind. Reformatting
        // someone's source on the way to the server would also move every line
        // number the dry run reports.
        FieldType::Script(_) => Value::String(raw.to_string()),
        FieldType::Enum(values) => {
            if values.iter().any(|v| v == trimmed) {
                Value::String(trimmed.to_string())
            } else {
                return Err(format!("must be one of: {}", values.join(", ")));
            }
        }
        FieldType::Json | FieldType::Union(_) | FieldType::Object(_) | FieldType::List(_) => {
            serde_json::from_str(trimmed).map_err(|e| format!("not valid JSON: {e}"))?
        }
    };
    Ok(Some(value))
}

/// One field's value, from however many boxes it is spread over.
///
/// The recursive half of the form, and the whole of what "conditional
/// rendering" comes to once the shape is known: an object is its fields, and a
/// union is *the fields of the variant that was picked* — so what is read here
/// depends on an answer given elsewhere in the same draft, exactly as what is
/// rendered does.
///
/// Errors are pushed as they're found rather than returned, and keyed by full
/// path, so a message lands under the box it belongs to however deep that is.
fn value_at(
    field: &FieldDoc,
    prefix: &str,
    draft: &ComponentDraft,
    errors: &mut Vec<(String, String)>,
) -> Option<Value> {
    let at = path(prefix, &field.name);
    match &field.field_type {
        FieldType::Object(fields) => {
            // Into a scratch list, because whether these are errors at all
            // depends on what the walk finds: nothing inside an object nobody
            // filled in can be *missing*, and a buffer's `until` — an optional
            // object with a required list inside it — reported "conditions is
            // required" on every draft that left the gate out until this held
            // them back.
            let mut nested = Vec::new();
            let body = object_at(fields, &at, draft, &mut nested);
            // an object nobody touched is an absent field, not an empty one:
            // `"rotate": {}` says "rotate on nothing", which is a different
            // thing to say than saying nothing
            if !field.required && !touched(draft, &at) {
                return None;
            }
            errors.append(&mut nested);
            Some(Value::Object(body))
        }
        // As many values as there are rows, each of them read exactly as the
        // element type would be read anywhere else — which is what keeps a list
        // of objects from needing anything of its own. The element's name is
        // its position: a list element has none until the form gives it one.
        FieldType::List(element) => {
            let rows = list_len(&draft.values, &at);
            let mut items = Vec::with_capacity(rows);
            for row in 0..rows {
                let mut element = (**element).clone();
                element.name = row.to_string();
                if let Some(value) = value_at(&element, &at, draft, errors) {
                    items.push(value);
                }
            }
            // no rows at all is an absent field, not `[]` — the same bargain
            // the object above makes, and what keeps `group_by` out of a config
            // that isn't grouping
            if items.is_empty() {
                if field.required {
                    errors.push((at, "required".to_string()));
                }
                return None;
            }
            Some(Value::Array(items))
        }
        FieldType::Union(union) => {
            let tag = path(&at, &union.tag);
            let chosen = draft.value(&tag).trim().to_string();
            if chosen.is_empty() {
                if field.required {
                    errors.push((tag, "required".to_string()));
                }
                return None;
            }
            let Some(variant) = union.variants.iter().find(|v| v.name == chosen) else {
                let names: Vec<&str> = union.variants.iter().map(|v| v.name.as_str()).collect();
                errors.push((tag, format!("must be one of: {}", names.join(", "))));
                return None;
            };
            let mut body = object_at(&variant.fields, &at, draft, errors);
            body.insert(union.tag.clone(), Value::String(chosen));
            Some(Value::Object(body))
        }
        _ => match parse_field(field, draft.value(&at)) {
            Ok(value) => value,
            Err(message) => {
                errors.push((at, message));
                None
            }
        },
    }
}

/// Whether anything under this path has been filled in.
///
/// Asked of the *draft* rather than of the built body, because the two differ
/// exactly where it matters: a box someone typed an unreadable number into
/// produces no value and an error, and an object holding only that is one
/// somebody plainly meant to fill in. Reading the body instead would drop it —
/// error and all — and the form would sit there refusing to submit with
/// nothing marked.
fn touched(draft: &ComponentDraft, at: &str) -> bool {
    let prefix = format!("{at}.");
    draft
        .values
        .iter()
        .any(|(key, value)| key.starts_with(&prefix) && !value.trim().is_empty())
}

/// A set of fields under one path, as the object they make up.
fn object_at(
    fields: &[FieldDoc],
    prefix: &str,
    draft: &ComponentDraft,
    errors: &mut Vec<(String, String)>,
) -> Map<String, Value> {
    let mut body = Map::new();
    for field in fields {
        if let Some(value) = value_at(field, prefix, draft, errors) {
            body.insert(field.name.clone(), value);
        }
    }
    body
}

/// One component as it goes on the wire: `{"type": "nats", ...}`, or
/// `{"type": "filter", "Numeric": {...}}` for the enum-shaped ones.
///
/// Errors come back per field rather than one at a time, so the whole form
/// lights up at once instead of one box per submit.
pub fn component_json(
    doc: &ComponentDoc,
    draft: &ComponentDraft,
) -> Result<Value, Vec<(String, String)>> {
    let mut errors = Vec::new();
    let mut body = object_at(
        &fields_of(doc, draft.variant.as_deref()),
        "",
        draft,
        &mut errors,
    );
    if !errors.is_empty() {
        return Err(errors);
    }

    let mut out = Map::new();
    out.insert("type".to_string(), Value::String(doc.kind.clone()));
    match (&draft.variant, doc.variants.is_empty()) {
        // the variant name is the key the fields hang off; the flattened enum
        // has no tag of its own
        (Some(variant), false) => {
            out.insert(variant.clone(), Value::Object(body));
        }
        _ => out.append(&mut body),
    }
    Ok(Value::Object(out))
}

/// Characters an id may contain. It ends up in a URL path
/// (`DELETE /api/pipelines/{id}`) and in another pipeline's `upstream`, so it is
/// kept to the set that needs no escaping anywhere.
fn is_id_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.')
}

/// The whole form: an id and a flat list of drafts, in, and the body of
/// `POST /api/pipelines` out.
///
/// A blank id is not an error — the server generates a petname, which is the
/// documented way to not care what a pipeline is called.
///
/// Every problem found is returned, not just the first: a form that reports one
/// error per submit is a form you submit five times.
pub fn build_config(
    id: &str,
    drafts: &[ComponentDraft],
    docs: &[ComponentDoc],
) -> Result<Value, Vec<FormError>> {
    let mut errors = Vec::new();
    let id = id.trim();
    if !id.is_empty() && !id.chars().all(is_id_char) {
        errors.push(FormError::Pipeline(
            "an id may only contain letters, digits, '-', '_' and '.'".to_string(),
        ));
    }

    let mut stages: HashMap<Family, Vec<Value>> = HashMap::new();
    for (index, draft) in drafts.iter().enumerate() {
        let Some(doc) = doc_for(docs, draft.family, &draft.kind) else {
            errors.push(FormError::Pipeline(format!(
                "there is no {} called '{}'",
                singular(draft.family),
                draft.kind
            )));
            continue;
        };
        match component_json(doc, draft) {
            Ok(value) => stages.entry(draft.family).or_default().push(value),
            Err(field_errors) => {
                errors.extend(
                    field_errors
                        .into_iter()
                        .map(|(field, message)| FormError::Field {
                            component: index,
                            field,
                            message,
                        }),
                )
            }
        }
    }

    // a pipeline with no input could never produce anything; the server rejects
    // one too, but saying so before the round trip is the point of this module
    if !drafts.iter().any(|d| d.family == Family::Input) {
        errors.push(FormError::Pipeline(
            "a pipeline needs at least one input".to_string(),
        ));
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    let mut stage = |family: Family| Value::Array(stages.remove(&family).unwrap_or_default());
    Ok(Value::Object(Map::from_iter([
        (
            "id".to_string(),
            if id.is_empty() {
                Value::Null
            } else {
                Value::String(id.to_string())
            },
        ),
        ("inputs".to_string(), stage(Family::Input)),
        ("transforms".to_string(), stage(Family::Transform)),
        ("outputs".to_string(), stage(Family::Output)),
    ])))
}

/// The draft's transforms as they would go on the wire, each with **its
/// position in the draft list**.
///
/// The position rather than its place among the transforms, because that is
/// what everything else about a draft is keyed by — a field error, a sample,
/// the suggestions behind a message-field box — and a second numbering that
/// agreed with the first only while there was exactly one input would be a
/// bug waiting to be written.
///
/// `Err` names the components that cannot be built yet, so the caller can say
/// which ones rather than "something is wrong". A half-filled transform is the
/// ordinary state of a form someone is still typing into, so this is a
/// question with an expected negative answer rather than a failure.
pub fn transform_chain(
    drafts: &[ComponentDraft],
    docs: &[ComponentDoc],
) -> Result<Vec<(usize, Value)>, Vec<usize>> {
    let mut chain = Vec::new();
    let mut unbuildable = Vec::new();
    for (index, draft) in drafts.iter().enumerate() {
        if draft.family != Family::Transform {
            continue;
        }
        match doc_for(docs, draft.family, &draft.kind).map(|doc| component_json(doc, draft)) {
            Some(Ok(value)) => chain.push((index, value)),
            _ => unbuildable.push(index),
        }
    }
    if unbuildable.is_empty() {
        Ok(chain)
    } else {
        Err(unbuildable)
    }
}

/// A family named as one component rather than as the stage — "there is no
/// input called 'x'" reads better than "no inputs called 'x'".
#[must_use]
pub fn singular(family: Family) -> &'static str {
    match family {
        Family::Input => "input",
        Family::Transform => "transform",
        Family::Output => "output",
        Family::Connection => "connection",
    }
}

/// The "add connection" form: a name and one component draft in, the body of
/// `POST /api/connections` out.
///
/// The pipeline form's smaller sibling, and deliberately built out of the same
/// parts — the fields come from the same reflection, the messages come back in
/// the same shape, so a new connection kind gets a working form for free.
///
/// The one real difference is the name: a pipeline may go without and be given
/// a petname, but a connection *is* its name — that is the whole of how a
/// component refers to it — so a blank one is an error.
pub fn build_connection(
    id: &str,
    draft: &ComponentDraft,
    docs: &[ComponentDoc],
) -> Result<Value, Vec<FormError>> {
    let mut errors = Vec::new();
    let id = id.trim();
    if id.is_empty() {
        errors.push(FormError::Pipeline(
            "a connection needs a name — it is what a pipeline refers to".to_string(),
        ));
    } else if !id.chars().all(is_id_char) {
        errors.push(FormError::Pipeline(
            "a name may only contain letters, digits, '-', '_' and '.'".to_string(),
        ));
    }

    let mut body =
        match doc_for(docs, Family::Connection, &draft.kind) {
            Some(doc) => match component_json(doc, draft) {
                Ok(Value::Object(body)) => body,
                // `component_json` builds an object or nothing; the other arms
                // cannot happen, and inventing an error for them would be worse
                // than the empty one they'd produce
                Ok(_) => Map::new(),
                Err(field_errors) => {
                    errors.extend(field_errors.into_iter().map(|(field, message)| {
                        FormError::Field {
                            component: 0,
                            field,
                            message,
                        }
                    }));
                    Map::new()
                }
            },
            None => {
                errors.push(FormError::Pipeline(format!(
                    "there is no connection called '{}'",
                    draft.kind
                )));
                Map::new()
            }
        };

    if !errors.is_empty() {
        return Err(errors);
    }
    // the name sits alongside the flattened connection, which is exactly one
    // entry of the connections file
    let mut out = Map::from_iter([("id".to_string(), Value::String(id.to_string()))]);
    out.append(&mut body);
    Ok(Value::Object(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use kayak_core::config::Config;
    use kayak_core::docs::all_components;
    use serde_json::json;

    fn docs() -> Vec<ComponentDoc> {
        all_components()
    }

    fn component(family: Family, kind: &str) -> ComponentDoc {
        match doc_for(&docs(), family, kind) {
            Some(c) => c.clone(),
            None => panic!("no {} called '{kind}'", singular(family)),
        }
    }

    /// A draft with its fields filled in.
    fn filled(family: Family, kind: &str, values: &[(&str, &str)]) -> ComponentDraft {
        let mut draft = draft_of(&component(family, kind));
        for (name, value) in values {
            draft
                .values
                .insert((*name).to_string(), (*value).to_string());
        }
        draft
    }

    fn dummy_input() -> ComponentDraft {
        filled(Family::Input, "dummy", &[("duration", "5")])
    }

    fn field_named<'a>(component: &'a ComponentDoc, name: &str) -> &'a FieldDoc {
        match component.fields.iter().find(|f| f.name == name) {
            Some(f) => f,
            None => panic!("no field '{name}' on '{}'", component.kind),
        }
    }

    /// One row of the postgres output's `columns`.
    fn column_element() -> FieldDoc {
        let postgres = component(Family::Output, "postgres");
        let columns = field_named(&postgres, "columns");
        match &columns.field_type {
            FieldType::List(element) => (**element).clone(),
            other => panic!("columns is a list, not a {other:?}"),
        }
    }

    /// The postgres output's `columns` is the list this exists for: a row per
    /// field the sample carried, with the path in the field box, a name made
    /// from it and the type the sample suggests.
    #[test]
    fn a_column_list_is_filled_in_from_a_sample() {
        let postgres = component(Family::Output, "postgres");
        let columns = field_named(&postgres, "columns");
        let FieldType::List(element) = &columns.field_type else {
            panic!("columns is a list");
        };
        let schema = kayak_core::schema::infer(&[json!({
            "current_time": "2026-08-18T09:12:00Z",
            "value": 0.5,
        })]);

        let mut values = HashMap::new();
        let rows = fill_from_sample(&mut values, "columns", element, &schema);

        assert_eq!(rows, 2);
        assert_eq!(values.get("columns"), Some(&"2".to_string()));
        assert_eq!(
            values.get("columns.0.field"),
            Some(&"current_time".to_string())
        );
        assert_eq!(
            values.get("columns.0.name"),
            Some(&"current_time".to_string())
        );
        // the shape of the string is what makes this a timestamp rather than
        // text — see `kayak_core::schema::TextFormat`
        assert_eq!(values.get("columns.0.type"), Some(&"timestamp".to_string()));
        assert_eq!(values.get("columns.1.field"), Some(&"value".to_string()));
        assert_eq!(values.get("columns.1.type"), Some(&"float".to_string()));
    }

    /// Pressing it twice must add nothing, and it must never overwrite a row
    /// someone has edited — appending rather than replacing is what makes it
    /// safe to press at all.
    #[test]
    fn filling_twice_adds_nothing_and_leaves_edited_rows_alone() {
        let element = column_element();
        let schema = kayak_core::schema::infer(&[json!({"a": 1, "b": 2})]);

        let mut values = HashMap::new();
        assert_eq!(fill_from_sample(&mut values, "columns", &element, &schema), 2);
        values.insert("columns.0.name".to_string(), "renamed_by_hand".to_string());

        assert_eq!(fill_from_sample(&mut values, "columns", &element, &schema), 2);
        assert_eq!(
            values.get("columns.0.name"),
            Some(&"renamed_by_hand".to_string())
        );
    }

    /// A message carrying `sensor.id` offers both `sensor` and `sensor.id`;
    /// mapping both would store the same data twice, once as JSON and once as
    /// a column.
    #[test]
    fn a_path_with_children_is_not_mapped_as_well_as_its_children() {
        let element = column_element();
        let schema = kayak_core::schema::infer(&[json!({"sensor": {"id": 1}})]);

        let mut values = HashMap::new();
        assert_eq!(fill_from_sample(&mut values, "columns", &element, &schema), 1);
        assert_eq!(
            values.get("columns.0.field"),
            Some(&"sensor.id".to_string())
        );
        // and the name carries the whole path, since two paths can share a leaf
        assert_eq!(
            values.get("columns.0.name"),
            Some(&"sensor_id".to_string())
        );
    }

    /// There is no honest suggestion for a field the sample disagreed about,
    /// and a required box nobody has answered is what that should look like.
    #[test]
    fn a_field_the_sample_disagreed_about_gets_no_type() {
        let element = column_element();
        let schema = kayak_core::schema::infer(&[json!({"a": 1}), json!({"a": "x"})]);

        let mut values = HashMap::new();
        fill_from_sample(&mut values, "columns", &element, &schema);
        assert_eq!(values.get("columns.0.field"), Some(&"a".to_string()));
        assert_eq!(values.get("columns.0.type"), None);
    }

    /// Five messages cannot prove a field is always there, so what the sample
    /// knows about absence is deliberately not written into the row.
    #[test]
    fn nullability_is_not_guessed_from_a_sample() {
        let element = column_element();
        let schema = kayak_core::schema::infer(&[json!({"a": 1, "b": 2}), json!({"a": 1})]);

        let mut values = HashMap::new();
        fill_from_sample(&mut values, "columns", &element, &schema);
        assert!(
            values.keys().all(|key| !key.ends_with("nullable")),
            "the sample decided a column's nullability: {values:?}"
        );
    }

    /// A list of something else is left alone rather than filled with nonsense
    /// — the row has to be one that maps a message field.
    #[test]
    fn a_list_that_maps_no_message_field_cannot_be_filled() {
        let reducer = component(Family::Transform, "reducer");
        let group_by = field_named(&reducer, "group_by");
        let FieldType::List(element) = &group_by.field_type else {
            panic!("group_by is a list");
        };
        assert!(!can_fill_from_sample(element));

        let mut values = HashMap::new();
        let schema = kayak_core::schema::infer(&[json!({"a": 1})]);
        assert_eq!(fill_from_sample(&mut values, "group_by", element, &schema), 0);
        assert!(values.is_empty());
    }

    /// The chain a dry run is given, keyed by draft position — which is the
    /// key everything else about a draft uses.
    #[test]
    fn the_transform_chain_carries_each_transforms_place_in_the_draft() {
        let drafts = vec![
            dummy_input(),
            filled(Family::Transform, "splitter", &[("out_size", "2")]),
            filled(Family::Output, "stdout", &[]),
            filled(Family::Transform, "buffer", &[("size", "10")]),
        ];
        let chain = match transform_chain(&drafts, &docs()) {
            Ok(chain) => chain,
            Err(unbuildable) => panic!("nothing should be unbuildable: {unbuildable:?}"),
        };
        let positions: Vec<usize> = chain.iter().map(|(at, _)| *at).collect();
        assert_eq!(positions, [1, 3], "the output is not a stage, and 3 is not 2");
        assert_eq!(chain[0].1["type"], "splitter");
    }

    /// A half-filled transform is the ordinary state of a form someone is
    /// still typing into, so it comes back as a list of which ones rather than
    /// as a failure with nothing in it.
    #[test]
    fn a_transform_that_cannot_be_built_yet_is_named() {
        let drafts = vec![
            dummy_input(),
            // `out_size` is required and empty
            filled(Family::Transform, "splitter", &[]),
        ];
        match transform_chain(&drafts, &docs()) {
            Ok(chain) => panic!("built an empty splitter: {chain:?}"),
            Err(unbuildable) => assert_eq!(unbuildable, [1]),
        }
    }

    #[test]
    fn the_dropdown_offers_every_component_of_one_family() {
        let docs = docs();
        let inputs: Vec<&str> = kinds_in(&docs, Family::Input)
            .iter()
            .map(|c| c.kind.as_str())
            .collect();
        assert!(inputs.contains(&"nats") && inputs.contains(&"dummy"));
        assert!(!inputs.contains(&"stdout"), "that's an output");
    }

    /// Two components share the name `nats`, one an input and one an output,
    /// with different fields. Picking by kind alone would build the wrong form.
    #[test]
    fn a_kind_is_looked_up_within_its_family() {
        let input = component(Family::Input, "nats");
        let output = component(Family::Output, "nats");
        assert!(input.fields.iter().any(|f| f.name == "buffer"));
        assert!(!output.fields.iter().any(|f| f.name == "buffer"));
    }

    /// The card's "add a pipeline fed by this one" opens the modal on this
    /// draft, so it has to be a complete, buildable input — not just the right
    /// kind with the id dropped somewhere.
    #[test]
    fn a_seeded_input_reads_from_the_pipeline_it_was_seeded_with() -> anyhow::Result<()> {
        let docs = docs();
        let draft = match draft_fed_by(&docs, "ingest") {
            Some(draft) => draft,
            None => panic!("no input takes a pipeline id"),
        };
        assert_eq!(draft.family, Family::Input);
        let doc = component(Family::Input, &draft.kind);
        let json = component_json(&doc, &draft).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert_eq!(json, json!({"type": "pipeline", "upstream": "ingest"}));
        Ok(())
    }

    /// The seed is looked up by the field's *type*, so the dropdown the modal
    /// renders for it is the same control that would be there if it had been
    /// picked by hand — and the marked field is the one that got the id.
    #[test]
    fn the_seeded_field_is_the_one_marked_as_a_pipeline_id() {
        let docs = docs();
        let draft = match draft_fed_by(&docs, "ingest") {
            Some(draft) => draft,
            None => panic!("no input takes a pipeline id"),
        };
        let doc = component(Family::Input, &draft.kind);
        let marked: Vec<&str> = doc
            .fields
            .iter()
            .filter(|f| f.field_type == FieldType::PipelineId)
            .map(|f| f.name.as_str())
            .collect();
        assert_eq!(marked.len(), 1, "one field should carry the id");
        assert_eq!(draft.values.get(marked[0]).map(String::as_str), Some("ingest"));
    }

    #[test]
    fn a_filled_in_component_becomes_its_wire_format() -> anyhow::Result<()> {
        let draft = filled(
            Family::Input,
            "nats",
            &[("connection", "local-nats"), ("subject", "test.subject")],
        );
        let json = component_json(&component(Family::Input, "nats"), &draft)
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert_eq!(
            json,
            json!({"type": "nats", "connection": "local-nats", "subject": "test.subject"})
        );
        Ok(())
    }

    /// The connection reference renders as a dropdown of the connections that
    /// exist, of the right kind — and goes on the wire as the plain name.
    #[test]
    fn a_connection_reference_is_built_as_the_name_it_picks() -> anyhow::Result<()> {
        let kafka = component(Family::Input, "kafka");
        assert_eq!(
            fields_of(&kafka, None)
                .iter()
                .find(|f| f.name == "connection")
                .map(|f| f.field_type.clone()),
            Some(FieldType::Connection("kafka".to_string())),
            "the form would render a text box for it"
        );

        let draft = filled(
            Family::Input,
            "kafka",
            &[
                ("connection", " prod-kafka "),
                ("topic", "orders"),
                ("group", "kayak"),
            ],
        );
        let json = component_json(&kafka, &draft).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert_eq!(json["connection"], json!("prod-kafka"));
        Ok(())
    }

    /// The add-connection form is the pipeline form's sibling: same fields from
    /// the same reflection, and a body that is one entry of the connections
    /// file.
    #[test]
    fn a_connection_form_builds_the_body_the_server_takes() -> anyhow::Result<()> {
        let draft = filled(
            Family::Connection,
            "kafka",
            &[("brokers", "${KAFKA_BROKERS}")],
        );
        let value = build_connection(" prod-kafka ", &draft, &docs())
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert_eq!(
            value,
            json!({"id": "prod-kafka", "type": "kafka", "brokers": "${KAFKA_BROKERS}"})
        );

        let request: kayak_core::connections::CreateConnectionRequest =
            serde_json::from_value(value)?;
        assert_eq!(request.id, "prod-kafka");
        Ok(())
    }

    /// A connection *is* its name — there is no petname fallback, because the
    /// name is the whole of how a component refers to it.
    #[test]
    fn a_connection_without_a_name_is_rejected() {
        let draft = filled(Family::Connection, "nats", &[("urls", "nats://localhost")]);
        let errors = build_connection("  ", &draft, &docs())
            .expect_err("a connection with no name cannot be referred to");
        assert_eq!(pipeline_errors(&errors).len(), 1);
    }

    #[test]
    fn a_connections_missing_fields_are_reported_against_them() {
        let draft = filled(Family::Connection, "postgres", &[("host", "db")]);
        let errors =
            build_connection("warehouse", &draft, &docs()).expect_err("most of it is missing");
        assert_eq!(field_error(&errors, 0, "user"), Some("required"));
        assert_eq!(field_error(&errors, 0, "host"), None);
    }

    /// A `pipeline` input names another pipeline. The modal renders that as a
    /// dropdown of the pipelines that exist, but the wire format is the plain
    /// id — and since it is an id, stray whitespace around it is a slip rather
    /// than part of the name.
    #[test]
    fn a_pipeline_reference_is_built_as_the_id_it_names() -> anyhow::Result<()> {
        let pipeline = component(Family::Input, "pipeline");
        assert_eq!(
            fields_of(&pipeline, None)
                .iter()
                .find(|f| f.name == "upstream")
                .map(|f| f.field_type.clone()),
            Some(FieldType::PipelineId),
            "the form would render a text box for it"
        );

        let draft = filled(Family::Input, "pipeline", &[("upstream", " source ")]);
        let json = component_json(&pipeline, &draft).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert_eq!(json, json!({"type": "pipeline", "upstream": "source"}));
        Ok(())
    }

    /// Nothing picked is still nothing picked: a pipeline with no upstream
    /// can't be built, and the message belongs on that field.
    #[test]
    fn a_pipeline_reference_left_unpicked_is_reported_as_required() {
        let draft = filled(Family::Input, "pipeline", &[]);
        let errors = component_json(&component(Family::Input, "pipeline"), &draft)
            .expect_err("a pipeline input with no upstream should not validate");
        assert_eq!(errors, [("upstream".to_string(), "required".to_string())]);
    }

    /// An untouched optional field is left out entirely. Emitting `""` would be
    /// a different config — and for `start_at`, an invalid one.
    #[test]
    fn an_empty_optional_field_is_omitted_rather_than_sent_blank() -> anyhow::Result<()> {
        let draft = filled(
            Family::Connection,
            "postgres",
            &[
                ("host", "localhost"),
                ("database", "kayak"),
                ("user", "kayak"),
                ("password", "${POSTGRES_PASSWORD}"),
            ],
        );
        let json = component_json(&component(Family::Connection, "postgres"), &draft)
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert!(json.get("port").is_none(), "an empty port was sent: {json}");
        Ok(())
    }

    #[test]
    fn a_missing_required_field_is_reported_against_that_field() {
        let draft = filled(Family::Input, "nats", &[("connection", "local-nats")]);
        let errors = component_json(&component(Family::Input, "nats"), &draft)
            .expect_err("a nats input with no subject should not validate");
        assert_eq!(errors, [("subject".to_string(), "required".to_string())]);
    }

    /// Every bad field at once: one message per submit makes for five submits.
    #[test]
    fn all_the_bad_fields_are_reported_together() {
        let draft = filled(Family::Input, "kafka", &[("connection", "prod-kafka")]);
        let errors = component_json(&component(Family::Input, "kafka"), &draft)
            .expect_err("two required fields are missing");
        let named: Vec<&str> = errors.iter().map(|(field, _)| field.as_str()).collect();
        assert_eq!(named, ["topic", "group"]);
    }

    #[test]
    fn a_number_field_rejects_text() {
        let draft = filled(Family::Input, "dummy", &[("duration", "soon")]);
        let errors = component_json(&component(Family::Input, "dummy"), &draft)
            .expect_err("'soon' is not a duration");
        assert_eq!(errors.len(), 1);
        assert!(errors[0].1.contains("whole number"), "got: {}", errors[0].1);
    }

    /// The `type` tag would happily accept a typo'd `sum`; the schema says
    /// which values exist, so the form can too — inside a list row like
    /// anywhere else.
    #[test]
    fn a_closed_set_field_rejects_a_value_outside_it() {
        let draft = filled(
            Family::Transform,
            "reducer",
            &[
                ("aggregations", "1"),
                ("aggregations.0.function", "average"),
                ("aggregations.0.field", "value"),
                ("aggregations.0.as", "total"),
            ],
        );
        let errors = component_json(&component(Family::Transform, "reducer"), &draft)
            .expect_err("'average' is not one of sum/avg/min/max");
        assert_eq!(errors[0].0, "aggregations.0.function");
        assert!(errors[0].1.contains("avg"), "got: {}", errors[0].1);
    }

    /// A list is as many values as there are rows, and each row is read exactly
    /// as the same shape would be read anywhere else.
    #[test]
    fn a_list_field_is_built_from_the_rows_under_it() -> anyhow::Result<()> {
        let reducer = component(Family::Transform, "reducer");
        let draft = filled(
            Family::Transform,
            "reducer",
            &[
                ("aggregations", "2"),
                ("aggregations.0.function", "sum"),
                ("aggregations.0.field", "value"),
                ("aggregations.0.as", "total"),
                ("aggregations.1.function", "count"),
                ("aggregations.1.as", "n"),
                ("group_by", "1"),
                ("group_by.0", "sensor"),
                ("on_missing", "skip"),
            ],
        );
        let json = component_json(&reducer, &draft).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert_eq!(
            json,
            json!({
                "type": "reducer",
                "aggregations": [
                    {"function": "sum", "field": "value", "as": "total"},
                    {"function": "count", "as": "n"}
                ],
                "group_by": ["sensor"],
                "on_missing": "skip"
            })
        );

        // ...and it is the config the server takes, not just the shape of one
        let config: Config = serde_json::from_value(
            build_config("p", &[dummy_input(), draft], &docs())
                .map_err(|e| anyhow::anyhow!("{e:?}"))?,
        )?;
        assert_eq!(config.transforms.len(), 1);
        Ok(())
    }

    /// A column mapping is the deepest thing the form builds — a list of
    /// objects, one of whose fields is a list of its own (`indexes.0.columns`)
    /// — so it is the one that says the recursion goes all the way down rather
    /// than stopping at the first row.
    #[test]
    fn a_column_mapping_can_be_built_from_the_form() -> anyhow::Result<()> {
        let postgres = component(Family::Output, "postgres");
        let draft = filled(
            Family::Output,
            "postgres",
            &[
                ("connection", "local-postgres"),
                ("table", "readings"),
                ("columns", "2"),
                ("columns.0.name", "sensor"),
                ("columns.0.type", "text"),
                ("columns.0.nullable", "false"),
                ("columns.1.name", "recorded_at"),
                ("columns.1.type", "timestamp"),
                ("columns.1.field", "ts"),
                ("columns.1.on_missing", "skip_row"),
                ("create_table", "true"),
                ("primary_key", "1"),
                ("primary_key.0", "sensor"),
                ("indexes", "1"),
                ("indexes.0.columns", "1"),
                ("indexes.0.columns.0", "recorded_at"),
                ("indexes.0.unique", "true"),
            ],
        );
        let json = component_json(&postgres, &draft).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert_eq!(
            json,
            json!({
                "type": "postgres",
                "connection": "local-postgres",
                "table": "readings",
                "columns": [
                    {"name": "sensor", "type": "text", "nullable": false},
                    {"name": "recorded_at", "type": "timestamp", "field": "ts",
                     "on_missing": "skip_row"}
                ],
                "create_table": true,
                "primary_key": ["sensor"],
                "indexes": [{"columns": ["recorded_at"], "unique": true}]
            })
        );

        // ...and it is the config the server takes, not just the shape of one
        let config: Config = serde_json::from_value(
            build_config("p", &[dummy_input(), draft], &docs())
                .map_err(|e| anyhow::anyhow!("{e:?}"))?,
        )?;
        assert_eq!(config.outputs.len(), 1);
        Ok(())
    }

    /// A list with no rows is an absent field rather than `[]` — the same
    /// bargain the nested object makes — unless it is required, which is a
    /// message about the list itself.
    #[test]
    fn an_empty_list_is_omitted_or_reported_against_the_list() -> anyhow::Result<()> {
        let reducer = component(Family::Transform, "reducer");
        let draft = filled(
            Family::Transform,
            "reducer",
            &[
                ("aggregations", "1"),
                ("aggregations.0.function", "count"),
                ("aggregations.0.as", "n"),
            ],
        );
        let json = component_json(&reducer, &draft).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert!(json.get("group_by").is_none(), "sent a group_by: {json}");

        let bare = filled(Family::Transform, "reducer", &[]);
        let errors =
            component_json(&reducer, &bare).expect_err("a reducer with nothing to compute");
        assert_eq!(errors, [("aggregations".to_string(), "required".to_string())]);
        Ok(())
    }

    /// A row's position is its identity, so taking one out has to move the ones
    /// after it down — otherwise row 2's boxes are orphaned under a path
    /// nothing renders and the gap reads as an empty row.
    #[test]
    fn removing_a_row_shifts_the_ones_after_it_down() {
        let mut values = HashMap::new();
        for (key, value) in [
            ("aggregations", "3"),
            ("aggregations.0.as", "a"),
            ("aggregations.1.as", "b"),
            ("aggregations.1.function", "sum"),
            ("aggregations.2.as", "c"),
            // a field whose name merely starts the same way must not move
            ("aggregations_note", "untouched"),
        ] {
            values.insert(key.to_string(), value.to_string());
        }

        assert_eq!(remove_list_element(&mut values, "aggregations", 0), 2);
        assert_eq!(list_len(&values, "aggregations"), 2);
        assert_eq!(values.get("aggregations.0.as").map(String::as_str), Some("b"));
        assert_eq!(
            values.get("aggregations.0.function").map(String::as_str),
            Some("sum"),
            "everything nested in the row moves with it"
        );
        assert_eq!(values.get("aggregations.1.as").map(String::as_str), Some("c"));
        assert_eq!(values.get("aggregations.2.as"), None, "no orphaned row");
        assert_eq!(
            values.get("aggregations_note").map(String::as_str),
            Some("untouched")
        );
    }

    /// Ten rows and one row share a prefix; only one of them is row 1.
    #[test]
    fn removing_a_row_leaves_the_double_digit_ones_alone() {
        let mut values = HashMap::new();
        values.insert("xs".to_string(), "12".to_string());
        values.insert("xs.1".to_string(), "one".to_string());
        values.insert("xs.10".to_string(), "ten".to_string());
        remove_list_element(&mut values, "xs", 0);
        assert_eq!(values.get("xs.0").map(String::as_str), Some("one"));
        assert_eq!(values.get("xs.9").map(String::as_str), Some("ten"));
    }

    #[test]
    fn adding_a_row_stops_at_the_ceiling() {
        let mut values = HashMap::new();
        values.insert("xs".to_string(), MAX_LIST_ROWS.to_string());
        assert_eq!(push_list_element(&mut values, "xs"), MAX_LIST_ROWS);
    }

    /// The rows below a removed one are different rows now, so their messages
    /// are about boxes that have moved. Dropping them is what stops a "required"
    /// pointing at the wrong row.
    #[test]
    fn removing_a_row_drops_the_lists_messages() {
        let mut errors = vec![
            FormError::Field {
                component: 0,
                field: "aggregations.1.as".to_string(),
                message: "required".to_string(),
            },
            FormError::Field {
                component: 0,
                field: "group_by.0".to_string(),
                message: "required".to_string(),
            },
            FormError::Field {
                component: 1,
                field: "aggregations.0.as".to_string(),
                message: "required".to_string(),
            },
        ];
        clear_list_errors(&mut errors, 0, "aggregations");
        assert_eq!(field_error(&errors, 0, "aggregations.1.as"), None);
        assert_eq!(field_error(&errors, 0, "group_by.0"), Some("required"));
        assert_eq!(
            field_error(&errors, 1, "aggregations.0.as"),
            Some("required"),
            "another component's list is not this one"
        );
    }

    /// A closed set whose variants are doc-commented is spelled differently in
    /// the schema, and used to arrive as [`FieldType::Json`] — which meant a
    /// box wanting `"number"`, quotes and all, where a dropdown belonged.
    #[test]
    fn a_documented_closed_set_is_a_dropdown_rather_than_a_json_box() -> anyhow::Result<()> {
        let dummy = component(Family::Input, "dummy");
        let payload = fields_of(&dummy, None)
            .into_iter()
            .find(|f| f.name == "payload")
            .ok_or_else(|| anyhow::anyhow!("the dummy input has no 'payload' field"))?;
        assert_eq!(
            payload.field_type,
            FieldType::Enum(vec!["number".to_string(), "text".to_string()])
        );

        let draft = filled(
            Family::Input,
            "dummy",
            &[("duration", "5"), ("payload", "text")],
        );
        let json = component_json(&dummy, &draft).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert_eq!(json["payload"], json!("text"), "sent as the bare value");
        // and the config the server takes agrees that it is one
        let config: Config = serde_json::from_value(
            build_config("p", &[draft], &docs()).map_err(|e| anyhow::anyhow!("{e:?}"))?,
        )?;
        assert_eq!(config.inputs.len(), 1);

        let typo = filled(
            Family::Input,
            "dummy",
            &[("duration", "5"), ("payload", "sine")],
        );
        let errors = component_json(&dummy, &typo).expect_err("'sine' is not a payload kind");
        assert!(errors[0].1.contains("number"), "got: {}", errors[0].1);
        Ok(())
    }

    /// The conditional case: what a `buffer` is asked for depends on which kind
    /// of buffer was picked, and the answer is assembled from the boxes under
    /// its path rather than typed as JSON into one.
    #[test]
    fn a_union_field_is_built_from_the_variant_that_was_chosen() -> anyhow::Result<()> {
        let dummy = component(Family::Input, "dummy");
        let draft = filled(
            Family::Input,
            "dummy",
            &[
                ("duration", "5"),
                ("buffer.type", "tumbling"),
                ("buffer.window_seconds", "30"),
            ],
        );
        let json = component_json(&dummy, &draft).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert_eq!(
            json["buffer"],
            json!({"type": "tumbling", "window_seconds": 30})
        );

        // ...and it is the config the server takes, not just the shape of one
        let config: Config = serde_json::from_value(
            build_config("p", &[draft], &docs()).map_err(|e| anyhow::anyhow!("{e:?}"))?,
        )?;
        assert!(matches!(
            config.inputs.first().and_then(|i| i.buffer.as_ref()),
            Some(kayak_core::config::BufferConfig::Tumbling { window_seconds: 30 })
        ));
        Ok(())
    }

    /// Switching the choice must switch the answer. A `size` left behind from
    /// a look at the static buffer is not part of a tumbling one, and sending
    /// it would be rejected by the very serde the form is standing in for.
    #[test]
    fn the_other_variants_fields_are_left_out() -> anyhow::Result<()> {
        let draft = filled(
            Family::Input,
            "dummy",
            &[
                ("duration", "5"),
                ("buffer.type", "static"),
                ("buffer.size", "10"),
                ("buffer.window_seconds", "30"),
            ],
        );
        let json = component_json(&component(Family::Input, "dummy"), &draft)
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert_eq!(json["buffer"], json!({"type": "static", "size": 10}));
        Ok(())
    }

    /// Nothing chosen is an absent optional field, not an empty object: every
    /// input has a `buffer` and almost none of them wants one.
    #[test]
    fn an_unchosen_union_field_is_omitted() -> anyhow::Result<()> {
        let draft = filled(Family::Input, "dummy", &[("duration", "5")]);
        let json = component_json(&component(Family::Input, "dummy"), &draft)
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert!(json.get("buffer").is_none(), "sent a buffer: {json}");
        Ok(())
    }

    /// A message about a nested box has to land under that box, which means it
    /// is keyed by the same path the box is.
    #[test]
    fn a_message_about_a_nested_field_is_keyed_by_its_path() {
        let drafts = vec![filled(
            Family::Input,
            "dummy",
            &[
                ("duration", "5"),
                ("buffer.type", "static"),
                ("buffer.size", "lots"),
            ],
        )];
        let errors = build_config("p", &drafts, &docs()).expect_err("'lots' is not a size");
        assert!(
            field_error(&errors, 0, "buffer.size").is_some_and(|m| m.contains("whole number")),
            "got: {errors:?}"
        );
        // and not against the field it is nested in, which has no box of its own
        assert_eq!(field_error(&errors, 0, "buffer"), None);
    }

    /// A variant is picked from a list, so the only way to be outside it is a
    /// draft that was built by something other than the form.
    #[test]
    fn a_union_tagged_with_something_that_is_not_a_variant_is_rejected() {
        let draft = filled(
            Family::Input,
            "dummy",
            &[("duration", "5"), ("buffer.type", "sliding")],
        );
        let errors =
            component_json(&component(Family::Input, "dummy"), &draft).expect_err("no such buffer");
        assert_eq!(errors[0].0, "buffer.type");
        assert!(errors[0].1.contains("tumbling"), "got: {}", errors[0].1);
    }

    /// Nesting without a choice: a file output's rotation is two triggers, and
    /// either one alone is a rotation.
    #[test]
    fn a_nested_object_is_built_from_the_fields_under_it() -> anyhow::Result<()> {
        let file = component(Family::Output, "file");
        let draft = filled(
            Family::Output,
            "file",
            &[
                ("connection", "local-files"),
                ("path", "orders"),
                ("rotate.max_rows", "1000"),
            ],
        );
        let json = component_json(&file, &draft).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert_eq!(json["rotate"], json!({"max_rows": 1000}));
        let config: Config = serde_json::from_value(
            build_config("p", &[dummy_input(), draft], &docs())
                .map_err(|e| anyhow::anyhow!("{e:?}"))?,
        )?;
        assert_eq!(config.outputs.len(), 1);

        // an untouched one is left out rather than sent as `{}`, which would
        // mean "rotate on nothing" — a different thing to say than nothing
        let bare = filled(
            Family::Output,
            "file",
            &[("connection", "local-files"), ("path", "orders")],
        );
        let json = component_json(&file, &bare).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert!(json.get("rotate").is_none(), "sent a rotation: {json}");
        Ok(())
    }

    /// The `filter` transform's fields live on its variants, and the wire
    /// format hangs them off the variant name.
    #[test]
    fn an_enum_shaped_component_is_built_from_the_selected_variant() -> anyhow::Result<()> {
        let filter = component(Family::Transform, "filter");
        let mut draft = draft_of(&filter);
        assert_eq!(
            draft.variant.as_deref(),
            Some("Numeric"),
            "the first is preselected"
        );

        draft.variant = Some("String".to_string());
        for (name, value) in [
            ("field", "level"),
            ("operator", "contains"),
            ("value", "warn"),
        ] {
            draft.values.insert(name.to_string(), value.to_string());
        }
        let json = component_json(&filter, &draft).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert_eq!(
            json,
            json!({
                "type": "filter",
                "String": {"field": "level", "operator": "contains", "value": "warn"}
            })
        );
        Ok(())
    }

    /// Switching variants swaps the form: `Numeric`'s `value` is a number and
    /// `String`'s is text, so the fields can't be shared.
    #[test]
    fn the_rendered_fields_follow_the_selected_variant() {
        let filter = component(Family::Transform, "filter");
        let numeric = fields_of(&filter, Some("Numeric"));
        let string = fields_of(&filter, Some("String"));
        let value_type = |fields: &[FieldDoc]| {
            fields
                .iter()
                .find(|f| f.name == "value")
                .map(|f| f.field_type.clone())
        };
        assert_eq!(value_type(&numeric), Some(FieldType::Number));
        assert_eq!(value_type(&string), Some(FieldType::Text));
    }

    /// The end to end shape: this is the body of `POST /api/pipelines`, and it
    /// has to deserialize as the very `Config` the server takes.
    #[test]
    fn a_valid_form_builds_a_config_the_server_accepts() -> anyhow::Result<()> {
        let drafts = vec![
            dummy_input(),
            filled(Family::Transform, "buffer", &[("size", "10")]),
            filled(Family::Output, "stdout", &[]),
        ];
        let value =
            build_config("my-pipeline", &drafts, &docs()).map_err(|e| anyhow::anyhow!("{e:?}"))?;

        let config: Config = serde_json::from_value(value.clone())?;
        assert_eq!(config.id.as_deref(), Some("my-pipeline"));
        assert_eq!(config.inputs.len(), 1);
        assert_eq!(config.transforms.len(), 1);
        assert_eq!(config.outputs.len(), 1);
        Ok(())
    }

    /// Components land in the stage their family names, however they were
    /// added — the modal's three sections are the only ordering there is.
    #[test]
    fn components_are_grouped_into_their_stages() -> anyhow::Result<()> {
        let drafts = vec![
            filled(Family::Output, "stdout", &[]),
            dummy_input(),
            filled(
                Family::Output,
                "file",
                &[("connection", "local-files"), ("path", "orders")],
            ),
        ];
        let value = build_config("", &drafts, &docs()).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert_eq!(value["inputs"].as_array().map(Vec::len), Some(1));
        assert_eq!(value["outputs"].as_array().map(Vec::len), Some(2));
        assert_eq!(value["transforms"], json!([]));
        Ok(())
    }

    /// Not naming a pipeline is a supported choice, not an omission: the server
    /// gives it a petname.
    #[test]
    fn a_blank_id_is_sent_as_null_rather_than_rejected() -> anyhow::Result<()> {
        let value =
            build_config("   ", &[dummy_input()], &docs()).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert_eq!(value["id"], Value::Null);
        let config: Config = serde_json::from_value(value)?;
        assert!(config.id.is_none());
        Ok(())
    }

    /// The id goes in a URL path and in other pipelines' `upstream`.
    #[test]
    fn an_id_with_characters_that_do_not_belong_in_a_url_is_rejected() {
        let errors = build_config("my pipeline/2", &[dummy_input()], &docs())
            .expect_err("a space and a slash are not allowed in an id");
        assert_eq!(pipeline_errors(&errors).len(), 1);
    }

    #[test]
    fn a_pipeline_with_no_input_is_rejected_before_the_round_trip() {
        let errors = build_config("p", &[filled(Family::Output, "stdout", &[])], &docs())
            .expect_err("a pipeline with no input can never produce anything");
        assert_eq!(
            pipeline_errors(&errors),
            ["a pipeline needs at least one input"]
        );
    }

    /// Zero outputs is legal: such a pipeline exists to feed the ones
    /// downstream of it.
    #[test]
    fn a_pipeline_with_no_output_is_accepted() -> anyhow::Result<()> {
        let value = build_config("feeder", &[dummy_input()], &docs())
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert_eq!(value["outputs"], json!([]));
        Ok(())
    }

    /// Two of the same kind is normal — a pipeline merging two nats subjects —
    /// so an error has to say *which* one is wrong.
    #[test]
    fn errors_name_the_component_they_belong_to() {
        let drafts = vec![
            filled(
                Family::Input,
                "nats",
                &[("connection", "a"), ("subject", "one")],
            ),
            filled(Family::Input, "nats", &[("connection", "b")]),
        ];
        let errors = build_config("p", &drafts, &docs()).expect_err("the second has no subject");
        assert_eq!(field_error(&errors, 1, "subject"), Some("required"));
        assert_eq!(field_error(&errors, 0, "subject"), None);
    }

    /// Only the field that was edited, and only on the component it belongs to.
    #[test]
    fn editing_a_field_clears_its_own_message_and_no_other() {
        let drafts = vec![
            filled(Family::Input, "nats", &[]),
            filled(Family::Input, "nats", &[]),
        ];
        let mut errors = build_config("p", &drafts, &docs()).expect_err("nothing is filled in");
        clear_field_error(&mut errors, 0, "connection");

        assert_eq!(field_error(&errors, 0, "connection"), None);
        assert_eq!(field_error(&errors, 0, "subject"), Some("required"));
        assert_eq!(field_error(&errors, 1, "connection"), Some("required"));
    }

    #[test]
    fn a_form_reports_every_problem_at_once() {
        let drafts = vec![
            filled(Family::Input, "dummy", &[("duration", "soon")]),
            filled(Family::Output, "nats", &[]),
        ];
        let errors = build_config("bad id", &drafts, &docs()).expect_err("three things are wrong");
        assert!(field_error(&errors, 0, "duration").is_some());
        assert!(field_error(&errors, 1, "connection").is_some());
        assert!(field_error(&errors, 1, "subject").is_some());
        assert_eq!(pipeline_errors(&errors).len(), 1, "the id");
    }

    /// An optional object left alone must not demand the fields it would need
    /// if it were there. The `opcua` input's `browse` is the shape: the whole
    /// object is optional, but a browse without a `root` is not a browse — so
    /// the requirement is real and must only apply once someone has started
    /// filling it in.
    #[test]
    fn an_untouched_optional_object_does_not_ask_for_its_required_fields() {
        let doc = component(Family::Input, "opcua");
        let mut draft = filled(
            Family::Input,
            "opcua",
            &[("connection", "plant"), ("nodes", "1"), ("nodes.0.node_id", "ns=2;i=1")],
        );
        let json = match component_json(&doc, &draft) {
            Ok(json) => json,
            Err(errors) => panic!("a browse nobody asked for should not be required: {errors:?}"),
        };
        assert!(json.get("browse").is_none(), "{json}");

        // ...and the moment one field of it is filled in, the rest of that
        // object is asked for as usual
        draft.values.insert("browse.depth".to_string(), "2".to_string());
        let Err(errors) = component_json(&doc, &draft) else {
            panic!("a half-filled browse should report its missing root");
        };
        assert!(
            errors.iter().any(|(at, _)| at == "browse.root"),
            "{errors:?}"
        );
    }

    /// The counterpart of the test above, and the reason `touched` asks the
    /// *draft* rather than the built body: an unreadable number produces no
    /// value **and** an error, so an optional object holding only that has an
    /// empty body. Deciding on the body would drop the object and take the
    /// error with it — the form would then refuse to submit with nothing
    /// marked to say why, which is the worst of both answers.
    #[test]
    fn an_optional_object_holding_only_an_unreadable_value_still_reports_it() {
        let doc = component(Family::Input, "opcua");
        let mut draft = filled(
            Family::Input,
            "opcua",
            &[("connection", "plant"), ("nodes", "1"), ("nodes.0.node_id", "ns=2;i=1")],
        );
        // the only thing in `browse` is a depth that is not a number
        draft.values.insert("browse.depth".to_string(), "deep".to_string());

        let Err(errors) = component_json(&doc, &draft) else {
            panic!("a browse whose only field is unreadable must not be dropped silently");
        };
        assert!(
            errors.iter().any(|(at, _)| at == "browse.depth"),
            "the unreadable box has to be the one marked: {errors:?}"
        );
    }

    /// Every component the server can build has to be reachable from the
    /// modal — a form that can't express a component quietly makes it
    /// unusable from the UI.
    #[test]
    fn every_documented_component_can_be_drafted_and_built() {
        for doc in docs() {
            let mut draft = draft_of(&doc);
            let fields = fields_of(&doc, draft.variant.as_deref());
            fill_required(&mut draft.values, &fields, "");
            assert!(
                component_json(&doc, &draft).is_ok(),
                "'{}' could not be built from a filled-in form",
                doc.kind
            );
        }
    }

    /// A plausible answer in every required box, nested ones included — the
    /// same walk the form renders, which is what makes it a check that every
    /// component can be filled in at all.
    fn fill_required(values: &mut HashMap<String, String>, fields: &[FieldDoc], prefix: &str) {
        for field in fields {
            if !field.required {
                continue;
            }
            let at = path(prefix, &field.name);
            let sample = match &field.field_type {
                FieldType::Text
                | FieldType::PipelineId
                | FieldType::Connection(_)
                | FieldType::MessageField => "x".to_string(),
                // a script that parses and emits its message — the smallest
                // thing that is actually a working transform
                FieldType::Script(_) => "emit(msg);".to_string(),
                FieldType::Integer => "1".to_string(),
                FieldType::Number => "1.5".to_string(),
                FieldType::Boolean => "true".to_string(),
                FieldType::Enum(values) => values.first().cloned().unwrap_or_default(),
                FieldType::Json => "{}".to_string(),
                FieldType::Object(fields) => {
                    fill_required(values, fields, &at);
                    continue;
                }
                FieldType::Union(union) => {
                    if let Some(variant) = union.variants.first() {
                        values.insert(path(&at, &union.tag), variant.name.clone());
                        fill_required(values, &variant.fields, &at);
                    }
                    continue;
                }
                // one row, filled in as the form would fill it: a required list
                // with none is a list nobody has added anything to yet
                FieldType::List(element) => {
                    let mut row = (**element).clone();
                    row.name = "0".to_string();
                    values.insert(at.clone(), "1".to_string());
                    fill_required(values, std::slice::from_ref(&row), &at);
                    continue;
                }
            };
            values.insert(at, sample);
        }
    }
}
