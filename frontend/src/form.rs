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
use serde_json::{Map, Value};

/// One component being filled in.
///
/// The values are raw strings — what an `<input>` holds — and stay that way
/// until the form is submitted. Parsing as you type would fight the user: a
/// half-typed `1.` is not a number yet, and neither is an empty box someone is
/// about to fill in.
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

/// Turn one field's raw text into the JSON value it stands for.
///
/// `Ok(None)` means "leave it out" — an empty optional field, which is not the
/// same as an empty string: writing `"subject": ""` where the field was meant
/// to be absent is a different config.
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
        FieldType::PipelineId | FieldType::Connection(_) => Value::String(trimmed.to_string()),
        FieldType::Enum(values) => {
            if values.iter().any(|v| v == trimmed) {
                Value::String(trimmed.to_string())
            } else {
                return Err(format!("must be one of: {}", values.join(", ")));
            }
        }
        FieldType::Json => {
            serde_json::from_str(trimmed).map_err(|e| format!("not valid JSON: {e}"))?
        }
    };
    Ok(Some(value))
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
    let mut body = Map::new();
    let mut errors = Vec::new();
    for field in fields_of(doc, draft.variant.as_deref()) {
        match parse_field(&field, draft.value(&field.name)) {
            Ok(Some(value)) => {
                body.insert(field.name.clone(), value);
            }
            Ok(None) => {}
            Err(message) => errors.push((field.name.clone(), message)),
        }
    }
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
    /// which values exist, so the form can too.
    #[test]
    fn a_closed_set_field_rejects_a_value_outside_it() {
        let draft = filled(
            Family::Transform,
            "reducer",
            &[("function", "average"), ("field", "value")],
        );
        let errors = component_json(&component(Family::Transform, "reducer"), &draft)
            .expect_err("'average' is not one of sum/avg/min/max");
        assert!(errors[0].1.contains("avg"), "got: {}", errors[0].1);
    }

    /// `buffer` has a shape of its own, so the form takes it as literal JSON —
    /// and has to say so when it isn't.
    #[test]
    fn a_json_field_is_parsed_rather_than_sent_as_a_string() -> anyhow::Result<()> {
        let draft = filled(
            Family::Input,
            "dummy",
            &[
                ("duration", "5"),
                ("buffer", r#"{"type": "static", "size": 10}"#),
            ],
        );
        let json = component_json(&component(Family::Input, "dummy"), &draft)
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert_eq!(json["buffer"], json!({"type": "static", "size": 10}));

        let broken = filled(
            Family::Input,
            "dummy",
            &[("duration", "5"), ("buffer", "{")],
        );
        let errors = component_json(&component(Family::Input, "dummy"), &broken)
            .expect_err("'{' is not JSON");
        assert!(errors[0].1.contains("JSON"), "got: {}", errors[0].1);
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
            ("operator", "Contains"),
            ("value", "warn"),
        ] {
            draft.values.insert(name.to_string(), value.to_string());
        }
        let json = component_json(&filter, &draft).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        assert_eq!(
            json,
            json!({
                "type": "filter",
                "String": {"field": "level", "operator": "Contains", "value": "warn"}
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
            filled(Family::Output, "file", &[]),
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

    /// Every component the server can build has to be reachable from the
    /// modal — a form that can't express a component quietly makes it
    /// unusable from the UI.
    #[test]
    fn every_documented_component_can_be_drafted_and_built() {
        for doc in docs() {
            let mut draft = draft_of(&doc);
            for field in fields_of(&doc, draft.variant.as_deref()) {
                if !field.required {
                    continue;
                }
                let sample = match field.field_type {
                    FieldType::Text | FieldType::PipelineId | FieldType::Connection(_) => {
                        "x".to_string()
                    }
                    FieldType::Integer => "1".to_string(),
                    FieldType::Number => "1.5".to_string(),
                    FieldType::Boolean => "true".to_string(),
                    FieldType::Enum(ref values) => values.first().cloned().unwrap_or_default(),
                    FieldType::Json => "{}".to_string(),
                };
                draft.values.insert(field.name.clone(), sample);
            }
            assert!(
                component_json(&doc, &draft).is_ok(),
                "'{}' could not be built from a filled-in form",
                doc.kind
            );
        }
    }
}
