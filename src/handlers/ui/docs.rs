use anyhow::Context;
use askama::Template;
use axum::response::{Html, IntoResponse};
use schemars::schema_for;

use crate::{
    config::{InputKind, OutputKind, TransformKind},
    handlers::error::AppError,
};

#[allow(dead_code)]
struct FieldDoc {
    name: String,
    description: Option<String>,
    type_hint: Option<String>,
}

#[allow(dead_code)]
struct ComponentDoc {
    title: String,
    component_type: String,
    description: Option<String>,
    fields: Vec<FieldDoc>,
}
#[derive(Template)]
#[template(path = "docs.html")]
struct Tmpl {
    inputs: Vec<ComponentDoc>,
    transforms: Vec<ComponentDoc>,
    outputs: Vec<ComponentDoc>,
}

fn value_to_component_doc_vec(value: &serde_json::Value) -> anyhow::Result<Vec<ComponentDoc>> {
    Ok(value["$defs"]
        .as_object()
        .context("schema missing $defs object")?
        .iter()
        .map(|item| ComponentDoc {
            title: item.1["title"].as_str().unwrap_or("failed").to_string(),
            description: None,
            component_type: "input".to_string(),
            fields: vec![],
        })
        .collect())
}

pub async fn get_docs() -> Result<impl IntoResponse, AppError> {
    let inputs = serde_json::to_value(schema_for!(InputKind))?;
    let transforms = serde_json::to_value(schema_for!(TransformKind))?;
    let outputs = serde_json::to_value(schema_for!(OutputKind))?;
    let inputs = value_to_component_doc_vec(&inputs)?;
    let transforms = value_to_component_doc_vec(&transforms)?;
    let outputs = value_to_component_doc_vec(&outputs)?;
    let template = Tmpl {
        inputs,
        transforms,
        outputs,
    };
    Ok(Html(template.render()?))
}
