use askama::Template;
use axum::response::{Html, IntoResponse};
use schemars::schema_for;

use crate::config::{InputKind, OutputKind, TransformKind};

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

fn value_to_component_doc_vec(value: serde_json::Value) -> anyhow::Result<Vec<ComponentDoc>> {
    Ok(value["$defs"]
        .as_object()
        .unwrap()
        .iter()
        .map(|item| ComponentDoc {
            title: item.1["title"].as_str().unwrap_or("failed").to_string(),
            description: None,
            component_type: "input".to_string(),
            fields: vec![],
        })
        .collect())
}

pub async fn get_docs() -> impl IntoResponse {
    let inputs = serde_json::to_value(schema_for!(InputKind)).unwrap();
    let transforms = serde_json::to_value(schema_for!(TransformKind)).unwrap();
    let outputs = serde_json::to_value(schema_for!(OutputKind)).unwrap();
    let inputs = value_to_component_doc_vec(inputs).unwrap();
    let transforms = value_to_component_doc_vec(transforms).unwrap();
    let outputs = value_to_component_doc_vec(outputs).unwrap();
    let template = Tmpl {
        inputs,
        transforms,
        outputs,
    };
    Html(template.render().unwrap())
}
