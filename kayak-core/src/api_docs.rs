//! The HTTP surface, described as data.
//!
//! This is the counterpart of [`crate::docs`] one level up: that module answers
//! "what components can a pipeline be built from", this one answers "what
//! requests can be made of the server". Both live in `kayak-core` for the same
//! reason — the frontend renders them and has no async or network dependencies
//! to spare — and both have several consumers off one description.
//!
//! Unlike the component reference, this table is *written* rather than
//! reflected: a Rust doc comment on an axum handler is not readable at runtime,
//! so there is nothing to reflect over. What keeps it honest instead is that
//! `api_router()` is **built from this table** — an endpoint that isn't here
//! doesn't get registered, and an entry with no handler doesn't compile. The
//! prose therefore lives here and the handlers carry a one-line `///` pointing
//! at it, rather than the other way round.
//!
//! Bodies are named rather than inlined ([`Body::Json`] carries a schema name),
//! and [`schemas`] maps those names to the generated JSON Schemas. That keeps
//! this table small enough to read in one screen, and lets each consumer
//! resolve a body the way it wants to: `openapi.rs` hoists them into
//! `components/schemas`, and the `/docs` page just prints the name.

use std::collections::BTreeMap;

use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::Config;
use crate::connections::{Connections, CreateConnectionRequest};
use crate::docs::ComponentDoc;
use crate::state::{BucketContents, BucketSummary};
use crate::layout::LayoutFile;
use crate::{
    IngestRequest, IngestResponse, PipelineDto, SaveConfigRequest, SaveConfigResponse, SettingsDto,
    UiEvent,
};

/// The error body every failing request comes back with.
///
/// A Rust type rather than a hand-written schema because it has to stay in step
/// with what `AppError` actually serializes — `an_error_body_matches_the_documented_shape`
/// in `tests/api.rs` is what says so.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ApiError {
    /// What went wrong, as one line. `anyhow`'s context chain is rendered into
    /// it, so the cause is in there as "context: cause" rather than nested.
    pub error: String,
}

/// The HTTP methods this API uses. Not the full set — a method kayak does not
/// serve has no business being spellable here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Method {
    Get,
    Post,
    Put,
    Delete,
}

impl Method {
    /// How the method is written in a request line, and on a badge in the UI.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Delete => "DELETE",
        }
    }

    /// The lowercase spelling OpenAPI keys an operation by.
    #[must_use]
    pub fn key(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Post => "post",
            Self::Put => "put",
            Self::Delete => "delete",
        }
    }
}

/// Every operation the API serves, as a closed set.
///
/// An enum rather than a string because this is what makes the router provably
/// complete: `endpoints::handler_for` matches on it, so the compiler is what
/// says a new entry has no handler yet. It also means a generated client's
/// method names are a set someone has to edit deliberately.
///
/// The spelled-out ids are wire format — they are what a generated client calls
/// its methods, so renaming one breaks anybody who generated one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Operation {
    ListPipelines,
    CreatePipeline,
    DeletePipeline,
    IngestMessages,
    ListConnections,
    ListStateBuckets,
    GetStateBucket,
    CreateConnection,
    DeleteConnection,
    GetSettings,
    SaveConfig,
    RevertConfig,
    GetLayout,
    ReplaceLayout,
    StreamEvents,
    ListComponents,
    GetOpenApi,
    ApiReference,
}

impl Operation {
    /// The `operationId` in the spec, and the method name in a generated client.
    #[must_use]
    pub fn id(self) -> &'static str {
        match self {
            Self::ListPipelines => "listPipelines",
            Self::CreatePipeline => "createPipeline",
            Self::DeletePipeline => "deletePipeline",
            Self::IngestMessages => "ingestMessages",
            Self::ListConnections => "listConnections",
            Self::ListStateBuckets => "listStateBuckets",
            Self::GetStateBucket => "getStateBucket",
            Self::CreateConnection => "createConnection",
            Self::DeleteConnection => "deleteConnection",
            Self::GetSettings => "getSettings",
            Self::SaveConfig => "saveConfig",
            Self::RevertConfig => "revertConfig",
            Self::GetLayout => "getLayout",
            Self::ReplaceLayout => "replaceLayout",
            Self::StreamEvents => "streamEvents",
            Self::ListComponents => "listComponents",
            Self::GetOpenApi => "getOpenApi",
            Self::ApiReference => "apiReference",
        }
    }
}

/// How the endpoints are grouped, in both the generated reference and the UI.
///
/// The order is the order the page lists them in: the graph first, then what it
/// is built out of, then the file it is saved to, then the two endpoints that
/// are about the API rather than about kayak.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Tag {
    Pipelines,
    Connections,
    State,
    Config,
    Layout,
    Events,
    Reference,
}

/// Every tag, in the order pages list them.
pub const TAGS: [Tag; 7] = [
    Tag::Pipelines,
    Tag::Connections,
    Tag::State,
    Tag::Config,
    Tag::Layout,
    Tag::Events,
    Tag::Reference,
];

impl Tag {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Pipelines => "pipelines",
            Self::Connections => "connections",
            Self::State => "state",
            Self::Config => "config",
            Self::Layout => "layout",
            Self::Events => "events",
            Self::Reference => "reference",
        }
    }

    /// What the group is for, shown as a heading's subtitle.
    #[must_use]
    pub fn description(self) -> &'static str {
        match self {
            Self::Pipelines => {
                "The running graph. Creating and deleting pipelines takes effect \
                 immediately and writes nothing to disk."
            }
            Self::Connections => {
                "The systems pipelines talk to, named once and referred to by the \
                 components that use them."
            }
            Self::State => {
                "What the pipelines remember between batches. Read-only: buckets are \
                 declared in the config and filled by `remember` transforms, so there \
                 is nothing here to write."
            }
            Self::Config => {
                "The config file: how the server was started, writing the running \
                 graph out to it, and throwing the graph away to start again from it."
            }
            Self::Layout => {
                "Where the cards sit on the canvas. Not configuration — this is \
                 written to its own file, and immediately."
            }
            Self::Events => "What the pipelines are doing, as it happens.",
            Self::Reference => "The API describing itself.",
        }
    }
}

/// What a request or response carries.
///
/// [`Body::Json`] and [`Body::JsonArray`] name a schema from [`schemas`] rather
/// than inlining it; the rest are shapes no schema describes usefully.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Body {
    /// No body at all — a 204, or a request that carries nothing.
    None,
    /// A JSON object of the named schema.
    Json(&'static str),
    /// A JSON array of the named schema.
    JsonArray(&'static str),
    /// `text/event-stream`, whose *events* are the named schema. OpenAPI has no
    /// way to say that, so the name is for the prose and for the UI; the spec
    /// gets a string body and a description that says what is in it.
    EventStream(&'static str),
    /// An HTML page.
    Html,
}

impl Body {
    /// The schema this body names, if it names one.
    #[must_use]
    pub fn schema_name(self) -> Option<&'static str> {
        match self {
            Self::Json(name) | Self::JsonArray(name) | Self::EventStream(name) => Some(name),
            Self::None | Self::Html => None,
        }
    }

    /// How the body reads in a table: `Config`, `[PipelineDto]`, `—`.
    #[must_use]
    pub fn type_name(self) -> String {
        match self {
            Self::None => "—".to_string(),
            Self::Json(name) => name.to_string(),
            Self::JsonArray(name) => format!("[{name}]"),
            Self::EventStream(name) => format!("event-stream of {name}"),
            Self::Html => "html".to_string(),
        }
    }

    /// The `Content-Type` of a body of this shape, where it has one.
    #[must_use]
    pub fn content_type(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Json(_) | Self::JsonArray(_) => Some("application/json"),
            Self::EventStream(_) => Some("text/event-stream"),
            Self::Html => Some("text/html"),
        }
    }
}

/// A `{placeholder}` in the path. Always required — a path parameter that could
/// be left out would be a different path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParamDoc {
    pub name: &'static str,
    pub description: &'static str,
}

/// What a request has to carry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestDoc {
    pub body: Body,
    pub description: &'static str,
}

/// One documented outcome. Every endpoint documents its failures as well as its
/// success — which statuses a client has to handle is the part of an API that
/// is least guessable from the happy path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseDoc {
    pub status: u16,
    pub description: &'static str,
    pub body: Body,
}

/// One endpoint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiDoc {
    /// The route as axum spells it, placeholders and all:
    /// `/api/pipelines/{pipeline_id}`. OpenAPI uses the same braces, so it
    /// travels unchanged — and it is the *same string* `api_router` registers,
    /// because the router is built from this table.
    pub path: &'static str,
    pub method: Method,
    /// Which operation this is. The router's handler match is over this, so a
    /// new variant doesn't compile until it has a handler.
    pub operation: Operation,
    /// One line, shown in the endpoint list.
    pub summary: &'static str,
    /// The full explanation, in the same doc-comment style as the component
    /// reference — blank lines separate paragraphs, `backticks` are code.
    pub description: &'static str,
    pub tag: Tag,
    pub params: Vec<ParamDoc>,
    pub request: Option<RequestDoc>,
    pub responses: Vec<ResponseDoc>,
}

impl ApiDoc {
    /// The `operationId` a generated client names its method after.
    #[must_use]
    pub fn operation_id(&self) -> &'static str {
        self.operation.id()
    }

    /// A stable per-endpoint id, for the sidebar's scroll-to and for linking
    /// someone straight at an endpoint. The method has to be part of it: `GET`
    /// and `POST /api/pipelines` are two entries on one path.
    #[must_use]
    pub fn anchor_id(&self) -> String {
        let path = self
            .path
            .trim_start_matches('/')
            .replace(['/', '{', '}'], "-");
        format!("{}-{path}", self.method.key())
    }

    /// Whether this endpoint matches a search box query.
    ///
    /// The path and the description are searched as well as the summary, so
    /// "how do I revert" finds the endpoint and so does "409".
    #[must_use]
    pub fn matches(&self, query: &str) -> bool {
        let query = query.trim().to_lowercase();
        if query.is_empty() {
            return true;
        }
        let contains = |text: &str| text.to_lowercase().contains(&query);
        contains(self.path)
            || contains(self.method.label())
            || contains(self.operation_id())
            || contains(self.summary)
            || contains(self.description)
            || contains(self.tag.label())
            || self.params.iter().any(|p| contains(p.name))
            || self
                .responses
                .iter()
                .any(|r| contains(&r.status.to_string()) || contains(r.description))
            || self
                .request
                .is_some_and(|r| contains(&r.body.type_name()) || contains(r.description))
    }

    /// Every schema this endpoint names, request and responses alike.
    #[must_use]
    pub fn schema_names(&self) -> Vec<&'static str> {
        self.request
            .and_then(|r| r.body.schema_name())
            .into_iter()
            .chain(self.responses.iter().filter_map(|r| r.body.schema_name()))
            .collect()
    }
}

/// A 500, which every endpoint that can fail at all can produce. Spelled once
/// rather than repeated in fifteen tables.
fn server_error() -> ResponseDoc {
    ResponseDoc {
        status: 500,
        description: "Something went wrong on the server. The body says what.",
        body: Body::Json("ApiError"),
    }
}

fn not_found(what: &'static str) -> ResponseDoc {
    ResponseDoc {
        status: 404,
        description: what,
        body: Body::Json("ApiError"),
    }
}

/// Every endpoint the server serves, in tag order.
///
/// This is the list `api_router` is folded over, so it is not a description of
/// the routes — it *is* the routes. Adding one here without a handler doesn't
/// compile; adding a handler without one here leaves it unroutable.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn endpoints() -> Vec<ApiDoc> {
    vec![
        ApiDoc {
            path: "/api/pipelines",
            method: Method::Get,
            operation: Operation::ListPipelines,
            summary: "Every pipeline the server is running",
            description: "The running graph, as the configs the pipelines were built \
                          from — each with the id it is actually running under, which \
                          is generated when the config omitted one.\n\n\
                          This is the runtime's view rather than the file's: a pipeline \
                          created since startup is here and not in the config file, and \
                          `GET /api/settings` is what says whether the two have diverged.",
            tag: Tag::Pipelines,
            params: vec![],
            request: None,
            responses: vec![
                ResponseDoc {
                    status: 200,
                    description: "The running pipelines, in no particular order.",
                    body: Body::JsonArray("PipelineDto"),
                },
                server_error(),
            ],
        },
        ApiDoc {
            path: "/api/pipelines",
            method: Method::Post,
            operation: Operation::CreatePipeline,
            summary: "Build and start a pipeline",
            description: "The body is one pipeline's config, exactly as it would appear \
                          in the `pipelines` array of a config file. Omitting `id` \
                          generates a readable random one, which comes back in the \
                          response.\n\n\
                          The pipeline is built and started before the response is sent, \
                          so a 201 means it is running — a component that could not be \
                          built (an unknown connection, an unresolved secret) is a 422 \
                          and nothing is started. Nothing is written to disk: the config \
                          file is a load source and a save target, never a mirror of the \
                          runtime.",
            tag: Tag::Pipelines,
            params: vec![],
            request: Some(RequestDoc {
                body: Body::Json("Config"),
                description: "The pipeline to build.",
            }),
            responses: vec![
                ResponseDoc {
                    status: 201,
                    description: "Built and running, with the id it took.",
                    body: Body::Json("PipelineDto"),
                },
                ResponseDoc {
                    status: 409,
                    description: "A pipeline with this id is already running.",
                    body: Body::Json("ApiError"),
                },
                ResponseDoc {
                    status: 422,
                    description: "The config is well-formed JSON but could not be built \
                                  — an unknown connection, a missing secret, an upstream \
                                  that does not exist.",
                    body: Body::Json("ApiError"),
                },
                server_error(),
            ],
        },
        ApiDoc {
            path: "/api/pipelines/{pipeline_id}",
            method: Method::Delete,
            operation: Operation::DeletePipeline,
            summary: "Stop and remove a pipeline",
            description: "Cancels the pipeline's run loop and drops it from the graph. \
                          Pipelines downstream of it keep running and stop receiving \
                          from it.\n\n\
                          Like creating one, this writes nothing to disk.",
            tag: Tag::Pipelines,
            params: vec![ParamDoc {
                name: "pipeline_id",
                description: "The id the pipeline is running under.",
            }],
            request: None,
            responses: vec![
                ResponseDoc {
                    status: 204,
                    description: "Stopped and removed.",
                    body: Body::None,
                },
                not_found("No pipeline is running under that id."),
                server_error(),
            ],
        },
        ApiDoc {
            path: "/api/pipelines/{pipeline_id}/messages",
            method: Method::Post,
            operation: Operation::IngestMessages,
            summary: "Post messages into a pipeline",
            description: "The endpoint a pipeline's `http` input serves. Every pipeline \
                          with one has this path, derived from its id, and it exists for \
                          as long as the pipeline is running — this is how a system \
                          pushes data into kayak without a broker in between.\n\n\
                          The body is one JSON message or an array of them; an array \
                          arrives as a single batch, so posting ten messages is one pass \
                          through the transforms rather than ten. There is no envelope \
                          and no schema: whatever is posted is what the transforms see.\n\n\
                          Accepted means queued, not processed. The batch is handed to \
                          the pipeline's run loop and the response is sent without \
                          waiting for the outputs, so a 202 says nothing about whether \
                          the data has landed anywhere.",
            tag: Tag::Pipelines,
            params: vec![ParamDoc {
                name: "pipeline_id",
                description: "The id of the pipeline to post to.",
            }],
            request: Some(RequestDoc {
                body: Body::Json("IngestRequest"),
                description: "One message, or an array of messages to deliver as one batch.",
            }),
            responses: vec![
                ResponseDoc {
                    status: 202,
                    description: "Queued for the pipeline, with the number of messages taken.",
                    body: Body::Json("IngestResponse"),
                },
                not_found(
                    "No pipeline is running under that id, or the one that is has no \
                     `http` input to post to.",
                ),
                ResponseDoc {
                    status: 503,
                    description: "The pipeline's queue is full — it is not reading as fast \
                                  as this is being posted. Nothing was taken; send it again.",
                    body: Body::Json("ApiError"),
                },
                server_error(),
            ],
        },
        ApiDoc {
            path: "/api/connections",
            method: Method::Get,
            operation: Operation::ListConnections,
            summary: "The connections pipelines can name",
            description: "Keyed by name, which is the same shape as the connections \
                          file itself — what the UI lists and what gets committed are \
                          one thing, so there is no second format to keep in step.\n\n\
                          Credentials come back as the unresolved `${NAME}` templates \
                          they are configured as, never as their values.",
            tag: Tag::Connections,
            params: vec![],
            request: None,
            responses: vec![ResponseDoc {
                status: 200,
                description: "Every configured connection, in name order.",
                body: Body::Json("Connections"),
            }],
        },
        ApiDoc {
            path: "/api/state",
            method: Method::Get,
            operation: Operation::ListStateBuckets,
            summary: "The state buckets and how full they are",
            description: "One entry per bucket declared under `state` in the config, in \
                          name order, with the number of keys it is currently holding \
                          and the bounds it is held to.\n\n\
                          Buckets are not created or deleted through the API — they are \
                          part of the graph's logic and live in the config file, so \
                          this family is read-only.",
            tag: Tag::State,
            params: vec![],
            request: None,
            responses: vec![ResponseDoc {
                status: 200,
                description: "Every declared bucket, in name order.",
                body: Body::Json("BucketSummary"),
            }],
        },
        ApiDoc {
            path: "/api/state/{bucket}",
            method: Method::Get,
            operation: Operation::GetStateBucket,
            summary: "What one bucket is holding",
            description: "The keys and the values remembered under each, most recently \
                          written first — which is the order that makes a live bucket \
                          readable, since the key that just changed is the one worth \
                          seeing.\n\n\
                          Capped: a bucket may hold thousands of keys and this returns \
                          a page of them, with `truncated` saying so and `keys` giving \
                          the real total. It is a snapshot taken under the bucket's \
                          lock, so it is consistent with itself and stale the moment it \
                          is sent.",
            tag: Tag::State,
            params: vec![ParamDoc {
                name: "bucket",
                description: "Name of the bucket, as declared in the config.",
            }],
            request: None,
            responses: vec![
                ResponseDoc {
                    status: 200,
                    description: "The bucket's contents.",
                    body: Body::Json("BucketContents"),
                },
                ResponseDoc {
                    status: 404,
                    description: "No bucket of that name is declared.",
                    body: Body::Json("ApiError"),
                },
            ],
        },
        ApiDoc {
            path: "/api/connections",
            method: Method::Post,
            operation: Operation::CreateConnection,
            summary: "Add a connection",
            description: "Changes what the *next* pipeline build can name, and nothing \
                          else: a component reads its connection once, when it is built, \
                          so editing or adding one reaches only new and rebuilt \
                          pipelines.\n\n\
                          Like creating a pipeline this writes nothing to disk; the save \
                          does, and it writes the config and the connections file \
                          together.",
            tag: Tag::Connections,
            params: vec![],
            request: Some(RequestDoc {
                body: Body::Json("CreateConnectionRequest"),
                description: "The connection, and the name to file it under.",
            }),
            responses: vec![
                ResponseDoc {
                    status: 201,
                    description: "Added, echoed back as stored.",
                    body: Body::Json("CreateConnectionRequest"),
                },
                ResponseDoc {
                    status: 409,
                    description: "A connection of that name already exists.",
                    body: Body::Json("ApiError"),
                },
                server_error(),
            ],
        },
        ApiDoc {
            path: "/api/connections/{connection_id}",
            method: Method::Delete,
            operation: Operation::DeleteConnection,
            summary: "Remove a connection",
            description: "Refused while a running pipeline still names it — that comes \
                          back as a 409 listing the pipelines, so the answer says what \
                          to do about it.",
            tag: Tag::Connections,
            params: vec![ParamDoc {
                name: "connection_id",
                description: "The name the connection is filed under.",
            }],
            request: None,
            responses: vec![
                ResponseDoc {
                    status: 204,
                    description: "Removed.",
                    body: Body::None,
                },
                not_found("No connection of that name exists."),
                ResponseDoc {
                    status: 409,
                    description: "Running pipelines still name it; the body lists them.",
                    body: Body::Json("ApiError"),
                },
                server_error(),
            ],
        },
        ApiDoc {
            path: "/api/settings",
            method: Method::Get,
            operation: Operation::GetSettings,
            summary: "How the server was started, and whether it has drifted",
            description: "Which config file the server is working against, where a save \
                          would land, and whether the running graph has diverged from \
                          what was last loaded or saved.\n\n\
                          The absence of a config file doesn't mean edits can't be \
                          saved: it means there is no file *yet*, and a save creates one.",
            tag: Tag::Config,
            params: vec![],
            request: None,
            responses: vec![ResponseDoc {
                status: 200,
                description: "The server's configuration state.",
                body: Body::Json("SettingsDto"),
            }],
        },
        ApiDoc {
            path: "/api/config/save",
            method: Method::Post,
            operation: Operation::SaveConfig,
            summary: "Write the running graph to a config file",
            description: "Writes the running pipelines out in a deterministic order \
                          (topological, ties by id) via a temp file and a rename, \
                          because the result is meant to be committed. The connections \
                          file and the canvas layout are written beside it by the same \
                          save — a config saved without the connections it names would \
                          not start.\n\n\
                          `name` is a **bare file name**, not a path, and is validated as \
                          one: the file lands in the server's save directory and \
                          nowhere else. Using the loaded file's own name is how you \
                          overwrite it.\n\n\
                          `format` picks JSON or YAML; leaving it out takes the format \
                          from the name's extension. On a server started without \
                          `--config` this is how a config file comes into existence at \
                          all, and from that save on it is the file `revert` reloads.",
            tag: Tag::Config,
            params: vec![],
            request: Some(RequestDoc {
                body: Body::Json("SaveConfigRequest"),
                description: "The file name to write, and optionally the format.",
            }),
            responses: vec![
                ResponseDoc {
                    status: 200,
                    description: "Written, with the path it landed at.",
                    body: Body::Json("SaveConfigResponse"),
                },
                ResponseDoc {
                    status: 422,
                    description: "`name` is not a bare file name.",
                    body: Body::Json("ApiError"),
                },
                server_error(),
            ],
        },
        ApiDoc {
            path: "/api/config/revert",
            method: Method::Post,
            operation: Operation::RevertConfig,
            summary: "Throw the running graph away and reload the config file",
            description: "The undo for a session of editing, and as destructive as it \
                          sounds: every running pipeline is stopped and the graph is \
                          rebuilt from the file.\n\n\
                          The file is parsed *before* the runtime is torn down, so a \
                          file broken by hand costs you nothing. The connections are \
                          reloaded first, since the pipelines being rebuilt name them. \
                          It waits for the old pipelines to actually stop before \
                          rebuilding, so the response landing means the new graph is the \
                          only one running.",
            tag: Tag::Config,
            params: vec![],
            request: None,
            responses: vec![
                ResponseDoc {
                    status: 204,
                    description: "Reloaded; the graph is what the file says.",
                    body: Body::None,
                },
                ResponseDoc {
                    status: 500,
                    description: "There is no config file to revert to, or it could not \
                                  be read or parsed — in which case the running graph is \
                                  left alone.",
                    body: Body::Json("ApiError"),
                },
            ],
        },
        ApiDoc {
            path: "/api/layout",
            method: Method::Get,
            operation: Operation::GetLayout,
            summary: "Where the cards sit on the canvas",
            description: "Served separately from `/api/pipelines` because it is a \
                          different kind of thing: that is what the server is running, \
                          this is how someone chose to look at it. A client that ignores \
                          this endpoint gets an automatically laid out graph, which is \
                          the point.\n\n\
                          Only pipelines someone has actually moved appear.",
            tag: Tag::Layout,
            params: vec![],
            request: None,
            responses: vec![ResponseDoc {
                status: 200,
                description: "The stored arrangement.",
                body: Body::Json("LayoutFile"),
            }],
        },
        ApiDoc {
            path: "/api/layout",
            method: Method::Put,
            operation: Operation::ReplaceLayout,
            summary: "Replace the arrangement and write it to disk",
            description: "The whole map, not a patch: the canvas already holds the \
                          complete arrangement, and a full replacement is what makes \
                          \"reset everything to automatic\" a send of `{}` rather than \
                          its own endpoint.\n\n\
                          This is the one edit that writes immediately rather than \
                          waiting for a save, and it never counts as an unsaved change — \
                          moving a card changes nothing the server runs, so there is \
                          nothing worth reviewing before it lands. Without a config file \
                          there is nowhere to write, and the arrangement is kept in \
                          memory until a save creates one.",
            tag: Tag::Layout,
            params: vec![],
            request: Some(RequestDoc {
                body: Body::Json("LayoutFile"),
                description: "The complete arrangement.",
            }),
            responses: vec![
                ResponseDoc {
                    status: 204,
                    description: "Stored, and written if there is a file to write.",
                    body: Body::None,
                },
                server_error(),
            ],
        },
        ApiDoc {
            path: "/events",
            method: Method::Get,
            operation: Operation::StreamEvents,
            summary: "What the pipelines are doing, as it happens",
            description: "A `text/event-stream` of `UiEvent`s: a batch arriving at a \
                          stage, or a failure handling one. Each SSE `data:` field is one \
                          event as JSON.\n\n\
                          It is a broadcast that drops rather than blocks, so a slow \
                          consumer misses events instead of slowing the pipelines down — \
                          which is what `seq` is for, since a gap in it is the honest \
                          report of what was missed. Run loops only publish at all while \
                          somebody is listening.\n\n\
                          The stream is explicitly a dev-tooling affordance rather than \
                          a durable feed, and is marked temporary in the source.",
            tag: Tag::Events,
            params: vec![],
            request: None,
            responses: vec![ResponseDoc {
                status: 200,
                description: "An event stream that stays open.",
                body: Body::EventStream("UiEvent"),
            }],
        },
        ApiDoc {
            path: "/api/docs",
            method: Method::Get,
            operation: Operation::ListComponents,
            summary: "The component reference, as data",
            description: "Every input, transform, output and connection kayak can \
                          build, with their fields, types and documentation — reflected \
                          out of the config schemas, so it cannot drift from what the \
                          server actually accepts.\n\n\
                          The `/docs` *page* generates the same thing in the browser \
                          from the same code, so this endpoint isn't what renders it. It \
                          exists because the component reference is useful to things \
                          that aren't a browser: a config linter, editor completion, a \
                          test.",
            tag: Tag::Reference,
            params: vec![],
            request: None,
            responses: vec![ResponseDoc {
                status: 200,
                description: "Every component, grouped by nothing — `family` says which \
                              plugin point each one plugs into.",
                body: Body::JsonArray("ComponentDoc"),
            }],
        },
        ApiDoc {
            path: "/api/openapi.json",
            method: Method::Get,
            operation: Operation::GetOpenApi,
            summary: "This API, as an OpenAPI 3.1 document",
            description: "Generated from the same table the routes are registered from, \
                          with schemas reflected out of the Rust types — so it describes \
                          the server that is serving it.\n\n\
                          Point a renderer, a client generator or a contract test at it. \
                          `GET /api/reference` is one such renderer, served alongside.",
            tag: Tag::Reference,
            params: vec![],
            request: None,
            responses: vec![ResponseDoc {
                status: 200,
                description: "The OpenAPI document.",
                body: Body::Json("OpenApiDocument"),
            }],
        },
        ApiDoc {
            path: "/api/reference",
            method: Method::Get,
            operation: Operation::ApiReference,
            summary: "The rendered API reference",
            description: "An HTML page rendering `/api/openapi.json`, with a request \
                          panel for trying endpoints against this server.\n\n\
                          The `/docs` page in the UI covers the same endpoints in \
                          kayak's own furniture; this is the full reference, schemas and \
                          all.",
            tag: Tag::Reference,
            params: vec![],
            request: None,
            responses: vec![ResponseDoc {
                status: 200,
                description: "The reference page.",
                body: Body::Html,
            }],
        },
    ]
}

/// The JSON Schema behind every name [`endpoints`] mentions.
///
/// Generated here rather than passed in, for the same reason
/// [`crate::docs::all_components`] generates its own: a caller can't document a
/// stale one. `OpenApiDocument` is deliberately absent — the OpenAPI document's
/// own schema is the OpenAPI meta-schema, which is not ours to reproduce, and
/// [`openapi_document_schema`] stands in for it.
#[must_use]
pub fn schemas() -> BTreeMap<&'static str, Value> {
    // a schema that can't be serialized would mean schemars produced something
    // non-serialisable, which can't happen
    let of = |schema: schemars::Schema| serde_json::to_value(schema).unwrap_or(Value::Null);

    let mut schemas = BTreeMap::new();
    schemas.insert("Config", of(schema_for!(Config)));
    schemas.insert("PipelineDto", of(schema_for!(PipelineDto)));
    schemas.insert("IngestRequest", of(schema_for!(IngestRequest)));
    schemas.insert("IngestResponse", of(schema_for!(IngestResponse)));
    schemas.insert("Connections", of(schema_for!(Connections)));
    schemas.insert("BucketSummary", of(schema_for!(Vec<BucketSummary>)));
    schemas.insert("BucketContents", of(schema_for!(BucketContents)));
    schemas.insert(
        "CreateConnectionRequest",
        of(schema_for!(CreateConnectionRequest)),
    );
    schemas.insert("LayoutFile", of(schema_for!(LayoutFile)));
    schemas.insert("SettingsDto", of(schema_for!(SettingsDto)));
    schemas.insert("SaveConfigRequest", of(schema_for!(SaveConfigRequest)));
    schemas.insert("SaveConfigResponse", of(schema_for!(SaveConfigResponse)));
    schemas.insert("ComponentDoc", of(schema_for!(ComponentDoc)));
    schemas.insert("UiEvent", of(schema_for!(UiEvent)));
    schemas.insert("ApiError", of(schema_for!(ApiError)));
    schemas.insert("OpenApiDocument", openapi_document_schema());
    schemas
}

/// A stand-in schema for the OpenAPI document `/api/openapi.json` serves.
///
/// It describes itself, and describing it properly would mean vendoring the
/// OpenAPI meta-schema. An open object with the two keys that identify it is
/// the honest amount to say.
fn openapi_document_schema() -> Value {
    serde_json::json!({
        "type": "object",
        "title": "OpenApiDocument",
        "description": "An OpenAPI 3.1 document. See https://spec.openapis.org/oas/v3.1.0",
        "required": ["openapi", "paths"],
        "properties": {
            "openapi": { "type": "string" },
            "info": { "type": "object" },
            "paths": { "type": "object" },
            "components": { "type": "object" },
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The docs are only as good as the prose, and an endpoint added in a hurry
    /// with an empty description is the failure mode this catches — the same
    /// bargain `every_component_has_a_description` makes for components.
    #[test]
    fn every_endpoint_has_a_summary_and_a_description() {
        for endpoint in endpoints() {
            assert!(
                !endpoint.summary.trim().is_empty(),
                "{} {} has no summary",
                endpoint.method.label(),
                endpoint.path
            );
            assert!(
                endpoint.description.trim().len() > 40,
                "{} {} has no real description",
                endpoint.method.label(),
                endpoint.path
            );
        }
    }

    /// An operation id is what a generated client names its method, so a
    /// duplicate produces a client that won't compile.
    #[test]
    fn operation_ids_are_unique() {
        let mut ids: Vec<&str> = endpoints().iter().map(ApiDoc::operation_id).collect();
        let count = ids.len();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), count, "two endpoints share an operation id");
    }

    /// `GET` and `POST /api/pipelines` are two entries on one path, so the
    /// method has to be part of the anchor.
    #[test]
    fn anchors_are_unique_across_every_endpoint() {
        let mut anchors: Vec<String> = endpoints().iter().map(ApiDoc::anchor_id).collect();
        let count = anchors.len();
        anchors.sort_unstable();
        anchors.dedup();
        assert_eq!(anchors.len(), count, "two endpoints share an anchor");
    }

    /// A body naming a schema that isn't generated would render as a dangling
    /// `$ref` in the spec and as a link to nothing in the UI.
    #[test]
    fn every_named_schema_exists() {
        let schemas = schemas();
        for endpoint in endpoints() {
            for name in endpoint.schema_names() {
                assert!(
                    schemas.contains_key(name),
                    "{} {} names schema '{name}', which is not generated",
                    endpoint.method.label(),
                    endpoint.path
                );
            }
        }
    }

    /// The other direction: a schema nothing names is dead weight in the spec,
    /// and usually means a body was changed to a different type.
    #[test]
    fn every_generated_schema_is_named_by_an_endpoint() {
        let named: Vec<&str> = endpoints().iter().flat_map(|e| e.schema_names()).collect();
        for name in schemas().keys() {
            assert!(
                named.contains(name),
                "schema '{name}' is generated but no endpoint uses it"
            );
        }
    }

    /// Every path parameter in the table has to appear in the path it is
    /// documented on, and vice versa — a mismatch means axum's extractor and
    /// the docs disagree about what the request carries.
    #[test]
    fn path_parameters_match_the_path() {
        for endpoint in endpoints() {
            let placeholders: Vec<String> = endpoint
                .path
                .split('/')
                .filter_map(|segment| {
                    segment
                        .strip_prefix('{')
                        .and_then(|s| s.strip_suffix('}'))
                        .map(ToString::to_string)
                })
                .collect();
            let documented: Vec<String> = endpoint
                .params
                .iter()
                .map(|p| p.name.to_string())
                .collect();
            assert_eq!(
                placeholders, documented,
                "{} {} documents {documented:?} but its path has {placeholders:?}",
                endpoint.method.label(),
                endpoint.path
            );
        }
    }

    /// A 204 with a body, or a 200 without one, is a documentation bug that
    /// would mislead a generated client into looking for the wrong thing.
    #[test]
    fn no_content_responses_carry_no_body() {
        for endpoint in endpoints() {
            for response in &endpoint.responses {
                assert_eq!(
                    response.status == 204,
                    response.body == Body::None,
                    "{} {} documents a {} with body {:?}",
                    endpoint.method.label(),
                    endpoint.path,
                    response.status,
                    response.body
                );
            }
        }
    }

    /// Every endpoint documents at least one success, and every failure it
    /// documents comes back as the shared error body.
    #[test]
    fn responses_are_a_success_and_error_bodies() {
        for endpoint in endpoints() {
            assert!(
                endpoint.responses.iter().any(|r| r.status < 300),
                "{} {} documents no success",
                endpoint.method.label(),
                endpoint.path
            );
            for response in endpoint.responses.iter().filter(|r| r.status >= 400) {
                assert_eq!(
                    response.body,
                    Body::Json("ApiError"),
                    "{} {} documents a {} that isn't an ApiError",
                    endpoint.method.label(),
                    endpoint.path,
                    response.status
                );
            }
        }
    }

    /// A request body on a GET or DELETE would be ignored by the handler.
    #[test]
    fn only_writes_carry_a_request_body() {
        for endpoint in endpoints() {
            if matches!(endpoint.method, Method::Get | Method::Delete) {
                assert!(
                    endpoint.request.is_none(),
                    "{} {} documents a request body",
                    endpoint.method.label(),
                    endpoint.path
                );
            }
        }
    }

    /// The descriptions are searched too, so an endpoint that *mentions* the
    /// term is a hit — `saveConfig`'s prose talks about what `revert` reloads.
    /// That's the search working rather than a false positive: someone typing
    /// "revert" wants both of these.
    #[test]
    fn a_query_narrows_the_list() {
        let matching: Vec<&str> = endpoints()
            .iter()
            .filter(|e| e.matches("revert"))
            .map(ApiDoc::operation_id)
            .collect();
        assert_eq!(matching, ["saveConfig", "revertConfig"]);

        let by_path: Vec<&str> = endpoints()
            .iter()
            .filter(|e| e.matches("/api/layout"))
            .map(ApiDoc::operation_id)
            .collect();
        assert_eq!(by_path, ["getLayout", "replaceLayout"]);
    }

    #[test]
    fn an_empty_query_keeps_every_endpoint() {
        assert_eq!(
            endpoints().iter().filter(|e| e.matches("  ")).count(),
            endpoints().len()
        );
    }

    /// Searching by status is the point of searching the responses: "which
    /// endpoints can 409 at me" is a real question with a real answer.
    #[test]
    fn a_status_code_finds_the_endpoints_that_return_it() {
        let matching: Vec<&str> = endpoints()
            .iter()
            .filter(|e| e.matches("409"))
            .map(ApiDoc::operation_id)
            .collect();
        assert_eq!(matching, ["createPipeline", "createConnection", "deleteConnection"]);
    }

    #[test]
    fn a_body_reads_as_a_type_name() {
        assert_eq!(Body::Json("Config").type_name(), "Config");
        assert_eq!(Body::JsonArray("PipelineDto").type_name(), "[PipelineDto]");
        assert_eq!(Body::None.type_name(), "—");
    }
}
