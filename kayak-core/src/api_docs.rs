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
use crate::history::PipelineHistory;
use crate::layout::LayoutFile;
use crate::script::{DryRunRequest, DryRunResponse};
use crate::server_config::Role;
use crate::state::{BucketContents, BucketSummary};
use crate::{
    AuthDto, IngestRequest, IngestResponse, LoginRequest, PipelineDto, SaveConfigRequest,
    TokenLoginRequest,
    SaveConfigResponse, SettingsDto, UiEvent,
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
    GetPipelineHistory,
    DryRunScript,
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
    Login,
    TokenLogin,
    Logout,
    WhoAmI,
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
            Self::GetPipelineHistory => "getPipelineHistory",
            Self::DryRunScript => "dryRunScript",
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
            Self::Login => "login",
            Self::TokenLogin => "tokenLogin",
            Self::Logout => "logout",
            Self::WhoAmI => "whoAmI",
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
    Auth,
    Reference,
}

/// Every tag, in the order pages list them.
pub const TAGS: [Tag; 8] = [
    Tag::Pipelines,
    Tag::Connections,
    Tag::State,
    Tag::Config,
    Tag::Layout,
    Tag::Events,
    Tag::Auth,
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
            Self::Auth => "auth",
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
            Self::Auth => {
                "Signing in and out. Present on every server; on one with no accounts \
                 configured they report that there is nothing to sign into."
            }
            Self::Reference => "The API describing itself.",
        }
    }
}

/// Who may call an endpoint.
///
/// This sits in the table for the reason everything else does: `api_router` is
/// **built from this table**, so the access an endpoint is documented with is
/// the access the middleware enforces, not a second fact that agrees with it
/// today. A new endpoint can't be added without answering the question, and it
/// can't be answered in two places.
///
/// The alternative — deriving it from the method, GET being read and everything
/// else admin — is wrong on the two endpoints that matter most:
/// `POST /api/pipelines/{id}/messages` is a POST that is not an administrative
/// act at all, and `PUT /api/layout` is a write to a committed file.
///
/// On a server with [`AuthConfig::None`](crate::server_config::AuthConfig) none
/// of this applies: nobody is identified, so nothing is checked and every
/// endpoint behaves as it did before roles existed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Access {
    /// No credentials needed even when authentication is on.
    ///
    /// Three kinds of thing end up here and they are not an accident: the
    /// endpoints you need *in order* to log in, the ones that describe the
    /// software rather than the deployment (the component reference and the
    /// spec — they say what kayak is, not what this server is running), and the
    /// ingest endpoint, which is a data plane rather than a control plane and
    /// has its own mechanism — the `auth` on the `http` input it serves, which
    /// is per pipeline and checked by the input rather than by the router.
    Public,
    /// Any authenticated user. Everything that looks at the running graph
    /// without changing it.
    Read,
    /// Changes what the server is running, or writes a file. Requires
    /// [`Role::Admin`].
    Admin,
}

impl Access {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Read => "read",
            Self::Admin => "admin",
        }
    }

    /// What the reference says about who may call this.
    #[must_use]
    pub fn description(self) -> &'static str {
        match self {
            Self::Public => "Callable without credentials, even when authentication is on.",
            Self::Read => "Any signed-in user.",
            Self::Admin => "Signed-in users with the `admin` role.",
        }
    }

    /// Whether a caller in this role may make the call. `None` is a caller who
    /// presented no credentials at all.
    #[must_use]
    pub fn permits(self, role: Option<Role>) -> bool {
        match (self, role) {
            (Self::Public, _) => true,
            (Self::Read, Some(_)) | (Self::Admin, Some(Role::Admin)) => true,
            (Self::Read | Self::Admin, None) | (Self::Admin, Some(Role::Read)) => false,
        }
    }

    /// Whether reaching this endpoint takes credentials at all — which is what
    /// decides whether the spec attaches a security requirement to it, and
    /// whether a 401 is among its documented outcomes.
    #[must_use]
    pub fn is_protected(self) -> bool {
        !matches!(self, Self::Public)
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
    /// Who may call it. Enforced by the middleware the router applies from this
    /// same entry, so the documentation and the check are one fact.
    pub access: Access,
    /// Path placeholders, in the order they appear in `path`.
    pub params: Vec<ParamDoc>,
    /// Query parameters. Always optional — an endpoint that cannot answer
    /// without one has it in the path instead, which is what makes a bare
    /// request to any documented path a working request.
    pub query: Vec<ParamDoc>,
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
            access: Access::Read,
            params: vec![],
            query: vec![],
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
            access: Access::Admin,
            params: vec![],
            query: vec![],
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
            access: Access::Admin,
            params: vec![ParamDoc {
                name: "pipeline_id",
                description: "The id the pipeline is running under.",
            }],
            query: vec![],
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
                          the data has landed anywhere.\n\n\
                          This endpoint does not use the server's sign-in — it is a data \
                          plane, and a system pushing readings should not need an account \
                          that can rewrite the graph. Protecting it is the `http` input's \
                          own `auth` field: a token the sender repeats in a header, \
                          declared per pipeline. Without one the endpoint takes anything \
                          that reaches it, which is the default.",
            tag: Tag::Pipelines,
            access: Access::Public,
            params: vec![ParamDoc {
                name: "pipeline_id",
                description: "The id of the pipeline to post to.",
            }],
            query: vec![],
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
                    status: 401,
                    description: "The input has an `auth` and this post didn't satisfy it. \
                                  This is the input's own credential, not the server's \
                                  sign-in: an account on the server does not let you post, \
                                  and the token does not let you do anything else.",
                    body: Body::Json("ApiError"),
                },
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
            path: "/api/pipelines/{pipeline_id}/history",
            method: Method::Get,
            operation: Operation::GetPipelineHistory,
            summary: "What a pipeline has been doing, after the fact",
            description: "Throughput and failures over time, kept in the server's memory \
                          so that something which broke overnight can still be read in \
                          the morning.\n\n\
                          This is the counterpart to `/events`, not a replay of it. The \
                          event stream is a live sample: it is only produced while a \
                          browser is attached and it drops passes under load on \
                          purpose. History is fed by counters the run loop keeps \
                          regardless of who is watching, so it is complete in what it \
                          counts — and correspondingly it carries no message payloads \
                          at all, only counts, and failures aggregated to one entry per \
                          distinct message with a first-seen, a last-seen and a tally.\n\n\
                          Buckets are contiguous and oldest first, including empty ones: \
                          a run of zeroes is a pipeline that stopped, which is a \
                          different fact from a gap and is spelled differently.\n\n\
                          An unknown or newly created pipeline answers with an empty \
                          history rather than a 404 — a pipeline that has not done \
                          anything yet is not an error. How much is kept is the \
                          `history.retention_secs` in the server config; when that is \
                          zero nothing is recorded and this always answers empty.",
            tag: Tag::Pipelines,
            access: Access::Read,
            params: vec![ParamDoc {
                name: "pipeline_id",
                description: "Id of the pipeline.",
            }],
            query: vec![ParamDoc {
                name: "resolution",
                description: "`coarse` (the default) — a minute a bucket, over the \
                              configured retention, which is the overnight record. \
                              `fine` — five seconds a bucket over the last half hour, \
                              which is what a card's live chart is backfilled from so \
                              it starts full rather than drawing itself over the next \
                              two minutes.",
            }],
            request: None,
            responses: vec![ResponseDoc {
                status: 200,
                description: "The pipeline's history at the resolution asked for.",
                body: Body::Json("PipelineHistory"),
            }],
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
            access: Access::Read,
            params: vec![],
            query: vec![],
            request: None,
            responses: vec![ResponseDoc {
                status: 200,
                description: "Every configured connection, in name order.",
                body: Body::Json("Connections"),
            }],
        },
        ApiDoc {
            path: "/api/scripts/dry-run",
            method: Method::Post,
            operation: Operation::DryRunScript,
            summary: "Run a script over some messages, without creating a pipeline",
            description: "A `script` transform is the one component whose configuration can \
                          be wrong in a way the config's *shape* cannot express: for every \
                          other component, a config that deserializes and builds does what \
                          it says, and for this one the interesting mistakes are all inside \
                          a string. This endpoint is where that string gets checked.\n\n\
                          It compiles the script and runs it over the messages in the body, \
                          through the same runner and under the same operation budget and \
                          sandbox a running transform gets — a dry run that could disagree \
                          with production would be worse than none, because it would be \
                          trusted.\n\n\
                          **A script with a bug in it is a 200, not a 400.** The request was \
                          well formed and the server answered it completely; where the bug \
                          is *is* the answer. The response is a tagged union: `emitted` \
                          carries the batches, `failed` carries the message with a line and \
                          column an editor can point at. A 400 here means the request itself \
                          was wrong — malformed JSON, or a `file` source naming something \
                          unreadable.\n\n\
                          State is **never live**. The run gets a private bucket seeded from \
                          `state` in the body and thrown away afterwards, and what it holds \
                          at the end comes back in the response. Reading production state \
                          would make the answer depend on what the server happened to be \
                          doing; writing it would give a dry run side effects.",
            tag: Tag::Pipelines,
            access: Access::Admin,
            params: vec![],
            query: vec![],
            request: Some(RequestDoc {
                body: Body::Json("DryRunRequest"),
                description: "The script, and the messages to run it over.",
            }),
            responses: vec![
                ResponseDoc {
                    status: 200,
                    description: "The script ran, or it did not compile — the `outcome` \
                                  field says which.",
                    body: Body::Json("DryRunResponse"),
                },
                ResponseDoc {
                    status: 400,
                    description: "The request itself was wrong: malformed JSON, or a `file` \
                                  source that could not be read.",
                    body: Body::Json("ApiError"),
                },
                server_error(),
            ],
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
            access: Access::Read,
            params: vec![],
            query: vec![],
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
            access: Access::Read,
            params: vec![ParamDoc {
                name: "bucket",
                description: "Name of the bucket, as declared in the config.",
            }],
            query: vec![],
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
            access: Access::Admin,
            params: vec![],
            query: vec![],
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
            access: Access::Admin,
            params: vec![ParamDoc {
                name: "connection_id",
                description: "The name the connection is filed under.",
            }],
            query: vec![],
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
            access: Access::Read,
            params: vec![],
            query: vec![],
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
                          all, and from that save on it is the file `revert` reloads.\n\n\
                          `overwrite` defaults to `true`, which is what makes saving \
                          over the loaded file the ordinary thing it has always been. \
                          Sending `false` turns the save into a **create**: if the name \
                          — or either of the two files written beside it — is already \
                          on disk, the request is refused with a 409 and nothing is \
                          written. That is what the UI's project creator sends, since \
                          it suggests a file name into a directory its user has often \
                          never looked at.",
            tag: Tag::Config,
            access: Access::Admin,
            params: vec![],
            query: vec![],
            request: Some(RequestDoc {
                body: Body::Json("SaveConfigRequest"),
                description: "The file name to write, optionally the format, and \
                              whether an existing file may be replaced.",
            }),
            responses: vec![
                ResponseDoc {
                    status: 200,
                    description: "Written, with the path it landed at.",
                    body: Body::Json("SaveConfigResponse"),
                },
                ResponseDoc {
                    status: 409,
                    description: "`overwrite` was `false` and the file — or one of the \
                                  two written beside it — is already there. Nothing was \
                                  written; the message names the files.",
                    body: Body::Json("ApiError"),
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
            access: Access::Admin,
            params: vec![],
            query: vec![],
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
            access: Access::Read,
            params: vec![],
            query: vec![],
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
            access: Access::Admin,
            params: vec![],
            query: vec![],
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
            access: Access::Read,
            params: vec![],
            query: vec![],
            request: None,
            responses: vec![ResponseDoc {
                status: 200,
                description: "An event stream that stays open.",
                body: Body::EventStream("UiEvent"),
            }],
        },
        ApiDoc {
            path: "/api/auth/me",
            method: Method::Get,
            operation: Operation::WhoAmI,
            summary: "Who the caller is, and whether this server asks",
            description: "`authentication_required` says whether this server checks \
                          credentials at all — a server started without a \
                          `--server-config`, or with one declaring `auth: {type: none}`, \
                          answers `false` and lets everybody do everything.\n\n\
                          `username` and `role` describe the caller, and are both null \
                          for one who presented nothing. Note that a null `role` is not \
                          the same as `read`: a reader may see the graph, a signed-out \
                          caller may not.\n\n\
                          Callable without credentials, necessarily — it is the endpoint \
                          that answers 'do I need to show a login page'.",
            tag: Tag::Auth,
            access: Access::Public,
            params: vec![],
            query: vec![],
            request: None,
            responses: vec![ResponseDoc {
                status: 200,
                description: "Who you are. Not an error even when the answer is nobody.",
                body: Body::Json("AuthDto"),
            }],
        },
        ApiDoc {
            path: "/api/auth/login",
            method: Method::Post,
            operation: Operation::Login,
            summary: "Exchange credentials for a session",
            description: "Checks a username and password against the accounts in the \
                          server's settings file and, on success, sets an `HttpOnly` \
                          session cookie.\n\n\
                          This is for browsers. Everything else should send \
                          `Authorization: Basic` on each request instead and never come \
                          here — the cookie exists because `EventSource`, which the UI \
                          consumes `/events` with, cannot send headers.\n\n\
                          A wrong password and an unknown username are the same 401, \
                          deliberately: the endpoint is not a way to find out who has an \
                          account. On a server with no accounts configured this is not an \
                          error either — it answers 200 with `authentication_required` \
                          false, because there is nothing to sign into.",
            tag: Tag::Auth,
            access: Access::Public,
            params: vec![],
            query: vec![],
            request: Some(RequestDoc {
                body: Body::Json("LoginRequest"),
                description: "The credentials to check.",
            }),
            responses: vec![
                ResponseDoc {
                    status: 200,
                    description: "Signed in. The session cookie is in `Set-Cookie`.",
                    body: Body::Json("AuthDto"),
                },
                ResponseDoc {
                    status: 401,
                    description: "Wrong username or password — the body does not say which.",
                    body: Body::Json("ApiError"),
                },
                server_error(),
            ],
        },
        ApiDoc {
            path: "/api/auth/token",
            method: Method::Post,
            operation: Operation::TokenLogin,
            summary: "Exchange an identity provider's JWT for a session",
            description: "The embedding flow's endpoint, on a server whose auth section is \
                          `jwt`: a host application that already holds a token from the \
                          shared identity provider — Cognito, Keycloak — puts it on the \
                          iframe URL as `?auth_token=`, and the UI posts it here once. The \
                          token is checked against the issuer's published keys and, on \
                          success, exchanged for the same `HttpOnly` session cookie a \
                          password login sets — so the token itself appears in exactly one \
                          request and never in an access log again.\n\n\
                          The session ends no later than the token's `exp`: the cookie \
                          must not outlive the identity provider's word that the caller is \
                          signed in.\n\n\
                          API callers don't need this exchange — on a `jwt` server, \
                          `Authorization: Bearer <token>` works directly on every endpoint.\n\n\
                          Every way of being refused is the same 401, deliberately: an \
                          expired token, a wrong issuer and a server that doesn't take \
                          tokens at all are not distinctions worth handing to a guesser.",
            tag: Tag::Auth,
            access: Access::Public,
            params: vec![],
            query: vec![],
            request: Some(RequestDoc {
                body: Body::Json("TokenLoginRequest"),
                description: "The token to check.",
            }),
            responses: vec![
                ResponseDoc {
                    status: 200,
                    description: "Signed in. The session cookie is in `Set-Cookie`.",
                    body: Body::Json("AuthDto"),
                },
                ResponseDoc {
                    status: 401,
                    description: "The token was not accepted, or this server does not \
                                  take tokens.",
                    body: Body::Json("ApiError"),
                },
                server_error(),
            ],
        },
        ApiDoc {
            path: "/api/auth/logout",
            method: Method::Post,
            operation: Operation::Logout,
            summary: "End the session this request carries",
            description: "Clears the cookie in the browser and drops the session on the \
                          server, so a copy of the cookie taken from somewhere else stops \
                          working too.\n\n\
                          Idempotent: 204 whether or not there was a session to end.",
            tag: Tag::Auth,
            access: Access::Read,
            params: vec![],
            query: vec![],
            request: None,
            responses: vec![ResponseDoc {
                status: 204,
                description: "Signed out.",
                body: Body::None,
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
            access: Access::Public,
            params: vec![],
            query: vec![],
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
            access: Access::Public,
            params: vec![],
            query: vec![],
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
            access: Access::Public,
            params: vec![],
            query: vec![],
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
    schemas.insert("PipelineHistory", of(schema_for!(PipelineHistory)));
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
    schemas.insert("DryRunRequest", of(schema_for!(DryRunRequest)));
    schemas.insert("DryRunResponse", of(schema_for!(DryRunResponse)));
    schemas.insert("ApiError", of(schema_for!(ApiError)));
    schemas.insert("LoginRequest", of(schema_for!(LoginRequest)));
    schemas.insert("TokenLoginRequest", of(schema_for!(TokenLoginRequest)));
    schemas.insert("AuthDto", of(schema_for!(AuthDto)));
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
            let documented: Vec<String> =
                endpoint.params.iter().map(|p| p.name.to_string()).collect();
            assert_eq!(
                placeholders,
                documented,
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

    /// The access level of every endpoint, written out.
    ///
    /// A list rather than a rule, and deliberately: the rules you would write
    /// instead ("a GET is a read", "a write is admin") are both wrong here, and
    /// wrong in the direction that hands an anonymous caller a delete button.
    /// So the whole assignment is spelled out, and a new endpoint fails this
    /// test until someone has looked at it and added a line — which is the
    /// point at which the question gets asked.
    #[test]
    fn every_endpoint_is_pinned_to_the_access_it_was_reviewed_at() {
        let actual: Vec<(&str, &str)> = endpoints()
            .iter()
            .map(|e| (e.operation_id(), e.access.label()))
            .collect();
        assert_eq!(
            actual,
            [
                ("listPipelines", "read"),
                ("createPipeline", "admin"),
                ("deletePipeline", "admin"),
                // the data plane, not the control plane: a device posting
                // readings is not an operator, and this endpoint gets its own
                // mechanism rather than the operators' credentials
                ("ingestMessages", "public"),
                // counts and failure texts, no message payloads — the same
                // thing a reader can already watch go past on `/events`, only
                // after the fact
                ("getPipelineHistory", "read"),
                ("listConnections", "read"),
                // executes code the caller supplied. It is sandboxed and its
                // state is a scratch bucket, so it cannot reach the running
                // graph — but "runs what you send it" is an operator's
                // capability whatever the sandbox does, and it is the same
                // capability `createPipeline` already grants. Never lower this
                // to `read`.
                ("dryRunScript", "admin"),
                ("listStateBuckets", "read"),
                ("getStateBucket", "read"),
                ("createConnection", "admin"),
                ("deleteConnection", "admin"),
                ("getSettings", "read"),
                ("saveConfig", "admin"),
                ("revertConfig", "admin"),
                ("getLayout", "read"),
                // a write to a file that gets committed, so admin — a reader
                // can look at the canvas, they just can't rearrange it
                ("replaceLayout", "admin"),
                ("streamEvents", "read"),
                // the endpoints you need in order to log in, so they cannot
                // themselves need you to be logged in
                ("whoAmI", "public"),
                ("login", "public"),
            ("tokenLogin", "public"),
                // ...but signing out is something only a signed-in caller can
                // meaningfully do
                ("logout", "read"),
                // these three describe kayak rather than this deployment
                ("listComponents", "public"),
                ("getOpenApi", "public"),
                ("apiReference", "public"),
            ]
        );
    }

    /// Anything that changes what the server is running, or writes to disk, is
    /// an administrative act. The converse isn't a rule — `ingestMessages` is a
    /// POST that is neither — which is why this only checks one direction.
    #[test]
    fn nothing_that_changes_the_graph_is_reachable_by_a_reader() {
        for endpoint in endpoints() {
            let changes_the_graph = matches!(
                endpoint.operation,
                Operation::CreatePipeline
                    | Operation::DeletePipeline
                    | Operation::CreateConnection
                    | Operation::DeleteConnection
                    | Operation::SaveConfig
                    | Operation::RevertConfig
                    | Operation::ReplaceLayout
            );
            if changes_the_graph {
                assert!(
                    !endpoint.access.permits(Some(Role::Read)),
                    "{} {} lets a reader change the server",
                    endpoint.method.label(),
                    endpoint.path
                );
            }
        }
    }

    /// The three answers, in one place. An admin may do anything; a reader may
    /// do anything that doesn't change the server; someone who presented no
    /// credentials may only reach what is public.
    #[test]
    fn a_role_permits_its_own_level_and_below() {
        assert!(Access::Public.permits(None));
        assert!(Access::Public.permits(Some(Role::Read)));
        assert!(Access::Public.permits(Some(Role::Admin)));

        assert!(!Access::Read.permits(None));
        assert!(Access::Read.permits(Some(Role::Read)));
        assert!(Access::Read.permits(Some(Role::Admin)));

        assert!(!Access::Admin.permits(None));
        assert!(!Access::Admin.permits(Some(Role::Read)));
        assert!(Access::Admin.permits(Some(Role::Admin)));
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
        assert_eq!(
            matching,
            [
                "createPipeline",
                "createConnection",
                "deleteConnection",
                // a save that asked not to overwrite, onto a name that is taken
                "saveConfig"
            ]
        );
    }

    #[test]
    fn a_body_reads_as_a_type_name() {
        assert_eq!(Body::Json("Config").type_name(), "Config");
        assert_eq!(Body::JsonArray("PipelineDto").type_name(), "[PipelineDto]");
        assert_eq!(Body::None.type_name(), "—");
    }
}
