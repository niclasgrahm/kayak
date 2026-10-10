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
use crate::format::PipelineSource;
use crate::history::PipelineHistory;
use crate::layout::LayoutFile;
use crate::dry_run::{PipelineDryRunRequest, PipelineDryRunResponse};
use crate::sample::{SampleRequest, SampleResponse};
use crate::script::{DryRunRequest, DryRunResponse, LoadedScript};
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
    /// The error, as one line. The line includes the cause, in the form
    /// `context: cause`.
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
    GetPipelineScript,
    GetPipelineConfig,
    DryRunScript,
    SampleInput,
    DryRunPipeline,
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
            Self::GetPipelineScript => "getPipelineScript",
            Self::GetPipelineConfig => "getPipelineConfig",
            Self::DryRunScript => "dryRunScript",
            Self::SampleInput => "sampleInput",
            Self::DryRunPipeline => "dryRunPipeline",
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
                "The running graph. A create or a delete has an immediate effect. \
                 It writes nothing to disk."
            }
            Self::Connections => {
                "The systems that pipelines connect to. You declare each system one \
                 time, with a name, and components refer to the name."
            }
            Self::State => {
                "The contents of the state buckets. These endpoints are read-only. \
                 You declare buckets in the config, and `remember` transforms write \
                 to them."
            }
            Self::Config => {
                "The config file: how the server started, a save of the running \
                 graph to the file, and a reload of the graph from the file."
            }
            Self::Layout => {
                "The positions of the pipelines in the web UI. kayak writes the \
                 layout to its own file immediately. It is not configuration."
            }
            Self::Events => "A live feed of the activity of the pipelines.",
            Self::Auth => {
                "Sign in and sign out. These endpoints exist on every server. On a \
                 server with no accounts, they report that there is nothing to sign \
                 in to."
            }
            Self::Reference => "The description of the API.",
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
            Self::Public => "No credentials are necessary, also when authentication is on.",
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
        description: "An error occurred on the server. The body describes the error.",
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
            summary: "List the running pipelines",
            description: "Returns the config of each running pipeline, with the id that it \
                          runs under. If the config has no `id`, kayak generates one.\n\n\
                          This is the state of the server, not of the config file. A \
                          pipeline that you created after startup is in this list, but not \
                          in the file. `GET /api/settings` tells you if the two are \
                          different.",
            tag: Tag::Pipelines,
            access: Access::Read,
            params: vec![],
            query: vec![],
            request: None,
            responses: vec![
                ResponseDoc {
                    status: 200,
                    description: "The running pipelines, in no specified order.",
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
            description: "The body is the config of one pipeline, as in the `pipelines` \
                          array of a config file. If you leave out `id`, kayak generates a \
                          random id that people can read. The response contains the id.\n\n\
                          kayak builds and starts the pipeline before it sends the \
                          response. Thus, a 201 means that the pipeline runs. If a \
                          component does not build, the response is a 422 and nothing \
                          starts. For example, an unknown connection or a missing secret \
                          gives a 422.\n\n\
                          This endpoint writes nothing to disk. To write the config file, \
                          use `POST /api/config/save`.",
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
                    description: "The pipeline is built and runs. The body contains its \
                                  id.",
                    body: Body::Json("PipelineDto"),
                },
                ResponseDoc {
                    status: 409,
                    description: "A pipeline with this id already runs.",
                    body: Body::Json("ApiError"),
                },
                ResponseDoc {
                    status: 422,
                    description: "The JSON is valid, but the pipeline does not build. For \
                                  example, a connection is unknown, a secret is missing, \
                                  or an upstream pipeline does not exist.",
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
            description: "Stops the run loop of the pipeline and removes the pipeline from \
                          the graph. Downstream pipelines continue to run. They receive \
                          nothing more from this pipeline.\n\n\
                          This endpoint writes nothing to disk.",
            tag: Tag::Pipelines,
            access: Access::Admin,
            params: vec![ParamDoc {
                name: "pipeline_id",
                description: "The id of the pipeline.",
            }],
            query: vec![],
            request: None,
            responses: vec![
                ResponseDoc {
                    status: 204,
                    description: "The pipeline is stopped and removed.",
                    body: Body::None,
                },
                not_found("No pipeline runs with that id."),
                server_error(),
            ],
        },
        ApiDoc {
            path: "/api/pipelines/{pipeline_id}/messages",
            method: Method::Post,
            operation: Operation::IngestMessages,
            summary: "Post messages into a pipeline",
            description: "The endpoint of the `http` input of a pipeline. The path comes \
                          from the id of the pipeline. The endpoint exists while the \
                          pipeline runs. A system can use it to send data to kayak without \
                          a broker.\n\n\
                          The body is one JSON message or an array of messages. An array \
                          becomes one batch, so ten messages go through the transforms in \
                          one pass. The transforms get the posted JSON with no change.\n\n\
                          A 202 means that the pipeline put the batch in its queue. kayak \
                          sends the response before the outputs write the batch. Thus, a \
                          202 does not tell you that the data arrived at an output.\n\n\
                          This endpoint does not use the sign-in of the server. To protect \
                          it, set `auth` on the `http` input of the pipeline. The sender \
                          then puts a token in a header. Without `auth`, the endpoint \
                          accepts all requests. This is the default.",
            tag: Tag::Pipelines,
            access: Access::Public,
            params: vec![ParamDoc {
                name: "pipeline_id",
                description: "The id of the pipeline to post to.",
            }],
            query: vec![],
            request: Some(RequestDoc {
                body: Body::Json("IngestRequest"),
                description: "One message, or an array of messages to send as one batch.",
            }),
            responses: vec![
                ResponseDoc {
                    status: 202,
                    description: "The pipeline accepted the messages. The body contains \
                                  the number of messages.",
                    body: Body::Json("IngestResponse"),
                },
                not_found(
                    "No pipeline runs with that id, or the pipeline has no `http` input.",
                ),
                ResponseDoc {
                    status: 401,
                    description: "The input has `auth`, and the request did not satisfy \
                                  it. This is the credential of the input, not the sign-in \
                                  of the server. A server account does not give access to \
                                  this endpoint. The token gives access to nothing else.",
                    body: Body::Json("ApiError"),
                },
                ResponseDoc {
                    status: 503,
                    description: "The queue of the pipeline is full, because the pipeline \
                                  reads slower than the requests arrive. The pipeline \
                                  accepted nothing. Send the request again.",
                    body: Body::Json("ApiError"),
                },
                server_error(),
            ],
        },
        ApiDoc {
            path: "/api/pipelines/{pipeline_id}/history",
            method: Method::Get,
            operation: Operation::GetPipelineHistory,
            summary: "Get the history of a pipeline",
            description: "Returns the throughput and the failures of a pipeline over time. \
                          The server keeps this data in memory.\n\n\
                          The history is not a copy of `/events`. The event stream is a \
                          live sample. It runs only while a client listens, and it drops \
                          passes under load. The history comes from counters that the run \
                          loop always updates. Thus, its counts are complete. It contains \
                          no message payloads. It contains counts, and one failure record \
                          for each different error message, with the first time, the last \
                          time and a count.\n\n\
                          The buckets are oldest first, with no gaps, and empty buckets \
                          are included. A sequence of zero counts means that the pipeline \
                          stopped.\n\n\
                          For an unknown pipeline or a new pipeline, the response is an \
                          empty history, not a 404. `history.retention_secs` in the server \
                          config sets how long the server keeps the history. When it is \
                          zero, the server records nothing and the response is always \
                          empty.",
            tag: Tag::Pipelines,
            access: Access::Read,
            params: vec![ParamDoc {
                name: "pipeline_id",
                description: "The id of the pipeline.",
            }],
            query: vec![ParamDoc {
                name: "resolution",
                description: "`coarse` (the default): one bucket for each minute, over the \
                              configured retention. `fine`: one bucket for each 5 s, over \
                              the last 30 minutes. An unknown value gives `coarse`.",
            }],
            request: None,
            responses: vec![ResponseDoc {
                status: 200,
                description: "The history of the pipeline at the requested resolution.",
                body: Body::Json("PipelineHistory"),
            }],
        },
        ApiDoc {
            path: "/api/pipelines/{pipeline_id}/transforms/{index}/script",
            method: Method::Get,
            operation: Operation::GetPipelineScript,
            summary: "Get the script of a running transform",
            description: "Returns the rhai source of one `script` transform in a running \
                          pipeline, and each module that it imports. Use it to read the \
                          script of a `file` source over HTTP.\n\n\
                          This is the text that kayak used to **build** the pipeline. \
                          kayak reads a file source and its imports one time, when it \
                          builds the pipeline. A running script does not read the \
                          filesystem again. If a file changed or was removed after the \
                          build, `changed_on_disk` is true on that script or module. To \
                          use the change, reload the config from disk.",
            tag: Tag::Pipelines,
            access: Access::Read,
            params: vec![
                ParamDoc {
                    name: "pipeline_id",
                    description: "The id of the pipeline.",
                },
                ParamDoc {
                    name: "index",
                    description: "The position of the transform in the chain of the \
                                  pipeline, from zero.",
                },
            ],
            query: vec![],
            request: None,
            responses: vec![
                ResponseDoc {
                    status: 200,
                    description: "The script, as built.",
                    body: Body::Json("LoadedScript"),
                },
                ResponseDoc {
                    status: 404,
                    description: "No pipeline runs with that id, or the transform at that \
                                  position is not a `script`, or there is no transform at \
                                  that position.",
                    body: Body::Json("ApiError"),
                },
            ],
        },
        ApiDoc {
            path: "/api/pipelines/{pipeline_id}/config",
            method: Method::Get,
            operation: Operation::GetPipelineConfig,
            summary: "Get the config of one pipeline as text",
            description: "Returns the config of a running pipeline as YAML or JSON text. \
                          kayak uses the same code as a save to the config file. Thus, the \
                          text is the same as the entry of the pipeline in a saved file, \
                          with the `id` included.\n\n\
                          This is the config that the pipeline **runs**. It can be \
                          different from the file on disk, because you can change the \
                          graph without a save. Credentials appear as their `${NAME}` \
                          references. They are on the connections that the config names, \
                          not in the config.",
            tag: Tag::Pipelines,
            access: Access::Read,
            params: vec![ParamDoc {
                name: "pipeline_id",
                description: "The id of the pipeline.",
            }],
            query: vec![ParamDoc {
                name: "format",
                description: "`yaml` or `json`. Without a value, or with a different \
                              value, the response uses the format of the config file. On a \
                              server with no config file, it uses JSON. The response tells \
                              you the format.",
            }],
            request: None,
            responses: vec![
                ResponseDoc {
                    status: 200,
                    description: "The config of the pipeline as text.",
                    body: Body::Json("PipelineSource"),
                },
                ResponseDoc {
                    status: 404,
                    description: "No pipeline runs with that id.",
                    body: Body::Json("ApiError"),
                },
            ],
        },
        ApiDoc {
            path: "/api/connections",
            method: Method::Get,
            operation: Operation::ListConnections,
            summary: "List the connections",
            description: "Returns the connections, as an object from name to connection. \
                          This is the same shape as the connections file.\n\n\
                          Credentials appear as their `${NAME}` references. The response \
                          never contains the secret values.",
            tag: Tag::Connections,
            access: Access::Read,
            params: vec![],
            query: vec![],
            request: None,
            responses: vec![ResponseDoc {
                status: 200,
                description: "All connections, in alphabetical order of name.",
                body: Body::Json("Connections"),
            }],
        },
        ApiDoc {
            path: "/api/scripts/dry-run",
            method: Method::Post,
            operation: Operation::DryRunScript,
            summary: "Run a script over messages, without a pipeline",
            description: "Compiles a `script` and runs it over the messages in the body. \
                          Use it to test a script before you put it in a pipeline. The dry \
                          run uses the same runner, the same operation limit and the same \
                          sandbox as a running transform.\n\n\
                          **A script with an error gives a 200, not a 400.** The response \
                          is a tagged union on `outcome`. With `emitted`, it contains the \
                          batches. With `failed`, it contains the error message, with a \
                          line and a column. A 400 means that the request is not valid: \
                          the JSON is malformed, or a `file` source cannot be read.\n\n\
                          The dry run **never uses live state**. It gets a private bucket, \
                          filled from `state` in the body. The response contains the \
                          contents of the bucket at the end. Then kayak discards the \
                          bucket.",
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
                    description: "The script ran, or it did not compile. The `outcome` \
                                  field tells you which.",
                    body: Body::Json("DryRunResponse"),
                },
                ResponseDoc {
                    status: 400,
                    description: "The request is not valid: the JSON is malformed, or a \
                                  `file` source cannot be read.",
                    body: Body::Json("ApiError"),
                },
                server_error(),
            ],
        },
        ApiDoc {
            path: "/api/inputs/sample",
            method: Method::Post,
            operation: Operation::SampleInput,
            summary: "Read a few messages from an input, without a pipeline",
            description: "Builds the input in the body as a pipeline does. Then it reads \
                          up to `max_messages` messages in `timeout_ms`, and stops the \
                          input. Use it to see the fields of a stream before you configure \
                          the transforms and the outputs.\n\n\
                          The sample uses the **real** input, with its `envelope`. Thus, \
                          the sample contains the metadata fields. The sample ignores the \
                          `buffer` of the input. `notes` in the response lists each change \
                          that the sample made.\n\n\
                          **Some inputs change their behavior for a sample, and `notes` \
                          tells you.** A kafka sample uses a temporary consumer group. \
                          Thus, it does not rebalance the group of the pipeline and does \
                          not commit offsets. An mqtt sample uses its own client id, \
                          because a broker disconnects the older client with the same id. \
                          An `http` input gives a 400, because clients post to it and \
                          there is nothing to read.\n\n\
                          **No messages gives a 200 with an empty list.** These inputs \
                          cannot read messages that were published before the sample \
                          started.",
            tag: Tag::Pipelines,
            access: Access::Admin,
            params: vec![],
            query: vec![],
            request: Some(RequestDoc {
                body: Body::Json("SampleRequest"),
                description: "The input to read from, and the limits of the read.",
            }),
            responses: vec![
                ResponseDoc {
                    status: 200,
                    description: "kayak took the sample. `outcome` tells you if it \
                                  contains messages or if it failed.",
                    body: Body::Json("SampleResponse"),
                },
                ResponseDoc {
                    status: 400,
                    description: "The request is not valid: the JSON is malformed, the \
                                  input type is unknown, or the input cannot be sampled.",
                    body: Body::Json("ApiError"),
                },
                server_error(),
            ],
        },
        ApiDoc {
            path: "/api/pipelines/dry-run",
            method: Method::Post,
            operation: Operation::DryRunPipeline,
            summary: "Run transforms over messages, without a pipeline",
            description: "Builds the transforms in the body as a pipeline does, and puts \
                          the messages through the chain. The response tells you **what \
                          each stage sent on**. Use it to see what a `map` writes, what a \
                          `filter` drops or what a `reduce` makes from a batch.\n\n\
                          The response gives a list of batches for each stage. For \
                          example, a `splitter` sends many batches, and a `filter` that \
                          dropped all messages sends none. A `buffer` sends nothing while \
                          it holds the messages. At the end, kayak drains the chain. The \
                          response gives what a transform releases then as `on_flush`. A \
                          transform that still holds messages after the drain sends \
                          nothing, and the response shows that. A dry run has no clock \
                          tick, so a window with 30 s left does not close.\n\n\
                          **A dry run has no outputs.** It changes no external system.\n\n\
                          The dry run **never uses live state**, as with a script dry run. \
                          The buckets are private to the request. `buckets` in the body \
                          fills them, the response returns them, and then kayak discards \
                          them.\n\n\
                          A transform that does not build, or that fails on a message, \
                          gives a 200 with `outcome` set to `failed`. The response also \
                          contains the stages that completed before the failure.",
            tag: Tag::Pipelines,
            access: Access::Admin,
            params: vec![],
            query: vec![],
            request: Some(RequestDoc {
                body: Body::Json("PipelineDryRunRequest"),
                description: "The messages, and the transforms to put them through.",
            }),
            responses: vec![
                ResponseDoc {
                    status: 200,
                    description: "The chain ran, or it failed. The `outcome` field tells \
                                  you which.",
                    body: Body::Json("PipelineDryRunResponse"),
                },
                ResponseDoc {
                    status: 400,
                    description: "The request is not valid: the JSON is malformed, or a \
                                  transform type is unknown.",
                    body: Body::Json("ApiError"),
                },
                server_error(),
            ],
        },
        ApiDoc {
            path: "/api/state",
            method: Method::Get,
            operation: Operation::ListStateBuckets,
            summary: "List the state buckets",
            description: "Returns one entry for each bucket under `state` in the config, \
                          in alphabetical order. Each entry has the number of keys in the \
                          bucket and the limits of the bucket.\n\n\
                          The API cannot make or remove buckets. You declare buckets in \
                          the config file. These endpoints are read-only.",
            tag: Tag::State,
            access: Access::Read,
            params: vec![],
            query: vec![],
            request: None,
            responses: vec![ResponseDoc {
                status: 200,
                description: "All declared buckets, in alphabetical order.",
                body: Body::Json("BucketSummary"),
            }],
        },
        ApiDoc {
            path: "/api/state/{bucket}",
            method: Method::Get,
            operation: Operation::GetStateBucket,
            summary: "Get the contents of a bucket",
            description: "Returns the keys and the values for each key. The key with the \
                          most recent write is first.\n\n\
                          The response has a limit. A bucket can hold thousands of keys, \
                          and the response returns one page of them. `truncated` tells you \
                          that the response is not complete, and `keys` gives the total. \
                          kayak takes the snapshot under the lock of the bucket. Thus, the \
                          snapshot is consistent, but it can be out of date when it \
                          arrives.",
            tag: Tag::State,
            access: Access::Read,
            params: vec![ParamDoc {
                name: "bucket",
                description: "The name of the bucket, as declared in the config.",
            }],
            query: vec![],
            request: None,
            responses: vec![
                ResponseDoc {
                    status: 200,
                    description: "The contents of the bucket.",
                    body: Body::Json("BucketContents"),
                },
                ResponseDoc {
                    status: 404,
                    description: "No bucket with that name is declared.",
                    body: Body::Json("ApiError"),
                },
            ],
        },
        ApiDoc {
            path: "/api/connections",
            method: Method::Post,
            operation: Operation::CreateConnection,
            summary: "Add a connection",
            description: "Adds a connection. A component reads its connection one time, \
                          when kayak builds the component. Thus, a new or changed \
                          connection has an effect only on new and rebuilt pipelines.\n\n\
                          This endpoint writes nothing to disk. A save writes the config \
                          file and the connections file together.",
            tag: Tag::Connections,
            access: Access::Admin,
            params: vec![],
            query: vec![],
            request: Some(RequestDoc {
                body: Body::Json("CreateConnectionRequest"),
                description: "The connection, and its name.",
            }),
            responses: vec![
                ResponseDoc {
                    status: 201,
                    description: "The connection is added. The body is the connection as \
                                  stored.",
                    body: Body::Json("CreateConnectionRequest"),
                },
                ResponseDoc {
                    status: 409,
                    description: "A connection with that name already exists.",
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
            description: "Removes a connection. If a running pipeline uses the connection, \
                          kayak does not remove it. The response is then a 409, and the \
                          body lists the pipelines.",
            tag: Tag::Connections,
            access: Access::Admin,
            params: vec![ParamDoc {
                name: "connection_id",
                description: "The name of the connection.",
            }],
            query: vec![],
            request: None,
            responses: vec![
                ResponseDoc {
                    status: 204,
                    description: "The connection is removed.",
                    body: Body::None,
                },
                not_found("No connection with that name exists."),
                ResponseDoc {
                    status: 409,
                    description: "Running pipelines use the connection. The body lists \
                                  them.",
                    body: Body::Json("ApiError"),
                },
                server_error(),
            ],
        },
        ApiDoc {
            path: "/api/settings",
            method: Method::Get,
            operation: Operation::GetSettings,
            summary: "Get the config state of the server",
            description: "Returns the config file of the server, the location where a save \
                          writes, and whether the running graph is different from the last \
                          load or save.\n\n\
                          If the server has no config file, you can still save. The save \
                          makes the file.",
            tag: Tag::Config,
            access: Access::Read,
            params: vec![],
            query: vec![],
            request: None,
            responses: vec![ResponseDoc {
                status: 200,
                description: "The config state of the server.",
                body: Body::Json("SettingsDto"),
            }],
        },
        ApiDoc {
            path: "/api/config/save",
            method: Method::Post,
            operation: Operation::SaveConfig,
            summary: "Write the running graph to a config file",
            description: "Writes the running pipelines to a config file, in a \
                          deterministic order: topological, then by id. kayak writes a \
                          temporary file and then renames it. The same save writes the \
                          connections file and the layout file beside the config file. A \
                          config file without its connections does not start.\n\n\
                          `name` must be a **file name with no path**. kayak writes the \
                          file only to the save directory of the server. To overwrite the \
                          loaded file, use its name.\n\n\
                          `format` selects JSON or YAML. Without `format`, the extension \
                          of the name sets the format. On a server started without \
                          `--config`, a save makes the config file. After that save, \
                          `revert` reloads this file.\n\n\
                          The default of `overwrite` is `true`. With `false`, the save \
                          only makes new files. If the file, or one of the two files \
                          beside it, exists, the response is a 409 and kayak writes \
                          nothing.",
            tag: Tag::Config,
            access: Access::Admin,
            params: vec![],
            query: vec![],
            request: Some(RequestDoc {
                body: Body::Json("SaveConfigRequest"),
                description: "The file name, the format (optional), and whether to replace \
                              an existing file.",
            }),
            responses: vec![
                ResponseDoc {
                    status: 200,
                    description: "The file is written. The body contains its path.",
                    body: Body::Json("SaveConfigResponse"),
                },
                ResponseDoc {
                    status: 409,
                    description: "`overwrite` is `false`, and the file or one of the two \
                                  files beside it exists. kayak wrote nothing. The message \
                                  names the files.",
                    body: Body::Json("ApiError"),
                },
                ResponseDoc {
                    status: 422,
                    description: "`name` is not a file name with no path.",
                    body: Body::Json("ApiError"),
                },
                server_error(),
            ],
        },
        ApiDoc {
            path: "/api/config/revert",
            method: Method::Post,
            operation: Operation::RevertConfig,
            summary: "Stop the running graph and reload the config file",
            description: "Stops all running pipelines and builds the graph again from the \
                          config file. Use it to discard the changes of a session. You \
                          cannot undo it.\n\n\
                          kayak parses the file **before** it stops the pipelines. If the \
                          file has an error, the running graph does not change. kayak \
                          reloads the connections first, because the pipelines use them. \
                          kayak waits for the old pipelines to stop before it builds the \
                          new ones. Thus, when the response arrives, only the new graph \
                          runs.",
            tag: Tag::Config,
            access: Access::Admin,
            params: vec![],
            query: vec![],
            request: None,
            responses: vec![
                ResponseDoc {
                    status: 204,
                    description: "The graph is reloaded and is the same as the file.",
                    body: Body::None,
                },
                ResponseDoc {
                    status: 500,
                    description: "There is no config file, or kayak cannot read or parse \
                                  it. The running graph does not change.",
                    body: Body::Json("ApiError"),
                },
            ],
        },
        ApiDoc {
            path: "/api/layout",
            method: Method::Get,
            operation: Operation::GetLayout,
            summary: "Get the layout of the web UI",
            description: "Returns the positions of the pipelines in the web UI. The layout \
                          is separate from `/api/pipelines`, because it does not change \
                          what the server runs. A client that ignores the layout gets an \
                          automatic layout.\n\n\
                          The layout contains only the pipelines that a user moved.",
            tag: Tag::Layout,
            access: Access::Read,
            params: vec![],
            query: vec![],
            request: None,
            responses: vec![ResponseDoc {
                status: 200,
                description: "The stored layout.",
                body: Body::Json("LayoutFile"),
            }],
        },
        ApiDoc {
            path: "/api/layout",
            method: Method::Put,
            operation: Operation::ReplaceLayout,
            summary: "Replace the layout and write it to disk",
            description: "Replaces the full layout. This is not a patch. To reset all \
                          positions to automatic, send `{}`.\n\n\
                          kayak writes the layout to disk immediately. A layout change is \
                          never an unsaved change, because it does not change what the \
                          server runs. Without a config file, kayak keeps the layout in \
                          memory until a save makes the file.",
            tag: Tag::Layout,
            access: Access::Admin,
            params: vec![],
            query: vec![],
            request: Some(RequestDoc {
                body: Body::Json("LayoutFile"),
                description: "The complete layout.",
            }),
            responses: vec![
                ResponseDoc {
                    status: 204,
                    description: "The layout is stored, and written to disk if there is a \
                                  config file.",
                    body: Body::None,
                },
                server_error(),
            ],
        },
        ApiDoc {
            path: "/events",
            method: Method::Get,
            operation: Operation::StreamEvents,
            summary: "Stream the activity of the pipelines",
            description: "A `text/event-stream` of `UiEvent`s. An event is a batch that \
                          arrives at a stage, or a failure. Each SSE `data:` field is one \
                          event as JSON.\n\n\
                          The stream is a broadcast. A slow client misses events, and the \
                          pipelines do not wait for it. A gap in `seq` shows the missed \
                          events. The run loops publish events only while a client \
                          listens.\n\n\
                          The stream is a tool for development. It is not a durable feed, \
                          and it can change.",
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
            summary: "Get the caller, and whether the server asks for credentials",
            description: "`authentication_required` tells you whether the server checks \
                          credentials. It is `false` on a server started without \
                          `--server-config`, or with `auth: {type: none}`. Such a server \
                          permits all operations to all callers.\n\n\
                          `username` and `role` describe the caller. Both are null for a \
                          caller with no credentials. A null `role` is different from \
                          `read`: a `read` user can see the graph, and a caller with no \
                          credentials cannot.\n\n\
                          This endpoint needs no credentials. A client uses it to decide \
                          whether to show a login page.",
            tag: Tag::Auth,
            access: Access::Public,
            params: vec![],
            query: vec![],
            request: None,
            responses: vec![ResponseDoc {
                status: 200,
                description: "The caller. This is not an error, also when the caller has \
                              no credentials.",
                body: Body::Json("AuthDto"),
            }],
        },
        ApiDoc {
            path: "/api/auth/login",
            method: Method::Post,
            operation: Operation::Login,
            summary: "Change credentials into a session",
            description: "Checks a username and a password against the accounts in the \
                          server settings file. If they are correct, the response sets an \
                          `HttpOnly` session cookie.\n\n\
                          This endpoint is for browsers. Other clients send \
                          `Authorization: Basic` with each request and do not use this \
                          endpoint. The cookie exists because `EventSource` cannot send \
                          headers, and the web UI reads `/events` with `EventSource`.\n\n\
                          A wrong password and an unknown username give the same 401. \
                          Thus, the endpoint does not tell a caller which accounts exist. \
                          On a server with no accounts, the response is a 200 with \
                          `authentication_required` set to false.",
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
                    description: "Signed in. The `Set-Cookie` header contains the session \
                                  cookie.",
                    body: Body::Json("AuthDto"),
                },
                ResponseDoc {
                    status: 401,
                    description: "The username or the password is wrong. The body does not \
                                  tell you which.",
                    body: Body::Json("ApiError"),
                },
                server_error(),
            ],
        },
        ApiDoc {
            path: "/api/auth/token",
            method: Method::Post,
            operation: Operation::TokenLogin,
            summary: "Change a JWT from an identity provider into a session",
            description: "Use this endpoint on a server with the `jwt` auth scheme, to \
                          embed kayak in a host application. The host has a token from an \
                          identity provider, for example Cognito or Keycloak. It puts the \
                          token on the iframe URL as `?auth_token=`, and the web UI posts \
                          it here one time.\n\n\
                          kayak checks the token against the published keys of the issuer. \
                          If the token is valid, the response sets the same `HttpOnly` \
                          session cookie as a password login. Thus, the token is in one \
                          request only, and not in later access logs.\n\n\
                          The session ends at the `exp` of the token, or earlier.\n\n\
                          API clients do not need this endpoint. On a `jwt` server, \
                          `Authorization: Bearer <token>` works on all endpoints.\n\n\
                          All refusals give the same 401. For example, an expired token, a \
                          wrong issuer and a server that does not accept tokens all give a \
                          401.",
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
                    description: "Signed in. The `Set-Cookie` header contains the session \
                                  cookie.",
                    body: Body::Json("AuthDto"),
                },
                ResponseDoc {
                    status: 401,
                    description: "kayak did not accept the token, or this server does not \
                                  accept tokens.",
                    body: Body::Json("ApiError"),
                },
                server_error(),
            ],
        },
        ApiDoc {
            path: "/api/auth/logout",
            method: Method::Post,
            operation: Operation::Logout,
            summary: "End the session of this request",
            description: "Clears the cookie in the browser and removes the session on the \
                          server. Thus, a copy of the cookie also stops working.\n\n\
                          The response is a 204, also when there was no session.",
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
            summary: "Get the component reference as data",
            description: "Returns all inputs, transforms, outputs and connections that \
                          kayak can build, with their fields, types and documentation. \
                          kayak generates the data from the config schemas. Thus, the data \
                          agrees with what the server accepts.\n\n\
                          The `/docs` page generates the same data in the browser. Use \
                          this endpoint for other tools, for example a config linter, \
                          editor completion or a test.",
            tag: Tag::Reference,
            access: Access::Public,
            params: vec![],
            query: vec![],
            request: None,
            responses: vec![ResponseDoc {
                status: 200,
                description: "All components. `family` gives the family of each component.",
                body: Body::JsonArray("ComponentDoc"),
            }],
        },
        ApiDoc {
            path: "/api/openapi.json",
            method: Method::Get,
            operation: Operation::GetOpenApi,
            summary: "Get this API as an OpenAPI 3.1 document",
            description: "kayak generates the document from the same table that registers \
                          the routes. The schemas come from the Rust types. Thus, the \
                          document describes the server that serves it.\n\n\
                          Use it with a renderer, a client generator or a contract test. \
                          `GET /api/reference` is a renderer on the same server.",
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
            summary: "Get the rendered API reference",
            description: "An HTML page that renders `/api/openapi.json`. It has a request \
                          panel to try the endpoints on this server.\n\n\
                          The `/docs` page in the web UI shows the same endpoints. This \
                          page is the full reference, with the schemas.",
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
    schemas.insert("LoadedScript", of(schema_for!(LoadedScript)));
    schemas.insert("PipelineSource", of(schema_for!(PipelineSource)));
    schemas.insert(
        "PipelineDryRunRequest",
        of(schema_for!(PipelineDryRunRequest)),
    );
    schemas.insert(
        "PipelineDryRunResponse",
        of(schema_for!(PipelineDryRunResponse)),
    );
    schemas.insert("SampleRequest", of(schema_for!(SampleRequest)));
    schemas.insert("SampleResponse", of(schema_for!(SampleResponse)));
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
                // the same level as `listPipelines`, which already hands an
                // inline script's code to any reader: a file script is the
                // same kind of thing kept somewhere else, and the path can only
                // reach what the running pipeline was built from
                ("getPipelineScript", "read"),
                // the same configs `listPipelines` already hands any reader,
                // in another spelling
                ("getPipelineConfig", "read"),
                ("listConnections", "read"),
                // executes code the caller supplied. It is sandboxed and its
                // state is a scratch bucket, so it cannot reach the running
                // graph — but "runs what you send it" is an operator's
                // capability whatever the sandbox does, and it is the same
                // capability `createPipeline` already grants. Never lower this
                // to `read`.
                ("dryRunScript", "admin"),
                // Sampling opens a connection to somebody's broker with the
                // server's own credentials and reads what is on it. That is a
                // capability, not a view of the graph — the same one
                // `createPipeline` grants, one message at a time. Never lower
                // this to `read`.
                ("sampleInput", "admin"),
                // Same capability as the script dry run: it builds and runs
                // real transforms, including one that can reach an http
                // endpoint. Never lower this to `read`.
                ("dryRunPipeline", "admin"),
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
