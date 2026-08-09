//! Named state buckets: what a pipeline can remember between batches.
//!
//! Buckets are **global and named**, like connections rather than like a
//! pipeline's transforms — declared once at the top of the config and referred
//! to by the pipelines that use them. That is what lets a slow-moving reference
//! stream (a recipe per machine, a device registry) be written by one pipeline
//! and read by several without the fact being copied into each of them.
//!
//! What it costs is worth knowing before reaching for it. **Two pipelines
//! sharing a bucket have no ordering between them**: they are separate run
//! loops, so a reader can see the value from before or after a given write
//! depending on nothing it can observe. The rule that follows is not enforced
//! and has to be understood — *ordering-sensitive correlation belongs in one
//! pipeline; sharing is for state whose value doesn't change on the timescale
//! of a message.* A recipe that updates hourly is safe to share. A unit id that
//! changes every cycle is not.
//!
//! The whole section is optional: a config with no `state` key is a config with
//! no buckets, which is every config written before this existed.
//!
//! These types are the *declaration*. The store itself is in the root crate —
//! this crate compiles to wasm and holds no runtime.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// How many keys a bucket holds when its config doesn't say.
///
/// There is a default at all — rather than "unbounded" — because an unbounded
/// keyed store is a leak with a slow fuse: one key per machine is fine, one key
/// per request id is a server that dies next week. A number that is generous
/// for the first case and obviously wrong for the second is the useful default.
pub const DEFAULT_MAX_KEYS: usize = 10_000;

/// One named bucket, and the bounds on it.
///
/// Both bounds have defaults and neither can be turned off. A keyed store with
/// no limit is not a feature, it is a memory leak that takes a week to show up
/// — so the question is only ever *what* the limits are.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "state bucket")]
pub struct StateBucketConfig {
    /// most keys to hold at once. Past this the least recently written key is
    /// dropped to make room, so a bucket is a cache of the *active* keys rather
    /// than a record of every key ever seen. Defaults to 10000.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_keys: Option<usize>,
    /// forget a key this many seconds after it was last written. Without it a
    /// machine that is decommissioned holds its slot until the bucket fills.
    ///
    /// Measured from the last write rather than the last read: a value nothing
    /// has written for an hour is stale whether or not something is still
    /// asking for it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_timeout_secs: Option<u64>,
}

impl StateBucketConfig {
    #[must_use]
    pub fn max_keys(&self) -> usize {
        // zero can only mean "hold nothing", which is never what someone meant
        // by writing it — the same reading `batch_cap` gives a zero batch
        self.max_keys.unwrap_or(DEFAULT_MAX_KEYS).max(1)
    }

    #[must_use]
    pub fn idle_timeout(&self) -> Option<std::time::Duration> {
        self.idle_timeout_secs
            .filter(|secs| *secs > 0)
            .map(std::time::Duration::from_secs)
    }
}

/// The buckets a config declares, by name.
///
/// A `BTreeMap` newtype for the reason [`crate::connections::Connections`] is
/// one: iteration order is the name order, so the file a save writes is
/// deterministic and diffs cleanly.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(transparent)]
pub struct StateBuckets(BTreeMap<String, StateBucketConfig>);

impl StateBuckets {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&StateBucketConfig> {
        self.0.get(name)
    }

    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    pub fn insert(&mut self, name: impl Into<String>, config: StateBucketConfig) {
        self.0.insert(name.into(), config);
    }

    pub fn remove(&mut self, name: &str) -> Option<StateBucketConfig> {
        self.0.remove(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &StateBucketConfig)> {
        self.0.iter()
    }

    #[must_use]
    pub fn names(&self) -> Vec<&String> {
        self.0.keys().collect()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl FromIterator<(String, StateBucketConfig)> for StateBuckets {
    fn from_iter<I: IntoIterator<Item = (String, StateBucketConfig)>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

/// A pipeline's binding to a bucket: which one, and what its messages are keyed
/// by.
///
/// The key lives here rather than on the bucket because it is a property of
/// *this stream* — the same machine id arrives as `_meta.machine_id` from a
/// nats subscription and as `machine_id` after a reducer has flattened it, and
/// both are correct. The cost is that two pipelines sharing a bucket can key it
/// differently with nothing to catch them, which is the sharp edge of sharing
/// and is documented rather than prevented.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "pipeline state")]
pub struct PipelineState {
    /// name of the bucket this pipeline reads and writes — one of the ones
    /// declared under `state` at the top of the config. A pipeline naming a
    /// bucket that isn't declared fails to build.
    pub bucket: String,
    /// the field whose value identifies the thing being remembered, e.g.
    /// `_meta.machine_id`. A dotted path like anywhere else.
    ///
    /// Leave it out for one bucket-wide value — which is the right answer for
    /// something there is only ever one of, and the wrong one for anything
    /// per-device.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

/// One bucket as the API reports it.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BucketSummary {
    pub name: String,
    /// How many keys it is holding right now, expired ones included — they are
    /// dropped on the next write, and reporting a number that doesn't match
    /// what is in memory would make the readout useless for the one thing it is
    /// for.
    pub keys: usize,
    pub max_keys: usize,
    pub idle_timeout_secs: Option<u64>,
}

/// One key's contents as the API reports them.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BucketEntry {
    pub key: String,
    pub values: BTreeMap<String, Value>,
    pub updated_at: String,
}

/// What a bucket holds, for the UI's card.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BucketContents {
    pub name: String,
    pub keys: usize,
    pub entries: Vec<BucketEntry>,
    /// Whether `entries` is short of `keys` because of the cap. A bucket can
    /// hold ten thousand keys and the card shows a page of them; saying so is
    /// what stops the card reading as the whole truth.
    pub truncated: bool,
}

/// A whole config file: the buckets, and the pipelines.
///
/// **Two spellings, and both are permanent.** A file that is a bare array of
/// pipelines is what every config written before buckets existed looks like,
/// and it stays valid and stays the form a save writes when there are no
/// buckets to declare — so adding this feature changed no existing file by a
/// byte. A file with buckets is a document with `state` and `pipelines` keys.
///
/// ```yaml
/// state:
///   machine_state:
///     max_keys: 5000
/// pipelines:
///   - id: machine_cycles
///     ...
/// ```
///
/// The choice is made by [`ConfigFile::render_as_document`] rather than
/// remembered: nothing past the parser knows which spelling a file used, the
/// same way nothing past it knows whether the file was JSON or YAML.
#[derive(Clone, Debug, Default)]
pub struct ConfigFile {
    pub state: StateBuckets,
    pub pipelines: Vec<crate::config::Config>,
}

impl ConfigFile {
    #[must_use]
    pub fn new(state: StateBuckets, pipelines: Vec<crate::config::Config>) -> Self {
        Self { state, pipelines }
    }

    /// Pipelines and nothing else — what a caller with no interest in buckets
    /// builds, and what the bare-array spelling parses to.
    #[must_use]
    pub fn of_pipelines(pipelines: Vec<crate::config::Config>) -> Self {
        Self {
            state: StateBuckets::new(),
            pipelines,
        }
    }

    /// Whether this has to be written as a document rather than a bare array.
    ///
    /// Only buckets force it, which is what keeps a config that doesn't use
    /// them byte-identical to what it always was.
    #[must_use]
    pub fn render_as_document(&self) -> bool {
        !self.state.is_empty()
    }
}

/// The two spellings, for serde to try in order. Arrays and maps can't be
/// confused for one another, so the untagged match is unambiguous rather than
/// merely lucky.
#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum ConfigFileWire {
    Bare(Vec<crate::config::Config>),
    Document {
        #[serde(default)]
        state: StateBuckets,
        pipelines: Vec<crate::config::Config>,
    },
}

impl<'de> Deserialize<'de> for ConfigFile {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(match ConfigFileWire::deserialize(deserializer)? {
            ConfigFileWire::Bare(pipelines) => Self::of_pipelines(pipelines),
            ConfigFileWire::Document { state, pipelines } => Self { state, pipelines },
        })
    }
}

impl Serialize for ConfigFile {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.render_as_document() {
            ConfigFileWire::Document {
                state: self.state.clone(),
                pipelines: self.pipelines.clone(),
            }
            .serialize(serializer)
        } else {
            self.pipelines.serialize(serializer)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ConfigFile, DEFAULT_MAX_KEYS, StateBucketConfig, StateBuckets};

    #[test]
    fn an_unconfigured_bucket_is_still_bounded() {
        let bucket = StateBucketConfig::default();
        assert_eq!(bucket.max_keys(), DEFAULT_MAX_KEYS);
        assert_eq!(bucket.idle_timeout(), None);
    }

    /// Zero keys can only mean "hold nothing", which nobody means; and a zero
    /// timeout would forget every key before it could be read.
    #[test]
    fn zero_bounds_read_as_the_thing_that_was_obviously_meant() {
        let bucket = StateBucketConfig {
            max_keys: Some(0),
            idle_timeout_secs: Some(0),
        };
        assert_eq!(bucket.max_keys(), 1);
        assert_eq!(bucket.idle_timeout(), None);
    }

    #[test]
    fn buckets_iterate_in_name_order_whatever_they_were_added_in() {
        let mut buckets = StateBuckets::new();
        buckets.insert("zebra", StateBucketConfig::default());
        buckets.insert("alpha", StateBucketConfig::default());
        assert_eq!(buckets.names(), vec!["alpha", "zebra"]);
    }


    /// The spelling every config written before buckets existed uses. It stays
    /// valid, and it stays what a save writes when there is nothing to declare.
    #[test]
    fn a_bare_array_of_pipelines_is_a_whole_config_file() -> Result<(), serde_json::Error> {
        let file: ConfigFile = serde_json::from_str(
            r#"[{"id":"p1","inputs":[],"transforms":[],"outputs":[]}]"#,
        )?;
        assert!(file.state.is_empty());
        assert_eq!(file.pipelines.len(), 1);
        assert!(!file.render_as_document());
        // and back out the way it came in, not as a document
        assert!(serde_json::to_string(&file)?.starts_with('['));
        Ok(())
    }

    #[test]
    fn buckets_make_it_a_document_with_two_keys() -> Result<(), serde_json::Error> {
        let file: ConfigFile = serde_json::from_str(
            r#"{"state":{"machines":{"max_keys":5}},
                "pipelines":[{"id":"p1","inputs":[],"transforms":[],"outputs":[]}]}"#,
        )?;
        assert_eq!(file.state.len(), 1);
        assert_eq!(file.state.get("machines").map(|b| b.max_keys()), Some(5));
        assert!(file.render_as_document());

        let rendered = serde_json::to_value(&file)?;
        assert!(rendered.get("state").is_some());
        assert!(rendered.get("pipelines").is_some());
        Ok(())
    }

    /// A document that declares no buckets is still a document on the way in —
    /// it just isn't one on the way back out, since there is nothing to say.
    #[test]
    fn a_document_without_a_state_key_is_valid() -> Result<(), serde_json::Error> {
        let file: ConfigFile = serde_json::from_str(
            r#"{"pipelines":[{"id":"p1","inputs":[],"transforms":[],"outputs":[]}]}"#,
        )?;
        assert!(file.state.is_empty());
        assert_eq!(file.pipelines.len(), 1);
        assert!(serde_json::to_string(&file)?.starts_with('['));
        Ok(())
    }
}
