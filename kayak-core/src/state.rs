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

/// A named state bucket and its limits.
///
/// Both limits have a default. You cannot remove a limit, so the memory that a
/// bucket uses is always limited.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "state bucket")]
pub struct StateBucketConfig {
    /// The maximum number of keys in the bucket. When the bucket is full, kayak
    /// removes the key with the oldest write. The default is 10000. The value
    /// must be more than zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_keys: Option<usize>,
    /// Remove a key when this number of seconds went by after its last write.
    /// A read does not reset the time. The value must be more than zero.
    ///
    /// Without it, a key stays until the bucket is full. For example, a
    /// machine that you remove from service keeps its key.
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

/// The state buckets of a config file, as an object from name to bucket.
/// kayak writes the names in alphabetical order.
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

/// The state bucket of a pipeline, and the field that gives the key of each
/// message.
///
/// Each pipeline sets its own key, because the same value can have different
/// field names in two streams. For example, one stream has
/// `_meta.machine_id` and another stream has `machine_id`.
///
/// Make sure that all pipelines that share a bucket use keys with the same
/// values. kayak does not check this.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "pipeline state")]
pub struct PipelineState {
    /// The name of the bucket that this pipeline reads and writes. Declare the
    /// bucket under `state` at the top of the config. If the bucket is not
    /// declared, the pipeline does not build.
    pub bucket: String,
    /// The field that gives the key, as a dotted path. For example,
    /// `_meta.machine_id`.
    ///
    /// Leave it out for one value for the full bucket. Use that only for an
    /// item that has one value. For a value for each device, set `key`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

/// One bucket as the API reports it.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BucketSummary {
    pub name: String,
    /// The number of keys in the bucket now. The number includes expired keys.
    /// kayak removes them on the next write.
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

/// The contents of a bucket.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BucketContents {
    pub name: String,
    pub keys: usize,
    pub entries: Vec<BucketEntry>,
    /// True when `entries` has fewer entries than `keys`, because the response
    /// has a limit.
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
