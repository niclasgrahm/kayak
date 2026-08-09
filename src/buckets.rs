//! The state buckets themselves — what `remember` writes into and `recall`
//! reads out of.
//!
//! The declaration is in `kayak_core::state`; this is the live thing. It is
//! **in-process and in-memory, and deliberately so.** The store is touched on
//! every message, which rules out anything with a network round trip in it; and
//! durability without checkpointed input positions would be worse than none,
//! because restoring a half-finished piece of work whose remaining messages were
//! never replayed produces an answer that is wrong in a way nothing downstream
//! can see. See "state" in the readme for the whole argument. A durable backend
//! is a later swap behind this same shape, not a different design.
//!
//! Two properties are load-bearing:
//!
//! - **Every bucket is bounded, and there is no way to ask for an unbounded
//!   one.** A keyed store with no limit is a leak with a week-long fuse. Both
//!   limits are enforced here rather than by the transforms, so a new stateful
//!   component cannot forget them.
//! - **Eviction is lazy — there is no sweeper task.** Expiry is applied when a
//!   bucket is touched, which is what lets this work at all: transforms are
//!   only ever driven by arriving batches and get no tick of their own. A
//!   bucket nothing touches holds its memory until something does, which is a
//!   bounded amount by the rule above.
//!
//! Locking follows the house rule: a `std::sync::Mutex` per bucket, taken and
//! released inside [`Buckets::with`], never held across an `.await`. `with`
//! takes a closure for exactly that reason — there is no way to get a guard out
//! of here and hold it over one.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, MutexGuard};
use std::time::Instant;

use kayak_core::state::{BucketContents, BucketEntry, BucketSummary, StateBucketConfig, StateBuckets};
use serde_json::Value;

/// One key's worth of remembered values.
struct Entry {
    values: BTreeMap<String, Value>,
    /// For expiry and for choosing what to evict. Monotonic, so a clock that
    /// steps backwards can't make an entry immortal.
    updated: Instant,
    /// The same moment as a wall clock, for showing in the UI. Kept beside the
    /// `Instant` rather than derived from it because the two answer different
    /// questions and neither can be turned into the other.
    updated_at: chrono::DateTime<chrono::Utc>,
}

/// One bucket's contents and the bounds on them.
struct Bucket {
    config: StateBucketConfig,
    entries: HashMap<String, Entry>,
}

impl Bucket {
    fn new(config: StateBucketConfig) -> Self {
        Self {
            config,
            entries: HashMap::new(),
        }
    }

    /// Whether an entry is old enough to count as gone.
    fn expired(&self, entry: &Entry, now: Instant) -> bool {
        self.config
            .idle_timeout()
            .is_some_and(|timeout| now.duration_since(entry.updated) >= timeout)
    }

    /// Drop what has timed out. Called before a write rather than on a timer —
    /// see the module docs on why there is no sweeper.
    fn expire(&mut self, now: Instant) {
        if self.config.idle_timeout().is_none() {
            return;
        }
        let stale: Vec<String> = self
            .entries
            .iter()
            .filter(|(_, entry)| self.expired(entry, now))
            .map(|(key, _)| key.clone())
            .collect();
        for key in stale {
            self.entries.remove(&key);
        }
    }

    /// Make room for one more key, by dropping the one written longest ago.
    ///
    /// Least *recently written* rather than least recently read: a bucket is a
    /// cache of what is currently active, and something nothing has written for
    /// an hour has stopped being active whether or not it is still being asked
    /// about.
    fn make_room(&mut self) {
        while self.entries.len() >= self.config.max_keys() {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.updated)
                .map(|(key, _)| key.clone())
            else {
                return;
            };
            self.entries.remove(&oldest);
        }
    }

    fn remember(&mut self, key: &str, values: impl IntoIterator<Item = (String, Value)>) {
        let now = Instant::now();
        self.expire(now);
        if !self.entries.contains_key(key) {
            self.make_room();
        }
        let entry = self.entries.entry(key.to_string()).or_insert_with(|| Entry {
            values: BTreeMap::new(),
            updated: now,
            updated_at: chrono::Utc::now(),
        });
        for (name, value) in values {
            entry.values.insert(name, value);
        }
        entry.updated = now;
        entry.updated_at = chrono::Utc::now();
    }

    /// One remembered value, or `None` when there is nothing under that key —
    /// including when what was there has expired but not yet been dropped.
    fn recall(&self, key: &str, name: &str) -> Option<&Value> {
        let entry = self.entries.get(key)?;
        if self.expired(entry, Instant::now()) {
            return None;
        }
        entry.values.get(name)
    }
}

/// How many entries one read of a bucket returns.
pub const MAX_ENTRIES_SHOWN: usize = 200;

/// The live buckets, by name.
///
/// The *set* of buckets is fixed at construction — they are declared in the
/// config, not created by running pipelines — which is what lets each one carry
/// its own lock with no map-wide one above it.
#[derive(Default)]
pub struct Buckets {
    inner: HashMap<String, Mutex<Bucket>>,
}

impl Buckets {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The buckets a config declares, empty.
    #[must_use]
    pub fn from_config(config: &StateBuckets) -> Self {
        Self {
            inner: config
                .iter()
                .map(|(name, bucket)| (name.clone(), Mutex::new(Bucket::new(bucket.clone()))))
                .collect(),
        }
    }

    /// The buckets a config declares, keeping what the previous set already
    /// held wherever the declaration is **unchanged**.
    ///
    /// This is what a revert calls, and the rule is the one that makes state
    /// survivable without being unaccountable: reloading the config rebuilds
    /// every pipeline, and resetting the machines someone has been watching for
    /// an hour because of an edit to an unrelated pipeline would be its own
    /// kind of data loss. A bucket whose *bounds* changed is a different bucket
    /// — its contents may not satisfy the new limits — so that one starts
    /// empty.
    #[must_use]
    pub fn rebuilt(&self, config: &StateBuckets) -> Self {
        let mut inner = HashMap::new();
        for (name, declared) in config.iter() {
            let carried = self
                .inner
                .get(name)
                .map(|bucket| Self::lock(bucket, name))
                .filter(|bucket| &bucket.config == declared)
                .map(|mut bucket| std::mem::take(&mut bucket.entries));
            inner.insert(
                name.clone(),
                Mutex::new(Bucket {
                    config: declared.clone(),
                    entries: carried.unwrap_or_default(),
                }),
            );
        }
        Self { inner }
    }

    /// A poisoned lock means a thread panicked while holding it. Nothing under
    /// it can leave a bucket half-updated, so recovering the guard is safe —
    /// the same rule `AppState`'s locks follow.
    fn lock<'a>(bucket: &'a Mutex<Bucket>, name: &str) -> MutexGuard<'a, Bucket> {
        bucket.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("the lock on state bucket '{name}' was poisoned; recovering");
            poisoned.into_inner()
        })
    }

    /// Do something with one bucket, holding its lock for exactly that long.
    ///
    /// A closure rather than a returned guard on purpose: there is then no way
    /// to hold the lock across an `.await`, which is the rule the rest of the
    /// runtime follows and the one that is easiest to break by accident.
    ///
    /// `None` when no bucket of that name is declared. Components resolve the
    /// name at build time, so at run time that means the config changed under a
    /// pipeline that is still running.
    fn with<R>(&self, name: &str, f: impl FnOnce(&mut Bucket) -> R) -> Option<R> {
        let bucket = self.inner.get(name)?;
        let mut guard = Self::lock(bucket, name);
        Some(f(&mut guard))
    }

    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.inner.contains_key(name)
    }

    /// Write values under a key. Silently does nothing for an undeclared
    /// bucket — see [`Buckets::with`].
    pub fn remember(&self, bucket: &str, key: &str, values: Vec<(String, Value)>) {
        self.with(bucket, |b| b.remember(key, values));
    }

    /// Read several names under one key, in the order asked for. A name with
    /// nothing under it comes back as `None` rather than being left out, so the
    /// caller can tell "not remembered" from "remembered as null" and can
    /// answer for each name separately.
    #[must_use]
    pub fn recall(&self, bucket: &str, key: &str, names: &[String]) -> Vec<Option<Value>> {
        self.with(bucket, |b| {
            names
                .iter()
                .map(|name| b.recall(key, name).cloned())
                .collect()
        })
        .unwrap_or_else(|| names.iter().map(|_| None).collect())
    }

    /// Every bucket, in name order, with what it is holding.
    #[must_use]
    pub fn summaries(&self) -> Vec<BucketSummary> {
        let mut out: Vec<BucketSummary> = self
            .inner
            .iter()
            .map(|(name, bucket)| {
                let bucket = Self::lock(bucket, name);
                BucketSummary {
                    name: name.clone(),
                    keys: bucket.entries.len(),
                    max_keys: bucket.config.max_keys(),
                    idle_timeout_secs: bucket.config.idle_timeout_secs,
                }
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// One bucket's contents, newest first and capped.
    ///
    /// Newest first because a bucket is watched to see what is *happening* —
    /// the key that just changed is the one worth showing — and capped because
    /// the cap is the whole reason the count is reported beside it.
    #[must_use]
    pub fn contents(&self, name: &str) -> Option<BucketContents> {
        self.with(name, |bucket| {
            let keys = bucket.entries.len();
            let mut entries: Vec<(&String, &Entry)> = bucket.entries.iter().collect();
            entries.sort_by(|(a_key, a), (b_key, b)| {
                b.updated.cmp(&a.updated).then_with(|| a_key.cmp(b_key))
            });
            let shown: Vec<BucketEntry> = entries
                .into_iter()
                .take(MAX_ENTRIES_SHOWN)
                .map(|(key, entry)| BucketEntry {
                    key: key.clone(),
                    values: entry.values.clone(),
                    updated_at: entry.updated_at.to_rfc3339(),
                })
                .collect();
            BucketContents {
                name: name.to_string(),
                keys,
                truncated: shown.len() < keys,
                entries: shown,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Buckets;
    use kayak_core::state::{StateBucketConfig, StateBuckets};
    use serde_json::{Value, json};

    fn buckets(config: StateBucketConfig) -> Buckets {
        let mut declared = StateBuckets::new();
        declared.insert("b", config);
        Buckets::from_config(&declared)
    }

    fn pairs(name: &str, value: Value) -> Vec<(String, Value)> {
        vec![(name.to_string(), value)]
    }

    #[test]
    fn what_was_remembered_can_be_recalled() {
        let buckets = buckets(StateBucketConfig::default());
        buckets.remember("b", "machine_1", pairs("unit_id", json!("u-7")));

        assert_eq!(
            buckets.recall("b", "machine_1", &["unit_id".to_string()]),
            vec![Some(json!("u-7"))]
        );
    }

    /// Keys are separate: that is the whole point of the key.
    #[test]
    fn one_key_does_not_see_anothers_values() {
        let buckets = buckets(StateBucketConfig::default());
        buckets.remember("b", "machine_1", pairs("unit_id", json!("u-7")));

        assert_eq!(
            buckets.recall("b", "machine_2", &["unit_id".to_string()]),
            vec![None]
        );
    }

    /// A name nothing has written is `None`, told apart from a name written as
    /// `null` — the caller answers for each separately.
    #[test]
    fn an_unwritten_name_is_told_apart_from_one_written_as_null() {
        let buckets = buckets(StateBucketConfig::default());
        buckets.remember("b", "k", pairs("written", json!(null)));

        assert_eq!(
            buckets.recall(
                "b",
                "k",
                &["written".to_string(), "never_written".to_string()]
            ),
            vec![Some(json!(null)), None]
        );
    }

    /// Remembering under a key that already has values adds to it rather than
    /// replacing it — a recipe and a unit id are written by two different
    /// transforms and both have to survive.
    #[test]
    fn a_second_write_merges_rather_than_replacing() {
        let buckets = buckets(StateBucketConfig::default());
        buckets.remember("b", "k", pairs("unit_id", json!("u-7")));
        buckets.remember("b", "k", pairs("recipe", json!("fast")));

        assert_eq!(
            buckets.recall("b", "k", &["unit_id".to_string(), "recipe".to_string()]),
            vec![Some(json!("u-7")), Some(json!("fast"))]
        );
    }

    #[test]
    fn writing_the_same_name_twice_keeps_the_newer_value() {
        let buckets = buckets(StateBucketConfig::default());
        buckets.remember("b", "k", pairs("unit_id", json!("u-7")));
        buckets.remember("b", "k", pairs("unit_id", json!("u-8")));

        assert_eq!(
            buckets.recall("b", "k", &["unit_id".to_string()]),
            vec![Some(json!("u-8"))]
        );
    }

    /// The bound is not advisory. Past it the least recently written key goes,
    /// which is what makes a bucket a cache of the active keys.
    #[test]
    fn past_max_keys_the_oldest_key_is_dropped() {
        let buckets = buckets(StateBucketConfig {
            max_keys: Some(2),
            idle_timeout_secs: None,
        });
        for key in ["a", "b", "c"] {
            // the resolution of `Instant` is fine enough that these order, but
            // the write below re-touches `a` to make the intent explicit
            buckets.remember("b", key, pairs("n", json!(key)));
        }

        assert_eq!(buckets.recall("b", "a", &["n".to_string()]), vec![None]);
        assert_eq!(
            buckets.recall("b", "c", &["n".to_string()]),
            vec![Some(json!("c"))]
        );
        let summary = buckets.summaries();
        assert_eq!(summary[0].keys, 2, "the bound is not exceeded");
    }

    /// Writing to a key that is already there must not evict anything — it
    /// needs no room made for it.
    #[test]
    fn updating_a_key_at_the_limit_evicts_nothing() {
        let buckets = buckets(StateBucketConfig {
            max_keys: Some(2),
            idle_timeout_secs: None,
        });
        buckets.remember("b", "a", pairs("n", json!(1)));
        buckets.remember("b", "b", pairs("n", json!(2)));
        buckets.remember("b", "a", pairs("n", json!(3)));

        assert_eq!(
            buckets.recall("b", "b", &["n".to_string()]),
            vec![Some(json!(2))],
            "'b' was evicted by an update that needed no room"
        );
    }

    /// Naming a bucket that isn't declared can't panic and can't invent a
    /// value: a running pipeline outliving its config is the case.
    #[test]
    fn an_undeclared_bucket_reads_as_empty_and_swallows_writes() {
        let buckets = Buckets::new();
        buckets.remember("nope", "k", pairs("n", json!(1)));
        assert_eq!(buckets.recall("nope", "k", &["n".to_string()]), vec![None]);
        assert!(!buckets.contains("nope"));
    }

    #[test]
    fn contents_report_the_keys_and_their_values() {
        let buckets = buckets(StateBucketConfig::default());
        buckets.remember("b", "machine_1", pairs("unit_id", json!("u-7")));

        let Some(contents) = buckets.contents("b") else {
            panic!("bucket 'b' is declared")
        };
        assert_eq!(contents.keys, 1);
        assert!(!contents.truncated);
        assert_eq!(contents.entries[0].key, "machine_1");
        assert_eq!(contents.entries[0].values["unit_id"], json!("u-7"));
        assert!(!contents.entries[0].updated_at.is_empty());
        assert!(buckets.contents("nope").is_none());
    }

    /// A revert rebuilds every pipeline; it must not empty the buckets someone
    /// has been watching fill for an hour.
    #[test]
    fn a_rebuild_keeps_what_an_unchanged_bucket_held() {
        let mut declared = StateBuckets::new();
        declared.insert("b", StateBucketConfig::default());
        let buckets = Buckets::from_config(&declared);
        buckets.remember("b", "k", pairs("n", json!(1)));

        let rebuilt = buckets.rebuilt(&declared);
        assert_eq!(
            rebuilt.recall("b", "k", &["n".to_string()]),
            vec![Some(json!(1))]
        );
    }

    /// ...but a bucket whose bounds changed is a different bucket: what it
    /// holds may not satisfy the new limits, so it starts empty.
    #[test]
    fn a_rebuild_empties_a_bucket_whose_declaration_changed() {
        let mut declared = StateBuckets::new();
        declared.insert("b", StateBucketConfig::default());
        let buckets = Buckets::from_config(&declared);
        buckets.remember("b", "k", pairs("n", json!(1)));

        let mut changed = StateBuckets::new();
        changed.insert(
            "b",
            StateBucketConfig {
                max_keys: Some(5),
                idle_timeout_secs: None,
            },
        );
        let rebuilt = buckets.rebuilt(&changed);
        assert_eq!(rebuilt.recall("b", "k", &["n".to_string()]), vec![None]);
    }

    /// A bucket taken out of the config goes, contents and all.
    #[test]
    fn a_rebuild_drops_a_bucket_that_is_no_longer_declared() {
        let mut declared = StateBuckets::new();
        declared.insert("b", StateBucketConfig::default());
        let buckets = Buckets::from_config(&declared);
        buckets.remember("b", "k", pairs("n", json!(1)));

        let rebuilt = buckets.rebuilt(&StateBuckets::new());
        assert!(!rebuilt.contains("b"));
        assert!(rebuilt.summaries().is_empty());
    }

    #[tokio::test]
    async fn an_expired_key_reads_as_absent() {
        let buckets = buckets(StateBucketConfig {
            max_keys: None,
            idle_timeout_secs: Some(1),
        });
        buckets.remember("b", "k", pairs("n", json!(1)));
        assert_eq!(
            buckets.recall("b", "k", &["n".to_string()]),
            vec![Some(json!(1))]
        );

        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        assert_eq!(
            buckets.recall("b", "k", &["n".to_string()]),
            vec![None],
            "a key past its idle timeout is gone"
        );

        // and the next write is what actually reclaims it
        buckets.remember("b", "other", pairs("n", json!(2)));
        assert_eq!(buckets.summaries()[0].keys, 1);
    }
}
