//! Checkpoints: exactly-once state and at-least-once delivery across a crash.
//!
//! `docs/durability.md` is the design and the argument; this is the
//! coordinator. In short: every `interval` an epoch is triggered; root
//! pipelines take a barrier between two passes, the rest receive it down their
//! edges; each run loop makes its outputs durable, snapshots what it holds and
//! hands that here together with the acknowledgements it has been holding; and
//! once every member of a **region** (a connected component of the graph, by
//! edges and by shared buckets) has submitted, the writer task commits the
//! region's snapshots and only then fires the acks.
//!
//! Three rules are load-bearing:
//!
//! - **Nothing here is on the hot path.** A run loop talks to the coordinator
//!   once per checkpoint, never per pass; the per-pass cost of checkpointing
//!   is [`PendingAcks::push`] and a `select!` arm on a `watch` receiver.
//! - **An ack never fires before the checkpoint that covers it is durable**,
//!   and an ack from an epoch that never completes is not dropped — it fires
//!   with the next epoch of its pipeline's region that does, since a later
//!   position covers an earlier one.
//! - **Locks**: [`Checkpoints::inner`] is a leaf except that a bucket's own
//!   lock may be taken *after* it, never before. Nothing awaits under it.

pub mod pending;
pub mod store;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::buckets::{BucketSnapshot, Buckets};
use crate::inputs::Ack;
use crate::inputs::ack::Barrier;
use crate::state::PipelineId;

pub use pending::PendingAcks;
pub use store::{Entry, Manifest, Store};

/// One component's part of a snapshot: the config it was built from, and
/// what it held.
///
/// The config travels with the state because it is what decides whether the
/// state can be restored at all: a transform whose config has changed is a
/// different transform, and handing it its predecessor's window would be
/// wrong in ways nothing downstream could see. Compared as JSON, at the same
/// position in the chain.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ComponentState {
    pub config: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<Value>,
}

/// One pipeline's snapshot: where each input had got to, and what each
/// transform and output was holding, all by position in the config.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PipelineSnapshot {
    #[serde(default)]
    pub inputs: Vec<ComponentState>,
    #[serde(default)]
    pub transforms: Vec<ComponentState>,
    #[serde(default)]
    pub outputs: Vec<ComponentState>,
}

impl PipelineSnapshot {
    /// Whether there is nothing in it to restore — no file is written for
    /// such a snapshot.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        [&self.inputs, &self.transforms, &self.outputs]
            .iter()
            .all(|list| list.iter().all(|c| c.state.is_none()))
    }

    fn find(list: &[ComponentState], index: usize, config: &Value) -> Option<Value> {
        list.get(index)
            .filter(|c| &c.config == config)
            .and_then(|c| c.state.clone())
    }

    /// The position input `index` had reached, if its config is unchanged.
    #[must_use]
    pub fn input(&self, index: usize, config: &Value) -> Option<Value> {
        Self::find(&self.inputs, index, config)
    }

    #[must_use]
    pub fn transform(&self, index: usize, config: &Value) -> Option<Value> {
        Self::find(&self.transforms, index, config)
    }

    #[must_use]
    pub fn output(&self, index: usize, config: &Value) -> Option<Value> {
        Self::find(&self.outputs, index, config)
    }
}

/// How a server checkpoints.
#[derive(Clone, Debug)]
pub struct Settings {
    pub dir: PathBuf,
    /// How often an epoch is triggered. `None` triggers none on a timer —
    /// only [`Checkpoints::trigger`] and the savepoints do, which is what the
    /// tests want.
    pub interval: Option<Duration>,
    /// How long an epoch may take before it is abandoned, and the longest a
    /// writer waits for the other writers of its bucket.
    pub timeout: Duration,
}

/// A running pipeline taking part in checkpoints.
#[derive(Clone, Debug)]
struct Member {
    token: u64,
    upstreams: Vec<PipelineId>,
    bucket: Option<String>,
    root: bool,
    /// The epoch that had been triggered when it joined. A root cannot take
    /// a barrier triggered before it was listening, so it is not waited on
    /// for that one.
    joined: u64,
    /// The latest barrier it has reached — what tells a writer waiting on a
    /// bucket that a co-writer has gone past the epoch without stopping at it.
    last_barrier: u64,
    /// `false` once its run loop has ended without stopping at a savepoint —
    /// crashed, or its input died. A dead member stays in its region and
    /// holds the region's checkpoints, exactly as a failed task holds a Flink
    /// job's: committing the others past it would move an upstream's
    /// position past batches it never snapshotted, and a restart would lose
    /// them. It leaves when it is rebuilt (a new member under its id) or
    /// deleted ([`Checkpoints::forget`]).
    alive: bool,
}

struct Submission {
    member: Member,
    /// `None` when the pipeline could not make its outputs durable: the
    /// epoch cannot complete for its region.
    snapshot: Option<PipelineSnapshot>,
    acks: PendingAcks,
}

enum BucketResult {
    /// One writer is copying it right now.
    Copying,
    /// Its write version had not moved since the last committed copy.
    Unchanged,
    Copied {
        snapshot: BucketSnapshot,
        instance: u64,
        version: u64,
    },
    /// A writer went past the epoch without stopping at it, or the wait
    /// timed out; the epoch cannot complete for this bucket's region.
    Failed,
}

struct Epoch {
    started: Instant,
    submitted: HashMap<PipelineId, Submission>,
    buckets: HashMap<String, BucketResult>,
    arrivals: HashMap<String, HashSet<PipelineId>>,
    /// Pipelines whose region has completed this epoch.
    done: HashSet<PipelineId>,
}

impl Epoch {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            submitted: HashMap::new(),
            buckets: HashMap::new(),
            arrivals: HashMap::new(),
            done: HashSet::new(),
        }
    }
}

#[derive(Default)]
struct Inner {
    /// The last epoch triggered.
    epoch: u64,
    /// No timer triggers while a savepoint is in progress — see
    /// [`Checkpoints::savepoint`].
    stopping: bool,
    next_token: u64,
    members: HashMap<PipelineId, Member>,
    epochs: BTreeMap<u64, Epoch>,
    /// Acks from epochs that were abandoned, waiting for the next completion
    /// of their pipeline's region.
    orphans: HashMap<PipelineId, PendingAcks>,
    /// The write version of each bucket's last committed copy.
    committed_buckets: HashMap<String, (u64, u64)>,
    /// The last epoch committed for each pipeline.
    completed: HashMap<PipelineId, u64>,
    /// The last epoch each pipeline submitted a snapshot for — what tells a
    /// savepoint that a pipeline which has left still has a commit coming.
    submitted: HashMap<PipelineId, u64>,
    /// Epochs a region failed to commit — for the status readout.
    failures: u64,
}

/// What the writer task is asked to do, in order.
enum Job {
    Commit {
        epoch: u64,
        pipelines: Vec<(PipelineId, PipelineSnapshot)>,
        buckets: Vec<(String, BucketSnapshot)>,
        acks: Vec<(PipelineId, PendingAcks)>,
    },
    Forget(PipelineId),
}

/// The checkpoint coordinator. One per server, shared by every run loop.
pub struct Checkpoints {
    settings: Settings,
    store: Arc<Store>,
    trigger: watch::Sender<Barrier>,
    inner: Mutex<Inner>,
    /// Bumped on every change a waiter might be waiting for: a bucket copied,
    /// a region committed, a member gone.
    changed: watch::Sender<u64>,
    /// The manifest as last committed.
    manifest: Mutex<Manifest>,
    writer: mpsc::UnboundedSender<Job>,
}

/// The connected components of the pipelines that take part in an epoch:
/// connected by an edge between two of them, or by both being bound to the
/// same bucket.
fn regions(nodes: &HashMap<PipelineId, Member>) -> Vec<BTreeSet<PipelineId>> {
    fn find(parent: &mut [usize], mut i: usize) -> usize {
        while parent[i] != i {
            parent[i] = parent[parent[i]];
            i = parent[i];
        }
        i
    }
    fn union(a: usize, b: usize, parent: &mut [usize]) {
        let (ra, rb) = (find(parent, a), find(parent, b));
        if ra != rb {
            parent[ra] = rb;
        }
    }
    let ids: Vec<&PipelineId> = nodes.keys().collect();
    let index: HashMap<&PipelineId, usize> = ids.iter().enumerate().map(|(i, id)| (*id, i)).collect();
    let mut parent: Vec<usize> = (0..ids.len()).collect();
    let mut by_bucket: HashMap<&str, usize> = HashMap::new();
    for (id, member) in nodes {
        let i = index[id];
        for upstream in &member.upstreams {
            if let Some(&j) = index.get(upstream) {
                union(i, j, &mut parent);
            }
        }
        if let Some(bucket) = &member.bucket {
            match by_bucket.get(bucket.as_str()) {
                Some(&j) => union(i, j, &mut parent),
                None => {
                    by_bucket.insert(bucket, i);
                }
            }
        }
    }
    let mut groups: HashMap<usize, BTreeSet<PipelineId>> = HashMap::new();
    for (i, id) in ids.iter().enumerate() {
        let root = find(&mut parent, i);
        groups.entry(root).or_default().insert((*id).clone());
    }
    groups.into_values().collect()
}

impl Checkpoints {
    /// Open the state directory and start the writer (and, with an interval,
    /// the timer). Needs a tokio runtime.
    pub fn open(settings: Settings) -> anyhow::Result<Arc<Self>> {
        let (store, manifest) = Store::open(&settings.dir)?;
        // Epochs carry on from the last process's, never restart: a snapshot
        // file is named by its epoch, and a new epoch 5 written over a file
        // the committed manifest still names as the old epoch 5 would, if the
        // process died before its own manifest landed, pair one pipeline's new
        // state with another's old.
        let resumed = manifest
            .pipelines
            .values()
            .chain(manifest.buckets.values())
            .map(|entry| entry.epoch)
            .max()
            .unwrap_or(0);
        let (writer, jobs) = mpsc::unbounded_channel();
        let checkpoints = Arc::new(Self {
            store: Arc::new(store),
            trigger: watch::Sender::new(Barrier {
                epoch: resumed,
                stop: false,
            }),
            inner: Mutex::new(Inner {
                epoch: resumed,
                ..Inner::default()
            }),
            changed: watch::Sender::new(0),
            manifest: Mutex::new(manifest),
            writer,
            settings,
        });
        tokio::spawn(write_jobs(Arc::downgrade(&checkpoints), jobs));
        if let Some(interval) = checkpoints.settings.interval {
            tokio::spawn(tick(Arc::downgrade(&checkpoints), interval));
        }
        Ok(checkpoints)
    }

    #[must_use]
    pub fn dir(&self) -> &std::path::Path {
        self.store.dir()
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("checkpoint coordinator lock was poisoned; recovering");
            poisoned.into_inner()
        })
    }

    fn lock_manifest(&self) -> MutexGuard<'_, Manifest> {
        self.manifest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn signal(&self) {
        self.changed.send_modify(|n| *n = n.wrapping_add(1));
    }

    /// The snapshot `id` should be restored from, if the last committed
    /// checkpoint has one. A file that cannot be read is logged and treated as
    /// absent: the pipeline then starts fresh rather than not at all.
    #[must_use]
    pub fn restored(&self, id: &str) -> Option<PipelineSnapshot> {
        let entry = self.lock_manifest().pipelines.get(id).cloned()?;
        match self.store.read_pipeline(&entry) {
            Ok(snapshot) => snapshot,
            Err(e) => {
                tracing::error!("[{id}]\t could not read its checkpoint, starting fresh: {e:#}");
                None
            }
        }
    }

    /// Fill `buckets` from the last committed checkpoint — every bucket the
    /// manifest names whose declaration is unchanged.
    pub fn restore_buckets(&self, buckets: &Buckets) {
        let entries: Vec<(String, Entry)> = self
            .lock_manifest()
            .buckets
            .iter()
            .map(|(name, entry)| (name.clone(), entry.clone()))
            .collect();
        for (name, entry) in entries {
            match self.store.read_bucket(&entry) {
                Ok(Some(snapshot)) => {
                    let keys = snapshot.entries.len();
                    if buckets.restore(&name, snapshot) {
                        tracing::info!("restored state bucket '{name}' ({keys} keys) from its checkpoint");
                    } else {
                        tracing::warn!(
                            "state bucket '{name}' was not restored: its declaration has changed \
                             since the checkpoint"
                        );
                    }
                }
                Ok(None) => {}
                Err(e) => tracing::error!("could not read the checkpoint of bucket '{name}': {e:#}"),
            }
        }
    }

    /// The epoch last committed for `id`.
    #[must_use]
    pub fn completed(&self, id: &str) -> Option<u64> {
        self.lock().completed.get(id).copied()
    }

    /// How many region commits have failed since the process started.
    #[must_use]
    pub fn failures(&self) -> u64 {
        self.lock().failures
    }

    /// Start checkpointing a pipeline. The returned [`Participant`] belongs to
    /// its run loop and takes it out of checkpoints again when dropped.
    #[must_use]
    pub fn join(
        self: &Arc<Self>,
        id: PipelineId,
        upstreams: Vec<PipelineId>,
        bucket: Option<String>,
        root: bool,
        buckets: Arc<Buckets>,
        positions: Vec<Option<Value>>,
    ) -> Participant {
        let token = {
            let mut inner = self.lock();
            inner.next_token += 1;
            let token = inner.next_token;
            let joined = inner.epoch;
            inner.members.insert(
                id.clone(),
                Member {
                    token,
                    upstreams,
                    bucket,
                    root,
                    joined,
                    last_barrier: joined,
                    alive: true,
                },
            );
            token
        };
        let trigger = root.then(|| {
            let mut rx = self.trigger.subscribe();
            rx.borrow_and_update();
            rx
        });
        Participant {
            checkpoints: Arc::clone(self),
            id,
            token,
            trigger,
            pending: PendingAcks::new(),
            positions,
            buckets,
            last_epoch: 0,
            stopped: false,
            waiting: None,
        }
    }

    /// A run loop has ended. `clean` when it stopped at a savepoint, having
    /// submitted it: then it leaves. Otherwise it stays, dead — see
    /// [`Member::alive`].
    fn leave(&self, id: &str, token: u64, clean: bool) {
        {
            let mut inner = self.lock();
            if let Some(member) = inner.members.get_mut(id).filter(|m| m.token == token) {
                if clean {
                    inner.members.remove(id);
                } else {
                    member.alive = false;
                    tracing::warn!(
                        "[{id}]\t stopped without a savepoint; the checkpoints of the pipelines \
                         connected to it are held until it is rebuilt or deleted, so none of them \
                         commits past what it never processed"
                    );
                }
            }
            // a member gone may complete a region that was only waiting on it
            let epochs: Vec<u64> = inner.epochs.keys().copied().collect();
            for epoch in epochs {
                self.try_complete(&mut inner, epoch);
            }
        }
        self.signal();
    }

    /// A deleted pipeline's checkpoint is dropped at the next commit, so a
    /// pipeline created later under the same id does not inherit it.
    pub fn forget(&self, id: &str) {
        // out of the members now, whatever the token, so nothing its run loop
        // submits on the way down can put the entry back after the forget
        self.lock().members.remove(id);
        let _ = self.writer.send(Job::Forget(id.to_string()));
        self.signal();
    }

    /// Trigger an epoch now and return it.
    pub fn trigger(&self) -> u64 {
        self.trigger_with(false)
    }

    fn trigger_with(&self, stop: bool) -> u64 {
        let epoch = {
            let mut inner = self.lock();
            self.abandon_stale(&mut inner);
            inner.epoch += 1;
            let epoch = inner.epoch;
            inner.epochs.insert(epoch, Epoch::new());
            epoch
        };
        self.trigger.send_replace(Barrier { epoch, stop });
        epoch
    }

    /// Abandon every epoch older than the timeout, keeping its acks for the
    /// next completion of their pipelines' regions.
    fn abandon_stale(&self, inner: &mut Inner) {
        let stale: Vec<u64> = inner
            .epochs
            .iter()
            .filter(|(_, e)| e.started.elapsed() >= self.settings.timeout)
            .map(|(n, _)| *n)
            .collect();
        for epoch in stale {
            let Some(abandoned) = inner.epochs.remove(&epoch) else {
                continue;
            };
            let incomplete = abandoned.submitted.len();
            for (id, submission) in abandoned.submitted {
                inner.orphans.entry(id).or_default().extend(submission.acks);
            }
            if incomplete > 0 {
                inner.failures += 1;
                tracing::warn!(
                    "checkpoint {epoch} did not complete within {}s and was abandoned",
                    self.settings.timeout.as_secs()
                );
            }
        }
    }

    /// Stop the graph at a consistent cut: trigger an epoch whose barrier
    /// also tells every run loop to stop once it has passed it, and wait for
    /// it to commit. Returns whether every pipeline that was running got
    /// there within `grace`; the caller cancels whatever did not.
    ///
    /// No timer triggers until [`Checkpoints::resume`] — a later epoch
    /// overtaking the stopping one would hand a root a barrier without the
    /// stop on it.
    pub async fn savepoint(&self, grace: Duration) -> bool {
        let members: Vec<PipelineId> = {
            let mut inner = self.lock();
            inner.stopping = true;
            inner
                .members
                .iter()
                .filter(|(_, m)| m.alive)
                .map(|(id, _)| id.clone())
                .collect()
        };
        if members.is_empty() {
            return true;
        }
        let epoch = self.trigger_with(true);
        let mut changed = self.changed.subscribe();
        let deadline = tokio::time::Instant::now() + grace;
        loop {
            changed.borrow_and_update();
            // committed, or gone without having anything to commit — a
            // pipeline whose input died before the barrier reached it. One
            // that stopped at the savepoint has *also* gone, and its commit
            // may still be on the writer's queue: that one is waited for.
            let finished = {
                let inner = self.lock();
                members.iter().all(|id| {
                    let gone = inner.members.get(id).is_none_or(|m| !m.alive);
                    inner.completed.get(id).is_some_and(|&e| e >= epoch)
                        || (gone && inner.submitted.get(id).is_none_or(|&e| e < epoch))
                })
            };
            if finished {
                return true;
            }
            tokio::select! {
                res = changed.changed() => if res.is_err() { return false },
                () = tokio::time::sleep_until(deadline) => return false,
            }
        }
    }

    /// Let the timer trigger again after a savepoint.
    pub fn resume(&self) {
        self.lock().stopping = false;
    }

    /// Wait until `id` has committed `epoch` or later. For tests and for the
    /// savepoint; `false` on a timeout.
    pub async fn wait_for(&self, id: &str, epoch: u64, timeout: Duration) -> bool {
        let mut changed = self.changed.subscribe();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            changed.borrow_and_update();
            if self.completed(id).is_some_and(|e| e >= epoch) {
                return true;
            }
            tokio::select! {
                res = changed.changed() => if res.is_err() { return false },
                () = tokio::time::sleep_until(deadline) => return false,
            }
        }
    }

    fn submit(
        &self,
        id: &str,
        token: u64,
        epoch: u64,
        snapshot: Option<PipelineSnapshot>,
        acks: PendingAcks,
    ) {
        {
            let mut inner = self.lock();
            let Some(member) = inner.members.get_mut(id).filter(|m| m.token == token) else {
                // a run loop whose pipeline has been deleted or replaced: its
                // state is gone, and so is any point in its acks
                return;
            };
            member.last_barrier = member.last_barrier.max(epoch);
            let member = member.clone();
            if snapshot.is_some() {
                let last = inner.submitted.entry(id.to_string()).or_insert(0);
                *last = (*last).max(epoch);
            }
            let Some(state) = inner.epochs.get_mut(&epoch) else {
                inner.orphans.entry(id.to_string()).or_default().extend(acks);
                return;
            };
            state.submitted.insert(
                id.to_string(),
                Submission {
                    member,
                    snapshot,
                    acks,
                },
            );
            self.try_complete(&mut inner, epoch);
        }
        self.signal();
    }

    /// Commit every region of `epoch` that now has everything it needs.
    fn try_complete(&self, inner: &mut Inner, epoch: u64) {
        let Some(state) = inner.epochs.get(&epoch) else {
            return;
        };
        // the members as they are now, plus whoever submitted and has since
        // left — a pipeline that stopped at a savepoint has submitted and
        // gone, and its snapshot must still be committed
        let mut nodes = inner.members.clone();
        for (id, submission) in &state.submitted {
            nodes.entry(id.clone()).or_insert_with(|| submission.member.clone());
        }
        let mut commits = Vec::new();
        for region in regions(&nodes) {
            if region.iter().any(|id| state.done.contains(id)) {
                continue;
            }
            let expected = region.iter().filter(|id| {
                state.submitted.contains_key(*id)
                    || nodes.get(*id).is_some_and(|m| !(m.root && m.joined >= epoch))
            });
            let mut complete = true;
            for id in expected {
                match state.submitted.get(id) {
                    Some(Submission {
                        snapshot: Some(_), ..
                    }) => {}
                    _ => complete = false,
                }
            }
            let buckets: BTreeSet<&String> = region
                .iter()
                .filter_map(|id| nodes.get(id).and_then(|m| m.bucket.as_ref()))
                .collect();
            for bucket in &buckets {
                if !matches!(
                    state.buckets.get(*bucket),
                    Some(BucketResult::Unchanged | BucketResult::Copied { .. })
                ) {
                    complete = false;
                }
            }
            if complete && !region.is_empty() {
                commits.push((region, buckets.into_iter().cloned().collect::<Vec<_>>()));
            }
        }
        for (region, buckets) in commits {
            self.commit_region(inner, epoch, &region, &buckets);
        }
    }

    fn commit_region(
        &self,
        inner: &mut Inner,
        epoch: u64,
        region: &BTreeSet<PipelineId>,
        buckets: &[String],
    ) {
        let mut pipelines = Vec::new();
        let mut acks: HashMap<PipelineId, PendingAcks> = HashMap::new();
        let mut copied = Vec::new();
        for (n, state) in inner.epochs.range_mut(..=epoch) {
            for id in region {
                if let Some(submission) = state.submitted.remove(id) {
                    acks.entry(id.clone()).or_default().extend(submission.acks);
                    if *n == epoch
                        && let Some(snapshot) = submission.snapshot
                    {
                        pipelines.push((id.clone(), snapshot));
                    }
                }
                state.done.insert(id.clone());
            }
        }
        if let Some(state) = inner.epochs.get_mut(&epoch) {
            for bucket in buckets {
                // left marked as decided, so a late look at it waits for
                // nothing
                if let Some(BucketResult::Copied {
                    snapshot,
                    instance,
                    version,
                }) = state.buckets.insert(bucket.clone(), BucketResult::Unchanged)
                {
                    inner
                        .committed_buckets
                        .insert(bucket.clone(), (instance, version));
                    copied.push((bucket.clone(), snapshot));
                }
            }
        }
        for id in region {
            if let Some(orphaned) = inner.orphans.remove(id) {
                acks.entry(id.clone()).or_default().extend(orphaned);
            }
        }
        // an epoch every remaining member is done with has nothing left to do
        let members: Vec<PipelineId> = inner.members.keys().cloned().collect();
        inner.epochs.retain(|n, state| {
            *n > epoch || !state.submitted.is_empty() || !members.iter().all(|m| state.done.contains(m))
        });
        let _ = self.writer.send(Job::Commit {
            epoch,
            pipelines,
            buckets: copied,
            acks: acks.into_iter().collect(),
        });
    }

    /// The writer half of a commit. Runs on the writer task, one job at a
    /// time, so commits land in the order regions completed.
    fn write(&self, job: Job) {
        match job {
            Job::Forget(id) => {
                let mut manifest = self.lock_manifest().clone();
                if manifest.pipelines.remove(&id).is_some() {
                    if let Err(e) = self.store.commit(&manifest) {
                        tracing::error!("could not drop '{id}' from the checkpoint: {e:#}");
                        return;
                    }
                    self.store.collect_garbage(&manifest);
                    *self.lock_manifest() = manifest;
                }
            }
            Job::Commit {
                epoch,
                pipelines,
                buckets,
                acks,
            } => {
                let committed: Vec<PipelineId> = pipelines.iter().map(|(id, _)| id.clone()).collect();
                match self.commit_files(epoch, &pipelines, &buckets) {
                    Ok(manifest) => {
                        self.store.collect_garbage(&manifest);
                        *self.lock_manifest() = manifest;
                        for (_, held) in acks {
                            held.fire();
                        }
                        let mut inner = self.lock();
                        for id in committed {
                            let last = inner.completed.entry(id).or_insert(0);
                            *last = (*last).max(epoch);
                        }
                    }
                    Err(e) => {
                        tracing::error!("checkpoint {epoch} could not be written: {e:#}");
                        let mut inner = self.lock();
                        inner.failures += 1;
                        // not durable, so not acknowledged: they wait for the
                        // next commit, which covers them
                        for (id, held) in acks {
                            inner.orphans.entry(id).or_default().extend(held);
                        }
                        // the copies were never written, so the next epoch
                        // must not take them as unchanged
                        for (name, _) in &buckets {
                            inner.committed_buckets.remove(name);
                        }
                    }
                }
                self.signal();
            }
        }
    }

    fn commit_files(
        &self,
        epoch: u64,
        pipelines: &[(PipelineId, PipelineSnapshot)],
        buckets: &[(String, BucketSnapshot)],
    ) -> anyhow::Result<Manifest> {
        let mut manifest = self.lock_manifest().clone();
        for (id, snapshot) in pipelines {
            let file = if snapshot.is_empty() {
                None
            } else {
                Some(self.store.write_pipeline(id, epoch, snapshot)?)
            };
            manifest.pipelines.insert(id.clone(), Entry { epoch, file });
        }
        for (name, snapshot) in buckets {
            let file = self.store.write_bucket(name, epoch, snapshot)?;
            manifest.buckets.insert(
                name.clone(),
                Entry {
                    epoch,
                    file: Some(file),
                },
            );
        }
        self.store.commit(&manifest)?;
        Ok(manifest)
    }

    /// One writer of `bucket` has reached `epoch`. Returns what the caller
    /// should do next.
    fn arrive(&self, id: &str, token: u64, bucket: &str, epoch: u64) -> Arrival {
        let mut inner = self.lock();
        if inner.members.get(id).is_none_or(|m| m.token != token) {
            return Arrival::Done;
        }
        let Some(state) = inner.epochs.get_mut(&epoch) else {
            return Arrival::Done;
        };
        state
            .arrivals
            .entry(bucket.to_string())
            .or_default()
            .insert(id.to_string());
        Self::decide_bucket(&mut inner, bucket, epoch)
    }

    /// Whether `bucket` can be copied for `epoch` yet: every writer currently
    /// bound to it has to have reached the epoch, and stopped there.
    fn decide_bucket(inner: &mut Inner, bucket: &str, epoch: u64) -> Arrival {
        let writers: Vec<(&PipelineId, &Member)> = inner
            .members
            .iter()
            .filter(|(_, m)| m.bucket.as_deref() == Some(bucket))
            .collect();
        let Some(state) = inner.epochs.get(&epoch) else {
            return Arrival::Done;
        };
        if state.buckets.contains_key(bucket) {
            return Arrival::Done;
        }
        // a dead writer will never arrive, and its region cannot commit this
        // epoch anyway; waiting the timeout out on it would only stall the
        // living ones
        if writers.iter().any(|(_, m)| !m.alive) {
            if let Some(state) = inner.epochs.get_mut(&epoch) {
                state.buckets.insert(bucket.to_string(), BucketResult::Failed);
            }
            return Arrival::Done;
        }
        if writers.iter().any(|(_, m)| m.last_barrier < epoch) {
            return Arrival::Wait;
        }
        let arrived = state.arrivals.get(bucket);
        let all_stopped = writers
            .iter()
            .all(|(id, _)| arrived.is_some_and(|a| a.contains(*id)));
        let result = if all_stopped {
            Arrival::Copy
        } else {
            Arrival::Done
        };
        let mark = if all_stopped {
            BucketResult::Copying
        } else {
            tracing::warn!(
                "a writer of state bucket '{bucket}' went past checkpoint {epoch} without stopping \
                 at it; that checkpoint is skipped for the bucket's region"
            );
            BucketResult::Failed
        };
        if let Some(state) = inner.epochs.get_mut(&epoch) {
            state.buckets.insert(bucket.to_string(), mark);
        }
        result
    }

    fn bucket_decided(&self, bucket: &str, epoch: u64) -> bool {
        self.lock()
            .epochs
            .get(&epoch)
            .is_none_or(|state| state.buckets.contains_key(bucket))
    }

    /// Record the copy (or the decision not to copy) of `bucket` at `epoch`.
    fn record_bucket(&self, bucket: &str, epoch: u64, buckets: &Buckets) {
        let committed = self.lock().committed_buckets.get(bucket).copied();
        let current = buckets.version(bucket).map(|v| (buckets.instance(), v));
        let result = if current.is_some() && current == committed {
            BucketResult::Unchanged
        } else {
            match buckets.snapshot(bucket) {
                Some((version, snapshot)) => BucketResult::Copied {
                    snapshot,
                    instance: buckets.instance(),
                    version,
                },
                None => BucketResult::Unchanged,
            }
        };
        {
            let mut inner = self.lock();
            if let Some(state) = inner.epochs.get_mut(&epoch) {
                state.buckets.insert(bucket.to_string(), result);
            }
            self.try_complete(&mut inner, epoch);
        }
        self.signal();
    }

    fn fail_bucket(&self, bucket: &str, epoch: u64) {
        {
            let mut inner = self.lock();
            if let Some(state) = inner.epochs.get_mut(&epoch) {
                state
                    .buckets
                    .entry(bucket.to_string())
                    .or_insert(BucketResult::Failed);
            }
        }
        self.signal();
    }
}

enum Arrival {
    /// Copy the bucket now: everyone else is waiting at the barrier.
    Copy,
    /// Someone else has (or will have) decided; wait for it.
    Wait,
    /// Already decided — nothing to do.
    Done,
}

/// The writer task: commits in order, off every run loop.
async fn write_jobs(checkpoints: Weak<Checkpoints>, mut jobs: mpsc::UnboundedReceiver<Job>) {
    while let Some(job) = jobs.recv().await {
        let Some(checkpoints) = checkpoints.upgrade() else {
            return;
        };
        // fsync is blocking IO; it must not hold a runtime worker
        let result = tokio::task::spawn_blocking(move || checkpoints.write(job)).await;
        if let Err(e) = result {
            tracing::error!("the checkpoint writer panicked: {e}");
        }
    }
}

/// The timer: one epoch per interval, for as long as the coordinator lives.
async fn tick(checkpoints: Weak<Checkpoints>, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let Some(checkpoints) = checkpoints.upgrade() else {
            return;
        };
        let stopping = {
            let mut inner = checkpoints.lock();
            checkpoints.abandon_stale(&mut inner);
            inner.stopping || !inner.members.values().any(|m| m.alive)
        };
        if !stopping {
            checkpoints.trigger();
        }
    }
}

/// One run loop's part in checkpointing: its trigger (if it is a root), the
/// acks it is holding, and where its inputs have got to. Owned by the run
/// loop; dropping it takes the pipeline out of checkpoints.
pub struct Participant {
    checkpoints: Arc<Checkpoints>,
    id: PipelineId,
    token: u64,
    trigger: Option<watch::Receiver<Barrier>>,
    pending: PendingAcks,
    positions: Vec<Option<Value>>,
    buckets: Arc<Buckets>,
    last_epoch: u64,
    /// Whether it has submitted a savepoint — what makes its leaving clean.
    stopped: bool,
    /// A wait on the trigger, kept across polls — see
    /// [`Participant::poll_trigger`].
    waiting: Option<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + Sync>>>,
}

impl Drop for Participant {
    fn drop(&mut self) {
        self.checkpoints.leave(&self.id, self.token, self.stopped);
    }
}

impl Participant {
    /// The next barrier the coordinator triggers, for a root. Never resolves
    /// for a pipeline whose barriers come down its edges. Cancel-safe.
    pub async fn next_trigger(&mut self) -> Barrier {
        std::future::poll_fn(|cx| self.poll_trigger(cx)).await
    }

    /// [`Participant::next_trigger`] as a poll, which is how the run loop asks:
    /// a `poll_fn` over this is one pointer wide, where an `async` wait on the
    /// trigger is a nested future the run loop's `select!` would move on
    /// every pass — measured, a tenth of a busy pipeline's throughput, paid
    /// whether or not anything was waiting.
    ///
    /// The wait itself is boxed once and kept across polls, on a clone of the
    /// trigger — so an idle root registers with the trigger once, not once
    /// per wakeup, and a busy one (whose input wins the `select!`) never
    /// creates it at all.
    pub fn poll_trigger(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Barrier> {
        loop {
            if let Some(barrier) = self.trigger_ready() {
                self.waiting = None;
                return std::task::Poll::Ready(barrier);
            }
            let Some(rx) = self.trigger.as_ref() else {
                return std::task::Poll::Pending;
            };
            // a coordinator that has gone away triggers nothing more, and a
            // wait on it would resolve at once, forever
            if rx.has_changed().is_err() {
                self.trigger = None;
                self.waiting = None;
                return std::task::Poll::Pending;
            }
            let waiting = self.waiting.get_or_insert_with(|| {
                let mut rx = rx.clone();
                Box::pin(async move {
                    let _ = rx.changed().await;
                })
            });
            match waiting.as_mut().poll(cx) {
                std::task::Poll::Ready(()) => self.waiting = None,
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
    }

    /// The barrier the coordinator has triggered since this root's last one,
    /// if there is one — without waiting, and without registering anything:
    /// an atomic load when nothing has changed. What the run loop asks between
    /// two passes, so that a busy pipeline never has to poll
    /// [`Participant::next_trigger`] at all.
    pub fn trigger_ready(&mut self) -> Option<Barrier> {
        let rx = self.trigger.as_mut()?;
        if !rx.has_changed().unwrap_or(false) {
            return None;
        }
        let barrier = *rx.borrow_and_update();
        (barrier.epoch > self.last_epoch).then_some(barrier)
    }

    /// Hold `ack` for the next checkpoint. Which input it came from is on the
    /// ack, when there is more than one — see
    /// [`crate::inputs::ack::FromInput`].
    pub fn defer(&mut self, ack: Box<dyn Ack>) {
        self.pending.push(0, ack);
    }

    /// Where every input has got to, with everything held so far folded in.
    pub fn positions(&mut self) -> Vec<Option<Value>> {
        self.pending.record_positions(&mut self.positions);
        self.positions.clone()
    }

    /// Hand this pipeline's snapshot for `barrier` to the coordinator, with
    /// every ack held since the last one. `None` says the pipeline could not
    /// make its outputs durable, and the epoch cannot complete for its
    /// region — the acks are still handed over, to fire with a later one.
    pub fn submit(&mut self, barrier: Barrier, snapshot: Option<PipelineSnapshot>) {
        self.pending.record_positions(&mut self.positions);
        self.last_epoch = self.last_epoch.max(barrier.epoch);
        self.stopped |= barrier.stop && snapshot.is_some();
        let acks = self.pending.take();
        self.checkpoints
            .submit(&self.id, self.token, barrier.epoch, snapshot, acks);
    }

    /// If this pipeline is bound to a bucket, wait until every writer of it
    /// has reached `epoch`, copying it if this is the last to arrive. Returns
    /// at once for a pipeline with no bucket, or a bucket with no other
    /// writer (which it then copies itself). Returns early when `cancel`
    /// fires, and gives up after the coordinator's timeout.
    pub async fn bucket_barrier(&self, bucket: Option<&str>, epoch: u64, cancel: &CancellationToken) {
        let Some(bucket) = bucket else {
            return;
        };
        let mut changed = self.checkpoints.changed.subscribe();
        let deadline = tokio::time::Instant::now() + self.checkpoints.settings.timeout;
        let mut arrival = self.checkpoints.arrive(&self.id, self.token, bucket, epoch);
        loop {
            match arrival {
                Arrival::Copy => {
                    self.checkpoints.record_bucket(bucket, epoch, &self.buckets);
                    return;
                }
                Arrival::Done => return,
                Arrival::Wait => {}
            }
            changed.borrow_and_update();
            if self.checkpoints.bucket_decided(bucket, epoch) {
                return;
            }
            tokio::select! {
                res = changed.changed() => if res.is_err() { return },
                () = cancel.cancelled() => return,
                () = tokio::time::sleep_until(deadline) => {
                    tracing::warn!(
                        "[{}]\t gave up waiting for the other writers of state bucket '{bucket}' at \
                         checkpoint {epoch}",
                        self.id
                    );
                    self.checkpoints.fail_bucket(bucket, epoch);
                    return;
                }
            }
            arrival = {
                let mut inner = self.checkpoints.lock();
                Checkpoints::decide_bucket(&mut inner, bucket, epoch)
            };
        }
    }

    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(upstreams: &[&str], bucket: Option<&str>) -> Member {
        Member {
            token: 0,
            upstreams: upstreams.iter().map(ToString::to_string).collect(),
            bucket: bucket.map(str::to_string),
            root: upstreams.is_empty(),
            joined: 0,
            last_barrier: 0,
            alive: true,
        }
    }

    fn sorted(mut regions: Vec<BTreeSet<PipelineId>>) -> Vec<Vec<String>> {
        let mut out: Vec<Vec<String>> = regions
            .drain(..)
            .map(|r| r.into_iter().collect())
            .collect();
        out.sort();
        out
    }

    /// Regions are what keep one stuck pipeline from holding up everyone's
    /// checkpoints: pipelines are in one region only if an edge or a shared
    /// bucket connects them.
    #[test]
    fn regions_are_connected_by_edges_and_shared_buckets() {
        let nodes: HashMap<PipelineId, Member> = [
            ("a", member(&[], None)),
            ("b", member(&["a"], None)),
            ("c", member(&[], Some("recipes"))),
            ("d", member(&[], Some("recipes"))),
            ("e", member(&[], None)),
        ]
        .into_iter()
        .map(|(id, m)| (id.to_string(), m))
        .collect();
        assert_eq!(
            sorted(regions(&nodes)),
            vec![
                vec!["a".to_string(), "b".to_string()],
                vec!["c".to_string(), "d".to_string()],
                vec!["e".to_string()],
            ]
        );
    }

    /// An edge to a pipeline that is not taking part (deleted, never built)
    /// connects nothing.
    #[test]
    fn an_edge_to_an_absent_pipeline_connects_nothing() {
        let nodes: HashMap<PipelineId, Member> =
            [("b".to_string(), member(&["gone"], None))].into_iter().collect();
        assert_eq!(sorted(regions(&nodes)), vec![vec!["b".to_string()]]);
    }

    #[test]
    fn an_empty_snapshot_is_one_with_no_state_anywhere() {
        let mut snapshot = PipelineSnapshot {
            transforms: vec![ComponentState {
                config: serde_json::json!({"type": "filter"}),
                state: None,
            }],
            ..PipelineSnapshot::default()
        };
        assert!(snapshot.is_empty());
        snapshot.inputs.push(ComponentState {
            config: Value::Null,
            state: Some(serde_json::json!({"offset": 1})),
        });
        assert!(!snapshot.is_empty());
    }

    /// State is restored only to a component whose config is unchanged, at
    /// the same position.
    #[test]
    fn state_is_restored_only_to_an_unchanged_component() {
        let config = serde_json::json!({"type": "buffer", "size": 3});
        let snapshot = PipelineSnapshot {
            transforms: vec![ComponentState {
                config: config.clone(),
                state: Some(serde_json::json!([1, 2])),
            }],
            ..PipelineSnapshot::default()
        };
        assert_eq!(snapshot.transform(0, &config), Some(serde_json::json!([1, 2])));
        assert_eq!(
            snapshot.transform(0, &serde_json::json!({"type": "buffer", "size": 4})),
            None
        );
        assert_eq!(snapshot.transform(1, &config), None);
    }
}
