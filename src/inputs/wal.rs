//! The `http` input's write-ahead log — see "the http input's write-ahead log"
//! in `docs/durability.md`.
//!
//! Every other rewindable input has somewhere upstream that keeps the messages
//! (a kafka topic, a database table). A post does not: the client was told
//! `202`, and nothing but this process ever held the message. So under
//! checkpointing a post is appended here and fsynced **before** the `202` goes
//! out, and the input's position is a record's offset. Restore replays every
//! record from the checkpointed offset on; a committed checkpoint truncates the
//! segments wholly behind it.
//!
//! ```text
//! <state-dir>/http/<hex id>/<first offset, 20 digits>.log
//! ```
//!
//! One record per post, one line of JSON each. A torn last line — the process
//! died mid-write — is cut off when the log is opened: its post was never
//! fsynced, so it was never acknowledged, and its client has its 5xx to retry
//! on.
//!
//! Group commit is what makes the fsync affordable: the writer takes every post
//! that arrived while the previous fsync was running and covers them with one.

use std::collections::VecDeque;
use std::io::{BufRead, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::inputs::Ack;
use crate::inputs::http::PostMeta;

/// A segment is closed and the next one started past this size. Truncation
/// works in whole segments, so this is also how much log a committed
/// checkpoint can leave behind.
pub const SEGMENT_BYTES: u64 = 16 * 1024 * 1024;

/// One post, as the log holds it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    #[serde(rename = "o")]
    pub offset: u64,
    #[serde(rename = "m")]
    pub messages: Vec<Value>,
    #[serde(rename = "p", default)]
    pub meta: PostMeta,
}

/// Where a log for `pipeline` lives under the state directory. Hex, for the
/// reason snapshot files are: an id never picks a path.
#[must_use]
pub fn dir_for(state_dir: &Path, pipeline: &str) -> PathBuf {
    use std::fmt::Write as _;
    let hex = pipeline.bytes().fold(String::new(), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    });
    state_dir.join("http").join(hex)
}

/// Remove a deleted pipeline's log, so a pipeline created later under the
/// same id does not replay a stranger's posts.
pub fn remove(state_dir: &Path, pipeline: &str) {
    let dir = dir_for(state_dir, pipeline);
    if let Err(e) = std::fs::remove_dir_all(&dir)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!("could not remove the http log {}: {e}", dir.display());
    }
}

/// The segments, shared between the writer and the acks that truncate.
#[derive(Debug)]
pub struct Wal {
    dir: PathBuf,
    /// `(first offset, path)`, oldest first. The last is the one being
    /// written and is never removed.
    segments: Mutex<Vec<(u64, PathBuf)>>,
}

impl Wal {
    fn lock(&self) -> MutexGuard<'_, Vec<(u64, PathBuf)>> {
        self.segments
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Remove every segment all of whose records are before `offset` — what a
    /// committed checkpoint whose position is `offset` makes safe. A segment
    /// qualifies when the *next* one starts at or before `offset`.
    pub fn truncate_before(&self, offset: u64) {
        let doomed: Vec<PathBuf> = {
            let mut segments = self.lock();
            let keep_from = segments
                .windows(2)
                .take_while(|pair| pair[1].0 <= offset)
                .count();
            segments.drain(..keep_from).map(|(_, path)| path).collect()
        };
        for path in doomed {
            if let Err(e) = std::fs::remove_file(&path) {
                tracing::warn!("could not remove http log segment {}: {e}", path.display());
            }
        }
    }

    /// How many segments the log holds. For tests.
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.lock().len()
    }

    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

/// The appending half. Owned by the writer task.
pub struct Writer {
    wal: Arc<Wal>,
    file: std::fs::File,
    written: u64,
    next: u64,
    segment_bytes: u64,
}

fn segment_path(dir: &Path, first: u64) -> PathBuf {
    dir.join(format!("{first:020}.log"))
}

/// Open (creating if need be) the log in `dir`: the shared half, the writer,
/// and every record it holds, oldest first, for the caller to replay what a
/// checkpoint has not covered.
pub fn open(dir: &Path) -> anyhow::Result<(Arc<Wal>, Writer, VecDeque<Record>)> {
    open_with(dir, SEGMENT_BYTES)
}

/// As [`open`], with the segment size named — tests use a tiny one.
pub fn open_with(
    dir: &Path,
    segment_bytes: u64,
) -> anyhow::Result<(Arc<Wal>, Writer, VecDeque<Record>)> {
    std::fs::create_dir_all(dir)
        .with_context(|| format!("failed to create the http log {}", dir.display()))?;
    let mut segments: Vec<(u64, PathBuf)> = std::fs::read_dir(dir)
        .with_context(|| format!("failed to list the http log {}", dir.display()))?
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();
            let first = path.file_stem()?.to_str()?.parse::<u64>().ok()?;
            (path.extension()? == "log").then_some((first, path))
        })
        .collect();
    segments.sort();

    let mut records = VecDeque::new();
    let mut next = segments.first().map_or(0, |(first, _)| *first);
    let last = segments.len().saturating_sub(1);
    for (index, (_, path)) in segments.iter().enumerate() {
        let file = std::fs::File::open(path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        let mut reader = std::io::BufReader::new(file);
        let mut good = 0u64;
        let mut line = String::new();
        loop {
            line.clear();
            let read = reader
                .read_line(&mut line)
                .with_context(|| format!("failed to read {}", path.display()))?;
            if read == 0 {
                break;
            }
            match serde_json::from_str::<Record>(line.trim_end()) {
                Ok(record) if line.ends_with('\n') => {
                    next = record.offset + 1;
                    records.push_back(record);
                    good += read as u64;
                }
                // a torn write: only ever the tail of the newest segment
                _ if index == last => {
                    tracing::warn!(
                        "cutting a torn record off the end of {}: it was never acknowledged",
                        path.display()
                    );
                    let file = std::fs::OpenOptions::new()
                        .write(true)
                        .open(path)
                        .with_context(|| format!("failed to open {}", path.display()))?;
                    file.set_len(good)
                        .and_then(|()| file.sync_all())
                        .with_context(|| format!("failed to truncate {}", path.display()))?;
                    break;
                }
                _ => anyhow::bail!(
                    "{} holds an unreadable record before its end — the http log is damaged",
                    path.display()
                ),
            }
        }
    }

    if segments.is_empty() {
        segments.push((next, segment_path(dir, next)));
    }
    let (_, current) = segments.last().cloned().unwrap_or((next, segment_path(dir, next)));
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&current)
        .with_context(|| format!("failed to open {}", current.display()))?;
    let written = file.seek(std::io::SeekFrom::End(0))?;
    crate::checkpoint::store::sync_dir(dir)?;
    let wal = Arc::new(Wal {
        dir: dir.to_path_buf(),
        segments: Mutex::new(segments),
    });
    Ok((
        Arc::clone(&wal),
        Writer {
            wal,
            file,
            written,
            next,
            segment_bytes,
        },
        records,
    ))
}

impl Writer {
    /// Append one record per post and fsync once for all of them. Returns
    /// each post's offset. Durable when this returns `Ok`; nothing of a call
    /// that fails may be acknowledged.
    pub fn append(&mut self, posts: &[(&[Arc<Value>], &PostMeta)]) -> anyhow::Result<Vec<u64>> {
        if self.written >= self.segment_bytes {
            self.rotate()?;
        }
        let mut out = Vec::new();
        let mut offsets = Vec::with_capacity(posts.len());
        for (messages, meta) in posts {
            let record = Record {
                offset: self.next,
                messages: messages.iter().map(|m| (**m).clone()).collect(),
                meta: (*meta).clone(),
            };
            serde_json::to_writer(&mut out, &record).context("failed to render a post")?;
            out.push(b'\n');
            offsets.push(self.next);
            self.next += 1;
        }
        self.file
            .write_all(&out)
            .and_then(|()| self.file.sync_data())
            .context("failed to write the http log")?;
        self.written += out.len() as u64;
        Ok(offsets)
    }

    fn rotate(&mut self) -> anyhow::Result<()> {
        let path = segment_path(&self.wal.dir, self.next);
        self.file = std::fs::OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("failed to start {}", path.display()))?;
        crate::checkpoint::store::sync_dir(&self.wal.dir)?;
        self.written = 0;
        self.wal.lock().push((self.next, path));
        Ok(())
    }
}

/// The acknowledgement of a logged post: a position (the next offset to
/// replay from) and, once the checkpoint holding it is durable, permission to
/// truncate the log behind it.
pub struct WalAck {
    pub wal: Arc<Wal>,
    pub next: u64,
}

impl Ack for WalAck {
    fn ack(&self) {
        self.wal.truncate_before(self.next);
    }

    fn defers_to_checkpoint(&self) -> bool {
        true
    }

    fn position(&self) -> Option<Value> {
        Some(serde_json::json!({ "offset": self.next }))
    }

    fn absorb(&mut self, later: &dyn Ack) -> bool {
        let Some(later) = (later as &dyn std::any::Any).downcast_ref::<WalAck>() else {
            return false;
        };
        if !Arc::ptr_eq(&self.wal, &later.wal) {
            return false;
        }
        self.next = self.next.max(later.next);
        true
    }
}

/// The offset a checkpointed position says to replay from — `0`, everything,
/// when there is none: a post the log holds and no checkpoint covers was
/// acknowledged to its client and never processed.
#[must_use]
pub fn replay_from(position: Option<&Value>) -> u64 {
    position
        .and_then(|p| p.get("offset"))
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn post(n: i64) -> Vec<Arc<Value>> {
        vec![Arc::new(json!({"n": n}))]
    }

    fn append(writer: &mut Writer, n: i64) -> anyhow::Result<u64> {
        let meta = PostMeta::default();
        let messages = post(n);
        Ok(writer.append(&[(&messages, &meta)])?.first().copied().unwrap_or(0))
    }

    /// What was appended is what the next open reads back, with the offsets it
    /// was given — and appending carries on after them.
    #[test]
    fn records_survive_a_reopen_and_offsets_carry_on() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        {
            let (_, mut writer, records) = open(dir.path())?;
            assert!(records.is_empty());
            assert_eq!(append(&mut writer, 1)?, 0);
            assert_eq!(append(&mut writer, 2)?, 1);
        }
        let (_, mut writer, records) = open(dir.path())?;
        assert_eq!(
            records.iter().map(|r| (r.offset, r.messages.clone())).collect::<Vec<_>>(),
            vec![(0, vec![json!({"n": 1})]), (1, vec![json!({"n": 2})])]
        );
        assert_eq!(append(&mut writer, 3)?, 2);
        Ok(())
    }

    /// Group commit: several posts in one append get consecutive offsets.
    #[test]
    fn one_append_covers_several_posts() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let (_, mut writer, _) = open(dir.path())?;
        let meta = PostMeta::default();
        let (a, b) = (post(1), post(2));
        assert_eq!(writer.append(&[(&a, &meta), (&b, &meta)])?, vec![0, 1]);
        Ok(())
    }

    /// The crash mid-write: a torn last line is cut off, and the next append
    /// lands on a clean line after the last good record.
    #[test]
    fn a_torn_tail_is_cut_off() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        {
            let (_, mut writer, _) = open(dir.path())?;
            append(&mut writer, 1)?;
        }
        let segment = segment_path(dir.path(), 0);
        let mut file = std::fs::OpenOptions::new().append(true).open(&segment)?;
        file.write_all(br#"{"o":1,"m":[{"n":"#)?;
        drop(file);

        let (_, mut writer, records) = open(dir.path())?;
        assert_eq!(records.len(), 1);
        assert_eq!(append(&mut writer, 2)?, 1);
        let (_, _, records) = open(dir.path())?;
        assert_eq!(records.iter().map(|r| r.offset).collect::<Vec<_>>(), vec![0, 1]);
        Ok(())
    }

    /// Truncation removes whole segments behind a committed position and never
    /// the one being written; records after the position all survive.
    #[test]
    fn truncation_removes_only_segments_wholly_behind_the_position() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        // one record per segment
        let (wal, mut writer, _) = open_with(dir.path(), 1)?;
        for n in 0..4 {
            append(&mut writer, n)?;
        }
        assert_eq!(wal.segment_count(), 4);
        wal.truncate_before(2);
        assert_eq!(wal.segment_count(), 2);
        let (_, _, records) = open_with(dir.path(), 1)?;
        assert_eq!(records.iter().map(|r| r.offset).collect::<Vec<_>>(), vec![2, 3]);
        // everything, but the current segment stays
        wal.truncate_before(100);
        assert_eq!(wal.segment_count(), 1);
        Ok(())
    }

    /// Acks from one log fold to the furthest position; firing truncates.
    #[test]
    fn acks_fold_to_the_furthest_offset() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let (wal, _, _) = open(dir.path())?;
        let mut held = WalAck { wal: Arc::clone(&wal), next: 3 };
        assert!(held.absorb(&WalAck { wal: Arc::clone(&wal), next: 7 }));
        assert!(held.absorb(&WalAck { wal, next: 5 }));
        assert_eq!(held.position(), Some(json!({"offset": 7})));
        Ok(())
    }

    #[test]
    fn with_no_checkpoint_everything_is_replayed() {
        assert_eq!(replay_from(None), 0);
        assert_eq!(replay_from(Some(&json!({"offset": 9}))), 9);
    }

    #[test]
    fn an_id_never_picks_a_path() {
        let dir = dir_for(Path::new("/state"), "../../etc");
        assert!(dir.starts_with("/state/http"));
        assert_eq!(dir.components().count(), Path::new("/state/http/x").components().count());
    }
}
