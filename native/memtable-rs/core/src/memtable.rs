// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The concurrent memtable: partitions sharded by key range, each shard a
//! `BTreeMap` behind a `RwLock`.
//!
//! # Concurrency design
//!
//! Keys are split into contiguous byte ranges by caller-supplied boundaries
//! (Java computes them from token ranges, as `ShardBoundaries` does for
//! `ShardedSkipListMemtable`). A write takes its shard's write lock for the
//! duration of one partition merge; reads and flush take read locks. This is
//! the model of `ShardedSkipListMemtable` with `serialize_writes: true`:
//! writers to the same shard queue, writers to different shards do not
//! interact. It was chosen over a lock-free skip list because its correctness
//! argument is a few lines long and it needs no `unsafe` or epoch-based
//! reclamation; its cost (writer queueing within a shard) is measured, not
//! assumed (see `docs/spike/rust-memtable.md`).
//!
//! # Freezing and flush
//!
//! [`Memtable::freeze`] sets a flag *inside* each shard while holding that
//! shard's write lock. A write checks the flag under the same lock, so once
//! `freeze` returns no write is in progress and none can start. A
//! [`FlushIter`] then walks the shards in order without holding any lock
//! between steps: it re-finds its position by key, which is sound because a
//! frozen memtable never changes. The iterator owns an `Arc<Memtable>`, so
//! the data lives until both the last iterator and the owner's handle are
//! dropped.
//!
//! # Panics and poisoning
//!
//! Nothing here is expected to panic. If a writer did panic, the shard's lock
//! is poisoned and every later access to that shard returns
//! [`MemtableError::Poisoned`]: a half-merged partition is never served.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt;
use std::mem::size_of;
use std::ops::Bound;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::codec::{PartitionUpdate, TooLarge, encode_partition};
use crate::model::{DeletionTime, Row};

/// Errors from memtable operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemtableError {
    /// A write arrived after [`Memtable::freeze`].
    Frozen,
    /// A flush iterator was requested before [`Memtable::freeze`].
    NotFrozen,
    /// A thread panicked while holding a shard's write lock.
    Poisoned,
    /// Shard boundaries were not strictly increasing.
    BadBoundaries,
    /// A partition is too large for the wire format.
    TooLarge,
}

impl fmt::Display for MemtableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Frozen => "memtable is frozen for flush; writes are not accepted",
            Self::NotFrozen => "memtable must be frozen before it is flushed",
            Self::Poisoned => "a writer panicked; the memtable is unusable",
            Self::BadBoundaries => "shard boundaries must be strictly increasing",
            Self::TooLarge => "partition too large for the wire format",
        })
    }
}

impl std::error::Error for MemtableError {}

impl From<TooLarge> for MemtableError {
    fn from(_: TooLarge) -> Self {
        Self::TooLarge
    }
}

/// One partition: its deletion and its rows by clustering.
#[derive(Debug, Default)]
struct Partition {
    deletion: DeletionTime,
    rows: BTreeMap<Box<[u8]>, Row>,
}

impl Partition {
    /// The rows as reads and flushes see them: filtered by the partition
    /// deletion, as Java's memtable does on read (see [`Row::visible_under`]).
    fn visible<'a>(
        &self,
        rows: impl Iterator<Item = (&'a Box<[u8]>, &'a Row)>,
    ) -> impl Iterator<Item = (&'a [u8], Cow<'a, Row>)> {
        let deletion = self.deletion;
        rows.filter_map(move |(c, r)| r.visible_under(deletion).map(|r| (&**c, r)))
    }
}

#[derive(Debug, Default)]
struct Shard {
    frozen: bool,
    partitions: BTreeMap<Box<[u8]>, Partition>,
}

/// Estimated bytes one `BTreeMap` entry costs beyond its key's and value's
/// heap data. std's B-tree nodes hold up to 11 key/value slots and, under
/// random inserts, run about two thirds full, so each entry pays roughly 1.5
/// times its inline size. This is an estimate; the `accounting` integration
/// test measures it against a counting allocator.
const fn map_entry_bytes<K, V>() -> usize {
    (size_of::<K>() + size_of::<V>()) * 3 / 2
}

fn row_entry_bytes(clustering: &[u8], row: &Row) -> usize {
    map_entry_bytes::<Box<[u8]>, Row>() + clustering.len() + row.heap_bytes()
}

fn partition_entry_bytes(key: &[u8]) -> usize {
    map_entry_bytes::<Box<[u8]>, Partition>() + key.len()
}

/// Converts a byte count to a signed delta. Counts are bounded by the
/// address space, far below `i64::MAX`.
fn signed(n: usize) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

/// A concurrent memtable. `Send + Sync`: all shared state is behind
/// `RwLock`s or atomics.
#[derive(Debug)]
pub struct Memtable {
    /// `boundaries[i]` is the first key of shard `i + 1`.
    boundaries: Box<[Box<[u8]>]>,
    shards: Box<[RwLock<Shard>]>,
    owned_bytes: AtomicI64,
    partition_count: AtomicUsize,
}

impl Memtable {
    /// Creates a memtable with `boundaries.len() + 1` shards. Shard `i` holds
    /// the keys `k` with `boundaries[i - 1] <= k < boundaries[i]`.
    ///
    /// # Errors
    /// [`MemtableError::BadBoundaries`] unless the boundaries are strictly
    /// increasing.
    pub fn new(boundaries: Vec<Box<[u8]>>) -> Result<Self, MemtableError> {
        if boundaries.windows(2).any(|w| w[0] >= w[1]) {
            return Err(MemtableError::BadBoundaries);
        }
        let shards = (0..=boundaries.len()).map(|_| RwLock::default()).collect();
        Ok(Self {
            boundaries: boundaries.into_boxed_slice(),
            shards,
            owned_bytes: AtomicI64::new(0),
            partition_count: AtomicUsize::new(0),
        })
    }

    /// Number of shards.
    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    fn shard_of(&self, key: &[u8]) -> usize {
        self.boundaries.partition_point(|b| **b <= *key)
    }

    fn read_shard(&self, i: usize) -> Result<RwLockReadGuard<'_, Shard>, MemtableError> {
        self.shards[i].read().map_err(|_| MemtableError::Poisoned)
    }

    fn write_shard(&self, i: usize) -> Result<RwLockWriteGuard<'_, Shard>, MemtableError> {
        self.shards[i].write().map_err(|_| MemtableError::Poisoned)
    }

    /// Estimated heap bytes owned by this memtable's data. Updated after each
    /// write, so concurrent readers see a value at most one write behind per
    /// writer.
    #[must_use]
    pub fn owned_bytes(&self) -> i64 {
        self.owned_bytes.load(Ordering::Relaxed)
    }

    /// Number of distinct partitions.
    #[must_use]
    pub fn partition_count(&self) -> usize {
        self.partition_count.load(Ordering::Relaxed)
    }

    /// Applies one partition update, merging it as
    /// `BTreePartitionUpdater.mergePartitions` does
    /// (`partitions/BTreePartitionUpdater.java:65-129`). Returns the change in
    /// owned bytes.
    ///
    /// The partition deletion is merged as `MutableDeletionInfo.add`
    /// (`MutableDeletionInfo.java:110-114`). It does not remove the rows it
    /// shadows: neither does Java's memtable, which applies it on read and
    /// flush (as [`Memtable::read`] and [`FlushIter`] do here).
    /// A new row is inserted as given, without filtering by its own deletion
    /// (`BTreePartitionUpdater.insert`, `partitions/BTreePartitionUpdater.java:160-168`);
    /// an existing row is merged by [`Row::merge`].
    ///
    /// # Errors
    /// [`MemtableError::Frozen`] after [`Memtable::freeze`];
    /// [`MemtableError::Poisoned`] if the shard is poisoned.
    pub fn apply(&self, update: PartitionUpdate) -> Result<i64, MemtableError> {
        let PartitionUpdate {
            key,
            deletion,
            rows,
        } = update;
        let mut shard = self.write_shard(self.shard_of(&key))?;
        if shard.frozen {
            return Err(MemtableError::Frozen);
        }
        let mut delta = 0i64;
        let partition = match shard.partitions.entry(key) {
            Entry::Vacant(v) => {
                delta += signed(partition_entry_bytes(v.key()));
                self.partition_count.fetch_add(1, Ordering::Relaxed);
                v.insert(Partition::default())
            }
            Entry::Occupied(o) => o.into_mut(),
        };
        if deletion.supersedes(partition.deletion) {
            partition.deletion = deletion;
        }
        for (clustering, row) in rows {
            match partition.rows.entry(clustering) {
                Entry::Vacant(v) => {
                    delta += signed(row_entry_bytes(v.key(), &row));
                    v.insert(row);
                }
                Entry::Occupied(mut o) => {
                    let before = o.get().heap_bytes();
                    o.get_mut().merge(row);
                    delta += signed(o.get().heap_bytes()) - signed(before);
                }
            }
        }
        self.owned_bytes.fetch_add(delta, Ordering::Relaxed);
        Ok(delta)
    }

    /// Appends the partition `key`, restricted to the rows whose clustering
    /// lies in `[start, end)`, to `out` as one partition record. `None` for
    /// `end` means unbounded. Returns false (and appends nothing) if the
    /// partition is absent. Byte-comparable clustering *bounds* sort between
    /// clusterings, so inclusive and exclusive slices are both expressible.
    ///
    /// # Errors
    /// [`MemtableError::Poisoned`] or [`MemtableError::TooLarge`].
    pub fn read(
        &self,
        key: &[u8],
        start: &[u8],
        end: Option<&[u8]>,
        out: &mut Vec<u8>,
    ) -> Result<bool, MemtableError> {
        let shard = self.read_shard(self.shard_of(key))?;
        let Some(partition) = shard.partitions.get(key) else {
            return Ok(false);
        };
        let mark = out.len();
        let result = match end {
            // `BTreeMap::range` panics on an inverted range; such a slice is
            // simply empty.
            Some(end) if end <= start => encode_partition::<&Row>(out, key, partition.deletion, []),
            _ => {
                let upper = end.map_or(Bound::Unbounded, Bound::Excluded);
                let rows = partition
                    .rows
                    .range::<[u8], _>((Bound::Included(start), upper));
                encode_partition(out, key, partition.deletion, partition.visible(rows))
            }
        };
        if result.is_err() {
            out.truncate(mark);
        }
        result?;
        Ok(true)
    }

    /// Stops accepting writes. When this returns, no write is in progress.
    /// Idempotent.
    ///
    /// # Errors
    /// [`MemtableError::Poisoned`] if a shard is poisoned.
    pub fn freeze(&self) -> Result<(), MemtableError> {
        for i in 0..self.shards.len() {
            self.write_shard(i)?.frozen = true;
        }
        Ok(())
    }

    /// An iterator over all partitions in key order. The iterator keeps the
    /// memtable alive.
    ///
    /// # Errors
    /// [`MemtableError::NotFrozen`] unless [`Memtable::freeze`] has returned.
    pub fn flush_iter(self: &Arc<Self>) -> Result<FlushIter, MemtableError> {
        // Shards are frozen in order, so the last one being frozen means all
        // are.
        let last = self.shards.len() - 1;
        if !self.read_shard(last)?.frozen {
            return Err(MemtableError::NotFrozen);
        }
        Ok(FlushIter {
            memtable: Arc::clone(self),
            shard: 0,
            after: None,
        })
    }

    /// Recomputes the owned-bytes estimate from scratch; for tests of the
    /// incremental accounting.
    ///
    /// # Errors
    /// [`MemtableError::Poisoned`] if a shard is poisoned.
    pub fn recount_owned_bytes(&self) -> Result<i64, MemtableError> {
        let mut total = 0usize;
        for i in 0..self.shards.len() {
            let shard = self.read_shard(i)?;
            for (key, p) in &shard.partitions {
                total += partition_entry_bytes(key);
                total += p
                    .rows
                    .iter()
                    .map(|(c, r)| row_entry_bytes(c, r))
                    .sum::<usize>();
            }
        }
        Ok(signed(total))
    }
}

/// An ordered walk over a frozen memtable, emitting partition records.
#[derive(Debug)]
pub struct FlushIter {
    memtable: Arc<Memtable>,
    shard: usize,
    /// The last key emitted from `shard`, if any.
    after: Option<Box<[u8]>>,
}

impl FlushIter {
    /// Appends whole partition records to `out` while its length stays within
    /// `max_bytes`, and returns how many it appended. Returns 0 with `out`
    /// unchanged when the walk is over. If the next record alone exceeds
    /// `max_bytes`, returns 0 with that record appended and the position
    /// unchanged, so the caller can retry with a larger limit.
    ///
    /// # Errors
    /// [`MemtableError::Poisoned`] or [`MemtableError::TooLarge`].
    pub fn next_batch(
        &mut self,
        out: &mut Vec<u8>,
        max_bytes: usize,
    ) -> Result<usize, MemtableError> {
        let start_len = out.len();
        let mut count = 0;
        while self.shard < self.memtable.shards.len() {
            let shard = self.memtable.read_shard(self.shard)?;
            let lower = self
                .after
                .as_deref()
                .map_or(Bound::Unbounded, Bound::Excluded);
            let mut range = shard.partitions.range::<[u8], _>((lower, Bound::Unbounded));
            loop {
                let Some((key, p)) = range.next() else {
                    drop(range);
                    drop(shard);
                    self.shard += 1;
                    self.after = None;
                    break;
                };
                let mark = out.len();
                if let Err(e) = encode_partition(out, key, p.deletion, p.visible(p.rows.iter())) {
                    out.truncate(mark);
                    return Err(e.into());
                }
                if out.len() - start_len > max_bytes {
                    if count > 0 {
                        out.truncate(mark);
                    }
                    return Ok(count);
                }
                self.after = Some(key.clone());
                count += 1;
            }
        }
        Ok(count)
    }
}
