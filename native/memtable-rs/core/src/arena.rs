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

//! The concurrent memtable, with each shard's data in a few large chunks.
//!
//! # Why
//!
//! The first version ([`crate::boxed`]) gave every key, row, cell array and
//! value its own heap allocation: about seven per one-row partition, two
//! million for a 300,000-partition memtable. Freeing them when the memtable
//! was discarded took most of a second with glibc's allocator, and with two
//! writers the flushes fell behind (see `docs/spike/rust-memtable-arena.md`).
//! Here a shard keeps everything in append-only stores ([`crate::region`]):
//!
//! | Store | Holds | Indexed by |
//! |-------|-------|------------|
//! | `bytes: Region<u8>` | keys, clusterings, cell values | [`Span`] |
//! | `cells: Region<StoredCell>` | each row's cells, as one contiguous run | [`Span`] |
//! | `partitions: Pool<PartitionRec>` | key span and partition deletion | `u32` |
//! | `rows: Pool<RowRec>` | partition id, clustering span, liveness, deletion, cell run | `u32` |
//! | `nodes: Pool<Node>` | the nodes of two B-trees ([`crate::btree`]) | `u32` |
//!
//! The partition tree orders partition ids by key bytes. The row tree orders
//! row ids by (partition id, clustering bytes): all of a partition's rows are
//! adjacent, and the order between partitions is just a numeric one, so
//! comparing two rows of different partitions never reads a key. Dropping a
//! shard frees its chunks: a few dozen allocations however much it holds.
//!
//! # Semantics
//!
//! The reconciliation rules are not repeated here. A merge builds the
//! existing row and the update as [`Row<Val>`](Row), whose values are
//! references, runs the same [`Row::merge_by`] the boxed memtable uses, and
//! writes the result back; reads and flushes filter by the partition deletion
//! with the same [`Row::visible_under`]. The property tests check that both
//! memtables return the same bytes for the same updates, and the Java
//! differential test checks this one against Cassandra's.
//!
//! # Waste
//!
//! Nothing is freed before the memtable is. Two overwrites are done in place
//! instead of appending: a merge that produces at most as many cells as the
//! row had rewrites the row's cell run, and a winning value that fits in the
//! value it displaced (same column) takes its bytes. Anything else (a longer
//! value, a row that gains cells, cells removed by a deletion) leaves the old
//! bytes behind until the memtable is discarded, as Cassandra's slab
//! allocator does with its regions. [`Memtable::space`] reports how much of
//! the allocated space is live.
//!
//! # Concurrency, freezing and flush
//!
//! As in the boxed memtable (one `RwLock` per shard; see its module comment).
//! If a write fails part-way (a store ran out of indices, or a panic) the
//! shard refuses every later operation, so a partly applied update is never
//! served.

use std::cmp::Ordering;
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::btree::{Node, Tree};
use crate::codec::{
    PartitionUpdate, TooLarge, begin_partition, end_partition, put_cell, put_row_header,
};
use crate::error::MemtableError;
use crate::model::{Cell, DeletionTime, LivenessInfo, Row, RowDeletion, row_unaffected_by};
use crate::region::{Full, Pool, Region, Span};
use crate::rows::signed;

impl From<Full> for MemtableError {
    fn from(_: Full) -> Self {
        Self::Full
    }
}

/// A cell as stored: its value is a span of the shard's bytes.
type StoredCell = Cell<Span>;

#[derive(Clone, Copy, Debug)]
struct PartitionRec {
    key: Span,
    deletion: DeletionTime,
}

#[derive(Clone, Copy, Debug)]
struct RowRec {
    partition: u32,
    clustering: Span,
    liveness: LivenessInfo,
    deletion: RowDeletion,
    cells: Span,
}

/// A cell value during a merge: already stored, or the `i`th value of the
/// update, not stored unless it wins.
#[derive(Clone, Copy, Debug)]
enum Val {
    Stored(Span),
    New(usize),
}

/// Space accounting of a memtable, in bytes (see [`Memtable::space`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Space {
    /// Chunk capacities: what the memtable holds from the allocator.
    pub allocated: usize,
    /// Bytes ever written into the chunks.
    pub used: usize,
    /// Bytes reachable from the indexes: `used - live` is overwrite waste.
    pub live: usize,
    /// Number of chunks (heap allocations) across all shards.
    pub chunks: usize,
}

#[derive(Debug, Default)]
struct Shard {
    frozen: bool,
    /// Set when a write failed part-way; see the module comment.
    failed: bool,
    bytes: Region<u8>,
    cells: Region<StoredCell>,
    partitions: Pool<PartitionRec>,
    rows: Pool<RowRec>,
    nodes: Pool<Node>,
    partition_tree: Tree,
    row_tree: Tree,
    /// Reused for building cell runs; its capacity is counted as allocated.
    scratch: Vec<StoredCell>,
}

/// `partition.cmp(other)`, then `clustering.cmp(other's)`: the row tree order.
fn row_order(bytes: &Region<u8>, row: &RowRec, partition: u32, clustering: &[u8]) -> Ordering {
    partition
        .cmp(&row.partition)
        .then_with(|| clustering.cmp(bytes.get(row.clustering)))
}

impl Shard {
    fn allocated_bytes(&self) -> usize {
        self.bytes.allocated_bytes()
            + self.cells.allocated_bytes()
            + self.partitions.allocated_bytes()
            + self.rows.allocated_bytes()
            + self.nodes.allocated_bytes()
            + self.scratch.capacity() * std::mem::size_of::<StoredCell>()
    }

    fn check_usable(&self) -> Result<(), MemtableError> {
        if self.failed {
            Err(MemtableError::Poisoned)
        } else {
            Ok(())
        }
    }

    fn find_partition(&self, key: &[u8]) -> Option<u32> {
        let Self {
            bytes,
            partitions,
            nodes,
            partition_tree,
            ..
        } = self;
        partition_tree.find(nodes, |p| key.cmp(bytes.get(partitions.get(p).key)))
    }

    fn apply(&mut self, update: PartitionUpdate) -> Result<(), MemtableError> {
        let PartitionUpdate {
            key,
            deletion,
            rows,
        } = update;
        let p = if let Some(p) = self.find_partition(&key) {
            p
        } else {
            let Self {
                bytes,
                partitions,
                nodes,
                partition_tree,
                ..
            } = self;
            let span = bytes.push(&key)?;
            let p = partitions.push(PartitionRec {
                key: span,
                deletion: DeletionTime::LIVE,
            })?;
            partition_tree.insert(nodes, p, |q| key[..].cmp(bytes.get(partitions.get(q).key)))?;
            p
        };
        // `MutableDeletionInfo.add` (`MutableDeletionInfo.java:110-114`).
        let record = self.partitions.get_mut(p);
        if deletion.supersedes(record.deletion) {
            record.deletion = deletion;
        }
        for (clustering, row) in rows {
            self.upsert_row(p, &clustering, row)?;
        }
        Ok(())
    }

    /// `BTreePartitionUpdater` for one row: a new row is stored as given
    /// (`partitions/BTreePartitionUpdater.java:160-168`), an existing one is
    /// merged by [`Row::merge_by`].
    fn upsert_row(&mut self, p: u32, clustering: &[u8], update: Row) -> Result<(), MemtableError> {
        let Self {
            bytes,
            cells,
            rows,
            nodes,
            row_tree,
            scratch,
            ..
        } = self;
        let found = row_tree.find(nodes, |r| row_order(bytes, rows.get(r), p, clustering));
        let Some(r) = found else {
            scratch.clear();
            for c in update.cells {
                let value = bytes.push(&c.value)?;
                scratch.push(stored(&c, value));
            }
            let record = RowRec {
                partition: p,
                clustering: bytes.push(clustering)?,
                liveness: update.liveness,
                deletion: update.deletion,
                cells: cells.push(scratch)?,
            };
            let r = rows.push(record)?;
            row_tree.insert(nodes, r, |q| row_order(bytes, rows.get(q), p, clustering))?;
            return Ok(());
        };

        let record = *rows.get(r);
        let mut existing = Row {
            liveness: record.liveness,
            deletion: record.deletion,
            cells: cells
                .get(record.cells)
                .iter()
                .map(|c| with_value(c, Val::Stored(c.value)))
                .collect(),
        };
        let mut values = Vec::with_capacity(update.cells.len());
        let update = Row {
            liveness: update.liveness,
            deletion: update.deletion,
            cells: update
                .cells
                .into_iter()
                .enumerate()
                .map(|(i, c)| {
                    let cell = with_value(&c, Val::New(i));
                    values.push(c.value);
                    cell
                })
                .collect(),
        };
        {
            let resolve = |v: &Val| -> &[u8] {
                match *v {
                    Val::Stored(s) => bytes.get(s),
                    Val::New(i) => &values[i],
                }
            };
            existing.merge_by(update, |a, b| resolve(a).cmp(resolve(b)));
        }

        scratch.clear();
        let old_cells = cells.get(record.cells);
        for c in &existing.cells {
            let value = match c.value {
                Val::Stored(s) => s,
                Val::New(i) => {
                    let new = &values[i][..];
                    // The update won this column. If the value it displaced
                    // has room, reuse its bytes: a stored value span belongs
                    // to exactly one live cell, this one, and the merge
                    // result holds one cell per column, so nothing else can
                    // still refer to it.
                    match old_cells.binary_search_by(|o| o.column.cmp(&c.column)) {
                        Ok(j) if old_cells[j].value.len() >= new.len() => {
                            let span = old_cells[j].value.prefix(new.len());
                            bytes.get_mut(span).copy_from_slice(new);
                            span
                        }
                        _ => bytes.push(new)?,
                    }
                }
            };
            scratch.push(with_value(c, value));
        }
        let run = if scratch.len() <= record.cells.len() {
            let run = record.cells.prefix(scratch.len());
            cells.get_mut(run).copy_from_slice(scratch);
            run
        } else {
            cells.push(scratch)?
        };
        *rows.get_mut(r) = RowRec {
            liveness: existing.liveness,
            deletion: existing.deletion,
            cells: run,
            ..record
        };
        Ok(())
    }

    /// Appends the record of partition `p` with its rows in
    /// `[start, end)` as reads and flushes see them, filtered by the partition
    /// deletion ([`Row::visible_under`]).
    fn encode(
        &self,
        out: &mut Vec<u8>,
        p: u32,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<(), TooLarge> {
        let Self {
            bytes,
            cells,
            partitions,
            rows,
            nodes,
            row_tree,
            ..
        } = self;
        let partition = partitions.get(p);
        let active = partition.deletion;
        let header = begin_partition(out, bytes.get(partition.key), active)?;
        let mut count = 0usize;
        let walked = row_tree.for_each_from(
            nodes,
            |r| row_order(bytes, rows.get(r), p, start) == Ordering::Greater,
            |r| {
                let row = rows.get(r);
                let clustering = bytes.get(row.clustering);
                if row.partition != p || end.is_some_and(|e| clustering >= e) {
                    return ControlFlow::Break(Ok(()));
                }
                match encode_row(out, bytes, cells.get(row.cells), row, active) {
                    Ok(true) => count += 1,
                    Ok(false) => {}
                    Err(e) => return ControlFlow::Break(Err(e)),
                }
                ControlFlow::Continue(())
            },
        );
        if let ControlFlow::Break(Err(e)) = walked {
            return Err(e);
        }
        end_partition(out, header, count)
    }

    /// Bytes reachable from the indexes.
    fn live_bytes(&self) -> usize {
        let cell = std::mem::size_of::<StoredCell>();
        let mut live = self.nodes.used_bytes()
            + self.partitions.used_bytes()
            + self.rows.used_bytes()
            + self.scratch.capacity() * cell;
        let _ = self.partition_tree.for_each::<()>(&self.nodes, |p| {
            live += self.partitions.get(p).key.len();
            ControlFlow::Continue(())
        });
        let _ = self.row_tree.for_each::<()>(&self.nodes, |r| {
            let row = self.rows.get(r);
            live += row.clustering.len() + row.cells.len() * cell;
            live += self
                .cells
                .get(row.cells)
                .iter()
                .map(|c| c.value.len())
                .sum::<usize>();
            ControlFlow::Continue(())
        });
        live
    }

    fn used_bytes(&self) -> usize {
        self.bytes.used_bytes()
            + self.cells.used_bytes()
            + self.partitions.used_bytes()
            + self.rows.used_bytes()
            + self.nodes.used_bytes()
            + self.scratch.capacity() * std::mem::size_of::<StoredCell>()
    }

    fn chunk_count(&self) -> usize {
        self.bytes.chunk_count()
            + self.cells.chunk_count()
            + self.partitions.chunk_count()
            + self.rows.chunk_count()
            + self.nodes.chunk_count()
            + usize::from(self.scratch.capacity() > 0)
    }
}

fn with_value<V, W>(c: &Cell<V>, value: W) -> Cell<W> {
    Cell {
        column: c.column,
        timestamp: c.timestamp,
        ttl: c.ttl,
        local_deletion_time: c.local_deletion_time,
        value,
    }
}

fn stored(c: &Cell, value: Span) -> StoredCell {
    with_value(c, value)
}

/// Appends one row as a read sees it under the partition deletion `active`;
/// returns false if nothing of it is visible.
fn encode_row(
    out: &mut Vec<u8>,
    bytes: &Region<u8>,
    cells: &[StoredCell],
    row: &RowRec,
    active: DeletionTime,
) -> Result<bool, TooLarge> {
    let clustering = bytes.get(row.clustering);
    if row_unaffected_by(row.deletion, active) {
        // The common case, without building a `Row`.
        put_row_header(out, clustering, row.liveness, row.deletion, cells.len())?;
        for c in cells {
            put_cell(out, c, bytes.get(c.value))?;
        }
        return Ok(true);
    }
    let full = Row {
        liveness: row.liveness,
        deletion: row.deletion,
        cells: cells.to_vec(),
    };
    let Some(visible) = full.visible_under(active) else {
        return Ok(false);
    };
    put_row_header(
        out,
        clustering,
        visible.liveness,
        visible.deletion,
        visible.cells.len(),
    )?;
    for c in &visible.cells {
        put_cell(out, c, bytes.get(c.value))?;
    }
    Ok(true)
}

/// A shard and its counters, aligned so that no two shards share a cache
/// line. The counters are written only under the shard's write lock.
#[derive(Debug, Default)]
#[repr(align(128))]
struct ShardSlot {
    lock: RwLock<Shard>,
    owned_bytes: AtomicI64,
    partition_count: AtomicUsize,
}

/// A concurrent memtable. `Send + Sync`: all shared state is behind
/// `RwLock`s or atomics.
#[derive(Debug)]
pub struct Memtable {
    /// `boundaries[i]` is the first key of shard `i + 1`.
    boundaries: Box<[Box<[u8]>]>,
    shards: Box<[ShardSlot]>,
}

impl Memtable {
    /// Creates a memtable with `boundaries.len() + 1` shards. Shard `i` holds
    /// the keys `k` with `boundaries[i - 1] <= k < boundaries[i]`. A shard
    /// allocates nothing until its first write.
    ///
    /// # Errors
    /// [`MemtableError::BadBoundaries`] unless the boundaries are strictly
    /// increasing.
    pub fn new(boundaries: Vec<Box<[u8]>>) -> Result<Self, MemtableError> {
        if boundaries.windows(2).any(|w| w[0] >= w[1]) {
            return Err(MemtableError::BadBoundaries);
        }
        let shards = (0..=boundaries.len())
            .map(|_| ShardSlot::default())
            .collect();
        Ok(Self {
            boundaries: boundaries.into_boxed_slice(),
            shards,
        })
    }

    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    fn shard_of(&self, key: &[u8]) -> usize {
        self.boundaries.partition_point(|b| **b <= *key)
    }

    fn read_shard(&self, i: usize) -> Result<RwLockReadGuard<'_, Shard>, MemtableError> {
        let shard = self.shards[i]
            .lock
            .read()
            .map_err(|_| MemtableError::Poisoned)?;
        shard.check_usable()?;
        Ok(shard)
    }

    fn write_shard(&self, i: usize) -> Result<RwLockWriteGuard<'_, Shard>, MemtableError> {
        let shard = self.shards[i]
            .lock
            .write()
            .map_err(|_| MemtableError::Poisoned)?;
        shard.check_usable()?;
        Ok(shard)
    }

    /// Bytes this memtable holds from the allocator (chunk capacities), the
    /// sum of per-shard counts updated at the end of each write. O(shards).
    #[must_use]
    pub fn owned_bytes(&self) -> i64 {
        self.shards
            .iter()
            .map(|s| s.owned_bytes.load(AtomicOrdering::Relaxed))
            .sum()
    }

    /// Number of distinct partitions. O(shards).
    #[must_use]
    pub fn partition_count(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.partition_count.load(AtomicOrdering::Relaxed))
            .sum()
    }

    /// Applies one partition update with the rules of
    /// `BTreePartitionUpdater.mergePartitions`
    /// (`partitions/BTreePartitionUpdater.java:65-129`); see
    /// [`crate::boxed::Memtable::apply`] for how the partition deletion is
    /// treated. Returns the change in owned bytes (zero unless a chunk was
    /// added).
    ///
    /// # Errors
    /// [`MemtableError::Frozen`] after [`Memtable::freeze`];
    /// [`MemtableError::Poisoned`] if the shard is poisoned or failed;
    /// [`MemtableError::Full`] if a store ran out of indices (the shard is
    /// unusable afterwards).
    pub fn apply(&self, update: PartitionUpdate) -> Result<i64, MemtableError> {
        let index = self.shard_of(&update.key);
        let slot = &self.shards[index];
        let mut shard = self.write_shard(index)?;
        if shard.frozen {
            return Err(MemtableError::Frozen);
        }
        let before = shard.allocated_bytes();
        let result = shard.apply(update);
        if result.is_err() {
            shard.failed = true;
        }
        let delta = signed(shard.allocated_bytes()) - signed(before);
        slot.owned_bytes.fetch_add(delta, AtomicOrdering::Relaxed);
        slot.partition_count
            .store(shard.partitions.len(), AtomicOrdering::Relaxed);
        result.map(|()| delta)
    }

    /// Appends the partition `key`, restricted to the rows whose clustering
    /// lies in `[start, end)`, to `out` as one partition record; see
    /// [`crate::boxed::Memtable::read`].
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
        let Some(p) = shard.find_partition(key) else {
            return Ok(false);
        };
        let mark = out.len();
        if let Err(e) = shard.encode(out, p, start, end) {
            out.truncate(mark);
            return Err(e.into());
        }
        Ok(true)
    }

    /// Stops accepting writes. When this returns, no write is in progress.
    /// Idempotent.
    ///
    /// # Errors
    /// [`MemtableError::Poisoned`] if a shard is poisoned.
    pub fn freeze(&self) -> Result<(), MemtableError> {
        for i in 0..self.shards.len() {
            // A failed shard can still be frozen; it refuses reads anyway.
            let mut shard = self.shards[i]
                .lock
                .write()
                .map_err(|_| MemtableError::Poisoned)?;
            shard.frozen = true;
        }
        Ok(())
    }

    /// An iterator over all partitions in key order. The iterator keeps the
    /// memtable alive.
    ///
    /// # Errors
    /// [`MemtableError::NotFrozen`] unless [`Memtable::freeze`] has returned.
    pub fn flush_iter(self: &Arc<Self>) -> Result<FlushIter, MemtableError> {
        let last = self.shards.len() - 1;
        let frozen = self.shards[last]
            .lock
            .read()
            .map_err(|_| MemtableError::Poisoned)?
            .frozen;
        if !frozen {
            return Err(MemtableError::NotFrozen);
        }
        Ok(FlushIter {
            memtable: Arc::clone(self),
            shard: 0,
            after: None,
        })
    }

    /// Recomputes the owned-bytes figure from the shards; for tests of the
    /// incremental accounting.
    ///
    /// # Errors
    /// [`MemtableError::Poisoned`] if a shard is poisoned.
    pub fn recount_owned_bytes(&self) -> Result<i64, MemtableError> {
        let mut total = 0usize;
        for i in 0..self.shards.len() {
            total += self.read_shard(i)?.allocated_bytes();
        }
        Ok(signed(total))
    }

    /// Allocated, used and live bytes and the chunk count. Walks every
    /// partition and row: O(memtable), for measurements and tests.
    ///
    /// # Errors
    /// [`MemtableError::Poisoned`] if a shard is poisoned.
    pub fn space(&self) -> Result<Space, MemtableError> {
        let mut space = Space::default();
        for i in 0..self.shards.len() {
            let shard = self.read_shard(i)?;
            space.allocated += shard.allocated_bytes();
            space.used += shard.used_bytes();
            space.live += shard.live_bytes();
            space.chunks += shard.chunk_count();
        }
        Ok(space)
    }
}

/// An ordered walk over a frozen memtable, emitting partition records.
#[derive(Debug)]
pub struct FlushIter {
    memtable: Arc<Memtable>,
    shard: usize,
    /// The last partition emitted from `shard`, if any. Ids are stable
    /// because a frozen memtable never changes.
    after: Option<u32>,
}

/// Why a batch walk stopped early.
enum Stop {
    BatchFull,
    Error(TooLarge),
}

impl FlushIter {
    /// Appends whole partition records to `out` while its length stays within
    /// `max_bytes`, and returns how many it appended; see
    /// [`crate::boxed::FlushIter::next_batch`] for the exact contract.
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
            let after_key = self
                .after
                .map(|a| shard.bytes.get(shard.partitions.get(a).key));
            let after = &mut self.after;
            let walked = shard.partition_tree.for_each_from(
                &shard.nodes,
                |p| after_key.is_some_and(|a| shard.bytes.get(shard.partitions.get(p).key) <= a),
                |p| {
                    let mark = out.len();
                    if let Err(e) = shard.encode(out, p, &[], None) {
                        out.truncate(mark);
                        return ControlFlow::Break(Stop::Error(e));
                    }
                    if out.len() - start_len > max_bytes {
                        if count > 0 {
                            out.truncate(mark);
                        }
                        return ControlFlow::Break(Stop::BatchFull);
                    }
                    *after = Some(p);
                    count += 1;
                    ControlFlow::Continue(())
                },
            );
            match walked {
                ControlFlow::Continue(()) => {
                    drop(shard);
                    self.shard += 1;
                    self.after = None;
                }
                ControlFlow::Break(Stop::BatchFull) => return Ok(count),
                ControlFlow::Break(Stop::Error(e)) => return Err(e.into()),
            }
        }
        Ok(count)
    }
}
