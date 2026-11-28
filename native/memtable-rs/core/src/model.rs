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

//! The data model (deletion times, liveness, cells, rows, partitions) and
//! Cassandra's reconciliation rules over it.
//!
//! Every rule cites the Java it mirrors. Paths are relative to
//! `src/java/org/apache/cassandra/db/` in the repository this crate lives in.
//! The rules are copied, not reinterpreted: where the Java is asymmetric or
//! surprising, so is this code, and the comment says why.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::mem::size_of;

/// `Cell.NO_TTL` (`rows/Cell.java:48`) and `LivenessInfo.NO_TTL`.
pub const NO_TTL: i32 = 0;
/// `Cell.NO_DELETION_TIME` (`rows/Cell.java:49`), also
/// `LivenessInfo.NO_EXPIRATION_TIME`.
pub const NO_DELETION_TIME: i64 = i64::MAX;
/// `Cell.MAX_DELETION_TIME` (`rows/Cell.java:51`): the largest valid local
/// deletion time, `2^32 - 1 - 2`.
pub const MAX_DELETION_TIME: i64 = (1 << 32) - 1 - 2;
/// `LivenessInfo.NO_TIMESTAMP` (`LivenessInfo.java:42`).
pub const NO_TIMESTAMP: i64 = i64::MIN;
/// `LivenessInfo.EXPIRED_LIVENESS_TTL` (`LivenessInfo.java:50`).
pub const EXPIRED_LIVENESS_TTL: i32 = i32::MAX;

/// Returns true if `t` is a local deletion time Java would accept without
/// building an `InvalidDeletionTime` (`DeletionTime.java:56-62`), or is the
/// "none" marker.
fn valid_local_deletion_time(t: i64) -> bool {
    t == NO_DELETION_TIME || (0..=MAX_DELETION_TIME).contains(&t)
}

/// Mirrors `DeletionTime` (`DeletionTime.java`).
///
/// Java stores the local deletion time as an unsigned 32-bit integer and
/// compares it unsigned (`DeletionTime.java:171`). The mapping from the
/// `long` view to the unsigned view is monotonic over valid values
/// (`0..=MAX_DELETION_TIME` and `NO_DELETION_TIME` mapping to the largest
/// value), so comparing the `long` view gives the same order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DeletionTime {
    pub marked_for_delete_at: i64,
    pub local_deletion_time: i64,
}

impl Default for DeletionTime {
    fn default() -> Self {
        Self::LIVE
    }
}

impl DeletionTime {
    /// `DeletionTime.LIVE` (`DeletionTime.java:48`).
    pub const LIVE: DeletionTime = DeletionTime {
        marked_for_delete_at: i64::MIN,
        local_deletion_time: NO_DELETION_TIME,
    };

    /// `DeletionTime.isLive()` (`DeletionTime.java:111-114`).
    #[must_use]
    pub fn is_live(self) -> bool {
        self == Self::LIVE
    }

    /// `DeletionTime.supersedes(DeletionTime)` (`DeletionTime.java:169-172`).
    #[must_use]
    pub fn supersedes(self, other: DeletionTime) -> bool {
        self.marked_for_delete_at > other.marked_for_delete_at
            || (self.marked_for_delete_at == other.marked_for_delete_at
                && self.local_deletion_time > other.local_deletion_time)
    }

    /// `DeletionTime.deletes(Cell)` (`DeletionTime.java:195-199`): guarded
    /// against `LIVE`, whose `markedForDeleteAt` equals `NO_TIMESTAMP`.
    #[must_use]
    pub fn deletes_cell(self, cell: &Cell) -> bool {
        self.marked_for_delete_at != i64::MIN && cell.timestamp <= self.marked_for_delete_at
    }

    /// `DeletionTime.deletes(LivenessInfo)` (`DeletionTime.java:174-177`,
    /// `201-204`). Deliberately *not* guarded against `LIVE`; the Java comment
    /// at `DeletionTime.java:186-188` explains why. With `LIVE` it only
    /// "deletes" `LivenessInfo::EMPTY`, which changes nothing.
    #[must_use]
    pub fn deletes_liveness(self, info: LivenessInfo) -> bool {
        info.timestamp <= self.marked_for_delete_at
    }

    /// Whether Java would build this value as a valid `DeletionTime`.
    #[must_use]
    pub fn is_valid(self) -> bool {
        self.is_live()
            || (self.marked_for_delete_at != i64::MIN
                && valid_local_deletion_time(self.local_deletion_time)
                && self.local_deletion_time != NO_DELETION_TIME)
    }
}

/// Mirrors a row's primary-key `LivenessInfo` (`LivenessInfo.java`): the
/// three concrete classes (`ImmutableLivenessInfo`, `ExpiringLivenessInfo`,
/// `ExpiredLivenessInfo`) are told apart by `ttl` as `LivenessInfo.java:91-96`
/// does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LivenessInfo {
    pub timestamp: i64,
    pub ttl: i32,
    pub local_expiration_time: i64,
}

impl LivenessInfo {
    /// `LivenessInfo.EMPTY` (`LivenessInfo.java:53`).
    pub const EMPTY: LivenessInfo = LivenessInfo {
        timestamp: NO_TIMESTAMP,
        ttl: NO_TTL,
        local_expiration_time: NO_DELETION_TIME,
    };

    /// `isExpiring()`: true for `ExpiringLivenessInfo` and its subclass
    /// `ExpiredLivenessInfo` (`LivenessInfo.java:251, 307-310`).
    #[must_use]
    pub fn is_expiring(self) -> bool {
        self.ttl != NO_TTL
    }

    /// `isExpired()` (`LivenessInfo.java:261-264`).
    #[must_use]
    pub fn is_expired(self) -> bool {
        self.ttl == EXPIRED_LIVENESS_TTL
    }

    /// `LivenessInfo.supersedes(LivenessInfo)` (`LivenessInfo.java:197-212`).
    #[must_use]
    pub fn supersedes(self, other: LivenessInfo) -> bool {
        if self.timestamp != other.timestamp {
            return self.timestamp > other.timestamp;
        }
        if self.is_expired() ^ other.is_expired() {
            return self.is_expired();
        }
        if self.is_expiring() == other.is_expiring() {
            return self.local_expiration_time > other.local_expiration_time
                || (self.local_expiration_time == other.local_expiration_time
                    && self.ttl < other.ttl);
        }
        self.is_expiring()
    }

    /// Whether Java could hold this value: `EMPTY`, a plain timestamp, an
    /// expiring info with a valid expiration time, or an expired one.
    #[must_use]
    pub fn is_valid(self) -> bool {
        if self.ttl == NO_TTL {
            return self.local_expiration_time == NO_DELETION_TIME;
        }
        self.ttl > 0
            && self.timestamp != NO_TIMESTAMP
            && valid_local_deletion_time(self.local_expiration_time)
            && self.local_expiration_time != NO_DELETION_TIME
    }
}

/// Mirrors `Row.Deletion` (`rows/Row.java:396-478`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RowDeletion {
    pub time: DeletionTime,
    /// Deprecated in Cassandra (CASSANDRA-10261) but still readable from old
    /// data, so it is carried.
    pub shadowable: bool,
}

impl RowDeletion {
    /// `Row.Deletion.LIVE` (`rows/Row.java:398`).
    pub const LIVE: RowDeletion = RowDeletion {
        time: DeletionTime::LIVE,
        shadowable: false,
    };

    #[must_use]
    pub fn is_live(self) -> bool {
        self.time.is_live()
    }

    /// `Row.Deletion.isShadowedBy(LivenessInfo)` (`rows/Row.java:465-468`).
    #[must_use]
    pub fn is_shadowed_by(self, liveness: LivenessInfo) -> bool {
        self.shadowable && liveness.timestamp > self.time.marked_for_delete_at
    }

    /// `Row.Deletion` asserts a live deletion is never shadowable
    /// (`rows/Row.java:406`).
    #[must_use]
    pub fn is_valid(self) -> bool {
        self.time.is_valid() && !(self.time.is_live() && self.shadowable)
    }
}

/// A regular (non-counter, simple-column) cell.
///
/// The three kinds are told apart as in Java: a live cell has neither a TTL
/// nor a local deletion time, an expiring cell has both, and a tombstone has a
/// local deletion time and no TTL (`rows/CellLivenessInfo.java:37-49`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Cell {
    /// The column's position in the table's column order. Java assigns these
    /// so that comparing ids gives the same order as comparing the columns.
    pub column: u32,
    pub timestamp: i64,
    pub ttl: i32,
    pub local_deletion_time: i64,
    pub value: Box<[u8]>,
}

/// The outcome of the non-counter cell tie-break,
/// `CellLivenessInfo.Resolution` (`rows/CellLivenessInfo.java:69-72`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Resolution {
    Left,
    Right,
    Compare,
}

impl Cell {
    /// Whether Java could build this cell (`BufferCell` asserts that an
    /// expiring cell has a deletion time, `rows/BufferCell.java`).
    #[must_use]
    pub fn is_valid(&self) -> bool {
        if !valid_local_deletion_time(self.local_deletion_time) || self.ttl < 0 {
            return false;
        }
        // An expiring cell must carry the time it expires at.
        !(self.ttl != NO_TTL && self.local_deletion_time == NO_DELETION_TIME)
    }

    /// `CellLivenessInfo.resolve` (`rows/CellLivenessInfo.java:81-94`) and its
    /// same-timestamp tie-break (`rows/CellLivenessInfo.java:104-149`).
    fn resolve(left: &Cell, right: &Cell) -> Resolution {
        if left.timestamp != right.timestamp {
            return if left.timestamp > right.timestamp {
                Resolution::Left
            } else {
                Resolution::Right
            };
        }
        let left_has_ldt = left.local_deletion_time != NO_DELETION_TIME;
        let right_has_ldt = right.local_deletion_time != NO_DELETION_TIME;
        if !left_has_ldt && !right_has_ldt {
            return Resolution::Compare;
        }
        // Tombstones and expiring cells win over live cells of the same
        // timestamp (CASSANDRA-14592, `CellLivenessInfo.java:112-116`).
        if left_has_ldt != right_has_ldt {
            return if left_has_ldt {
                Resolution::Left
            } else {
                Resolution::Right
            };
        }
        // Both carry a deletion time, so "no TTL" means "tombstone"; a
        // tombstone wins over an expiring cell (`CellLivenessInfo.java:118-129`).
        let left_is_tombstone = left.ttl == NO_TTL;
        let right_is_tombstone = right.ttl == NO_TTL;
        if left_is_tombstone != right_is_tombstone {
            return if left_is_tombstone {
                Resolution::Left
            } else {
                Resolution::Right
            };
        }
        // The later local deletion time wins (`CellLivenessInfo.java:135-136`).
        if left.local_deletion_time != right.local_deletion_time {
            return if left.local_deletion_time > right.local_deletion_time {
                Resolution::Left
            } else {
                Resolution::Right
            };
        }
        // Both expiring at the same time: the lower TTL wins
        // (`CellLivenessInfo.java:142-146`).
        if !left_is_tombstone && left.ttl != right.ttl {
            return if left.ttl < right.ttl {
                Resolution::Left
            } else {
                Resolution::Right
            };
        }
        Resolution::Compare
    }

    /// `Cells.reconcile` for regular cells (`rows/Cells.java:68-108`): returns
    /// true if `existing` wins over `update`.
    ///
    /// The final fallback compares the value bytes unsigned and keeps the left
    /// (existing) cell on a tie (`rows/Cells.java:107`, `ValueAccessor.compare`).
    /// The memtable calls `Cells.reconcile(existing, update)` in that order
    /// (`rows/ColumnData.java:120-121`).
    #[must_use]
    pub fn existing_wins(existing: &Cell, update: &Cell) -> bool {
        match Self::resolve(existing, update) {
            Resolution::Left => true,
            Resolution::Right => false,
            Resolution::Compare => existing.value.cmp(&update.value) != Ordering::Less,
        }
    }
}

/// A row: primary-key liveness, row deletion and cells sorted by column id.
/// The clustering is the key of the map that holds the row.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Row {
    pub liveness: LivenessInfo,
    pub deletion: RowDeletion,
    /// Strictly increasing by `column`.
    pub cells: Vec<Cell>,
}

impl Row {
    /// Heap bytes this row owns outside its own inline size (the cell array
    /// and the values), for memory accounting. O(cells in the row).
    #[must_use]
    pub fn heap_bytes(&self) -> usize {
        self.cells.capacity() * size_of::<Cell>()
            + self.cells.iter().map(|c| c.value.len()).sum::<usize>()
    }

    /// Merges `update` into `self` as `BTreeRow.merge(existing, update)` does
    /// (`rows/BTreeRow.java:645-697`), including its asymmetry: the shadowed
    /// cells are filtered only from the side whose deletion lost.
    pub fn merge(&mut self, update: Row) {
        let Row {
            liveness: update_liveness,
            deletion: update_deletion,
            cells: mut update_cells,
        } = update;

        // `BTreeRow.java:654`.
        let mut liveness = if self.liveness.supersedes(update_liveness) {
            self.liveness
        } else {
            update_liveness
        };

        // `BTreeRow.java:656`: ties go to the update. Java then tests
        // `rowDeletion == existing.deletion()` by reference
        // (`BTreeRow.java:667`); an update is a fresh object, so that is true
        // exactly when the existing deletion strictly superseded.
        let existing_deletion_won = self.deletion.time.supersedes(update_deletion.time);
        let mut row_deletion = if existing_deletion_won {
            self.deletion
        } else {
            update_deletion
        };

        // `BTreeRow.java:658-661`.
        if row_deletion.time.deletes_liveness(liveness) {
            liveness = LivenessInfo::EMPTY;
        } else if row_deletion.is_shadowed_by(liveness) {
            row_deletion = RowDeletion::LIVE;
        }

        // `BTreeRow.java:663-686`: drop the cells the winning deletion
        // shadows, from the losing side only.
        let deletion = row_deletion.time;
        if !deletion.is_live() {
            if existing_deletion_won {
                update_cells.retain(|c| !deletion.deletes_cell(c));
            } else {
                self.cells.retain(|c| !deletion.deletes_cell(c));
            }
        }

        // `BTreeRow.java:687` (`BTree.update` with `ColumnData.Reconciler`,
        // `rows/ColumnData.java:116-124`): a sorted merge by column, keeping
        // the reconciled cell where both sides have one.
        self.cells = merge_cells(std::mem::take(&mut self.cells), update_cells);
        self.liveness = liveness;
        self.deletion = row_deletion;
    }
}

impl Row {
    /// The row as a read or flush sees it under the partition deletion
    /// `active`, or `None` if nothing of it remains.
    ///
    /// Java's memtable stores rows unfiltered by the partition deletion and
    /// filters them on every read and flush: memtable partitions report
    /// `canHaveShadowedData`, so `AbstractBTreePartition.merge`
    /// (`partitions/AbstractBTreePartition.java:199-208`) builds a
    /// `RowAndDeletionMergeIterator` that calls
    /// `row.filter(selection, partitionDeletion, false, metadata)`
    /// (`rows/RowAndDeletionMergeIterator.java:180-184`). This is that
    /// filter (`rows/BTreeRow.java:343-395`) with a select-all column filter
    /// and no dropped columns:
    ///
    /// * it acts only if `active` is not live and the row's own deletion does
    ///   not supersede it (`BTreeRow.java:349`);
    /// * then the liveness is cleared if `active` covers its timestamp, the
    ///   row deletion is dropped (`setActiveDeletionToRow` is false), and the
    ///   cells `active` deletes are dropped (`BTreeRow.java:356-389`);
    /// * a row left with no liveness, no deletion and no cells disappears
    ///   (`BTreeRow.java:536-537`).
    #[must_use]
    pub fn visible_under(&self, active: DeletionTime) -> Option<Cow<'_, Row>> {
        if active.is_live() || self.deletion.time.supersedes(active) {
            return Some(Cow::Borrowed(self));
        }
        let liveness = if active.deletes_liveness(self.liveness) {
            LivenessInfo::EMPTY
        } else {
            self.liveness
        };
        let cells: Vec<Cell> = self
            .cells
            .iter()
            .filter(|c| !active.deletes_cell(c))
            .cloned()
            .collect();
        if liveness.timestamp == NO_TIMESTAMP && cells.is_empty() {
            return None;
        }
        Some(Cow::Owned(Row {
            liveness,
            deletion: RowDeletion::LIVE,
            cells,
        }))
    }
}

/// Sorted merge of two cell lists, each strictly increasing by column.
fn merge_cells(existing: Vec<Cell>, update: Vec<Cell>) -> Vec<Cell> {
    if update.is_empty() {
        return existing;
    }
    let mut out = Vec::with_capacity(existing.len() + update.len());
    let mut existing = existing.into_iter().peekable();
    let mut update = update.into_iter().peekable();
    loop {
        let next = match (existing.peek(), update.peek()) {
            (None, None) => break,
            (Some(_), None) => existing.next(),
            (None, Some(_)) => update.next(),
            (Some(e), Some(u)) => match e.column.cmp(&u.column) {
                Ordering::Less => existing.next(),
                Ordering::Greater => update.next(),
                Ordering::Equal => {
                    let (e, u) = (existing.next(), update.next());
                    match (e, u) {
                        (Some(e), Some(u)) => Some(if Cell::existing_wins(&e, &u) { e } else { u }),
                        // Both were just peeked as present.
                        _ => unreachable!("peeked cells vanished"),
                    }
                }
            },
        };
        out.extend(next);
    }
    out.shrink_to_fit();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live(column: u32, timestamp: i64, value: &[u8]) -> Cell {
        Cell {
            column,
            timestamp,
            ttl: NO_TTL,
            local_deletion_time: NO_DELETION_TIME,
            value: value.into(),
        }
    }

    fn tombstone(column: u32, timestamp: i64, ldt: i64) -> Cell {
        Cell {
            column,
            timestamp,
            ttl: NO_TTL,
            local_deletion_time: ldt,
            value: Box::default(),
        }
    }

    fn expiring(column: u32, timestamp: i64, ttl: i32, ldt: i64, value: &[u8]) -> Cell {
        Cell {
            column,
            timestamp,
            ttl,
            local_deletion_time: ldt,
            value: value.into(),
        }
    }

    fn row(deletion: RowDeletion, cells: Vec<Cell>) -> Row {
        Row {
            liveness: LivenessInfo::EMPTY,
            deletion,
            cells,
        }
    }

    fn deleted_at(ts: i64) -> RowDeletion {
        RowDeletion {
            time: DeletionTime {
                marked_for_delete_at: ts,
                local_deletion_time: 100,
            },
            shadowable: false,
        }
    }

    #[test]
    fn newer_timestamp_wins() {
        assert!(Cell::existing_wins(&live(0, 2, b"a"), &live(0, 1, b"z")));
        assert!(!Cell::existing_wins(&live(0, 1, b"z"), &live(0, 2, b"a")));
    }

    #[test]
    fn same_timestamp_tombstone_beats_live_and_expiring() {
        let t = tombstone(0, 5, 10);
        assert!(!Cell::existing_wins(&live(0, 5, b"zz"), &t));
        assert!(!Cell::existing_wins(&expiring(0, 5, 3, 99, b"zz"), &t));
        // An expiring cell beats a live one regardless of value (CASSANDRA-14592).
        assert!(Cell::existing_wins(
            &expiring(0, 5, 3, 1, b""),
            &live(0, 5, b"zz")
        ));
    }

    #[test]
    fn same_timestamp_expiring_ties() {
        // Later expiry wins; at equal expiry the lower TTL wins; then the value.
        assert!(Cell::existing_wins(
            &expiring(0, 5, 9, 20, b"a"),
            &expiring(0, 5, 1, 19, b"z")
        ));
        assert!(Cell::existing_wins(
            &expiring(0, 5, 1, 20, b"a"),
            &expiring(0, 5, 9, 20, b"z")
        ));
        assert!(!Cell::existing_wins(
            &expiring(0, 5, 3, 20, b"a"),
            &expiring(0, 5, 3, 20, b"b")
        ));
    }

    #[test]
    fn same_timestamp_live_cells_compare_values_unsigned() {
        assert!(Cell::existing_wins(
            &live(0, 1, &[0x80]),
            &live(0, 1, &[0x7f])
        ));
        assert!(!Cell::existing_wins(
            &live(0, 1, &[0x01]),
            &live(0, 1, &[0x01, 0x00])
        ));
    }

    #[test]
    fn update_deletion_filters_existing_cells() {
        let mut existing = row(RowDeletion::LIVE, vec![live(0, 5, b"a"), live(1, 15, b"b")]);
        existing.merge(row(deleted_at(10), vec![]));
        assert_eq!(existing.cells, vec![live(1, 15, b"b")]);
        assert_eq!(existing.deletion, deleted_at(10));
    }

    #[test]
    fn existing_deletion_filters_only_the_update() {
        // A new row is inserted unfiltered, so a stale cell can sit beside
        // its own deletion; a later merge that the existing deletion wins
        // filters only the update side and keeps the stale cell, as
        // BTreeRow.merge does.
        let mut existing = row(deleted_at(10), vec![live(0, 5, b"stale")]);
        existing.merge(row(
            RowDeletion::LIVE,
            vec![live(1, 7, b"x"), live(2, 11, b"y")],
        ));
        assert_eq!(
            existing.cells,
            vec![live(0, 5, b"stale"), live(2, 11, b"y")]
        );
    }

    #[test]
    fn equal_deletions_go_to_the_update_and_filter_existing() {
        let mut existing = row(deleted_at(10), vec![live(0, 5, b"stale")]);
        existing.merge(row(deleted_at(10), vec![]));
        assert_eq!(existing.cells, Vec::<Cell>::new());
    }

    #[test]
    fn row_deletion_clears_older_liveness() {
        let mut existing = row(RowDeletion::LIVE, vec![]);
        existing.liveness = LivenessInfo {
            timestamp: 3,
            ..LivenessInfo::EMPTY
        };
        existing.merge(row(deleted_at(3), vec![]));
        assert_eq!(existing.liveness, LivenessInfo::EMPTY);
    }

    #[test]
    fn newer_liveness_cancels_shadowable_deletion() {
        let shadowable = RowDeletion {
            shadowable: true,
            ..deleted_at(10)
        };
        let mut existing = row(shadowable, vec![live(0, 5, b"a")]);
        let mut update = row(RowDeletion::LIVE, vec![]);
        update.liveness = LivenessInfo {
            timestamp: 11,
            ..LivenessInfo::EMPTY
        };
        existing.merge(update);
        assert_eq!(existing.deletion, RowDeletion::LIVE);
        // The deletion became live before filtering, so the old cell survives.
        assert_eq!(existing.cells, vec![live(0, 5, b"a")]);
    }

    #[test]
    fn liveness_supersedes_rules() {
        let plain = |ts| LivenessInfo {
            timestamp: ts,
            ..LivenessInfo::EMPTY
        };
        let expiring = |ts, ttl, let_| LivenessInfo {
            timestamp: ts,
            ttl,
            local_expiration_time: let_,
        };
        assert!(plain(2).supersedes(plain(1)));
        assert!(expiring(1, 5, 10).supersedes(plain(1)));
        assert!(!plain(1).supersedes(expiring(1, 5, 10)));
        assert!(expiring(1, EXPIRED_LIVENESS_TTL, 3).supersedes(expiring(1, 5, 10)));
        assert!(expiring(1, 5, 11).supersedes(expiring(1, 6, 10)));
        assert!(expiring(1, 5, 10).supersedes(expiring(1, 6, 10)));
        assert!(!plain(1).supersedes(plain(1)));
    }

    #[test]
    fn deletion_time_rules() {
        let d = |m, l| DeletionTime {
            marked_for_delete_at: m,
            local_deletion_time: l,
        };
        assert!(d(2, 0).supersedes(d(1, 9)));
        assert!(d(1, 9).supersedes(d(1, 8)));
        assert!(!d(1, 9).supersedes(d(1, 9)));
        assert!(d(i64::MIN + 1, 0).supersedes(DeletionTime::LIVE));
        assert!(!DeletionTime::LIVE.deletes_cell(&live(0, i64::MIN, b"")));
        assert!(DeletionTime::LIVE.deletes_liveness(LivenessInfo::EMPTY));
        assert!(d(5, 0).deletes_cell(&live(0, 5, b"")));
        assert!(!d(5, 0).deletes_cell(&live(0, 6, b"")));
    }
}
