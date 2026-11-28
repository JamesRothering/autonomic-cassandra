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

//! A partition's rows, ordered by clustering bytes.
//!
//! Most partitions in a memtable hold one or a few rows, and an empty std
//! `BTreeMap` allocates a whole leaf node (11 slots, about 1.1 KiB here) on
//! its first insert. So a partition starts as a sorted `Vec` sized exactly to
//! its rows and becomes a `BTreeMap` once it exceeds [`SMALL_LIMIT`] rows, where
//! inserting into the middle of a `Vec` would start to cost. The `accounting`
//! integration test measured the difference: 1.3 KB per one-row partition with
//! a `BTreeMap` alone.

use std::collections::BTreeMap;
use std::collections::btree_map::{self, Entry};
use std::mem::size_of;
use std::ops::Bound;

use crate::model::Row;

/// Rows above which a partition switches to a `BTreeMap`.
pub const SMALL_LIMIT: usize = 16;

type Clustering = Box<[u8]>;

#[derive(Debug)]
pub enum Rows {
    /// Strictly increasing by clustering; capacity equals length.
    Small(Vec<(Clustering, Row)>),
    Large(BTreeMap<Clustering, Row>),
}

impl Default for Rows {
    fn default() -> Self {
        Self::Small(Vec::new())
    }
}

/// Estimated bytes one `BTreeMap` entry costs: std's B-tree nodes hold up to
/// 11 key/value slots and, under random inserts, run about two thirds full,
/// so each entry pays roughly 1.5 times its inline size. An estimate, checked
/// by the `accounting` integration test.
pub const fn map_entry_bytes<K, V>() -> usize {
    (size_of::<K>() + size_of::<V>()) * 3 / 2
}

/// Converts a byte count to a signed delta. Counts are bounded by the
/// address space, far below `i64::MAX`.
pub fn signed(n: usize) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

fn data_bytes(clustering: &[u8], row: &Row) -> usize {
    clustering.len() + row.heap_bytes()
}

impl Rows {
    /// Bytes of the container itself, excluding the rows' heap data. O(1).
    fn structure_bytes(&self) -> usize {
        match self {
            Self::Small(v) => v.capacity() * size_of::<(Clustering, Row)>(),
            Self::Large(m) => m.len() * map_entry_bytes::<Clustering, Row>(),
        }
    }

    /// All bytes owned: container and row data. O(rows).
    pub fn heap_bytes(&self) -> usize {
        self.structure_bytes() + self.iter().map(|(c, r)| data_bytes(c, r)).sum::<usize>()
    }

    /// Inserts `row`, or merges it into the existing row with the same
    /// clustering by [`Row::merge`]. Returns the change in owned bytes.
    pub fn upsert(&mut self, clustering: Clustering, row: Row) -> i64 {
        let structure_before = self.structure_bytes();
        let data_delta = match self {
            Self::Small(v) => match v.binary_search_by(|(c, _)| (**c).cmp(&*clustering)) {
                Ok(i) => merge_counted(&mut v[i].1, row),
                Err(i) => {
                    let added = signed(data_bytes(&clustering, &row));
                    // Exact growth keeps capacity equal to length; at most
                    // SMALL_LIMIT elements are moved per insert.
                    v.reserve_exact(1);
                    v.insert(i, (clustering, row));
                    added
                }
            },
            Self::Large(m) => match m.entry(clustering) {
                Entry::Occupied(mut o) => merge_counted(o.get_mut(), row),
                Entry::Vacant(v) => {
                    let added = signed(data_bytes(v.key(), &row));
                    v.insert(row);
                    added
                }
            },
        };
        if let Self::Small(v) = self
            && v.len() > SMALL_LIMIT
        {
            *self = Self::Large(std::mem::take(v).into_iter().collect());
        }
        data_delta + signed(self.structure_bytes()) - signed(structure_before)
    }

    /// The rows with `start <= clustering < end` (`None`: no upper bound).
    pub fn range<'a>(&'a self, start: &[u8], end: Option<&[u8]>) -> RowsIter<'a> {
        if end.is_some_and(|e| e <= start) {
            // `BTreeMap::range` panics on an inverted range; it is just empty.
            return RowsIter::Small([].iter());
        }
        match self {
            Self::Small(v) => {
                let from = v.partition_point(|(c, _)| **c < *start);
                let to = end.map_or(v.len(), |e| v.partition_point(|(c, _)| **c < *e));
                RowsIter::Small(v[from..to].iter())
            }
            Self::Large(m) => {
                let upper = end.map_or(Bound::Unbounded, Bound::Excluded);
                RowsIter::Large(m.range::<[u8], _>((Bound::Included(start), upper)))
            }
        }
    }

    pub fn iter(&self) -> RowsIter<'_> {
        self.range(&[], None)
    }
}

fn merge_counted(existing: &mut Row, update: Row) -> i64 {
    let before = existing.heap_bytes();
    existing.merge(update);
    signed(existing.heap_bytes()) - signed(before)
}

/// Iterator over [`Rows::range`], in clustering order.
pub enum RowsIter<'a> {
    Small(std::slice::Iter<'a, (Clustering, Row)>),
    Large(btree_map::Range<'a, Clustering, Row>),
}

impl<'a> Iterator for RowsIter<'a> {
    type Item = (&'a [u8], &'a Row);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Small(it) => it.next().map(|(c, r)| (&**c, r)),
            Self::Large(it) => it.next().map(|(c, r)| (&**c, r)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{LivenessInfo, RowDeletion};

    fn row(ts: i64) -> Row {
        Row {
            liveness: LivenessInfo {
                timestamp: ts,
                ..LivenessInfo::EMPTY
            },
            deletion: RowDeletion::LIVE,
            cells: vec![],
        }
    }

    #[test]
    fn small_and_large_agree_and_switch_once() {
        let mut rows = Rows::default();
        let mut total = 0;
        // Insert in a scrambled order, past the switch, with repeats.
        // 17 is coprime with 40, so this visits 0..40 once each, out of order.
        for i in (0..40u32)
            .map(|i| u8::try_from(i * 17 % 40).unwrap())
            .chain(0..5)
        {
            total += rows.upsert(vec![i].into(), row(i64::from(i)));
            let small = matches!(rows, Rows::Small(_));
            let len = rows.iter().count();
            assert_eq!(small, len <= SMALL_LIMIT);
            assert_eq!(total, signed(rows.heap_bytes()));
        }
        let keys: Vec<u8> = rows.iter().map(|(c, _)| c[0]).collect();
        assert_eq!(keys, (0..40).collect::<Vec<_>>());
        let slice: Vec<u8> = rows.range(&[10], Some(&[13])).map(|(c, _)| c[0]).collect();
        assert_eq!(slice, vec![10, 11, 12]);
        assert_eq!(rows.range(&[13], Some(&[10])).count(), 0);
    }
}
