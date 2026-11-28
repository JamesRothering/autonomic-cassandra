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

//! Property tests of the core. The byte-for-byte comparison with Cassandra's
//! own memtable is the Java differential test; these check the properties
//! that hold independently of Java.

use std::collections::BTreeSet;
use std::sync::Arc;

use memtable_core::model::{EXPIRED_LIVENESS_TTL, NO_DELETION_TIME, NO_TTL};
use memtable_core::{
    Cell, DeletionTime, LivenessInfo, Memtable, PartitionUpdate, Row, RowDeletion, boxed,
    decode_partition, decode_partitions, encode_update,
};
use proptest::prelude::*;

/// Fewer cases under Miri, which is ~1000x slower, and no failure file
/// (Miri isolates the file system).
fn config() -> ProptestConfig {
    if cfg!(miri) {
        ProptestConfig {
            cases: 4,
            failure_persistence: None,
            ..ProptestConfig::default()
        }
    } else {
        ProptestConfig::with_cases(512)
    }
}

// Small domains, so timestamps, deletion times and values collide often.
fn timestamp() -> impl Strategy<Value = i64> {
    0i64..4
}

fn ldt() -> impl Strategy<Value = i64> {
    prop_oneof![Just(0i64), Just(1), Just(4_294_967_293)]
}

fn value() -> impl Strategy<Value = Box<[u8]>> {
    prop::collection::vec(prop_oneof![Just(0u8), Just(1), Just(0xff)], 0..3)
        .prop_map(Vec::into_boxed_slice)
}

fn cell(column: u32) -> impl Strategy<Value = Cell> {
    let kind = prop_oneof![
        Just((NO_TTL, None)),
        (1i32..3, ldt()).prop_map(|(t, l)| (t, Some(l))),
        ldt().prop_map(|l| (NO_TTL, Some(l))),
    ];
    (timestamp(), kind, value()).prop_map(move |(timestamp, (ttl, l), value)| Cell {
        column,
        timestamp,
        ttl,
        local_deletion_time: l.unwrap_or(NO_DELETION_TIME),
        // Tombstones carry no value.
        value: if ttl == NO_TTL && l.is_some() {
            Box::default()
        } else {
            value
        },
    })
}

fn deletion() -> impl Strategy<Value = DeletionTime> {
    prop_oneof![
        2 => Just(DeletionTime::LIVE),
        1 => (timestamp(), ldt()).prop_map(|(m, l)| DeletionTime {
            marked_for_delete_at: m,
            local_deletion_time: l.min(4_294_967_293),
        }),
    ]
}

fn liveness() -> impl Strategy<Value = LivenessInfo> {
    prop_oneof![
        Just(LivenessInfo::EMPTY),
        timestamp().prop_map(|timestamp| LivenessInfo {
            timestamp,
            ..LivenessInfo::EMPTY
        }),
        (
            timestamp(),
            prop_oneof![Just(1i32), Just(2), Just(EXPIRED_LIVENESS_TTL)],
            ldt()
        )
            .prop_map(|(timestamp, ttl, l)| LivenessInfo {
                timestamp,
                ttl,
                local_expiration_time: l.min(4_294_967_293)
            }),
    ]
}

fn row() -> impl Strategy<Value = Row> {
    let cells = prop::collection::btree_map(0u32..4, timestamp(), 0..4)
        .prop_flat_map(|cols| cols.keys().map(|&c| cell(c)).collect::<Vec<_>>());
    (liveness(), deletion(), any::<bool>(), cells).prop_map(
        |(liveness, time, shadowable, cells)| Row {
            liveness,
            deletion: RowDeletion {
                time,
                shadowable: shadowable && !time.is_live(),
            },
            cells,
        },
    )
}

fn key() -> impl Strategy<Value = Box<[u8]>> {
    prop::collection::vec(0u8..4, 1..3).prop_map(Vec::into_boxed_slice)
}

fn update() -> impl Strategy<Value = PartitionUpdate> {
    (
        key(),
        deletion(),
        prop::collection::btree_map(key(), row(), 0..3),
    )
        .prop_map(|(key, deletion, rows)| PartitionUpdate {
            key,
            deletion,
            rows: rows.into_iter().collect(),
        })
}

fn boundaries() -> Vec<Box<[u8]>> {
    vec![vec![1].into(), vec![2, 1].into(), vec![3].into()]
}

/// Reads every partition through the flush iterator, in batches of at most
/// `batch` bytes.
fn flush_all(m: &Arc<Memtable>, batch: usize) -> Vec<PartitionUpdate> {
    m.freeze().unwrap();
    let mut it = m.flush_iter().unwrap();
    let mut all = Vec::new();
    loop {
        let mut out = Vec::new();
        let mut n = it.next_batch(&mut out, batch).unwrap();
        if n == 0 && !out.is_empty() {
            // One record larger than the batch: retry with room for it.
            let need = out.len();
            out.clear();
            n = it.next_batch(&mut out, need).unwrap();
            assert_eq!(n, 1);
        }
        if n == 0 {
            return all;
        }
        all.extend(decode_partitions(&out).unwrap());
    }
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn codec_round_trips(u in update()) {
        let mut buf = Vec::new();
        encode_update(&mut buf, &u).unwrap();
        prop_assert_eq!(decode_partition(&buf).unwrap(), u);
    }

    #[test]
    fn decoder_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..200)) {
        let _ = decode_partition(&bytes);
        let _ = decode_partitions(&bytes);
    }

    #[test]
    fn decoder_rejects_every_truncation(u in update()) {
        let mut buf = Vec::new();
        encode_update(&mut buf, &u).unwrap();
        for len in 0..buf.len() {
            prop_assert!(decode_partition(&buf[..len]).is_err());
        }
    }

    /// `Cells.reconcile` is documented as commutative (`rows/Cells.java:59`):
    /// whichever order two cells meet in, the same cell survives.
    #[test]
    fn cell_reconcile_is_commutative(a in cell(0), b in cell(0)) {
        let ab = if Cell::existing_wins(&a, &b) { &a } else { &b };
        let ba = if Cell::existing_wins(&b, &a) { &b } else { &a };
        prop_assert_eq!(ab, ba);
    }

    /// Without deletions, merging is a per-cell maximum, so the result does
    /// not depend on the order of the updates.
    #[test]
    fn cell_only_merge_is_order_independent(cells in prop::collection::vec(cell(0), 1..6)) {
        let fold = |cells: &[Cell]| {
            let mut row = Row { liveness: LivenessInfo::EMPTY, deletion: RowDeletion::LIVE, cells: vec![] };
            for c in cells {
                row.merge(Row { liveness: LivenessInfo::EMPTY, deletion: RowDeletion::LIVE, cells: vec![c.clone()] });
            }
            row
        };
        let mut reversed = cells.clone();
        reversed.reverse();
        prop_assert_eq!(fold(&cells), fold(&reversed));
    }

    #[test]
    fn supersedes_is_asymmetric(a in liveness(), b in liveness(), c in deletion(), d in deletion()) {
        prop_assert!(!(a.supersedes(b) && b.supersedes(a)));
        prop_assert!(!(c.supersedes(d) && d.supersedes(c)));
    }

    /// The incremental byte count equals a full recount, and reads, the flush
    /// iterator (at several batch sizes) and a plain map of the last state
    /// agree on which partitions exist and in what order.
    #[test]
    fn memtable_reads_flush_and_accounting_agree(updates in prop::collection::vec(update(), 1..20), batch in 1usize..400) {
        let m = Arc::new(Memtable::new(boundaries()).unwrap());
        let mut keys = BTreeSet::new();
        let mut total = 0;
        for u in updates {
            keys.insert(u.key.clone());
            total += m.apply(u).unwrap();
        }
        prop_assert_eq!(total, m.owned_bytes());
        prop_assert_eq!(m.recount_owned_bytes().unwrap(), m.owned_bytes());
        prop_assert_eq!(m.partition_count(), keys.len());
        let space = m.space().unwrap();
        prop_assert!(space.live <= space.used && space.used <= space.allocated, "{:?}", space);
        prop_assert_eq!(i64::try_from(space.allocated).unwrap(), m.owned_bytes());

        let mut reads = Vec::new();
        for k in &keys {
            let mut out = Vec::new();
            prop_assert!(m.read(k, &[], None, &mut out).unwrap());
            reads.push(decode_partition(&out).unwrap());
        }
        let flushed = flush_all(&m, batch);
        prop_assert_eq!(&flushed, &reads);
        let flushed_keys: Vec<_> = flushed.iter().map(|p| p.key.clone()).collect();
        prop_assert_eq!(flushed_keys, keys.into_iter().collect::<Vec<_>>());
    }

    /// A slice read returns exactly the rows of a full read within the bounds.
    #[test]
    fn slice_reads_filter_rows(updates in prop::collection::vec(update(), 1..10), start in key(), end in proptest::option::of(key())) {
        let m = Memtable::new(boundaries()).unwrap();
        for u in updates {
            m.apply(u).unwrap();
        }
        let probe = vec![1u8].into_boxed_slice();
        let mut full = Vec::new();
        if !m.read(&probe, &[], None, &mut full).unwrap() {
            return Ok(());
        }
        let full = decode_partition(&full).unwrap();
        let mut out = Vec::new();
        prop_assert!(m.read(&probe, &start, end.as_deref(), &mut out).unwrap());
        let sliced = decode_partition(&out).unwrap();
        let expected: Vec<_> = full.rows.into_iter()
            .filter(|(c, _)| **c >= *start && end.as_ref().is_none_or(|e| c < e))
            .collect();
        prop_assert_eq!(sliced.rows, expected);
        prop_assert_eq!(sliced.deletion, full.deletion);
    }
}

/// Every byte a flush iterator returns, in batches of at most `batch` bytes.
fn flush_bytes_boxed(m: &Arc<boxed::Memtable>, batch: usize) -> Vec<u8> {
    m.freeze().unwrap();
    let mut it = m.flush_iter().unwrap();
    let mut all = Vec::new();
    let mut limit = batch;
    loop {
        let mut out = Vec::new();
        let n = it.next_batch(&mut out, limit).unwrap();
        if n == 0 && !out.is_empty() {
            limit = out.len();
            continue;
        }
        if n == 0 {
            return all;
        }
        all.extend(out);
        limit = batch;
    }
}

fn flush_bytes(m: &Arc<Memtable>, batch: usize) -> Vec<u8> {
    m.freeze().unwrap();
    let mut it = m.flush_iter().unwrap();
    let mut all = Vec::new();
    let mut limit = batch;
    loop {
        let mut out = Vec::new();
        let n = it.next_batch(&mut out, limit).unwrap();
        if n == 0 && !out.is_empty() {
            limit = out.len();
            continue;
        }
        if n == 0 {
            return all;
        }
        all.extend(out);
        limit = batch;
    }
}

fn wide_update() -> impl Strategy<Value = PartitionUpdate> {
    // Up to 40 rows, so partitions cross the boxed memtable's switch from a
    // sorted Vec to a BTreeMap (16 rows) and fill several B-tree nodes.
    (
        key(),
        deletion(),
        prop::collection::btree_map(
            prop::collection::vec(0u8..8, 1..3).prop_map(Vec::into_boxed_slice),
            row(),
            0..40,
        ),
    )
        .prop_map(|(key, deletion, rows)| PartitionUpdate {
            key,
            deletion,
            rows: rows.into_iter().collect(),
        })
}

proptest! {
    #![proptest_config(config())]

    /// The arena memtable and the boxed one (spike 1, checked byte for byte
    /// against Cassandra) return identical bytes for every whole read, slice
    /// read and flush of the same updates.
    #[test]
    fn arena_and_boxed_agree(
        updates in prop::collection::vec(prop_oneof![3 => update(), 1 => wide_update()], 1..60),
        slices in prop::collection::vec((key(), key(), proptest::option::of(key())), 0..8),
        batch in 1usize..2000,
    ) {
        let arena = Arc::new(Memtable::new(boundaries()).unwrap());
        let boxed = Arc::new(boxed::Memtable::new(boundaries()).unwrap());
        let mut keys = BTreeSet::new();
        for u in updates {
            keys.insert(u.key.clone());
            arena.apply(u.clone()).unwrap();
            boxed.apply(u).unwrap();
        }
        prop_assert_eq!(arena.partition_count(), boxed.partition_count());
        for k in &keys {
            let (mut a, mut b) = (Vec::new(), Vec::new());
            prop_assert!(arena.read(k, &[], None, &mut a).unwrap());
            prop_assert!(boxed.read(k, &[], None, &mut b).unwrap());
            prop_assert_eq!(a, b);
        }
        for (k, start, end) in &slices {
            let (mut a, mut b) = (Vec::new(), Vec::new());
            let found = arena.read(k, start, end.as_deref(), &mut a).unwrap();
            prop_assert_eq!(found, boxed.read(k, start, end.as_deref(), &mut b).unwrap());
            prop_assert_eq!(a, b);
        }
        prop_assert_eq!(flush_bytes(&arena, batch), flush_bytes_boxed(&boxed, batch));
    }
}

#[test]
fn writes_after_freeze_are_rejected_and_iterator_outlives_owner() {
    let m = Arc::new(Memtable::new(boundaries()).unwrap());
    let u = PartitionUpdate {
        key: vec![2].into(),
        deletion: DeletionTime::LIVE,
        rows: vec![],
    };
    assert!(m.flush_iter().is_err());
    m.apply(u.clone()).unwrap();
    m.freeze().unwrap();
    assert_eq!(m.apply(u), Err(memtable_core::MemtableError::Frozen));
    let mut it = m.flush_iter().unwrap();
    // The owner lets go first; the iterator keeps the data alive.
    drop(m);
    let mut out = Vec::new();
    assert_eq!(it.next_batch(&mut out, 1 << 20).unwrap(), 1);
    out.clear();
    assert_eq!(it.next_batch(&mut out, 1 << 20).unwrap(), 0);
    assert_eq!(out, Vec::<u8>::new());
}

#[test]
fn bad_boundaries_are_rejected() {
    assert!(Memtable::new(vec![vec![2].into(), vec![1].into()]).is_err());
    assert!(Memtable::new(vec![vec![1].into(), vec![1].into()]).is_err());
}

/// Concurrent cell-only writes from several threads give the same contents
/// as applying them on one thread, because cell reconciliation is
/// commutative and associative.
#[test]
fn concurrent_cell_writes_match_sequential() {
    let threads = 4;
    let per_thread = if cfg!(miri) { 20 } else { 5000 };
    let make = |t: usize, i: usize| {
        let n = t * 7919 + i * 104_729;
        let column = u32::try_from(n % 3).unwrap();
        PartitionUpdate {
            key: vec![u8::try_from(n % 5).unwrap()].into(),
            deletion: DeletionTime::LIVE,
            rows: vec![(
                vec![u8::try_from(n % 7).unwrap()].into(),
                Row {
                    liveness: LivenessInfo::EMPTY,
                    deletion: RowDeletion::LIVE,
                    cells: vec![Cell {
                        column,
                        timestamp: i64::try_from(n % 11).unwrap(),
                        ttl: NO_TTL,
                        local_deletion_time: NO_DELETION_TIME,
                        value: vec![u8::try_from(n % 13).unwrap()].into(),
                    }],
                },
            )],
        }
    };
    let concurrent = Arc::new(Memtable::new(boundaries()).unwrap());
    std::thread::scope(|s| {
        for t in 0..threads {
            let m = &concurrent;
            s.spawn(move || {
                for i in 0..per_thread {
                    m.apply(make(t, i)).unwrap();
                }
            });
        }
    });
    let sequential = Arc::new(Memtable::new(boundaries()).unwrap());
    for t in (0..threads).rev() {
        for i in 0..per_thread {
            sequential.apply(make(t, i)).unwrap();
        }
    }
    assert_eq!(
        concurrent.owned_bytes(),
        concurrent.recount_owned_bytes().unwrap()
    );
    assert_eq!(flush_all(&concurrent, 4096), flush_all(&sequential, 4096));
}
