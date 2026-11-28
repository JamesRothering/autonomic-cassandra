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

//! Checks the memtable's owned-bytes estimate against the bytes it really
//! holds, measured by a counting global allocator (requested sizes; malloc's
//! own per-block overhead is not included).
//!
//! This test binary is a separate crate from `memtable-core`, so the core's
//! `forbid(unsafe_code)` does not cover it: implementing `GlobalAlloc` needs
//! `unsafe`. It runs single-threaded (one test function) so that the counter
//! sees only the memtable's allocations.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};

use memtable_core::model::{NO_DELETION_TIME, NO_TTL};
use memtable_core::{
    Cell, DeletionTime, LivenessInfo, Memtable, PartitionUpdate, Row, RowDeletion,
};

struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);

fn signed(n: usize) -> isize {
    isize::try_from(n).unwrap_or(isize::MAX)
}

// SAFETY: every method forwards to `System` with the caller's arguments
// unchanged, so `System`'s guarantees carry over; the counter is a side
// effect that does not touch the memory.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LIVE.fetch_add(signed(layout.size()), Ordering::Relaxed);
        // SAFETY: forwarded unchanged (see the impl comment).
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(signed(layout.size()), Ordering::Relaxed);
        // SAFETY: forwarded unchanged (see the impl comment).
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        LIVE.fetch_add(signed(new_size) - signed(layout.size()), Ordering::Relaxed);
        // SAFETY: forwarded unchanged (see the impl comment).
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn update(key: u32, clustering: u32, cells: u32, value_len: usize) -> PartitionUpdate {
    PartitionUpdate {
        key: [&[0x40][..], &key.to_be_bytes(), &[0u8; 11][..]]
            .concat()
            .into(),
        deletion: DeletionTime::LIVE,
        rows: vec![(
            clustering.to_be_bytes().to_vec().into(),
            Row {
                liveness: LivenessInfo::EMPTY,
                deletion: RowDeletion::LIVE,
                cells: (0..cells)
                    .map(|column| Cell {
                        column,
                        timestamp: 1,
                        ttl: NO_TTL,
                        local_deletion_time: NO_DELETION_TIME,
                        value: vec![7; value_len].into(),
                    })
                    .collect(),
            },
        )],
    }
}

/// Returns (estimated, measured) bytes for the updates.
type Workload = Box<dyn Fn() -> Box<dyn Iterator<Item = PartitionUpdate>>>;

fn measure(updates: impl Iterator<Item = PartitionUpdate>) -> (i64, i64) {
    let boundaries = (1..16u8).map(|i| vec![0x40, i * 16].into()).collect();
    let m = Memtable::new(boundaries).expect("sorted boundaries");
    let before = LIVE.load(Ordering::Relaxed);
    for u in updates {
        m.apply(u).expect("not frozen");
    }
    let measured = LIVE.load(Ordering::Relaxed) - before;
    (m.owned_bytes(), i64::try_from(measured).unwrap_or(i64::MAX))
}

#[test]
#[cfg_attr(miri, ignore = "allocation-heavy; covered natively")]
fn owned_bytes_estimate_is_close_to_real_allocation() {
    let cases: Vec<(&str, Workload)> = vec![
        (
            "100k one-row partitions, 3x32B cells",
            Box::new(|| {
                Box::new((0..100_000u32).map(|k| update(k.wrapping_mul(2_654_435_761), 0, 3, 32)))
            }),
        ),
        (
            "1k partitions x 100 rows, 3x32B cells",
            Box::new(|| {
                Box::new(
                    (0..100_000u32)
                        .map(|i| update((i % 1000).wrapping_mul(2_654_435_761), i / 1000, 3, 32)),
                )
            }),
        ),
        (
            "10k partitions x 10 rows, 1x512B cell",
            Box::new(|| {
                Box::new(
                    (0..100_000u32).map(|i| {
                        update((i % 10_000).wrapping_mul(2_654_435_761), i / 10_000, 1, 512)
                    }),
                )
            }),
        ),
        (
            "overwrites: 1k rows written 100 times",
            Box::new(|| {
                Box::new(
                    (0..100_000u32)
                        .map(|i| update((i % 1000).wrapping_mul(2_654_435_761), 0, 3, 32)),
                )
            }),
        ),
    ];
    let mut worst = 0.0f64;
    for (name, make) in &cases {
        let (estimated, measured) = measure(make());
        #[allow(clippy::cast_precision_loss)] // byte counts far below 2^52
        let ratio = estimated as f64 / measured as f64;
        println!("{name:42} estimated {estimated:>11} measured {measured:>11} ratio {ratio:.2}");
        worst = worst.max((ratio - 1.0).abs());
    }
    assert!(
        worst < 0.25,
        "estimate off by {:.0}% in the worst case",
        worst * 100.0
    );
}
