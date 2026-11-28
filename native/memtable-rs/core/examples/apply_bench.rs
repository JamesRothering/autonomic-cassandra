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

//! The Rust-only cost of the operations the Java benchmark times through JNI:
//! decode + apply of a one-row update, and a whole-partition read.
//!
//! `cargo run --release --example apply_bench`
//!
//! Updates mirror `RustMemtableBench`: a 20-byte key (the size of a Murmur3
//! decorated key's byte-comparable form with an 8-byte key), a 6-byte
//! clustering, three 32-byte live cells. Keys are random, so most updates
//! create a partition.

use std::sync::Arc;
use std::time::Instant;

use memtable_core::model::{NO_DELETION_TIME, NO_TTL};
use memtable_core::{
    Cell, DeletionTime, LivenessInfo, Memtable, PartitionUpdate, Row, RowDeletion,
    decode_partition, encode_update,
};

/// A small deterministic generator (xorshift64*), to avoid a dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn bytes(&mut self, n: usize) -> Box<[u8]> {
        (0..n).map(|_| self.next().to_le_bytes()[0]).collect()
    }
}

fn records(n: usize, seed: u64) -> Vec<Vec<u8>> {
    let mut rng = Rng(seed);
    (0..n)
        .map(|i| {
            let mut key = rng.bytes(20);
            key[0] = 0x40;
            let update = PartitionUpdate {
                key,
                deletion: DeletionTime::LIVE,
                rows: vec![(
                    rng.bytes(6),
                    Row {
                        liveness: LivenessInfo::EMPTY,
                        deletion: RowDeletion::LIVE,
                        cells: (0..3)
                            .map(|column| Cell {
                                column,
                                timestamp: i64::try_from(i).unwrap_or(i64::MAX),
                                ttl: NO_TTL,
                                local_deletion_time: NO_DELETION_TIME,
                                value: rng.bytes(32),
                            })
                            .collect(),
                    },
                )],
            };
            let mut buf = Vec::new();
            encode_update(&mut buf, &update).expect("small record");
            buf
        })
        .collect()
}

fn boundaries(shards: u16) -> Vec<Box<[u8]>> {
    // Keys start with 0x40 and two uniform bytes: split evenly on those.
    (1..u32::from(shards))
        .map(|i| {
            let v = u16::try_from(i * 65_536 / u32::from(shards)).unwrap_or(u16::MAX);
            let [hi, lo] = v.to_be_bytes();
            vec![0x40, hi, lo].into()
        })
        .collect()
}

/// Times a flush of `recs.len()` one-row partitions: the walk in 64 KiB
/// batches, then the drop, on a different thread from the writer (as in
/// Cassandra, and as in the Java benchmark).
#[allow(clippy::cast_precision_loss)] // counts far below 2^52
fn flush_cost(recs: &[Vec<u8>]) {
    let m = Arc::new(Memtable::new(boundaries(1024)).expect("sorted boundaries"));
    std::thread::scope(|s| {
        s.spawn(|| {
            for r in recs {
                m.apply(decode_partition(r).expect("valid record"))
                    .expect("not frozen");
            }
        });
    });
    m.freeze().expect("not poisoned");
    let t0 = Instant::now();
    let mut it = m.flush_iter().expect("frozen");
    let mut batch = Vec::with_capacity(1 << 16);
    let mut bytes = 0;
    loop {
        batch.clear();
        if it.next_batch(&mut batch, 1 << 16).expect("readable") == 0 {
            break;
        }
        bytes += batch.len();
    }
    let walk = t0.elapsed();
    drop(it);
    let t0 = Instant::now();
    drop(m);
    let free = t0.elapsed();
    println!(
        "rust-only: flush {} partitions: walk {:.0} ms ({bytes} bytes), drop {:.0} ms",
        recs.len(),
        walk.as_secs_f64() * 1e3,
        free.as_secs_f64() * 1e3
    );
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

#[allow(clippy::cast_precision_loss)] // nanosecond counts and op counts are far below 2^52
fn main() {
    let n = 200_000;
    let recs = records(n, 42);
    let mut apply = Vec::new();
    let mut read = Vec::new();
    let mut sink = 0usize;
    for _ in 0..7 {
        let m = Memtable::new(boundaries(shards())).expect("sorted boundaries");
        let t0 = Instant::now();
        for r in &recs {
            m.apply(decode_partition(r).expect("valid record"))
                .expect("not frozen");
        }
        apply.push(t0.elapsed().as_nanos() as f64 / n as f64);
        let keys: Vec<Box<[u8]>> = recs
            .iter()
            .map(|r| decode_partition(r).expect("valid record").key)
            .collect();
        let mut out = Vec::new();
        let t0 = Instant::now();
        for i in 0..n {
            out.clear();
            m.read(&keys[(i * 7919) % n], &[], None, &mut out)
                .expect("readable");
            sink += out.len();
        }
        read.push(t0.elapsed().as_nanos() as f64 / n as f64);
    }
    println!(
        "rust-only: decode+apply new row      {:8.1} ns/op (median of 7)",
        median(apply)
    );
    flush_cost(&recs);
    println!(
        "rust-only: read whole partition      {:8.1} ns/op (median of 7)",
        median(read)
    );

    // Multi-threaded apply, each thread with its own records: into one shared
    // memtable, then into one memtable per thread. If both scale the same,
    // the shard locks are not the bottleneck.
    for shared in [true, false] {
        for threads in [1usize, 2, 4] {
            let per = 500_000;
            let sets: Vec<Vec<Vec<u8>>> =
                (0..threads).map(|t| records(per, 7 + t as u64)).collect();
            let tables: Vec<Arc<Memtable>> = (0..if shared { 1 } else { threads })
                .map(|_| Arc::new(Memtable::new(boundaries(shards())).expect("sorted boundaries")))
                .collect();
            let t0 = Instant::now();
            std::thread::scope(|s| {
                for (t, set) in sets.iter().enumerate() {
                    let m = &tables[t % tables.len()];
                    s.spawn(move || {
                        for r in set {
                            m.apply(decode_partition(r).expect("valid record"))
                                .expect("not frozen");
                        }
                    });
                }
            });
            let elapsed = t0.elapsed().as_secs_f64();
            println!(
                "rust-only: {threads} thread(s), {} decode+apply {:10.0} ops/s",
                if shared {
                    "one shared memtable, "
                } else {
                    "one memtable per thread,"
                },
                (threads * per) as f64 / elapsed
            );
        }
    }
    println!("sink {sink}");
}

/// Shard count: `APPLY_BENCH_SHARDS` (1..=65535), default 16.
fn shards() -> u16 {
    std::env::var("APPLY_BENCH_SHARDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16)
}
