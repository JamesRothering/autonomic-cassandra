<!--
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
-->

# Spike: a Rust memtable that Java calls through JNI

Status: spike result, 2026-11-28, branch `spike/rust-memtable`. Evidence, not a commitment: nothing is merged and
Cassandra's behaviour is unchanged (the new Java classes are inert unless a caller loads them). Decisions are left
to the owner (section 12).

## 1. Short answer

- **The premise held in this experiment.** With the memtable's data in Rust, G1 spent 1.3 s per minute in GC
  pauses instead of 13.9 s (default `SkipListMemtable`) or 7.3 s (`TrieMemtable` with `offheap_objects`), promoted
  57 MiB to the old generation instead of 3.5-7.5 GB, and needed no mixed collections. Under generational ZGC the
  Java memtables caused hundreds of allocation stalls a minute; the Rust one caused none. Write throughput with
  2-4 writers was 1.25-2.6x the best Java memtable. Single-threaded, `TrieMemtable` is still faster.
- **Correctness for what was built is well evidenced:** byte-for-byte agreement with Cassandra's own
  `SkipListMemtable` and `TrieMemtable` on random operation sequences (cells, TTLs, tombstones, row and partition
  deletions, slices, flushes), with an oracle that a mutation check shows can fail.
- **Safety:** no `unsafe` blocks in library code (14 `#[unsafe(no_mangle)]` attributes are the whole inventory);
  Miri, AddressSanitizer in a live JVM, `-Xcheck:jni` and a million fuzzed boundary calls are clean.
- **It is not a memtable Cassandra can use yet:** no range tombstones, static rows, collections or counters, no
  `Memtable.Factory`, and the Java-side decode for reads and flush is not written (section 10).
- **The surprises were in cost, not safety:** freeing a flushed memtable is slow with glibc's allocator (0.9 s per
  300k partitions while writers run; mimalloc 0.3 s; an arena per memtable is the real fix), the shard count has
  a cliff (16 shards did not scale, 1024 did), and an early design used 3x the memory it reported.

## 2. What was built

| Part | Where | Size |
|------|-------|------|
| Rust core: data model and reconciliation rules, wire codec, sharded concurrent store, flush iterator, memory accounting | `native/memtable-rs/core/src/` | 1,410 lines (`codec.rs` 343, `memtable.rs` 397, `model.rs` 469, `rows.rs` 163, `lib.rs` 38) |
| Rust JNI boundary: 14 exported functions, handle tables, error mapping | `native/memtable-rs/jni/src/` | 744 lines (`exports.rs` 213, `bridge.rs` 213, `handles.rs` 141, `error.rs` 132, `lib.rs` 45), plus 87 in the optional `ffm_probe.rs` |
| Java wrapper: `NativeMemtable` (natives, loader), `RustMemtable` (API), `PartitionRecords` (encoder) | `src/java/org/apache/cassandra/db/memtable/rust/` | 3 files |
| Differential test, fuzz test, benchmarks, test helpers | `test/unit/org/apache/cassandra/db/memtable/rust/` | 6 files |
| Rust tests: unit tests, proptest properties, allocator-checked accounting test | `core/src/*.rs`, `core/tests/`, `jni/src/handles.rs` | |
| Scripts: Java benchmark runner, throughput and GC suites, GC log summary, mutation check | `native/memtable-rs/bench/` | |

Line counts are `wc -l` of the source files including licence headers, comments and doc comments, and excluding
the `#[cfg(test)]` modules, `core/tests/` and `core/examples/`. About 2,150 lines of Rust in all.

**What the memtable supports.** Partitions ordered by key; rows ordered by clustering; regular (simple-column)
cells with timestamp, TTL and local deletion time; cell tombstones; expiring cells; row primary-key liveness
(plain, expiring, expired); row deletions (regular and the deprecated shadowable kind); partition deletions.
Point read of a partition, optionally restricted to a clustering slice; an ordered flush iterator over a frozen
memtable; an owned-bytes counter for flush decisions.

**What it does not support** (each listed again in section 9): range tombstones, static rows, complex
(collection/UDT) columns, counters, secondary-index notifications, and the `Memtable` interface itself (no
`Memtable.Factory`; section 10 is the plan).

### Boundary and lifetime

```
Java                                   Rust (jni crate)                    Rust (core crate)
RustMemtable.put(PartitionUpdate)
  PartitionRecords.write -> byte[]  -> bridge::put: GetByteArrayRegion -> decode_partition -> Memtable::apply
                                       handle -> Arc<Memtable> (read lock on the handle table)
RustMemtable.read(key[, slice])     -> bridge::read                     -> Memtable::read -> record
  byte[] <- SetByteArrayRegion      <-
RustMemtable.freeze / openFlush     -> FlushIter (holds Arc<Memtable>)  -> next_batch -> records
RustMemtable.close (= discard)      -> drop the table's Arc
```

Memory is freed when the last `Arc` goes: after Java has called `close` on the memtable *and* closed every flush
iterator, in either order (`jni/src/handles.rs`, tested in `handles::tests` and
`RustMemtableDifferentialTest.boundaryErrorsBecomeJavaExceptions`).

## 3. Design choices and why

1. **Byte-comparable keys; Rust never compares by CQL type.** Java encodes the decorated partition key
   (token first) and each clustering with Cassandra's byte-comparable encoding (`ByteComparable.Version.OSS50`,
   `utils/bytecomparable/ByteComparable.md`), and Rust orders everything with `memcmp` (`Ord` on `[u8]`).
   Clustering *bounds* have their own encodings that sort between clusterings, so inclusive and exclusive slices
   are plain byte ranges. This is how `TrieMemtable` and BTI sstables already work.
   *Cost:* Java pays the encoding on every write and read. Measured: about 1.0 µs to encode a one-row, three-cell
   update (section 7.1), a quarter of the Rust put path; probably most of it in `ByteSource`'s byte-at-a-time
   iteration (**not profiled**). The read path would also pay to *decode* keys and clusterings back into Java objects; the spike
   does not do that (the differential test compares encoded bytes), so that cost is not measured. Keys and
   clusterings are also stored in encoded form, which is a little larger than the raw values (separator and
   terminator bytes, escaping of zero bytes).

2. **Cell values are opaque bytes; columns are small integers.** A cell's column is its index in the table's
   regular columns, which sorts like the columns. Values are compared, where reconciliation needs it, as unsigned
   bytes, which is what Java does (`Cells.java:107`, `ValueAccessor.compare`).

3. **Reconciliation copied rule by rule, asymmetries included.** Every rule in `core/src/model.rs` cites the
   Java it mirrors (`Cells.reconcile` and `CellLivenessInfo.resolve`, `LivenessInfo.supersedes`,
   `DeletionTime.supersedes/deletes`, `Row.Deletion`, `BTreeRow.merge`, `MutableDeletionInfo.add`,
   `BTreePartitionUpdater`). Two Java behaviours were surprising and are kept deliberately:
   - `BTreeRow.merge` filters the cells shadowed by the winning row deletion *only from the losing side*
     (`BTreeRow.java:663-686`), and a new row is inserted unfiltered (`BTreePartitionUpdater.insert`). A stale
     cell written in the same update as a newer row deletion therefore survives in the memtable.
   - The memtable does **not** apply a partition deletion to the rows it stores. Every read and flush applies it
     instead (`AbstractBTreePartition.java:199-208` builds a `RowAndDeletionMergeIterator` that calls
     `BTreeRow.filter(..., partitionDeletion, false, ...)`, `RowAndDeletionMergeIterator.java:180-184`,
     `BTreeRow.java:343-395`). The first differential run found this (section 6.2); Rust now does the same in
     `Row::visible_under`.

4. **Concurrency: shards by key range, one `RwLock` each.** Writers take their shard's write lock for one
   partition merge; reads and flush take read locks. This is `ShardedSkipListMemtable` with
   `serialize_writes: true`, and the same model as `TrieMemtable`, which takes a `ReentrantLock` per shard
   (`TrieMemtable.java:520, 560-600`). Chosen because its correctness argument is short, it needs no `unsafe`
   and no epoch-based reclamation, and std locks poison on panic (a half-merged partition is never served).
   Shards are `#[repr(align(128))]` and keep their own byte and partition counters.
   *Cost, measured:* with 16 shards, 4 writer threads were no faster than 1 (critical sections of about 2 µs,
   and parking a waiter on a futex costs several µs on this VM). With 1024 shards a shared memtable scaled like
   four private ones (section 7.2). The benchmarks use 1024 shards; Cassandra's sharded memtables default to one
   shard per core (`AbstractShardedMemtable.java:46`). Shard boundaries come from Java (equal Murmur3 token
   ranges, `RustMemtable.tokenBoundaries`), so the flush order is the global token order.
   *Alternative not taken:* a concurrent skip list (`crossbeam-skiplist`) for the partition index with a mutex
   per partition. It would remove the shard-count knob; it is the first thing to try if lock-free scaling matters.

5. **Freeze, then flush.** `freeze()` sets a flag inside each shard while holding that shard's write lock; writers
   check it under the same lock, so when `freeze()` returns no write is in progress and none can start (Cassandra
   gets the same guarantee from its `OpOrder` write barrier). The flush iterator holds no lock between steps and
   re-finds its position by key, which is sound because a frozen memtable never changes.

6. **Small partitions are a sorted `Vec`.** An empty std `BTreeMap` allocates a full leaf node (11 slots,
   about 1.1 KiB here) on its first insert. The counting-allocator test showed a one-row partition costing
   1.3 KB against an estimate of 450 B. A partition now keeps up to 16 rows in an exact-capacity sorted `Vec` and
   switches to a `BTreeMap` above that (`core/src/rows.rs`): about 420 B per one-row partition with three 32-byte
   cells.

7. **Data crosses the boundary by copy only.** In: `GetByteArrayRegion` into a fresh `Vec`. Out:
   `SetByteArrayRegion` into a Java-owned `byte[]` that the caller reuses (no per-call Java allocation; if it is
   too small the call returns `-needed` and consumes nothing). There is never a Rust reference into Java memory,
   so the GC can move arrays and Java can reuse them immediately; no direct `ByteBuffer` aliasing argument is
   needed. *Cost:* one extra copy of each update (about 150 bytes here), included in the measured put cost.

8. **Opaque handles.** Java holds `long` ids from a counter that never repeats, looked up in
   `RwLock<HashMap<i64, Arc<_>>>`. A stale, closed or forged handle throws `IllegalStateException`; it cannot
   reach freed memory. *Cost, measured:* about 30 ns per call on top of the JNI transition and `with_env` (section 7.1).

9. **Errors and panics become Java exceptions.** Every entry point runs inside `jni` 0.22's
   `EnvUnowned::with_env`, which wraps `catch_unwind`, and resolves through `ThrowJavaException`
   (`jni/src/error.rs`): malformed input -> `IllegalArgumentException`, bad handle or state ->
   `IllegalStateException`, panic -> `RuntimeException("Rust panic: ...")`. The decoder rejects every value Java
   could not have built and every record that is not exactly one partition, and never allocates more than the
   input could hold.

10. **Memory accounting is an estimate, checked against reality.** `owned_bytes` sums per-shard counters that
    each write adjusts by the change in the bytes it owns (struct sizes, `Vec` capacities, B-tree entries at 1.5x
    their inline size, key and value bytes). `core/tests/accounting.rs` installs a counting `GlobalAlloc` and
    compares (requested sizes; malloc's own per-block overhead is not counted):
    
    | Workload (100k updates each) | Estimated | Measured | Ratio |
    |------------------------------|----------:|---------:|------:|
    | 100k one-row partitions, 3 x 32 B cells | 42.0 MB | 41.7 MB | 1.01 |
    | 1k partitions x 100 rows, 3 x 32 B cells | 35.3 MB | 38.1 MB | 0.93 |
    | 10k partitions x 10 rows, 1 x 512 B cell | 65.5 MB | 65.5 MB | 1.00 |
    | 1k rows overwritten 100 times | 0.42 MB | 0.42 MB | 0.99 |

    The test fails if any ratio is off by 25% or more. Before design choice 6, the first row was 0.35.

## 4. Unsafe inventory

**Library code: zero `unsafe` blocks, zero `unsafe fn`, zero `unsafe impl`.** The core crate is
`#![forbid(unsafe_code)]`. The JNI crate is `#![deny(unsafe_code)]`, with one module, `jni/src/exports.rs`,
allowed `unsafe` for one purpose:

| Where | What | Count | Why it is sound |
|-------|------|------:|-----------------|
| `jni/src/exports.rs` | `#[unsafe(no_mangle)]` on `extern "system"` functions | 14 | The JVM resolves each function by its mangled name and calls it with the C ABI and the Java declaration's types. Soundness rests on the *signature contract*, stated once in the module comment and referenced by each `// SAFETY:` comment: the name is unique (it carries the full Java class name) and the parameter and return types match the `native` declaration in `NativeMemtable.java` one for one (`EnvUnowned` and `JByteArray<'l>` are `#[repr(transparent)]` over `JNIEnv*` and `jobject`; `jlong`/`jint` are `i64`/`i32`). A missing or misspelt symbol fails the tests with `UnsatisfiedLinkError`; **a wrong parameter type would not be detected by anything** and is the residual risk to check by eye (14 short functions). |

No function body in `exports.rs` contains `unsafe`. Each forwards to safe code in `bridge.rs` through
`EnvUnowned::with_env` (which wraps `catch_unwind`), except `nopRaw`, a benchmark probe whose body is `x` and cannot
panic.

**Feature-gated benchmark probe (`ffm-probe`, off by default):** `jni/src/ffm_probe.rs` adds four more
`#[unsafe(no_mangle)]` functions for the FFM-versus-JNI measurement and **one `unsafe` block**: `memtable_ffm_sum`
builds a slice from a pointer and length passed by `FfmProbe` through the FFM API. The function is itself an
`unsafe extern "C" fn` with a `# Safety` contract (valid for `len` bytes, not mutated during the call), so no safe
Rust caller can misuse it; its only caller passes a confined-arena segment or a heap array under
`Linker.Option.critical`. Not part of the memtable.

**Test code:** `core/tests/accounting.rs` implements `GlobalAlloc` for a counting allocator: one `unsafe impl` and
three `unsafe` blocks, each forwarding unchanged to `std::alloc::System` (`// SAFETY:` comments in place). It is a
separate test crate, so the core's `forbid` does not cover it.

**No `unsafe impl Send` or `Sync` anywhere.** `Memtable` and `FlushIter` are `Send + Sync` / `Send` by
composition of std types (`RwLock`, atomics, `Arc`, `Box`, `Vec`, `BTreeMap`); the handle tables are a
`LazyLock` of `RwLock`/`Mutex`/atomic fields. `jni`'s `Env` and local references are `!Send` and never stored.

**`unsafe` in dependencies** (not audited here): `jni` 0.22.4 (every JNI call, `AttachGuard`), `jni-sys`,
`bytemuck` (`cast_slice` between `[u8]` and `[i8]`, sound because both are 1-byte types with no invalid bit
patterns), and, only with the optional `mimalloc` feature, `mimalloc`/`libmimalloc-sys` (a C allocator).

## 5. How to reproduce

All commands are in `native/memtable-rs/README.md`. Environment: Rust 1.100.1 stable (clippy, rustfmt), nightly
for Miri, JDK 17.0.21 for the build and tests, JDK 21.0.13 for the ZGC runs, 4-core VM with 5.9 GB RAM.

## 6. Tests and tool results

Every result below was run on this branch on 2026-11-28; "passed" means the command was run and passed.

### 6.1 Rust

| What | Command (in `native/memtable-rs`) | Result |
|------|-----------------------------------|--------|
| Unit and property tests | `cargo test --release` | core: 12 unit tests, 11 properties (512 cases each), 1 accounting test; jni: 3 handle-table tests. All passed. |
| Clippy | `cargo clippy --all-targets -- -D warnings -W clippy::pedantic` | Clean. No lint is allowed in library code. Allowed elsewhere: `clippy::cast_precision_loss` on two benchmark/test functions that turn counts into `f64` for printing. |
| Formatting | `cargo fmt --all --check` | Clean. |
| Miri, core | `cargo +nightly miri test -p memtable-core` | 12 unit tests and 11 properties passed (properties at 4 cases each under Miri, including the 4-thread concurrent-write test at 20 writes per thread); the allocation-counting test is ignored under Miri (it is slow there and the core has no `unsafe` for it to check). No undefined behaviour reported. |
| Miri, JNI handle tables | `cargo +nightly miri test -p memtable-jni handles` | 3 passed (uniqueness and stale handles, iterator keeps memory alive until release *and* close, concurrent lookups and releases). |
| Not under Miri | | `bridge.rs` and `exports.rs`: they need a live JVM, which Miri cannot host. They are covered by AddressSanitizer and `-Xcheck:jni` instead (below). |

The properties: codec round-trip; the decoder never panics on arbitrary bytes and rejects every truncation of a
valid record; `Cells.reconcile` is commutative; cell-only merges are order-independent; `supersedes` is
asymmetric for liveness and deletion times; reads, the flush iterator at random batch sizes and the incremental
byte count agree with each other and with a recount; slice reads return exactly the rows of a full read within
the bounds. Plus: writes after freeze are rejected and an iterator outlives the owner; concurrent cell-only writes
from 4 threads equal the same writes applied serially.

### 6.2 The differential test (the oracle)

`RustMemtableDifferentialTest` (a `CQLTester`) creates a table with a two-column clustering key, one of them
`DESC`, on the Murmur3 partitioner, and drives **Cassandra's own memtable** (created by the table's factory:
`SkipListMemtable` and `TrieMemtable`, as two test parameters) and the Rust memtable (4 shards) with the same random
`PartitionUpdate`s. Updates mix cells (live, expiring, tombstones), row liveness (plain, expiring, expired),
row deletions (regular, shadowable), partition deletions and multi-row updates, over tiny domains so timestamps,
deletion times and values collide. Every whole-partition read, every slice read (random inclusive/exclusive,
prefix and full bounds) and every flush (Cassandra: `partitionIterator` over the whole memtable; Rust: the flush
iterator) must produce the same bytes. The reference is the real memtable rather than a re-implementation, so
the test checks what Cassandra does, not what I think it does.

Results (`ant testsome -Dtest.name=...RustMemtableDifferentialTest`, JDK 17):

| Run | Per memtable type | Result |
|-----|-------------------|--------|
| Default (40 sequences x 400 operations), `-Xcheck:jni` | ~9,600 puts, ~6,100 reads (~5,100 of existing partitions), ~360 flushes | identical, `SkipListMemtable` and `TrieMemtable` |
| Long (600 x 1,000; `-Drust_memtable.sequences=600 -Drust_memtable.ops=1000`), `-Xcheck:jni`, seed 231608113493456 | 360,109 puts, 227,870 reads (196,083 hits), 12,621 flushes | identical, both |
| Default, against the AddressSanitizer build | ~9,700 puts, ~6,000 reads, ~350 flushes | identical, both |
| Concurrent: 4 threads x 5,000 cell-only updates into one Rust memtable vs the same applied serially to Cassandra's | | identical, both |
| An earlier run on `ByteOrderedPartitioner` (the test configuration's default; one Rust shard), commit `bbb79d7` | 40 x 400 | identical, both |

**It found a real divergence on its first run.** Cassandra applies a partition deletion on read and flush, not
when it stores rows (design choice 3); my first version did neither, and the first flush comparison failed.
Reads had not caught it because few reads hit a partition with a deletion. Rust now filters the same way.

**Is the oracle strong enough? Mutation check** (`bench/mutants.sh`): each mutant breaks one rule in the Rust
core and must make the test fail.

| Mutant | Result |
|--------|--------|
| Row-deletion tie goes to the existing side | killed (after the dense profile was added; survived the first generator) |
| Liveness: equal expiry, higher TTL wins | killed (after the liveness-heavy profile was added; survived the first two) |
| Cell value tie-break flipped | killed |
| Read-time partition-deletion filter removed | killed |
| Deletion-time tie on local deletion time flipped | killed |
| Tombstone-vs-expiring preference flipped | killed |
| Partition deletion: last writer wins instead of the superseding one | killed |
| Shadowable deletion cancelled at an *equal* liveness timestamp (`>` to `>=`) | survived: **equivalent**. `BTreeRow.merge` first asks whether the deletion deletes the liveness (`timestamp <= markedForDeleteAt`), so the shadowable check only runs when the liveness is strictly newer; `>` and `>=` cannot differ there (`BTreeRow.java:658-661`, `model.rs` `Row::merge`). |

Two of these mutants surviving the first generator is the most useful result of the check: random tests over
wide domains rarely produce exact ties, which is where the subtle rules live. The generator now runs three
profiles in rotation (wide; tiny with many deletions; tiny with two timestamps and mostly liveness and shadowable
deletions).

### 6.3 The JNI boundary

| What | How | Result |
|------|-----|--------|
| Boundary errors | `RustMemtableDifferentialTest.boundaryErrorsBecomeJavaExceptions` | A Rust panic arrives as `RuntimeException("Rust panic: ...")`; malformed bytes and a length beyond the array as `IllegalArgumentException`; unknown handle, use after release or close, write after freeze, flush before freeze as `IllegalStateException`; the flush iterator still yields the data after the memtable was released; no native object leaks. Passed. |
| Fuzz | `bench/fuzz-boundary.sh` (`NativeMemtableFuzzTest`, 1,000,000 calls, `-Xcheck:jni`): random bytes, valid records with a flipped byte, truncated records, random handles and lengths, to `put`, `read`, `create` and `flushNext` | 209,228 calls accepted, 790,772 rejected with `IllegalArgumentException`/`IllegalStateException`, no other exception, no crash, no `-Xcheck:jni` warning; the memtable still froze and flushed 15.8 MB afterwards; no leak. |
| `-Xcheck:jni` | The differential and fuzz tests with `-Xcheck:jni` | No JNI warnings. |
| AddressSanitizer | `bench/asan.sh`: the JNI crate and std built with `-Zsanitizer=address` (nightly, `-Zbuild-std`), loaded into a normal JDK 17 with GCC 13's `libasan.so.8` preloaded (`ASAN_OPTIONS=detect_leaks=0:handle_segv=0:allow_user_segv_handler=1`) | On the final code: the fuzz test (200,000 calls, with `-Xcheck:jni`) and the whole differential test (6 cases) passed with no ASan report. The runtime was confirmed active (its start-up messages with `verbosity=1`). Limits: ASan instruments the Rust code (ours, `jni`, std), not the JVM; leak detection is off (meaningless inside a JVM); the ASan runtime is GCC's rather than the LLVM one matching rustc, which worked here but is not a supported combination. |

### 6.4 Java

`ant checkstyle checkstyle-test` passes on the new Java files (one suppression per spike-only system property,
with a comment: the properties are deliberately not added to `CassandraRelevantProperties` so that no core file
changes).

## 7. Measurements

**Setup.** One 4-vCPU VM (5.9 GB RAM), nothing else heavy running. Benchmarks in `RustMemtableBench`, each run
in a fresh JVM through `bench/run-java-bench.sh`; Rust measured at commit `3dc4508` (the code changes after it are
a `merge_cells` restructuring and a panic-payload guard, neither on the measured path). Each update is one row of
three 32-byte blob cells, a random 8-byte partition key out of 2^20 and a clustering out of 16 (so almost every
update creates a partition), built as a `PartitionUpdate` on both sides: the per-update garbage before the memtable
is the same; what differs is what the memtable keeps. Java memtables are written directly with
`Memtable.put` under an `OpOrder` group (no commit log); the memtable pool is sized at half the heap and the pool
cleaner is disabled (the benchmark flushes itself). Rust uses 1024 shards. "Flush" means: switch to a new
memtable every 300,000 updates, then, on a separate flush thread, iterate the whole old memtable in order and
discard it. **No sstable is written** in either case, so the GC cost of a real flush (writer buffers, and for Rust
the decoding of records into Java rows, which is not built) is not included. Medians of 3 runs, `[min-max]`.

### 7.1 Per-call costs (single thread)

`RustMemtableBench overhead`, JDK 17 + G1, 200,000 distinct one-row updates per round, rounds 3-7 of each JVM
(rounds 1-2 are JIT warm-up), 3 JVMs: median over the 15 rounds.

| Operation | JDK 17 + G1, ns (median [min-max], n=15) | JDK 21 + ZGC, ns (median, 1 JVM, n=5) |
|-----------|---:|---:|
| Native call, empty body (`nopRaw`) | 12 [12-13] | 12 |
| Native call through `with_env` and the error policy (`nop`) | 70 [69-71] | 71 |
| Native call with a handle lookup (`ownedBytes`) | 100 [99-103] | 101 |
| Encode an update to a record (Java only) | 1,019 [812-1,342] | 1,211 |
| Rust put of a new row: encode + JNI + copy + decode + insert | 3,532 [3,348-3,850] | 4,024 |
| Rust whole-partition read: encode key + JNI + lookup + copy out | 3,594 [3,282-3,699] | 4,070 |
| `SkipListMemtable.put` of a new row (`heap_buffers`) | 7,488 [6,839-8,066] | 6,806 |
| `SkipListMemtable` whole-partition read (iterate the rows) | 4,881 [3,990-5,577] | 6,183 |

Raw output: `docs/spike/data/rust-memtable-overhead-*.txt`.

Reading the table: the bare JNI transition is about 12 ns; `jni` 0.22's `with_env` plus the error policy adds about
58 ns; the handle lookup (an `RwLock` read and a `HashMap` probe with the default SipHash hasher) about 30 ns more. A whole put through Rust costs
about half of a put into `SkipListMemtable`, of which encoding the update on the Java side is about a quarter.
Rust-only, without JNI or Java (`cargo run --release --example apply_bench`, medians of 7): decode + apply of the
same kind of update 1.85-1.98 µs, a whole-partition read 1.86-1.94 µs. So the boundary (copying in, `with_env`,
lookup) is a few hundred nanoseconds of a ~3.5 µs put; most of the rest is the Java encoding and the Rust insert.
Reads cost about the same on both sides; both are dominated by cache misses on a random key among 200,000.

**FFM (JDK 25) versus JNI, standalone** (`bench/ffm/FfmProbe.java`, the library built with `ffm-probe`, 3 JVMs,
medians of 12 rounds):

| Call | ns per call |
|------|------------:|
| JNI, empty body | 11.5 [11.3-11.9] |
| FFM downcall, empty body | 15.4 [14.9-16.8] |
| FFM downcall, `Linker.Option.critical` | 6.2 [6.0-6.5] |
| JNI through `jni` 0.22 (`with_env`), copy 160 bytes in with `GetByteArrayRegion`, sum them | 240 [231-256] |
| FFM: copy 160 bytes into native memory from Java, downcall, sum | 52.5 [51.1-54.7] |
| FFM `critical`, heap array passed directly, sum | 61.0 [52.2-111] |

The bare transitions are close; the difference is in moving data: about 190 ns per call less with FFM for an
update-sized record. On a ~3.5 µs put that is about 5%: worth having, not decisive. FFM is final only from JDK 22,
and Cassandra supports 11, 17 and 21 (`build.xml` `java.supported`), so JNI stays the boundary for a drop-in.

### 7.2 Write throughput

`bench/throughput-suite.sh`: JDK 17 + G1, 2 GB heap, 5 s warm-up then 10 s measured, 1/2/4 writer threads, flush
every 300,000 updates. Updates per second:

| Memtable | 1 thread | 2 threads | 4 threads | Memtable size at flush |
|----------|---------:|----------:|----------:|-----------------------:|
| `SkipListMemtable`, `heap_buffers` (default) | 121k [88k-129k] | 179k [170k-200k] | 197k [185k-215k] | 240 MiB |
| `TrieMemtable`, `offheap_objects` | 281k [249k-329k] | 220k [202k-222k] | 209k [143k-212k] | 144 MiB |
| Rust (glibc malloc) | 154k [140k-162k] | 328k [309k-343k] | 415k [393k-479k] | 117 MiB |
| Rust (mimalloc) | 166k [164k-170k] | 381k [356k-382k] | 486k [480k-507k] | 117 MiB |

"Memtable size" is each implementation's own figure for 300,000 updates (Java: allocator ownership on and off heap;
Rust: `owned_bytes`), so it is also a rough memory-efficiency comparison. Single-threaded, `TrieMemtable` is the
fastest; Rust is about 25% faster than the default memtable and 45% slower than the trie. With 2 and 4 writers the
Java memtables stop scaling (GC and, for the trie, its 4 shard locks), while Rust scales to about 2.2x the best
Java figure at 4 threads. Rust-only (no JVM), a shared memtable reached 756k updates/s at 4 threads.

Caveat: 4 writer threads on 4 vCPUs also compete with GC threads and the flush thread, so the 4-thread column
measures a saturated machine.

### 7.3 Garbage collection

`bench/gc-suite.sh`: 2 writer threads for 60 s, 2 GB heap, flush every 300,000 updates, GC logged with
`-Xlog:gc*,safepoint`; JDK 17 with G1 and JDK 21 with generational ZGC (`-XX:+UseZGC -XX:+ZGenerational`, as in
`conf/jvm21-server.options`). Summary by `bench/gc-summary.py`; per-run lines in `docs/spike/data/rust-memtable-gc.txt`.

**JDK 17, G1:**

| Memtable | Updates/s | GC pauses | Total pause time | Longest pause | Promoted to old gen | Mixed GCs | Process RSS peak |
|----------|----------:|----------:|-----------------:|--------------:|--------------------:|----------:|-----------------:|
| SkipList, `heap_buffers` | 180k [167k-181k] | 162 | 13.9 s [13.4-13.9] | 233 ms [230-282] | 7,464 MiB [6,913-7,536] | 24 | 2,164 MiB |
| SkipList, `offheap_objects` | 221k [217k-239k] | 122 | 13.1 s [12.8-13.7] | 294 ms [273-310] | 4,990 MiB [4,906-5,384] | 9 | 2,406 MiB |
| Trie, `offheap_objects` | 223k [214k-240k] | 84 | 7.3 s [7.2-7.5] | 171 ms [157-182] | 3,491 MiB [3,419-3,817] | 5 | 2,420 MiB |
| Rust (glibc malloc) | 280k [238k-287k] | 32 | 1.3 s [1.2-1.4] | 123 ms [83-124] | 57 MiB [56-57] | 0 | 3,329 MiB [2,990-3,597] |
| Rust (mimalloc) | 387k [380k-400k] | 39 | 1.4 s [1.3-1.4] | 65 ms [49-66] | 56 MiB [56-57] | 0 | 1,825 MiB |

**JDK 21, generational ZGC:**

| Memtable | Updates/s | GC pauses (total, longest) | Allocation stalls | Stall time (summed over threads) | Promoted to old gen | Process RSS peak |
|----------|----------:|---------------------------:|------------------:|---------------------------------:|--------------------:|-----------------:|
| SkipList, `heap_buffers` | 104k [83k-129k] | 17 ms, 1.3 ms | 392 [296-649] | 222 s [191-342] | 383 MiB [223-1,022] | 2,547 MiB |
| SkipList, `offheap_objects` | 111k [105k-122k] | 18 ms, 0.7 ms | 483 [379-747] | 211 s [207-242] | 508 MiB | 2,554 MiB |
| Trie, `offheap_objects` | 145k [142k-147k] | 26 ms, 1.0 ms | 259 [231-265] | 122 s [119-127] | 825 MiB [656-916] | 2,487 MiB |
| Rust (glibc malloc) | 243k [241k-245k] | 13 ms, 0.7 ms | 0 | 0 | 35 MiB | 3,488 MiB [3,134-3,704] |
| Rust (mimalloc) | 370k [367k-398k] | 16 ms, 0.7 ms | 0 | 0 | 35 MiB | 2,437 MiB |

What these say, at face value:

- **G1:** with the default memtable, the JVM spent 13.9 s of every 60 s in GC pauses (23%), promoted 7.5 GB of
  memtable data to the old generation and needed 24 mixed collections to reclaim it after flushes. The existing
  off-heap options help (the trie halves the pause time) but still promote 3.5-5 GB, because rows and cells remain
  Java objects. With the Rust memtable, pause time fell to 1.3 s (-90% against the default, -82% against the
  trie), promotion to 57 MiB (-99%), and there were no mixed collections: what garbage is left (the
  `PartitionUpdate`s) dies young.
- **Generational ZGC:** pauses are sub-millisecond for everyone, as designed, but with the Java memtables ZGC
  could not keep up with the allocation rate in a 2 GB heap: writers stalled hundreds of times, for a total
  (summed over the writer and flush threads) of 2-3.7 times the run's length. With the Rust memtable there were no
  allocation stalls at all.
- **Throughput follows:** against the best Java memtable (the trie), Rust wrote 1.25x (glibc) to 1.7x (mimalloc)
  as many updates per second under G1, and 1.7x to 2.6x under ZGC.

Caveats (read before quoting these numbers):

- Each GC log covers the whole JVM, including about 10 s of identical Cassandra test setup before the measured
  60 s. The Rust runs' longest G1 pause (123 ms) is a young pause during that setup (`GC(10)` at 14 s in run 1),
  not during writing.
- "Promoted" for G1 is the sum of old-region growth across young pauses times the region size (an approximation:
  it ignores humongous regions and counts promotion that a concurrent cycle later reclaims); for ZGC it is the
  young collections' reported promoted bytes.
- ZGC's stall total is summed over threads and the GC MXBeans' "time" for ZGC is cycle time, not pause time, so the
  log is the source here.
- **Rust with glibc malloc used the most memory** (3.3-3.5 GB RSS peak): its flush (1.3 s per 300,000 partitions,
  0.9 s of it freeing) was slower than the writers filled the next memtable, so old memtables queued up waiting to
  be flushed. With mimalloc the flush kept up (0.56 s) and RSS was the lowest of all. In a real node a backed-up
  flush would block writers on the memory pool instead; here it shows as memory.
- The workload is one shape (one-row partitions, small cells, insert-only); no reads, compaction, commit log,
  network or real sstable writes compete for the heap. On a real node the heap holds other things, so the
  relative effect of the memtable on GC should be smaller than here.

## 8. What the measurements mean for the premise

The premise is "replace JVM components with Rust that Java calls, so hot in-memory state stops producing Java
garbage". For the memtable, this spike supports it, with three qualifications:

1. **Most of the gain is about where the data lives, not about Rust.** The garbage that hurts is long-lived
   memtable objects that get promoted and then die together at flush. Any structure outside the Java heap avoids
   that; Cassandra's own `offheap_objects` and the trie already move part of it and get part of the gain
   (section 7.3). What Rust adds is that *all* of it moves (row and cell structure too, not just values), with
   memory safety that `sun.misc.Unsafe`-style off-heap Java code does not have, and with ordinary data structures
   instead of hand-managed buffers.
2. **The boundary is cheap enough.** A few hundred nanoseconds of a ~3.5 µs write go to the JNI boundary
   (copying, `with_env`, handle lookup); the Java-side encoding is the larger cost (about 1 µs) and is the first
   thing to profile and optimise. FFM would save about 190 ns per call but is not
   available on Cassandra's supported JDKs.
3. **Rust brings its own memory-management problems.** Freeing 2 million small allocations at flush, on a
   different thread from the one that allocated them, is slow with glibc's allocator; it made the Rust flush
   slower than Java's (where the GC frees in bulk later) and, with two writers, let flushes fall behind. The
   design answer is the one Cassandra reached for in Java: allocate a memtable's data from its own regions so that
   discarding it frees a few large blocks. That is the main open engineering question for a real implementation,
   and it is a data-structure question (offsets into slabs instead of `Box`es), not a language one.

Not measured, and needed before drawing conclusions for a whole node: the decode of records into Java rows on
read and flush (it creates young garbage the Rust side avoids today); a real flush writing sstables; reads mixed
with writes; and other workload shapes (wide partitions, large values, overwrites).

## 9. Gaps and known risks

**Not implemented** (each needs design, not just code):

1. **Range tombstones.** The Java encoder throws if it meets one. They are the largest missing piece: they need
   their own ordered structure per partition, interaction with row and cell shadowing on read, and the
   bound/boundary markers in the flush stream.
2. **Static rows, complex columns (collections, non-frozen UDTs), counters.** Each throws in the encoder.
   Complex columns need cell paths and complex deletions (`ColumnData.Reconciler`, `ComplexColumnData`);
   counters need `CounterContext.merge`, which would have to be ported or called back into Java.
3. **Decoding results back into Java objects.** The read and flush paths return records; Cassandra needs
   `UnfilteredRowIterator`s. The decode (byte-comparable back to `DecoratedKey`/`Clustering`, cells to
   `BufferCell`s) is not written, so its cost and its garbage are not measured. A flush that hands records to an
   sstable writer would allocate young-generation garbage at flush time.
4. **Everything around `put`:** secondary-index `UpdateTransaction` notifications, the `colUpdateTimeDelta` that
   `put` returns (used for metrics), `EncodingStats` and min-timestamp / min-local-deletion-time tracking,
   columns-seen tracking, commit-log bounds, the memtable pool's memory limits and cleaner, and `getFlushSet`'s
   key-range splitting for multiple data directories.
5. **Cassandra's write-path cloning and OpOrder.** Cassandra writes under an `OpOrder.Group` and its allocator can
   block a writer when the memory pool is full; here, a full pool has no effect on the Rust side.

**Known risks and weaknesses of what exists:**

- **The `no_mangle` signature contract is checked by nobody** (section 4). `RegisterNatives` with signatures
  generated from Rust types (the `jni` crate has a `native_method!` macro) would move the check to load time.
- **Freeing a large memtable is slow with glibc's allocator**: 0.9 s for 300k partitions on the flush thread while
  writers allocate (section 7.3). mimalloc cuts it to about 0.3 s. A real design should allocate a memtable's data
  from its own arena (as Cassandra's slab and native allocators do), so discarding it frees a few large blocks.
  That changes the data structures (offsets into slabs instead of `Box`es) and is the main engineering item this
  spike leaves open.
- **The shard count is a tuning knob with a cliff**: 16 shards did not scale at all on 4 threads; 1024 did. The
  differential test uses 4 shards and the benchmarks 1024; nothing chooses it automatically.
- **Holding the handle table's read lock for a whole call** means `release` waits for in-flight calls on *any*
  memtable, and std's `RwLock` then queues new readers behind it: one release can stall every writer for the
  duration of one write. Harmless at Cassandra's flush rate; worth a per-handle `Arc` clone if it ever shows up.
- **The owned-bytes figure is an estimate** (within 7% in the tested workloads) of requested bytes. It does not
  include malloc's per-block overhead or fragmentation, which RSS does.
- **Encoded keys are stored as given**; nothing checks they are valid byte-comparable encodings. Garbage in
  orders as garbage, but cannot cause memory unsafety.
- **Sharding assumes a hashing partitioner** for balance (Murmur3 token ranges). With `ByteOrderedPartitioner`
  Java passes no boundaries and the memtable has one shard (correct, slow under concurrency).
- **Tested on one machine, one JDK build per version, one workload shape.** The benchmark's update is one row of
  three 32-byte cells with random keys; wide partitions, large values and overwrite-heavy workloads are
  measured only in the Rust-only accounting test.

## 10. What it would take to become a real memtable

Item 7 of the brief (a `Memtable.Factory` so a node can run with it) was **not attempted**: items 1-6 took the
session, and a partial factory would be worse than none. What it would take, in order:

1. **`RustMemtable extends AbstractAllocatorMemtable`** (or `AbstractMemtableWithCommitlog` plus own accounting)
   with a `FACTORY`/`factory(Map)` so `memtable: configurations: rust: class_name: ...` works
   (`Memtable_API.md`). Report `ownedBytes` to the allocator's off-heap pool (`allocator.offHeap().allocate`) so
   the existing flush triggers and the memtable pool limits work unchanged.
2. **`put`**: encode, call Rust, then do on the Java side what `BTreePartitionUpdater` does for the rest of the
   node: `UpdateTransaction` callbacks (secondary indexes need the old and new rows, so Rust would have to return
   them, or tables with indexes keep the Java memtable), `EncodingStats`/min-timestamp collection (can be done
   from the `PartitionUpdate` before the call), columns collection, operation counts, `colUpdateTimeDelta`
   (Rust can return it from the merge).
3. **Reads**: `rowIterator(key, slices, columnFilter, reversed, listener)` and `partitionIterator(columnFilter,
   dataRange, listener)`: encode bounds, call Rust, decode records into `BTreeRow`s and wrap them in an
   `UnfilteredRowIterator` (reversed order can be done on the Java side or by a reverse range in Rust; the column
   filter and multiple slices on the Java side at first). Key-range scans need a range-read entry point in Rust
   (the shard boundaries make this a walk over a contiguous run of shards).
4. **Flush**: `getFlushSet(from, to)` must yield `UnfilteredRowIterator`s in token order to the sstable writer:
   freeze on `switchOut`, then decode the flush iterator's batches. Respect `from`/`to` (a range-restricted flush
   iterator).
5. **The missing data model**: range tombstones, static rows, complex columns, counters (section 9). Until then
   the factory must refuse tables that use them (validate at `create`), falling back to the default memtable.
6. **Lifecycle**: `discard()` releases the handle; the flush iterator's `Arc` keeps memory alive until the sstable
   is written. Library loading at start-up, with a clear error if the configured library is missing.
7. **Validation**: rerun the differential test through the factory (CQLTester with `memtable = 'rust'`), then the
   existing memtable test suites (`MemtableQuickTest`, `MemtableSizeTest*`, flush-set tests), then the dtests
   that cover flush and streaming, then a mixed-version cluster test for the upgrade path (the sstables written
   are Cassandra's own, so the on-disk format does not change).

Rough size: steps 1-4 and 6 are a few hundred lines of Java plus a range-read and a reverse-read entry point in
Rust; step 5 is the bulk (range tombstones alone are comparable to everything in this spike).

## 11. "AI writes bad Rust": an honest assessment of this code

The code was written by an AI agent (Claude) in one session, without a Rust expert in the loop. What the
evidence supports, and what it does not:

**What the evidence supports.**

- *Memory safety of the code as written:* no `unsafe` in library code beyond 14 `no_mangle` attributes; Miri
  clean on the core and the handle tables; AddressSanitizer clean on the JNI layer inside a real JVM;
  `-Xcheck:jni` clean; a million fuzzed boundary calls without a crash. This is about as strong as safety evidence
  gets for a spike, *because* the design avoided `unsafe` rather than because the `unsafe` was checked.
- *Behavioural equivalence for the supported subset:* byte-for-byte agreement with Cassandra's two memtables over
  tens of thousands of random operations per run, with an oracle shown to catch 8 of 8 non-equivalent seeded
  rule mutants.
- *Clippy pedantic with nothing allowed in library code*, rustfmt, no `unwrap`/`expect` outside tests and
  examples, and every reconciliation rule citing the Java line it copies.

**What the agent got wrong first, and what caught it** (all fixed on the branch; listed because this is the
actual answer to "does AI write bad Rust"):

| First version | Caught by | Now |
|---------------|-----------|-----|
| Partition deletions not applied on read and flush | the differential test, first run | `Row::visible_under` |
| Murmur3 shard boundaries overflowed for half the shards (a `double`-to-`long` saturation in Java) | reading the code before the first run | unsigned step arithmetic |
| One `BTreeMap` per partition: 1.3 KB for a one-row partition, 3x the estimate | the counting-allocator test | `Rows` (sorted `Vec`, then `BTreeMap`) |
| 16 shards: 4 writer threads no faster than 1 | the Rust-only benchmark | 1024 shards in the benchmarks; the knob is documented as a risk |
| A random generator too sparse to hit two tie rules | the mutation check | three generator profiles |
| Benchmarks first ran on the test configuration's `ByteOrderedPartitioner` (one Rust shard) | reading the setup line | Murmur3 set explicitly, printed in every result |
| The benchmark's flush policy let the faster implementation build bigger memtables | reading the results (memtable sizes differed) | switch every N updates, flush on its own thread |
| Freeing a memtable on the flush thread was 4x slower than the flush walk | the benchmark's walk/free split | measured; mimalloc as an option; arena allocation named as the real fix |

The pattern: the Rust that compiled was memory-safe and mostly idiomatic from the start; the mistakes were in
*semantics* (what Cassandra really does), *performance assumptions* and *measurement method*. None of them would
have been caught by the compiler; all were caught by tests and measurements built to be able to fail. That is the
honest answer for this code: the risk is not "bad Rust" in the sense of unsound or unidiomatic code, it is
plausible-looking code whose behaviour or cost was never checked. The protection is the oracle and the
measurements, not the language.

**Where I was unsure, and what an expert should look at first:**

1. `jni/src/exports.rs` and `bridge.rs`: whether the signature contract is the whole obligation of
   `#[unsafe(no_mangle)]` with `jni` 0.22's `EnvUnowned`/`JByteArray<'l>` types, and whether `RegisterNatives`
   should replace it. `jni` 0.22 is a recent major version; the agent learnt its API from the crate's source, not
   from experience.
2. `jni/src/error.rs`: the custom `ErrorPolicy`. Its first version dropped the panic payload unguarded; the
   crate's own policy guards that drop with a second `catch_unwind` (a payload whose `Drop` panics), and mine now
   does too, which I found by reading the crate rather than by a test.
3. `core/src/memtable.rs`: the freeze protocol and the lock-free-between-steps flush iterator; and whether
   `RwLock` per shard plus many shards is a design an expert would defend, versus a concurrent skip list.
4. `core/src/rows.rs` and the accounting constants (`map_entry_bytes` assumes std B-tree nodes are two thirds
   full): fine for an estimate, fragile as a contract.
5. The decision to copy data across the boundary rather than share it, and the cost of `with_env` (about 58 ns a
   call, section 7.1), which may matter more than any of the Rust code once the per-call work shrinks.

## 12. For the owner

Decisions and questions for the owner (one at a time; none blocks anything, nothing is merged):

1. **Is this evidence enough to continue inside-out with the memtable as the first component?** The GC effect is
   large and consistent across both collectors; the gaps (section 9) are known work. My recommendation: yes,
   with the arena-allocation question (section 8, point 3) as the next spike, because it decides the Rust data
   structures.
2. **Allocator.** Shipping a Rust component means choosing its allocator: glibc's (no new dependency; slow
   cross-thread frees measured here), mimalloc (a C dependency, 3x faster frees here), or per-memtable arenas
   (no dependency, more design). Recommendation: arenas for the memtable's data, whatever the global allocator.
3. **Boundary.** JNI now (works on 11/17/21); FFM measured as about 190 ns cheaper per call for an update-sized
   record, available only from JDK 22. Recommendation: JNI, with the boundary kept narrow (bytes in, bytes out) so
   it can be swapped when Cassandra's minimum JDK allows.
4. **Next step choice:** (a) the arena spike; (b) the missing data model (range tombstones first); (c) the
   `Memtable.Factory` so a node can run with it (section 10); (d) post the forum review first and wait for
   comments. Recommendation: (d) in parallel with (a).
5. **The forum post** (`docs/spike/forum-post-draft.md`) is a draft for you to post; it names no person and links
   the branch.
