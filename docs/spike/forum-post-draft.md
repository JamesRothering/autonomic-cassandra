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

# Draft: code review request for a Rust users forum

Draft only; not posted. Suggested category: "code review". Title and body below.

---

**Title:** Review request: a concurrent in-memory store in safe Rust behind a JNI boundary (Cassandra memtable spike, ~2k lines)

Hi all,

I'd be grateful for a review of a small spike: a Cassandra *memtable* (the sorted in-memory write buffer that
is later flushed to disk) written in Rust and called from Java through JNI. The goal is to find out whether moving
hot in-memory state out of the JVM heap removes the garbage-collection cost it causes, without giving up
correctness or memory safety. The code was written with heavy AI assistance, which is part of why I'm asking for
expert eyes: I would like to know where it is unidiomatic, unsound, or just naive.

Code: https://github.com/JamesRothering/Cassandra-autonomic/tree/spike/rust-memtable, directory
`native/memtable-rs/` (a workspace with two crates). The design write-up with measurements is
`docs/spike/rust-memtable.md` on the same branch.

**Shape of it**

- `core` (`#![forbid(unsafe_code)]`, no dependencies): a data model and merge rules copied from Cassandra's
  Java; a hand-written big-endian codec for the records that cross the boundary; and the store itself: partitions
  sharded by key range, each shard a `BTreeMap<Box<[u8]>, Partition>` behind a `std::sync::RwLock`. Keys arrive
  already in Cassandra's *byte-comparable* encoding, so all ordering is plain `memcmp`. A partition holds its rows
  in an exact-capacity sorted `Vec` until it has 16 rows, then in a `BTreeMap` (an empty `BTreeMap` costs a whole
  ~1.1 KiB leaf node on first insert, which dominated memory for one-row partitions).
- `jni` (`jni` 0.22 and `bytemuck`): `extern "system"` entry points that only forward to safe code. Java holds
  opaque `long` handles (never pointers) that index a global `RwLock<HashMap<i64, Arc<Memtable>>>`; handles are
  never reused. All data is copied in with `GetByteArrayRegion` and out with `SetByteArrayRegion`, so no Rust
  reference into Java memory exists. Every entry point goes through `EnvUnowned::with_env` (which wraps
  `catch_unwind`) and a custom `ErrorPolicy` that maps errors to `IllegalArgumentException` /
  `IllegalStateException` and panics to `RuntimeException`.

The only `unsafe` in the library is `#[unsafe(no_mangle)]` on the 14 exported functions (`jni/src/exports.rs`),
plus one `unsafe` block in an optional, off-by-default benchmark probe for the FFM API (`jni/src/ffm_probe.rs`).

**What I'd most like reviewed**

1. *The JNI boundary* (`jni/src/exports.rs`, `bridge.rs`, `error.rs`, `handles.rs`).
   - Is "the Rust signature matches the Java `native` declaration" really the whole safety obligation of those
     `no_mangle` functions, given that `jni` 0.22's `EnvUnowned`/`JByteArray<'l>` are `#[repr(transparent)]`? Would
     you rather use `RegisterNatives` (or the crate's `native_method!` macro) so the signatures are checked at load
     time?
   - Copy-in/copy-out through `byte[]` instead of direct `ByteBuffer`s: I chose it so there is never a Rust slice
     over memory Java can mutate concurrently. Is there a sound zero-copy alternative you would trust (e.g.
     `GetPrimitiveArrayCritical` for the input), and is it worth it?
   - `with_env` + `resolve` measured at about 58 ns per call on top of a 12 ns bare JNI transition; the handle
     lookup adds about 30 ns. Is that expected for `jni` 0.22, and is hand-rolling on `jni-sys` a reasonable
     trade for a hot path?
   - Panics: a panic inside a shard's write lock poisons that `RwLock`, and every later call on that shard returns
     an error (I treat a half-merged partition as unrecoverable). The handle tables instead recover from poisoning
     (`PoisonError::into_inner`) because their invariants survive a panic. Is that split sound?
   - Lifetime rule: a flush iterator holds an `Arc<Memtable>`, so memory is freed only after Java releases the
     handle *and* closes every iterator. Anything I have missed around `Drop` running on a JVM thread?

2. *Concurrency design* (`core/src/memtable.rs`).
   - Sharding by key range with a `RwLock` per shard, writes holding the shard's write lock for one partition
     merge. At 16 shards, 4 writer threads were no faster than one (critical sections of ~2 µs, and futex parking
     on contention is expensive on this VM); at 1024 shards a shared memtable scales like four private ones. Is
     "many shards + plain locks" defensible here, or would you go to per-partition locks, a concurrent skip list
     (`crossbeam-skiplist`), or something epoch-based?
   - Freeze for flush: a `frozen` flag set under each shard's write lock, checked by writers under the same lock,
     so when `freeze()` returns no write is in flight. The flush iterator then holds no lock between steps and
     re-finds its position by key (`range((Excluded(last), Unbounded))`). Sound? Idiomatic?
   - Shards are `#[repr(align(128))]` with their own `AtomicI64` byte counters (written only under the shard lock,
     summed without locks). Reasonable, or should these just be plain fields read under the lock?

3. *Idiom and API shape*: error types without `thiserror`, the cursor-style decoder in `codec.rs` (it checks
   remaining length before every allocation so a corrupt length cannot make it allocate), the `Rows` enum with a
   hand-written iterator enum, `Cow` for the read-time filtered view, use of `#[must_use]`, clippy pedantic with no
   allowed lints in library code. Anything that reads as "not how Rust people write this"?

**How it is tested**

- A differential test drives Cassandra's own memtables (skip list and trie) and the Rust one with the same random
  operation sequences and compares every read and flush byte for byte; it is checked by mutation (8 of 9 seeded
  rule mutants fail it; the 9th is provably equivalent).
- proptest properties in the core, Miri on the core's tests, a counting `GlobalAlloc` that checks the memory
  estimate (within 7%), a fuzz test of every native method with random and corrupted input under `-Xcheck:jni`.
  The write-up says what was and was not run (e.g. the state of the AddressSanitizer run).

Thanks for any comments, however small.

---

Notes for whoever posts this (delete before posting):

- Check the numbers in the post against `docs/spike/rust-memtable.md` at the commit you link.
- Link a specific commit rather than the branch if the branch may move.
- The forum's code-review norm is to paste the most important snippet inline; `jni/src/exports.rs` (one entry
  point plus the module comment) and `Memtable::apply` are the best candidates.
