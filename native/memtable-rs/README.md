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

# memtable-rs: a Rust memtable spike for Cassandra

A spike, not a product: an in-memory Cassandra memtable written in safe Rust,
called from Java through JNI, and checked byte for byte against Cassandra's own
memtables. The write-up, with design choices, the `unsafe` inventory, test
results and measurements, is `docs/spike/rust-memtable.md` at the repository
root.

| Crate | What it is |
|-------|------------|
| `core` (`memtable-core`) | The data model, Cassandra's reconciliation rules, the wire codec and the sharded concurrent store. `#![forbid(unsafe_code)]`. |
| `jni` (`memtable-jni`) | The `cdylib` that implements `org.apache.cassandra.db.memtable.rust.NativeMemtable`. `#![deny(unsafe_code)]`, allowed only in `exports.rs` for `#[unsafe(no_mangle)]`. |

The Java side is in the main tree:
`src/java/org/apache/cassandra/db/memtable/rust/` (`NativeMemtable`,
`RustMemtable`, `PartitionRecords`) and
`test/unit/org/apache/cassandra/db/memtable/rust/` (differential test,
benchmarks). Nothing in Cassandra uses it unless a caller loads it.

## Build

```bash
source ~/.cargo/env                 # Rust 1.85+ (edition 2024); tested with 1.100.1
cd native/memtable-rs
cargo build --release               # target/release/libmemtable_jni.so
```

## Test

```bash
# Rust: unit tests and property tests (proptest, 512 cases per property)
cargo test --release

# Lints and formatting, as run for the spike
cargo clippy --all-targets -- -D warnings -W clippy::pedantic
cargo fmt --all --check

# Miri on the core crate (property tests drop to 4 cases under Miri)
rustup toolchain install nightly --profile minimal --component miri,rust-src
cargo +nightly miri test -p memtable-core

# The differential test against Cassandra's SkipListMemtable and TrieMemtable
# (from the repository root, JDK 17, after `cargo build --release`)
ANT_ARGS=-Dno-build-accord=true ant jar build-test
ANT_ARGS=-Dno-build-accord=true ant testsome \
  -Dtest.name=org.apache.cassandra.db.memtable.rust.RustMemtableDifferentialTest \
  "-Dtest.jvm.args=-Dcassandra.rust_memtable.library=$PWD/native/memtable-rs/target/release/libmemtable_jni.so"
# More sequences: add -Drust_memtable.sequences=400 -Drust_memtable.ops=1000 to test.jvm.args;
# replay a failure: -Drust_memtable.seed=N (printed in the failure message).
```

`bench/asan.sh` runs the differential test against a build with
AddressSanitizer, and `bench/fuzz-boundary.sh` sends random bytes through the
JNI entry points; see the write-up for what each covers.

## Benchmarks

```bash
cargo run --release -p memtable-core --example apply_bench      # Rust-only costs

# Java benchmarks (each in a fresh JVM): per-call overhead, throughput, GC behaviour
native/memtable-rs/bench/run-java-bench.sh ~/.local/opt/jdk-17 g1 2g overhead
native/memtable-rs/bench/run-java-bench.sh ~/.local/opt/jdk-17 g1 2g -Dbench.memtable=trie throughput rust 4 10
BENCH_JVM_OPTS="-Xlog:gc*:file=/tmp/gc.log" \
  native/memtable-rs/bench/run-java-bench.sh ~/.local/opt/jdk-21 zgc 2g gc rust 60
native/memtable-rs/bench/gc-suite.sh     # the full GC matrix used in the write-up
```

## Conventions

* No `unwrap`/`expect` in library code; tests, examples and benchmarks may use
  them (a failure there is a test failure).
* Every reconciliation rule cites the Java it copies (`file:line`, relative to
  `src/java/org/apache/cassandra/db/`).
* Every `unsafe` carries a `// SAFETY:` comment.
