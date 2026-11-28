#!/usr/bin/env bash
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

# Mutation check of the differential oracle: each mutant breaks one reconciliation rule in the Rust core, and
# RustMemtableDifferentialTest must then fail. Line numbers refer to the commit that added this script; check them
# (the script reports a mutant whose sed did not apply) before running it on a later revision.
# Needs cargo and ant on PATH and a clean working tree for core/src (it restores files with git checkout).
set -u
cd "$(dirname "$0")/../../.."
F=native/memtable-rs/core/src/model.rs
declare -a M=(
 "362s/let existing_deletion_won = self.deletion.time.supersedes(update_deletion.time);/let existing_deletion_won = !update_deletion.time.supersedes(self.deletion.time);/#row-deletion tie goes to existing"
 "159s/self.ttl < other.ttl/self.ttl > other.ttl/#liveness equal-expiry TTL tie flipped"
 "317s/!= Ordering::Less/!= Ordering::Greater/#cell value tie-break flipped"
 "419s/if active.is_live() ||/if true ||/#read-time partition-deletion filter removed"
 "86s/\&\& self.local_deletion_time > other.local_deletion_time/\&\& self.local_deletion_time < other.local_deletion_time/#deletion-time ldt tie flipped"
 "202s/liveness.timestamp > self.time/liveness.timestamp >= self.time/#shadowable deletion cancelled at equal timestamp"
 "276s/let left_is_tombstone = left.ttl == NO_TTL;/let left_is_tombstone = left.ttl != NO_TTL;/#tombstone-vs-expiring preference flipped"
 "mt:254s/if deletion.supersedes(partition.deletion) {/if !deletion.is_live() {/#partition deletion: last writer wins"
)
echo "baseline (no mutant) => $(ANT_ARGS=-Dno-build-accord=true ant testsome -Dtest.name=org.apache.cassandra.db.memtable.rust.RustMemtableDifferentialTest "-Dtest.jvm.args=-Dcassandra.rust_memtable.library=$PWD/native/memtable-rs/target/release/libmemtable_jni.so" 2>&1 | grep -E "Tests run:" | head -1)"
for m in "${M[@]}"; do
  expr="${m%%#*}"; name="${m##*#}"
  F=native/memtable-rs/core/src/model.rs
  case "$expr" in mt:*) F=native/memtable-rs/core/src/memtable.rs; expr="${expr#mt:}";; esac
  sed -i "$expr" $F
  if git diff --quiet $F; then echo "MUTANT NOT APPLIED: $name"; continue; fi
  (cd native/memtable-rs && cargo build --release -q 2>&1 | tail -2)
  out=$(ANT_ARGS=-Dno-build-accord=true ant testsome -Dtest.name=org.apache.cassandra.db.memtable.rust.RustMemtableDifferentialTest "-Dtest.jvm.args=-Dcassandra.rust_memtable.library=$PWD/native/memtable-rs/target/release/libmemtable_jni.so" 2>&1 | grep -E "Tests run:" | head -1)
  echo "$name => $out"
  git checkout -q $F
done
(cd native/memtable-rs && cargo build --release -q)
echo DONE
