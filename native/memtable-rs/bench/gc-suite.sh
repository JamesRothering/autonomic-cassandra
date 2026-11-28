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

# The GC matrix of docs/spike/rust-memtable.md: each memtable variant, on JDK 17 + G1 and JDK 21 + generational
# ZGC, REPEATS times, SECONDS each, in a fresh JVM. Writes GC logs and bench output to OUT (default build/rust-gc)
# and prints a summary (bench/gc-summary.py).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
OUT=${OUT:-build/rust-gc}; REPEATS=${REPEATS:-3}; SECONDS_PER_RUN=${SECONDS_PER_RUN:-60}; HEAP=${HEAP:-2g}
JDK17=${JDK17:-$HOME/.local/opt/jdk-17}; JDK21=${JDK21:-$HOME/.local/opt/jdk-21}
FLUSH_EVERY=${FLUSH_EVERY:-500000}
cd "$here/../../.."
mkdir -p "$OUT"
variants=("java skiplist heap_buffers" "java skiplist offheap_objects" "java trie offheap_objects" "rust skiplist heap_buffers")
for r in $(seq 1 "$REPEATS"); do
  for jvm in "17 g1 $JDK17" "21 zgc $JDK21"; do
    read -r jdk gc home <<< "$jvm"
    for v in "${variants[@]}"; do
      read -r impl memtable alloc <<< "$v"
      name="jdk${jdk}-${gc}-${impl}-${memtable}-${alloc}-r${r}"
      [[ $impl == rust ]] && name="jdk${jdk}-${gc}-rust-r${r}"
      echo "== $name"
      BENCH_JVM_OPTS="-Xlog:gc*,safepoint:file=$OUT/$name.gc.log:uptime,level,tags" \
        "$here/run-java-bench.sh" "$home" "$gc" "$HEAP" -Dbench.memtable="$memtable" -Dbench.allocation="$alloc" \
        -Dbench.flush_every="$FLUSH_EVERY" gc "$impl" "$SECONDS_PER_RUN" > "$OUT/$name.out" 2>&1 || echo "FAILED: $name"
      grep -E '(setup|result): ' "$OUT/$name.out" | sed 's/.* - //' || true
    done
  done
done
python3 "$here/gc-summary.py" "$OUT"
