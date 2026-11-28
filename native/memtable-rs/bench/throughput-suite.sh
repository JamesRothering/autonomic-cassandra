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

# Write throughput, 1/2/4 writer threads, Java memtables vs Rust, JDK 17 + G1, REPEATS times, each in a fresh JVM
# (5 s warm-up, then SECONDS_PER_RUN measured). Prints one result line per run and the medians.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
OUT=${OUT:-build/rust-throughput}; REPEATS=${REPEATS:-3}; SECONDS_PER_RUN=${SECONDS_PER_RUN:-10}
JDK=${JDK:-$HOME/.local/opt/jdk-17}; FLUSH_EVERY=${FLUSH_EVERY:-300000}
cd "$here/../../.."
mkdir -p "$OUT"
for r in $(seq 1 "$REPEATS"); do
  for v in "java skiplist heap_buffers" "java trie offheap_objects" "rust - -" "rust-mimalloc - -"; do
    read -r impl memtable alloc <<< "$v"
    for t in 1 2 4; do
      name="$impl-$memtable-$alloc-t$t-r$r"
      lib=$PWD/native/memtable-rs/target/release/libmemtable_jni.so
      if [[ $impl == rust* ]]; then
        name="$impl-t$t-r$r"; memtable=skiplist; alloc=heap_buffers
        [[ $impl == rust-mimalloc ]] && lib=$PWD/native/memtable-rs/target/mimalloc/release/libmemtable_jni.so
      fi
      RUST_LIB=$lib "$here/run-java-bench.sh" "$JDK" g1 2g -Dbench.memtable="$memtable" -Dbench.allocation="$alloc" -Dbench.flush_every="$FLUSH_EVERY" \
        throughput "${impl%-mimalloc}" "$t" "$SECONDS_PER_RUN" > "$OUT/$name.out" 2>&1 || echo "FAILED: $name"
      echo "$name $(grep -h 'result: ' "$OUT/$name.out" | sed 's/.*result: //')"
    done
  done
done | tee "$OUT/summary.txt"
python3 - "$OUT/summary.txt" <<'PY'
import re, sys, statistics, collections
runs = collections.defaultdict(list)
for line in open(sys.argv[1]):
    m = re.match(r'(\S+)-r\d+ .*ops_per_sec=(\d+).*memtable_bytes_at_flush=(\d+)', line)
    if m:
        runs[m.group(1)].append((int(m.group(2)), int(m.group(3))))
print('\nmedian ops/s [min-max], memtable MiB at flush (median)')
for k, v in sorted(runs.items()):
    ops = [x[0] for x in v]
    print(f'{k:40s} {statistics.median(ops):9.0f} [{min(ops)}-{max(ops)}]  {statistics.median(x[1] for x in v) / 2**20:7.0f} MiB  n={len(v)}')
PY
