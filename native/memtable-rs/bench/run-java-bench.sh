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

# Runs RustMemtableBench in its own JVM, from the repository root, after
#   ant jar build-test   (with JDK 17) and   cargo build --release   (in native/memtable-rs).
#
# Usage: run-java-bench.sh JAVA_HOME g1|zgc HEAP [-Dprop=value ...] MODE ARGS...
#   e.g. run-java-bench.sh ~/.local/opt/jdk-17 g1 2g -Dbench.memtable=trie throughput rust 4 10
# Extra JVM options (e.g. -Xlog:gc*:file=...) can be passed in BENCH_JVM_OPTS.
set -euo pipefail
cd "$(dirname "$0")/../../.."
java_home=$1; gc=$2; heap=$3; shift 3
props=()
while [[ $# -gt 0 && $1 == -D* ]]; do props+=("$1"); shift; done

case $gc in
  g1)  gc_opts=(-XX:+UseG1GC) ;;
  zgc) gc_opts=(-XX:+UseZGC -XX:+ZGenerational) ;;
  *) echo "gc must be g1 or zgc" >&2; exit 2 ;;
esac

# The module opens Cassandra needs on JDK 17+, from build.xml's test argument lists.
opens=(
  --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.base/jdk.internal.ref=ALL-UNNAMED
  --add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-exports java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED
  --add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED --add-exports java.rmi/sun.rmi.server=ALL-UNNAMED
  --add-exports java.sql/java.sql=ALL-UNNAMED --add-exports java.base/java.lang.ref=ALL-UNNAMED
  --add-exports jdk.unsupported/sun.misc=ALL-UNNAMED
  --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED
  --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.math=ALL-UNNAMED
  --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED
  --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED
  --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/jdk.internal.ref=ALL-UNNAMED
  --add-opens java.base/jdk.internal.reflect=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED
)

cp="build/apache-cassandra-7.0-SNAPSHOT.jar:build/test/classes:test/conf:build/lib/jars/*:build/test/lib/jars/*"
exec env MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4} "$java_home/bin/java" -Xms"$heap" -Xmx"$heap" "${gc_opts[@]}" "${opens[@]}" \
  -javaagent:build/lib/jars/jamm-0.4.0.jar \
  -Dcassandra.storagedir=build/test/cassandra -Dcassandra.memtable_row_overhead_computation_step=100 \
  -Dcassandra.rust_memtable.library="${RUST_LIB:-$PWD/native/memtable-rs/target/release/libmemtable_jni.so}" \
  -Djava.security.manager=allow -Djdk.attach.allowAttachSelf=true \
  ${BENCH_JVM_OPTS:-} "${props[@]}" \
  -cp "$cp" org.apache.cassandra.db.memtable.rust.RustMemtableBench "$@"
