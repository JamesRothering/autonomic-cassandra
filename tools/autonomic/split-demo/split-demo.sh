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

# Split demo (T7.10): shows the layer proposing a partition split by itself on a real single node.
#
# Starts a node from a scratch copy of conf/ (CASSANDRA_CONF) with the overlay autonomic-split-demo.yaml, storage in
# the scratch directory, a small heap and no changes to the checkout. It then drives a hot partition with the
# cassandra-stress profile hot-spot-wide.yaml and polls `nodetool autonomic history` until a
# partition_split action is recorded or the timeout passes, prints the action and the diagnosis it answers, stops
# the node and removes the scratch directory. The cluster gate is manual, so the action stays PROPOSED and nothing
# is split. Exit status: 0 proposal seen, 1 timeout, 2 setup failure.
#
# Needs the built jar (`.build/build-jars.sh -s`), the stress tool (build/tools/lib/stress.jar), a JDK on the PATH
# or in JAVA_HOME, and free ports 7000, 7199, 9042.
#
# Usage: tools/autonomic/split-demo/split-demo.sh
#   TIMEOUT_SECONDS  total time allowed for the proposal after the stress starts (default 240)
#   MAX_HEAP_SIZE    heap of the node (default 768M)
#   KEEP=1           keep the scratch directory (and print its path)

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CASSANDRA_HOME="$(cd "$HERE/../../.." && pwd)"
export CASSANDRA_HOME
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-240}"
export MAX_HEAP_SIZE="${MAX_HEAP_SIZE:-768M}"
PROFILE="$HERE/hot-spot-wide.yaml"

die() { echo "split-demo: $*" >&2; exit 2; }

ls "$CASSANDRA_HOME"/build/apache-cassandra*.jar >/dev/null 2>&1 || die "no build/apache-cassandra*.jar; run .build/build-jars.sh -s"
[ -f "$CASSANDRA_HOME/build/tools/lib/stress.jar" ] || die "no build/tools/lib/stress.jar (cassandra-stress is not built)"
if (exec 3<>/dev/tcp/127.0.0.1/9042) 2>/dev/null; then die "port 9042 is in use; stop the other node"; fi

WORK="$(mktemp -d "${TMPDIR:-/tmp}/split-demo.XXXXXX")"
export CASSANDRA_CONF="$WORK/conf"
export CASSANDRA_LOG_DIR="$WORK/logs"
PIDFILE="$WORK/cassandra.pid"
STRESS_PID=""

stop_all()
{
    # the stress tool is a shell wrapper around a JVM: stop the child first
    [ -n "$STRESS_PID" ] && { pkill -P "$STRESS_PID" 2>/dev/null; kill "$STRESS_PID" 2>/dev/null; }
    if [ -s "$PIDFILE" ]; then
        pid="$(cat "$PIDFILE")"
        kill "$pid" 2>/dev/null
        for _ in $(seq 1 60); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
        kill -9 "$pid" 2>/dev/null
    fi
    if [ -n "${KEEP:-}" ]; then echo "scratch directory kept: $WORK"; else rm -rf "$WORK"; fi
}
trap stop_all EXIT

# scratch conf: the stock files and storage inside the scratch directory (bin/cassandra.in.sh fixes
# cassandra.storagedir at $CASSANDRA_HOME/data, so every directory is named in the yaml). The layer is off in the
# first phase: the policy probes the size of a hot partition once and keeps the result for an hour, so the partition
# is loaded before the layer starts, or a probe taken mid-load would judge it small.
mkdir -p "$WORK/logs" "$WORK/storage"
cp -r "$CASSANDRA_HOME/conf" "$CASSANDRA_CONF"
cat >> "$CASSANDRA_CONF/cassandra.yaml" <<YAML

cluster_name: split-demo
data_file_directories: ["$WORK/storage/data"]
commitlog_directory: $WORK/storage/commitlog
saved_caches_directory: $WORK/storage/saved_caches
hints_directory: $WORK/storage/hints
accord:
  journal_directory: $WORK/storage/accord_journal
YAML

nodetool() { "$CASSANDRA_HOME/bin/nodetool" "$@"; }
stress() { "$CASSANDRA_HOME/tools/bin/cassandra-stress" "$@"; }

start_node()
{
    : > "$CASSANDRA_LOG_DIR/system.log"
    "$CASSANDRA_HOME/bin/cassandra" -p "$PIDFILE" > "$WORK/stdout.log" 2>&1 || die "cassandra did not start; see $WORK/stdout.log (KEEP=1)"
    local began=$SECONDS
    until grep -q "Starting listening for CQL clients" "$CASSANDRA_LOG_DIR/system.log" 2>/dev/null; do
        [ $((SECONDS - began)) -gt 180 ] && die "the node did not start within 180 s (KEEP=1 keeps $CASSANDRA_LOG_DIR)"
        sleep 2
    done
    echo "split-demo: node up after $((SECONDS - began)) s"
}

stop_node()
{
    local pid
    pid="$(cat "$PIDFILE")"
    kill "$pid" 2>/dev/null
    for _ in $(seq 1 60); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
}

echo "split-demo: scratch directory $WORK, heap $MAX_HEAP_SIZE"

# phase 1, layer off: the hot partition (id 1) must be large before the policy proposes a split on one node (see
# hot-spot-wide.yaml)
start_node
stress user profile="$PROFILE" "ops(hot_write=1)" n=120000 -rate threads=8 -node 127.0.0.1 > "$WORK/stress-load.log" 2>&1 || die "stress load phase failed; see $WORK/stress-load.log (KEEP=1)"
echo "split-demo: hot partition loaded (about 150 MiB, 24000 rows); restarting with the layer"
stop_node

# phase 2: the layer on, then a mix of hot reads, slices and writes with a little cold traffic
cat "$HERE/autonomic-split-demo.yaml" >> "$CASSANDRA_CONF/cassandra.yaml"
start_node
start=$SECONDS
until nodetool autonomic status 2>/dev/null | grep -q "Armed: true"; do
    [ $((SECONDS - start)) -gt 120 ] && die "the layer was not armed"
    sleep 2
done
# a split is a cluster action: enable them (the gate stays manual)
nodetool autonomic cluster enable --yes > /dev/null || die "could not enable cluster actions"
echo "split-demo: layer armed, cluster actions enabled (manual approval); driving the hot spot (timeout ${TIMEOUT_SECONDS} s)"
stress user profile="$PROFILE" "ops(hot_point=12,hot_write=3,hot_slice=1,cold_point=2)" duration="$((TIMEOUT_SECONDS + 30))s" -rate threads=4 -node 127.0.0.1 > "$WORK/stress-hot.log" 2>&1 &
STRESS_PID=$!

t0=$SECONDS
found=""
while [ $((SECONDS - t0)) -lt "$TIMEOUT_SECONDS" ]; do
    found="$(nodetool autonomic history 2>/dev/null | grep "actuator=partition_split" | head -1)"
    [ -n "$found" ] && break
    kill -0 "$STRESS_PID" 2>/dev/null || { echo "split-demo: stress ended early; see $WORK/stress-hot.log (KEEP=1)"; break; }
    sleep 3
done
elapsed=$((SECONDS - t0))

if [ -z "$found" ]; then
    echo "split-demo: no partition_split action after ${elapsed} s. Diagnoses:"
    nodetool autonomic history --diagnoses 2>&1 | grep -v baseline_drift | head -10
    # the policy's last word on the partition (profiling, or not_applicable(<reason>)); it alternates, so sample a few times
    for _ in $(seq 1 12); do
        "$CASSANDRA_HOME/bin/cqlsh" -e "select evidence from system_views.autonomic_diagnoses where type = 'hot_partition' allow filtering" 2>/dev/null | grep -o "'layout': '[^']*'"
        sleep 2
    done | sort | uniq -c
    exit 1
fi
echo "split-demo: the layer proposed a split by itself after ${elapsed} s of load:"
echo "$found"
echo "split-demo: the diagnosis behind it:"
nodetool autonomic history --diagnoses 2>&1 | grep "hot_partition" | head -2
echo "split-demo: approve it with 'nodetool autonomic approve <id>' on a node you keep; this run stops the node now"
exit 0
