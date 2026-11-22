#!/bin/bash
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

#
# Repeatability harness for the scenario and fault dtests.
#
# Usage: tools/autonomic/repeat-tests.sh [-n RUNS] [-a TYPE] [--dry-run] [--out DIR] CLASS...
#   -n RUNS     repeat each class RUNS times (default 5)
#   -a TYPE     test target type for run-tests.sh (default jvm-dtest)
#   --dry-run   print the commands that would run, then exit 0
#   --out DIR   log directory (default build/repeat-tests/<timestamp>)
#   CLASS...    test classes, or the shorthands @scenario and @fault
#
# This script does NOT build. Assumes the jar and test classes are already
# built. Run these first:
#   .build/build-jars.sh -s
#   ant build-test -q
#
# Each run's JUnit XML and node logs are kept under OUT/CLASS-RUN/.
#
# Env: RUN_TESTS overrides the runner (default .build/run-tests.sh).
#      BUILD_ACCORD=true rebuilds the accord submodule through gradle on every
#      run, as run-tests.sh does by default. Without it, once the accord jar is
#      built, the rebuild is skipped (-Dno-build-accord=true via ANT_ARGS): it
#      loads a small host for minutes and starves timing-sensitive scenarios.

[ -f "$HOME/.local/env-autonomic.sh" ] && . "$HOME/.local/env-autonomic.sh"
set -u

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RUN_TESTS="${RUN_TESTS:-.build/run-tests.sh}"
if [ "${BUILD_ACCORD:-false}" != true ] && ls "$ROOT"/build/lib/jars/cassandra-accord-*.jar >/dev/null 2>&1; then
  export ANT_ARGS="${ANT_ARGS:-} -Dno-build-accord=true"
fi
RUNS=5
TYPE=jvm-dtest
DRY_RUN=false
OUT=""
CLASSES=()

usage() {
  cat <<'EOF'
Usage: tools/autonomic/repeat-tests.sh [-n RUNS] [-a TYPE] [--dry-run] [--out DIR] CLASS...
  -n RUNS     repeat each class RUNS times (default 5)
  -a TYPE     test target type for run-tests.sh (default jvm-dtest)
  --dry-run   print the commands that would run, then exit 0
  --out DIR   log directory (default build/repeat-tests/<timestamp>)
  CLASS...    test classes, or the shorthands @scenario and @fault

This script does NOT build. Run these first:
  .build/build-jars.sh -s
  ant build-test -q
EOF
}

die() { echo "error: $1" >&2; exit 2; }
need() { [ $# -ge 2 ] && [ -n "$2" ] || die "$1 needs an argument"; }

while [ $# -gt 0 ]; do
  case "$1" in
    -n) need "$@"; RUNS="$2"; shift 2 ;;
    -a) need "$@"; TYPE="$2"; shift 2 ;;
    --out) need "$@"; OUT="$2"; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    -h|--help) usage; exit 0 ;;
    @scenario) CLASSES+=(HotSpotScenarioTest LoadSpikeScenarioTest RealLoadSpikeScenarioTest DegradedNodeScenarioTest); shift ;;
    @fault) CLASSES+=(ThrowingSensorFaultTest UnwritableStoreFaultTest CmsUnreachableClaimFaultTest OwnerKilledFaultTest StalledControlThreadFaultTest CircuitBreakerFaultTest); shift ;;
    -*) usage >&2; die "unknown option $1" ;;
    *) CLASSES+=("$1"); shift ;;
  esac
done

case "$RUNS" in ''|*[!0-9]*|0) die "-n needs a positive integer, got '$RUNS'" ;; esac
[ ${#CLASSES[@]} -gt 0 ] || { usage >&2; die "no test classes given"; }
[ -n "$OUT" ] || OUT="$ROOT/build/repeat-tests/$(date +%Y%m%d-%H%M%S)"

if $DRY_RUN; then
  for ((r = 1; r <= RUNS; r++)); do
    for cls in "${CLASSES[@]}"; do
      echo "cd $ROOT && $RUN_TESTS -s -a $TYPE -t $cls > $OUT/$cls-$r.log 2>&1"
    done
  done
  exit 0
fi

mkdir -p "$OUT" || exit 1
RAW=""
for ((r = 1; r <= RUNS; r++)); do
  for cls in "${CLASSES[@]}"; do
    log="$OUT/$cls-$r.log"
    t0=$(date +%s)
    (cd "$ROOT" && $RUN_TESTS -s -a "$TYPE" -t "$cls") > "$log" 2>&1
    rc=$?
    secs=$(( $(date +%s) - t0 ))
    # keep this run's JUnit report and node logs: the next run overwrites them
    keep="$OUT/$cls-$r"
    mkdir -p "$keep"
    find "$ROOT/build/test/output" -name "TEST-*.$cls.xml" -newermt "@$t0" -exec cp {} "$keep/" \; 2>/dev/null
    (cd "$ROOT/build/test/logs" 2>/dev/null && find . -type f -newermt "@$t0" -path "*$cls*" -exec cp --parents {} "$keep/" \;) 2>/dev/null
    echo "run $r/$RUNS $cls exit=$rc ${secs}s"
    RAW+="$cls $rc $secs $log"$'\n'
  done
done

# Summary: one row per class, sorted so each class's durations are in order.
SUMMARY="$OUT/summary.tsv"
{
  printf 'class\truns\tpassed\tfailed\tpass_rate%%\tmin_s\tmedian_s\tmax_s\n'
  printf '%s' "$RAW" | sort -k1,1 -k3,3n | awk '
    function flush() {
      if (cls == "") return
      med = (n % 2) ? d[(n + 1) / 2] : (d[n / 2] + d[n / 2 + 1]) / 2
      printf "%s\t%d\t%d\t%d\t%.1f\t%d\t%.1f\t%d\n", cls, n, p, n - p, 100 * p / n, d[1], med, d[n]
    }
    $1 != cls { flush(); cls = $1; n = 0; p = 0 }
    { n++; d[n] = $3; if ($2 == 0) p++ }
    END { flush() }'
} | tee "$SUMMARY"

FAILED=$(printf '%s' "$RAW" | awk '$2 != 0 { print "  " $4 " (exit " $2 ")" }')
if [ -n "$FAILED" ]; then
  echo "failed runs:"
  echo "$FAILED"
  echo "summary: $SUMMARY"
  exit 1
fi
echo "summary: $SUMMARY"
exit 0
