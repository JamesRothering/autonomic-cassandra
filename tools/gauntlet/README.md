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

# The gauntlet

An evaluation harness: it starts a real 3-node Cassandra cluster (127.0.0.1-3, RF 3, one datacenter), drives load and a
correctness probe, injects one scenario's faults on a timeline, and writes a verdict for a subject. Design:
`docs/plan/eval-harness.md`. Subjects are `stock` (`autonomic.enabled: false`) and `autonomic` (`enabled: true`,
`dry_run: false`, `startup_grace: 30s`, `safety.cluster_approval: manual`, node actions automatic). Both use the same
build directory; only the yaml differs.

## Run

    source ~/.local/env-autonomic.sh        # JDK 17; the build must exist (build/apache-cassandra-*.jar, build/tools/lib/stress.jar)
    tools/gauntlet/gauntlet.py run --subject stock --scenario tools/gauntlet/scenarios/S0.yaml --out build/gauntlet/try1 --quick
    tools/gauntlet/gauntlet.py matrix --subjects stock,autonomic --scenarios S0,S1,S2,S3,S4,S5,S6,S7 --out build/gauntlet/m1

One cluster at a time (4 cores, 5 GB RAM): a matrix runs its pairs sequentially. `--quick` scales the timeline down
(warm-up 30 s, baseline 45 s, fault 45 s, recovery 90 s, recovery deadline 45 s) for smoke tests; the full timeline is
60 s, 90 s, fault 90 s (S4 and S5: 120 s), 240 s, recovery deadline 120 s. A quick run takes about 5 minutes including
cluster start and the final ledger check. `--probe-rate`, `--heap`, `--keep-data` are options. Ctrl-C and SIGTERM stop
everything; teardown also runs on every error path. Check with `pgrep -a java` afterwards.

## Files

| file | what |
|---|---|
| `gauntlet.py` | command line, timeline runner, stress launcher, fault dispatcher, `result.json` and `report.md` |
| `cluster.py` | scratch conf per node, one systemd user scope per node, start/UN wait, SIGSTOP/SIGCONT, kill -9, restart, `CPUQuota` and `MemoryMax` at run time, nodetool, teardown |
| `probe.py` | probe and ledger client (bundled driver), per-operation log, final QUORUM check |
| `verdict.py` | the service level and the sailed / recovered / needs_operator rules |
| `miniyaml.py` | a small YAML reader (stdlib only; PyYAML is not installed) for the scenario files |
| `scenarios/S0..S7.yaml` | the eight scenarios |
| `profiles/*.yaml` | cassandra-stress profiles (RF 3, `datacenter1`) |

## Output (`<out>/<subject>-<scenario>/`)

`result.json` (all numbers), `probe.csv` (every probe operation: time since start, kind, key, latency, outcome),
`stress-base.log` / `stress-extra.log`, `autonomic-history-node<N>.txt` (autonomic subject), `cluster/node<N>/logs/`
(node logs, kept) and `cluster/node<N>/conf/` (the exact conf). Data and commit logs are deleted after the report is
written. `report.md` is at `<out>` (the matrix, the numbers, and what differed). Nothing under `build/` is committed.

## Method notes

- Probe: 40 ops/s open loop, QUORUM, 2 s client timeout: 50% insert of a new key, 30% read of an acknowledged key, 20%
  delete of an acknowledged key. Outcomes: ok, client_timeout, server_read_timeout, server_write_timeout, unavailable,
  overloaded, other. An insert or delete that did not complete is "unknown" (reported as present/absent at the end),
  never lost. Lost write: acknowledged insert not found at the end and not deleted. Zombie: acknowledged delete whose
  key returns. A read of an acknowledged key that finds nothing mid-run is counted as a "stale read" (not a verdict input).
- Load: cassandra-stress `-rate fixed=500/s` (a constant, not a calibration: about 1500 replica operations per second
  on three 512 MiB nodes, busy but well below saturation on this box; S7 uses 400/s of 1 KB writes). S1 adds a second
  stress process at 1000/s for the fault window (3x in total; it starts a few seconds into the window).
- Service level: per 10 s interval of the probe from the end of warm-up, p99 at most max(3 x the run's own baseline p99, 20 ms)
  and errors (any non-ok) below 0.5%. Sailed: no interval outside after the fault starts. Recovered: outside at some point,
  back inside by the recovery deadline after the fault ended and stayed. Needs operator: not back by the deadline, a node
  not UN at the end (a running node gets 90 s to become UN), any lost write or zombie, or ledger keys that cannot be read.
  Misconfigurations (S6, S7) have no end: the setting is in place from the start, the baseline is measured with it, and
  the verdict looks at the last 120 s (quick: last 40% of the run).
- Layer: `nodetool autonomic history` and `--diagnoses` are saved per node and counted in `result.json` (`layer`).

## Gaps

- No root, so no network partitions or delay/loss, no full or slow disks, no cgroup I/O limits, no clock skew
  (`faketime` is not installed). Faults available: SIGSTOP/SIGCONT, kill -9 and restart, CPU quota, memory limit,
  nodetool disablebinary/gossip, yaml misconfiguration, workload profiles.
- Not built: `upstream` and `cassandra-5.0` subjects, workload corner cases beyond the hot partition (wide partitions,
  tombstones, big batches, filtering scans, TTL expiry, LWT, far-future timestamps), more than 3 nodes, multi-DC.
- Stress `-rate fixed` spreads load over three coordinators but the stress client itself shares the 4 cores with the
  nodes; its own latency is not used in verdicts (the probe's is).
- A single run is evidence, not proof: run each pair at least 3 times before reporting a difference.
