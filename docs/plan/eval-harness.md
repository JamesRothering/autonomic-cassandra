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

# The gauntlet: an evaluation harness (story 0)

- **Status:** proposed, 2026-11-28; first version being built on `deliverable/E0-eval-harness` while the owner rests.
- **Owner's request (2026-11-28):** before any more work, a robust set of evaluations: "code-monkey on steroids" that injects every kind of stress, usage pattern, corner case, failure and misconfiguration, against our build and against native Cassandra, so we can compare when Cassandra fails or degrades and needs an operator, and we keep on sailing. First target: the products of sprint 1.

## What it answers

For each scenario and each subject: did the cluster **sail** through, **degrade and recover unaided**, or **need an operator**; how long it was outside its service level; and whether any acknowledged write was lost or any deleted value came back.

## Subjects

| Subject | What it is | Why |
|---|---|---|
| `stock` | this repository's build with `autonomic.enabled: false` | identical code except the layer, so any difference is the layer's |
| `autonomic` | the same build with the layer on and acting (`enabled: true`, `dry_run: false`, node actions automatic, cluster actions manual) | sprint 1 as delivered |
| `upstream` (later) | Apache Cassandra trunk at the fork's base `2249e37`, built in a worktree | rules out effects of the fork's inert code |
| `cassandra-5.0` (later) | the 5.0.9 release | the version operators run; the compatibility target |

The harness takes the subject as a directory with `bin/cassandra`, so the later subjects need no harness change.

## Cluster

Real processes, not in-JVM nodes: in-JVM tests share one heap and bypass the native transport (D7 finding T7.9 (3)), so they cannot show GC, memory or client-observed behaviour.

- 3 nodes on `127.0.0.1`, `.2`, `.3` (the loopback range needs no setup on Linux), replication factor 3, `NetworkTopologyStrategy`, one datacenter.
- Each node from a scratch copy of `conf/` (`CASSANDRA_CONF`) with its own data, commit log and log directories under the run directory, as `tools/autonomic/split-demo/split-demo.sh` does.
- Each node runs in a **systemd user scope** (`systemd-run --user --scope`), which on this machine allows CPU quota and memory limits without root (cgroup v2 controllers `cpu memory pids` delegated to the user). That gives a container-like limit to the node and two fault types below.
- Sized for this machine (4 cores, 5 GB RAM, about 7 GB free disk): heap 512 MiB per node by default; scenario data kept small; run directories deleted after their reports are written, except logs.

## Clients

Two, because the owner's point is that the client sees failures first:

1. **Load:** `tools/bin/cassandra-stress` (Java driver) with profiles from `tools/autonomic/stress/` and the harness's own, at a fixed rate per phase, reporting throughput, latency and errors per interval.
2. **Probe and ledger:** a Python client using the driver bundled with cqlsh (`lib/cassandra-driver-internal-only-3.29.0.zip`, put on `sys.path` as cqlsh does), at a low fixed rate, with a 2-second client timeout (the Java driver's default, **unverified** here). Every operation is timed and its outcome recorded: success, client timeout, server timeout, unavailable, overloaded, other error. It keeps a **ledger** of acknowledged writes and deletes at `QUORUM`, and at the end of the run reads every ledger key at `QUORUM`: an acknowledged write that is missing is a **lost write**; a deleted key that returns is a **zombie**.

## Scenarios

A scenario is a YAML file: category, configuration overrides per node, workload phases, a timeline of faults, the service level, and the recovery deadline. Default timeline: 60 s warm-up, 90 s baseline, the fault (90 s, or until the end for misconfigurations), 240 s recovery.

Faults available without root:

| Fault | How |
|---|---|
| node freeze (a long STW pause) | `SIGSTOP`, then `SIGCONT` |
| node crash and restart | `kill -9`, restart after a delay |
| CPU starvation (noisy neighbour, throttled container) | scope `CPUQuota` lowered at run time (`systemctl --user set-property`) |
| memory limit (container) | scope `MemoryMax` |
| client-facing outage | `nodetool disablebinary` / `enablebinary` |
| gossip outage | `nodetool disablegossip` / `enablegossip` |
| misconfiguration | `cassandra.yaml` overrides for one node or all, at start |
| client misuse and corner cases | workload profiles: hot partition, wide partitions, tombstone-heavy, large batches, `ALLOW FILTERING` scans, TTL mass expiry, LWT contention, client timestamps far in the future |

Not available without root, so not in the first version: network partitions, delay and loss between nodes (`tc` netem and `iptables` on loopback, filtered by node address); full or slow disks (small loop-mounted filesystems per node, and a `dm-delay` device); cgroup I/O limits; clock skew of a node (`faketime` is not installed). The owner offered root access (2026-11-28); a narrow sudo rule for `tc`, `iptables`, `ip`, `losetup`, `mount`, `umount`, `dmsetup` and `mkfs.ext4` is enough, and these faults are added once it is in place.

First set, eight scenarios, chosen to touch what sprint 1's layer claims to handle and what it does not:

| # | Scenario | Category | Expectation for the layer (to be shown, not assumed) |
|---|---|---|---|
| S0 | steady mixed load | control | both sail; establishes noise |
| S1 | load spike 3x the baseline rate for 90 s | usage | layer ladder acts (yield, pools, throttle) |
| S2 | hot partition (30% of operations on one key) | usage | layer diagnoses; split is a cluster action, manual here |
| S3 | node freeze 20 s | failure | Cassandra's own routing; layer adds severity |
| S4 | node crash, restart after 120 s | failure | hints replay on return |
| S5 | one node's CPU quota at 25% for 120 s | failure | degraded-node handling |
| S6 | one node with `concurrent_reads: 2` | misconfig | pool growth |
| S7 | `compaction_throughput: 1MiB/s` with write-heavy load | misconfig | compaction backlog handling |

## Verdicts

The service level of a run is derived from its own baseline phase, so each subject is judged against itself: client p99 within 3 times the baseline p99 (and at least 20 ms), and client errors (timeouts and failures) below 0.5% of operations, per 10-second interval of the probe.

- **sailed:** no interval outside the service level.
- **recovered:** outside it during the fault, back inside within the recovery deadline after the fault ends, and stayed there.
- **needs operator:** not back inside by the deadline, or a node down at the end, or any lost write or zombie. For a misconfiguration (no end), judged on the last 120 s.

Each verdict comes with the numbers: intervals outside the service level, worst p99, error counts by kind, lost writes and zombies, and for `autonomic` the layer's diagnoses and actions from `nodetool autonomic history`.

## Output

A run directory under `build/gauntlet/<run-id>/` (not committed): per subject and scenario the probe's per-operation log, stress output, node logs, the layer's history, and `result.json`. The harness writes `report.md`: the matrix of scenarios by subjects with verdicts and key numbers, and a short list of what differed.

## Repeatability

A single run is evidence, not proof. Each scenario is to be run at least 3 times per subject before a difference is reported as a finding; the first run tonight is one pass, to shake out the harness.

## Not in the first version

Network and disk faults (root), clock skew, `upstream` and `cassandra-5.0` subjects, multi-datacenter, the Python driver extension, more than three nodes.
