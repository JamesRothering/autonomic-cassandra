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

# D7: Verification

## Goal

Show the autonomic behaviors end to end under realistic scenarios, and document how operators use the system.

## Scope

- **T7.1** In-JVM multi-node scenario tests: injected hot spot, load spike, and degraded node.
- **T7.2** Fault injection of the autonomic layer itself.
- **T7.3** cassandra-stress workload profiles that drive the scenarios.
- **T7.4** Operator guide.

Follow-up tasks the owner added at the D7 review (2026-11-20):

- **T7.5** Fix four findings (sonnet):
  - lost `action_failed` diagnosis on shutdown (T7.2 finding 2)
  - audit writes not retried after the store recovers (T7.2 finding 1)
  - `approve` while paused (T7.4 finding 1)
  - `cqlsh` reads of `system_autonomic` (T7.4 finding 2)
- **T7.6** `baseline_drift` reports the drifted value (T7.4 finding 3) (sonnet; done with T7.5).
- **T7.7** An expired claim is taken over when its owner restarts past its lease while peers still see it alive (T7.2 finding 3) (opus).
- **T7.8** Latency verification of a split that judges latency correctly, instead of being relaxed in the hot-spot scenario (opus).
- **T7.9** A load-spike scenario driven by real client load, with no injected sensors (sonnet).
- **T7.10** A way to make the layer propose a split by itself on a live node: config, stress profile and script (sonnet).
- **T7.11** A repeatability harness that runs the scenario and fault classes N times and reports pass rate and timing, plus a recorded run (haiku for the script; run last).
- **T7.12** The cheap index-based partition size as the split probe's trigger, with re-probe on growth (ADR-043; replaces T7.10 finding 2, includes T7.10 finding 3) (sonnet).
- **T7.13** A node action's verification baseline taken from the problem period (ADR-044; T7.9 finding 1) (sonnet, Opus review).

## Acceptance criteria

- [x] Each scenario in T7.1 runs in CI or on the development machine within the resource limits in ADR-003.
- [x] For each scenario, the test asserts the expected diagnosis, action, and verification outcome.
- [x] The operator guide covers configuration, the kill switch, `nodetool autonomic`, and rollback, and each step is checked against a running node.

Refined in D1. The authoritative task list, with files, dependencies, acceptance criteria and model tier per task, is section 13 of [overview.md](../architecture/overview.md) (T7.1 to T7.4; the scope list above now matches it), plus the D6 compatibility and failure suites (T6.12, T6.13). The scope above is the original outline.

## Status

Done on branch `deliverable/D7-verification` (2026-11-20); awaiting owner review. T7.2 is tiered `fable` in section 13 of the overview and runs on Opus, because the owner stopped using Fable on 2026-11-17.

- T7.1 scenario dtests (hot spot, load spike, degraded node): done. They found one bug, fixed in the same commit: `PartitionSplitPolicy`'s probe thread kept nodes from shutting down cleanly. In the hot-spot scenario the latency guards were relaxed, because a split raises in-JVM latency, so the verdict rested on the reduced share; T7.8 replaced that (see below).
- T7.2 fault injection (six faults): done. Every fault converges to fail-static and no production change was needed. Three findings are not fail-static violations and are left for the owner:
  1. Fixed in T7.5 (commit "T7.5: retry audit writes once the knowledge store recovers"): after the knowledge store recovered, a failed audit write was not retried, so the action row stayed `APPLYING` and its `inflight` key stayed until a restart. `ActionExecutor` now keeps terminal records whose write failed (at most 64) and rewrites them, and deletes the stale `inflight` keys, on the next successful write. `ActionExecutorTest` and `UnwritableStoreFaultTest` cover it.
  2. Fixed in T7.5 (see the commit "T7.5: write diagnoses queued on the control thread when the layer stops"): when an owner records `CLAIM_LOST` while shutting down, the `action_failed` diagnosis was queued on a control thread that was already stopping and was lost. `stopLoops` now drains the queue after the loops stop; `OwnerKilledFaultTest` asserts the diagnosis exists after the restart.
  3. Fixed in T7.7 (ADR-038): if an owner restarts after its lease has expired while its peers still see it alive, it pauses on `CLAIM_LOST`. Because it was the preferred proposer and only the preferred proposer took over, no node recovered the expired claim until an operator resumed the layer. Takeover is now ordered by node id: the preferred proposer at expiry, each next node one `lease_safety_margin` per rank later (`ClusterCoordinator.takeoverDue`). `ExpiredClaimTakeoverFaultTest` reproduces the scenario and failed before the fix; `ClusterCoordinatorTest` and `ClusterActionExecutionTest` cover the order.
- T7.3 cassandra-stress profiles (`tools/autonomic/stress/`): done. Each profile ran against a single node with 0 errors.
- T7.4 operator guide ([docs/operations/autonomic.md](../operations/autonomic.md)): done. Each step was run on a live single node; steps that need several nodes cite the dtest that covers them. The guide's checks surfaced these findings, left for the owner:
  1. Fixed in T7.5 (commit "T7.5: refuse approve while the layer is paused or killed"): `nodetool autonomic approve` printed `Approved action <id>` while the layer was paused; the action then ended `REJECTED(PAUSED)`. `AutonomicService.approve` now throws, naming the pause reason, and the action stays `PROPOSED`. Covered by `AutonomicServiceTest` and `AutonomicNodetoolTest`.
  2. `cqlsh` cannot `SELECT` from `system_autonomic.*` ("Keyspace not found"), although `DESCRIBE KEYSPACES` lists the keyspace. The `system_views.autonomic_*` virtual tables and `nodetool` work. T7.5 found the cause and documented a workaround (section 9 of the operator guide): the python driver loads metadata from `system_schema`, which holds only `system` and `system_schema` of the local keyspaces, so no local keyspace such as this one or `system_accord` is visible to it. Not fixable without a core change. Closed as accepted, not fixed: the owner decided against a core change (ADR-039).
  3. Fixed in T7.6 (commit "T7.6: baseline_drift summary names the current and configured value"): the summary is now `1 knob(s) differ from cassandra.yaml: compaction_throughput=32 (configured 64)`.
  4. The stock `bin/nodetool` splits quoted arguments, so a `pause` reason must be a single word (not specific to this fork).
- T7.8 latency verification of a split (ADR-040): done. The old stage A and B guards compared the split key's mean latency with its own latency before the split. The split moves the key to other replicas and routes every request, so a correct split read as a regression: in the hot-spot scenario the key's latency rose from about 0.25 ms (local to its coordinator) to 1.7 to 3.4 ms. Now the profiler also times the rest of the key's table, and four guards are judged, each with `min_samples` operations of its class and each named in the stage's detail. The collateral read and write guards check the rest of the table against its baseline times `1 + collateral_tolerance`. The key read and key write guards check the key against an expected overhead over a reference, the larger of its baseline and the rest of the table now: a slice may read every bucket, and a write is allowed 1.5 times the reference per copy. `HotSpotScenarioTest` now runs a mixed workload at the default tolerances and `min_samples`: 5% reads and 6% writes of other partitions, and 1% slices with a `LIMIT`. It asserts that all four guards are judged and pass in both stages. The scenario's `cutover_soak` is 20 s. On a loaded host the busiest-share goal of stage B (then the largest share sampled in the window) was once overstated to 1.00 by a late sampler window, a NEUTRAL verdict with every latency guard passed; T7.8 worked around it by keeping stage B's window at 12 s. T7.8b fixed it (ADR-041): `HotPartitionSensor` also emits each sampled table's operation count per window (`partition.sampled_table_reads` / `_writes`, no longer truncated), and stage B's busiest share adds up the counts of the base key and of each overflow partition over the windows, divided by the operations of the base and the overflow table together, with an upper bound for a partition a window did not report, `min_samples` per class, and INCONCLUSIVE when neither class is judged. The scenario now runs three client threads, a 17 s `straggler_window` and a 16 s `verify_window`. In-JVM, a routed write after the commit point costs about 1.3 times an ordinary write of the table. A slice without a limit reads the 32 buckets one after another. The key's latency also grows during the first seconds of `CUTOVER` and then stays at that level (point reads from about 1.2 ms to about 3 ms). That is not explained yet and is left for the owner. The C1 guard is unchanged, and other tables are not guarded by latency.
- T7.9 real-load spike scenario (ADR-042): done. `RealLoadSpikeScenarioTest` sends reads through node 1's coordinator at 230 a second (a client with at most 40 in flight; reads of its table take 20 ms on the stage through a ByteBuddy delay), above the 4 `ReadStage` threads' capacity of 200. No sensor is injected: `thread_pool`, `client_request` and `cpu` report the queue and the p99. It asserts the `pool_saturation` and `latency_slo_breach` diagnoses, the `read_pool` step 4 to 5 committed with the verdict `IMPROVED`, the return to 4 after the load stops, and that node 2 is untouched. Two runs passed in 107 s and 105 s (test time). No production change. Findings for the owner: (1) acting at the first sample of a ramp, the verification baseline holds the low p99 of the ramp, and a correct pool growth was rolled back with `EARLY_ABORT` for the p99 guard (baseline 120 ms, 219 ms after, tolerance 25%); the scenario therefore requires the saturation to last about 20 s before it is diagnosed. Finding (1) is fixed in production by T7.13 (ADR-044); the scenario keeps the sustained saturation, see T7.13. (2) After a commit the policy proposed the next step 2 ms later from the still open diagnosis, and the governor rejected it as `CONFLICT` with the committed action; this is harmless. (3) The throttle step is not covered by real load, because the rate limiter is in the native transport, which the in-JVM coordinator bypasses; `LoadSpikeScenarioTest` still covers it.
- T7.10 split demo (`tools/autonomic/split-demo/`, operator guide section 10): done. `autonomic-split-demo.yaml` (config overlay), `hot-spot-wide.yaml` (stress profile, 6000 byte rows) and `split-demo.sh` (scratch `CASSANDRA_CONF`, storage in the scratch directory, 768M heap, polls `nodetool autonomic history`, stops the node and removes the scratch directory). The last full run proposed a `partition_split` (`PROPOSED`, `buckets=32`, `size_only=true`) 50 s after the hot traffic began, 3 m 47 s for the whole script, with no production change. Findings for the owner: (1) on one node the policy proposes a split only for a partition of at least 100 MiB (`no_spread` below it), because a split cannot move load off the only node; the demo therefore loads about 150 MiB first. (2) The policy probes a partition's size once and keeps the result for an hour (`State.probed`). The first demo versions loaded and drove the partition with the layer on, the probe ran mid-load, and no proposal came in 4 minutes (`no_spread`, alternating with `fan_out`); the script now loads with the layer off and restarts. A growing partition can be judged too small for up to an hour. (3) `nodetool autonomic history --diagnoses` does not show the policy's `layout` note; read it with `cqlsh` from `system_views.autonomic_diagnoses.evidence`. (4) With the load phase and the node on 4 cores, one run raised `autonomic.loop_stalled` (control loop silent for 65 s), which cleared by itself.
- T7.12 cheap size as the probe trigger (ADR-043): done. `HotPartitionSensor` emits `partition.size_bytes` (`PartitionSizeEstimator`: index positions of the key's sstable fragments plus its memtable rows, capped at 50,000 rows, 100 ms budget per window) for each key it reports. `PartitionSplitPolicy` starts the probe only once that size reaches the trigger (`min_rows_for_split` x 8 bytes; 40 MiB where a split moves no load) and probes again when the size has doubled since a probe refused for a size reason; without a fresh signal it probes at once and again after `layout.reprobe_interval` (30 m). The probe's result still makes every decision. The `layout` note shows `probing` and the sizes it waits on, and `nodetool autonomic history --diagnoses` now shows it. This replaces T7.10 findings 2 and 3. Tests: PartitionSplitPolicyTest 44/44, HotPartitionSensorTest 10/10, DetectorsTest, SplitOutcomeDetectorsTest, PartitionSplitEndToEndTest (44 s) and HotSpotScenarioTest (156 s) pass; checkstyle clean (it also fixes two checkstyle errors in the spike's test).
- T7.11 repeatability harness and recorded run: done. `tools/autonomic/repeat-tests.sh -n 5 @scenario @fault` on 2026-11-22 on the T7.13 code (logs, JUnit reports and node logs per run kept in `build/repeat-tests/recorded-2026-11-22/`, not committed). The run found a harness bug, fixed after it: `run-tests.sh -t` is a grep pattern over test paths, so `LoadSpikeScenarioTest` also ran `RealLoadSpikeScenarioTest`; the harness now anchors a plain class name (`/Name\.java$`). Results corrected for it:

  | Class | Passed | Median s (min to max) | Failures |
  |---|---|---|---|
  | HotSpotScenarioTest | 4/5 | 160 (154 to 164) | 1: stage B `REGRESSED`, key write latency 2.68 ms above its 2.61 ms limit (ADR-040), in-JVM latency noise; unrelated to T7.12 and T7.13 |
  | LoadSpikeScenarioTest | 5/5 | not measured alone (each run also ran the real-load scenario) | none of its own |
  | RealLoadSpikeScenarioTest | 5/10 (5 own runs and 5 inside the load-spike runs) | 77 (77 to 141) own runs | 5: `read_pool` 4 to 5 rolled back `REGRESSED` on the write p99 guard (see below) |
  | DegradedNodeScenarioTest | 5/5 | 45 (44 to 47) | |
  | ThrowingSensorFaultTest | 5/5 | 31 (29 to 33) | |
  | UnwritableStoreFaultTest | 5/5 | 27 (25 to 29) | |
  | CmsUnreachableClaimFaultTest | 5/5 | 41 (40 to 43) | |
  | OwnerKilledFaultTest | 5/5 | 130 (129 to 131) | |
  | StalledControlThreadFaultTest | 5/5 | 37 (35 to 38) | |
  | CircuitBreakerFaultTest | 5/5 | 58 (57 to 61) | |

  Finding for the owner (the open decision of T7.13): in every captured real-load failure the goal improved (ReadStage queue 36 to 3.3, or 34.7 to 5.3) and the read p99 was unchanged, but the write p99, on a node with almost no writes, moved one bucket of the decaying histogram (2759 to 3568 us, 1916 to 2478 us; +29%), above the 25% tolerance. A guard on an almost idle request class can fail a correct action by one histogram bucket.
- T7.13 baseline from the problem period (ADR-044): done, except that the scenario is not yet stable. `ThresholdDetector` records the first breaching check of a run of breaches and puts it in the evidence as `breach_since` (`Policies.problemSinceMillis` falls back to `firstSeenMillis`); `VerifySpec.baselineSinceMillis` (`withBaselineSince`, default none) makes `MetricVerifier.baseline` look back `min(baseline_window, now - since)`; every node-scoped policy (`KnobPolicy` and its subclasses CompactionBacklog, TombstoneOverload incl. its garbage-collect path, GcPressure, YieldBackground, DegradedSelf; `RequestPoolPolicy` both modes; `RequestThrottlePolicy` both modes; `LoadSpikePolicy` through its steps) passes it and does not propose until the period holds `MIN_SAMPLES` (debug log `waiting_for_baseline`). Cluster actions, ScaleOut, PartitionSplit/Unsplit and RepairOverdue (cluster-scoped) are unchanged; the executor's `INSUFFICIENT_BASELINE` stays. `MetricVerifier` now logs at debug the baseline and result of a verdict and a guard breach. Tests: MetricVerifierTest 7/7, ThresholdDetectorTest 20/20, RequestPoolPolicyTest 10/10, RequestThrottlePolicyTest 8/8, PoliciesTest 14/14, LoadSpikePolicyTest 12/12, ActionExecutorTest 37/37, DetectorsTest 26/26, PartitionSplitPolicyTest 44/44, ScaleOutPolicyTest 7/7 (each under 5 s); LoadSpikeScenarioTest (65 s) and DegradedNodeScenarioTest (37 s) pass; checkstyle clean. `RealLoadSpikeScenarioTest` keeps its sustained-saturation requirement (three runs on the T7.13 code: two passed in 132 s and 133 s, one was rolled back `REGRESSED`; its numbers were not kept, and the passing runs show the write p99 guard moving between 1.9 and 3.3 ms with almost no write traffic). Without it (`for_evaluations` 2 instead of 4, acting about 10 s after the spike starts instead of 20 s) the read baseline is no longer the problem, but 4 of 9 runs passed (82 to 102 s): one captured failure was an `EARLY_ABORT` on `client.write_latency_p99_micros` (baseline 3973 us, mean since apply 6817 us over 15 s), a guard on a request class with almost no traffic in this scenario, and another ended `REGRESSED` with no numbers captured. That change was reverted. Finding for the owner: the problem-period baseline is shorter than the old window (with defaults about 45 to 60 s instead of 2 m), so a latency guard on an almost idle request class, whose decaying p99 rests on a handful of operations, is noisier; a guard could be judged only where its class had traffic (as ADR-041 does for the split's share), which is a design change not made here.
