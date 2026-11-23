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

# Postmortem: cache sizing and memory observability near-miss

- **Date of the near-miss:** 2026-11-22. **Written:** 2026-11-23.
- **Caught by:** the owner, before any of it shipped.
- **Impact:** none shipped. The D7 tag (`f12f9cc`) is unaffected, but a defect that predates it was found while investigating (finding 3).
- **Author:** the orchestrator (Claude), who made the error.

## Summary

While adding a cache hit-rate signal (ADR-046, T7.15), the orchestrator wrote that the layer should not resize caches because "the right size depends on memory the layer does not own", and told the owner the same. The owner challenged it: the layer must observe everything down to the metal and act on what it knows.

The investigation shows the statement was not only an untested assumption, it was **contradicted by the project's own code and design**:

1. The D1 design already planned cache resizing: the knob table (overview 8.1) lists key, row and counter cache capacity with the direction "up on low hit rate with heap headroom; down under GC pressure".
2. D4 built the actuator for it: `CacheCapacityActuator` (T4.2, `0ed5315`, 2026-11-14).
3. Only the "down" half was ever wired: `GcPressurePolicy` (T4.3) shrinks caches under GC pressure. The "up on low hit rate" half was silently dropped, because no sensor ever measured a hit rate or the memory headroom.

So the gap was not in caches alone. **The layer is nearly blind to memory**: its only memory signal is `gc.heap_used_fraction`. That blindness was in the D1 design, survived every deliverable gate through D7, and produced the wrong statement in ADR-046.

## Timeline

| Date | Event |
|---|---|
| 2026-11-13 | D1 design (`2e79542`). The knob table plans cache capacity up and down. The same section says "Memtable, heap, and GC settings are not runtime-tunable and are out of scope", which concerns actuation only, but no memory observation beyond GC was planned either. |
| 2026-11-13 | T2.5 sensors (`e4d64ee`): table, client request, thread pool, compaction, disk, GC, peers, hot partition. No cache sensor and no memory sensor beyond heap used. The task's acceptance criterion was per sensor ("emits the expected signal"), not "every planned action has its inputs". |
| 2026-11-14 | T4.2 builds `CacheCapacityActuator` (`0ed5315`). T4.3 (`89fe5c4`) wires only `gc_pressure` to it, direction DOWN, row cache first. Nothing records that the planned UP direction was dropped. |
| 2026-11-14 to 22 | D4 to D7 reviews and gates pass. Each reviewed the diffs of its tasks; none compared the design's planned actions with what was wired. |
| 2026-11-22 | D7 approved, merged and tagged. The owner asks whether the layer can tell GC pauses and cache misses from load; the orchestrator finds no cache signal and offers one. |
| 2026-11-22 | ADR-046 written. Its alternatives say a cache-growing policy "was left out: the right size depends on memory the layer does not own". The orchestrator told the owner the same, without searching the code. |
| 2026-11-22 | The owner rejects the reason. The orchestrator then states correctly that Cassandra can resize the caches at runtime, but still does not notice that the project already has the actuator. |
| 2026-11-23 | This investigation finds the design entry, the actuator, the orphaned UP direction, and a second defect (finding 3). |

## Findings

1. **Unverified limitation stated as fact (ADR-046, and to the owner).** The claim was reasoned from plausibility, not checked against the code. A single search (`grep -rn CacheCapacity src/java/org/apache/cassandra/autonomic`) would have found the actuator.
2. **Orphaned design intent.** The cache knob's UP direction (design 8.1) has had no trigger since D1, because its inputs (hit rate, memory headroom) were never built. Its loss was never recorded as a decision.
3. **Defect: the GC policy shrinks an off-heap cache to relieve the heap.** `GcPressurePolicy` (javadoc: "shrink an on-heap cache by one step, the row cache first") steps the row cache down first. The default row cache, `OHCProvider` (`Config.row_cache_class_name`), lives off the heap, so shrinking it cannot relieve heap pressure. It costs hit rate, the verification finds no GC improvement, the step is rolled back, and the rollback counts against the action in the safety governor (`DefaultSafetyGovernor`: a cooldown and a consecutive-rollback count). Limited impact: the row cache is 0 MiB by default and is then not managed, so this affects only clusters that enable it. Same root cause as finding 1: properties of memory were assumed, not checked.
4. **Memory blindness.** Not observed today: individual heap pools; direct and mapped buffers; Cassandra's off-heap memory (memtables, chunk cache, row cache, bloom filters, index summaries, compression metadata); the process's resident memory; the machine's available memory, page cache and swap; the container's memory limit and usage.
5. **The gap is probably wider than memory.** A first pass over the signal list (`Signals.java`) finds other areas a node depends on that the layer does not see. These are candidates for the audit in R1, not yet confirmed:
   - CPU: only the 1-minute load average (`os.load_per_processor`), which on Linux mixes CPU demand with processes waiting on I/O; no utilisation, I/O wait or steal time.
   - Disk: only space (`disk.used_fraction`, `free_bytes`, `total_bytes`); no I/O throughput, latency, queue depth or utilisation.
   - Network and internode messaging: nothing; no dropped messages, internode latency or outbound queue backlog.
   - Commit log (pending syncs, waits for segments), memtable allocation blocking, hints in flight, streaming, open file descriptors against their limit, clock skew.

## Root causes

1. **No traceability from planned actions to the inputs they need.** The design listed what each knob should respond to, but no task, test or gate checked that a sensor produced each of those inputs. Tasks were accepted on local criteria ("the sensor emits its signal", "the policy returns the expected action").
2. **Observability was scoped by what was easy to read, not by what affects a node.** T2.5 wrapped the metrics that Cassandra already exposed in a convenient form. There was never a step that listed the resources a node depends on (CPU, memory tiers, disk I/O, network, internal queues) and checked each against the sensors.
3. **Gates reviewed diffs, not coverage.** Every review from D2 to D7 asked "is this change correct?" None asked "what did the design plan that is still missing?". The D7 close-out had no design-to-build reconciliation.
4. **The orchestrator wrote design claims from belief.** ADR alternatives and explanations to the owner stated limitations without citing code. With the orchestrator also reviewing its own ADRs, nothing stood between the belief and the record except the owner.

Contributing: tests drive policies with injected signals, so a policy whose real input does not exist still passes, and no scenario puts a real node under memory or cache pressure.

## Recommendations

| # | Recommendation | Prevents | When |
|---|---|---|---|
| R1 | **Observability coverage audit.** List every resource and internal queue a Cassandra node depends on (CPU, each memory tier, disk I/O, network, internode messaging, commit log, memtables, caches, hints, streaming, file descriptors, clock). For each, record what the layer observes (with `Signals.java` references), what Cassandra or the OS exposes, and the gap. Each gap becomes a sensor task or an explicit out-of-scope decision by the owner. | Root causes 2 and 3; finding 5 | First, before T7.15 resumes; it sizes D7.1 |
| R2 | **Automated input check.** A unit test that fails when a policy, detector or guard reads a signal that no registered sensor emits, and when an actuator is enabled but no policy uses one of its directions without a recorded reason. | Root cause 1; finding 2 | With R1 |
| R3 | **Design-to-build reconciliation at every gate.** Before a deliverable closes, walk the design's tables (knobs and their directions, sensors, detectors, policies) against the code and list anything planned but not built, each with a decision. | Root cause 3 | From D7.1 on |
| R4 | **Evidence rule for limitations.** Any "cannot", "not owned", "out of scope" or "not available" in an ADR or in an explanation to the owner cites code (file:line) or is marked as an assumption. Before writing an ADR's alternatives, search the code for an existing implementation. | Root cause 4; finding 1 | Now (recorded in the orchestrator's working notes) |
| R5 | **Check physical facts in tests.** Where a policy depends on a property of Cassandra (on-heap or off-heap, resizable at runtime), assert it against the real class in a unit test, not only in a comment. Fix finding 3 by choosing caches by where they actually live. | Finding 3 | With T7.16 |
| R6 | **A real-resource scenario per class.** For each resource class a policy acts on, at least one dtest with real pressure (memory, cache misses, disk I/O), not injected signals, as T7.9 did for the read pool. | Contributing cause | Scoped by R1 |
| R7 | **Correct ADR-046** (the wrong alternative) and record the owner's acceptance of the first hour without a reference level after a restart. | Finding 1 | When T7.15 resumes |

## What went well

- The owner's challenge caught it before anything shipped.
- The design had recorded the intent (knob table 8.1), which made the gap traceable.
- The verification loop limits the cost of finding 3: an ineffective cache step is rolled back, not kept.

## Proposed order of work

1. R1 audit, with R2's automated check, reported to the owner with the gaps and a proposed scope for D7.1.
2. R7 and the review of T7.15's uncommitted partial work, then T7.15.
3. ADR-047 and T7.16: the memory sensor, cache resizing in both directions with measured headroom, and the fix for finding 3 (R5).
4. R3 reconciliation and the R6 scenarios agreed in step 1, then D7.1 for owner review.
