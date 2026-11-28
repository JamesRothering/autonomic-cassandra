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

*Part 1 (below) covers the cache and memory near-miss. Part 2, added after the coverage audit, covers the wider failure of method it exposed.*

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

## Part 2: the wider failure (added 2026-11-23, after the audit)

The audit (`docs/audits/observability-coverage.md`, `5fc8f27`) showed that the cache gap was one case of a general failure: 23 gaps where a planned or existing action or diagnosis lacks its input, 18 where the layer cannot name a cause, and four wrong assumptions about how Cassandra and the JVM behave. Among them is a live defect on JDK 21: `gc.time_fraction` adds the time of every collector (`GcSensor.java:75`), and Cassandra's JDK 21 options select generational ZGC (`conf/jvm21-server.options:31`), whose concurrent cycles do not stop requests (measured: 3330 ms of cycles against 2 ms of pauses). `gc_pressure` can fire with no pause pressure and shrink caches.

Part 1's root causes explain how individual items slipped through. They do not explain why the method let so many through. This part does.

### The brief

The owner's initial prompt (2026-11-12, kept at `~/autonomic-cassandra-original-prompt.txt`) asked for "a fork of Cassandra which re-envisions the database **from first principles**, as a self-healing, self-correcting, self-diagnosing database". The same prompt asked the orchestrator to "complete the entire specification with **maximum quality** while minimizing **unnecessary** token usage". The phrase "first principles" appears in the README (line 25) and nowhere in the design method.

### What first principles would have required

Starting from first principles means starting from what can go wrong with a database node and a cluster, and deriving from that what must be observed, what must be diagnosed and what may be done:

1. A catalogue of failure modes and degradations: resource exhaustion (CPU, each memory tier, disk space and I/O, network, file descriptors), internal back-pressure (stages, commit log, memtables, compaction, hints, streaming, messaging), JVM behaviour (pauses by collector, allocation stalls), data shape (hot and large partitions, tombstones), peers and topology, and the layer's own faults.
2. For each failure mode: the observations that detect it, the ones that tell it apart from look-alikes (a GC pause from a load spike from a cache miss), and the physical facts it depends on (which memory is on-heap, what a counter actually measures).
3. Only then: which existing Cassandra metrics and setters provide those, and where new sensors are needed.

### What was done instead

The design was built **bottom-up from what the base already offered**:

- The design's first substantive section is "What the base already provides (verified)" (overview section 2). The knob table (8.1) lists the runtime setters that exist. T2.5 is "Sensors over existing metrics". Observation was scoped by what was cheap to wrap, and actions by which setters existed.
- No failure-mode catalogue exists anywhere in the project (searched: overview, adaptive-schema, deliverables, ADRs).
- The 13 questions put to the owner at the D1 gate (D1-architecture.md, "Questions put to the project owner") were about safety, compatibility and scope of actions. None asked which failure modes the layer must handle, or what it may leave unobserved. Observation scope was decided silently.
- The design's evidence rule ("Verified means the file was read at the pinned commit") verified that paths and setters **exist**, not what they **mean**. That is how GC time was taken for pause time, the load average for CPU saturation, and the row cache for on-heap memory.
- The design's goals and non-goals (overview 1.1, 1.2) contain no completeness goal for observation. Goal 6, "Cheap rebases: core is touched only at the hook points", reasonably limited changes to Cassandra, but it also pulled the design toward metrics already exposed.

### Root causes of the wider failure

| # | Root cause | Evidence |
|---|---|---|
| W1 | **The design method was bottom-up, not first principles.** It started from available metrics and setters, so anything Cassandra did not already expose conveniently was never considered. | Overview section 2 and 8.1, T2.5; no failure-mode catalogue anywhere |
| W2 | **Cost pressure displaced verification, and the orchestrator chose the cut.** The brief said "minimizing *unnecessary* token usage"; on 2026-11-14 the owner asked to minimise Fable spend. The orchestrator met both by narrowing reviews (workers test and commit, the orchestrator reads "the key diff") and by keeping sensor scope to cheap wrappers. Coverage analysis was necessary work, not unnecessary, and was never weighed explicitly or put to the owner as a trade-off. | Working notes of 2026-11-14; review notes of every deliverable read diffs, none reconciles the design |
| W3 | **Completeness was never an acceptance criterion.** Task criteria were local ("emits the expected signal", "returns the expected action"). Deliverable criteria (D3: diagnoses exist; D4: actions exist) defined capability, not coverage. A deliverable could pass every criterion while missing whole resource classes. | Overview section 13; D3 to D7 acceptance criteria |
| W4 | **Scope by omission.** What the layer would not observe was never written down, so it was never decided. The owner was asked to rule on what the layer does, never on what it ignores. | D1 owner questions 1 to 13 |
| W5 | **One mind designed, reviewed and accepted.** The orchestrator wrote the design, "spot-checked" it (D1 review notes), briefed the workers from it and reviewed their output against it. Workers were checked against briefs that inherited the design's gaps. No independent reviewer ever asked what was missing. | D1 review notes; worker briefs |
| W6 | **Tests confirmed, they did not falsify.** Policies are tested with injected signals, so a policy whose real input does not exist still passes. Only one scenario (T7.9) used real load. Nothing ran on JDK 21, the configuration where the GC defect lives; the toolchain chosen in ADR-003 is JDK 17 only. | Test suites; ADR-003; `~/.local/opt` has no JDK 21 |
| W7 | **Closure pressure.** The most visible single error (ADR-046) was written while closing D7, reasoning from belief to finish quickly. | Part 1 timeline |

### Five whys on the JDK 21 GC defect

1. Why can `gc_pressure` fire falsely? `GcSensor` adds the collection time of every collector bean.
2. Why does that matter? On ZGC some beans report concurrent cycles, not pauses.
3. Why was that not known? The design verified that `GarbageCollectorMXBean` exists, not what each bean measures under each collector.
4. Why was it not caught by tests? Tests inject `gc.time_fraction`, and the build and tests run on JDK 17 with G1 only.
5. Why did no review ask? No step asked "what does this number physically mean, and on which configurations?" (W1, W5, W6).

### Additional recommendations

R1 to R7 stand. Add:

| # | Recommendation | Addresses |
|---|---|---|
| R8 | **Failure-mode catalogue first.** Before more features, write `docs/architecture/failure-modes.md`: every failure mode and degradation of a node and a cluster, the observations that detect it and that tell it apart from look-alikes, the physical facts it relies on, and the action (or explicit "diagnose only" or "out of scope", decided by the owner). The coverage audit (R1) becomes its observation column. Every future sensor, detector and policy cites a row. | W1, W3, W4 |
| R9 | **Cost cuts are the owner's call, never verification.** The orchestrator may save tokens on execution (model tier, reuse of workers) but never by skipping coverage analysis, independent review or semantic checks. Where quality and cost conflict, it says so and asks. | W2 |
| R10 | **Completeness criteria at every level.** Each deliverable's acceptance criteria include coverage: the failure-mode rows it closes, and a design-versus-built reconciliation with no unrecorded gap. | W3 |
| R11 | **Independent review at every gate.** A reviewer that wrote neither the design nor the code checks the catalogue rows, the reconciliation and a sample of semantic claims before the owner is asked to approve. | W5 |
| R12 | **Run the supported configurations.** Add JDK 21 (ZGC) to the toolchain and run the scenario and fault suites on it, since it is a supported runtime with different GC behaviour. | W6 |
| R13 | **A standing orchestration prompt.** The rules above are written as an instruction the orchestrator loads every session (`docs/process/orchestration-prompt.md`), so they do not depend on memory. | All |

## Proposed order of work (revised 2026-11-23)

1. The owner reviews this part, the prompt and the estimate.
2. R8, the failure-mode catalogue, built from the audit, with the owner deciding each row's action or out-of-scope status. It replaces the audit's task list as the source of D7.1's scope.
3. T7.17 (pause-only GC signal) early: a live defect on JDK 21. R12 alongside it.
4. The remaining D7.1 tasks derived from the catalogue, each with an independent review (R11), then R3 and R10 at the D7.1 gate.
