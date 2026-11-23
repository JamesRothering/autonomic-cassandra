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

# Next steps: increment 2 (D7.1)

Status: proposed, 2026-11-23, for the owner to prioritise. Built from the postmortem
[`docs/postmortems/2026-11-22-cache-and-memory-near-miss.md`](postmortems/2026-11-22-cache-and-memory-near-miss.md)
(recommendations R1 to R13) and the coverage audit
[`docs/audits/observability-coverage.md`](audits/observability-coverage.md). The rules every step follows are the
standing prompt [`docs/process/orchestration-prompt.md`](process/orchestration-prompt.md), loaded by `CLAUDE.md`.

## Where sprint 1 ended

- `main` and tag `D7` (`f12f9cc`): D0 to D7 approved and tagged. Known defect on `main`: `gc.time_fraction` counts ZGC's concurrent cycles as GC time on JDK 21 (audit; fixed by T7.17).
- `deliverable/D7-verification` carries the post-D7 work for D7.1: ADR-046, the postmortem, the audit, `InputCoverageTest` (R2), the standing prompt, and T7.15, done on 2026-11-23 (`0b911bf`, `994673d`): cache hit rates and `cache_miss_pressure`. It was the first task under the new rules: the full diff was read, regression tests were seen to fail on the old code, and an independent review found two defects (a noisy drop learned into the reference; a single-sample reference after a gap), both fixed before push.
- Done from the postmortem: R1 (audit), R2 (input check), R4 (evidence rule, in the prompt), R7 (ADR-046 corrected), R11 first used on T7.15, R13 (the prompt).

## The goal of increment 2

Close the gap between what the brief asked (a database re-envisioned from first principles, that knows rather than guesses) and what sprint 1 built: start from what can go wrong, make the layer see it, fix the wrong assumptions, and prove it under real pressure on both supported JDKs.

## Proposed order

Estimates are in the cost units of the sprint report (model tokens weighted by relative list price), sized from sprint 1's measured tasks of the same kind. One weekly budget is about 350 units (range 350 to 480; it rests on one week's data and one free reset, see the sprint report). "Review" is the independent review of R11.

| # | Step | From | Tier | Units | Owner decision needed |
|---|---|---|---|---|---|
| 1 | **Failure-mode catalogue** `docs/architecture/failure-modes.md`: every failure mode of a node and a cluster, what detects it, what tells it from look-alikes, the physical facts it relies on, its action or "diagnose only" or "out of scope". Built from the audit. Becomes the source of D7.1's scope | R8 | opus | 40–70 | Yes: each row's action or out-of-scope status, in one review session |
| 2 | **T7.17 pause-only GC signal**: count only stop-the-world time per collector, so `gc_pressure` is right on ZGC | audit, five whys | sonnet | 3–5 | No (a defect) |
| 3 | **R12 JDK 21 toolchain** and the scenario and fault suites on JDK 21 with ZGC | R12 | sonnet | 5–10 | No |
| 4 | **T7.16 memory sensor and cache resizing**: every memory tier (heap pools, direct and mapped buffers, Cassandra off-heap, process, OS, container); grow caches within measured headroom, shrink only on-heap caches for heap pressure (fixes postmortem finding 3) | audit, R5 | opus | 60–90 | ADR-047 approval |
| 5 | **T7.18 CPU sensor**: utilisation, I/O wait, steal, cgroup throttling; pools judged on CPU, not load average | audit | sonnet | 6–12 | No |
| 6 | **T7.19 disk I/O sensor**: throughput, latency, queue depth and utilisation per data and commit-log device | audit | sonnet | 6–12 | No |
| 7 | **T7.20 inputs already collected**: client timeouts, unavailables and failures; per-table compaction data | audit | sonnet | 6–12 | No |
| 8 | **T7.21 write path and messaging**: commit-log waits, memtable allocation and flushing, dropped messages, internode latency | audit | sonnet | 6–12 | No |
| 9 | **T7.22 hints, streaming, client connections** | audit | sonnet | 6–12 | No |
| 10 | **T7.23 small inputs**: file descriptors, disk failures, metadata lag, snapshots, read repair | audit | haiku | 2–4 | No |
| 11 | **T7.24 the unbuilt design items**: fewer compaction threads, the in-flight-bytes knob, pools below configured size, recovery of throttled knobs and of the advertised severity, `DegradedSelfPolicy` | audit RC items | sonnet | 6–12 | Yes: build or drop each |
| 12 | **T7.25 real-pressure scenarios**: one per resource class the layer acts on (memory, cache misses, CPU, disk I/O), on a real node | R6 | sonnet | 30–45 | No |
| 13 | **T7.26 gate checklist and D7.1 gate**: design-versus-built reconciliation (R3), coverage in acceptance criteria (R10), independent review (R11), repeatability runs on both JDKs | R3, R10, R11 | haiku + opus | 30–60 | D7.1 approval |
| | Orchestration with full diff reads (+20–25%) | | | 45–95 | |
| | **Total** | | | **about 260–460** | |

**About one weekly budget (75% to 130%), most likely about 100%.** Step 1 can add tasks; it comes first so that the scope is known before most of the budget is spent.

## A split that fits one week with margin

- **This week (about 60–70%):** steps 1 to 6, then the D7.1 checkpoint review of what is done.
- **Next week:** steps 7 to 13 and the D7.1 gate.

Machine time, not tokens, is the slower constraint: the real-pressure scenarios and the JDK 21 runs take hours each on the 4-core machine.

## Open items carried over

- Hot spot scenario: about one run in five fails on a borderline stage-B key write latency verdict (2.68 ms against a 2.61 ms limit, ADR-040); real traffic, not covered by T7.14. To be analysed in step 12.
- The free-reset timing behind the budget estimate is unconfirmed (likely about 19 November); `/usage` gives the plan's real figure.
- Out of scope proposed by the audit, for the owner to confirm in step 1: clock skew, JVM safepoints, network card and TCP counters, Accord, authentication and TLS metrics.
