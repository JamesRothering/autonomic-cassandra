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


# Assessment: sprint 1 (D0 to D7 and the close-out)

*Revised 2026-11-24 to cover the whole sprint, 12 to 23 November 2026. The first version, written on 2026-11-19 before D7, covered D0 to D6 only and is in the history of this file (`git show a98c229:ASSESS.md`). Measurements are taken at commit `1132535` on `deliverable/D7-verification`; the D7 tag on `main` is `f12f9cc`. The figures match the owner's sprint report.*

This document covers what the project set out to do, how much it produced, what the same work would have cost a conventional engineering team, what it actually cost in model tokens, what was built, and where it falls short of the brief. Section 8, on the shortcomings, changes how the earlier sections should be read.

## 1. How it started

The project began on 2026-11-12 with one message from the owner. It had two parts: the vision, and how the work should be run.

The vision:

> This project is to create a fork of Cassandra which re-envisions the database from first principles, as a self-healing, self-correcting, self-diagnosing database. If demand spikes, it adapts to meet that elastically. If a hot spot develops, it changes the schema on the fly to mitigate the problem and still keep clients depending on the old schema satisfied -- at least eventually. Thats the basics of the vision, with a few examples. Structure the project with deliverables in the git structure. Track design decisions in a .md file something like "Architectural and design decisions log"

How the work should be run:

> Act as the lead orchestrator for this project.
>
> Review the entire specification and break it down into small, independently executable tasks with clear dependencies, acceptance criteria, and expected outcomes.
>
> Before starting any task, evaluate its complexity, reasoning requirements, risk, and implementation scope. Determine the most appropriate model for each change rather than defaulting everything to Fable 5.2.
>
> Use Haiku for simple, repetitive, and well-defined changes that require minimal reasoning.
>
> Use Sonnet for standard implementation, feature development, refactoring, test creation, and moderately complex debugging.
>
> Use Fable 5.2 for architectural decisions, difficult debugging, ambiguous requirements, complex reasoning, and changes with significant system-wide impact.
>
> Assign work to the most appropriate model wherever model-specific delegation is supported. Run independent tasks in parallel when appropriate, respect dependencies, and avoid conflicting changes.
>
> Maintain responsibility for coordination, integration, code review, and final verification. Escalate work to a stronger model only when complexity, risk, or failed attempts justify it.
>
> Before execution, show me the complete task breakdown, the model selected for each task, and a brief explanation of why that model is the best choice.
>
> Your goal is to complete the entire specification with maximum quality while minimizing unnecessary token usage, execution time, and cost. Never use Fable 5.2 for work that Sonnet or Haiku can reliably handle.

The owner later tightened the cost rules twice:

- **2026-11-14:** the orchestrator moved to a cheaper model for D3 to D5. Workers were to build, test, lint and commit their own work and send back short reports, and one warm worker was to be reused for related tasks instead of starting fresh ones.
- **2026-11-17:** Fable was dropped altogether, and Opus took over its tasks.

The owner acted as product owner and reviewer throughout. Every design decision is recorded as an ADR in [docs/DECISIONS.md](docs/DECISIONS.md).

One thing in the brief matters more than this assessment first recognised. It asked for a database re-envisioned **from first principles**, and for **maximum quality** while minimising **unnecessary** token usage. Section 8 shows that the design was not built from first principles, and that the token economy cut into work that was necessary.

## 2. What was produced

The fork point is upstream Apache Cassandra trunk `2249e37`, dated 2026-11-12. From there to `1132535` the work came to 160 commits.

| Area | Files | Lines added | Code only (no blank or comment lines) |
|---|---|---|---|
| Production Java (`src/java`) | 188 | 37,991 | **~27,300** in the 175 new files |
| Tests (`test/`) | 188 | 47,698 | **~37,100** in the 160 new files |
| TLA+ specifications and TLC model configurations | 3 + 24 | ~1,700 | — |
| Documentation (ADRs, deliverables, design, operations, spike, postmortem, audit, process) | 54 | 8,255 | — |

There is more test code than production code, about 1.35 lines of test for every line of production code: 140 test classes and 942 test methods. Almost everything is in new files under `org.apache.cassandra.autonomic`; about 20 existing upstream files were modified, so rebasing on newer upstream code stays cheap (ADR-011, ADR-022).

Lines added by period:

| Period | Production | Tests | Docs |
|---|---|---|---|
| D0 Foundation and D1 Architecture | 0 | 0 | 2,739 |
| D2 Framework | 12,566 | 9,811 | 73 |
| D3 Self-diagnosis | 1,857 | 1,406 | 28 |
| D4 Self-healing | 2,291 | 2,375 | 43 |
| D5 Elasticity | 2,912 | 4,145 | 30 |
| D6 Adaptive schema, including T6.17 (D6.1) | 16,788 | 24,408 | 3,245 |
| D7 Verification (T7.1 to T7.14) | 1,064 | 4,511 | 1,273 |
| Close-out (T7.15, the input check, postmortem, audit, prompt) | 755 | 1,118 | 675 |

It took **11 calendar days**, from the first message on 2026-11-12 at 20:19 UTC to the close-out on 2026-11-23. Across all sessions, the transcripts show about **56 hours of active time**, counting every period in which the sessions were working and the gaps between events were under 30 minutes. That is an upper bound on the owner's own time, because much of it was agents working on their own.

Verification at the end of D7 (5 runs per test): the six fault-injection tests passed 30 of 30, the degraded-node and both load-spike scenarios 5 of 5, and the hot-spot scenario 4 of 5 (one borderline latency verdict, still open).

## 3. What a team would have needed without LLMs

Lines of code are a rough proxy for effort, but they are the measure most estimation models use, so this section uses two of them and checks one against the other. The method is the same as in the first version; only the size has changed, from 26.1 to 27.3 KSLOC.

| Method | Result |
|---|---|
| COCOMO II on 27.3 KSLOC of production code, nominal settings | ~112 person-months |
| COCOMO II with the cost drivers adjusted for distributed-database internals (very high complexity, high reliability, data movement that must never lose a write) | ~170–220 person-months |
| Cross-check: 27,300 lines at 8–12 net lines per engineer-day (typical for Cassandra internals, including design, review and tests) | ~135–190 person-months |
| Additional work on top: TLA+ modelling by a formal-methods specialist, writing the ADRs, design reviews | +10–20 person-months |

**Estimate: about 155–230 person-months (13–19 person-years)**, for a team of **7–9 senior engineers** working for **18–24 months**.

This measures what was **built**, not what the brief **asked for**. Section 8 shows that part of the scope the code appears to cover lacks the inputs it needs. A conventional team's design reviews would probably have caught some of those gaps early, so the estimate is for a comparable amount of code, not for a finished product.

The estimate could also be off in the usual ways: LLM-written code and tests tend to be wordier than hand-written ones (the estimate may be too high), and a real team would spend time on review, rework and failures in real clusters that no line count shows (it may be too low).

## 4. Tokens and cost

These figures come from the Claude Code transcripts of every session in the project, orchestrators and workers, each API response counted once, up to the close-out on 2026-11-23.

| Model | Calls | Cache writes | Cache reads | Output | Cost at API list price |
|---|---|---|---|---|---|
| Opus 5.5 | 2,697 | 20.6 M | 601.2 M | 2.34 M | $280 |
| Fable 5.2 (until 2026-11-17) | 694 | 8.0 M | 121.6 M | 1.30 M | $170 |
| Sonnet 5.5 | 2,176 | 9.9 M | 368.3 M | 2.11 M | $84 |
| Haiku 5.5 | 585 | 1.3 M | 79.8 M | 0.41 M | ~$1 |
| **Total** | **6,152** | **39.8 M** | **1,170.9 M** | **6.16 M** | **~$535** |

- **About 1.2 billion tokens processed**, 96% of them cache reads.
- **By period:** about $441 up to D6.1, $67 for D7 and $42 for the close-out, including this revision.
- **List prices used** are per million tokens, as input / output / cache read: Fable 5.2 $8 / $40 / $0.20; Opus 5.5 $4 / $20 / $0.20; Sonnet 5.5 $2 / $10 / $0.10; Haiku 5.5 $0.10 / $0.50 / $0.01. Cache writes are priced at 1.25× the input price for the 5-minute cache and 2× for the 1-hour cache.
- **These are API-equivalent figures.** What was billed depends on the plan. Against the plan's limits, the sprint used about **three weekly usage budgets**: the weekly limit was reached on 15 November and again on 23 November, with one free reset between.

## 5. The productivity factor

| Basis | Conventional team | This sprint | Factor |
|---|---|---|---|
| Calendar time | 18–24 months | 11 days | **~50–65×** |
| Human hours | 155–230 person-months, about 23,000–35,000 hours | at most ~56 hours of active session time | **~400–600×** |
| Cash spend | ~$3.1–4.6 M, at ~$20k per fully loaded senior engineer-month | ~$535 in tokens, plus the owner's time | **~5,800–8,600×** |

**These factors overstate the result**, for three reasons:

1. They compare against a team building the same amount of code, but part of that code's apparent scope is not delivered (section 8).
2. They leave out the next increment, which the shortcomings require: about one weekly usage budget, roughly $170–300 at list prices, and one to two weeks of calendar time, most of it machine time for tests (see [docs/NEXT-STEPS.md](docs/NEXT-STEPS.md)).
3. They leave out the owner's review effort, which caught the main shortcoming.

Counting the next increment, the calendar factor falls to roughly **20–40×**. The cost factor stays above three orders of magnitude either way. The calendar factor is still the most conservative and the easiest to defend.

## 6. Revision: the token restrictions

The project ran under deliberate token economy: cheaper models wherever possible, no Fable after 2026-11-17, a cheaper orchestrator for D3 to D5, short capped worker reports, warm workers reused, and the orchestrator reading only the key part of each diff.

The first version of this assessment estimated that the restrictions saved about $700–1,500 and cost a day or two of calendar time. The close-out shows a cost that estimate missed: the restrictions narrowed reviews and sensor scope, and no one weighed that against quality or put it to the owner (postmortem root cause W2). Part of the next increment, about $170–300 and one to two weeks, is the price of that. The standing prompt now forbids saving tokens on verification: savings come only from how work is executed.

| | Restricted, as run | Unrestricted, estimated |
|---|---|---|
| Token cost of sprint 1 | ~$535 | ~$1,300–2,300 (1.7× for top-tier pricing, 1.5–2.5× for volume) |
| Rework the restrictions caused | ~$170–300 and 1–2 weeks (increment 2) | likely much less, though not zero |
| Calendar time | 11 days | ~8–10 days |

On these numbers the restrictions saved money on the sprint itself, but most of that saving is spent again in increment 2, and the calendar time is lost.

## 7. What was built

- **An in-database autonomic control loop.** Each node runs its own MAPE-K loop (monitor, analyze, plan, execute, over shared knowledge) on three dedicated threads. It reads Cassandra's existing metrics and adds no instrumentation to the request path, which is also the root of its main shortcoming (section 8).
- **Safety guardrails.** Dry-run mode, a cluster-wide kill switch, rate and blast-radius limits, cooldowns, a circuit breaker and operator approval for risky actions. An action is kept only if verification shows an improvement without breaking a guard; otherwise it is rolled back (ADR-008). Guards are not judged on request classes with too little traffic to measure (ADR-045).
- **Self-diagnosis.** Detectors for hot partitions, latency SLO breaches, pool saturation, tombstone overload, compaction backlog, disk pressure, GC pressure, node imbalance, degraded peers, overdue repair and, since T7.15, cache misses. A rules-based correlator links them to root causes in language an operator can act on.
- **Self-healing.** Actuators tune compaction throughput and concurrency, shrink caches under GC pressure, and run tombstone-driven compaction, each in an apply, verify and roll-back loop. Growing caches was designed but not wired. The fork drives Cassandra's built-in auto-repair rather than adding a second repair scheduler.
- **Elasticity.** Under a load spike a node first resizes its thread pools and applies adaptive throttling. Beyond that, a scale-out provider interface adds nodes, only with manual approval, with a simulated provider and a Kubernetes stub.
- **On-the-fly schema adaptation.** A hot wide partition is hash-split into a companion table while clients of the old schema keep getting the same results: coordinators rewrite queries at the `StorageProxy` boundary, a migration engine runs dual-write, backfill, verification, cutover and rollback through all-node epoch acknowledgement, and T6.17 undoes a split once the hot spot cools. This was the hardest item in the vision.
- **Formally modelled.** Three TLA+ specifications, checked by 24 TLC configurations, including deliberate negative tests that remove one safety rule and show that the checker finds the violation.
- **Operable, with caveats.** `nodetool autonomic` has 23 subcommands, the `cassandra.yaml` additions are documented, and an operator guide covers configuration, the kill switch, rollback and the split demo. The operator cannot yet see much of what the layer cannot see (section 8).
- **Every decision written down.** 46 ADRs, two measurement spikes, a postmortem and a coverage audit.
- **Tested in depth, within limits.** About 37,100 lines of test code: unit, in-JVM distributed, end-to-end, failure-injection, compatibility and repeatability suites, all on one 4-core, 5 GB machine, on JDK 17 only.

## 8. Where it falls short of the brief

The owner found the main shortcoming on 2026-11-22, while D7's follow-up work was being closed out. The postmortem ([docs/postmortems/2026-11-22-cache-and-memory-near-miss.md](docs/postmortems/2026-11-22-cache-and-memory-near-miss.md)) and the coverage audit ([docs/audits/observability-coverage.md](docs/audits/observability-coverage.md)) set out the evidence.

- **Not from first principles.** The design started from the metrics and settings Cassandra already exposed (overview section 2, "What the base already provides"), not from a catalogue of what can go wrong. No such catalogue exists yet.
- **Blind spots.** The audit found 23 places where an existing or planned action or diagnosis lacks the signal it needs, and 18 where the layer cannot name a cause. Memory beyond heap use, real CPU use, disk I/O, messaging between nodes, the write path, hints and streaming are not observed.
- **Wrong assumptions about Cassandra.** GC time is taken as pause time, a live defect on JDK 21, where ZGC's concurrent cycles count as pressure. The off-heap row cache is shrunk to relieve the heap. The load average is taken as CPU use.
- **Planned but never built.** Growing caches, fewer compaction threads, a client in-flight-bytes knob, pools below their configured size, and recovery of throttled hints and streams.
- **Test limits.** In-JVM clusters of one to three nodes, JDK 17 only, no performance benchmarks, no real cluster. The hot-spot scenario fails about one run in five.
- **Why it happened** (postmortem part 2): a bottom-up method; cost pressure that displaced verification without the owner deciding it; no completeness criterion at any level; scope decided by omission; one agent designing, reviewing and accepting its own work; tests that fed made-up signals; and closure pressure.

What has already changed: a standing prompt that every session loads ([docs/process/orchestration-prompt.md](docs/process/orchestration-prompt.md)), an automated test that fails when an action lacks its input (`InputCoverageTest`), an evidence rule for every claimed limitation, and an independent review before anything ships. T7.15 was the first task under these rules; its independent review found two defects before it shipped.

## 9. What comes next

Increment 2 (D7.1) starts with a failure-mode catalogue that sets its scope, fixes the JDK 21 defect, adds the missing observation, decides the unbuilt design items, proves each resource class under real pressure, and closes with a full gate. It is estimated at about one weekly usage budget. The plan, with tiers, estimates and the owner's decisions per step, is [docs/NEXT-STEPS.md](docs/NEXT-STEPS.md).

## Bottom line

In 11 days and about $535, the sprint produced a working, formally modelled autonomic layer inside Cassandra, of a size a conventional team would need 18 to 24 months to write. It is not yet the database the brief described: it sees and acts on what Cassandra made easy to read, not on everything that can go wrong. The method that caused that has been changed, and the gap is measured and planned for.
