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


# Assessment: sprint 1 (D0 to D7)

*Sprint 1 ran from 12 November 2026 to the D7 tag on Sunday 22 November (`f12f9cc` on `main`). All measurements stop there. The work after the tag (assessment, postmortem, audit and loose ends) is not counted, except in section 5.2, which prices the cost of autonomy. The figures match the owner's sprint report.*

This document covers what the project set out to do, how much it produced, what the same work would have cost a conventional engineering team, what it actually cost in model tokens, what was built, and where it falls short of the brief. Section 5 gives the productivity factors as if nothing went wrong, then prices what running without supervision cost.

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

The fork point is upstream Apache Cassandra trunk `2249e37`, dated 2026-11-12. From there to the D7 tag the work came to 151 commits.

| Area | Files | Lines added | Code only (no blank or comment lines) |
|---|---|---|---|
| Production Java (`src/java`) | 186 | 37,245 | **~26,700** in the 173 new files |
| Tests (`test/`) | 184 | 46,583 | **~36,200** in the 156 new files |
| TLA+ specifications and TLC model configurations | 3 + 24 | ~1,700 | — |
| Documentation (ADRs, deliverables, design, operations, spikes) | 47 | 7,314 | — |

There is more test code than production code, about 1.35 lines of test for every line of production code: 138 test classes and 942 test methods. Almost everything is in new files under `org.apache.cassandra.autonomic`; about 20 existing upstream files were modified, so rebasing on newer upstream code stays cheap (ADR-011, ADR-022).

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

It took **10 days**, from the first message on 2026-11-12 at 20:19 UTC to the D7 tag on 2026-11-22 at 23:11 UTC. Across all sessions, the transcripts show about **57 hours of active time**, counting every period in which the sessions were working and the gaps between events were under 30 minutes. That is an upper bound on the owner's own time, because much of it was agents working on their own.

Verification at the end of D7 (5 runs per test): the six fault-injection tests passed 30 of 30, the degraded-node and both load-spike scenarios 5 of 5, and the hot-spot scenario 4 of 5 (one borderline latency verdict, still open).

## 3. What a team would have needed without LLMs

Lines of code are a rough proxy for effort, but they are the measure most estimation models use, so this section uses two of them and checks one against the other, on 26.7 KSLOC.

| Method | Result |
|---|---|
| COCOMO II on 26.7 KSLOC of production code, nominal settings | ~109 person-months |
| COCOMO II with the cost drivers adjusted for distributed-database internals (very high complexity, high reliability, data movement that must never lose a write) | ~165–215 person-months |
| Cross-check: 26,700 lines at 8–12 net lines per engineer-day (typical for Cassandra internals, including design, review and tests) | ~130–185 person-months |
| Additional work on top: TLA+ modelling by a formal-methods specialist, writing the ADRs, design reviews | +10–20 person-months |

**Estimate: about 150–225 person-months (12–19 person-years)**, for a team of **7–9 senior engineers** working for **18–24 months**.

The estimate could also be off in the usual ways: LLM-written code and tests tend to be wordier than hand-written ones (the estimate may be too high), and a real team would spend time on review, rework and failures in real clusters that no line count shows (it may be too low).

## 4. Tokens and cost

These figures come from the Claude Code transcripts of every session in the project, orchestrators and workers, each API response counted once, up to the D7 tag. Prices are applied per response.

| Model | Calls | Cache writes | Cache reads | Output | Cost at API list price |
|---|---|---|---|---|---|
| Opus 5.5 | 2,510 | 19.4 M | 559.1 M | 2.15 M | $260 |
| Fable 5.2 (until 2026-11-17) | 694 | 8.0 M | 121.6 M | 1.30 M | $170 |
| Sonnet 5.5 | 2,132 | 9.6 M | 363.8 M | 2.07 M | $82 |
| Haiku 5.5 | 585 | 1.3 M | 79.8 M | 0.41 M | ~$1 |
| **Total** | **5,921** | **38.3 M** | **1,124.3 M** | **5.93 M** | **~$515** |

- **About 1.2 billion tokens processed**, 96% of them cache reads.
- **By period:** about $441 up to D6.1 and $74 for D7.
- **List prices used** are per million tokens, as input / output / cache read: Fable 5.2 $8 / $40 / $0.20; Opus 5.5 $4 / $20 / $0.20; Sonnet 5.5 $2 / $10 / $0.10; Haiku 5.5 $0.10 / $0.50 / $0.01. Cache writes are priced at 1.25× the input price for the 5-minute cache and 2× for the 1-hour cache.
- **These are API-equivalent figures.** What was billed depends on the plan. Against the plan's limits, the sprint used nearly **three weekly usage budgets**: the weekly limit was reached on 15 November and again early on 23 November, a few hours after the D7 tag, with one free reset between.

## 5. The productivity factor

### 5.1 Taken at face value

This is the experiment as it was run: the owner gave a general direction and trusted the result. It assumes nothing went wrong.

| Basis | Conventional team | This sprint | Factor |
|---|---|---|---|
| Calendar time | 18–24 months | 10 days | **~55–70×** |
| Human hours | 150–225 person-months, about 22,500–34,000 hours | at most ~57 hours of active session time | **~400–600×** |
| Cash spend | ~$3.0–4.5 M, at ~$20k per fully loaded senior engineer-month | ~$515 in tokens, plus the owner's time | **~5,800–8,800×** |

The calendar factor is the most conservative and the easiest to defend.

### 5.2 The cost of autonomy

The owner barely reviewed the design, so it could start from what Cassandra already exposes instead of from what can go wrong (section 8). Section 10 shows that a short review at the first gate would have caught this, had the clues been put plainly.

The cost of autonomy is the **rework**: work that was done wrong and has to be found and redone. Two other kinds of work are **not** counted:
- work the sprint never attempted, which is most of the 130 failure modes in the catalogue;
- design items that were planned but never built.

Both are scope still to deliver. A supervised sprint would have spent the same budget, aimed differently, and would not have delivered them either.

| Item | Cost |
|---|---|
| **Finding the problem** after the D7 tag: the coverage audit, both parts of the postmortem, `InputCoverageTest` and the standing prompt | ~$15–25 of the ~$28 spent between the tag and the close-out commit; the rest went on the cache signal (T7.15) and the sprint report |
| **Redoing the wrong parts** in increment 2: the GC signal on ZGC (T7.17), the CPU gate (part of T7.18), the cache shrink order (part of T7.16), the correlator fixes (T7.27), and checking the built code against the catalogue | ~30–60 units, ~$15–50 at $0.50–0.85 per unit |
| **Total** | **~$30–75, or 6–15% on top of the sprint**, plus about 1–3 days of calendar time |

With that cost included:

| Basis | Factor |
|---|---|
| Calendar time | **~40–65×** |
| Human hours | about **~400–600×**, roughly unchanged: the owner's time spent catching the problem is about what the first-gate review would have taken |
| Cash | **~5,100–8,300×** |

Running without supervision cost a few percent of the sprint, not an order of magnitude.

## 6. The token restrictions

The project ran under deliberate token economy:
- cheaper models wherever possible;
- no Fable after 2026-11-17;
- a cheaper orchestrator for D3 to D5;
- short, capped worker reports;
- warm workers reused;
- the orchestrator reading only the key part of each diff.

| | Restricted, as run | Unrestricted, estimated |
|---|---|---|
| Token cost of sprint 1 | ~$515 | ~$1,300–2,200 (1.7× for top-tier pricing, 1.5–2.5× for volume) |
| Calendar time | 10 days | ~8–9 days |

The restrictions saved about $800–1,700 and cost a day or two. They also narrowed reviews and sensor scope, and no one weighed that against quality or put it to the owner (postmortem root cause W2). That is one of the causes priced in section 5.2. The standing prompt now forbids saving tokens on verification: savings come only from how work is executed.

## 7. What was built

- **An in-database autonomic control loop.** Each node runs its own MAPE-K loop (monitor, analyze, plan, execute, over shared knowledge) on three dedicated threads. It reads Cassandra's existing metrics and adds no instrumentation to the request path, which is also the root of its main shortcoming (section 8).
- **Safety guardrails.** Dry-run mode, a cluster-wide kill switch, rate and blast-radius limits, cooldowns, a circuit breaker and operator approval for risky actions. An action is kept only if verification shows an improvement without breaking a guard; otherwise it is rolled back (ADR-008). Guards are not judged on request classes with too little traffic to measure (ADR-045).
- **Self-diagnosis.** Detectors for hot partitions, latency SLO breaches, pool saturation, tombstone overload, compaction backlog, disk pressure, GC pressure, node imbalance, degraded peers, and overdue repair. A cache-miss detector (T7.15) was added after the sprint. A rules-based correlator links them to root causes in language an operator can act on.
- **Self-healing.** Actuators tune compaction throughput and concurrency, shrink caches under GC pressure, and run tombstone-driven compaction, each in an apply, verify and roll-back loop. Growing caches was designed but not wired. The fork drives Cassandra's built-in auto-repair rather than adding a second repair scheduler.
- **Elasticity.** Under a load spike a node first resizes its thread pools and applies adaptive throttling. Beyond that, a scale-out provider interface adds nodes, only with manual approval, with a simulated provider and a Kubernetes stub.
- **On-the-fly schema adaptation.** A hot wide partition is hash-split into a companion table while clients of the old schema keep getting the same results: coordinators rewrite queries at the `StorageProxy` boundary, a migration engine runs dual-write, backfill, verification, cutover and rollback through all-node epoch acknowledgement, and T6.17 undoes a split once the hot spot cools. This was the hardest item in the vision.
- **Formally modelled.** Three TLA+ specifications, checked by 24 TLC configurations, including deliberate negative tests that remove one safety rule and show that the checker finds the violation.
- **Operable, with caveats.** `nodetool autonomic` has 23 subcommands, the `cassandra.yaml` additions are documented, and an operator guide covers configuration, the kill switch, rollback and the split demo. The operator cannot yet see much of what the layer cannot see (section 8).
- **Every decision written down.** 46 ADRs and two measurement spikes.
- **Tested in depth, within limits.** About 36,200 lines of test code: unit, in-JVM distributed, end-to-end, failure-injection, compatibility and repeatability suites, all on one 4-core, 5 GB machine, on JDK 17 only.

## 8. Where it falls short of the brief

The owner found the main shortcoming on 2026-11-22, while tying up loose ends at the end of the sprint. The postmortem ([docs/postmortems/2026-11-22-cache-and-memory-near-miss.md](docs/postmortems/2026-11-22-cache-and-memory-near-miss.md)) and the coverage audit ([docs/audits/observability-coverage.md](docs/audits/observability-coverage.md)) set out the evidence.

- **Not from first principles.** The design started from the metrics and settings Cassandra already exposed (overview section 2, "What the base already provides"), not from a catalogue of what can go wrong. The catalogue has since been drafted for increment 2 (`docs/architecture/failure-modes.md` on `deliverable/D7.1`, proposed).
- **Blind spots.** The audit found 23 places where an existing or planned action or diagnosis lacks the signal it needs, and 18 where the layer cannot name a cause. Memory beyond heap use, real CPU use, disk I/O, messaging between nodes, the write path, hints and streaming are not observed.
- **Wrong assumptions about Cassandra.** GC time is taken as pause time, a live defect on JDK 21, where ZGC's concurrent cycles count as pressure. The off-heap row cache is shrunk to relieve the heap. The load average is taken as CPU use.
- **Planned but never built.** Growing caches, fewer compaction threads, a client in-flight-bytes knob, pools below their configured size, and recovery of throttled hints and streams.
- **Test limits.** In-JVM clusters of one to three nodes, JDK 17 only, no performance benchmarks, no real cluster. The hot-spot scenario fails about one run in five.
- **Why it happened** (postmortem part 2): a bottom-up method; cost pressure that displaced verification without the owner deciding it; no completeness criterion at any level; scope decided by omission; one agent designing, reviewing and accepting its own work; tests that fed made-up signals; and closure pressure.

What has changed since: a standing prompt that every session loads ([docs/process/orchestration-prompt.md](docs/process/orchestration-prompt.md)), an automated test that fails when an action lacks its input (`InputCoverageTest`), an evidence rule for every claimed limitation, and an independent review before anything ships. T7.15 was the first task under these rules; its independent review found two defects before it shipped.

## 9. What comes next

Increment 2 (D7.1) starts with a failure-mode catalogue that sets its scope, fixes the JDK 21 defect, adds the missing observation, decides the unbuilt design items, proves each resource class under real pressure, and closes with a full gate. The catalogue puts it at about 380–690 units, or 1.1–2.0 weekly usage budgets, depending on 52 owner decisions. Most of that is new scope; section 5.2 prices the part that is rework. The plan, with tiers, estimates and the owner's decisions per step, is [docs/NEXT-STEPS.md](docs/NEXT-STEPS.md).

## 10. Could the owner have caught it at the first review?

The owner asked this after the sprint. The experiment was to give a general direction and let Claude run with it, so the first review point was not read closely. The question is whether enough was presented there to catch the failures of section 8 up front.

The first review point was the D1 gate on 13 November: the architecture overview, the adaptive-schema design, and 13 questions plus three scope calls (recorded in ADR-024). The evidence below is the D1 version of the overview (`git show 2e79542:docs/architecture/overview.md`; line numbers refer to that version) and the messages sent to the owner at the gate.

**Verdict: mostly not from what was shown, but yes from the spec the owner was pointed to.** The clues were in the 1,000-line overview, buried in assumptions and long prose. The chat summary and the questions steered attention away from them, and that is the orchestrator's failure, not the owner's. The owner has Cassandra and JVM expertise, so every clue below, put plainly as a question, would have been decided at the gate.

### What the owner was shown

The gate message summarised the hot-spot design and asked 13 questions and three scope calls. Eleven questions were about partition splitting, two about cluster-wide actions. None asked what the layer would watch or what could go wrong with a node. Self-healing was described in one line: node actions "run automatically, are checked, and are rolled back if they don't help." Nothing in the messages would have raised the problem.

### The clues in the spec

Visible without Cassandra knowledge:

1. **Actions depended on readings nothing supplied.** The knob table (section 8.1) planned to grow request pools "when CPU is not saturated" (line 699), grow caches "on low hit rate with heap headroom" (line 702), and lower in-flight request bytes "under heap pressure" (line 701). The table of what the layer reads (section 4.2, line 354) had no CPU reading, no cache hit rate and no heap headroom. Comparing the two tables would have shown three planned actions with no input, which is the cache gap the owner caught on 22 November.
2. **The method was stated in the headings.** Section 2 is "What the base already provides" (line 66), section 4.2 is "Core APIs read", and the sensor task T2.5 is "Sensors over existing metrics" (line 941). The overview had no list of what can go wrong with a node. The adaptive-schema design had one for its own mechanism ("Failure scenarios", its section 4.11); the self-healing design did not. The brief asked for first principles; the headings show the bottom-up method.
3. **Scope was set without asking.** "Memtable, heap, and GC settings are not runtime-tunable and are out of scope" (line 710): left out because no setter existed, not because the owner decided. The non-goal "anything outside the Cassandra process" (line 52) was about managing, but was later read as a reason not to observe the operating system.
4. **The tests could not catch a missing input.** Scenario tests were to use synthetic signals "without generating real load" (line 899), and performance benchmarks were out of the MVP (line 64). The tests checked logic, never that a reading existed under real pressure (postmortem R6).
5. **Nobody looked for blind spots.** The risk register (section 12, line 904) had 11 risks; none was "the layer cannot see X" or "a reading means something other than we think". "Verified" was defined as "the file was read" (line 31): existence, not meaning.

Visible with domain expertise, which the owner has:

6. **Cassandra.** The row cache was planned to shrink "under GC pressure" (line 702). The default row cache is off the heap, so shrinking it does not relieve garbage collection (postmortem finding 3).
7. **JVM.** GC time was to be read from `GarbageCollectorMXBean` "directly" (line 363), with nothing about pause time versus concurrent time, or about JDK 21 and ZGC. This is one line; stated plainly, it is a question the owner would have answered at once.
8. **Not visible at D1.** Judging CPU by the load average was designed in D5; D1 only said "CPU is not saturated" without a source.

### Which failures were catchable at D1

| Failure found later | Catchable at D1? |
|---|---|
| Blind spots: memory, CPU, disk I/O, write stalls and the rest of the audit | **Yes**, from clues 1 to 3, without domain knowledge |
| Caches could not grow (the owner's catch) | **Yes**, from clue 1 |
| Tests that cannot fail for a missing input | **Yes**, from clue 4 |
| Off-heap row cache shrunk for heap pressure | **Yes**, from clue 6, if put plainly |
| GC signal wrong on JDK 21 | **Yes**, from clue 7, if put plainly |
| CPU judged by the load average | No: designed later, in D5 |

### Conclusion

The information was there but buried: finding it meant reading the whole spec and comparing two tables about 345 lines apart, while the questions pointed at partition splitting. Put plainly, it was a few hours of review for an owner with this expertise. Delegating that review was a fair test of the experiment, and the brief already asked for first principles. Catching this was the orchestrator's job, not the reviewer's.

For anyone reviewing an AI-written design, four questions would have exposed most of it at the first gate, without reading the whole spec:

1. What can go wrong, and which of it does the design cover?
2. Does every action's input have a source?
3. What is out of scope, and who decided?
4. How do the tests prove an input exists?

The first question is the failure-mode catalogue that starts increment 2 (`docs/architecture/failure-modes.md`, step 1 of [docs/NEXT-STEPS.md](docs/NEXT-STEPS.md)). The other three are now gate rules in the standing prompt ([docs/process/orchestration-prompt.md](docs/process/orchestration-prompt.md), sections 3, 5 and 6).

## Bottom line

In 10 days and about $515, the sprint produced a working, formally modelled autonomic layer inside Cassandra, of a size a conventional team would need 18 to 24 months to write: about 55–70× faster in calendar time and several thousand times cheaper. Running it without supervision let the design start from what Cassandra made easy to read rather than from everything that can go wrong. Fixing the parts that were done wrong costs about $30–75 and a few days, 6–15% of the sprint. A few hours of plain review at the first gate would have avoided it. The method has been changed, and the scope still to deliver is catalogued and planned.
