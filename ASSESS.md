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

One thing in the brief matters more than this assessment first recognised. It asked for a database re-envisioned **from first principles**, and for **maximum quality** while minimising **unnecessary** token usage. Section 7 shows that the design was not built from first principles, and that the token economy cut into work that was necessary.

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

It took **10 days**, from the first message on 2026-11-12 at 20:19 UTC to the D7 tag on 2026-11-22 at 23:11 UTC. Across all sessions, the transcripts show about **57 hours with a session active**, counting every period with gaps between events under 30 minutes. That is machine time, mostly agents working unattended, not human time. The owner's own time was about **6 hours** (section 5.3).

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

The estimate could be off in two directions:
- **Too high.** LLM-written code tends to be wordier than hand-written code. If it is 1.5× wordier, COCOMO effort falls by about 36% (1.5 to the power 1.10). Schedule falls only about 13%, because schedule grows with roughly effort to the power 0.32. Section 5 shows this discount as the low end of each factor; it hits hours and cash, and barely touches calendar time.
- **Too low.** A real team would spend time on review, rework and failures in real clusters that no line count shows.

The comparison is for the **scope as built**: the team estimate is sized from the code that exists, not from the full brief. What the brief still needs is in section 8.

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

| Basis | Conventional team | This sprint | Factor | With the 1.5× verbosity discount |
|---|---|---|---|---|
| Calendar time | 18–24 months | 10 days | **~55–70×** | ~47–63× |
| Human hours | 150–225 person-months, about 22,500–34,000 hours | ~6.2 hours of the owner's own time (section 5.3) | **~3,600–5,500×** | ~2,300–3,500× |
| Cash spend | ~$3.0–4.5 M, at ~$20k per fully loaded senior engineer-month | ~$515 in tokens, plus the owner's time | **~5,800–8,800×** | ~3,700–5,600× |

Calendar time is the most robust: code verbosity moves it least (section 3). The owner's hours are the most striking figure, and the 6.2 hours are the owner's own estimate, episode by episode (section 5.3).

### 5.2 The cost of autonomy

The owner barely reviewed the design, so it could start from what Cassandra already exposes instead of from what can go wrong (section 7). Section 9 shows that a short review at the first gate would have caught this, had the clues been put plainly.

The cost of autonomy is the **rework**: work that was done wrong and has to be found and redone. Two other kinds of work are **not** counted:
- work the sprint never attempted, which is most of the 130 failure modes in the catalogue;
- design items that were planned but never built.

Both are scope still to deliver, and neither is in the team estimate, which is sized from the code that was built (section 3). A supervised sprint would have spent the same budget, aimed differently, and would not have delivered them either.

| Item | Cost |
|---|---|
| **Finding the problem** after the D7 tag: the coverage audit, both parts of the postmortem, `InputCoverageTest` and the standing prompt | ~$15–25 of the ~$28 spent between the tag and the close-out commit; the rest went on the cache signal (T7.15) and the sprint report |
| **Redoing the wrong parts** in increment 2: the GC signal on ZGC (T7.17), the CPU gate (part of T7.18), the cache shrink order (part of T7.16), the correlator fixes (T7.27), and checking the built code against the catalogue | ~30–60 units, ~$15–50 at $0.50–0.85 per unit (units defined below) |
| **Total** | **~$30–75, or 6–15% on top of the sprint**, plus about 1–3 days of calendar time |

With that cost included:

| Basis | Factor |
|---|---|
| Calendar time | **~40–65×** |
| Human hours | **~2,600–3,900×** (~1,650–2,500× with the verbosity discount), counting ~2.5 hours of the owner's time on the catch and the postmortem (section 5.3) |
| Cash | **~5,100–8,300×** |

Running without supervision cost a few percent of the sprint, not an order of magnitude.

**Units.** A unit is the project's measure of model spend: tokens weighted by each model's relative list price, the measure `docs/NEXT-STEPS.md` uses to size tasks. One weekly usage budget is about 350 units, or about $170–300 at list prices, so one unit is about $0.50–0.85.

### 5.3 The owner's time

The owner estimated each episode of their own time, from a list built out of the session transcripts. An episode is a run of the owner's messages less than an hour apart. Between episodes the owner was usually elsewhere, with the sessions left running; they returned to answer questions and decide. Times are Pacific.

| # | When (PT) | What the owner did | Time |
|---|---|---|---|
| 1 | Thu 12 Nov 12:19 | Kickoff prompt, setup answers, GitHub login | 15 min |
| 2 | Thu 12 Nov 23:41 | D1 gate: answered the 13 design questions | 15 min |
| 3 | Fri 13 Nov 11:32 | Continue after a limit reset | 1 min |
| 4 | Fri 13 Nov 13:04 | Asked for limit resets in Pacific time | 1 min |
| 5 | Fri 13 Nov 17:20 | Checked usage, continue | 1 min |
| 6 | Sat 14 Nov 00:53 | Approved D2; push rule; cheaper model for D3–D5 | 15 min |
| 7 | Sat 14 Nov 09:29 | Switched model, approved D3 | 15 min |
| 8 | Sat 14 Nov 13:51 | Picked an option, approved D4, go on T5.4 | 15 min |
| 9 | Sat 14 Nov 17:09 | Answered two questions | 15 min |
| 10 | Sat 14 Nov 23:47 | Restarted the orchestrator with a new prompt | 15 min |
| 11 | Sun 15 Nov 23:46 | "Keep going until D6 is ready" | 15 min |
| 12 | Mon 16 Nov 08:21 | Continue after a reset | 1 min |
| 13 | Mon 16 Nov 10:03 | Resumed the session | 1 min |
| 14 | Mon 16 Nov 15:33 | One instruction | 15 min |
| 15 | Mon 16 Nov 20:38 | Switched model, continue | 15 min |
| 16 | Tue 17 Nov 01:26 | "No more Fable, use Opus" | 1 min |
| 17 | Tue 17 Nov 11:38 | Progress check; ran the TLA+ checker | 15 min |
| 18 | Tue 17 Nov 13:57 | Reran the TLA+ checker | 15 min |
| 19 | Wed 18 Nov 00:15 | Merge D6 without review | 15 min |
| 20 | Wed 18 Nov 11:23 | D6 decisions | 15 min |
| 21 | Wed 18 Nov 13:35 | Accepted the remaining recommendations | 15 min |
| 22 | Wed 18 Nov 15:18 | "Start" | 15 min |
| 23 | Wed 18 Nov 16:33 | Accepted all recommendations for T6.17 | 15 min |
| 24 | Wed 18 Nov 22:09 | "Ok" | 1 min |
| 25 | Thu 19 Nov 02:12 | Asked for a code-size assessment | 15 min |
| 26–38 | Thu 19 Nov 10:27 to Sun 22 Nov 15:11 | The first assessment, D7 start and its findings, ADR-043 and ADR-044, the guard floor, approval of D7 | about 1.5 h in all, of which episodes 27, 30, 31 and 35 were 1 min each |
| | | **Sprint total** | **about 6.2 h** |

After the D7 tag, estimated from message times and not yet confirmed by the owner:

| When (PT) | What the owner did | Time | Counted as |
|---|---|---|---|
| Sun 22 Nov 15:22 | The catch, 11 minutes after the tag: "why can't you get the memory?" | ~15 min | cost of autonomy |
| Sun 22 Nov 23:45 to Mon 23 Nov 01:04 | The postmortem findings, "you took the easy path", the standing prompt, the wrap-up | ~1.5 h | cost of autonomy |
| Mon 23 Nov 21:13 to 21:52 | Started the failure-mode catalogue; asked for the first-review addendum | ~45 min | cost of autonomy |
| Tue 24 Nov 08:35 to 09:53 | Corrections to this assessment | ~1.5 h | assessment, not counted |

The ~57 hours with a session active (section 2) are not human time: they include agents running unattended.

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

## 7. Where it falls short of the brief

The owner found the main shortcoming on 2026-11-22, while tying up loose ends at the end of the sprint. The postmortem ([docs/postmortems/2026-11-22-cache-and-memory-near-miss.md](docs/postmortems/2026-11-22-cache-and-memory-near-miss.md)) and the coverage audit ([docs/audits/observability-coverage.md](docs/audits/observability-coverage.md)) set out the evidence.

- **Not from first principles.** The design started from the metrics and settings Cassandra already exposed (overview section 2, "What the base already provides"), not from a catalogue of what can go wrong. The catalogue has since been drafted for increment 2 (`docs/architecture/failure-modes.md` on `deliverable/D7.1`, proposed).
- **Blind spots.** The audit found 23 places where an existing or planned action or diagnosis lacks the signal it needs, and 18 where the layer cannot name a cause. Memory beyond heap use, real CPU use, disk I/O, messaging between nodes, the write path, hints and streaming are not observed.
- **Wrong assumptions about Cassandra.** GC time is taken as pause time, a live defect on JDK 21, where ZGC's concurrent cycles count as pressure. The off-heap row cache is shrunk to relieve the heap. The load average is taken as CPU use.
- **Planned but never built.** Growing caches, fewer compaction threads, a client in-flight-bytes knob, pools below their configured size, and recovery of throttled hints and streams.
- **Test limits.** In-JVM clusters of one to three nodes, JDK 17 only, no performance benchmarks, no real cluster. The hot-spot scenario fails about one run in five.
- **Why it happened** (postmortem part 2): a bottom-up method; cost pressure that displaced verification without the owner deciding it; no completeness criterion at any level; scope decided by omission; one agent designing, reviewing and accepting its own work; tests that fed made-up signals; and closure pressure.

What has changed since: a standing prompt that every session loads ([docs/process/orchestration-prompt.md](docs/process/orchestration-prompt.md)), an automated test that fails when an action lacks its input (`InputCoverageTest`), an evidence rule for every claimed limitation, and an independent review before anything ships. T7.15 was the first task under these rules; its independent review found two defects before it shipped.

## 8. What comes next

Increment 2 (D7.1) starts with a failure-mode catalogue that sets its scope, fixes the JDK 21 defect, adds the missing observation, decides the unbuilt design items, proves each resource class under real pressure, and closes with a full gate. The catalogue puts it at about 380–690 units, or 1.1–2.0 weekly usage budgets, depending on 52 owner decisions. Most of that is new scope; section 5.2 prices the part that is rework. The plan, with tiers, estimates and the owner's decisions per step, is [docs/NEXT-STEPS.md](docs/NEXT-STEPS.md).

## 9. Could the owner have caught it at the first review?

The owner asked this after the sprint. The experiment was to give a general direction and let Claude run with it, so the first review point was not read closely. The question is whether enough was presented there to catch the failures of section 7 up front.

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

## 10. What was built

In 10 days, one owner and a team of AI agents built what a conventional team would need 18 to 24 months to write. Here is what it does.

- **An in-database autonomic control loop.** Each node runs its own MAPE-K loop (monitor, analyze, plan, execute, over shared knowledge) on three dedicated threads. It reads Cassandra's existing metrics and adds no instrumentation to the request path, which is also the root of its main shortcoming (section 7).
- **Safety guardrails.** Dry-run mode, a cluster-wide kill switch, rate and blast-radius limits, cooldowns, a circuit breaker and operator approval for risky actions. An action is kept only if verification shows an improvement without breaking a guard; otherwise it is rolled back (ADR-008). Guards are not judged on request classes with too little traffic to measure (ADR-045).
- **Self-diagnosis.** Detectors for hot partitions, latency SLO breaches, pool saturation, tombstone overload, compaction backlog, disk pressure, GC pressure, node imbalance, degraded peers, and overdue repair. A cache-miss detector (T7.15) was added after the sprint. A rules-based correlator links them to root causes in language an operator can act on.
- **Self-healing.** Actuators tune compaction throughput and concurrency, shrink caches under GC pressure, and run tombstone-driven compaction, each in an apply, verify and roll-back loop. Growing caches was designed but not wired. The fork drives Cassandra's built-in auto-repair rather than adding a second repair scheduler.
- **Elasticity.** Under a load spike a node first resizes its thread pools and applies adaptive throttling. Beyond that, a scale-out provider interface adds nodes, only with manual approval, with a simulated provider and a Kubernetes stub.
- **On-the-fly schema adaptation.** A hot wide partition is hash-split into a companion table while clients of the old schema keep getting the same results: coordinators rewrite queries at the `StorageProxy` boundary, a migration engine runs dual-write, backfill, verification, cutover and rollback through all-node epoch acknowledgement, and T6.17 undoes a split once the hot spot cools. This was the hardest item in the vision.
- **Formally modelled.** Three TLA+ specifications, checked by 24 TLC configurations, including deliberate negative tests that remove one safety rule and show that the checker finds the violation.
- **Operable, with caveats.** `nodetool autonomic` has 23 subcommands, the `cassandra.yaml` additions are documented, and an operator guide covers configuration, the kill switch, rollback and the split demo. The operator cannot yet see much of what the layer cannot see (section 7).
- **Every decision written down.** 46 ADRs and two measurement spikes.
- **Tested in depth, within limits.** About 36,200 lines of test code: unit, in-JVM distributed, end-to-end, failure-injection, compatibility and repeatability suites, all on one 4-core, 5 GB machine, on JDK 17 only.

## Bottom line

In 10 days, with about 6 hours of the owner's time and about $515 in tokens, the sprint produced a working, formally modelled autonomic layer inside Cassandra, of a size a conventional team would need 18 to 24 months to write: about 55–70× faster in calendar time and several thousand times cheaper. Running it without supervision let the design start from what Cassandra made easy to read rather than from everything that can go wrong. Fixing the parts that were done wrong costs about $30–75 and a few days, 6–15% of the sprint. A few hours of plain review at the first gate would have avoided it. The method has been changed, and the scope still to deliver is catalogued and planned.
