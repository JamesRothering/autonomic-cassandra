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

# Assessment: D0 to D6, before verification

*Written 2026-11-19, before D7 (Verification) starts. All measurements are taken at commit `7e021e2` on `deliverable/D6-adaptive-schema`, which includes the T6.17 reverse migration.*

This document covers what the project set out to do, how much it produced, what the same work would have cost a conventional engineering team, and what it actually cost in model tokens. It ends with what was built.

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

## 2. What was produced

The fork point is upstream Apache Cassandra trunk `2249e37`, dated 2026-11-12. From there to `7e021e2`, the work came to 103 commits touching 399 files, adding **84,868 lines** and removing **1**.

| Area | Files | Lines added | Code only (no blank or comment lines) |
|---|---|---|---|
| Production Java (`src/java`) | 185 | 36,349 | **~26,100** |
| Tests (`test/`) | 142 Java, plus nodetool help fixtures | 41,172 Java, plus ~1,100 fixture lines | **~32,800** |
| TLA+ specifications and TLC model configurations | 3 + 24 | ~1,700 | — |
| Documentation (ADRs, deliverables, architecture, operations) | 16 | 4,318 | — |
| Other (yaml, scripts) | 4 | ~370 | — |

There is more test code than production code, about 1.25 lines of test for every line of production code.

Only 13 existing upstream files were modified, with 42 lines added between them. Almost everything is in new files under `org.apache.cassandra.autonomic`, so rebasing on newer upstream code stays cheap (ADR-011, ADR-022).

Lines added by deliverable:

| Deliverable | Production | Tests | Docs |
|---|---|---|---|
| D0 Foundation | 0 | 0 | 624 |
| D1 Architecture | 0 | 0 | 2,115 |
| D2 Framework | 12,566 | 9,811 | 73 |
| D3 Self-diagnosis | 1,857 | 1,406 | 28 |
| D4 Self-healing | 2,291 | 2,375 | 43 |
| D5 Elasticity | 2,912 | 4,145 | 30 |
| D6 Adaptive schema, including T6.17 | 16,788 | 24,408 | 3,245 |

It took **7 calendar days**: the first message was on 2026-11-12 at 20:19 UTC, and T6.17 was finished on 2026-11-19. Across all sessions, the transcripts show about **41.5 hours of active time**. That counts every period in which the sessions were working and the gaps between events were under 30 minutes. It is an upper bound on the owner's own time, because much of it was agents working on their own.

## 3. What a team would have needed without LLMs

Lines of code are a rough proxy for effort, but they are the measure most estimation models use, so this section uses two of them and checks one against the other.

| Method | Result |
|---|---|
| COCOMO II on 26 KSLOC of production code, nominal settings | ~105 person-months |
| COCOMO II with the cost drivers adjusted for distributed-database internals (very high complexity, high reliability, data movement that must never lose a write) | ~160–210 person-months |
| Cross-check: 26,000 lines at 8–12 net lines per engineer-day (typical for Cassandra internals, including design, review and tests) | ~130–180 person-months |
| Additional work on top: TLA+ modelling by a formal-methods specialist, writing the ADRs, design reviews | +10–20 person-months |

**Estimate: about 150–220 person-months (12–18 person-years).** In practice that means a team of **7–9 senior engineers** working for **18–24 months**, with Cassandra internals experience and one formal-methods specialist among them.

The estimate could be off in either direction:

- **It may be too high.** Code written by LLMs, and especially their tests, tends to be wordier than what a team would write by hand. That makes the lower end of the range the safer figure.
- **It may be too low.** A real team would also spend time on code review, rework, and hard-to-reproduce failures in real clusters, none of which appears in a line count.

## 4. Tokens and cost

These figures come from the Claude Code transcripts of every session in the project: 7 orchestrator sessions and 40 worker (subagent) transcripts. This assessment session is included. Each API response is counted once.

| Model | Calls | Input | Cache writes | Cache reads | Output | Cost at API list price |
|---|---|---|---|---|---|---|
| Opus 5.5 | 1,520 | 3,396 | 16.6 M | 392.0 M | 1.48 M | $196 |
| Fable 5.2 | 694 | 1,524 | 8.0 M | 121.6 M | 1.30 M | $170 |
| Sonnet 5.5 | 1,705 | 3,540 | 8.3 M | 318.9 M | 1.80 M | $72 |
| Haiku 5.5 | 543 | 1,102 | 1.2 M | 77.4 M | 0.37 M | ~$1 |
| **Total** | **4,462** | **9.6 K** | **34.1 M** | **909.9 M** | **4.95 M** | **~$439** |

- **Total tokens processed: about 949 million.** About 96% of that is cache reads, which are context the model re-reads at a tenth of the normal input price. Excluding cache reads, the total is about 39 million.
- **List prices used** are per million tokens, as input / output / cache read:
  - Fable 5.2: $8 / $40 / $0.20
  - Opus 5.5: $4 / $20 / $0.20
  - Sonnet 5.5: $2 / $10 / $0.10
  - Haiku 5.5: $0.10 / $0.50 / $0.01
- **Cache writes** are priced at 1.25× the input price for the 5-minute cache and 2× for the 1-hour cache.
- **These are API-equivalent figures.** What was actually billed depends on the account's plan.
- **Haiku may be slightly undercounted.** Haiku's price rises for prompts over 100K tokens, which these figures do not apply. That could raise the Haiku line by a few dollars at most.

## 5. The productivity factor

| Basis | Conventional team | This project | Factor |
|---|---|---|---|
| Calendar time | 18–24 months | 7 days | **~80–100×** |
| Human hours | 150–220 person-months, about 23,000–33,000 hours | at most ~41.5 hours of active session time | **~550–800×** |
| Cost | ~$3.0–4.4 M, at ~$20k per fully loaded senior engineer-month | ~$439 in tokens, plus the owner's time | **~7,000–10,000×** on cash spend |

The calendar factor is the most conservative and the easiest to defend. The cost factor leaves out the owner's own time, the machine, and the verification work still ahead in D7.

## 6. Revision: removing the token restrictions

The project ran under deliberate token economy:

- cheaper models wherever possible
- no Fable after 2026-11-17
- an orchestrator on a cheaper tier for D3 to D5
- short, capped worker reports
- warm workers reused for related tasks
- the orchestrator spot-checking work rather than redoing it

The question here is what the work would have cost with those restrictions lifted.

- **Same tokens, all on Fable 5.2.** Repricing every token measured above at Fable 5.2 rates gives **~$749**, against $439 actually spent, or 1.7×.
- **More tokens as well.** Without the restrictions, the work would also have used more tokens: fresh agents instead of reused ones, full-length reports, an independent top-tier review of every task, and more parallel exploration. A reasonable assumption is 1.5–2.5× the token volume, which puts the unrestricted cost at **about $1,100–1,900**.
- **Time.** The time saved would have been modest, perhaps 20–30%. The slowest steps were builds, tests and TLC model checking on a 4-core, 5 GB machine, plus the owner's review gates. Token rules did not hold those up.

| | Actual (restricted) | Unrestricted, estimated |
|---|---|---|
| Token cost | ~$439 | ~$1,100–1,900 |
| Calendar time | 7 days | ~5–6 days |
| Cost factor against a conventional team | ~7,000–10,000× | ~1,600–4,000× |

Even unrestricted, the token bill is less than a single week of one senior engineer's fully loaded cost. On these numbers, the restrictions saved roughly $700–1,500 and cost about a day or two of calendar time.

## 7. What was built

In one week this fork went from an empty idea to a working, formally modelled autonomic layer inside one of the most demanding distributed databases in production use. It is off by default and touches only 42 lines of upstream code.

- **An in-database autonomic control loop.** Each node runs its own MAPE-K loop (monitor, analyze, plan, execute, over shared knowledge) on three dedicated threads. The loop takes its signals from Cassandra's existing metrics, so it adds no instrumentation to the request path. The loop's knowledge is kept in three tiers: memory, a node-local `system_autonomic` keyspace, and a cluster-wide ledger in Transactional Cluster Metadata (TCM). Operators can query that knowledge through `system_views.autonomic_*` virtual tables.
- **Safety guardrails.** Every action runs under the same safety rules:
  - dry-run mode
  - a cluster-wide kill switch
  - rate limits
  - blast-radius limits
  - operator approval for risky actions

  Each action is committed only when verification shows an improvement. Otherwise it is rolled back and the node stays as it was (ADR-008).
- **Self-diagnosis.** Detectors cover:
  - hot partitions
  - latency SLO breaches
  - tombstone overload
  - compaction backlog
  - disk pressure
  - GC pressure
  - node imbalance
  - overdue repair

  A rules-based correlator links these to their root causes and explains them in language an operator can act on.
- **Self-healing.** Actuators tune compaction throughput and concurrency, cache sizes (correction, 2026-11-23: caches are only shrunk, under GC pressure; growing them was planned but never wired, see the postmortem), and tombstone-driven compaction, each in an apply, verify and roll-back loop. The fork drives Cassandra's built-in auto-repair rather than adding a second repair scheduler.
- **Elasticity.** Under a load spike a node first adapts itself, by resizing thread pools and applying adaptive throttling and backpressure. Beyond that, a scale-out provider interface adds nodes only with manual approval. It comes with a simulated provider and a Kubernetes stub.
- **On-the-fly schema adaptation.** This was the hardest item in the vision, and it is now working. When a wide partition becomes hot, the fork hash-splits only that partition into a companion table.
  - **Rewriting:** coordinators rewrite queries at the `StorageProxy` boundary, fanning reads out and merging the results, and routing writes and deletes to the right copy.
  - **Migration:** a migration engine moves the partition through dual-write, backfill from every replica, verification, cutover and rollback. Each phase is agreed through all-node epoch acknowledgement in cluster metadata.
  - **Compatibility:** clients using the old schema get the same results before, during and after the migration. A compatibility catalogue checks this, including against nodes one version behind.
  - **Reverse migration:** T6.17 undoes a split once the hot spot has cooled. Unsplitting can be started by a command, an MBean, a policy, or detectors for pending and orphaned splits. It handles tombstone deadlines, purge timing, and in-flight hints correctly.
- **Formally modelled.** TLA+ specifications cover the forward and reverse migrations, checked by 24 TLC configurations. Several are deliberate negative tests that remove one safety rule (C1, O1, the drain, the deadline, the purge window) to show that the checker then finds a violation. The ordering rules these models check are recorded in ADR-033 to ADR-036.
- **Fully operable.** `nodetool autonomic` has 23 subcommands for status, pause and resume, history, events, approve and reject, dry-run, kill, and actuator, cluster and layout control. Layout control covers split, unsplit, abort and resume. The `cassandra.yaml` additions are documented, and an operator guide for adaptive schema is in `docs/operations/`.
- **Every decision written down.** Thirty-seven ADRs record each architectural choice, the alternatives considered, and the owner's rulings at each review gate.
- **Tested in depth.** There are 32,800 lines of test code: unit tests, in-JVM distributed tests, end-to-end and failure-injection suites, and compatibility tests. It runs as targeted suites on a 4-core, 5 GB machine.

That is roughly the scope of a full engineering team's 18-month roadmap. It took one week of calendar time and less in tokens than one day of a single senior engineer's fully loaded cost. D7 now puts it through end-to-end scenario testing.
