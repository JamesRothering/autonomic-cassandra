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

# User stories

- **Status:** proposed, 2026-11-28. Stories 1–3 written in full for the owner to accept; 4–10 are headlines, to be written when their turn comes.
- **Source:** the premises review [`docs/architecture/premises.md`](../architecture/premises.md) and the owner's rulings in it (R-1 rolling in-place upgrade from 5.0.x then 6.0; R-14 Java driver first; R-20 Accord at the core).
- **How they are worked:** a few at a time. Each story closes like a deliverable (standing prompt, section 6): its acceptance criteria are met with evidence, it has an independent review, and the owner approves it.

Each story names who it serves, what they get and why, its acceptance criteria, the premises it serves, what is not in it, the questions it must answer, and a rough size. Sizes are in the cost units of `docs/NEXT-STEPS.md` (model tokens weighted by list price) and are **assumptions** from sprint 1 tasks of the same kind; none is measured.

## The ten stories

| # | Story | Premises | Depends on |
|---|---|---|---|
| 1 | The compatibility contract | PRE-1, PRE-20 | none |
| 2 | The end-state design | PRE-3, PRE-5 to PRE-7, PRE-9, PRE-17 | 1 (in part) |
| 3 | Garbage debt | PRE-19, PRE-9, PRE-2 | 2 (in part) |
| 4 | The storage engine moves out of the JVM | PRE-3, PRE-6, PRE-19 | 1, 2, 6 |
| 5 | The verification stack | PRE-4 | 1 |
| 6 | Rust standards, gates and the first spike | PRE-4 | 2 (in part) |
| 7 | The Java driver as first observer | PRE-14 to PRE-16, PRE-13 | 1 |
| 8 | Prevention first, and catalogue revision 3 | PRE-2 | the rulings |
| 9 | The autonomic layer as supervisor | PRE-17, PRE-18 | 2 |
| 10 | Settings, and the later engine work | PRE-10, PRE-8, PRE-11, PRE-12 | 2, 4 |

Accord (PRE-20) and SAI with vector search are part of stories 1, 2 and 4, not stories of their own.

---

## Story 1. The compatibility contract

**As** an operator running Cassandra 5.0.x, **I want** to replace my nodes one at a time with Autonomic Cassandra nodes in the live cluster, with no change to applications, drivers, configuration, tooling or dashboards, **so that** adopting it is an upgrade, not a migration project.

### What it delivers

1. **`docs/architecture/compatibility-contract.md`:** every surface the new node must match, with the version, the source of truth, how it is tested, and its status. At least:
   - **For applications:** CQL grammar and semantics, including the cell model (`WRITETIME()`, `USING TIMESTAMP`, per-cell TTL), collections, counters, user types, lightweight transactions, `BEGIN TRANSACTION` (6.0), vector columns and SAI queries with vector search; error codes; native protocol v3, v4 and v5 (`doc/native_protocol_v3.spec`, `v4`, `v5`).
   - **For a mixed cluster:** internode messaging at the 5.0 version (`MessagingService.VERSION_50`) and later 6.0 (`VERSION_60`); gossip-based membership and schema while any 5.0 node remains; the cluster metadata service after `nodetool cms initialize`; streaming (bootstrap, repair, decommission); repair's Merkle tree exchange; hints exchanged between nodes; Paxos messages; Accord messages once 6.0 nodes are present.
   - **For an in-place restart on the old node's disk:** the data directory layout; sstables from BIG `ma` to `pa` and BTI `da` and `ea`; the commit log, which the new node must replay after the stock node stops; hints files; saved caches (or a decision to drop them); system keyspaces, including `system_auth`; snapshots and the backup tools that read them.
   - **For operators:** `cassandra.yaml` of 5.0 (every setting accepted; each classified later by story 10); nodetool commands; JMX metric names; virtual tables; cqlsh; TLS for clients and between nodes; authentication and authorization.
2. **An oracle harness design:** the same workload sent to a stock 5.0.9 cluster and to a cluster with our nodes; results, errors and on-disk files compared.
3. **A rolling-upgrade test design:** a 3-node 5.0.9 cluster under a mixed workload; one node replaced at a time on its own disk; at each step, no client errors beyond the restart, the same results as the oracle, repair and streaming working in both directions; then a rollback of one node to stock 5.0.9.

### Acceptance criteria

- Every surface above is listed with its version, its source of truth (`file:line`, specification or upstream document) and how it will be tested.
- Each surface is marked **must match exactly**, **must accept** (for example settings that become meaningless) or **not supported**, with the reason; nothing is left out by omission.
- What changes between 5.0 and 6.0 for each surface is listed, so the 6.0 target adds rows, not a rewrite.
- The oracle and rolling-upgrade test designs say what they compare, on what workload, and what counts as a difference.
- An independent review has checked a sample of the cited sources and the completeness of the surface list.

### Not in it

Building the harness or any Rust code; Python driver specifics (later, R-14).

### Questions it must answer

- Can a node that crashed under stock Cassandra be restarted under ours on the same disk, commit log included, or must it be drained first? (Drained first is simpler; crash restart is the stronger promise.)
- Is rollback from our node to stock 5.0.9 on the same disk required? It constrains which sstable versions our node may write (a 5.0.9 node writes `nb` by default for that reason).
- Which nodetool commands and JMX metrics are "the common set" that must match exactly?

### Size

About 20–35 units: research by an opus worker over the code and the protocol specifications, the independent review, orchestration (**assumption**).

---

## Story 2. The end-state design

**As** the system programmer replacing the JVM from the inside out, **I want** the target architecture of the Rust engine designed and accepted before any production Rust is written, **so that** each part moved out of the JVM is built for the end state and the project does not end as Cassandra ported line by line to Rust.

### What it delivers

Architecture decision records, each with alternatives, evidence and an owner ruling:

1. **Runtime and sharding:** thread-per-core, one shard per core; how shards map to token ranges and vnodes, to Accord's per-range state, and to SAI and vector indexes.
2. **The scheduler:** one scheduler that owns all work (client reads and writes, flush, compaction, streaming, repair, hint replay, index builds), with classes, shares, floors and ceilings (PRE-7, PRE-19).
3. **The node budget:** memory and work accounting for all in-flight work, and admission with `OVERLOADED` (PRE-9).
4. **Memory:** arenas per lifetime (memtable, request), incremental freeing of large structures, memory reserved at start; no shared mutable structures across cores.
5. **I/O:** direct I/O with an engine-managed cache, io_uring on Linux (PRE-6).
6. **The boundary with Java while both exist:** how Java calls Rust (JNI on JDK 17 and 21; the foreign function and memory API is final only from JDK 22, **unverified** here), who owns which memory, how errors cross, how Rust's threads coexist with the JVM's.
7. **The order of migration:** storage first (memtables, sstables, compaction, caches), then SAI and vector indexes, Accord's storage, the coordinator and networking, then removing the JVM; what stays in Java until when; how the node keeps passing the rolling-upgrade test of story 1 at every step.
8. **A Lean model** of the scheduler and the budget, with the bounds stated as theorems to prove in story 3 and story 5.

### Acceptance criteria

- Every accepted premise (R-1 to R-20) is reflected in at least one ADR or marked as not affecting the design.
- For each component: its interface to Java in the interim, its interface in the end state, and why the interim interface does not force the end state to change.
- The order of migration names, for every step, what is measured to show the step worked (GC time and heap, latency, oracle differences).
- Independent review by an agent that wrote none of the ADRs; findings answered.

### Not in it

Production code. Spikes to answer a design question are allowed and recorded.

### Questions it must answer

- Shard per core inside a JVM process during the interim: does the Rust runtime get its own pinned cores, and how is the JVM's work kept off them?
- What is the smallest first step that moves real heap off the GC (memtables with their arenas, most likely)?

### Size

About 50–90 units: opus design, a Lean model, one or two spikes, review (**assumption**).

---

## Story 3. Garbage debt

**As** an operator, **I want** the node to pay for cleanup steadily, more in its quiet hours, and to refuse new work before cleanup falls dangerously behind, **so that** cleanup never arrives as a burst that cascades through the cluster.

### What it delivers

1. **The debt model** (design, `docs/architecture/garbage-debt.md`): each kind of debt, what creates it, its unit, how it is measured, its hard limit and any deadline:

   | Debt | Created by | Paid by | Limit or deadline |
   |---|---|---|---|
   | compaction | writes, flushes, streaming | compaction | sstables per read; disk headroom for the merge |
   | tombstones | deletes, expiring TTLs | compaction after `gc_grace_seconds` | tombstones scanned per read; purge before the table's repair window ends |
   | memtable memory | writes | flush | memory budget; commit log space |
   | commit log | writes | flush | commit log space |
   | hints | writes for a down replica | hint replay | hint space; `max_hint_window` |
   | streaming and repair | topology changes, repair | compaction of the streamed sstables | as compaction |
   | memory to free | flushed memtables, finished requests | arena release, incremental freeing | memory budget |

2. **The repayment rules:** a floor and a ceiling per class; the share between them rising with debt; a demand history by hour of day and day of week, and a higher share when demand is predicted to be low; the floor raised as a limit or deadline nears, whatever the time; admission refused at the limit.
3. **A Lean model** proving, under stated assumptions about device capacity: debt never exceeds its limit while admission control is on; no tick does more than a fixed amount of cleanup; a wrong prediction never lowers the share below the floor.
4. **A prototype on the Java fork,** using setters that exist today (compaction throughput and compactors, hint and streaming throughput, native transport rate limiting), so the rules are tested on a real node before the Rust scheduler exists, and the fork becomes a better oracle.
5. **A scenario on a real node:** sustained writes with deletes and TTLs, a peer down for a period and then back, a daily load curve. Compared with stock settings: the largest burst of cleanup work per minute, client p99 latency during the peer's return, debt over the run, refusals.

### Acceptance criteria

- Every kind of debt has a measured input on the Java fork (`file:line` of the metric or a new sensor), checked for meaning as the standing prompt requires.
- The Lean theorems are stated and proved, or the ones not yet proved are listed with the reason.
- The scenario shows, with numbers, a smaller largest burst and a lower p99 during recovery than stock settings, and no limit exceeded; the owner sets the target numbers before the run.
- A prediction made deliberately wrong in one run shows no limit exceeded.
- Independent review; findings answered.

### Not in it

The Rust scheduler (story 4 builds it to this design).

### Questions it must answer

- How much history before predictions are trusted (a new node knows nothing)?
- Which debts may be paid only in quiet hours, and which must be paid continuously?

### Size

About 60–110 units: design and Lean model (opus), prototype and sensors (sonnet), the real-node scenario and its runs, review (**assumption**). Machine time for the scenario runs is the slower constraint, as in sprint 1.

---

## Stories 4–10 (headlines)

4. **The storage engine moves out of the JVM.** Arena-based memtables, sstable reading and writing (BIG and BTI), compaction under the garbage-debt scheduler, the caches; then SAI and vector indexes and Accord's storage. Java calls Rust; the node passes the rolling-upgrade test at every step; GC time and heap measured before and after.
5. **The verification stack.** Lean models with proofs for merge and reconciliation, tombstone purging, consistency levels, the governor's limits and the debt bounds; differential random testing of Lean model against Rust, Cedar style; the Java fork as oracle on the same workloads; deterministic simulation of a whole cluster under faults.
6. **Rust standards, gates and the first spike.** Strict lints as errors, no `unwrap` in production, `unsafe` only in listed modules checked with Miri and Loom, fuzzing, allocation and copy budgets in CI, an independent Rust review pass with a checklist of known failure patterns. The spike: an sstable reader checked byte for byte against the oracle, then a post on a Rust users forum asking for comments on its quality.
7. **The Java driver as first observer.** What the server infers from unmodified drivers; a driver extension negotiated at connection, with failure reports through nodes that still respond and guidance back to drivers; the driver flight recorder, bound values off by default; client clock skew detection.
8. **Prevention first, and catalogue revision 3.** The order of preference as a rule in the standing prompt; the **prevent** disposition; the FM-DRV family, cleanup-cascade rows and Accord rows; rows removed by the Rust engine; the 52 decisions re-derived.
9. **The autonomic layer as supervisor.** Local controllers inside each component; the layer correlates, governs and decides what needs a human; correlator, governor, verify-and-roll-back and audit trail carried over from sprint 1; cluster-scoped decisions through the metadata service.
10. **Settings, and the later engine work.** All 498 settings classified as semantic, derived or obsolete; then continuous repair, splittable ranges for hot spots, tiered storage for fast elasticity.
