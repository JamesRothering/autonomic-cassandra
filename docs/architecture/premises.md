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

# Premises review

- **Status:** proposed, 2026-11-28, revision 2, for the owner to rule on each premise. Not yet independently reviewed. Revision 3 adds PRE-20 (Accord at the core) and the version targets. Ruled so far: R-1 (rolling in-place upgrade from 5.0.x, then 6.0), R-14 (Java driver first, Python later), R-20 (Accord at the core). Still under discussion with the owner before the remaining rulings: how the JVM is replaced (PRE-3).
- **Commit:** branch `deliverable/D7.1` at `a0aa7c6`. Every `file:line` below refers to that commit.
- **Why it exists:** the owner's brief is to re-engineer Cassandra **from first principles**. Sprint 1 instead took Cassandra's architecture as given and added a control loop that tunes it. That choice was never examined. This document examines it, and every other premise found so far, **before** the 52 decisions of the failure-mode catalogue ([`failure-modes.md`](failure-modes.md)), because those decisions sit downstream of these premises.
- **Owner direction so far (2026-11-28):** the only thing that must be kept is backward compatibility, so that this is a drop-in replacement for Cassandra; prevention may be better than healing; STW GC pauses are among Cassandra's worst problems; replace the JVM with Rust, or a formal specification proven in Lean; the driver is an overlooked, first-line observer of failures; a new protocol between drivers and nodes is allowed next to the native protocol, which is kept only for compatibility; drivers may keep their own history; Java driver first, Python later, Go deferred; rolling in-place upgrade, because it is the meaning of drop-in (a client was lost by ScyllaDB for lacking it); pay cleanup as a debt, with more repayment in historically quiet periods.

## How to read it

Each premise has the same parts:

- **Cassandra assumes:** what the current design takes for granted.
- **Evidence:** code (`file:line`), a measurement, or a source; otherwise labelled **assumption** (reasoned, not checked) or **unverified** (a fact from outside this repository, not checked here).
- **Why doubt it:** what goes wrong because of it.
- **First-principles alternative:** what we would build if starting fresh.
- **Compatibility:** what the alternative does to the drop-in promise.
- **Effect on the catalogue:** which failure-mode rows move, disappear or appear.
- **Recommendation** and the **ruling** the owner is asked for.

Paths: `C/` is `src/java/org/apache/cassandra/`. "The layer" is the autonomic layer built in sprint 1 (`C/autonomic/`). Premise numbers are `PRE-n`, to keep them apart from the audit's `P-n`.

### The order of preference for any failure

This order is used in every premise below. A higher level is always preferred when it is possible:

1. **Design it out:** the failure cannot happen.
2. **Bound it by construction:** it can happen, but only up to a limit chosen in advance.
3. **Predict and avoid:** act on a trend before it becomes a breach.
4. **Detect and heal:** notice it, act, check the effect, roll back if it did not help. This is what sprint 1 built.
5. **Diagnose for a human:** name it with evidence when nothing safe can be done automatically.

The failure-mode catalogue places almost every row at level 4 or 5. Revision 3 of the catalogue adds a disposition **prevent: X** (level 1 or 2) and moves rows to it where a premise below allows.

---

## Part 1. The compatibility contract

### PRE-1. What "drop-in replacement" fixes

**Cassandra assumes:** nothing; this premise is ours. Sprint 1 assumed the replacement must be the Cassandra codebase itself (overview 1.2 lists "changing the storage engine or the wire protocol" as non-goals, `docs/architecture/overview.md:50`).

**What must stay the same** (proposed contract):

| Surface | Kept because | Status |
|---|---|---|
| CQL: grammar, types, semantics, error codes, including lightweight transactions, `BEGIN TRANSACTION` (`src/antlr/Parser.g:757-774`), vector columns (`C/db/marshal/VectorType.java`) and SAI queries with vector search | applications | required |
| Native protocol v3, v4 and v5 (`C/transport/ProtocolVersion.java:48-50`; specifications in `doc/native_protocol_v*.spec`), unmodified drivers | applications | required |
| Reading Cassandra sstables: BIG from `ma` to `pa` (`C/io/sstable/format/big/BigFormat.java:404-406`), BTI `da` and `ea` (`C/io/sstable/format/bti/BtiFormat.java:262-263`); 5.0.9 writes `nb` by default (`storage_compatibility_mode: CASSANDRA_4` in its shipped `conf/cassandra.yaml`) | moving existing data without export | required |
| `cassandra.yaml` settings accepted (unknown or meaningless ones warned, not rejected) | operators | required |
| nodetool commands and JMX metric names used by dashboards | operators | required for the common set; full list to be drawn up |
| Internode protocol, cluster metadata (TCM) and schema exchange with stock Cassandra nodes | rolling in-place upgrade | **open: ruling R-1** |
| Language and runtime (JVM) | nothing; users never see it | **not part of the contract** |

**Version targets (owner, 2026-11-28):** rolling upgrade from **Cassandra 5.0.x** (latest stable: `cassandra-5.0.9`) first, then **6.0** once released (latest tag: `cassandra-6.0-alpha2`). The fork's base is trunk, which already calls itself 7.0 (`build.xml:36`). This matters: 5.0 has no cluster metadata service (no `C/tcm/` at `cassandra-5.0.9`); it arrives in 6.0 (`MessagingService.VERSION_60`, "TCM", `C/net/MessagingService.java:268`). In a cluster with 5.0 nodes, our node must speak gossip-based metadata and schema; it changes to the metadata service only after every node is upgraded and an operator runs `nodetool cms initialize` (`C/tools/nodetool/CMSAdmin.java:104`).

**Ruling R-1, the upgrade path:**

- **(a) Rolling in-place upgrade.** Stock Cassandra nodes are replaced one at a time in a live cluster. The new node must speak Cassandra's internode messaging, the cluster metadata service and schema exchange to old nodes, version by version. This is the strongest promise and the largest compatibility surface.
- **(b) Same drivers, same CQL, same sstables, new cluster.** Applications switch by configuration. Data moves by loading existing sstables or by streaming from the old cluster. Much smaller surface. (a) can be added later without redoing (b).

**Ruled (owner, 2026-11-28): (a).** Reason given: it is the meaning of drop-in, and at least one client the owner knows of did not choose ScyllaDB for lacking it. The recommendation had been (b) first. Consequence: the internode protocol, cluster metadata and schema exchange with stock Cassandra nodes are part of the contract from the start, and the replacement of the JVM must keep a node able to join a stock cluster at every step (PRE-3).

---

## Part 2. The engine

### PRE-2. Heal after the fact, rather than prevent

**Cassandra assumes:** operators prevent problems by sizing and tuning; the system reports. Sprint 1 assumed the layer heals what tuning missed.

**Evidence:** the catalogue's dispositions: 61 act, 56 diagnose only, 9 out of scope, 4 open; none prevents (`failure-modes.md`, coverage summary). Overview 1.2 rules out predictive control (`overview.md:48`).

**Why doubt it:** a layer that acts after the breach always lets the first breach through, and its actions are themselves a risk (it can act on a misread signal, as the near-miss showed). Many failures can be made impossible or bounded.

**Alternative:** apply the order of preference above to every row. Allow level 3 (prediction from trends) for slow resources: disk, memory growth, sstable count.

**Compatibility:** none affected.

**Effect on the catalogue:** a new disposition **prevent**; rows move up a level where PRE-3 to PRE-12 allow. Revision 3 lists each move.

**Ruling R-2:** adopt the order of preference as a rule of the project (it would go into the standing orchestration prompt), and lift the overview's ban on predictive control for slow resources.

### PRE-3. The JVM and its garbage collector

**Cassandra assumes:** a Java process with a garbage-collected heap, tuned by choice of collector and heap size.

**Evidence:**
- Shipped collectors: G1 on JDK 17 (`conf/jvm17-server.options:33`), generational ZGC on JDK 21 (`conf/jvm21-server.options:31-32`).
- Memtables, the largest heap user, are on the heap by default: `memtable_allocation_type: heap_buffers` (`conf/cassandra.yaml:886`).
- Caches the layer can resize hold at most about 150 MiB with defaults (`failure-modes.md`, physical facts).
- The catalogue has 11 JVM rows (FM-JVM-01 to -11) plus GC-related layer rows (FM-AUT-06, -08).

**Why doubt it:** a GC pause starts a cascade. The paused node looks slow or dead; coordinators time out and write hints for it; speculative retries add load (`C/service/reads/*SpeculativeRetryPolicy.java`); the dynamic snitch moves reads to the other replicas, whose GC load then rises. When the node returns, queued requests, hint replay, read repair and client retries arrive together, and that allocation causes the next pause. **Recovery causes the next failure.** The layer reacts after the fact, with a lever (cache size) that moves little.

**Alternative:** an engine in **Rust**, with no garbage collector. Memory is owned and freed at known points, so STW collection pauses do not exist (level 1), and memory accounting is exact, which the layer needs to *know* rather than guess. Request-scoped memory is freed when the request ends. Memory is still finite, so PRE-9 (memory budgets) still applies.

Options considered and their place:

| Option | Removes STW pauses | Notes |
|---|---|---|
| Rust engine | yes, by construction | owner's direction; recommended |
| Stay on the JVM: off-heap trie memtables (`C/db/memtable/TrieMemtable.java`), off-heap caches, ZGC, memory budgets | close to it (ZGC pauses are sub-millisecond, **unverified** here; its failure mode becomes the allocation stall) | the fallback if R-3 is rejected; also useful for the Java fork that serves as the oracle (PRE-4) |
| Panama arenas for request memory, still on the JVM | partly | reduces garbage, does not remove the collector |

**Compatibility:** none affected; the runtime is not part of the contract (PRE-1).

**Effect on the catalogue:** FM-JVM-01 to -10 are designed out (FM-JVM-11, thread deadlock, stays in a new form); FM-AUT-06 and the GC part of FM-AUT-08 go; FM-MEM-06 (native memory) changes to exact accounting. Most other families are untouched: Rust does not stop disks filling, networks partitioning, peers slowing, compaction falling behind, clients misbehaving or clocks skewing.

**What it means for sprint 1's code:** the layer runs inside Cassandra's JVM, so its Java code does not carry over. Its design does (sense, diagnose, correlate, act, verify, roll back, the safety governor), and so does the catalogue. The Java fork becomes the oracle of PRE-4.

**Ruling R-3:** a Rust engine as the target.

### PRE-4. How we know it is right: unit tests with injected signals

**Cassandra assumes:** unit tests, distributed tests (in-JVM dtests), Python dtests, and for some protocols formal models.

**Evidence:**
- Sprint 1's layer was proven mostly by unit tests with injected signals; the near-miss showed that they prove logic, not that an input exists (`docs/postmortems/2026-11-22-cache-and-memory-near-miss.md`).
- Upstream already uses Lean: a 619-line proof of Accord's execution order (`formalise/accord/execution/lean/AccordAcyclic.lean`), with TLA+ models beside it (`formalise/accord/execution/tla/`).
- Sprint 1 wrote three TLA+ specifications for schema layout migration (`docs/specs/layout-core.tla`, `layout-migration.tla`, `layout-reverse.tla`).
- Upstream has a deterministic cluster simulator (`test/simulator/`, 172 Java files).

**Why doubt it:** a new engine must behave exactly like Cassandra wherever drivers and applications depend on it. Tests written by the same team that wrote the code share its blind spots.

**Alternative**, in four parts that check each other:

1. **Lean** specifies and proves the algorithms: replication and consistency levels, membership, hinted handoff and repair, the read and write paths' guarantees, compaction's preservation of data, and the layer's safety rules (for example: every action is reversible; no node action while transports are stopped; actions per hour are bounded).
2. **Verified Rust** for the most critical modules, with a tool that checks Rust against a specification (Verus) or translates Rust into Lean (Aeneas). Aeneas is in production use for Rust cryptography at Microsoft (SymCrypt); it does not support `unsafe` Rust or interior mutability ([Aeneas](https://github.com/AeneasVerif/aeneas), [SymCrypt](https://microsoft.com/en-us/research/blog/verifying-rust-cryptography-in-symcrypt-from-standards-to-code)), so it fits pure logic (merge and reconciliation rules, tombstone purge conditions, partitioner arithmetic, the governor's rules), not I/O or concurrent code. Fitness for this code is **unverified**; a spike decides.
3. **Deterministic simulation testing** of the whole Rust cluster: one seed fixes the clock, the scheduler, the network and the disk faults, so any failure replays exactly. Checked against the Lean and TLA+ models.
4. **Cassandra as the oracle (differential testing):** run the same workloads against the Java fork and the Rust engine, and compare results, errors and sstables on disk. Any difference is a bug in one of them. This is how "drop-in" is proven.

Proving a whole database correct line by line is beyond the current state of the art: the largest comparable proofs (seL4, CompCert, IronFleet) cover much smaller systems (**unverified** comparison). The combination above is the practical form, and it has a production precedent: AWS's Cedar authorization language is built from an executable Lean model with proofs, a Rust implementation, and differential random testing of the two, and a release ships only when model, proofs and tests are current ([How We Built Cedar, FSE 2024](https://arxiv.org/pdf/2407.01688); [AWS blog](https://aws.amazon.com/blogs/opensource/lean-into-verified-software-development)).

Lean is for the specification and the proofs, not the code that runs. Lean 4 compiles to native code but manages memory by reference counting ([Lean reference, reference counting](https://lean-lang.org/doc/reference/4.36.0/Run-Time-Code/Reference-Counting/); [Counting Immutable Beans, IFL 2019](https://arxiv.org/pdf/1908.05647v2.pdf)), so a database written in Lean would bring back the freeing cascades of PRE-19.

**Compatibility:** part 4 is the proof of the contract.

**Effect on the catalogue:** none directly; every row that acts gets a property in Lean or a simulation scenario.

**Ruling R-4:** adopt the four parts; first spike to choose between Verus and Aeneas.

### PRE-5. Fixed thread pools that something has to tune

**Cassandra assumes:** staged event-driven architecture (SEDA): 16 stages (`C/concurrent/Stage.java:46-61`), sized in configuration (`concurrent_reads: 32`, `concurrent_writes: 32`, `conf/cassandra.yaml:780-781`).

**Why doubt it:** queues between stages are where overload hides; pool sizes are guesses; the layer then resizes them from outside, a second controller acting on the first. Context switches and shared state cost CPU.

**Alternative:** thread-per-core, shared-nothing: each core owns a shard of the data and runs its own event loop with asynchronous I/O (io_uring on Linux). No pools to size. Concurrency is limited by admission (PRE-9), not by thread counts.

**Compatibility:** none affected. `concurrent_*` settings are accepted and ignored with a warning.

**Effect on the catalogue:** FM-TP-01 and -02 (pool saturation) are designed out; FM-CPU-05 (one pinned thread) becomes "one hot shard", which PRE-11 handles.

**Ruling R-5:** thread-per-core in the Rust engine.

### PRE-6. The page cache and memory-mapped files

**Cassandra assumes:** the operating system's page cache holds hot data; index files are memory-mapped by default (`disk_access_mode = mmap_index_only`, `C/config/Config.java:132`).

**Why doubt it:** the page cache is invisible to the process: the layer cannot tell how much of the working set is cached (the near-miss's memory blindness). A page fault on a mapped file blocks the thread that touched it, with no way to schedule around it.

**Alternative:** direct I/O with a cache the engine manages and accounts for, read and written through the I/O scheduler of PRE-7.

**Compatibility:** none affected; `disk_access_mode` accepted and ignored with a warning.

**Effect on the catalogue:** FM-OS-03 (map count) designed out; FM-MEM rows on page cache become exact; FM-RD-05 (cache too small) becomes a decision the engine makes with real numbers.

**Ruling R-6:** direct I/O with an engine-managed cache.

### PRE-7. Compaction throttled by a fixed throughput cap

**Cassandra assumes:** compaction runs at up to `compaction_throughput: 64MiB/s` (`conf/cassandra.yaml:1309`), competing with client requests for the disk.

**Why doubt it:** a fixed cap is wrong in both directions: too low and the backlog grows, too high and latency suffers. The layer then turns the cap and the number of compactors.

**Alternative:** an I/O scheduler that shares disk time between classes (client reads, client writes, flush, compaction, streaming, repair) by priority and measured device capacity, so background work uses idle capacity and yields under load automatically.

**Compatibility:** none affected; the setting becomes a ceiling for the compaction class.

**Effect on the catalogue:** FM-CMP-01 and -02 move from heal to bound; the "yield background work" rung of the load ladder becomes built in.

**Ruling R-7:** an I/O scheduler with classes.

### PRE-8. Data tied to the node's local disk

**Cassandra assumes:** each node keeps its replicas on local disk; adding a node streams data to it, which takes hours for large nodes (**assumption** on duration; it depends on data size and network).

**Why doubt it:** the brief asks for elastic response to demand spikes. Streaming makes scale-out slow; a full local disk is a failure mode; replacing a node means copying its data.

**Alternative:** sstables on shared or object storage, with the local disk as a cache; ownership of a range moves without copying its data. Kept optional: local-only remains the default for deployments without object storage.

**Compatibility:** none affected for applications. Operators get a new deployment mode.

**Effect on the catalogue:** FM-DSK-01 (disk full) and FM-CLU scale-out rows change; new rows for object-store latency and availability.

**Ruling R-8:** tiered storage as an option, after the core engine.

### PRE-9. Overload handled by timeouts, hints and retries

**Cassandra assumes:** a node accepts work until it times out; coordinators write hints for replicas that do not answer; drivers and coordinators retry or speculate.

**Evidence:**
- Request rate limiting is off by default (`native_transport_rate_limiting_enabled = false`, `C/config/Config.java:381`); throwing on overload is off (`native_transport_throw_on_overload = false`, `:1643`).
- The in-flight limit covers request bodies only and defaults to a tenth of the heap (`C/config/DatabaseDescriptor.java:811-813`); responses, reads in progress, hint replay and streaming are outside it.
- The protocol already has an `OVERLOADED` error (`C/exceptions/ExceptionCode.java:41`), which drivers handle.

**Why doubt it:** timeouts and retries spread overload from node to node, and recovery competes with live traffic (PRE-3's cascade).

**Alternative:** **a memory and work budget per node** covering all in-flight work: requests, responses, reads in progress, mutations, hint replay, streaming, repair. Work that does not fit is refused at once with `OVERLOADED`, never allocated. **Recovery is paced** from the same budget: hint replay, read repair and catch-up after a slow period take a bounded share. The driver extension (PRE-14) lets drivers back off before the node must refuse.

**Compatibility:** `OVERLOADED` exists in the protocol; unmodified drivers handle it (their retry policy decides).

**Effect on the catalogue:** FM-CLI-01, FM-TP-03, FM-JVM-04 move to bound; the cascade becomes a row of its own (new, FM-CLU), handled by pacing.

**Ruling R-9:** a per-node budget with paced recovery.

### PRE-10. Hundreds of settings that operators tune

**Cassandra assumes:** configuration decides behaviour; `Config.java` has 498 public settings (count of `public` fields, `C/config/Config.java`).

**Why doubt it:** each setting an operator must get right is a failure mode. Many can be derived from the hardware and the workload at runtime.

**Alternative:** every setting is classified as one of: *semantic* (changes behaviour users see; kept), *derived* (computed from hardware and load; the setting becomes an optional ceiling), or *obsolete* (accepted and ignored with a warning). Derived values are what the autonomic layer of the new engine owns.

**Compatibility:** `cassandra.yaml` files keep working.

**Ruling R-10:** classify all settings; the list goes into revision 2 of this document.

### PRE-11. Token-ring placement with fixed ownership

**Cassandra assumes:** each node owns token ranges (vnodes); a hot range stays on its owners. Sprint 1 answered hot spots with on-the-fly schema changes (partition splitting).

**Evidence:** drivers route requests from token ranges they read from `system.local` and `system.peers` (`C/db/SystemKeyspace.java:1095-1096` reads peer tokens).

**Why doubt it:** a hot spot cannot move without moving ownership; schema changes to spread a partition are visible to users.

**Alternative:** ranges that split and move between nodes at the storage level when they get hot or large (as ScyllaDB's "tablets" do, **unverified** as to their details). Drivers that know the extension get routing updates (PRE-14); unmodified drivers keep working with a coarse token map, at the cost of an extra hop.

**Compatibility:** the coarse token map must stay valid for unmodified token-aware drivers; this is the main design constraint.

**Effect on the catalogue:** FM-RD and FM-CLU hot-spot rows move to bound; sprint 1's schema-splitting work becomes a fallback for single hot partitions, which range splitting cannot divide.

**Ruling R-11:** splittable ranges, after the core engine.

### PRE-12. Repair as an operator's chore

**Cassandra assumes:** operators schedule anti-entropy repair; trunk adds an optional automatic scheduler (`C/repair/autorepair/`; configuration commented out at `conf/cassandra.yaml:2930`).

**Alternative:** continuous anti-entropy built in, paced by the budget (PRE-9) and the I/O scheduler (PRE-7), always on.

**Effect on the catalogue:** FM-CLU repair rows move to bound.

**Ruling R-12:** continuous repair, on by default.

### PRE-13. Write timestamps chosen by clients

**Cassandra assumes:** drivers generate write timestamps by default, and the latest timestamp wins (`USING TIMESTAMP`, `C/cql3/Attributes.java:85`).

**Why doubt it:** a client with a skewed clock silently loses writes, or makes its writes impossible to overwrite. The server cannot see it today.

**Alternative:** keep the semantics (they are part of CQL), but detect skew: the server compares each client timestamp with its own clock and with the client's history (through PRE-14), names the client and the size of the skew, and can refuse timestamps beyond a bound (an existing guardrail family, **unverified** which one applies). Server-side timestamps stay available as today.

**Compatibility:** semantics kept; refusal is off unless the operator enables it.

**Effect on the catalogue:** new row FM-DRV (client clock skew), diagnose only, with optional bound.

**Ruling R-13:** detect and report client clock skew; refusal as an opt-in.

### PRE-19. Cleanup is deferred and paid in bursts

**Cassandra assumes:** cleanup happens later, when something triggers it: the garbage collector when the heap fills, compaction when sstables pile up, tombstone purging after `gc_grace_seconds`, hint replay when a node returns, cleanup after a topology change when an operator runs it.

**Why doubt it:** deferred cleanup builds up silently and is then paid all at once, and that burst is what cascades. A GC pause is one case. Rust removes the garbage collector (PRE-3) but not the rest:

- **Freeing memory in Rust** happens at known points, but a known point can be expensive: dropping a memtable of ten million rows frees each row on the thread that drops it, which in a thread-per-core engine stalls a whole shard; the last holder of a reference-counted structure pays for freeing all of it; lock-free structures with deferred reclamation hold all garbage back behind one slow reader and then free it in a burst (the shape of a GC cycle); the allocator returns memory to the OS in batches (**assumption** for the engine; all are known Rust patterns).
- **The storage engine is itself a garbage collector for data**, whatever the language: every overwrite, delete and expiring TTL creates garbage on disk.

| Deferred cleanup | How it cascades |
|---|---|
| Compaction | falls behind, so more sstables per read, slower reads and more CPU, so it falls further behind; one large merge floods the disk |
| Tombstones, kept until `gc_grace_seconds` so repair stays correct | reads scan dead data, time out, are retried |
| Memtable flush, which alone frees memtable memory | writes block when flush lags; commit log segments cannot be recycled and the disk fills |
| Hints for a down node | build up during the outage, replay in a storm when it returns |
| Streaming and repair | leave many small sstables, then a compaction storm |
| TTL expiry | data written together expires together, then a burst of compaction |
| Cleanup after a topology change | rewrites every sstable on the node |

**Alternative: garbage debt.** Cleanup is charged when the garbage is created and paid off at a bounded rate:

1. **Every operation records the debt it creates:** a write adds compaction debt, a delete adds tombstone debt, a write for a down replica adds hint debt, a flush adds the debt of its sstable.
2. **The scheduler pays the debt continuously** from a guaranteed share of CPU and disk (PRE-7). The share has a **floor**, so cleanup never stops, and a **ceiling** under load, so it never floods client work; between them it rises with the debt.
3. **More is paid in quiet periods.** The engine keeps a history of demand (by hour of day and day of week) and raises the share when demand is predicted to be low (level 3 of the order of preference), so most of the debt is paid off-peak. Prediction only moves the share between the floor and the ceiling; a wrong prediction costs efficiency, never safety.
4. **Debt has limits and due dates.** Some debt cannot wait for a quiet period: disk headroom, commit log segments, memory, and tombstones that must be purged in time. Each kind has a hard limit and, where it applies, a deadline; nearing either raises the floor regardless of the time of day.
5. **Admission control refuses work whose debt cannot be paid** (PRE-9): at the limit, new work gets `OVERLOADED` before anything is allocated.
6. **Data is laid out so that cleanup is cheap.** In memory: **arenas**, one block of memory per lifetime (a memtable, a request), handed back whole in one step at constant cost instead of freeing each object. On disk, the same idea: data that expires together is stored together, so its cleanup deletes a whole file instead of rewriting it (Cassandra's time-window compaction does this for one workload). Large structures that must be freed piece by piece are freed a slice per event-loop tick.
7. **Recovery work is debt too:** hint replay, streaming and catch-up after a slow period draw from the same budget, which breaks the "recovery causes the next failure" loop of PRE-3.

The result is a bound, set by design, on how far cleanup can fall behind and on how much cleanup any tick does, instead of a burst whose size depends on history. These bounds are properties to prove in Lean (PRE-4): for example, debt never exceeds its limit under admission control, and no tick does more than a fixed amount of cleanup.

**Compatibility:** none affected. `gc_grace_seconds`, TTLs and compaction settings keep their meaning; the scheduler decides *when* work is done, not *whether*.

**Effect on the catalogue:** FM-CMP-01 to -03, FM-WRT-02 to -04, FM-DSK-01 and -02, FM-DAT-03, FM-CLU hint and streaming rows move from heal to bound; a new row for the cascade itself (recovery causing the next failure).

**Ruling R-19:** garbage debt with floor, ceiling, quiet-period repayment, limits and deadlines.

### PRE-20. Transactions are an opt-in extra

**Cassandra assumes:** reads and writes are single-partition and last-write-wins; transactions are a later, optional addition. Lightweight transactions use Paxos, still v1 by default even in trunk (`C/config/Config.java:1215`); Accord, general transactions across partitions, is off by default (`C/config/AccordConfig.java:44`). The catalogue proposed Accord out of scope for that reason (FM-OPT-01).

**Why doubt it:** the owner's goal includes atomic transactions with linear scalability. The usual way to get them (a leader per range with Raft or Paxos, row locks, as in CockroachDB, YugabyteDB or Spanner) cannot be used: in a mixed cluster our node shares replica sets with stock nodes (R-1), and CQL exposes the cell model (`WRITETIME()`, `USING TIMESTAMP`, per-cell TTL, blind writes). Accord gives serializable transactions across partitions with a **leaderless** consensus protocol, which fits Cassandra's replication, has no leader bottleneck, and orders conflicting transactions instead of locking rows, so there are no distributed deadlocks. Upstream has proved part of it in Lean (`formalise/accord/execution/lean/AccordAcyclic.lean`).

**Alternative:** Accord at the core of the Rust engine, not an add-on: transactional tables are a first-class mode, designed with the storage engine, the scheduler and the budget (PRE-9, PRE-19) from the start; lightweight transactions run on Accord where the table's mode allows (upstream already has a Paxos-to-Accord migration path: `C/service/consensus/migration/`, `C/service/consensus/TransactionalMode.java`).

**Compatibility:** `BEGIN TRANSACTION` exists in trunk's grammar but not in 5.0, so transactions can be enabled only after every node runs 6.0 or our engine, like the metadata service. In a cluster with 6.0 nodes, our node must speak Accord's messages.

**Effect on the catalogue:** FM-OPT-01 moves from out of scope to the core; FM-WRT-08 (Paxos and LWT) changes; a new family of rows for Accord's own failure modes (coordinator recovery, dependency build-up, journal growth).

**Ruling R-20 (ruled, owner, 2026-11-28):** Accord at the core.

---

## Part 3. The driver

### PRE-14. Observation starts at the coordinator

**Cassandra assumes:** the server's own metrics describe the service. Sprint 1 made the same assumption: all 8 client rows of the catalogue (FM-CLI-01 to -08) treat the client as a source of load; none records what the client experiences. The layer does not read the driver name and version that drivers send when they connect (`C/transport/messages/StartupMessage.java:51-52, 128`; no reference in `C/autonomic/`).

**Why doubt it:** the driver is the first to notice a failure, and the only component that measures what users get. Failures the server does not see or sees late:

- **The client gives up first.** The server's read timeout is 5 s and its write timeout 2 s (`C/config/Config.java:155, 161`); the Java driver's default request timeout is 2 s and the Python driver's 10 s (**unverified**). A 3-second read is a slow success to the server and a timeout to a Java application.
- **The request never reaches a server:** a network fault between application and cluster, or a full connection pool or queue inside the driver. The server sees traffic drop, which the layer reads as less load.
- **Retries and speculative executions hide failures:** the server sees two requests where the application saw one slow one.
- **Drivers mark nodes down** from heartbeats and connection errors before gossip agrees.

Failures the driver **causes**, which the catalogue lacks: retry storms; speculative executions that double load on a slow cluster; unsafe retries of non-idempotent statements; client clock skew (PRE-13); re-prepare storms after a node restarts; routing that is not token-aware or goes to the wrong datacenter; stale topology or schema metadata; requests the client has abandoned while the server still works on them.

**Alternative:** observation starts at the client, in three tiers:

1. **Unmodified drivers:** the server infers what it can: connection churn, re-prepare bursts, requests that finish after the driver's likely timeout (the driver name and version tell which default applies), the same statement from the same client at two coordinators within milliseconds (a speculative execution), client timestamps far from server time.
2. **Drivers with the extension** (PRE-15) report what they see.
3. **Nodes guide the drivers:** back off, avoid a replica, a node is recovering, new routing for split ranges (PRE-11).

**Effect on the catalogue:** a new family **FM-DRV**, failures seen or caused by drivers, about 12 rows (revision 3); FM-CLI-01 (spike or a peer's failure, which client reports can tell apart) and FM-CLI-05 (client-observed errors) change.

**Ruling R-14 (ruled, owner, 2026-11-28):** observation starts at the client; Java driver first, Python later, Go deferred.

### PRE-15. The native protocol is the only channel

**Cassandra assumes:** drivers and nodes speak only the native protocol.

**Evidence:** the protocol already carries a key-value **custom payload** on requests and responses (`C/transport/Message.java:198-203, 360`), which drivers that do not know a key ignore; at connection, nodes advertise supported options (`C/transport/messages/OptionsMessage.java:79-81`) and drivers send theirs in `STARTUP`.

**Why doubt it:** a failing node cannot report its own failure, and the driver that noticed it tells no one.

**Alternative:** the native protocol stays, **only for backward compatibility**. Next to it, a **driver extension**, negotiated at connection (the node advertises it in `SUPPORTED`; a driver that knows it asks for it in `STARTUP`), carrying:

- **Failure reports through responding nodes:** "node X timed out for me 14 times in the last 30 s", sent to any node that answers. The cluster combines reports from many clients. One client seeing timeouts points to that client's network; many clients point to the node. The layer has never been able to tell these apart.
- **Guidance from nodes to drivers** (PRE-14 tier 3).
- **Request IDs** shared by driver and server records, for end-to-end tracing.

Whether the extension rides in custom payloads at first or is a protocol of its own is a design choice for the extension's ADR.

**Driver-to-driver gossip:** a fallback only, for when no node answers. Clients can number in the thousands, sit in different networks, and are less trusted than nodes; reports through responding nodes scale better and reuse the nodes' own gossip. When no node answers, a driver keeps its reports and sends them once one does.

**Trust:** client reports are weighted evidence, never the only reason for a drastic action. Reports are authenticated with the client's connection; a client whose reports disagree with everyone else's is itself a finding.

**Compatibility:** unmodified drivers never see the extension.

**Ruling R-15:** a negotiated driver extension, with failure reports through responding nodes; driver gossip only as a fallback.

### PRE-16. Applications log their own database access

**Alternative:** a **flight recorder in the driver**: a bounded ring buffer of recent requests (time, statement shape and ID, coordinator, replicas, consistency level, latency, error, retries, speculative executions). When a failure is reported, the relevant window goes to the cluster and is matched with the server's own records by request ID.

**Limits:** it can replace an application's logging of database access, not its business logging, because the driver does not know what a request meant. By default it records statement shapes and IDs, **not bound values**, which can hold personal data; recording values is an opt-in. Size and retention are settings with safe defaults.

**Ruling R-16:** a driver flight recorder, values off by default.

---

## Part 4. The autonomic layer itself

### PRE-17. One outside control loop tuning a system built for manual tuning

**Cassandra assumes:** nothing (it has no such loop). Sprint 1 assumed the layer is a separate MAPE-K loop over Cassandra's settings.

**Why doubt it:** a loop outside a component, acting on that component's settings, competes with whatever the component does itself, and sees it only through metrics.

**Alternative:** control built into each component of the new engine (the scheduler, the budget, the cache, compaction, placement), each a small local controller with its own limits; the autonomic layer becomes the **supervisor**: it correlates across components, owns the safety governor (rate of actions, reversibility, observe-only after a crash, no action while transports are stopped), and decides what needs a human.

**What carries over from sprint 1:** the correlator, the governor, verify-and-roll-back, the knowledge store and its audit trail, the catalogue.

**Ruling R-17:** local controllers plus a supervising layer.

### PRE-18. Each node decides alone, from gossip

**Cassandra assumes:** nodes learn about each other through gossip, which is seconds to a minute old (comment on the peer detectors, `conf/cassandra.yaml:3120-3121`); cluster metadata goes through the cluster metadata service.

**Alternative:** node-local decisions stay local; decisions that affect several nodes (scale-out, range moves, cluster-wide pacing) go through the cluster metadata service as proposals, so two nodes cannot make conflicting cluster changes. Client reports (PRE-15) feed both.

**Ruling R-18:** cluster-scoped decisions through the metadata service.

---

## Rulings asked of the owner

| Ruling | Premise | Recommendation |
|---|---|---|
| R-1 | Upgrade path | **ruled: (a) rolling in-place upgrade**, from 5.0.x, then 6.0 |
| R-2 | Prevent before healing | adopt the order of preference; allow prediction for slow resources |
| R-3 | Runtime | Rust engine |
| R-4 | Verification | Lean, verified Rust for critical modules, deterministic simulation, Cassandra as oracle |
| R-5 | Threads | thread-per-core |
| R-6 | Page cache | direct I/O with an engine-managed cache |
| R-7 | Compaction I/O | I/O scheduler with classes |
| R-8 | Storage placement | tiered storage as an option, after the core |
| R-9 | Overload | per-node budget, paced recovery |
| R-10 | Settings | classify every setting |
| R-11 | Placement of hot ranges | splittable ranges, after the core |
| R-12 | Repair | continuous, on by default |
| R-13 | Client timestamps | detect skew; refusal opt-in |
| R-14 | Observation | **ruled:** starts at the client; Java driver first, Python later |
| R-15 | Driver channel | negotiated extension; reports through responding nodes |
| R-16 | Driver history | flight recorder, values off by default |
| R-17 | Layer shape | local controllers plus a supervisor |
| R-18 | Cluster decisions | through the metadata service |
| R-19 | Cleanup | garbage debt: floor, ceiling, quiet-period repayment, limits and deadlines |
| R-20 | Transactions | **ruled:** Accord at the core |

## What follows the rulings

The work is planned as user stories in [`docs/plan/user-stories.md`](../plan/user-stories.md), taken a few at a time. Revision 3 of the failure-mode catalogue (the **prevent** disposition, the FM-DRV family, cleanup-cascade and Accord rows, rows removed by the Rust engine) comes with story 8; the 52 decisions are then re-derived.

## Not verified

- Java and Python driver default timeouts, and the driver extension points named in conversation; no driver source is in this repository.
- ZGC pause times on this project's workloads.
- Fitness of Verus and Aeneas for this code; the scope of seL4, CompCert and IronFleet as comparisons.
- ScyllaDB tablet details.
- The duration of streaming for scale-out.
- Which guardrail, if any, bounds client timestamps today.
