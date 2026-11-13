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

# Autonomic Cassandra: Adaptive Schema (D6) Design

Status: D1 / T1.2. Base: Apache Cassandra trunk at `2249e37b`. Builds on [`overview.md`](overview.md) (T1.1).
Decisions referenced as ADR-NNN are in [`docs/DECISIONS.md`](../DECISIONS.md); this document adds ADR-014 to ADR-022.

Conventions are those of the overview: paths are relative to the repository root, "verified" means the code was read at the pinned commit, anything else is marked **unverified**. `SP` abbreviates `src/java/org/apache/cassandra/service/StorageProxy.java` in prose only.

Summary of the design in one paragraph: when one partition of a table is both wide and hot, the system creates a second physical table in the same keyspace whose partition key is the logical partition key plus a bucket number, and moves **that partition only** into it, spread over N buckets by a hash of the clustering key. The logical table and every other partition are untouched. Coordinators route reads and writes for the moved partition at the `StorageProxy` boundary, below CQL, so statements, result metadata, prepared statements and paging states stay logical. The move is a phased, resumable, verifiable migration driven by one owner through TCM; it is loss-free to abort until one explicit commit point.

## 0. Findings in the code that shape the design

| # | Finding (verified unless marked) | Where | Consequence |
|---|----------------------------------|-------|-------------|
| F1 | A `SchemaTransformation` is serialized into the metadata log as its CQL text and re-parsed on every node. | `SchemaTransformation.SchemaTransformationSerializer` in `src/java/org/apache/cassandra/schema/SchemaTransformation.java` | Our own schema changes must be plain CQL DDL (`CREATE TABLE`, `DROP TABLE`) submitted through `Schema.submit`. A layout cannot be smuggled into an `AlterSchema`. |
| F2 | Replicas reject a read whose table metadata epoch is older than theirs, and a write only while the coordinator's *message* epoch is behind the schema; `StorageProxy` then catches up and **retries the same mutation objects**, which pass. | `ReadCommandVerbHandler.checkSchemaVersion`, `AbstractMutationVerbHandler.checkSchemaVersion`, retry loops in `SP` (`dispatchMutationsWithRetryOnDifferentSystem`, `mutateAtomically`, `dispatchReadWithRetryOnDifferentSystem`) | The existing schema fence cannot fence a stale *routing decision* on the write path. We need our own agreement mechanism (section 4.3). |
| F3 | `TableMetadata.equals` ignores `epoch`; a table is "altered" only if params, columns, indexes, triggers etc. differ. `changeAffectsPreparedStatements` is true for any epoch bump. | `src/java/org/apache/cassandra/schema/TableMetadata.java` | Bumping a table epoch as a fence would need a real param change and would evict all prepared statements on the table at every phase. Rejected (ADR-016). |
| F4 | `TCM_CURRENT_EPOCH_REQ` returns the receiver's `ClusterMetadata.current().epoch`; `ProgressBarrier` uses it but waits only for a quorum-style subset of replicas of affected ranges. | `src/java/org/apache/cassandra/tcm/CurrentEpochRequestHandler.java`, `tcm/sequences/ProgressBarrier.java` (`WatermarkRequest`) | The verb is reusable to learn each node's epoch. `ProgressBarrier` itself is not: we need every node, not a quorum of replicas. |
| F5 | All CQL writes without conditions reach `StorageProxy.mutateWithTriggers`; all conditional writes reach `StorageProxy.cas`; coordinator single-partition reads reach `StorageProxy.read(SinglePartitionReadCommand.Group, ...)`; range reads reach `StorageProxy.getRangeSlice`. The partition denylist is checked at exactly these four places. | `ModificationStatement.executeWithoutCondition` / `executeWithCondition`, `BatchStatement.executeWithoutConditions` / `executeWithConditions`, `SP` | These are the interception sites (section 3.1). The call chain from `SinglePartitionReadCommand.Group.execute` and from the pagers into `StorageProxy.read` was not traced line by line (**unverified**; T6.4 confirms first). |
| F6 | `PagingState` holds the partition key, a `RowMark` (the last clustering returned), `remaining` and `remainingInPartition`. Nothing physical. | `src/java/org/apache/cassandra/service/pager/PagingState.java` | If routing happens below the pagers, paging states stay logical and survive phase changes. |
| F7 | `ColumnMetadata.compareTo` orders by kind, position and name, not by table; `Cell.withUpdatedColumn(ColumnMetadata)` exists. | `schema/ColumnMetadata.java`, `db/rows/Cell.java` | Rows can be re-labelled between two tables with identical column definitions without touching values, timestamps or TTLs. |
| F8 | Materialized-view build reads each local partition unfiltered and sends derived mutations through `StorageProxy`; `ThrottledUnfilteredIterator` splits a large unfiltered partition into bounded batches. | `db/view/ViewBuilderTask.buildKey`, `db/rows/ThrottledUnfilteredIterator.java` (class read only at signature level) | Prior art for a replica-local, tombstone-preserving backfill. |
| F9 | Materialized views are disabled by default (`Config.materialized_views_enabled = false`). | `config/Config.java` | Not a mechanism we may build on; see ADR-014. |
| F10 | Accord's Paxos-to-Accord migration couples a per-table migration state in `ClusterMetadata` to schema changes inside `AlterSchema.execute`. | `AlterSchema.maybeUpdateConsensusMigrationState` | Precedent for one deterministic coupling hook in `AlterSchema.execute` (H21). |
| F11 | Per-partition signals that exist: samplers `READS`, `WRITES`, `WRITE_SIZE`, `CAS_CONTENTIONS`, `LOCAL_READ_TIME`, `READ_ROW_COUNT`, `READ_TOMBSTONE_COUNT`, `READ_SSTABLE_COUNT` (`metrics/Sampler.SamplerType`); `TopPartitionTracker` (repair-time only). Table-level: `TableMetrics.maxPartitionSize`, `estimatedPartitionSizeHistogram`, `casPrepare/casPropose/casCommit`. The partition-size guardrail fires in the sstable writers (`SortedTableWriter`, `SSTableCursorWriter`); what it emits beyond a log line is **unverified**. | cited files | Section 1. There is no continuously maintained per-partition size. |

## 1. Hot-spot taxonomy

### 1.1 Shapes, detection, and what a layout change can do

| Shape | What hurts | Detection at this commit | Relieved by a layout change? | What the system does |
|-------|-----------|--------------------------|------------------------------|----------------------|
| A. Wide and hot partition: many rows, accessed mostly row by row | One replica set serves all traffic; large-partition reads, compaction and repair cost | `hot_partition` diagnosis (T3.2: sampler `READS`/`WRITES` share), plus a row-count probe (section 7.2) or `TopPartitionTracker` size when repair has run | **Yes.** Splitting the partition over N buckets divides partition size by about N and, when the cluster has more nodes than RF, spreads replica load | **MVP transformation `HASH_SPLIT`** |
| B. Oversized but cold partition | Compaction, repair, heap during reads of the whole partition | `TopPartitionTracker` (stale unless repair runs), `maxPartitionSize` (table level, no key) | Yes, same transformation; no urgency | Diagnose; split only by operator command (`nodetool autonomic layout split`) in the MVP because the key cannot be identified reliably without repair |
| C. Small partition with a very high request rate (one or few rows read or written very often) | One replica set; nothing to divide | `hot_partition` with a row-count probe below `min_rows_for_split` | **No.** Every bucket function maps the same row to the same place. Salting the partition key would make every read a fan-out and still put each row on one replica set | Diagnose as `hot_partition` with evidence `layout=not_applicable(small)`. Existing node-scoped responses apply (row/key cache capacity, advertised severity, request throttle: overview 8.1). The partition denylist (`PartitionDenylist`) remains a manual last resort. No schema action is proposed |
| D. Time-series "current bucket": all writes go to the newest clustering range of one partition | Write hot spot on one replica set; partition grows without bound | as A; write share dominates | **Yes with a hash split** (writes spread over N buckets). A range split by time would *not* relieve it: the newest range stays hot | `HASH_SPLIT`. Cost: time-range slices fan out to all buckets (section 3.4) |
| E. Tombstone-heavy partition (queue pattern) | Reads scan tombstones | `tombstone_overload` (T3.2), sampler `READ_TOMBSTONE_COUNT` | Partly: a split divides tombstones per read by N for point reads only. The real fix is data-model (time-bucketed partitions that expire whole) or compaction | Targeted garbage-collecting compaction (T4.2). No layout action in the MVP |
| F. Hot token range or skewed key distribution (many moderately hot keys adjacent on the ring, or uneven token ownership) | Some nodes own more load | `node_imbalance` (T3.4) | **No.** This is placement, not schema | Diagnose. Token moves and scale-out are topology actions (D5), out of scope here |
| G. Hot partition dominated by slices, full-partition reads or range scans | As A | as A plus the shape profile (section 7.2) | **Made worse** by a hash split: every such read becomes N reads | Do not migrate (section 7.4). A range split (follow-up) would suit it |
| H. Coordinator hot spot: token-aware clients send all requests for one key to its replicas | CPU on the coordinators | client request metrics per node | **No.** After a split, token-aware drivers still coordinate on the replicas of the *logical* key (section 5.1) | Diagnose only |

### 1.2 MVP transformation and why

`HASH_SPLIT`: **one logical partition is moved into a companion physical table and spread over N buckets by a hash of its clustering key.** Chosen because (ADR-014):

- it relieves the shapes a schema change can actually relieve (A, B, D);
- it touches only the hot partition, so the migration's added load is proportional to that partition, not to the table. This matters because the migration runs on a system that is already hot;
- the bucket of a row is a pure function of its primary key, so point reads and writes stay single-partition operations, and no data-dependent directory is needed;
- it needs no knowledge of the data distribution and keeps working as the partition grows.

Its costs are stated in section 5 and are the reason for the eligibility rules in sections 6 and 7.

### 1.3 Follow-up transformations (not designed in detail)

| Transformation | Relieves | Why not MVP |
|----------------|----------|-------------|
| `RANGE_SPLIT`: buckets are clustering ranges with explicit split points | G (slice-heavy), B | Needs the clustering distribution; the tail bucket keeps growing; does not relieve D |
| Whole-table re-bucketing: every partition gets a bucket component | Tables where most partitions are wide | Backfill is the whole table; every partition read fans out; range scans can no longer follow logical token order |
| Vertical split by column group | Wide rows with a hot small column set | Different rewrite (column routing); row-existence semantics across two tables are subtle |
| Reverse migration (un-split) | A split that did not help, or a feature that needs the original layout | Section 4.9; needs the same machinery in the other direction plus a solution for the base tombstone |

## 2. Layout model

### 2.1 Logical and physical tables

- **Logical table** `ks.t`: what clients created and see. Its `TableMetadata` is never modified by this feature.
- **Base physical table**: the logical table's own storage. It keeps every partition that is not split.
- **Overflow physical table** `ks.<t'>_al_<8 hex>`: one per logical table with a layout, in the **same keyspace** (same replication, same snapshot and repair scope; `ALTER KEYSPACE` applies to both atomically). `<t'>` is the logical name truncated to 32 characters; the suffix is the first 8 hex digits of `layoutId`. Total length stays below `SchemaConstants.NAME_LENGTH` (48).

Derivation of the overflow table from the logical table (function `PhysicalTables.derive(TableMetadata logical, UUID layoutId, TableId physicalId)`; it emits CQL, per F1):

```sql
-- logical
CREATE TABLE ks.t (p1 A, p2 B, c1 C, c2 D, v1 E, v2 F, PRIMARY KEY ((p1, p2), c1, c2))
  WITH CLUSTERING ORDER BY (c1 DESC, c2 ASC);
-- derived
CREATE TABLE ks.t_al_1a2b3c4d (p1 A, p2 B, "__al_bucket" int, c1 C, c2 D, v1 E, v2 F,
  PRIMARY KEY ((p1, p2, "__al_bucket"), c1, c2))
  WITH ID = <physicalId> AND CLUSTERING ORDER BY (c1 DESC, c2 ASC)
  AND comment = 'Managed by autonomic layout of ks.t. Do not access directly.'
  AND default_time_to_live = 0
  AND gc_grace_seconds = <same> AND compaction = <same> AND compression = <same> AND caching = <same>;
```

Clustering columns, their order and all regular column names and types are identical, so `Clustering` objects, the clustering comparator and `PagingState.RowMark` are interchangeable between the two tables. `default_time_to_live` is 0 because the logical table's default TTL is applied at the CQL layer before our hook sees the mutation; cells arrive with their TTL already set.

The bucket column value is `incarnation * 256 + bucket`, where `bucket` is in `[0, N)` and `incarnation` is unique per migration attempt of a key (section 2.3). A new attempt therefore never collides with tombstones left by an aborted attempt.

### 2.2 Routing rule

```
route(logical table T, partition key K, clustering C):
    L = registry.byLogical[T.id]                 // absent -> base
    S = L.keys[K]                                // absent -> base
    bucket(C) = floorMod(hash(C, L.hashedClusteringColumns), L.buckets)
    physical key = (K components..., S.incarnation * 256 + bucket(C))
```

`hash` is fixed and versioned (`hashAlgo = 1`): `MurmurHash.hash2_64` (`src/java/org/apache/cassandra/utils/MurmurHash.java`) with seed 0 over the concatenation, for the first `hashedClusteringColumns` clustering columns, of a 4-byte big-endian length followed by the serialized value. It must never change for an existing layout. The MVP sets `hashedClusteringColumns` to the number of clustering columns (every row with a full primary key maps to one bucket). A smaller prefix (so that `c1 = ? AND c2 > ?` stays in one bucket) is representable but not chosen automatically.

The rule is a **predicate plus a bounded explicit key list** (question 15): the predicate is the bucket function; the key list names the split partitions.

### 2.3 Registry data structures

One extension in `ClusterMetadata.extensions`, key id `"org.apache.cassandra.autonomic.layouts"`, separate from the ledger (overview 9.1). The constraints of overview 6.2 apply unchanged: implement `ExtensionValue` directly, public no-arg constructor, class never renamed, never mutate the previous value, own `formatVersion`.

```java
package org.apache.cassandra.autonomic.layout;

public final class LayoutRegistry                               // immutable
{
    public final int formatVersion;                             // 1
    public final ImmutableMap<TableId, TableLayout> byLogical;  // max 8 (max_layouts)
}
public final class TableLayout
{
    public final UUID layoutId;
    public final TableId logical, physical;
    public final String keyspace, logicalName, physicalName;
    public final Transform transform;                           // HASH_SPLIT
    public final int hashAlgo;                                  // 1
    public final int buckets;                                   // 2..64
    public final int hashedClusteringColumns;                   // 1..clustering size
    public final int nextIncarnation;                           // starts at 1, never reused, max 2^23
    public final ImmutableSortedMap<ByteBuffer, KeyState> keys; // raw partition key bytes; max 8; each <= 1024 bytes
    public final long version;                                  // +1 on every change to this layout
}
public final class KeyState
{
    public final Phase phase;
    public final Epoch phaseEpoch;                              // epoch at which this phase was committed
    public final long phaseSinceMillis;                         // proposer-supplied
    public final int incarnation;
    public final int generation;                                // backfill generation, starts at 1
    public final Epoch generationEpoch;
    public final ImmutableSet<NodeId> backfillDone;             // replicas that finished `generation`; max 32
    public final UUID actionId;                                 // the action that owns the current migration, null in STEADY
}
public enum Phase { ANNOUNCED, DUAL_WRITE, BACKFILL, VERIFY, CUTOVER, NEW_ONLY, CLEANUP, STEADY, ROLLBACK_DUAL, ABORTING }
```

`LayoutRegistryValue implements ExtensionValue<LayoutRegistry>`. Serialization: `formatVersion` (vint), then counts and fields in declaration order; `TableId` and `NodeId` through their existing serializers; keys as vint length plus bytes.

Size bounds, enforced inside the transformations (reject with `INVALID`): 8 layouts, 8 keys per layout, 1024 bytes per key, 32 node ids per key. Worst case about 90 KiB; a typical registry with one layout and one key is under 300 bytes. The registry is in every metadata snapshot, so the bounds are part of the format, not configuration that can be raised casually.

### 2.4 Transformations

All are `Kind.CUSTOM` children registered from `AutonomicTransformations.register()` (hook H9, no new core edit), deterministic, with all times supplied as fields.

| Transformation | Requires | Effect |
|----------------|----------|--------|
| `RegisterKey(actionId, owner, expectedStep, layout header, keyBytes, nowMillis, leaseMillis)` | `SCHEMA_LAYOUT` claim held by `actionId`/`owner` at `expectedStep`; `prev.inProgressSequences.isEmpty()`; logical and physical tables exist in `prev.schema` with the given ids; `PhysicalTables.matches(logical, physical)`; `Eligibility.structural(logical)` empty (section 6); bounds; key not already present | Creates the `TableLayout` if absent; adds the key in `ANNOUNCED` with `incarnation = nextIncarnation`, increments `nextIncarnation` and `version`; advances the claim (`step + 1`, phase `ANNOUNCED`, lease renewed) |
| `AdvanceLayout(actionId, owner, expectedStep, logical, keyBytes, from, to, generation, nowMillis, leaseMillis)` | claim as above; key is in phase `from`; `(from, to)` is in the transition table of section 4.2 (or `from == to` with `generation == current + 1`, a backfill restart) | Sets phase, `phaseEpoch = prev.nextEpoch()`, `phaseSinceMillis`; on a generation change clears `backfillDone` and sets `generationEpoch`; `version + 1`; advances the claim in the same commit |
| `ReportBackfill(logical, keyBytes, generation, nodeId)` | key in `BACKFILL` or `CLEANUP`; `generation` equals current | Adds `nodeId` to `backfillDone`. Idempotent. No claim needed: it is committed by replicas |
| `RemoveKey(actionId, owner, expectedStep, logical, keyBytes)` | claim; key in `ABORTING` | Removes the key; removes the `TableLayout` if it has no keys left; advances the claim |
| `FinishKey(actionId, owner, expectedStep, logical, keyBytes)` | claim; key in `CLEANUP` | Phase `STEADY`, `actionId = null`; advances the claim |
| `RemoveOrphanLayout(logical)` | `logical` is not a table in `prev.schema` | Removes the layout (after `DROP TABLE` or `DROP KEYSPACE`, section 4.8) |

Claim and registry change in **one** commit, so they cannot disagree. This requires the ledger to expose a pure helper to advance a claim from another transformation (framework change FC2).

### 2.5 Consistency with schema at an epoch

- Registry and schema are both fields of one `ClusterMetadata`; a coordinator that reads `ClusterMetadata.current()` once sees a consistent pair.
- Invariant "a layout's physical table exists and matches": established by `RegisterKey` validation and preserved by hook H21 in `AlterSchema.execute` (section 4.8), which rejects or mirrors client DDL deterministically on every node.
- The physical table is created **before** `RegisterKey` and dropped **after** the layout is removed, each as its own `Schema.submit` of CQL text. Between those commits an unused empty table exists. It is listed in `system_views.autonomic_layouts` as `orphan` and dropped by the owner or by the next owner.

## 3. Query rewrite

### 3.1 Interception site

Decision (ADR-015): neither candidate of overview 9.1. Routing happens at the **`StorageProxy` boundary**, on the already-built mutations and read commands, at the same places the partition denylist is checked.

| Hook | Location in `SP` | Statement (one each) |
|------|------------------|----------------------|
| H16 | `mutateWithTriggers`, after the denylist block, before `TriggerExecutor` | `if (LayoutWrites.route(mutations, consistencyLevel, mutateAtomically, requestTime, preserveTimestamps)) return;` |
| H17 | `read(SinglePartitionReadCommand.Group, ConsistencyLevel, Dispatcher.RequestTime)`, after the denylist block | `{ PartitionIterator r = LayoutReads.read(group, consistencyLevel, requestTime); if (r != null) return r; }` |
| H18 | `getRangeSlice(PartitionRangeReadCommand, ...)`, after the denylist block | same shape with `LayoutReads.range(command, consistencyLevel, readCoordinator, requestTime)` |
| H19 | `cas(...)`, first statement | `LayoutWrites.checkCas(keyspaceName, cfName, key);` |
| H20 | `truncateBlocking(keyspace, cfname)`, first statement | `LayoutWrites.beforeTruncate(keyspace, cfname);` |
| H21 | `src/java/org/apache/cassandra/tcm/transformations/AlterSchema.java`, `execute`, directly after `newKeyspaces = schemaTransformation.apply(prev);` inside the `try` | `newKeyspaces = LayoutSchemaCoupling.apply(prev, newKeyspaces);` |

Why here and not at the CQL layer:

- **A wrapping `QueryHandler`** sees statements, not bound keys, would have to re-implement paging, aggregation and ordering around rewritten CQL, is a startup-time system property, and can be displaced by any other custom handler.
- **Hooks in `SelectStatement` / `ModificationStatement`** are closer, but still above the pagers (`SinglePartitionPager`, `PartitionRangeQueryPager`, `AggregationQueryPager`), above `ModificationStatement.readRequiredLists` (the read-before-write for list operations, which calls the read path directly), and would need separate edits for `BatchStatement`.
- At `StorageProxy` everything above is unchanged stock code operating on logical keys and logical rows: selection, `writetime()`/`ttl()`, aggregates, `GROUP BY`, `ORDER BY`, `IN`, `LIMIT`, `PER PARTITION LIMIT`, paging, result-set metadata, guardrails, audit logging, authorization. The hook works on `PartitionUpdate` and `ReadCommand` objects, where timestamps, TTLs, collection cells and tombstones are explicit, so nothing is re-derived from CQL.

Fast path: each hook first reads `ClusterMetadata.current().extensions` for the registry; when it is absent or empty the hook returns immediately. With a registry present it does one map lookup by `TableId` per mutation or command. This is new code on the request path and is a recorded exception to ADR-013 (ADR-015).

What bypasses the hooks, and what happens:

| Path | Effect | Handling |
|------|--------|----------|
| `QueryProcessor.executeInternal` and `executeLocally` (node-local reads and writes), consistency `NODE_LOCAL` | Sees the base table only | Not used for user tables by core. Documented: after cutover a node-local read of a split partition is empty or stale |
| Counter writes (`mutateCounter`) | n/a | Counter tables are ineligible |
| Paxos commit, Accord | n/a | LWT on split keys is rejected (H19); Accord tables are ineligible |
| Hints, batchlog replay, read repair, streaming, repair | Operate on physical tables | Correct by construction for physical consistency; late delivery to the base table is the "straggler" case of section 4.6 |
| `sstableloader` / bulk import into the base table for a split key | Data lands in the base table and is invisible | **Not handled in the MVP.** Documented limitation; risk R4 |

### 3.2 Coordinator classes

Coordinator behaviour depends only on the key's phase, grouped into four classes. "Authoritative" is the copy reads use.

| Class | Phases | Reads of K | Writes to K | LWT on K |
|-------|--------|-----------|-------------|----------|
| absent | key not in registry | base | base | allowed |
| C0 | `ANNOUNCED`, `ABORTING` | base | base | rejected |
| C1 | `DUAL_WRITE`, `BACKFILL`, `VERIFY`, `ROLLBACK_DUAL` | base | overflow first, then base | rejected |
| C2 | `CUTOVER` | overflow | base first, then overflow | rejected |
| C3 | `NEW_ONLY`, `CLEANUP`, `STEADY` | overflow | overflow only | rejected |

Rule for dual writes (ADR-017): **the authoritative copy is written last, after the other copy has acknowledged at the client's consistency level.**

### 3.3 Write path (`LayoutWrites.route`)

```
route(mutations, cl, atomic, requestTime, preserve):
    if reentrant (thread-local set by this class): return false
    for up to 3 attempts:
        cm0 = ClusterMetadata.current(); reg = registry(cm0)
        reject with InvalidRequestException any update whose table is an overflow table      // direct client write
        plan = split(mutations, reg)       // per PartitionUpdate on a logical table with a split key
        if plan.nothingRouted: return false                                                   // stock path
        cl' = (cl == ANY) ? ONE : cl                                                          // see below
        execute(plan, cl', atomic)                                                            // table below
        if routingVersions(plan, ClusterMetadata.current()) == routingVersions(plan, cm0): return true
    throw WriteTimeoutException                                                               // outcome unknown to the client
```

`split` maps one logical `PartitionUpdate` for split key K to shadow updates (`RowMapper.toPhysical`):

| Content of the logical update | Shadow updates |
|-------------------------------|----------------|
| Row (insert, update, row delete, cell or collection delete) with clustering C | the same row, cells re-labelled with `Cell.withUpdatedColumn`, in bucket(C); row liveness, row deletion, complex deletions, cell paths, timestamps, TTLs and local deletion times copied bit for bit |
| Range tombstone (`DELETE ... WHERE pk = ? AND c1 > ?`) | the same range tombstone in **every** bucket |
| Partition deletion | the same partition deletion in **every** bucket |
| Static row | cannot occur: tables with static columns are ineligible |

`execute` per class (all calls set the re-entrancy flag; "original" means the caller's `mutations` unchanged, sent through the stock tail of `mutateWithTriggers`):

| Class of K | Caller is not atomic (single statement or unlogged batch) | Caller is atomic (logged batch) |
|------------|-----------------------------------------------------------|---------------------------------|
| C0 | original | original |
| C1 | 1. shadows at `cl'` (`StorageProxy.mutate` if one mutation, else `mutateAtomically`). 2. original | one `mutateAtomically(original + shadows)` |
| C2 | 1. original. 2. shadows as above | one `mutateAtomically(original + shadows)` |
| C3 | original minus the base updates for K, plus shadows: `mutate` if the result is one mutation, else `mutateAtomically` | one `mutateAtomically` of the same set |

Notes:

- A batch that mixes classes (two split keys in different phases) is executed as: all "shadow-first" shadows, then original, then all "shadow-last" shadows. With a logged batch it is one `mutateAtomically`.
- A write that touches several buckets (range or partition delete) always goes through the batchlog, so it cannot stay half-applied across buckets.
- Logged batches keep stock batchlog atomicity (all mutations eventually applied or none recorded) at the price of not ordering the two copies; the consequence is contract item D6 in section 5.
- `ANY` is raised to `ONE` for routed writes: a hint-only acknowledgement gives rule 3.2 nothing to stand on. Client-visible effect: a write at `ANY` to a split key can fail with `Unavailable` where stock would have stored a hint.
- `USING TIMESTAMP`, `USING TTL`, server timestamps: already resolved into cells before the hook. Both copies get identical cells.
- Counters: ineligible. Triggers and views: ineligible, so the stock tail's handling of them is unaffected.
- The re-check after execution ("validate after execute") is what makes phase changes safe; section 4.3.

### 3.4 Read path, single partition (`LayoutReads.read`)

For each command in the group whose table has a layout and whose key K is in class C2 or C3, replace the command by a fan-out; other commands run unchanged. If nothing is replaced return `null`. `SERIAL` and `LOCAL_SERIAL` on a replaced command throw `InvalidRequestException`.

Fan-out for one logical command `cmd` (table T, key K, filter F, limits Lm, `nowInSec`):

1. Buckets: if F is a names filter, the buckets of the named clusterings, each with the names that hash to it; if F is a slice filter, all N buckets with the same slices and the same `isReversed`.
2. Per bucket build `SinglePartitionReadCommand.create(physical, nowInSec, ColumnFilter.all(physical), rowFilter', bucketLimits, physicalKey, F')`. `rowFilter'` re-targets each simple expression to the physical column of the same name (API **unverified**; if a filter cannot be translated, fetch unfiltered and apply `cmd.rowFilter()` on the merged logical rows with `bucketLimits = NONE`).
3. `bucketLimits`: for plain CQL limits, `DataLimits.cqlLimits(min(Lm.count(), Lm.perPartitionCount()))`; for group-by or distinct limits, `DataLimits.NONE`.
4. Execute the bucket commands as one `SinglePartitionReadCommand.Group` through `StorageProxy.read` at the client's consistency level (they target a physical table, so the hook passes them through).
5. Merge the per-bucket `RowIterator`s with a k-way merge on the clustering comparator (reversed if F is reversed). A clustering appears in exactly one bucket, so there are no ties.
6. Present the result as one `RowIterator` whose `metadata()` is T, whose `partitionKey()` is K, and whose rows are re-labelled to T's columns (`RowMapper.toLogical`). Apply `Lm.filter(...)` on top so limits are exact.
7. Re-check the routing version as in 3.3. On change, discard and retry (3 attempts), then `ReadTimeoutException`.

Memory: when `bucketLimits.count() <= fanout_eager_rows` (default 10 000; a paged query is always under this because the pager sets the page size as the limit) the buckets are read eagerly. Otherwise each bucket is read through its own `SinglePartitionPager` in chunks of `fanout_chunk_rows` (1000) and merged lazily; a routing change detected after rows were already returned fails the request with `ReadTimeoutException`.

Per statement shape (everything above `StorageProxy` is stock):

| Shape on a split key | Status | Cost / note |
|----------------------|--------|-------------|
| `SELECT` by full primary key; `IN` on clustering | supported | one sub-read per distinct bucket |
| `SELECT` by partition key only, or with clustering slice | supported | N sub-reads, merged; logical clustering order preserved |
| `ORDER BY` clustering `DESC`, `LIMIT`, `PER PARTITION LIMIT` | supported | limit pushed to each bucket, re-applied after merge; up to N times the rows are fetched |
| `IN` on partition key (mixed split and normal keys) | supported | stock `MultiPartitionPager` and post-query ordering above the hook |
| Paging | supported | The stock pager sits above the hook and re-issues the command with `forPaging`; the paging state is logical (F6). **A paging state issued in any phase is valid in every later phase**, including across cutover and abort |
| Aggregates, `GROUP BY`, `DISTINCT`, `count(*)` | supported | group-by and distinct limits are not pushed down; whole partition is streamed through the lazy merge |
| `writetime()`, `ttl()`, collections, UDTs, subselections | supported | cells are identical |
| Regular-column restrictions with `ALLOW FILTERING` inside the partition | supported, degraded | filter pushed if translatable, else applied on the coordinator |
| `SERIAL` / `LOCAL_SERIAL` read | **rejected** | `InvalidRequestException`; see LWT |
| Static columns | n/a | ineligible |
| Index-backed query | n/a | tables with indexes are ineligible |

Consistency level for a fan-out (question 12): each sub-read runs at the client's level against its own replica set. Per row the guarantee is exactly the stock one (a row lives in one bucket; a `QUORUM` read of that bucket overlaps a `QUORUM` write to it). What differs: the logical read needs **every** touched bucket's replica set to satisfy the level, so availability is lower for slice and full-partition reads; and the result is not a single-partition snapshot (contract item D5).

### 3.5 Read path, range and token scans (`LayoutReads.range`)

Let S be the keys of the command's table in class C2 or C3 that lie inside `command.dataRange().keyRange()`, in ring order. If S is empty return `null` (in C0/C1 the base table still holds the authoritative copy, so the stock scan is correct).

Otherwise return the lazy concatenation of:

- for each gap between consecutive members of S (and before the first, after the last): `command.forSubRange(bounds, ...)` with bounds that **exclude** the split key (`ExcludingBounds` / `IncludingExcludingBounds` in `src/java/org/apache/cassandra/dht/`), executed through `StorageProxy.getRangeSlice`. Excluding K is required because the base table may still contain K's old or stray data;
- for each K in S: the fan-out of 3.4 with the filter `command.dataRange().clusteringIndexFilter(K)` (this carries the paging position when the scan resumes inside K).

`command.limits().filter(...)` is applied over the concatenation; later segments are only executed if the consumer gets that far. The logical token of K decides its position, so `token(pk)` restrictions and token-range scans (for example Spark) are **supported** and return each logical partition once and contiguously. Cost: N extra sub-reads per split key in range. The interaction of `forSubRange` with the first-partition paging filter was read only at signature level (**unverified**; T6.5 has a dedicated test).

### 3.6 LWT, counters, TRUNCATE

- **LWT (H19).** `StorageProxy.cas` on a key in any class other than "absent" throws `InvalidRequestException("Lightweight transactions are not supported on partition <K> of ks.t while it is adapted")`. Reason: Paxos is per physical partition; moving the linearization point between two partitions without a window in which two coordinators run Paxos on different instances needs machinery the MVP does not have, and a CAS cannot be re-executed by "validate after execute". Mitigation: tables with any LWT activity are not proposed (section 6). This is the largest non-transparent point of the design.
- **Counters.** Ineligible.
- **TRUNCATE (H20).** If any key of the table is in a phase other than `STEADY`: `InvalidRequestException` (retry after the migration). Otherwise truncate the overflow table first, then let the stock truncate of the base table proceed. Split keys stay split and empty.

### 3.7 Result metadata and prepared statements

- Result-set metadata, bind-variable metadata and `getPartitionKeyBindVariableIndexes()` come from the statement prepared against the logical table. Nothing changes them.
- Prepared statements are **not invalidated** by phase changes: the routing decision is taken per execution from `ClusterMetadata.current()`, and no schema object of the logical table changes (F3). `QueryProcessor.StatementInvalidatingListener` is not involved. Creating the overflow table produces one ordinary `CREATED TABLE` schema event; it concerns a different table.
- If a client DDL on the logical table is mirrored (section 4.8) the stock invalidation for that table applies, as for any `ALTER TABLE`.

## 4. Migration state machine

### 4.1 Phases

Per key. "Owner work" runs in `ClusterActuator.step`; "replica work" runs in the unconditional data plane (section 4.4).

| Phase | Class | Owner work in this phase | Replica work |
|-------|-------|--------------------------|--------------|
| `ANNOUNCED` | C0 | Capture per-key latency baseline (section 7.3). Wait for all-node ack (4.3), then `cas_drain` (default 15 s) so Paxos rounds started before LWT was rejected have ended | none |
| `DUAL_WRITE` | C1 | Wait for all-node ack | none |
| `BACKFILL` | C1 | Watch `backfillDone`; restart the generation if K's base replica set changed (4.7); enforce the deadline (4.5) | every base replica of K copies its local data (4.5) and commits `ReportBackfill` |
| `VERIFY` | C1 | Equivalence check (4.6). Pass: go to `CUTOVER`. Fail: one more backfill generation, then abort | none |
| `CUTOVER` | C2 | Wait for all-node ack; soak for `cutover_soak` (default 10 min) while checking the stage-A guards (7.3) | none |
| `NEW_ONLY` | C3 | **Commit point.** Wait for all-node ack; stage-B verification window (7.3) | none |
| `CLEANUP` | C3 | Wait `straggler_window`; start a reconcile generation and wait for `backfillDone`; write the base partition tombstone for K at `ALL`; `FinishKey` | reconcile backfill pass (4.6) |
| `STEADY` | C3 | none; claim released with `COMMITTED` | none |
| `ROLLBACK_DUAL` | C1 | Wait for all-node ack, then `ABORTING` | none |
| `ABORTING` | C0 | Wait for all-node ack; delete the N bucket partitions of this incarnation (`mutateAtomically` at `ALL`, best effort); `RemoveKey`; if the layout is now empty, `DROP TABLE` the overflow table; release with `ROLLED_BACK` | none |

### 4.2 Transitions

```
            RegisterKey
 (absent) ------------> ANNOUNCED --> DUAL_WRITE --> BACKFILL --> VERIFY --> CUTOVER --> NEW_ONLY --> CLEANUP --> STEADY
                            |             |             |  ^         |          |       (commit point)
                            |             |             |  +---------+          v
                            |             |             |  (re-backfill)   ROLLBACK_DUAL
                            v             v             v            |          |
                         ABORTING <-------+-------------+------------+----------+
                            |
                            v  RemoveKey
                         (absent)
```

Allowed `(from, to)` pairs, enforced in `AdvanceLayout`: the forward chain; `VERIFY -> BACKFILL`; `{ANNOUNCED, DUAL_WRITE, BACKFILL, VERIFY, ROLLBACK_DUAL} -> ABORTING`; `CUTOVER -> ROLLBACK_DUAL`; `X -> X` with `generation + 1` for `X` in `{BACKFILL, CLEANUP}`. Every transition moves between identical or **adjacent** classes in the order `C0 - C1 - C2 - C3`. There is no transition out of `NEW_ONLY`, `CLEANUP` or `STEADY` except forward.

Why adjacency is enough:

| Coexisting classes | Readers read | Every writer writes | Safe because |
|--------------------|--------------|---------------------|--------------|
| absent/C0 + C1 | base | base | C1 additionally writes overflow |
| C1 + C2 | base or overflow | both | every acknowledged write is in both copies |
| C2 + C3 | overflow | overflow | C2 additionally writes base |
| C0 + C2, C1 + C3, C0 + C3 | | | **unsafe**: a reader's copy is not written by some writer |

### 4.3 Agreement on a phase: all-node epoch acknowledgement plus validate-after-execute

Decision: ADR-016. Two rules, both required.

**Rule O1 (owner).** The owner commits a transition out of phase P only after **every** node in `ClusterMetadata.current().directory` whose state is not `LEFT` has answered a `TCM_CURRENT_EPOCH_REQ` with an epoch `>= KeyState.phaseEpoch` of P. `EpochWatermarks.allAtLeast(epoch)` sends the verb the way `ProgressBarrier.WatermarkRequest` does and is retried every `ack_poll_interval` (5 s). There is no quorum fallback and no timeout that lets the owner proceed. A node that is down or unreachable **stalls** the migration in its current phase; every phase is safe to hold indefinitely.

**Rule C1 (coordinator).** A routed operation captures `ClusterMetadata.current()` once before it starts (`cm0`), executes entirely according to `cm0`, and before returning to its caller compares `TableLayout.version` for the tables it routed in `ClusterMetadata.current()` against `cm0`. If they differ it re-executes under the new metadata. Writes are idempotent (same cells, same timestamps), reads are side-effect free, so re-execution is safe. After 3 attempts it fails with a timeout exception.

Argument. Suppose an operation on node X is acknowledged having executed under phase P of class c: X's metadata showed P both before and after. Node metadata epochs only grow (assumption A1). If, before the operation finished, some phase two transitions later had been committed, then by O1 X would have answered with an epoch at or past the first of those transitions before the operation finished, contradicting what X saw at the end. So **during every acknowledged operation executed under class c, no node has been told a phase more than one transition away.** With the adjacency table above: every acknowledged write is in the copy every concurrent or later reader uses, and every completed read used a copy that all writes acknowledged before it started had reached. This holds without clocks, leases or drain waits, and a coordinator paused arbitrarily long between its checks simply fails the final comparison.

Unacknowledged effects are not covered by the argument and are handled separately: a mutation sent under an old phase can still arrive later (network delay, hint, batchlog replay). Such a mutation targets the base table only, or both tables. Section 4.6 ("stragglers") covers the base-only case; the both-tables case is harmless.

What about question 9, replica-side fencing? Not used. F2 shows the existing schema fence does not stop a retried mutation, and F3 shows that arming it would cost a schema change and a prepared-statement eviction per phase. Rule C1 is an end-to-end check that does not depend on which replicas are stale.

Assumptions (each has a test in T6.6 or T6.13):

- **A1.** A node's `ClusterMetadata.current().epoch` never decreases, including across restart (the log is persisted before it is applied). **Unverified** beyond reading `LocalLog` usage in the overview.
- **A2.** A node that is in the directory and not `LEFT` either answers the watermark request truthfully or is not coordinating client requests with a newer view than it reports.
- **A3.** A node that (re)starts does not serve client requests from an epoch older than one it has already reported. Follows from A1.

Feature-disabled nodes (question 8, overview 6.5): routing (hooks H16 to H21), the watermark handler (stock), and the replica backfill worker are **unconditional**: they run on every node of the fork regardless of `autonomic.enabled`, `dry_run`, pause or kill. Only proposing and owning a migration requires an enabled, armed node. The operator's `nodetool autonomic cluster enable` attestation (overview 6.5) is what guarantees every node has this code. Framework change FC1.

### 4.4 Mapping onto the T1.1 contract

- Actuator `partition_split`, class `PartitionSplitActuator implements ClusterActuator`, `lane() = SCHEMA_LAYOUT`, `scope() = CLUSTER`, `reversible() = false` (it has a commit point), approval `manual` by default.
- `Claim.phase` equals `KeyState.phase`; `Claim.state` holds `{layout: <layoutId>, table: <TableId>, key: <first 16 hex of MD5 of key>, sub: <owner sub-step>}`. The registry is the source of truth; the claim carries ownership and the lease.
- `step(action, claim, ctx)`: one bounded unit: before the first phase, `Schema.submit` of the derived `CREATE TABLE IF NOT EXISTS` and then `RegisterKey`; afterwards "check acks", "check backfillDone", "verify next page range", "check guards", or one `AdvanceLayout`. Long work (verification) is cut into steps under `action_step_timeout`; verification progress is kept in memory only.
- `onPhase(claim, ctx)`: not used for data-plane work, because it only runs on enabled nodes. `LayoutService` registers its own `ChangeListener` instead (FC1).
- `recover(expired, ctx)` on takeover: read `KeyState.phase`. Before the commit point: drive the abort path (consistent with "MVP actuators always undo"). At or after `NEW_ONLY`: resume forward to `STEADY`. Both are safe because every phase is idempotent and its progress is in the registry.
- Kill switch: before the commit point the owner drives the abort path at its next step. After it, the owner releases the claim with final state `COMMITTED_CLEANUP_PENDING` and leaves the key in its C3 phase, which is safe indefinitely; `nodetool autonomic layout resume` or the next armed owner finishes cleanup under a new claim (FC5).
- CMS unavailable: no phase can change; coordinators keep routing by the last committed phase.

### 4.5 Backfill

Decision: ADR-018. **Every base replica of K copies its own local, unfiltered data for K into the overflow table.** The union over replicas contains everything any replica holds, including tombstones, without depending on read repair or on the consistency level clients wrote with.

`BackfillTask` (one per key and generation, on the single `LayoutBackfill` thread of `LayoutService`, created lazily):

```
runs on node X when: key phase in {BACKFILL, CLEANUP}, X is a natural replica of K's token for the keyspace,
                     X not in backfillDone, local resume record is for this generation (else start from the beginning)
loop:
    cmd   = SinglePartitionReadCommand for base, key K, slice (resumeClustering, end], ColumnFilter.all(base)
    batch = first batch of ThrottledUnfilteredIterator(cmd.executeLocally(controller), backfill_chunk_rows)   // unfiltered
    if batch has no rows, no markers, and partition deletion already sent: break
    updates = RowMapper.toPhysical(batch)        // rows by bucket; range tombstones and partition deletion to all buckets
    StorageProxy.mutate(updates, ConsistencyLevel.ALL, ...)   // re-entrancy flag set; retried with backoff until success or phase change
    persist resumeClustering = last clustering of batch in system_autonomic.local_state key 'layout/<layoutId>/<keyhash>/<generation>'
    rate-limit to backfill_throughput (default 1 MiB/s per node)
commit ReportBackfill(logical, K, generation, self)
```

Properties:

| Property | How |
|----------|-----|
| Preserves timestamps, TTLs, expiry times | cells are re-labelled, never rebuilt from values |
| Preserves deletions | the local read is unfiltered: row deletions, range tombstones, partition deletion, complex (collection) deletions and expired-but-unpurged cells are copied. A later write with an old timestamp is shadowed in the overflow table exactly as in the base table |
| Idempotent | same cells and timestamps; last-write-wins makes repeats and RF-fold duplication harmless |
| Resumable | the resume clustering is persisted locally per generation; a restart continues; a new generation starts over |
| Throttled | bytes per second per node; chunk size bounds mutation size (must stay below `max_mutation_size`) |
| Durable everywhere | writes at `ALL`, so a client reading at `ONE` after cutover cannot miss old data that every base replica had |
| No tombstone resurrection | all backfill and reconcile writes for a key must land before `phaseSinceMillis(DUAL_WRITE) + gc_grace_seconds / 2`. The owner enforces `backfill_deadline = min(max_migration_duration, gc_grace / 2 - straggler_window)` and aborts when it is exceeded; tables with `gc_grace_seconds < 2 * (straggler_window + 1h)` are ineligible |

Cost, stated plainly: each of RF replicas rewrites the whole partition, so the cluster writes RF times the partition (times RF again for replication) before compaction removes duplicates. This is the price of not trusting any single replica's view. A cheaper variant (one designated replica after a blocking read of the partition at `ALL`) is a follow-up.

Completion test in the owner: `backfillDone` contains every natural replica of K's token in the current placement, and the placement entry for that token was not modified after `generationEpoch` (`VersionedEndpoints.lastModified()`, used the same way in `AbstractMutationVerbHandler.checkTokenOwnership`). If the placement changed, the owner commits `AdvanceLayout(BACKFILL -> BACKFILL, generation + 1)`.

### 4.6 Equivalence verification, cutover, stragglers, cleanup

**Verification (`VERIFY`, owner).** `LayoutVerifier` reads the logical partition twice with the same `nowInSec`, both at `ALL`, in pages of `verify_page_rows` (default 1000): once from the base table (stock command) and once through the fan-out of 3.4 forced to the overflow table. Both streams are in clustering order; it compares them row by row: clustering, row liveness (timestamp, TTL, expiry), and for each cell path, value, timestamp, TTL and local deletion time. Rows that differ are re-read individually after `verify_recheck_delay` (2 s), up to 3 times, to let in-flight dual writes settle. A row that still differs is a mismatch. Zero mismatches: pass. Any mismatch: one more backfill generation and one more verification; a second failure aborts and raises diagnosis `layout.verify_failed` (CRITICAL) with up to 20 sample clusterings. Verification is read-only. It is throttled by the same rate limit.

What verification cannot see: tombstones (reads are filtered). Tombstone equivalence rests on the unfiltered backfill and on dual writes carrying every delete since `DUAL_WRITE`.

**Cutover.** One `AdvanceLayout(VERIFY -> CUTOVER)`. By the argument of 4.3 some coordinators read base and some overflow for a short time; both copies contain every acknowledged write.

**Commit.** `AdvanceLayout(CUTOVER -> NEW_ONLY)` after the soak passes and, if `commit_approval: manual`, after `nodetool autonomic approve <id>` a second time. From here writes exist only in the overflow table.

**Stragglers.** A base-only mutation sent before all coordinators were dual-writing can reach a base replica after that replica's backfill pass: a hint delivered late, a batchlog replay, a delayed message. If the original write was acknowledged, at least one base replica already had it when its backfill ran, so it was copied. If it was **not** acknowledged (timeout, coordinator crash), its outcome was already unknown to the client. To converge these to "applied" rather than silently dropping them, `CLEANUP` waits `straggler_window` (default: `max_hint_window`, 3 h) and then runs one more full backfill generation (the same code) before deleting. Anything arriving later than that is discarded by cleanup. `straggler_reconcile: none` skips the second pass and discards all stragglers; both are legal outcomes for a write whose outcome was unknown.

**Cleanup.** After the reconcile generation completes: one partition deletion for K on the base table with the current timestamp at `ALL`. Reads never consult the base table for a C3 key, and range scans exclude K explicitly (3.5), so correctness does not depend on this delete; it reclaims space. Cells with client timestamps in the future survive it as invisible garbage. Then `FinishKey`.

### 4.7 Topology changes during a migration

- A claim is only granted with `inProgressSequences.isEmpty()` (FC3) and all directory members alive.
- If a topology change starts later, the owner **holds** the current phase while `inProgressSequences` is non-empty (it keeps renewing the lease). Holding is safe in every phase; routing continues.
- Bootstrap, decommission, move or replace that changes K's base replica set during `BACKFILL` or `CLEANUP`: detected by the completion test (4.5) and answered with a new generation, so every *current* replica runs a full pass over data it holds after streaming finished. A replica that left can hold nothing that was not streamed to its successors, except data stock Cassandra would also lose.
- Changes to the overflow table's replica sets are ordinary range movements of an ordinary table.
- A joining node appears in the directory before it coordinates client requests and must acknowledge like any other node (O1).

### 4.8 Concurrent client DDL (H21)

`LayoutSchemaCoupling.apply(prev, newKeyspaces)` is a pure function run inside `AlterSchema.execute` on every node (like `maybeUpdateConsensusMigrationState`). For each layout in `prev`:

| Change produced by the client's statement | Result |
|-------------------------------------------|--------|
| Logical table dropped (or its keyspace) | Remove the overflow table from `newKeyspaces` too. The registry entry becomes an orphan that routers ignore (its `TableId` is not in the schema); any enabled node commits `RemoveOrphanLayout`; an in-flight migration sees its table gone and releases its claim as `FAILED(TABLE_DROPPED)` |
| Overflow table created, altered or dropped by anything | `InvalidRequestException` (it is managed) |
| Logical table: column added, dropped, renamed, or type altered | MVP: `InvalidRequestException("ks.t has an adapted layout; ...")`. T6.14 replaces this by mirroring `ADD` and `DROP` of regular columns onto the overflow table in the same epoch |
| Logical table: static column, index, trigger, `cdc = true`, masking, `transactional_mode`, or a materialized view on it | `InvalidRequestException` (would make the table ineligible) |
| Logical table: `gc_grace_seconds` | mirrored to the overflow table |
| Logical table: other params (compaction, compression, caching, comment, TTL default) | allowed, base only |
| `ALTER KEYSPACE` replication | allowed; applies to both tables; forces a backfill restart (4.7) |

`AlterSchema.execute` converts `InvalidRequestException` into `Rejected(INVALID, ...)`, which `Schema.submit` rethrows to the client. Because the check runs at commit against `prev`, a DDL racing with `RegisterKey` is ordered by the log: whichever commits first wins, the other is validated against the result (`RegisterKey` re-validates eligibility in `execute`).

### 4.9 Rollback

| From | Path | Loss-free? |
|------|------|-----------|
| `ANNOUNCED` | `ABORTING` -> removed | yes; nothing was written |
| `DUAL_WRITE`, `BACKFILL`, `VERIFY` | `ABORTING` -> removed | yes; base is authoritative and has every acknowledged write. Writes whose shadow succeeded but whose base write did not were never acknowledged and are discarded |
| `CUTOVER` | `ROLLBACK_DUAL` -> `ABORTING` -> removed | yes; in C2 the base copy is written first for every write, so it has every acknowledged write |
| `NEW_ONLY`, `CLEANUP`, `STEADY` | none | **no.** The base copy is stale. Undoing is a *reverse migration*: a new action with the roles swapped (dual write overflow-authoritative, backfill overflow to base, verify, cut back). It is not in the MVP. Its additional problem: the base partition tombstone written by cleanup shadows reverse-backfilled cells with older timestamps until it is purged, so the reverse target cannot be the same base partition before `gc_grace` has passed and compaction has purged it. Open issue O3 |

Each abort step obeys rule O1, so an abort can also stall on an unreachable node; it stalls in a base-authoritative phase.

### 4.10 Invariants

For model checking (T6.15). "Acked" means the coordinator returned success to the client.

| ID | Invariant |
|----|-----------|
| I1 | For every (layout, key) and epoch, exactly one copy is authoritative: base in C0/C1, overflow in C2/C3 |
| I2 | If any node has been told a phase of class c, every non-`LEFT` node has been told a phase of class at least c-1 on the path taken (from O1) |
| I3 | An operation is acked only if its coordinator's routing version was the same before and after its execution (C1) |
| I4 | From I2 and I3: the classes under which any two overlapping acked operations executed are equal or adjacent |
| I5 | Every acked write to K is stored at its consistency level in every copy that is authoritative for any acked read that starts after it |
| I6 | In C1, any routed non-atomic write that reached the base table was first acknowledged at the client's level by the overflow table; in C2 the converse |
| I7 | For any cell present in both copies: value, timestamp, TTL, local deletion time and cell path are identical |
| I8 | At the commit of `VERIFY`, every cell and tombstone that any current base replica of K held when its backfill pass started is stored on all overflow replicas |
| I9 | No backfill or reconcile write for K is issued after `DUAL_WRITE` start plus `gc_grace / 2` (no tombstone written by a dual write can have been purged before older data arrives) |
| I10 | After `NEW_ONLY` is committed, no acked read of K uses the base copy, and no transition returns to C0, C1 or C2 |
| I11 | Only the claim owner at the expected step can change a key's phase; a stale owner's commit is rejected |
| I12 | A layout's overflow table exists with the derived shape at every epoch at which the layout has keys and its logical table exists |
| I13 | An incarnation number is used by at most one migration attempt of one key |
| I14 | A logical read result contains each live logical row exactly once, in logical clustering order, with logical metadata |
| I15 | Registry size stays within the bounds of 2.3 |

Liveness is deliberately weaker: a migration makes progress only while every node is reachable and the CMS is available; otherwise it holds.

### 4.11 Failure scenarios

| Scenario | What happens |
|----------|--------------|
| **Coordinator crashes between the two writes** (C1: shadow written, base not) | The client got no acknowledgement. Reads (base) do not see the write. If the migration completes, the write becomes visible at cutover, as a timed-out write may in stock Cassandra when a hint arrives. If it aborts, the write is discarded. In C2 (base written, overflow not): invisible; discarded at cleanup or visible after a rollback. Never acked, never lost. For a logged batch the batchlog replays both copies |
| **Owner crashes mid-backfill** | Replica backfill tasks keep running; they are driven by the registry, not by the owner. The lease expires; a new owner claims the lane with `RECOVERING`, reads `BACKFILL`, and drives the abort path. Coordinators stayed in C1 throughout; nothing is lost. (Resuming instead of aborting would also be safe; aborting is the MVP rule.) |
| **Stale coordinator writes with an old layout after cutover** | By O1 it cannot be more than one class behind while others advance, unless it is unreachable, in which case the owner is not advancing. A coordinator one class behind writes both copies (C1 vs C2) or still writes the overflow table (C2 vs C3). A coordinator paused for a long time inside one request finishes its sends, then fails the version comparison (C1) and re-executes under the current phase before acknowledging; if it crashes instead, the write was never acknowledged and is a straggler at worst |
| **Concurrent client DDL on the logical table** | Decided at commit by H21 against the metadata of that epoch: dropped tables take the overflow table with them; column changes are rejected (MVP) or mirrored atomically; nothing can leave the two tables with different columns |
| **Node bootstrap or decommission during migration** | The owner holds while a sequence is in progress. If K's base replica set changed, the backfill generation restarts. Coordinators keep their class |
| **Delete racing with backfill** | Backfill reads row r (timestamp 50) on replica X; a client deletes r (timestamp 100) through a C1 coordinator, which writes the tombstone to the overflow bucket and to base; the backfill then writes r (50) to the overflow bucket. The tombstone (100) wins on every overflow replica regardless of arrival order. It cannot have been purged, by I9 |
| A base replica is down during `BACKFILL` | Other replicas' writes at `ALL` to overflow do not depend on it, but it cannot report, and O1 cannot be satisfied either. The migration holds; at `backfill_deadline` it aborts |
| An overflow replica is down | Backfill writes at `ALL` retry until it returns or the deadline aborts the migration. Client dual writes follow the client's consistency level as usual |
| Network partition isolates a coordinator with clients | It cannot acknowledge the watermark, so the owner holds. It keeps serving under its current class, which is adjacent to everyone else's |
| Late hint or batchlog replay of a pre-migration base write | Straggler; reconciled in `CLEANUP` or discarded (4.6) |
| CMS unavailable | No phase changes. Routing continues from local metadata |
| Layer killed or disabled on all nodes | Routing and backfill continue (unconditional). Phase stays where it is until an owner exists again |

## 5. Compatibility contract for old-schema clients

Proposed default (ADR-020, status Proposed until the owner accepts). "Adapted partition" means a partition key present in a layout; all other partitions and tables behave exactly as stock.

**Always preserved (P)**

| ID | Guarantee |
|----|-----------|
| P1 | **CQL surface and metadata.** The logical table's definition in `system_schema` and `DESCRIBE`, result-set and bind metadata, and prepared statement ids are unchanged by a migration. No re-prepare is forced by phase changes |
| P2 | **Results.** Every supported read returns the same rows, values, `writetime()`, `ttl()` and ordering it would return from the original table for the same acknowledged history |
| P3 | **Acknowledged writes are durable** at their consistency level in every phase and across abort. No acknowledged write is lost; no deleted data is resurrected |
| P4 | **Per-row consistency levels** mean what they meant: a read at level R overlaps a write at level W for the same row as in stock Cassandra |
| P5 | **Paging states** issued before or during a migration remain valid afterwards |
| P6 | **Timestamps and TTLs** supplied by the client or the server are stored unchanged |
| P7 | **Authorization** is evaluated on the logical table exactly as before |

**May degrade (D)**

| ID | What | Bound |
|----|------|-------|
| D1 | **Latency of writes** to an adapted partition: two sequential writes during dual-write phases; a batchlog round trip for range and partition deletes permanently | dual-write: from `DUAL_WRITE` to `NEW_ONLY`, at most `max_migration_duration` (default 24 h); deletes: permanent |
| D2 | **Latency and cost of reads** that do not name full primary keys on an adapted partition: N sub-reads instead of one | permanent while adapted |
| D3 | **Availability of fan-out reads and multi-bucket deletes**: they need every touched bucket's replica set to meet the consistency level | permanent while adapted |
| D4 | **Transient errors**: an operation that overlaps a phase change may be retried internally and, rarely, fail with a timeout; a write at `ANY` behaves as `ONE` | per phase change (a handful per migration); `ANY`: permanent while adapted |
| D5 | **Isolation of multi-row writes within one partition**: stock applies them atomically and in isolation on each replica; on an adapted partition rows in different buckets are applied by a logged batch (all eventually applied, but a concurrent read can see some and not others) | permanent while adapted |
| D6 | **Writes whose outcome the client never learned** (timeout, coordinator failure) during a migration: may become visible only at cutover, may be discarded on abort or cleanup, and, for a logged batch issued around cutover, may be seen, then not seen, then seen again until the batchlog replays | visibility settles by the end of `CLEANUP`; the logged-batch flicker lasts at most the batchlog replay delay |
| D7 | **DDL on a table that has a layout**: column changes rejected (MVP) | until T6.14; then only changes that make the table ineligible are rejected, permanently while adapted |

**Not preserved (N)**: the design cannot make these transparent

| ID | What | Behaviour | Mitigation |
|----|------|-----------|-----------|
| N1 | **Lightweight transactions and `SERIAL` reads on an adapted partition** | rejected with `InvalidRequestException` from `ANNOUNCED` on, permanently while adapted | tables with any LWT activity are never proposed (section 6); a client that starts using LWT on an adapted partition later gets errors until a reverse migration exists |
| N2 | **`TRUNCATE` during a migration** | rejected until the migration ends | retry |
| N3 | **Node-local access** (`NODE_LOCAL`, internal queries), `nodetool getendpoints`, sstable tools, bulk load into the base table | see the physical layout | documented |
| N4 | **An extra table** `ks.<t>_al_<hex>` is visible in `system_schema.tables` and `DESCRIBE KEYSPACE` | cosmetic for drivers; operators must back it up with the logical table | comment on the table; virtual table `autonomic_layouts` |

"Eventually satisfied" is therefore interpreted as: **results and durability are never compromised; latency, availability of some query shapes, and isolation may degrade within the stated bounds; LWT on the adapted partition is the one capability that is withdrawn.**

Where the design changes if the owner tightens the contract:

| Tightening | Consequence |
|-----------|-------------|
| "No capability may be withdrawn" (N1) | LWT must be supported: Paxos on the bucket partition in C3 for single-bucket conditions, plus a quiescing cutover for Paxos state, or restricting the feature to Accord. Large; until then any table that *might* use LWT must be excluded by operator allow-list rather than by observation |
| "No latency degradation for existing query shapes" (D2) | `HASH_SPLIT` is only allowed when the observed fan-out fraction is zero, or `RANGE_SPLIT` must be built first |
| "Unknown-outcome writes must never flicker" (D6) | Replace coordinator dual writes by a replica-side forwarder in the base table's write path (as materialized views do in `Keyspace.apply`), a core change in the storage engine, or route all dual writes through the batchlog and add a read-both phase |
| "Single-partition isolation must hold" (D5) | Only tables whose writes are single-row may be adapted (cannot be known in advance), or multi-row writes must be made Accord transactions |
| "No extra visible table" (N4) | Hooks in `SchemaKeyspace` and `DescribeStatement` to hide managed tables; backup tooling then needs another way to find them |
| "DDL must always work" (D7) | T6.14 becomes part of the MVP, and index/view/CDC creation must trigger a reverse migration first |
| "Migration must proceed with a node down" | O1 must be weakened to a quorum plus replica-side fencing with a new check in `AbstractMutationVerbHandler` and `ReadCommandVerbHandler`; safety at consistency level `ONE` is then no longer absolute |

### 5.1 Client-visible metadata, routing, authorization

- **`system_schema`, `DESCRIBE`**: logical table unchanged; overflow table visible as an ordinary table with an explanatory comment. Driver behaviour on seeing it is ordinary; nothing driver-specific was tested (**unverified**).
- **Token-aware routing**: drivers hash the logical partition key and send the request to a replica of the *base* partition. That node is always a valid coordinator. For an adapted partition it is no longer a replica of the data, so each request has one more network hop, and **coordinator load for the hot key stays concentrated on the base replicas** (shape H in section 1). Storage and replica read/write load is what gets spread. A client that wants to avoid this must disable token-awareness for the statement; the server cannot redirect it.
- **Authorization**: the statement is authorized against the logical table before it reaches the hooks; fan-out and shadow operations are internal and carry no further checks. Direct access to the overflow table: client **writes** are rejected by H16 regardless of permissions; client **reads** follow ordinary RBAC on that table (a role with keyspace-level `SELECT` can read it and sees the same data it could read logically; a role with only table-level grants on the logical table cannot). Tables with masked columns are ineligible, because the overflow table would expose unmasked values to keyspace-level readers. DDL on the overflow table is rejected by H21.

## 6. Feature interaction matrix

Eligibility is checked in three places: `Eligibility.structural(TableMetadata, KeyspaceMetadata)` (pure; called by the policy, by `Actuator.validate`, and inside `RegisterKey.execute` so it is atomic with schema), `Eligibility.runtime(...)` on the proposing node, and H21 afterwards.

| Feature | Verdict | How it is checked / handled |
|---------|---------|-----------------------------|
| LWT / Paxos | **blocks** proposal; rejected on adapted keys | runtime: `TableMetrics.casPrepare`, `casPropose`, `casCommit` latency counts must be zero since start on the proposer, and the shape profile must show no `cas` call (section 7.2). Not provable for the future (N1) |
| Accord tables | **blocks** | structural: `TableMetadata.requiresAccordSupport()`, or `params.transactionalMode` / `transactionalMigrationFrom` not off |
| Counters | **blocks** | `TableMetadata.isCounter()` |
| Secondary indexes, SAI | **blocks** | `!table.indexes.isEmpty()`; H21 rejects later `CREATE INDEX` |
| Materialized views | **blocks** | any view in the keyspace with this base table; `isView()`; H21 rejects later `CREATE MATERIALIZED VIEW` |
| CDC | **blocks** | `params.cdc`; after a split, writes to the key would no longer appear in the logical table's CDC stream |
| Triggers | **blocks** | `!table.triggers.isEmpty()` |
| Static columns | **blocks** | `hasStaticColumns()` |
| Dynamic data masking | **blocks** | `hasMaskedColumns()` (section 5.1) |
| Compact storage, virtual, system keyspaces | **blocks** | `isCompactTable()`, `isVirtual()`, `SchemaConstants.isSystemKeyspace` |
| No clustering columns | **blocks** | nothing to split |
| Transient replication | **blocks** | `mutateAtomically` asserts against it |
| Column named `__al_bucket`, partition key over 1024 bytes | **blocks** | structural / bounds |
| `gc_grace_seconds` too small | **blocks** | section 4.5 |
| TTL (per write and table default) | works unchanged | cells carry TTL and expiry |
| Collections, UDTs, frozen types, vectors | works unchanged | cell-level copy with paths and complex deletions. List operations that read before write go through H17 |
| Client timestamps | works unchanged | |
| Logged / unlogged batches | handled | section 3.3 |
| Repair (full, incremental, auto-repair) | works unchanged | two ordinary tables |
| Streaming, bootstrap, decommission, replace | handled | section 4.7 |
| Hints, read repair, speculative retry, short-read protection | works unchanged per physical partition | late hints: section 4.6 |
| Snapshots, backups | works, with an operator obligation | both tables are in the keyspace snapshot; a table-level snapshot of the logical table alone misses adapted partitions; restoring into a cluster without the registry leaves adapted partitions in the overflow table only (risk R5) |
| `nodetool cleanup`, compaction, scrub | works unchanged | |
| Compaction strategies | works unchanged | overflow table copies the base table's compaction params. Window placement of backfilled data under TWCS is **unverified** |
| Row cache, key cache | works unchanged per physical table | |
| Partition denylist | works unchanged | checked on the logical key before the hooks |
| Guardrails, audit log, full query log, tracing | works unchanged | statement-level, logical |
| Bulk load (`sstableloader`) into the base table | **not handled** | risk R4 |

## 7. Triggering and verification

### 7.1 Diagnosis

Input: the `hot_partition` diagnosis of T3.2 (subject `PARTITION:ks.t:<key>`), which exists only for tables above the sampling rate threshold. `PartitionSplitPolicy` handles it when it has been open for `layout.for_evaluations` (default 20, about 5 minutes) on the preferred proposer. The policy needs the raw key bytes; the diagnosis must carry them as evidence `key_hex` (FC6).

### 7.2 Qualification before proposing

In order; the first failure ends with no plan and an INFO annotation on the diagnosis (`layout=not_applicable(<reason>)`) so the operator can see why.

1. `Eligibility.structural` and `Eligibility.runtime` (section 6).
2. **Row-count probe.** A coordinator read of the key selecting only clustering columns with `LIMIT min_rows_for_split` (default 10 000) at `ONE`. Fewer rows: shape C, stop. This bounds the probe's cost regardless of partition size.
3. **Shape profile.** `LayoutProfiler.watch(tableId, key, profile_window)` (default 5 min) makes the hooks on *this node* count, for that key: named-row reads, slice reads, row writes, range and partition deletes, `cas` calls, and range scans on the table, and record read and write latency. Node-local, in memory, no TCM. Let `p` be the fraction of operations that touch a single bucket (named-row reads and row writes).
4. **Benefit estimate.** With N buckets the busiest bucket's replica set sees about `p/N + (1 - p)` of the key's operations (each on a partition N times smaller), and total sub-operations become `p + (1 - p) * N` times the original. Require `p >= min_point_op_fraction` (default 0.8) and no `cas`. Choose `N = clamp(nextPowerOfTwo(ceil(share / target_share)), 4, 32)` where `share` is the key's share of table operations and `target_share` defaults to 0.05.
5. **Spread precondition.** If the number of nodes in every datacenter is not greater than its replication factor, every bucket lives on the same nodes: the split bounds partition size but moves no load. Proceed only if the row-count probe hit its limit *and* `READ_ROW_COUNT` or `TopPartitionTracker` evidence marks the partition as large; record `expected_benefit=partition_size_only`.
6. **Cost estimate.** Partition bytes are estimated from `TopPartitionTracker` when it has an entry for the key; otherwise as the mean encoded row size seen by the probe times a row count obtained by continuing the probe as a paged, throttled `count` at `ONE` up to `max_probe_rows` (default 5 000 000; beyond it the partition is treated as too large to estimate and not proposed automatically). The value is recorded as an estimate. Estimate times RF, divided by `backfill_throughput`, must fit in `backfill_deadline`. Each base and overflow replica needs free disk of at least twice the estimate.
7. **Headroom.** No open CRITICAL `pool_saturation`, `compaction_backlog`, `disk_pressure` or `gc_pressure` diagnosis on the proposer; all directory members alive; `inProgressSequences` empty; registry bounds not reached; no cooldown for this key (doubling after each abort, overview 7.1).

The plan carries the profile, `p`, N and the estimates in `Plan.rationale` and `Action.params`, so the approving operator sees the expected benefit and cost.

### 7.3 Verification signal and window

Signals come from the router itself (`LayoutSensor`, fed by `LayoutProfiler`, which stays on for the key for the whole migration): `layout.key_read_latency_micros`, `layout.key_write_latency_micros`, `layout.key_error_rate`, and from the samplers `partition.ops_share` for the base key and for each bucket partition of the overflow table.

| Stage | When | Baseline | Rule | On failure |
|-------|------|----------|------|-----------|
| Baseline | `ANNOUNCED`, for `baseline_window` (default 3 min) | | need `min_samples` | abort (`INSUFFICIENT_BASELINE`) |
| Guard during C1 | `DUAL_WRITE` to `VERIFY`, every control tick | baseline | error rate not above baseline + 1%; table-level coordinator write latency not above 3x baseline; no new CRITICAL saturation diagnosis | the owner aborts (loss-free). There is no cluster-wide backfill pause in the MVP |
| Stage A (reversible) | `CUTOVER`, for `cutover_soak` (10 min) | baseline | key read latency mean not above `baseline * (1 + read_regression_tolerance)` (default 1.0, that is 2x; fan-out reads are expected to cost more); error rate as above | `ROLLBACK_DUAL` -> abort: loss-free |
| Stage B (after commit) | `NEW_ONLY`, after `settle` (2 min), for `verify_window` (10 min) | baseline | **goal**: max over base key and buckets of `partition.ops_share` reduced by at least `min_improvement` (default 0.5), or, for `partition_size_only`, bucket row counts within 2x of `rows / N`. **Guards**: key write latency not above baseline + 25%; key read latency within stage-A tolerance | verdict recorded; the key stays split (no loss-free undo). `NEUTRAL` or `REGRESSED` raises `layout.split_not_effective` (WARN), counts as a rollback for the policy's quarantine, and is the trigger for a reverse migration once that exists |

The C1 guard row is deliberately simple: in the MVP the only response to a guard breach before cutover is to abort. Backfill load is excluded from every baseline because the baseline is taken before `DUAL_WRITE`, and from stage B because backfill has finished by then.

This two-stage shape does not fit rule 7 of overview 7.3 ("only IMPROVED commits") after the commit point; see FC4.

### 7.4 When the system decides not to migrate

Small hot partition (shape C); fan-out fraction above 20% (shape G); any LWT activity; any blocking feature; cluster not larger than RF and partition not demonstrably large; estimated backfill longer than the deadline or not enough disk; system already at CRITICAL saturation (the migration would add load; wait for D4/D5 actions to act first); a node down or a topology change in progress; registry bounds reached; cooldown after a previous abort of the same key; `dry_run`, actuator disabled, cluster actions not enabled, or approval not given.

## 8. MVP cut

Smallest end-to-end slice that shows the owner's sentence truthfully:

- One transformation (`HASH_SPLIT`), one key per action, one action at a time, eligibility of section 6 in full (blocking is cheap and is what keeps the slice honest).
- Hooks H16 to H21. Reads: single-partition (all shapes of 3.4) and range scans (3.5; required for correctness, not optional). Writes: all of 3.3. LWT and `TRUNCATE` guards.
- All forward phases, abort from every phase before the commit point, `recover`, kill.
- Replica backfill with resume; full verification; straggler reconcile; cleanup.
- Trigger: `nodetool autonomic layout split <ks> <table> <key-hex> [--buckets N]` (creates a proposed action; still passes the governor and approval), and the policy path driven by `InjectedSensor` in tests.
- H21 in its rejecting form.

Demonstration dtest (`PartitionSplitEndToEndTest`, 3 nodes, RF 2, so buckets land on different node pairs; fallback 2 nodes RF 1 if three instances do not fit the 1024m fork, overview R9): a table `((id), seq) -> v`; one key with 5000 rows; a background thread issues point writes, point reads, slices, a paged full-partition read and a full-table scan through different coordinators and checks every result against an in-memory model; the test injects the diagnosis, approves the action, and asserts: every phase appears in `system_views.autonomic_layouts`; no model mismatch at any time; a paging state taken before `CUTOVER` continues correctly after `STEADY`; afterwards the base partition is empty, the overflow table has N non-empty partitions, and their replicas are not all the same nodes. Phase waits are configured to hundreds of milliseconds.

Deferred: mirroring DDL (T6.14), reverse migration, `RANGE_SPLIT`, whole-table re-bucketing, static columns, LWT support, designated-replica backfill, bulk-load reconciliation, hiding the overflow table, automatic selection of `hashedClusteringColumns`, multiple keys per action.

## 9. Refined D6 task list

Replaces the D6 outline of overview section 13. New code is under `src/java/org/apache/cassandra/autonomic/layout/` (abbreviated `layout/`); unit tests under `test/unit/org/apache/cassandra/autonomic/layout/`; dtests under `test/distributed/org/apache/cassandra/distributed/test/autonomic/layout/`. Tiers as in the overview.

| ID | Description | Files | Depends | Acceptance and tests | Tier |
|----|-------------|-------|---------|----------------------|------|
| T6.1 | Registry: `LayoutRegistry`, `TableLayout`, `KeyState`, `Phase`, `LayoutRegistryValue`, the six transformations of 2.4 with serializers and the transition table, registration in `AutonomicTransformations.register()`; virtual table `system_views.autonomic_layouts` (one row per layout and key: phase, epoch, generation, backfill_done, buckets, physical table, orphan flag) | `layout/*`, `autonomic/coord/AutonomicTransformations.java`, `autonomic/knowledge/AutonomicVirtualTables.java` | T2.8 (with FC2) | unit: every accept and reject case of each transformation against a hand-built `ClusterMetadata`; illegal transitions rejected; bounds rejected; serializer round trip; claim and registry change together; value never mutated in place. dtest 2 nodes: restart and `nodetool cms snapshot` with a non-empty registry | sonnet |
| T6.2 | `PhysicalTables.derive` / `matches` (CQL generation), `Eligibility.structural`, `BucketFunction` (frozen hash), `RowMapper.toPhysical` / `toLogical` for rows, range tombstones, partition deletions, complex deletions | `layout/PhysicalTables.java`, `Eligibility.java`, `BucketFunction.java`, `RowMapper.java` | none | unit (`CQLTester`): derived table parses and matches for composite keys, reversed clustering, collections, UDTs; each blocking feature is reported; hash values pinned by golden vectors; `toLogical(toPhysical(u))` equals `u` cell for cell including timestamps, TTLs and deletions; a range tombstone appears in all buckets | sonnet |
| T6.3 | Write routing: `LayoutWrites.route`, `checkCas`, `beforeTruncate`; class table of 3.3; re-entrancy flag; validate-after loop; overflow-table write guard | `layout/LayoutWrites.java`; **H16, H19, H20** in `src/java/org/apache/cassandra/service/StorageProxy.java` | T6.1, T6.2 | unit with a registry injected into test metadata: per class, the exact sequence of `StorageProxy` calls for single statement, unlogged batch, logged batch, range delete, partition delete; `ANY` raised to `ONE`; routing change between attempts causes re-execution; three changes cause `WriteTimeoutException`; with an empty registry the hook returns after one lookup (asserted by a counter). dtest 2 nodes with a hand-committed registry in each class: data lands where the table says; LWT and direct overflow writes are rejected | fable |
| T6.4 | Single-partition read routing: `LayoutReads.read`, fan-out, k-way merge, logical re-labelling, limits, eager and lazy modes. First step: confirm the call chain of F5 and record it in the class javadoc | `layout/LayoutReads.java`, `FanOutIterator.java`; **H17** | T6.1, T6.2 | dtest 2 nodes, key forced to C3 with data placed by T6.3: every row of the table in 3.4 compared against the same data in an unsplit twin table, including `ORDER BY DESC`, `LIMIT`, `PER PARTITION LIMIT`, `IN`, paging with page sizes 1, 7 and 5000, `count(*)`, `GROUP BY`, `writetime`/`ttl`, list index update; `SERIAL` rejected; a paging state taken in C1 resumes in C3 | fable |
| T6.5 | Range read routing: `LayoutReads.range` (3.5) | `layout/LayoutReads.java`; **H18** | T6.4 | dtest: full scan and `token()`-bounded scans over a table with two split keys and stray rows left in the base table return each logical partition once; paging with a page boundary inside a split partition, at its first row and at its last row; `LIMIT` across segments | fable |
| T6.6 | Data plane service: `LayoutService` (started unconditionally from `AutonomicService.start()`, FC1), its `ChangeListener`, `EpochWatermarks.allAtLeast`, lazily created `LayoutBackfill` thread | `layout/LayoutService.java`, `EpochWatermarks.java`, `autonomic/AutonomicService.java` | T6.1 | unit: listener reacts to registry changes only; no thread exists while the registry is empty. dtest 3 nodes: watermark is false while one node is isolated from the log by a message filter and true after; a node with `autonomic.enabled: false` answers and routes; restart never reports a lower epoch (A1) | sonnet |
| T6.7 | Replica backfill: `BackfillTask` (4.5) with local unfiltered read, chunking, `ALL` writes with retry, throttle, local resume record, `ReportBackfill` | `layout/BackfillTask.java` | T6.2, T6.6, T2.3 | dtest 3 nodes RF 3: partition with live rows, TTL rows, row tombstones, a range tombstone, a collection overwrite, with one replica deliberately missing a delete; after backfill the overflow data is cell-identical (compare `writetime`, `ttl`) and a late write older than the tombstone stays dead; restart mid-backfill resumes; an overflow replica down stalls and recovers; throttle observed | fable |
| T6.8 | `LayoutVerifier` (4.6): paged compare, recheck, mismatch report | `layout/LayoutVerifier.java` | T6.4 | unit: equal streams pass; a differing value, timestamp, TTL, a missing and an extra row are each reported; a row changed between first read and recheck passes. dtest: corrupt one overflow row, verification fails with that clustering | sonnet |
| T6.9 | `PartitionSplitActuator`: `validate`, `step` for every phase of 4.1, abort path, `recover`, kill behaviour, deadlines, topology hold and generation restart, base cleanup; `nodetool autonomic layout status\|split\|abort\|resume` and MBean methods; help fixtures | `autonomic/execute/actuators/PartitionSplitActuator.java`, `src/java/org/apache/cassandra/tools/nodetool/Autonomic.java`, `AutonomicServiceMBean.java`, `test/resources/nodetool/help/` | T6.3 to T6.8, T2.8 (FC2 to FC5) | unit with fake watermark, fake backfill reports and hand-built metadata: every transition; no advance without full acknowledgement; abort from each pre-commit phase; no abort after commit; takeover in each phase; deadline abort. dtest: the demonstration test of section 8 | fable |
| T6.10 | DDL coupling: `LayoutSchemaCoupling.apply` in rejecting form (4.8), orphan removal | `layout/LayoutSchemaCoupling.java`; **H21** in `src/java/org/apache/cassandra/tcm/transformations/AlterSchema.java` | T6.1, T6.2 | unit: each row of the table in 4.8 against hand-built keyspaces; purity (same input, same output, no clock). dtest: `DROP TABLE` of the logical table during `BACKFILL` drops both and the action ends `FAILED(TABLE_DROPPED)`; `ALTER TABLE ADD` is rejected with the documented message; DDL on unrelated tables is unaffected and replays after restart | sonnet |
| T6.11 | Triggering: `LayoutProfiler`, `LayoutSensor`, `Eligibility.runtime`, `PartitionSplitPolicy` with qualification 7.2, stage A/B evaluation 7.3, diagnoses `layout.verify_failed`, `layout.split_not_effective`; `key_hex` evidence in `hot_partition` (FC6) | `layout/LayoutProfiler.java`, `autonomic/monitor/sensors/LayoutSensor.java`, `autonomic/plan/policies/PartitionSplitPolicy.java`, T3.2 detector | T3.2, T6.3, T6.4, T6.9 | unit: one fixture per "do not migrate" reason of 7.4 yields no plan and the stated annotation; N selection; stage A and B verdict tables. dtest 1 node: profiler counts shapes for a watched key and costs nothing when not watching | sonnet |
| T6.12 | Compatibility suite: a parameterized dtest that runs a fixed catalogue of statements (every row of 3.3, 3.4, 3.5, 3.6) against a model **while the key is held in each phase in turn**, and across each transition | dtests | T6.9 | every statement returns the model's result in every phase, or the documented rejection; results are identical between a coordinator one class behind (held back with a message filter on log delivery) and one that is current | sonnet |
| T6.13 | Failure suite: the scenarios of 4.11, each as a dtest using message filters and instance shutdown: coordinator killed between the two writes (both orders); owner killed in `BACKFILL` and in `CLEANUP`; coordinator held at an old epoch across a phase change (owner must not advance; after release, validate-after re-executes); delete racing with backfill; decommission during `BACKFILL`; late hint after backfill (reconciled); CMS unavailable | dtests | T6.9, T6.10 | invariants I5, I7, I10, I14 asserted through a model in each test; no test relies on sleeps longer than configured windows | fable |
| T6.14 | Mirror `ALTER TABLE ADD` / `DROP` of regular columns onto the overflow table inside H21; router tolerance for the epoch in which columns change | `layout/LayoutSchemaCoupling.java`, `RowMapper.java` | T6.10, T6.12 | dtest: add and drop a column on an adapted table in `STEADY` and during `BACKFILL`; both tables change in one epoch; writes and reads of the new column work on adapted and normal partitions | sonnet |
| T6.15 | TLA+ model of sections 4.1 to 4.3: owner, N coordinators with independent epochs, two copies, acked and unacked writes; check I1, I4, I5, I10 and that removing O1 or C1 produces a counterexample | `docs/specs/layout-migration.tla` | none (can start with T6.1) | model checks with 3 coordinators and 2 writes; both negative variants fail | fable |
| T6.16 | Operator documentation: the contract of section 5, limitations N1 to N4, backup obligation, `nodetool autonomic layout`, how to read `autonomic_layouts` | `docs/` | T6.9 | each command executed against a running node | haiku |
| T6.17 | **Reverse migration** (follow-up after D6; ADR-024 as amended by ADR-025): a `partition_unsplit` cluster actuator that returns an adapted key in `STEADY` to the base table, with the roles of the two copies swapped (dual write with overflow authoritative, backfill overflow to base, verify, cut back, clean up the buckets, `RemoveKey`). First step is a design note, appended to this document and recorded as an ADR, that resolves open issue O3 (the base partition tombstone written by forward cleanup shadows reverse-backfilled cells with older timestamps); candidate approaches: skip the base tombstone in forward cleanup and rely on range-scan exclusion plus a purge at reverse time, or wait out `gc_grace_seconds` plus a forced purge before reverse backfill. Triggered by `nodetool autonomic layout unsplit` and proposed automatically on `layout.split_not_effective`, under manual approval | `layout/*`, `autonomic/execute/actuators/PartitionUnsplitActuator.java`, phase and transition additions in `layout/Phase.java` and the registry transformations | T6.9, T6.12, T6.13, T6.15 (model extended to the reverse path) | design note reviewed before code; unit: every reverse transition and abort; dtest: split a key, write through it, unsplit it, and compare every statement of the T6.12 catalogue against the model in each reverse phase; data written before the split, during it and after it is all present with original timestamps after the unsplit; a delete issued while split stays deleted | fable |

Order: T6.1, T6.2, T6.15 in parallel; then T6.3, T6.4, T6.6, T6.10; then T6.5, T6.7, T6.8; then T6.9; then T6.11, T6.12, T6.13; T6.14 and T6.16 after those; T6.17 is the first follow-up after D6 and is not required for the D6 tag (ADR-025); T6.9 must choose its base-cleanup method with open issue O3 in mind. The `layout:` configuration also gains `excluded_tables` (ADR-024, decision 1).

### 9.1 Required changes to the T1.1 framework

Not applied to `overview.md` here; each needs to be folded into the framework design or its tasks.

| ID | Change | Affects |
|----|--------|---------|
| FC1 | `AutonomicService.start()` must start `LayoutService` **even when `autonomic.enabled` is false** (register one `ChangeListener`; no threads until the registry is non-empty), and `shutdownBlocking()` must stop it. The statement "if `autonomic.enabled` is false ... no listeners" in overview 3.6 and the T2.1 acceptance ("zero `Autonomic*` threads") remain true for threads; the listener is the exception. Overview 6.5 "never run `onPhase` work" needs the qualification that the layout data plane is not `onPhase` work | overview 3.6, 6.5; T2.1 |
| FC2 | The ledger must expose pure helpers (`ClusterLedger.withAdvancedClaim(actionId, owner, expectedStep, phase, state, nowMillis, leaseMillis)` returning a result or a rejection reason) so that D6 transformations can advance the claim and the registry in one commit; `ClusterCoordinator` must let a `ClusterActuator` step return "commit this transformation instead of `AdvanceAction`" | overview 6.2, 6.4; T2.8 |
| FC3 | `ClaimAction` for lane `SCHEMA_LAYOUT` must also require `prev.inProgressSequences.isEmpty()`; governor check `NODE_STATE` for this lane must require every directory member alive | overview 6.2, 7.1; T2.6, T2.8 |
| FC4 | The verify contract (overview 7.3 rules 7 and 9) assumes one verification followed by commit or rollback, with a dead-man rollback. A layout migration verifies in two stages around a commit point and cannot be rolled back by a timer. Needed: a cluster actuator may own its verification inside `step` and return terminal `COMMITTED` with a recorded verdict or `ROLLED_BACK`; the dead-man deadline does not apply to cluster actuators (lease expiry and `recover` take its role); rule 10's "only at severity CRITICAL" should not apply when approval is manual | overview 7.2, 7.3; T2.6 |
| FC5 | Kill and restart recovery for a cluster action past its commit point: release as `COMMITTED_CLEANUP_PENDING` instead of rolling back (overview 3.6, 7.6) | T2.6, T2.8 |
| FC6 | `hot_partition` diagnoses must carry the raw partition key as evidence `key_hex`; `Subject.detail` (a CQL literal) is not enough to rebuild key bytes safely | T3.2 |
| FC7 | Hook budget: six new one-statement hooks in two core files (five in `StorageProxy.java`, one in `AlterSchema.java`), numbered H16 to H21 (ADR-022 extends ADR-011). The T2.10 hook-budget script must accept them once D6 lands | overview 4.1; T2.10 |
| FC8 | ADR-013 exception: D6 adds code to request paths (one registry lookup per operation; counters only while a key is being profiled) | ADR-013 |
| FC9 | Configuration: a `layout:` block under `autonomic:` (`max_layouts`, `max_keys_per_layout`, `default_buckets`, `min_rows_for_split`, `max_probe_rows`, `min_point_op_fraction`, `target_share`, `profile_window`, `baseline_window`, `backfill_throughput`, `backfill_chunk_rows`, `verify_page_rows`, `cutover_soak`, `verify_window`, `straggler_window`, `straggler_reconcile`, `max_migration_duration`, `ack_poll_interval`, `cas_drain`, `commit_approval`, `fanout_eager_rows`, `fanout_chunk_rows`, `read_regression_tolerance`), and actuator entry `partition_split: { enabled: false }`. Backfill settings are read on every node, including nodes where the layer is disabled | overview 10.1; T2.1 |
| FC10 | `nodetool autonomic layout` subcommands and help fixtures (H15 grows) | overview 10.2; T2.7 |

## 10. Risks and open questions

| ID | Risk | Mitigation / status |
|----|------|---------------------|
| R1 | Rule O1 makes progress depend on every node being reachable. In a large cluster some node is often down. | Holding is safe; deadline aborts before the commit point. After the commit point only cleanup stalls. Weakening O1 is a contract decision (section 5) |
| R2 | Assumption A1 (epochs never go backwards across restart) is unverified. If false, a restarted coordinator could act two classes behind. | T6.6 test; if it fails, coordinators must refuse routed operations until they have re-confirmed their epoch against a CMS member after start |
| R3 | LWT cannot be excluded for the future, only observed in the past (N1). | Operator allow-list option; owner question 1 |
| R4 | Bulk load or any writer that bypasses `StorageProxy` puts data for an adapted key into the base table, where it is invisible. | Documented. Follow-up: periodic residue probe on replicas (a `LIMIT 1` local read of the base partition) that triggers a reconcile generation |
| R5 | Restoring a backup into a cluster without the registry, or restoring only the logical table. | Operator guide; follow-up `nodetool autonomic layout export/import` |
| R6 | Backfill cost is RF times the partition on each of RF replicas, on a system that is hot. | Throttle, headroom checks, abort on guard breach. Follow-up: designated-replica backfill |
| R7 | The hooks sit in the hottest methods of `StorageProxy`; upstream changes there are frequent. | One statement each, next to the denylist checks, which upstream must keep in the same places |
| R8 | Upstream uses the extension point only in tests (overview R1); the registry adds a second, larger value. | Same fallback as R1 |
| R9 | Unverified API details the implementer must confirm: `RowFilter` re-targeting; building composite partition keys for the overflow table; `ThrottledUnfilteredIterator` behaviour at batch boundaries with open range tombstones; `forSubRange` with a paging `DataRange`; placement lookup for a token; hint expiry; batchlog replay delay. | Named in the tasks that depend on them |
| R10 | Stage-B verification can say "not effective" when nothing can be undone. | Stage A catches read regressions before the commit point; conservative qualification; manual approval; reverse migration as the first follow-up |
| R11 | In a cluster with nodes equal to RF (including the 3-node dtest at RF 3) a split moves no load between nodes. | Policy precondition 7.2 step 5; the demonstration test uses RF 2 on 3 nodes |
| R12 | Coordinator load for the hot key stays on the base replicas with token-aware drivers (shape H). | Stated in the contract; not fixable on the server |

Open issues for later design: O1 LWT on adapted partitions; O2 `RANGE_SPLIT` and the choice between split kinds from the shape profile; O3 reverse migration and the base tombstone; O4 hiding managed tables; O5 several keys per action and whole-table promotion when many keys of one table are split.

Questions the owner must answer:

1. Is the proposed contract (section 5) acceptable, in particular N1 (LWT withdrawn on adapted partitions), D5 (isolation of multi-row writes), D6 (unknown-outcome writes) and N4 (a visible companion table)?
2. Is moving only the hot partition an acceptable reading of "changes the schema on the fly", or is re-partitioning the whole table required?
3. Is a commit point after which there is no loss-free rollback acceptable in the MVP, with reverse migration as a follow-up?
4. Should approval stay manual for layout migrations, and should the commit point require a second approval (`commit_approval`)?
5. Is stalling while any node is unreachable (R1) acceptable, or must migrations proceed with nodes down at the price of weaker guarantees at consistency level `ONE`?
6. Are six more core hook statements, five of them in `StorageProxy`, within the spirit of ADR-011?
7. Should DDL mirroring (T6.14) be part of the MVP rather than a follow-up?
8. Is raising `ANY` to `ONE` for writes to adapted partitions acceptable?

## Appendix A. Answers to the open questions of overview 9.3

| # | Question | Answer | Section |
|---|----------|--------|---------|
| 1 | Transformations in scope | MVP: per-partition hash split by clustering key. Follow-ups: range split, whole-table re-bucketing, vertical split, reverse. Shapes relieved: wide-and-hot, oversized, current-bucket write heat. Not relieved: small hot key, token skew, coordinator heat, slice-heavy | 1 |
| 2 | Rewrite site; prepared statements | Neither candidate: hooks at the `StorageProxy` boundary on mutations and read commands. Prepared statements are not invalidated and need no re-binding because routing is decided per execution and the logical table's metadata never changes | 3.1, 3.7 |
| 3 | Ordering and paging | Logical clustering order is preserved by a k-way merge. Paging states are logical and stay valid across cutover and abort | 3.4, F6 |
| 4 | Range and token queries | Supported: the scan is cut around split keys and each is injected at its logical token position | 3.5 |
| 5 | Dual-write atomicity | Ordered writes, authoritative copy last; batchlog for logged batches and multi-bucket writes; no Accord. A coordinator dying between the writes leaves an unacknowledged write in the non-authoritative copy only | 3.3, 4.11, ADR-017 |
| 6 | Timestamps, TTLs, tombstones | Cell-level copy of local unfiltered data by every base replica; all copying finishes within `gc_grace / 2` of dual-write start | 4.5 |
| 7 | Unsupported features | All listed features block the migration up front; LWT is additionally rejected on adapted keys | 6 |
| 8 | Epoch agreement | Every non-`LEFT` node must report an epoch at or past the phase's epoch via `TCM_CURRENT_EPOCH_REQ`. `ProgressBarrier` is not reusable (quorum of replicas). Feature-disabled nodes participate because routing and backfill are unconditional | 4.3, FC1 |
| 9 | Stale coordinators | Adjacent phases are compatible by construction; rule O1 keeps everyone within one class; rule C1 (validate after execute) stops an operation that outlived its phase from being acknowledged. No replica-side fence | 4.2, 4.3, ADR-016 |
| 10 | Rollback boundary | Loss-free up to and including `CUTOVER`. `NEW_ONLY` is the commit point; afterwards only a reverse migration | 4.9 |
| 11 | "Eventually satisfied" | Contract of section 5: results and durability always; latency, fan-out availability, isolation and unknown-outcome writes may degrade within stated bounds; LWT on the adapted partition is withdrawn | 5 |
| 12 | Consistency levels for fan-out | Applied per bucket; per-row guarantee unchanged; the logical read needs all touched buckets to meet the level and is not a single-partition snapshot | 3.4 |
| 13 | Client-visible metadata, token-aware routing | Logical table unchanged; companion table visible; drivers still reach a valid coordinator, one hop further from the data, and coordinator load stays on base replicas | 5.1 |
| 14 | Authorization | Checked on the logical table; internal operations unchecked; direct writes to the overflow table rejected; direct reads follow RBAC; masked tables ineligible | 5.1 |
| 15 | Registry size | Predicate (hash, bucket count) plus a bounded explicit key list: at most 8 layouts, 8 keys each, 1 KiB per key | 2.2, 2.3 |
| 16 | Verification signal | Per-key latency and error signals from the router and per-partition operation share from the samplers; baseline before dual write; reversible stage A during `CUTOVER`; stage B after the commit point | 7.3 |
