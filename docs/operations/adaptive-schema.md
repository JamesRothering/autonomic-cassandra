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

# Adaptive schema: partition split, operator guide

This guide is for operators of a fork node that runs the autonomic layer. It covers what a partition split does, what clients can and cannot rely on while a split exists, how to turn the feature on, the `nodetool autonomic layout` commands, and how to read `system_views.autonomic_layouts`. Section 9 covers the reverse, `layout unsplit`.

The design is in [adaptive-schema.md](../architecture/adaptive-schema.md). The decisions are in [DECISIONS.md](../DECISIONS.md) (ADR-024 to ADR-034 are the ones that shape this guide).

All outputs below were captured from one node (RF 1, one token range) running this build. Where something needs more than one node, the text says so.

## 1. What a split does

When one partition of a table is both wide and hot, you can split that one partition. The system:

1. creates a companion table in the same keyspace, named `<table>_al_<8 hex>` (the overflow table);
2. copies the partition into it, spread over N buckets by a hash of the clustering key;
3. switches reads and writes of that one partition to the buckets, through the coordinator.

The logical table and every other partition are not changed. The table's `CREATE TABLE` statement, its prepared statements and its result metadata stay the same. Clients keep using the logical table name.

A split is one action per partition. It is proposed by hand with `nodetool autonomic layout split`, approved by hand, and moves through phases. Each phase is visible in `layout status` and in `autonomic_layouts` (section 5).

### 1.1 Guarantees

These come from section 5 of the design.

Always preserved:

| ID | Guarantee |
|----|-----------|
| P1 | The table definition (`system_schema`, `DESCRIBE`), result and bind metadata and prepared statement ids do not change. Phase changes do not force a re-prepare. |
| P2 | Reads return the same rows, values, `writetime()`, `ttl()` and ordering as the original table would, for the same acknowledged history. |
| P3 | Acknowledged writes stay durable at their consistency level in every phase and across an abort. No acknowledged write is lost and no deleted data comes back. |
| P4 | Per-row consistency levels mean what they mean in stock Cassandra. |
| P5 | Paging states issued before or during a migration stay valid afterwards. |
| P6 | Client or server timestamps and TTLs are stored unchanged. |
| P7 | Authorization is checked on the logical table, as before. |

May degrade, within the stated bounds:

| ID | What | Bound |
|----|------|-------|
| D1 | Write latency on the split partition. Dual writes are two sequential writes; range and partition deletes go through the batchlog for good. | Dual-write phases last at most `max_migration_duration` (default 24h). Deletes: permanent. |
| D2 | Reads that do not name the full primary key on the split partition cost N sub-reads instead of one. | Permanent while the partition is split. |
| D3 | Availability of fan-out reads and multi-bucket deletes. Every touched bucket's replicas must meet the consistency level. | Permanent while split. |
| D4 | A few transient timeouts around phase changes. A write at `ANY` to the key behaves as `ONE` from `ANNOUNCED` on. Writes at `ANY` made before the split are not lost: the split waits for their hints (section 2.1). | Per phase change; `ANY` as `ONE` permanent while split. |
| D5 | Multi-row writes inside the split partition are applied by a logged batch. A concurrent read can see some rows and not others. | Permanent while split. |
| D6 | A write whose outcome the client never learned (timeout, coordinator failure) during a migration may appear only at cutover, or be discarded. | Settles by the end of `CLEANUP`. |
| D7 | Column changes on a table with a layout. | Column `ADD` and `DROP` are mirrored to the overflow table (ADR-034). Renames, type changes and other changes are rejected while the table has a layout. |

Note that this is not a promise of "transparent". Section 3 lists what the design cannot make transparent.

## 2. Limitations N1 to N4

These are the parts of the contract a client can notice. Each one is permanent while the partition is split unless noted.

### N1: LWT and SERIAL reads on a split partition are rejected

Lightweight transactions (`IF`, `IF NOT EXISTS`, `IF EXISTS`) and `SERIAL` or `LOCAL_SERIAL` reads of a split partition return `InvalidRequestException`. The design rejects them from `ANNOUNCED` onwards, and rejects `SERIAL` reads in every class except "absent" (ADR-032).

The split is only proposed for tables with no LWT activity (design section 6). The rule does not cover LWT that starts later. A client that starts using LWT on a split partition gets errors until there is a reverse migration. Reverse migration (`unsplit`) is not built yet (T6.17, see section 6).

Observed on the test node, in `CLEANUP`:

```
$ cqlsh> INSERT INTO ks1.t (id, seq, v) VALUES (2, 1, 'z') IF NOT EXISTS;
<stdin>:1:InvalidRequest: Error from server: code=2200 [Invalid query] message="Lightweight transactions are not supported on partition 0x00000002 of ks1.t while it is adapted"

$ cqlsh> CONSISTENCY SERIAL; SELECT * FROM ks1.t WHERE id = 2;
<stdin>:1:InvalidRequest: Error from server: code=2200 [Invalid query] message="Lightweight transactions are not supported on partition 0x00000002 of ks1.t while it is adapted"
```

Mitigation: do not split a table that uses LWT. If the table might use LWT, exclude it with `layout.excluded_tables` (section 4.2). The check on observed activity cannot prove that the future will not use LWT.

### N2: TRUNCATE is rejected during a migration

`TRUNCATE` of a table that has a split partition in progress is rejected until the migration ends. Retry after the migration ends.

Observed in `CLEANUP`:

```
$ cqlsh> TRUNCATE ks1.t;
<stdin>:1:InvalidRequest: Error from server: code=2200 [Invalid query] message="ks1.t cannot be truncated while a partition is being adapted (phase CLEANUP); retry after the migration"
```

### N3: Node-local access sees the physical layout

These paths read the physical tables, not the logical one:

- `NODE_LOCAL` queries and internal queries;
- `nodetool getendpoints` (it reports the base partition's replicas);
- sstable tools (`sstabledump`, `sstableutil`, and so on);
- bulk load (`sstableloader`) into the base table for a split key. The data lands in the base table and is invisible to clients. The MVP does not handle this (risk R4).

Documented, not fixed.

### N4: A companion table is visible

The overflow table `<ks>.<t>_al_<hex>` appears in `system_schema.tables` and in `DESCRIBE KEYSPACE`. Drivers see an ordinary table with a comment. Operators must back it up together with the logical table (section 3).

Observed on the test node:

```
$ cqlsh> DESCRIBE KEYSPACE ks1;
...
Keyspace ks1
------------
t  t_al_14810cff
```

`system_views.autonomic_layouts` (section 5) is the way to see which overflow tables belong to which split.

### 2.1 Other things that look like bugs but are documented behavior

- **A split waits in `ANNOUNCED` for old hints** (ADR-035). Before `DUAL_WRITE`, every node must report that it holds no hint, created before the split, for any replica of the key. Hint replay writes to the base table only, so a hint delivered after the migration would be lost; a write at `ANY` can be acknowledged on such a hint alone. The check is by hint file age and target host, not by key: hints created after the split started, for any key, do not hold it up. The wait runs alongside the `cas_drain` wait and normally adds nothing.
  - What holds it: hint delivery paused (`nodetool pausehandoff`) on any node, a replica that refuses or times out on hints, a corrupt hints file, or a large backlog toward one of the key's replicas. `nodetool autonomic layout status` shows `waits for hint reports (ADR-035), <n> of <m> nodes reported`, and each waiting node logs `waits in ANNOUNCED until this node has delivered the hints created before the split` once a minute with the oldest file per host.
  - If the reports are not all in within `layout.hint_drain_timeout` (default 10m) after every node acknowledged `ANNOUNCED`, the migration aborts before the commit point. The abort reason starts with `HINTS_NOT_DRAINED` and names the nodes that still hold hints (`nodetool autonomic events <id>`, `layout status` while it aborts). No data is lost: the key goes back to the base table, where the hints land.
  - To clear it: on each named node, run `nodetool listpendinghints` and `nodetool statushandoff`; resume delivery with `nodetool resumehandoff` and wait for the backlog to drain, or, if you accept losing those hints (and repair the replicas afterwards), drop them with `nodetool truncatehints <replica address>`. Then propose the split again.
- **Base rows stay on disk** after the split is complete (`STEADY`, status `base residue kept`) until a separate `reclaim` action writes the base partition tombstone, `autonomic.layout.reclaim_window` after the key froze (default: half of the table's `gc_grace_seconds` less `straggler_window`, `reconcile_budget` and `cutover_soak`, i.e. when a fast reverse can no longer be completed). The reclaim is proposed by the policy and needs the usual cluster approval. The rows are unreachable through the logical table. Reverse migration must not assume the base partition is empty (ADR-033, ADR-036).
- **Replicas stop backfilling** once the local clock is past `DUAL_WRITE` start plus half of the table's `gc_grace_seconds` (ADR-031). This depends on clock sync between nodes.
- **A split that verification finds ineffective stays split.** It raises `layout.split_not_effective`. Until reverse migration exists, the only way back is an abort before the commit point (ADR-025).

## 3. Backup obligation

Back up the keyspace, not the table.

- A keyspace snapshot contains the logical table and every overflow table of the keyspace. This is what you need.
- A table-level snapshot of the logical table alone misses every split partition. Its data lives in the overflow table, which the table snapshot does not include.
- Restoring a backup into a cluster that does not have the layout registry leaves the split partitions only in the overflow table, so their rows are not reachable through the logical table (design R5; not tested here). A tool to export and import the registry (`layout export` / `import`) is a follow-up (risk R5), not built.

Observed on the test node. The table-level snapshot holds only the base table, and the keyspace snapshot holds both:

```
$ nodetool snapshot -t doc_table_only -cf t ks1
$ nodetool snapshot -t doc_keyspace ks1
$ nodetool listsnapshots
Snapshot details:
Snapshot name                       Keyspace name Column family name True size Size on disk Creation time            Expiration time
1795025015407-dropped-t_al_f2d6a02c ks1           t_al_f2d6a02c      6.62 KiB  6.62 KiB     2026-11-18T18:03:35.407Z
doc_keyspace                        ks1           t_al_14810cff      1.34 KiB  6.53 KiB     2026-11-18T18:14:54.238Z
doc_keyspace                        ks1           t                  1.23 KiB  11.15 KiB    2026-11-18T18:14:54.238Z
doc_table_only                      ks1           t                  1.23 KiB  11.15 KiB    2026-11-18T18:14:52.583Z
```

The `-dropped-t_al_...` snapshot is created automatically when an abort drops the overflow table (`auto_snapshot` is on by default). It is a record of the data that the abort discarded. Clear it when you no longer need it.

## 4. Enabling the feature

### 4.1 Settings

The whole layer is off by default. Settings are in `cassandra.yaml`, under `autonomic:`. The `layout:` block is read on every node, also when the layer is disabled. The settings you need for a split:

```yaml
autonomic:
  enabled: true                    # off by default
  dry_run: false                   # true by default: decisions are made, nothing is executed
  startup_grace: 5m                # the layer is not armed until this long after start
  actuators:
    partition_split: { enabled: true }   # off until enabled
  layout:
    default_buckets: 8             # used when `layout split` names no bucket count (2 to 64)
    excluded_tables: []            # tables that are never proposed (ADR-024, question 1)
    commit_approval: manual        # a second `approve` is needed before the commit point
    cutover_soak: 10m              # CUTOVER is held this long before the commit point
    straggler_window: 3h           # CLEANUP waits this long for late writes
    # reclaim_window: 10d          # time after a split before the base residue is deleted (default: gc_grace/2 - straggler_window - reconcile_budget - cutover_soak)
    # force_purge_compaction: true  # slow reverse: replicas force a compaction of the key to purge the base partition deletion; false = only probe, wait for natural compaction
    max_migration_duration: 24h    # the longest time before the commit point
    backfill_throughput: 1MiB/s    # per node, 0 is unlimited
    cas_drain: 15s                 # ANNOUNCED waits at least this, and the longest request timeout
    hint_drain_timeout: 10m        # longest wait in ANNOUNCED for pre-split hints to be delivered; then abort (ADR-035)
```

The rest of the `layout:` block (eligibility thresholds, verification, triggers) is documented with its defaults in `conf/cassandra.yaml`. Read the comments there before you change them.

Two settings deserve a warning:

- `excluded_tables` is the only protection against LWT that starts after a split. Put every table that might use LWT on this list.
- `commit_approval: manual` is the default. Keep it. Without it, a migration reaches the commit point without a human decision, and after the commit point there is no abort.

A table is refused when its `gc_grace_seconds` is too short for one migration within the replica backfill window. The threshold is a constant of 28800 seconds (ADR-029, proposed). A table with `gc_grace_seconds` of 864000 (the default) is eligible.

### 4.2 Turning it on

1. Set the settings above on every node and restart them.
2. Wait for `startup_grace` (5 minutes by default). Until then `nodetool autonomic status` shows `Armed: false`, and a split is rejected with `NOT_ARMED` (observed, see section 4.3).
3. Enable cluster-scoped actions once, on any node:

```
$ nodetool autonomic cluster enable
WARNING: enabling cluster actions is ONE-WAY with respect to stock Apache Cassandra.
Once enabled, the cluster metadata log contains entries that stock Cassandra nodes cannot read.
Every node of the cluster must run this fork before and after enabling.
Re-run with --yes to enable cluster actions.
cluster_actions_enabled: false
...

$ nodetool autonomic cluster enable --yes
```

The enable step is one-way. Every node must run this fork before and after it, and it cannot be undone by configuration.

### 4.3 Checking the node

```
$ nodetool autonomic status
State: RUNNING
Enabled: true
Dry run: false
Armed: true
Paused: false
Killed: false
In-flight actions: 0
Awaiting approval: 0
Cluster actions enabled: true
```

`Armed: true` and `Cluster actions enabled: true` are both needed before a split can proceed. Before the grace period ends, the same command shows `State: STARTING` and `Armed: false`. A split proposed then is rejected at once:

```
$ nodetool autonomic layout split ks1 t 00000001 --buckets 4
id=84f458b0-e379-11f1-8254-f3e7632bd421 state=REJECTED state_reason=NOT_ARMED actuator=partition_split scope=CLUSTER target=PARTITION:ks1.t:00000001 params={buckets=4, mode=split} policy=operator ...
```

## 5. Commands

All commands are `nodetool autonomic layout ...`. They work whether or not the layer is enabled on the node you run them on, except that a split needs an armed node. Each command takes a keyspace, a table and the raw partition key bytes in hex. For an `int` key, the hex is the four bytes of the value: `00000001` is `id = 1`.

### 5.1 `layout status`

Prints one line per key of every adapted layout. Running `nodetool autonomic layout` with no subcommand does the same.

```
$ nodetool autonomic layout status
No records.
```

During a migration, one line per key:

```
$ nodetool autonomic layout status
table=ks1.t key=00000002 phase=CUTOVER overflow=ks1.t_al_14810cff buckets=4 incarnation=1 generation=1 backfill_done=[NodeId{id=1}] action=972a9970-e37b-11f1-9e30-47820973f337 status=running; the commit point waits for `nodetool autonomic approve 972a9970-e37b-11f1-9e30-47820973f337` orphaned=false
```

After the commit point:

```
table=ks1.t key=00000002 phase=CLEANUP overflow=ks1.t_al_14810cff buckets=4 incarnation=1 generation=1 backfill_done=[NodeId{id=1}] action=972a9970-e37b-11f1-9e30-47820973f337 status=running on node 1 orphaned=false
```

The `status=` field says what the owner is doing or waiting for. `backfill_done` here lists node ids in `NodeId{id=...}` form. `autonomic_layouts` prints the same nodes as UUIDs (section 6).

### 5.2 `layout split`

Proposes the split of one partition. The proposal goes through the same checks as a proposal from a policy. Approval is manual by default, so the action waits in `PROPOSED` until `nodetool autonomic approve <id>`.

```
$ nodetool autonomic layout split ks1 t 00000002 --buckets 4
id=972a9970-e37b-11f1-9e30-47820973f337 state=PROPOSED state_reason= actuator=partition_split scope=CLUSTER target=PARTITION:ks1.t:00000002 params={buckets=4, mode=split} policy=operator diagnosis_id=750326dd-21ba-444f-a6ec-696cc646d599 dry_run=false verdict= baseline={} result={} proposed_at=1795025538183 updated_at=1795025538183

$ nodetool autonomic approve 972a9970-e37b-11f1-9e30-47820973f337
Approved action 972a9970-e37b-11f1-9e30-47820973f337
```

Other outcomes:

- `--buckets` must be from 2 to 64. Otherwise:
  ```
  $ nodetool autonomic layout split ks1 t 00000003 --buckets 1
  nodetool: --buckets must be between 2 and 64
  ```
- A key that is already in a layout is rejected with `VALIDATION`. Its reason is in the action's events:
  ```
  $ nodetool autonomic layout split ks1 t 00000001 --buckets 4
  id=59a050f0-e37a-11f1-8254-f3e7632bd421 state=REJECTED state_reason=VALIDATION actuator=partition_split ...

  $ nodetool autonomic events 59a050f0-e37a-11f1-8254-f3e7632bd421
  at=1795025005441 from_state=null to_state=PROPOSED detail=plan 9fb237bd-... by operator: nodetool autonomic layout split
  at=1795025005442 from_state=PROPOSED to_state=REJECTED detail=the key is already in the layout of ks1.t in phase CUTOVER
  ```
- A proposal on a table that does not exist is rejected with `VALIDATION` in the same way (the reason is in the events).
- A table whose `gc_grace_seconds` is too small for `layout.straggler_window` is rejected with `VALIDATION`, with the same check a policy's proposal gets (ADR-029): half of `gc_grace_seconds` less `straggler_window` must leave time for the migration. The events say `gc_grace_seconds <n> of <ks.table> is too small for straggler_window <w>: ...`. Raise `gc_grace_seconds` or lower `straggler_window`.
- An identical rejection repeated within a short interval is not written again. The command prints an error instead:
  ```
  nodetool: The proposal was rejected for the same reason as an identical one just before; see `autonomic history`
  ```
  Use `nodetool autonomic history` to see the earlier rejection.

Run `nodetool autonomic history` for every proposal from the last 7 days, including rejected ones.

### 5.3 `layout abort`

Asks the owner of the migration to take the abort path. It works before the commit point and is refused after it.

Before the commit point, in `CUTOVER` on the test node:

```
$ nodetool autonomic layout abort ks1 t 00000001
Abort of PARTITION:ks1.t:00000001 requested in phase CUTOVER; action fcabedf0-e379-11f1-8254-f3e7632bd421 takes the abort path
```

The command returns when the request is recorded. The owner then does the work. In `CUTOVER` the path is `ROLLBACK_DUAL`, then `ABORTING`, which deletes the bucket partitions and drops the overflow table. On the test node, the next `layout status` (about 40 seconds later) showed `No records.`, and the final history line was:

```
$ nodetool autonomic history
id=fcabedf0-e379-11f1-8254-f3e7632bd421 state=ROLLED_BACK state_reason=ACTUATOR_ROLLBACK actuator=partition_split scope=CLUSTER target=PARTITION:ks1.t:00000001 params={buckets=4, mode=split} policy=operator ...
```

What an abort does to the data, in each phase before the commit point (the design, section 4.11):

| Phase when aborted | Result |
|--------------------|--------|
| `ANNOUNCED`, `DUAL_WRITE`, `BACKFILL`, `VERIFY` | The base table is authoritative and has every acknowledged write. Writes that reached only the overflow table were never acknowledged and are discarded. |
| `CUTOVER` | The base copy was written first for every write in `CUTOVER`, so it has every acknowledged write. |

After the abort on the test node, the base table held every row (402 rows for `id = 1`, the 2 original rows and the 400 inserted by the test), and `autonomic_layouts` was empty:

```
$ cqlsh> SELECT count(*) FROM ks1.t WHERE id = 1;
 count
-------
   402
$ cqlsh> SELECT count(*) FROM system_views.autonomic_layouts;
 count
-------
     0
```

An abort does not touch an ordinary (unsplit) table, and it does not need the other nodes to agree on anything once they have acknowledged the phase.

A few things to know:

- An abort is a request. Read the result with `layout status` and `autonomic history`.
- An abort can stall on an unreachable node. Each abort step needs every node (rule O1). The migration then stays in its current phase until the node returns or the `backfill_deadline` aborts it.
- The abort path is the only way back before the commit point. After it, the only way back is the reverse migration in section 9.

Past the commit point (here `CLEANUP`):

```
$ nodetool autonomic layout abort ks1 t 00000002
nodetool: The split of PARTITION:ks1.t:00000002 is past its commit point (phase CLEANUP): it cannot be aborted. The way back is a reverse migration, `nodetool autonomic layout unsplit`, which is not built yet (T6.17)
```

The text of this message is out of date: the reverse migration is built (section 9). It is printed by the code as it stands, so the message is what the build says until the code changes.

### 5.4 `layout resume`

Takes over a split that has no live action. Before the commit point it aborts; after it, it finishes the migration. It proposes a new action, which is subject to approval like a split.

A split with a live action is not taken over. The command reports why:

```
$ nodetool autonomic layout resume ks1 t 00000002
id=ec484d80-e37b-11f1-9e30-47820973f337 state=REJECTED state_reason=VALIDATION actuator=partition_split scope=CLUSTER target=PARTITION:ks1.t:00000002 params={mode=resume} policy=operator ...

$ nodetool autonomic events ec484d80-e37b-11f1-9e30-47820973f337
at=1795025680990 from_state=null to_state=PROPOSED detail=plan 9fb237bd-... by operator: nodetool autonomic layout resume
at=1795025680996 from_state=PROPOSED to_state=REJECTED detail=the migration of the key is running under action 972a9970-e37b-11f1-9e30-47820973f337
```

On a key with no layout, the command gives the repeated-rejection error from section 5.2:

```
$ nodetool autonomic layout resume ks1 t 00000001
nodetool: The proposal was rejected for the same reason as an identical one just before; see `autonomic history`
```

Not verified: a successful resume. Taking over a stalled migration needs a migration whose owner is gone. That needs a second node, and the test node had only one.

## 6. Reading `system_views.autonomic_layouts`

This virtual table has one row per split key. It is read from the cluster metadata, so every node shows the same rows.

```
$ cqlsh> SELECT * FROM system_views.autonomic_layouts;

 keyspace_name | table_name | partition_key | action_id                            | backfill_done                          | buckets | dual_write_since                | epoch | generation | generation_epoch | incarnation | layout_id                            | layout_version | orphan | phase   | phase_since                     | physical_table | transform
---------------+------------+---------------+--------------------------------------+----------------------------------------+---------+---------------------------------+-------+------------+------------------+-------------+--------------------------------------+----------------+--------+---------+---------------------------------+----------------+------------
           ks1 |          t |      00000002 | 972a9970-e37b-11f1-9e30-47820973f337 | {6d194555-f6eb-41d0-c000-000000000001} |       4 | 2026-11-18 18:13:25.049000+0000 |    42 |          1 |               29 |           1 | 14810cff-3981-4aac-9444-bde5e951eb46 |              8 |  False | CLEANUP | 2026-11-18 18:14:30.856000+0000 |  t_al_14810cff | HASH_SPLIT

(1 rows)
```

The output is the test node in `CLEANUP`, after the commit point. Each column:

| Column | Meaning |
|--------|---------|
| `keyspace_name` | The keyspace. Partition key of the table. |
| `table_name`, `partition_key` | The logical table and the split partition's key. Clustering columns, in that order. |
| `partition_key` | The raw key bytes of the split partition, in hex. |
| `layout_id` | Id of the layout. A layout id is never reused for the same physical table. |
| `physical_table` | The overflow table, `<table>_al_<8 hex>` (the first 8 hex digits of `layout_id`). |
| `transform` | How the partition is split. `HASH_SPLIT` is the only one in the MVP. |
| `buckets` | Number of buckets (the `--buckets` value, or `layout.default_buckets`). |
| `layout_version` | Goes up on every change to the layout registry, including backfill reports. Not a reliable indicator of a phase change. |
| `phase` | The phase of the key (section 6.1). |
| `epoch` | The cluster metadata epoch at which the current phase was agreed by every node. |
| `phase_since` | When the key entered the current phase. |
| `dual_write_since` | When the key entered `DUAL_WRITE` for the current incarnation. `null` before that. The replica backfill deadline is half of `gc_grace_seconds` after this time (ADR-031). |
| `incarnation` | The attempt number of this key. Every attempt uses fresh buckets. The bucket column value in the overflow table is `incarnation * 256 + bucket`. |
| `generation` | The backfill generation. A new generation starts when a backfill has to be redone (a changed replica set, or a failed verification). |
| `generation_epoch` | The epoch at which the current generation started. |
| `backfill_done` | The nodes that have finished copying their local data for the current generation. A node's id is shown as a UUID here. |
| `action_id` | The action that owns the migration. |
| `orphan` | `True` when the logical table no longer exists in the schema. Such a split needs `layout resume` or an abort. |

Empty result (`(0 rows)`) means no key is split.

### 6.1 Phases

| Phase | Meaning for a client |
|-------|----------------------|
| `ANNOUNCED` | Every node knows about the split. LWT and `SERIAL` reads are rejected. Waits `cas_drain` after acknowledgement, and until no node holds hints from before the split for the key's replicas (at most `hint_drain_timeout`). |
| `DUAL_WRITE` | Writes go to both copies. Reads still come from the base table. |
| `BACKFILL` | Replicas copy their local data to the overflow table. Writes still go to both. |
| `VERIFY` | Both copies are compared row by row. A mismatch fails the check and starts one more backfill generation. |
| `CUTOVER` | Writes go to both copies, the base copy first. Reads move to the overflow table. The soak (`cutover_soak`) starts here. An abort here is still possible. |
| `NEW_ONLY` | The commit point. Writes go only to the overflow table. Abort is refused. |
| `CLEANUP` | The owner waits `straggler_window` and copies late data. It does not delete the base rows: they stay on disk as residue until the later `reclaim` writes the base partition tombstone (ADR-036). |
| `STEADY` | The split is done. The claim is released. |
| `ROLLBACK_DUAL`, `ABORTING` | Abort in progress. The key is removed from the table when the abort finishes. |

The reverse phases (`REVERSE_ANNOUNCED` to `REVERSE_ROLLBACK`) are in section 9.2.

On the test node the migration of key 1 went from `ANNOUNCED` to `CUTOVER` in about 30 seconds (the baseline and `cas_drain` were short on a quiet node). The migration of key 2 used a `cutover_soak` of 30 seconds, not the default 10 minutes. Expect the defaults to take at least `cutover_soak` plus `straggler_window` (3 hours by default) on a real cluster. The test did not reach `STEADY`.

### 6.2 How a split is verified

A split that the policy proposed is judged twice, by `SplitEvaluation` (ADR-040). An operator's split carries no profile and is not judged.

- **Stage A**, at the end of `CUTOVER`'s soak (`cutover_soak`). A failure rolls the split back, with no loss.
- **Stage B**, when the key reaches `STEADY`, over the samples from the commit point on, at most the last `verify_window`. The verdict is recorded and the key stays split. `NEUTRAL` and `REGRESSED` raise `layout.split_not_effective`.

Both stages compare with a baseline taken in `ANNOUNCED` over `baseline_window`. The latency guards are:

| Guard | Passes when |
|-------|-------------|
| `collateral read latency`, `collateral write latency` | The rest of the table (operations on its other partitions, coordinated by the owner node) is within its baseline times `1 + collateral_tolerance` (default 0.5). |
| `key read latency` | The key's reads are within `reference * (1 + s * (N - 1)) * (1 + read_regression_tolerance)`. Here `s` is the share of slices among the key's reads in the policy's profile and `N` is the number of buckets: a point read reads one bucket, and a slice may read every bucket. |
| `key write latency` | The key's writes are within `reference * copies * 1.5`, where `copies` is 2 in `CUTOVER` and 1 after the commit point. The 50% allows for routing. |

The `reference` is the larger of the key's own baseline and the latency of the rest of the table now. If the rest of the table has too few operations in the window, its baseline is used instead. The split moves the key to other replicas and puts every request through routing, so the key's latency before the split is not what it should cost afterwards. The rest of the table, on the same nodes at the same time, is.

Stage A also checks the key's error rate (at most its baseline plus 1%). The goal of stage B is the busiest partition's share of the table: it must fall by `min_improvement` (default 0.5).

Each guard is judged only when the baseline and the window each have `layout.min_samples` operations of its class. The stage's detail names every guard as `ok ... within ...`, as a breach (`... above ...`), or as `not judged (<baseline>/<window> ops, min <n>)`. Stage B's detail is in the action's last event (`nodetool autonomic events <id>`). A passed stage A is logged on the owner node as `stage A passed: ...`. A table whose only traffic is the hot key has no collateral guard, and its key guards then compare with the key's own baseline.

Other tables are not guarded by latency. Node-level damage before the commit point is caught by the C1 guard: new CRITICAL saturation diagnoses, the key's error rate, and the table's coordinator write p99 within 3 times its baseline.

## 7. Verification notes

- Every command in this guide was run against a running node with the layer armed, except the cases marked "not verified".
- Not verified: a split on a multi-node cluster, `layout resume` on a stalled migration, `STEADY` (it needs `straggler_window`), and LWT or `SERIAL` in `ANNOUNCED`.
- Not verified: a restart of a node while a migration is in `CLEANUP`. Stopping the test node in that phase logged this, during shutdown:

  ```
  ERROR [StorageServiceShutdownHook] AutonomicService.java:1090 - Autonomic layer could not roll back in-flight actions while stopping
  ERROR [StorageServiceShutdownHook] AutonomicService.java:1095 - Autonomic layer stopped with actions still applied: [972a9970-e37b-11f1-9e30-47820973f337]; they are reported as FAILED when it is next enabled
  ```

  The release of the claim did not commit before the process exited. Check `nodetool autonomic layout status` and `nodetool autonomic history` after every restart during a migration. The migration is not lost (the phase is in the cluster metadata), but the action record may say `FAILED`.

## 8. What is not built yet

- **Reverse migration** is built (T6.17, section 9). Not observed on a running node: slow mode, the residue reasons, `--when-ready` and `layout.split_not_effective` (section 9.12). A split that verification finds ineffective stays split and raises `layout.split_not_effective` (ADR-025) until it is unsplit.
- **Registry export and import** for backup restores (R5). Until it exists, restore the keyspace and the layout registry together, and check the result with `autonomic_layouts` and the overflow tables.
- **Whole-table re-partitioning.** Only one partition is split at a time (ADR-024, question 6).

## 9. Unsplit: the reverse migration

An unsplit moves a split key back into the base table and then drops the overflow copy of that key. It is always proposed and approved by hand, and it has its own commit point. The design is in [adaptive-schema.md section 11](../architecture/adaptive-schema.md), decided in ADR-036 and ADR-037.

- Two approvals: the proposal, and the reverse commit point `BASE_ONLY` (with `commit_approval: manual`, the default).
- Two modes, chosen automatically: fast, which merges into the base residue, and slow, which needs the base tombstone to be purged first.
- Before `BASE_ONLY` an abort returns the key to `STEADY`, and the key stays split. After `BASE_ONLY` the unsplit runs to the end.
- The layer never approves an unsplit itself, whatever `cluster_approval` says.

### 9.1 When an unsplit is proposed

- By hand, with `nodetool autonomic layout unsplit` (section 9.3).
- Automatically, when stage B of a split raises `layout.split_not_effective` (WARN, ADR-025). The policy then proposes an unsplit of that key with manual approval. This was not observed on the test node (section 9.12).
- From a `--when-ready` request, once its time has come (diagnosis `layout.unsplit_pending`).
- For a key in a reverse phase whose action holds no claim (`layout.unsplit_orphaned`: WARN before `BASE_ONLY`, CRITICAL after). The policy proposes `mode resume`.

### 9.2 The phases

| Phase | What it means | Abort before `BASE_ONLY` |
|-------|---------------|--------------------------|
| `REVERSE_ANNOUNCED` | Every node has the reverse. A slow reverse waits here for every base replica to purge clean (9.5). | Returns to `STEADY` |
| `REVERSE_DUAL_WRITE`, `REVERSE_BACKFILL`, `REVERSE_VERIFY` | Writes go to both copies, the bucket partitions are copied into base, and the two copies are compared. | Returns to `STEADY` |
| `CUTBACK` | Reads move back to base; writes go to both copies, overflow first. After `cutover_soak` a latency and error guard is checked, and the commit-point approval is asked for. | Returns to `STEADY` (`REVERSE_ROLLBACK`) |
| `BASE_ONLY` | The reverse commit point. Base holds every acknowledged write. | Refused |
| `REVERSE_CLEANUP` | `straggler_window`, then a reconcile pass that copies late writes into base. | Refused |
| `KEY_REMOVED` | The key is removed from the layout. Once every node has acknowledged the removal and the write timeout has passed, the bucket partitions are deleted (or the overflow table is dropped if the layout went with its last key). | Refused |

`nodetool autonomic layout status` shows the phase. A proposal's history is in `nodetool autonomic events <id>`. Here is the event trail of an unsplit that the guard rolled back in `CUTBACK` on the test node (section 9.10), with the ids and times shortened:

```
$ nodetool autonomic events 680d22d0-e41a-11f1-9390-857e887a9d30
at=1795093749139 from_state=null to_state=PROPOSED detail=plan ... by operator: nodetool autonomic layout unsplit
at=1795093758530 from_state=PROPOSED to_state=APPROVED detail=operator
at=1795093758652 from_state=APPROVED to_state=APPLYING detail=claimed lane SCHEMA_LAYOUT
at=1795093758758 from_state=APPLYING to_state=APPLYING detail=phase BEGINNING (step 1)
at=1795093758978 from_state=APPLYING to_state=APPLYING detail=phase REVERSE_ANNOUNCED (step 2)
...
at=1795093771489 from_state=APPLYING to_state=APPLYING detail=phase CUTBACK (step 10)
at=1795093781714 from_state=APPLYING to_state=APPLYING detail=phase REVERSE_ROLLBACK (step 13)
at=1795093782075 from_state=APPLYING to_state=ROLLED_BACK detail=ks1.t key 0x00000002 stays split: C1 guard failed in CUTBACK: table write latency 1.60e+03 above 0.00us; ; the base partition is RESIDUE
```

### 9.3 The command

```
nodetool [connection options] autonomic layout unsplit [--when-ready] [--] <keyspace> <table> <key-hex>
```

`<key-hex>` is the raw partition key bytes, as for `layout split`. The command has no mode option: the mode is chosen when the action is validated (9.4). The proposal is held for approval:

```
$ nodetool autonomic layout unsplit ks1 t 00000002
id=680d22d0-e41a-11f1-9390-857e887a9d30 state=PROPOSED state_reason= actuator=partition_unsplit scope=CLUSTER target=PARTITION:ks1.t:00000002 params={mode=auto} policy=operator diagnosis_id=1054eb97-1714-4480-9401-c36359698e51 dry_run=false verdict= baseline={} result={} proposed_at=1795093749131 updated_at=1795093749131

$ nodetool autonomic approve 680d22d0-e41a-11f1-9390-857e887a9d30
Approved action 680d22d0-e41a-11f1-9390-857e887a9d30
```

Then `nodetool autonomic layout status` shows the phase. When the action reaches `CUTBACK` it waits for the second approval, the same `nodetool autonomic approve <id>` (the code requires it with `commit_approval: manual`; the status text while it waits was not captured). Approving it is what starts `BASE_ONLY`. The guard of `CUTBACK` is checked too: an approval does not skip it (9.10).

Rejections. A rejected proposal is printed as a record with `state=REJECTED`, and its reason is in `nodetool autonomic events <id>`. These were observed:

| Case | Output |
|------|--------|
| The key is not in a layout | `state=REJECTED state_reason=VALIDATION`; the event says `the key is not split: it is not in the layout of ks1.t` |
| The same rejection again, with `--when-ready` | `nodetool: The proposal was rejected for the same reason as an identical one just before; see 'autonomic history'` (exit 1) |
| The key rolled back less than `safety.cooldown_after_rollback` ago (30 minutes by default) | `state_reason=COOLDOWN`; the event says `until <ms>`, the time the cooldown ends |
| The key is in `STEADY` and `layout resume` is run on it | `state_reason=VALIDATION`; the event says `the key is split and released; nothing to resume (a base residue is reclaimed by mode reclaim)` |
| Too many actions in the window | `state_reason=RATE`, with the limit named in the event (`cluster`, `actuator` or `node`); defaults 2 per hour for the cluster and for each actuator |

A cooldown that is already recorded is not shortened by changing `cooldown_after_rollback`. It runs to the time it was set for.

With a time in it, a rejection names the earliest time a mode becomes allowed: `slow mode from <ms>: the base tombstone at <Tt> plus gc_grace_seconds <n>`. This follows the code, and was not observed on the test node, because a slow mode needs a tombstone that was ten days old. Without `--when-ready`, the command fails with that text and `(earliest <ms> ms; --when-ready proposes it then)`. With `--when-ready`, the request is stored on the node where the command ran, under `layout.unsplit_pending/<keyspace.table>/<key-hex>`, and the command prints a `state=PENDING` record with `due_millis`. A rejection that names no time is not stored: the action is proposed and rejected at once.

### 9.4 Fast mode and slow mode

| | Fast | Slow |
|--|------|------|
| Base state needed | `RESIDUE`: the base residue is still there and no tombstone was written | `TOMBSTONED`: the reclaim wrote the base partition tombstone at `Tt` |
| Allowed when | the copy and `cutover_soak` end before the reverse commit window closes | `Tt + gc_grace_seconds` has passed (at once if `Tt` is unknown) |
| Window by default | The base residue is kept for up to about `gc_grace_seconds / 2` after the split (5 days for `gc_grace_seconds` of 864000), less the margins for `straggler_window`, `reconcile_budget` and `cutover_soak` | Ten days after the tombstone by default |
| Extra work | None | Every base replica waits for its tombstone to pass `gc_grace`, forces a compaction of the key (`force_purge_compaction`), and reports a clean probe (9.5) |
| Typical time | Hours | Ten days or more |

**Reclaim.** When fast mode can no longer finish before its window closes, and no reverse owns the key, an automatic continuation of the split writes the base partition tombstone at every node (`ALL`). It is not a command. The key's status shows the due time (`reclaim due at <ms>`). `layout.reclaim_window` sets the time after the split instead. Observed: `status=split; base residue kept, reclaim due at 1795523961716` about 4.98 days after the split on the test node, with the default settings.

**Disk.** The base residue is one partition times the replication factor. It is kept until the reclaim, at most about 5 days after the commit by default. Its bytes go only when compaction purges them, which is after the tombstone's `gc_grace` (10 days by default). Plan disk for the residue of every split key until then.

From the commit on, the residue is unreachable: reads of the key go to the overflow copy, and range scans skip the key in base. The residue does not change what clients see.

### 9.5 Residue: `layout.unsplit_residue`, its reasons, and the purge

A slow reverse waits in `REVERSE_ANNOUNCED` until every natural base replica of the key has purged clean. Each replica waits until its partition deletion has passed `gc_grace_seconds`, forces a compaction of the key, and runs a probe: a local read with nothing purged, which must find no row, no range tombstone and no partition deletion. A replica that is not clean publishes its reason in gossip application state `X1`, reserved for the autonomic layer (ADR-037). After `layout.purge_timeout` (6 hours by default, counted from the acknowledgement, or from `Tt + gc_grace` if that is later), the reverse aborts to `STEADY` and raises `layout.unsplit_residue`. The event names each node and its reason, and gives the remedy:

| Reason | Meaning | Remedy |
|--------|---------|--------|
| `WAITING_GC_GRACE` | The replica's clock has not passed the deletion's local deletion time plus `gc_grace_seconds` | Check the replica's clock, then try again later |
| `STILL_SHADOWED` | The partition deletion, or data at or below it, was not purged. Often the deletion and the data it shadows are in sstables of different repair states, which compact apart | Run a repair of the key's token range, so both reach one repair state, then start the unsplit again. Or set `layout.force_purge_compaction: false` and wait for natural compaction |
| `ONLY_PURGE_REPAIRED_TOMBSTONES` | `only_purge_repaired_tombstones` is set and the key's sstables are not repaired, or are pending repair | The same as above: repair the range, or wait for natural compaction |
| `RESIDUE_ABOVE_TT` | Cells newer than the base tombstone survive the purge (client timestamps in the future, or stragglers from a coordinator whose clock runs ahead). They are never purged automatically | An operator decision. Name the largest timestamp in the event. Delete the cells with a higher timestamp, or keep the key split. Nothing is fixed automatically |

Two more settings:

- `never_purge_tombstones` is a system property, `-Dcassandra.never_purge_tombstones=true`, not a table option. While it is set, a node does not purge tombstones at all, so no probe can come clean there and a slow unsplit aborts at `purge_timeout`. Clear it and restart the node to continue.
- `layout.purge_timeout` (default `6h`) is the time a slow reverse waits in `REVERSE_ANNOUNCED` for every replica to report clean.

### 9.6 Keys stuck at `RECONCILE_PAST_DEADLINE`

A split key stuck in `CLEANUP` at `RECONCILE_PAST_DEADLINE` (ADR-031) has no fast window. It can be unsplit in slow mode. The unsplit action writes the base tombstone itself once the forward replicas' limit plus `straggler_window` has passed (owner answer 6, ADR-037), then starts the slow reverse. The rejection names that time (`slow mode from <ms>: the key is stuck in CLEANUP ...`). This follows the code and was not run on the test node.

### 9.7 Contract D6: unacknowledged writes after an unsplit

D6 said that a write whose outcome the client never learned may appear only at cutover, or be discarded, and that this settles by the end of `CLEANUP`. Amended (ADR-036): **such a write may also become visible after an unsplit.** An unacknowledged write that reached the base residue after the forward reconcile is visible again after a fast unsplit, days after the forward cleanup. Acknowledged writes are not affected (P3). Treat a write as unknown until you check it.

### 9.8 Bulk load into a split key

Data loaded directly into the base table of a split key (for example with `sstableloader`) is not read while the key is split, because reads go to the overflow copy. It becomes visible after an unsplit, subject to last-write-wins against the residue. Do not bulk load into a split key's base partition. Unsplit the key first, or load through the logical table.

### 9.9 Known limitations

- A `--when-ready` request is stored only on the node that ran the command, in that node's local state. It is lost if that state is wiped, and only that node proposes it. Check `nodetool autonomic history --diagnoses` for `layout.unsplit_pending`.
- Gossip application state `X1` is reserved for the autonomic layer. Do not write to it from other tooling.
- A key rolled back in a reverse gets a cooldown of `safety.cooldown_after_rollback` (30 minutes by default) before the next proposal (9.3).
- The `CUTBACK` guard compares the table's write latency and the key's error rate with a baseline taken over `layout.baseline_window`. On a node with no write traffic in that window, the baseline is zero and the guard rolls the reverse back. Section 9.10 has the observed case.

### 9.10 A reverse rolled back by the guard (observed)

On the test node, which had little traffic, three reverse attempts on two keys each rolled back in `CUTBACK`:

```
ROLLED_BACK detail=ks1.t key 0x00000002 stays split: C1 guard failed in CUTBACK: table write latency 1.60e+03 above 0.00us; ; the base partition is RESIDUE
ROLLED_BACK detail=ks1.t key 0x00000003 stays split: C1 guard failed in CUTBACK: error rate ok 0.00 within 0.0100; table write latency 3.53e+03 above 0.00us; ; the base partition is RESIDUE
```

The first attempt had read traffic only. The later attempts had reads and writes, and a `layout.baseline_window` of 60 seconds with a 5-second analyze interval. The baseline stayed at `0.00us` in every attempt, so the cause is most likely that no write-latency sample fell in the baseline window on an idle single node. That was not resolved. The key stayed split: `layout status` showed `STEADY`, base `RESIDUE`, and a reclaim due time. Approving the commit point while the action sat in `CUTBACK` was accepted (`Approved action ...`), but the guard still rolled the action back. Once the action has rolled back, `approve` reports `No action awaiting approval with id <id>`.

On a production node, `layout.baseline_window` defaults to 3 minutes and the guard has a baseline of real traffic. Watch `nodetool autonomic events <id>` for the guard's reason before you approve the commit point.

### 9.11 Configuration

```yaml
autonomic:
  actuators:
    partition_split: { enabled: true }
    partition_unsplit: { enabled: true }   # off by default; approval is still manual
  layout:
    commit_approval: manual                # the second approval, before BASE_ONLY (also for a split)
    # reclaim_window: 10d                  # time after a split before the base tombstone; default gc_grace/2 minus the margins
    # force_purge_compaction: true         # false: probe only, wait for natural compaction
    # purge_timeout: 6h                    # slow mode: wait for every base replica to purge clean
```

`partition_unsplit` is off by default. `partition_unsplit` and `purge_timeout` are not in `conf/cassandra.yaml`; their defaults are in code (`AutonomicConfig`). `reclaim_window` and `force_purge_compaction` are in `conf/cassandra.yaml`, commented out.

A reverse, like a split, needs cluster actions enabled: `nodetool autonomic cluster enable --yes` (4.2).

### 9.12 Verification notes for section 9

Run on one node, RF 1, one token range, from a `.build/build-jars.sh -s` build of this branch, with `autonomic.enabled: true`, `dry_run: false`, `partition_split` and `partition_unsplit` enabled, and the timing windows shortened for the test (`straggler_window: 10s`, `cutover_soak: 10s`, `verify_window: 20s`, `baseline_window: 10s` then `60s`, `default_buckets: 4`). The safety limits were raised for the test (`max_actions_per_hour`, `max_cluster_actions_per_hour`, `max_actions_per_actuator_per_hour`) and the rollback cooldown shortened to 1 minute. Those are test settings, not defaults.

Observed on the node:
- `nodetool autonomic layout unsplit` on a key that is not split, with and without `--when-ready`, and its events (9.3).
- `nodetool autonomic layout unsplit` on a split key in `STEADY` (proposal, `approve`), with the reverse run to `ROLLED_BACK` (9.2, 9.10).
- `nodetool autonomic layout resume` and `nodetool autonomic layout abort` on a key in `STEADY`.
- `nodetool autonomic layout status`, `nodetool autonomic history`, `nodetool autonomic history --diagnoses`, `nodetool autonomic events`, `nodetool autonomic approve`, `nodetool autonomic cluster enable --yes`.
- `nodetool help` for `autonomic layout unsplit`, `autonomic layout`, `autonomic layout abort`, `autonomic layout split` and `autonomic approve`: the output matches the help fixtures in `test/resources/nodetool/help/`.
- The reclaim due time in `layout status`, and the split with `layout split` and its approvals (the split is covered by section 5).

Not observed, and why:
- A reverse that reaches `BASE_ONLY` and `KEY_REMOVED`. Every attempt was rolled back in `CUTBACK` (9.10). Covered by `UnsplitEndToEndTest` (fast mode, the second approval and removal).
- The `--when-ready` `PENDING` record and `layout.unsplit_pending`, and rejections that name an earliest time. A slow mode needs a tombstone ten days old (gc_grace and the reclaim). Covered by `UnsplitTriggerTest`.
- Slow mode, `layout.unsplit_residue`, and every reason in the table of 9.5. Covered by `UnsplitSlowTest` and `UnsplitFailureTest`.
- `layout.split_not_effective` and the automatic unsplit proposal. Not raised within the test window. Covered by `UnsplitTriggerTest`.
- `layout.unsplit_orphaned` and `mode resume` of a reverse. Needs a second node.
- The `never_purge_tombstones` and `RECONCILE_PAST_DEADLINE` cases: code only (9.5, 9.6).

Disagreements between the design and the code (the code is what is described above):
- The rejection text names an earliest time only for some cases. Other rejections (a key in another phase, a key not split) name no time, and `--when-ready` for them proposes the action at once.
