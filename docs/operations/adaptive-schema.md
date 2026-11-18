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

This guide is for operators of a fork node that runs the autonomic layer. It covers what a partition split does, what clients can and cannot rely on while a split exists, how to turn the feature on, the `nodetool autonomic layout` commands, and how to read `system_views.autonomic_layouts`.

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
- **Base rows stay on disk** after `CLEANUP` until the base partition tombstone is written. They are unreachable through the logical table. Reverse migration must not assume the base partition is empty (ADR-033).
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
- The abort path is the only way back before the commit point. There is no way back after it yet (section 6).

Past the commit point (here `CLEANUP`):

```
$ nodetool autonomic layout abort ks1 t 00000002
nodetool: The split of PARTITION:ks1.t:00000002 is past its commit point (phase CLEANUP): it cannot be aborted. The way back is a reverse migration, `nodetool autonomic layout unsplit`, which is not built yet (T6.17)
```

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
| `CLEANUP` | The owner waits `straggler_window`, copies late data, then writes the base partition tombstone. Base rows stay on disk until then (ADR-033). |
| `STEADY` | The split is done. The claim is released. |
| `ROLLBACK_DUAL`, `ABORTING` | Abort in progress. The key is removed from the table when the abort finishes. |

On the test node the migration of key 1 went from `ANNOUNCED` to `CUTOVER` in about 30 seconds (the baseline and `cas_drain` were short on a quiet node). The migration of key 2 used a `cutover_soak` of 30 seconds, not the default 10 minutes. Expect the defaults to take at least `cutover_soak` plus `straggler_window` (3 hours by default) on a real cluster. The test did not reach `STEADY`.

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

- **Reverse migration** (`layout unsplit`, T6.17). Until it exists, a split that is past the commit point is permanent. A split that verification finds ineffective stays split and raises `layout.split_not_effective` (ADR-025).
- **Registry export and import** for backup restores (R5). Until it exists, restore the keyspace and the layout registry together, and check the result with `autonomic_layouts` and the overflow tables.
- **Whole-table re-partitioning.** Only one partition is split at a time (ADR-024, question 6).
