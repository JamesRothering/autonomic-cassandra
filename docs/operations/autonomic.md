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

# Autonomic layer: operator guide

This guide is for the person who runs a node with the autonomic layer. Everything is off by default. Nothing in the layer changes your cluster until you enable it, turn dry-run off, and enable the specific actuator.

Split and unsplit of hot partitions (adaptive schema) have their own guide: [adaptive-schema.md](adaptive-schema.md). This guide shows how those actions pass the gates and how to follow them, and leaves their phases, limits and failure modes to that guide.

Every command and output below was run against one real node built from this branch (`.build/build-jars.sh -s`), with the layer enabled in a scratch copy of `conf/`. Output is trimmed where it is long. Steps that need two or more nodes are marked "not run on one node" and name the dtest that covers them.

Contents: [1 Configuration](#1-configuration), [2 Gates](#2-gates), [3 Kill switch](#3-the-kill-switch), [4 nodetool autonomic](#4-nodetool-autonomic), [5 Rollback and history](#5-rollback-and-reading-the-record), [6 Sampler contention](#6-sampler-contention), [7 Cluster enable](#7-one-way-cluster-enable), [8 Folding values into yaml](#8-folding-committed-values-into-cassandrayaml), [9 Known limitations](#9-known-limitations), [10 Load testing](#10-load-testing).

## 1. Configuration

The settings are in the `autonomic:` section of `conf/cassandra.yaml` (commented out, near the end of the file). The section is read at startup. `enabled` cannot be changed with `nodetool`: change it in the file and restart the node. While `enabled` is false no threads are created.

To turn the layer on in dry-run mode (recommended first step), add:

```yaml
autonomic:
  enabled: true
  dry_run: true          # plan and record, never act
```

In dry-run the layer observes, diagnoses and plans. Every plan is recorded as an action in state `REJECTED` with reason `DRY_RUN` (section 5). Turn dry-run off at runtime with `nodetool autonomic dryrun off` (section 2). An action runs only when all of these are true:

1. `enabled: true`.
2. `dry_run: false`.
3. The actuator for that action is enabled: `actuators.<name>.enabled: true`, or `nodetool autonomic actuator <name> enable` until the next restart.

After a start the layer is in state `STARTING` and not armed until `startup_grace` (default 5m) has passed:

```
$ nodetool autonomic status
State: STARTING
Enabled: true
Dry run: true
Armed: false
Paused: false
Killed: false
In-flight actions: 0
Awaiting approval: 0
Cluster actions enabled: false
```

The settings that matter most:

| Setting | Default | What it does |
|---|---|---|
| `enabled` | `false` | Master switch. Restart to change. |
| `dry_run` | `true` | Plan and record only. |
| `startup_grace` | `5m` | Time after start before the layer is armed. |
| `monitor_interval`, `analyze_interval` | `5s`, `15s` | How often it measures and diagnoses. |
| `slo.read_latency_p99`, `slo.write_latency_p99` | `50ms`, `20ms` | Your latency targets. Set them to your real SLO: they drive `latency_slo_breach` and the verdict of an action. |
| `safety.max_actions_per_hour` | `6` | Node-scoped action rate (section 2). |
| `safety.node_approval`, `safety.cluster_approval` | `auto`, `manual` | Whether an action waits for `nodetool autonomic approve`. |
| `actuators.<name>.enabled` | `false` | Per-actuator gate. `nodetool autonomic actuators` lists them. |
| `actuators.<name>.min_factor`, `max_factor` | per actuator | The envelope: the layer moves a knob only between configured value times min and max. |
| `sampling.enabled`, `window`, `interval` | `true`, `10s`, `60s` | Partition sampling for hot-partition detection (section 6). |
| `detectors.<name>` | all enabled | Per-detector thresholds. |
| `history_ttl` | `30d` | How long actions and diagnoses are kept. |

The `safety:` block in the template shows only a few limits. The others are also read from `safety:` (names from `AutonomicConfig`): `max_actions_per_actuator_per_hour` (2), `max_cluster_actions_per_hour` (2), `max_step_fraction` (0.25), `cooldown_after_commit` (10m), `cooldown_after_rollback` (30m), `cooldown_max` (6h), `quarantine_after_rollbacks` (3), `quarantine_window` (1h), `quarantine_duration` (6h), `breaker_rollbacks` (4), `breaker_window` (6) and `proposal_ttl` (1h).

Check the settings the node read: `nodetool autonomic actuators` lists every actuator, its scope (`NODE` or `CLUSTER`), whether it is irreversible and whether it is enabled:

```
$ nodetool autonomic actuators
advertised_severity: NODE disabled
compaction_throughput: NODE enabled
...
garbage_collect: NODE irreversible disabled
partition_split: CLUSTER irreversible enabled
partition_unsplit: CLUSTER irreversible disabled
...
view_mutation_pool: NODE disabled
```

## 2. Gates

An action is a proposal from a policy (or from you, for a split) that is checked by the safety governor in a fixed order. The first check that fails rejects the action, and its name is the `state_reason` in `history`. Reasons you will see, in order: `KILLED`, `PAUSED`, `NOT_ARMED`, `DRY_RUN`, `ACTUATOR_DISABLED`, `CLUSTER_DISABLED`, `VALIDATION`, `NODE_STATE`, `STORE_UNAVAILABLE`, `QUARANTINED`, `COOLDOWN`, `CONFLICT`, `CONCURRENCY`, `RATE`, `BLAST_RADIUS`, `APPROVAL`.

### Dry-run

```
$ nodetool autonomic dryrun
Dry run: true
$ nodetool autonomic layout split ks1 t 00000001
id=dbcf7c70-... state=REJECTED state_reason=DRY_RUN actuator=partition_split scope=CLUSTER target=PARTITION:ks1.t:00000001 params={mode=split} policy=operator ... dry_run=true
$ nodetool autonomic dryrun off
Dry run set to false
```

`dryrun on|off` lasts until restart. The yaml value applies at the next start.

### Manual approval

With `node_approval: manual` or `cluster_approval: manual` (cluster actions are manual by default) an action that passes the other checks waits in state `PROPOSED`. `status` shows it as awaiting approval. It expires after `proposal_ttl` (1h) with reason `PROPOSAL_TTL`.

```
$ nodetool autonomic layout split ks1 t 00000001
id=f2797cf0-... state=PROPOSED state_reason= actuator=partition_split scope=CLUSTER ...
$ nodetool autonomic status
...
Awaiting approval: 1
$ nodetool autonomic reject f2797cf0-e4f3-11f1-8f34-b54de0faee1a
Rejected action f2797cf0-e4f3-11f1-8f34-b54de0faee1a
$ nodetool autonomic reject f2797cf0-e4f3-11f1-8f34-b54de0faee1a
nodetool: No action awaiting approval with id f2797cf0-e4f3-11f1-8f34-b54de0faee1a
```

A rejected action ends `REJECTED` with `OPERATOR_REJECTED`. `approve <id>` works the same way and prints `Approved action <id>`. An id that is not a UUID is refused (`Invalid UUID string`). A split also needs a second approval at its commit point; see the adaptive-schema guide.

`approve` is refused while the layer is paused or killed. It fails with an error that names the pause reason, and the action stays `PROPOSED`. Resume the layer first (or reject the action):

```
$ nodetool autonomic pause maintenance-window
$ nodetool autonomic approve 0286d520-e4f4-11f1-8f34-b54de0faee1a
error: Cannot approve action 0286d520-e4f4-11f1-8f34-b54de0faee1a: the autonomic layer is paused (reason: maintenance-window); the action stays PROPOSED. Resume the layer first, or reject the action
```

If the layer is paused or killed after you approved but before the action ran, the action still ends `REJECTED` with `PAUSED` or `KILLED`. Check `history` afterwards.

### Rate, cooldown and blast-radius limits

| Limit | Default | Reason on rejection |
|---|---|---|
| Rate | 6 node actions/hour, 2 per actuator per hour, 2 cluster actions/hour | `RATE` |
| Cooldown | 10m after a commit, 30m after a rollback, doubling on consecutive rollbacks up to 6h, per target | `COOLDOWN` |
| Quarantine | a policy with 3 rollbacks in 1h is stopped for 6h | `QUARANTINED` |
| Concurrency | 1 node action in flight; 1 cluster action per lane | `CONCURRENCY` |
| Blast radius | a knob moves at most 25% of its current value per step and stays inside its envelope; at most one node per topology action | `BLAST_RADIUS` |

Observed on the node with the defaults. After two split proposals were approved and aborted within the hour, the third key was refused, and a key that had just been rolled back was in cooldown:

```
$ nodetool autonomic layout split ks1 t 00000005     # rolled back minutes ago
id=60987280-... state=REJECTED state_reason=COOLDOWN actuator=partition_split ...
$ nodetool autonomic layout split ks1 t 00000009
id=6242a010-... state=REJECTED state_reason=RATE actuator=partition_split ...
```

`QUARANTINED`, `CONCURRENCY` and `BLAST_RADIUS` were not run on the node. `DefaultSafetyGovernorTest` (unit) covers each limit.

## 3. The kill switch

| Command | Effect |
|---|---|
| `nodetool autonomic pause <reason>` | No new action is approved. Sensors and detectors keep running. Actions in flight finish and are verified. |
| `nodetool autonomic kill <reason>` | Pauses, and rolls back every in-flight node-scoped action. |
| `nodetool autonomic resume` | Allows new actions. Clears a kill, a self-pause and the circuit breaker's count. |

Give the reason as one word, such as `maintenance-window`. The `bin/nodetool` script splits arguments on spaces, so `pause "maintenance window"` fails with `Unmatched argument at index 5: 'window'`. This is how `bin/nodetool` treats every command, not specific to the layer.

```
$ nodetool autonomic pause maintenance-window
Autonomic layer paused: maintenance-window
$ nodetool autonomic status
State: PAUSED
Enabled: true
Dry run: false
Armed: true
Paused: true (reason: maintenance-window)
Killed: false
...
$ nodetool autonomic kill operator-test
Autonomic layer killed: operator-test
$ nodetool autonomic status
State: PAUSED
...
Paused: true (reason: operator-test)
Killed: true
$ nodetool autonomic layout split ks1 t 00000003
id=5ba996b0-... state=REJECTED state_reason=KILLED actuator=partition_split ...
$ nodetool autonomic resume
Autonomic layer resumed
```

The pause is stored on the node and survives a restart.

### Why is the layer paused?

`status` prints `Paused: true (reason: <text>)`. The reason is what an operator typed, or one the layer chose itself. The same fields are in `system_views.autonomic_status`:

```
cqlsh> SELECT name, value FROM system_views.autonomic_status WHERE name IN ('state','paused','pause_reason','killed');
 name         | value
--------------+----------
       killed |    false
 pause_reason | why-test
       paused |     true
        state |   PAUSED
```

The layer pauses itself in three cases. Each raises a CRITICAL diagnosis (see `nodetool autonomic history --diagnoses`) and waits for you to `resume`:

| `pause_reason` | Cause | Diagnosis |
|---|---|---|
| `CIRCUIT_BREAKER` | 4 of the last 6 actions were rolled back or failed (`breaker_rollbacks`, `breaker_window`). | `autonomic.circuit_breaker` |
| `ACTION_FAILED` | An action failed: its rollback threw, or it lost its claim. | `autonomic.action_failed` |
| `CLAIM_LOST` | The node lost the cluster claim of its own action. | `autonomic.action_failed` |

Find the cause, fix it, then resume. Resuming forgets the breaker's count, so a layer that resumes while the problem is still there trips again. Cooldowns and quarantines stay.

The circuit breaker is not run on a node here: it needs four regressing actions. `CircuitBreakerFaultTest` covers it. It asserts `pause_reason` `CIRCUIT_BREAKER`, `paused` true, `killed` false, the persisted pause, the diagnosis, and that the next proposal is rejected `PAUSED`.

## 4. nodetool autonomic

Every subcommand follows. They match the help fixtures `test/resources/nodetool/help/autonomic*`. Add the usual `-h`, `-p`, `-u`, `-pw` options for a remote node.

| Subcommand | What it does |
|---|---|
| `status` | State, gates, in-flight and awaiting actions. |
| `pause <reason>` | Stop approving new actions. |
| `resume` | Allow new actions; clears a kill and the breaker. |
| `kill <reason>` | Pause and roll back in-flight node actions. |
| `dryrun [on\|off]` | Print or set dry-run. |
| `approve <action-id>` / `reject <action-id>` | Decide a held proposal. |
| `actuators` | List actuators, scope and gate. |
| `actuator <name> <enable\|disable>` | Gate one actuator until restart. |
| `revert` | Restore every managed knob to its configured value. |
| `history [--limit N] [--diagnoses]` | Recent actions (7 days) or diagnoses (1 day); default 20 records. |
| `events <action-id>` | Audit trail of one action, oldest first. |
| `cluster status\|enable\|disable\|release` | Cluster actions gate and claims (section 7). |
| `layout status\|split\|abort\|resume\|unsplit` | Adaptive schema (see [adaptive-schema.md](adaptive-schema.md)). |

`status` after the layer is armed and a split is in flight:

```
$ nodetool autonomic status
State: RUNNING
Enabled: true
Dry run: false
Armed: true
Paused: false
Killed: false
In-flight actions: 1
Awaiting approval: 0
Cluster actions enabled: true
```

`actuator` gates one actuator without a restart (and reports an unknown name):

```
$ nodetool autonomic actuator compaction_throughput disable
Actuator compaction_throughput disabled
$ nodetool autonomic actuators | grep compaction_throughput
compaction_throughput: NODE disabled
$ nodetool autonomic actuator compaction_throughput enable
Actuator compaction_throughput enabled
$ nodetool autonomic actuator nosuch enable
nodetool: Unknown actuator: nosuch
```

`revert` restores every managed knob to its `cassandra.yaml` value, and prints `Reverted all managed knobs` (section 8). `layout` subcommands take `<keyspace> <table> <key-hex>`; the key is the raw partition key bytes in hex (for an `int` key 1, `00000001`):

```
$ nodetool autonomic layout status
No records.
$ nodetool autonomic layout split ks1 t 00000001
id=26e2b1a0-... state=PROPOSED ...
$ nodetool autonomic approve 26e2b1a0-e4f4-11f1-8f34-b54de0faee1a
Approved action 26e2b1a0-e4f4-11f1-8f34-b54de0faee1a
$ nodetool autonomic layout status
table=ks1.t key=00000001 phase=ANNOUNCED overflow=ks1.t_al_40d6f111 buckets=4 incarnation=1 generation=1 backfill_done=[] action=26e2b1a0-... status=running on node 1; waits for hint reports (ADR-035), 0 of 1 nodes reported: [] orphaned=false
$ nodetool autonomic layout abort ks1 t 00000001
Abort of PARTITION:ks1.t:00000001 requested in phase ANNOUNCED; action 26e2b1a0-... takes the abort path
```

A split needs `actuators.partition_split.enabled: true` and cluster actions enabled (section 7). A reverse needs `partition_unsplit` enabled too: with it off the proposal is rejected `ACTUATOR_DISABLED`, and `unsplit --when-ready` then says `The proposal was rejected for the same reason as an identical one just before; see autonomic history`. `layout resume` on a key that is not split is rejected `VALIDATION`. See [adaptive-schema.md](adaptive-schema.md) for phases, `unsplit`, `--buckets` and `--when-ready`.

## 5. Rollback and reading the record

### Verify and roll back

Every node-scoped action is verified, then kept or undone:

1. **Baseline.** Before it acts, the layer records the goal signal and the guard signals over `baseline_window` (default 2m). For an action on this node, the baseline starts at the first check that saw the problem (the diagnosis's `breach_since`) when that is more recent, so it is not diluted by the calm before; the policy proposes only once the period holds enough samples (it waits, `waiting_for_baseline` in the debug log). Too few samples reject the action as `INSUFFICIENT_BASELINE`.
2. **Apply.** The undo is stored before the action runs, so a rollback needs nothing else.
3. **Verify.** After `observation_window` (default 5m) the layer compares the same signals. `IMPROVED` commits the action (`COMMITTED`). `REGRESSED`, `NEUTRAL` and `INCONCLUSIVE` roll it back (`ROLLED_BACK`). A change must earn its place.
4. **Early abort.** A guard far beyond its tolerance rolls the action back at once.
5. **Dead-man deadline.** If the control thread stalls, the action still rolls back when its window and a grace period end.
6. **Overridden.** If you changed the same knob during the window the action ends `SUPERSEDED` and the layer does not touch your value.

A rollback restores exactly the values the action changed. Terminal states: `COMMITTED`, `ROLLED_BACK`, `REJECTED`, `SUPERSEDED`, `FAILED`. A `FAILED` action raises `autonomic.action_failed` and pauses the layer. Irreversible actuators (`garbage_collect`, `repair_priority`, `scale_out`, split and unsplit past their commit point) are verified for the record only and are not rolled back. A cluster action has no dead-man timer: it verifies itself, and its lease and recovery take that role.

`kill` rolls back all in-flight node actions at once. `revert` is different: it restores every managed knob to its configured value, including values from actions that already committed (section 8).

### `history`, `events` and the virtual tables

`history` prints one line per action, newest first, from this node only:

```
$ nodetool autonomic history --limit 3
id=60987280-... state=REJECTED state_reason=COOLDOWN actuator=partition_split scope=CLUSTER target=PARTITION:ks1.t:00000005 params={mode=split} policy=operator diagnosis_id=... dry_run=false verdict= baseline={} result={} proposed_at=... updated_at=...
```

`verdict`, `baseline` and `result` are filled when an action was verified. `policy=operator` means you proposed it. `events <id>` shows every state change of one action, oldest first, with the cause in `detail`:

```
$ nodetool autonomic events 26e2b1a0-e4f4-11f1-8f34-b54de0faee1a
at=... from_state=null to_state=PROPOSED detail=plan 9e108d3d-... by operator: nodetool autonomic layout split
at=... from_state=PROPOSED to_state=APPROVED detail=operator
at=... from_state=APPROVED to_state=APPLYING detail=claimed lane SCHEMA_LAYOUT
at=... from_state=APPLYING to_state=APPLYING detail=phase ANNOUNCED (step 3)
at=... from_state=APPLYING to_state=APPLYING detail=phase ABORTING (step 5)
at=... from_state=APPLYING to_state=ROLLED_BACK detail=aborted by operator command
```

`history` of that action ends with `state=ROLLED_BACK state_reason=ACTUATOR_ROLLBACK`. `at=` is milliseconds since the epoch. `history --diagnoses` prints diagnoses of the last day:

```
$ nodetool autonomic history --diagnoses
id=3664eb60-... type=autonomic.baseline_drift subject=NODE severity=INFO state=OPEN first_seen=... last_seen=... summary=1 knob(s) differ from cassandra.yaml: compaction_throughput=32 (configured 64)
```

The same data is in `system_views`, all read-only and local to the node you ask:

| Table | Content |
|---|---|
| `autonomic_status` | `name`, `value`: state, dry_run, paused, pause_reason, killed, in_flight_actions, awaiting_approval, cooldowns, quarantined_policies, store_available and counters. |
| `autonomic_actions` | Actions of the last 7 days: `id`, `state`, `state_reason`, `actuator`, `target`, `policy`, `verdict`, `baseline`, `result`, `params`. Listed oldest first; `history` lists newest first. |
| `autonomic_diagnoses` | Open and recent diagnoses: `type`, `subject`, `severity`, `state`, `summary`. |
| `autonomic_signals` | The latest observation of each signal and subject. |
| `autonomic_components` | Each sensor, detector and policy: `kind`, `name`, `enabled`, `healthy`, `consecutive_failures`. |
| `autonomic_cluster_ledger` | Cluster claims by lane. |
| `autonomic_layouts` | One row per split partition. |

```
cqlsh> SELECT id, state, state_reason, actuator, target, policy FROM system_views.autonomic_actions LIMIT 2;
 id                                   | state    | state_reason | actuator        | target                   | policy
--------------------------------------+----------+--------------+-----------------+--------------------------+----------
 dbcf7c70-e4f3-11f1-8f34-b54de0faee1a | REJECTED |      DRY_RUN | partition_split | PARTITION:ks1.t:00000001 | operator

cqlsh> SELECT kind, name, enabled, healthy FROM system_views.autonomic_components LIMIT 2;
 kind     | name               | enabled | healthy
----------+--------------------+---------+---------
 detector |     baseline_drift |    True |    True
 detector | capacity_exhausted |    True |    True
```

A component that fails repeatedly is disabled by the layer; `resume` re-enables it. Each state change is also logged at INFO with the action id, and failures at WARN, in `system.log`.

## 6. Sampler contention

The hot-partition sensor samples reads and writes of busy tables: a window of `sampling.window` (10s) every `sampling.interval` (60s), only for tables above `sampling.min_ops_per_sec`. Cassandra allows one sampling window per sampler at a time, and `nodetool toppartitions` and `nodetool profileload` use the same samplers. If one of them starts while the layer's window is open, it fails:

```
$ nodetool toppartitions autonomic_stress hotspot 300
error: Sampling already in progress
-- StackTrace --
java.lang.RuntimeException: Sampling already in progress
	at org.apache.cassandra.metrics.FrequencySampler.beginSampling(FrequencySampler.java:58)
```

The layer yields the other way round: it checks for an active window and skips the table for that round, and a failed start is not counted as a sensor failure. So the failure is always yours, never the layer's.

On the node, with `cassandra-stress` driving a hot table (section 10) and the same command in a loop, the second try hit the layer's window. The same error appears when two operators overlap, `toppartitions` against `profileload` for instance. What to do:

- Retry. The layer's window is 10s of every 60s, so the next attempt usually works. For example: `until nodetool toppartitions ks t 5000; do sleep 3; done`.
- For a long manual session, set `autonomic.sampling.enabled: false` and restart; hot-partition detection then has no data.
- Or pause the layer: `pause` does not stop the sensors, so it does not free the sampler.

The overview calls the command `nodetool profileload`. `toppartitions` is the old name; both exist in this build and both collide.

## 7. One-way cluster enable

Actions that touch the whole cluster (split, unsplit, scale-out, repair priority) use a ledger kept in the cluster metadata log. `nodetool autonomic cluster` operates it:

```
$ nodetool autonomic cluster status
cluster_actions_enabled: false
epoch: 3
lane.MAINTENANCE: free
lane.SCHEMA_LAYOUT: free
lane.TOPOLOGY: free
ledger_format: 1
ledger_present: false
node_id: 1
preferred_proposer: true
recent_actions: 0
Claims:
No records.
```

`enable` needs `--yes`. Without it nothing changes:

```
$ nodetool autonomic cluster enable
Once enabled, the cluster metadata log contains entries that stock Cassandra nodes cannot read.
Every node of the cluster must run this fork before and after enabling.
Re-run with --yes to enable cluster actions.
$ nodetool autonomic cluster enable --yes
Cluster actions enabled
$ nodetool autonomic cluster status | grep ledger
ledger_present: true
```

What is one-way: until `enable`, nothing is written to the metadata log. The first `enable --yes` commits a ledger entry. A stock Apache Cassandra node cannot read it, and the entry cannot be removed. `disable` stops new claims but leaves it:

```
$ nodetool autonomic cluster disable
Cluster actions disabled
$ nodetool autonomic cluster status | grep -E "enabled|ledger"
cluster_actions_enabled: false
ledger_format: 1
ledger_present: true
```

So do not add a stock node to the cluster, or roll back to a stock build, after `enable --yes`. The command is refused if the nodes run different versions. Enable it from one node; the flag is cluster-wide.

`cluster release <lane>` forcibly removes a claim: the lane is `TOPOLOGY`, `SCHEMA_LAYOUT` or `MAINTENANCE`. Use it only when the owner of an action is gone and `cluster status` still shows the claim after its lease (default 10m) ran out. A lane with no claim:

```
$ nodetool autonomic cluster release TOPOLOGY
nodetool: Lane TOPOLOGY is not claimed
```

A claim is shown as `lane.SCHEMA_LAYOUT: partition_split PARTITION:ks1.t:00000001 action 26e2b1a0-... owner 1 phase ANNOUNCED step 3 lease until <ms>`. Releasing a live claim of a running action was not run. The cluster behavior with two or more nodes (preferred proposer, claim handover, lease expiry, release of a dead owner's claim) was not run on one node: `ClusterLedgerTest` and `OwnerKilledFaultTest` cover it.

## 8. Folding committed values into cassandra.yaml

When an action commits, the new value is an in-memory override. It is not written to `cassandra.yaml`, and a restart returns to the file. While a knob differs from the file, the `baseline_drift` detector keeps an INFO diagnosis open:

```
$ nodetool setcompactionthroughput 32        # stands in for a committed change
$ nodetool autonomic history --diagnoses
id=3664eb60-... type=autonomic.baseline_drift subject=NODE severity=INFO state=OPEN ... summary=1 knob(s) differ from cassandra.yaml: compaction_throughput=32 (configured 64)
```

(A change you make by hand is reported the same way, and an action in flight on the knob is not reported.) The diagnosis names the knob but not the value. To fold a value into the file:

1. Find the drifting knob in `history --diagnoses` (`autonomic.baseline_drift`) and the action that moved it in `history` (state `COMMITTED`).
2. Read the live value with the usual `nodetool` getter (table below).
3. Put that value in `cassandra.yaml` on every node where you want it. It takes effect at the next restart.
4. Until that restart, the node still has the old file value as its configured value. `nodetool autonomic revert` would put the old value back, so do not run it before you restart.

| Actuator | Read it with | `cassandra.yaml` key |
|---|---|---|
| `compaction_throughput` | `nodetool getcompactionthroughput` | `compaction_throughput` |
| `concurrent_compactors` | `nodetool getconcurrentcompactors` | `concurrent_compactors` |
| `stream_throughput` | `nodetool getstreamthroughput -d` | `stream_throughput_outbound` |
| `hint_throttle` | no `nodetool` getter; read the `HintedHandoffThrottleInKB` attribute of the `StorageService` MBean over JMX | `hinted_handoff_throttle` |
| `key_cache_capacity`, `row_cache_capacity`, `counter_cache_capacity` | `nodetool info` (cache lines) | `key_cache_size`, `row_cache_size`, `counter_cache_size` |
| `read_pool`, `mutation_pool`, `counter_mutation_pool`, `view_mutation_pool` | `nodetool getconcurrency read` (also `mutation`, `counter_mutation`, `view_mutation`) | `concurrent_reads`, `concurrent_writes`, `concurrent_counter_writes`, `concurrent_materialized_view_writes` |
| `request_throttle` | no `nodetool` getter; read `NativeTransportRateLimitingEnabled` on the `StorageService` MBean over JMX | `native_transport_rate_limiting_enabled`, `native_transport_max_requests_per_second` |
| `advertised_severity` | gossip `SEVERITY` (`nodetool gossipinfo`) | not a yaml setting |

The key names above come from the Cassandra settings behind each knob; check them against your yaml. Live values read on the node: `nodetool getcompactionthroughput` printed `Current compaction throughput: 64 MiB/s`, `nodetool getconcurrentcompactors` printed `2`, and `nodetool getconcurrency read` printed `ReadStage 0 32`. The other getters were not exercised. `nodetool getstreamthroughput` without `-d` printed an error that asks for `-d`.

If you do not want a value, run `nodetool autonomic revert`:

```
$ nodetool setcompactionthroughput 32
$ nodetool getcompactionthroughput | head -1
Current compaction throughput: 32 MiB/s
$ nodetool autonomic revert
Reverted all managed knobs
$ nodetool getcompactionthroughput | head -1
Current compaction throughput: 64 MiB/s
```

Within a few analyze intervals the diagnosis turns `CLEARED`. `revert` restores every managed knob, not only drifting ones. An action in flight on a reverted knob ends `SUPERSEDED`. No action was committed on the test node (it was idle), so the committed path was shown with a manual change, which the detector treats the same way.

## 9. Known limitations

### From fault injection (T7.2, `docs/deliverables/D7-verification.md`)

Every fault tested ends fail-static. Three findings are fixed: a `CLAIM_LOST` recorded during shutdown used to lose its `action_failed` diagnosis (it is now written on the shutdown path), and a failed audit write used to stay unwritten after the knowledge store recovered. Now the first successful write afterwards rewrites the latest state of each terminal action whose row is stale and deletes its `inflight` key, up to 64 pending entries; anything beyond that is left to restart recovery.

The third fix (T7.7, ADR-038): an owner that restarts after its lease expired, while peers still see it alive, pauses on `CLAIM_LOST`. It is still the preferred proposer, and it used to be the only node allowed to take over the expired claim, so the lane stayed claimed until an operator acted. Now the next node in id order takes the claim over one `lease_safety_margin` after expiry (the node after it two margins after, and so on) and rolls it back or finishes it. The same applies when the preferred proposer is paused or killed for any other reason. Expect such a recovery up to `rank × lease_safety_margin` after the lease expired. A ledger commit to an unreachable CMS gives up at half of `action_step_timeout`, so it does not pause the node as a stalled actuator thread. The paused owner itself stays paused: check its `action_failed` diagnosis, then `nodetool autonomic resume`. `ExpiredClaimTakeoverFaultTest` covers it.

### cqlsh cannot read `system_autonomic` (T7.5)

`SELECT` on `system_autonomic.*` in `cqlsh` fails with `Keyspace 'system_autonomic' not found.`, although `DESCRIBE KEYSPACES` lists it. This is not specific to the layer. `cqlsh` formats results with table metadata that the python driver loads from `system_schema`, and Cassandra writes only `system` and `system_schema` of its local (`LocalStrategy`) keyspaces to `system_schema`. `system_autonomic` is registered as a local system keyspace like the others (`SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES`), so the server reads and writes it, but the driver never sees it. `select keyspace_name from system_schema.keyspaces` does not list it, while `DESCRIBE` is answered by the server and does. Changing this needs a change to core schema handling, not to the layer, and was not made. Read the same data with `nodetool autonomic history` (add `--diagnoses`, or `events <id>`), or with `SELECT` on the `system_views.autonomic_*` virtual tables, which `cqlsh` can read. `DESCRIBE KEYSPACE system_autonomic` shows the table definitions.

### From reverse migration (T6.17)

Details are in section 9 of the [adaptive-schema guide](adaptive-schema.md#99-known-limitations).

- A `--when-ready` request is kept only on the node that ran the command. If that node's local state is lost, so is the request. Check `nodetool autonomic history --diagnoses` for `layout.unsplit_pending`, and run the command again if it is missing.
- Gossip application state `X1` is reserved for the layer. Do not write to it.
- After a rolled-back reverse, the key waits `safety.cooldown_after_rollback` (30m) before the next proposal. Wait, or look at `events` for the reason it rolled back.
- The `CUTBACK` guard compares with a baseline over `layout.baseline_window` (3m). On a node with no write traffic in that window the baseline is zero and the reverse rolls back. Expect this on an idle test node. On a loaded node it does not happen. Read `events <id>` before you approve the commit point of a reverse.

## 10. Load testing

`tools/autonomic/stress/` holds three `cassandra-stress` profiles that recreate the scenarios of the dtests. Each profile names its own keyspace, so it does not touch your tables:

| Profile | Scenario | Command |
|---|---|---|
| `hot-spot.yaml` | one wide partition takes most of the traffic | `tools/bin/cassandra-stress user profile=tools/autonomic/stress/hot-spot.yaml "ops(hot_point=6,hot_slice=2,hot_write=1,cold_point=1)" n=20000 -rate threads=8 -node 127.0.0.1` |
| `load-spike.yaml` | a burst of reads and writes | `tools/bin/cassandra-stress user profile=tools/autonomic/stress/load-spike.yaml "ops(spike_write=1,spike_read=1)" n=20000 -rate threads=8 -node 127.0.0.1` |
| `degraded-node.yaml` | a write-heavy mix that builds a compaction backlog | `tools/bin/cassandra-stress user profile=tools/autonomic/stress/degraded-node.yaml "ops(backlog_write=9,backlog_read=1)" n=20000 -rate threads=8 -node 127.0.0.1` |

Only `hot-spot.yaml` was run on the node, with `duration=150s` in place of `n=20000`. It created `autonomic_stress.hotspot` and drove about 2,000 operations a second. In that time, with the layer enabled, no `hot_partition` diagnosis was raised (it needs several evaluations over a busier table; the thresholds are in the `detectors:` section). Run against a test cluster, with `dry_run: true`, and watch `nodetool autonomic history --diagnoses` and the virtual tables. The in-JVM scenario dtests (`HotSpotScenarioTest`, `LoadSpikeScenarioTest`, `RealLoadSpikeScenarioTest`, `DegradedNodeScenarioTest`) assert the full diagnosis, action and verdict.

### Making the layer propose a split by itself (split demo)

`tools/autonomic/split-demo/` shows the layer proposing a partition split on a real single node, with no operator command for the split itself:

| File | Content |
|---|---|
| `autonomic-split-demo.yaml` | Overlay for `cassandra.yaml`: the layer on, `dry_run: false`, `partition_split` enabled, short sampler windows and `layout` thresholds a small node reaches in minutes (`min_rows_for_split: 500`, `profile_window: 15s`, `min_samples: 50`). `safety.cluster_approval` stays `manual`. Do not use these values in production. |
| `hot-spot-wide.yaml` | `cassandra-stress` profile: partition 1 takes nearly all operations and each row has a 6000 byte payload. |
| `split-demo.sh` | Copies `conf/` to a scratch directory (`CASSANDRA_CONF`), appends the overlay and storage paths inside the scratch directory (`bin/cassandra.in.sh` fixes `cassandra.storagedir`, so the yaml names every directory), starts the node with a 768M heap and the layer off, loads the hot partition, restarts the node with the overlay, runs `nodetool autonomic cluster enable --yes`, drives the hot partition, polls `nodetool autonomic history` for a `partition_split` action, prints it and the `hot_partition` diagnosis, stops the node and removes the scratch directory (`KEEP=1` keeps it). Exit 0: proposal seen; 1: timeout; 2: setup failure. |

Run it from the checkout after `.build/build-jars.sh -s`: `tools/autonomic/split-demo/split-demo.sh` (about 4 minutes). The split is a cluster action and the cluster gate is manual, so the action stays `PROPOSED`; nothing is split unless you approve it on a node you keep running.

A single node has nowhere to move load to, so the policy proposes a split there only for a partition of at least 100 MiB (otherwise the diagnosis carries `layout=not_applicable(no_spread)`). That is why the profile writes 6000 byte rows: about 24,000 rows pass 100 MiB. The partition is loaded before the layer starts because the policy probes a partition's size once and keeps the result for an hour: a probe taken while the load is still running judges the partition too small, and a demo that loaded and drove it with the layer on never got a proposal. On two or more nodes any partition above `layout.min_rows_for_split` rows qualifies. A run on this branch's single node proposed the split 50 s after the hot traffic began: `partition_split`, `PROPOSED`, `buckets=32`, `size_only=true`, target `PARTITION:autonomic_stress.hotwide:00000001`.
