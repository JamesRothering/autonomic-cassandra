/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test.autonomic.layout;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.coord.TcmEnvironment;
import org.apache.cassandra.autonomic.layout.AdvanceLayout;
import org.apache.cassandra.autonomic.layout.FinishKey;
import org.apache.cassandra.autonomic.layout.KeyState;
import org.apache.cassandra.autonomic.layout.LayoutHeader;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.PhysicalTables;
import org.apache.cassandra.autonomic.layout.RegisterKey;
import org.apache.cassandra.autonomic.layout.RemoveKey;
import org.apache.cassandra.autonomic.layout.RowMapper;
import org.apache.cassandra.autonomic.layout.TableLayout;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * T6.13: what the failure suite of adaptive-schema design 4.11 shares. Each scenario is its own class; this holds
 *
 * <ul>
 * <li>the <b>model</b>: the twin table of {@link LayoutCompatibilitySuite}, which is never split and receives every
 * acknowledged statement with the same timestamp, plus, per clustering, the writes and deletes whose outcome is
 * unknown (the client got an error): a read may show the model's row or any of them (4.11: never acked, never
 * lost);</li>
 * <li>a background <b>workload</b> through the live coordinators that writes, deletes and reads back through another
 * coordinator, checking each read against the model;</li>
 * <li>{@link Model#check}: invariants I5 (every acknowledged write and delete, read through every live coordinator), I7
 * (node-locally, every cell that both copies hold with the same timestamp is identical in value, TTL and local
 * deletion time), I10 (no node seen to go back from class C3; in STEADY a base-only row is never read) and I14 (each
 * live row once, in clustering order ascending and descending, with the same rows paged and unpaged, with logical
 * column names, and contiguous in a scan of the table);</li>
 * <li>driving a key by hand (as {@link LayoutWritesTest}) or through the real actuator (as
 * {@link PartitionSplitEndToEndTest}).</li>
 * </ul>
 *
 * Every wait is on a condition, bounded by {@link #WAIT_MILLIS}; the only fixed waits are observation periods that
 * show something does <em>not</em> happen, each a small multiple of the configured {@code ack_poll_interval}.
 */
public abstract class SplitFailureSupport extends TestBaseImpl
{
    static final String KS = "fail_ks";
    static final String COLUMNS = " (p int, c int, v int, s text, m map<int, text>, PRIMARY KEY (p, c))";
    static final long WAIT_MILLIS = 240_000;
    /** The owner's poll interval in these tests; observation periods are multiples of it. */
    static final long POLL_MILLIS = 200;
    /** Clusterings the scenario writes itself; the workload stays below. */
    static final int SCENARIO_ROWS = 1000;
    /** A base-only row that no read of a key in STEADY may return (I10). */
    static final int POISON = -7;

    // ---------------------------------------------------------------------------------------------------------
    // configuration
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Short windows: request timeouts of 2 s (so the ADR-028 drain is 3 s), an ack poll of 200 ms, a straggler window
     * and soak of 1 s, automatic commit approval. {@code layout} and {@code extra} override; nodes in {@code enabled}
     * run the autonomic layer with the partition_split actuator, the others only its unconditional data plane.
     */
    static Consumer<IInstanceConfig> config(Set<Integer> enabled, Map<String, Object> layout, Map<String, Object> extra)
    {
        return c -> {
            Map<String, Object> l = new LinkedHashMap<>();
            l.put("ack_poll_interval", POLL_MILLIS + "ms");
            l.put("cas_drain", "100ms");
            l.put("cutover_soak", "1s");
            l.put("straggler_window", "1s");
            l.put("commit_approval", "auto");
            l.put("verify_recheck_delay", "200ms");
            l.put("backfill_throughput", "0MiB/s");
            l.putAll(layout);
            c.with(NETWORK, GOSSIP)
             .set("write_request_timeout", "2000ms")
             .set("counter_write_request_timeout", "2000ms")
             .set("cas_contention_timeout", "1000ms")
             .set("range_request_timeout", "2000ms")
             .set("read_request_timeout", "2000ms")
             .set("truncate_request_timeout", "2000ms")
             .set("autonomic.layout", l);
            for (Map.Entry<String, Object> e : extra.entrySet())
                c.set(e.getKey(), e.getValue());
            if (!enabled.contains(c.num()))
                return;
            c.set("autonomic.enabled", true)
             .set("autonomic.dry_run", false)
             .set("autonomic.startup_grace", "1s")
             .set("autonomic.monitor_interval", "500ms")
             .set("autonomic.analyze_interval", "500ms")
             .set("autonomic.sampling.enabled", false)
             .set("autonomic.safety", Map.of("max_actions_per_hour", 50, "max_actions_per_actuator_per_hour", 50, "max_cluster_actions_per_hour", 50,
                                             "cooldown_after_commit", "1s", "cooldown_after_rollback", "1s", "action_step_timeout", "5s"))
             .set("autonomic.actuators", Map.of("partition_split", Map.of("enabled", true)));
        };
    }

    /** As {@link #config}, with the partition_unsplit actuator enabled too (T6.17k). */
    static Consumer<IInstanceConfig> unsplitConfig(Set<Integer> enabled, Map<String, Object> layout, Map<String, Object> extra)
    {
        Consumer<IInstanceConfig> base = config(enabled, layout, extra);
        return c -> {
            base.accept(c);
            if (enabled.contains(c.num()))
                c.set("autonomic.actuators", Map.of("partition_split", Map.of("enabled", true), "partition_unsplit", Map.of("enabled", true)));
        };
    }

    /** For tests that drive the registry by hand: the layer runs on node 1 only, without actuators (LayoutWritesTest). */
    static Consumer<IInstanceConfig> handConfig(Map<String, Object> layout)
    {
        return c -> {
            c.with(NETWORK, GOSSIP)
             .set("autonomic.enabled", c.num() == 1)
             .set("write_request_timeout", "2000ms")
             .set("read_request_timeout", "2000ms")
             .set("range_request_timeout", "2000ms");
            if (!layout.isEmpty())
                c.set("autonomic.layout", layout);
        };
    }

    static Cluster start(int nodes, Consumer<IInstanceConfig> config) throws java.io.IOException
    {
        return init(Cluster.build(nodes).withConfig(config).start());
    }

    static void createTables(Cluster cluster, int rf, String... tables)
    {
        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KS + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + rf + '}');
        for (String table : tables)
        {
            cluster.schemaChange("CREATE TABLE " + KS + '.' + table + COLUMNS);
            cluster.schemaChange("CREATE TABLE " + KS + '.' + table + "_twin" + COLUMNS);
        }
    }

    static void await(String what, BooleanSupplier condition) throws InterruptedException
    {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MILLIS);
        while (!condition.getAsBoolean())
        {
            if (System.nanoTime() > deadline)
                throw new AssertionError("timed out waiting for " + what);
            Thread.sleep(50);
        }
    }

    static boolean quietly(BooleanSupplier condition)
    {
        try
        {
            return condition.getAsBoolean();
        }
        catch (Throwable t)
        {
            return false;
        }
    }

    static ByteBuffer bytes(int key)
    {
        return Int32Type.instance.decompose(key);
    }

    static String hex(int key)
    {
        return ByteBufferUtil.bytesToHex(bytes(key));
    }

    /** True if the node is a natural replica of the key of the table, as it sees the placements. */
    static boolean isReplica(IInvokableInstance node, String table, int key)
    {
        return node.callOnInstance(() -> {
            TableMetadata logical = Schema.instance.getTableMetadata(KS, table);
            KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(KS);
            DecoratedKey dk = logical.partitioner.decorateKey(bytes(key));
            for (InetAddressAndPort replica : ClusterMetadata.current().placement(ksm.params.replication).reads.forToken(dk.getToken()).get().endpoints())
                if (replica.equals(FBUtilities.getBroadcastAddressAndPort()))
                    return true;
            return false;
        });
    }

    /** The first key from 1 on whose replicas, as {@code node} sees them, are exactly what {@code wanted} says per node. */
    static int keyWithReplicas(Cluster cluster, String table, boolean... wanted)
    {
        for (int key = 1; key < 10_000; key++)
        {
            boolean match = true;
            for (int n = 1; n <= wanted.length && match; n++)
                match = isReplica(cluster.get(n), table, key) == wanted[n - 1];
            if (match)
                return key;
        }
        throw new AssertionError("no key with replicas " + Arrays.toString(wanted));
    }

    // ---------------------------------------------------------------------------------------------------------
    // the registry: phase as a node sees it
    // ---------------------------------------------------------------------------------------------------------

    /** The phase of the key as the node sees it; "absent" if it is not registered. */
    static String phaseOf(IInvokableInstance node, String table, int key)
    {
        return node.callOnInstance(() -> {
            TableMetadata logical = Schema.instance.getTableMetadata(KS, table);
            if (logical == null)
                return "absent";
            TableLayout layout = LayoutRegistry.of(ClusterMetadata.current()).layout(logical.id);
            KeyState state = layout == null ? null : layout.keyState(bytes(key));
            return state == null ? "absent" : state.phase.name();
        });
    }

    /** "generation:reports" of the key as the node sees it. */
    static String progress(IInvokableInstance node, String table, int key)
    {
        return node.callOnInstance(() -> {
            TableMetadata logical = Schema.instance.getTableMetadata(KS, table);
            TableLayout layout = LayoutRegistry.of(ClusterMetadata.current()).layout(logical.id);
            KeyState state = layout == null ? null : layout.keyState(bytes(key));
            return state == null ? "absent" : state.generation + ":" + state.backfillDone.size();
        });
    }

    static long phaseEpoch(IInvokableInstance node, String table, int key)
    {
        return node.callOnInstance(() -> {
            TableMetadata logical = Schema.instance.getTableMetadata(KS, table);
            TableLayout layout = LayoutRegistry.of(ClusterMetadata.current()).layout(logical.id);
            KeyState state = layout == null ? null : layout.keyState(bytes(key));
            return state == null ? -1L : state.phaseEpoch.getEpoch();
        });
    }

    static void awaitPhase(IInvokableInstance node, String table, int key, String phase) throws InterruptedException
    {
        await(phase + " of " + table, () -> phase.equals(phaseOf(node, table, key)));
    }

    static void awaitBackfill(IInvokableInstance node, String table, int key, int generation, int reports) throws InterruptedException
    {
        String expected = generation + ":" + reports;
        await("backfill " + expected + " of " + table, () -> expected.equals(progress(node, table, key)));
    }

    // ---------------------------------------------------------------------------------------------------------
    // the registry by hand (LayoutWritesTest, for any table and key)
    // ---------------------------------------------------------------------------------------------------------

    private static ClusterCoordinator coordinator()
    {
        return new ClusterCoordinator(new TcmEnvironment(Clock.Global::currentTimeMillis, DatabaseDescriptor::getAutonomicConfig));
    }

    static UUID layoutId(String table)
    {
        return UUID.nameUUIDFromBytes(("layout of " + table).getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /** Creates the overflow table of {@code table}, claims the lane on node 1 and registers the key: ANNOUNCED. */
    static String registerByHand(Cluster cluster, String table, int key, int buckets)
    {
        IInvokableInstance n1 = cluster.get(1);
        UUID layoutId = layoutId(table);
        String physical = PhysicalTables.physicalName(table, layoutId);
        String derived = n1.callOnInstance(() -> PhysicalTables.derive(Schema.instance.getTableMetadata(KS, table), layoutId, TableId.generate()));
        cluster.schemaChange(derived);
        n1.runOnInstance(() -> AutonomicService.instance.enableClusterActions(true));
        String id = n1.callOnInstance(() -> TimeUUID.Generator.nextTimeUUID().asUUID().toString());
        n1.runOnInstance(() -> {
            ClusterCoordinator.Outcome outcome = coordinator().claim(Lane.SCHEMA_LAYOUT, UUID.fromString(id), "partition_split", "TABLE:" + table,
                                                                    "CLAIMED", ImmutableMap.of());
            assertTrue(String.valueOf(outcome.detail), outcome.ok());
            ClusterCoordinator c = coordinator();
            Claim own = c.ownClaim(UUID.fromString(id));
            assertNotNull(own);
            TableId logical = Schema.instance.getTableMetadata(KS, table).id;
            TableId physicalId = Schema.instance.getTableMetadata(KS, physical).id;
            LayoutHeader header = new LayoutHeader(layoutId, logical, physicalId, KS, table, physical,
                                                   TableLayout.Transform.HASH_SPLIT, LayoutRegistry.HASH_ALGO, buckets, 1);
            RegisterKey t = new RegisterKey(own.actionId, own.owner, own.step, header, bytes(key), Clock.Global.currentTimeMillis(), 600_000);
            assertTrue(c.commitInstead(own, t.wrap(), Phase.ANNOUNCED.name(), own.state).ok());
        });
        return id;
    }

    /**
     * Moves the key by hand: AdvanceLayout, FinishKey for STEADY, RemoveKey for null. Waits for the log to reach every
     * node but {@code down}.
     */
    static void advanceByHand(Cluster cluster, String id, String table, int key, Phase from, Phase to, int generation, int... down)
    {
        String fromName = from.name();
        String toName = to == null ? null : to.name();
        cluster.get(1).runOnInstance(() -> {
            ClusterCoordinator c = coordinator();
            Claim now = c.ownClaim(UUID.fromString(id));
            TableId logical = Schema.instance.getTableMetadata(KS, table).id;
            long millis = Clock.Global.currentTimeMillis();
            if (toName == null)
                assertTrue(c.commitInstead(now, new RemoveKey(now.actionId, now.owner, now.step, logical, bytes(key), millis, 600_000).wrap(),
                                           "KEY_REMOVED", now.state).ok());
            else if (Phase.valueOf(toName) == Phase.STEADY)
                assertTrue(c.commitInstead(now, new FinishKey(now.actionId, now.owner, now.step, logical, bytes(key), millis, 600_000).wrap(),
                                           toName, now.state).ok());
            else
                assertTrue(c.commitInstead(now, new AdvanceLayout(now.actionId, now.owner, now.step, logical, bytes(key), Phase.valueOf(fromName),
                                                                  Phase.valueOf(toName), generation, millis, 600_000).wrap(), toName, now.state).ok());
        });
        ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1), down);
    }

    static void releaseByHand(Cluster cluster, String id, String finalState)
    {
        cluster.get(1).runOnInstance(() -> {
            ClusterCoordinator c = coordinator();
            assertTrue(c.release(c.ownClaim(UUID.fromString(id)), finalState).ok());
        });
    }

    // ---------------------------------------------------------------------------------------------------------
    // the real actuator (PartitionSplitEndToEndTest)
    // ---------------------------------------------------------------------------------------------------------

    static void arm(IInvokableInstance... nodes) throws InterruptedException
    {
        for (IInvokableInstance node : nodes)
            await("the layer to be armed on node " + node.config().num(), () -> node.nodetoolResult("autonomic", "status").getStdout().contains("Armed: true"));
        nodes[0].nodetoolResult("autonomic", "cluster", "enable", "--yes").asserts().success();
    }

    /** Proposes the split on the node and approves it there: the node is the owner. Returns the action id. */
    static String split(IInvokableInstance owner, String table, int key, int buckets)
    {
        org.apache.cassandra.distributed.api.NodeToolResult split = owner.nodetoolResult("autonomic", "layout", "split", KS, table, hex(key),
                                                                                           "--buckets", Integer.toString(buckets));
        split.asserts().success();
        String out = split.getStdout();
        assertTrue(out, out.contains("state=PROPOSED"));
        String id = out.substring(out.indexOf("id=") + 3, out.indexOf("id=") + 3 + 36);
        owner.nodetoolResult("autonomic", "approve", id).asserts().success();
        return id;
    }

    /** {@code nodetool autonomic layout unsplit} on the node, approved there: the node is the owner. Returns the action id. */
    static String unsplit(IInvokableInstance owner, String table, int key)
    {
        org.apache.cassandra.distributed.api.NodeToolResult unsplit = owner.nodetoolResult("autonomic", "layout", "unsplit", KS, table, hex(key));
        String out = unsplit.getStdout() + unsplit.getStderr();
        assertTrue(out, unsplit.getRc() == 0 && out.contains("state=PROPOSED"));
        return approveProposal(owner, out);
    }

    /** The id of the action of {@code actuator} proposed on the node whose params contain {@code param}, once there is one. */
    static String proposed(IInvokableInstance node, String actuator, String param) throws InterruptedException
    {
        String[] id = new String[1];
        await("a proposal of " + actuator + " with " + param, () -> {
            List<Map<String, String>> history = node.callOnInstance(() -> AutonomicService.instance.getHistory(50));
            for (Map<String, String> r : history)
                if ("PROPOSED".equals(r.get("state")) && actuator.equals(r.get("actuator")) && String.valueOf(r.get("params")).contains(param))
                {
                    id[0] = r.get("id");
                    return true;
                }
            return false;
        });
        return id[0];
    }

    /** {@code nodetool autonomic layout resume} on the node, approved there. Returns the action id. */
    static String resume(IInvokableInstance node, String table, int key)
    {
        org.apache.cassandra.distributed.api.NodeToolResult resume = node.nodetoolResult("autonomic", "layout", "resume", KS, table, hex(key));
        resume.asserts().success();
        String out = resume.getStdout();
        assertTrue(out, out.contains("state=PROPOSED"));
        String id = out.substring(out.indexOf("id=") + 3, out.indexOf("id=") + 3 + 36);
        node.nodetoolResult("autonomic", "approve", id).asserts().success();
        return id;
    }

    /** {@code nodetool autonomic layout resume} on the node, not approved: its output. */
    static String proposeResume(IInvokableInstance node, String table, int key)
    {
        org.apache.cassandra.distributed.api.NodeToolResult resume = node.nodetoolResult("autonomic", "layout", "resume", KS, table, hex(key));
        return resume.getStdout() + resume.getStderr();
    }

    /** Approves the proposal printed by a layout command on the node; returns its id. */
    static String approveProposal(IInvokableInstance node, String out)
    {
        String id = out.substring(out.indexOf("id=") + 3, out.indexOf("id=") + 3 + 36);
        node.nodetoolResult("autonomic", "approve", id).asserts().success();
        return id;
    }

    static String layoutStatus(IInvokableInstance node)
    {
        return node.nodetoolResult("autonomic", "layout", "status").getStdout();
    }

    static String history(IInvokableInstance node)
    {
        return node.nodetoolResult("autonomic", "history").getStdout();
    }

    static String trail(IInvokableInstance node, String id)
    {
        List<Map<String, String>> events = node.callOnInstance(() -> AutonomicService.instance.getActionEvents(id));
        StringBuilder sb = new StringBuilder();
        for (Map<String, String> e : events)
            sb.append(e).append('\n');
        return sb.toString();
    }

    /** True if the node holds nothing live of the key in the base table (cleanup's partition tombstone). */
    static boolean baseEmpty(IInvokableInstance node, String table, int key)
    {
        return node.executeInternal("SELECT c FROM " + KS + '.' + table + " WHERE p = ?", key).length == 0;
    }

    /** Holds the node's metadata log back, as LayoutCompatibilitySuite does: it stays at the epoch it has. */
    static void holdLog(Cluster cluster, int node)
    {
        cluster.filters().verbs(org.apache.cassandra.net.Verb.TCM_REPLICATION.id, org.apache.cassandra.net.Verb.TCM_NOTIFY_REQ.id,
                                org.apache.cassandra.net.Verb.TCM_FETCH_CMS_LOG_RSP.id, org.apache.cassandra.net.Verb.TCM_FETCH_PEER_LOG_RSP.id)
               .to(node).drop().on();
    }

    // ---------------------------------------------------------------------------------------------------------
    // the model
    // ---------------------------------------------------------------------------------------------------------

    /** A write or delete whose outcome is unknown: {@code v} null is a delete. */
    static final class Alt
    {
        final Integer v;
        final long ts;

        Alt(Integer v, long ts)
        {
            this.v = v;
            this.ts = ts;
        }

        @Override
        public String toString()
        {
            return (v == null ? "delete" : "v=" + v) + "@" + ts;
        }
    }

    /**
     * The twin-table model of one key of one table: acknowledged statements are applied to {@code <table>_twin} with
     * the same timestamp; statements whose outcome is unknown are kept per clustering as alternatives.
     */
    static final class Model
    {
        final Cluster cluster;
        final String table;
        final int key;
        private final Map<Integer, List<Alt>> uncertain = new ConcurrentHashMap<>();
        private final AtomicLong clock = new AtomicLong();
        final AtomicLong acked = new AtomicLong();
        final AtomicLong failed = new AtomicLong();
        final List<String> mismatches = new CopyOnWriteArrayList<>();
        /** Coordinators that are up; the twin is written and read through them. */
        volatile int[] live;
        /**
         * False: no base-only probe row in STEADY. The probe leaves a tombstone with a timestamp a day ahead in the base
         * residue, which a slow unsplit must never find there (it is a residue above {@code Tt}, T6.17k).
         */
        volatile boolean poisonProbe = true;
        private final PhaseRecorder phases;

        Model(Cluster cluster, String table, int key, int... live)
        {
            this.cluster = cluster;
            this.table = table;
            this.key = key;
            this.live = live;
            this.phases = new PhaseRecorder(this);
        }

        long nextTimestamp()
        {
            long now = System.currentTimeMillis() * 1000;
            return clock.updateAndGet(last -> Math.max(last + 1, now));
        }

        void setLive(int... nodes)
        {
            live = nodes;
        }

        private String t()
        {
            return KS + '.' + table;
        }

        private String twin()
        {
            return KS + '.' + table + "_twin";
        }

        // ---- statements ----

        /** Rows 0..rows-1 of the key and 7 rows of each of partitions key+1000..key+1004, at ALL, before any split. */
        void initialData(int rows)
        {
            long ts = nextTimestamp();
            for (int start = 0; start < rows; start += 50)
            {
                StringBuilder batch = new StringBuilder("BEGIN UNLOGGED BATCH ");
                for (int c = start; c < Math.min(rows, start + 50); c++)
                {
                    batch.append("INSERT INTO %T (p, c, v, s, m) VALUES (").append(key).append(", ").append(c).append(", ").append(c).append(", '")
                         .append("init-").append(c).append("-").append("x".repeat(60)).append("', ").append(c % 3 == 0 ? "{1: 'a', 2: 'b'}" : "null")
                         .append(") USING TIMESTAMP ").append(ts + c).append(c % 7 == 3 ? " AND TTL 100000; " : "; ");
                }
                both(1, ConsistencyLevel.ALL, batch.append("APPLY BATCH").toString());
            }
            ts = nextTimestamp() + rows;
            clock.set(ts);
            // row tombstones, a range tombstone, a map overwrite, a cell tombstone
            for (int c = 5; c < rows; c += 11)
                both(1, ConsistencyLevel.ALL, "DELETE FROM %T USING TIMESTAMP " + nextTimestamp() + " WHERE p = " + key + " AND c = " + c);
            both(1, ConsistencyLevel.ALL, "DELETE FROM %T USING TIMESTAMP " + nextTimestamp() + " WHERE p = " + key + " AND c >= " + rows / 2 + " AND c < " + (rows / 2 + 10));
            both(1, ConsistencyLevel.ALL, "UPDATE %T USING TIMESTAMP " + nextTimestamp() + " SET m = {9: 'nine'} WHERE p = " + key + " AND c = 9");
            both(1, ConsistencyLevel.ALL, "DELETE s FROM %T USING TIMESTAMP " + nextTimestamp() + " WHERE p = " + key + " AND c = 10");
            for (int p = key + 1000; p < key + 1005; p++)
                for (int c = 0; c < 7; c++)
                    both(1, ConsistencyLevel.ALL, "INSERT INTO %T (p, c, v) VALUES (" + p + ", " + c + ", " + c + ") USING TIMESTAMP " + nextTimestamp());
        }

        /** An acknowledged statement of the table, {@code %T}, applied to the table and then to the twin. */
        synchronized void both(int node, ConsistencyLevel cl, String cql)
        {
            cluster.coordinator(node).execute(cql.replace("%T", t()), cl);
            cluster.coordinator(node).execute(cql.replace("%T", twin()), cl);
        }

        /** A write whose outcome is not known yet: reads may show it from now on. Returns its timestamp. */
        long begin(int c, Integer v)
        {
            long ts = nextTimestamp();
            begin(c, v, ts);
            return ts;
        }

        synchronized void begin(int c, Integer v, long ts)
        {
            uncertain.computeIfAbsent(c, x -> new CopyOnWriteArrayList<>()).add(new Alt(v, ts));
        }

        /** The statement of {@link #begin} was acknowledged: the twin receives it, and older alternatives are settled. */
        void acknowledged(int c, Integer v, long ts)
        {
            String cql = v == null ? "DELETE FROM %T USING TIMESTAMP " + ts + " WHERE p = " + key + " AND c = " + c
                                   : "INSERT INTO %T (p, c, v, s) VALUES (" + key + ", " + c + ", " + v + ", 'w" + v + "') USING TIMESTAMP " + ts;
            Throwable last = null;
            for (int attempt = 0; attempt < 20; attempt++)
            {
                int[] nodes = live;
                try
                {
                    cluster.coordinator(nodes[attempt % nodes.length]).execute(cql.replace("%T", twin()), ConsistencyLevel.QUORUM);
                    last = null;
                    break;
                }
                catch (Throwable t)
                {
                    last = t;
                    sleep(100);
                }
            }
            if (last != null)
                mismatches.add("the twin could not be written: " + last);
            synchronized (this)
            {
                List<Alt> alts = uncertain.get(c);
                if (alts != null)
                {
                    alts.removeIf(a -> a.ts <= ts);
                    if (alts.isEmpty())
                        uncertain.remove(c);
                }
            }
            acked.incrementAndGet();
        }

        /** The statement of the table for a write ({@code v} non-null) or a row delete at the timestamp. */
        String statement(int c, Integer v, long ts)
        {
            return v == null ? "DELETE FROM " + t() + " USING TIMESTAMP " + ts + " WHERE p = " + key + " AND c = " + c
                             : "INSERT INTO " + t() + " (p, c, v, s) VALUES (" + key + ", " + c + ", " + v + ", 'w" + v + "') USING TIMESTAMP " + ts;
        }

        /** A write or delete through the node; true if it was acknowledged. Never throws. */
        boolean write(int node, int c, Integer v, ConsistencyLevel cl)
        {
            long ts = begin(c, v);
            try
            {
                cluster.coordinator(node).execute(statement(c, v, ts), cl);
            }
            catch (Throwable t)
            {
                failed.incrementAndGet();
                return false;
            }
            acknowledged(c, v, ts);
            return true;
        }

        synchronized List<Alt> alternatives(int c)
        {
            List<Alt> alts = uncertain.get(c);
            return alts == null ? Collections.emptyList() : new ArrayList<>(alts);
        }

        synchronized int uncertainCount()
        {
            return uncertain.size();
        }

        // ---- reads ----

        static final String SELECT = "SELECT c, v, writetime(v), s, m FROM %s WHERE p = %d";

        private static String render(Object[] row)
        {
            return row[0] + "|" + row[1] + "|" + row[2] + "|" + row[3] + "|" + (row[4] == null ? null : new TreeMap<>((Map<?, ?>) row[4]));
        }

        private static Map<Integer, String> byClustering(Object[][] rows)
        {
            Map<Integer, String> m = new TreeMap<>();
            for (Object[] row : rows)
                m.put((Integer) row[0], render(row));
            return m;
        }

        private static Map<Integer, Object[]> raw(Object[][] rows)
        {
            Map<Integer, Object[]> m = new TreeMap<>();
            for (Object[] row : rows)
                m.put((Integer) row[0], row);
            return m;
        }

        /** Null if the row the coordinator returned for c is what the model allows, else why not. */
        private String judge(int c, Object[] actual, String expected)
        {
            String a = actual == null ? null : render(actual);
            if (Objects.equals(a, expected))
                return null;
            for (Alt alt : alternatives(c))
            {
                if (alt.v == null && actual == null)
                    return null;
                if (alt.v != null && actual != null && alt.v.equals(actual[1]) && Long.valueOf(alt.ts).equals(actual[2]))
                    return null;
            }
            return "clustering " + c + ": read " + a + ", model " + expected + ", unknown outcomes " + alternatives(c);
        }

        /** I5 for one clustering, read through the node; null if correct. */
        String pointCheck(int node, int c)
        {
            Object[][] actual = cluster.coordinator(node).execute(String.format(SELECT, t(), key) + " AND c = " + c, ConsistencyLevel.QUORUM);
            int[] nodes = live;
            Object[][] model = cluster.coordinator(nodes[0]).execute(String.format(SELECT, twin(), key) + " AND c = " + c, ConsistencyLevel.QUORUM);
            return judge(c, actual.length == 0 ? null : actual[0], model.length == 0 ? null : render(model[0]));
        }

        // ---- the invariants ----

        /**
         * I5, I7, I10, I14 now, through every live coordinator and on every live node. Holds the model, so no
         * statement of the workload starts meanwhile; one that is in flight has an unknown outcome and is tolerated.
         */
        synchronized void check(String label)
        {
            List<String> failures = new ArrayList<>();
            int[] nodes = live;
            Map<Integer, String> model = byClustering(cluster.coordinator(nodes[0]).execute(String.format(SELECT, twin(), key), ConsistencyLevel.QUORUM));
            for (int node : nodes)
            {
                String via = label + ", through node " + node + ": ";
                // I14: logical metadata, ascending order, each row once
                SimpleQueryResult result = cluster.coordinator(node).executeWithResult(String.format(SELECT, t(), key), ConsistencyLevel.QUORUM);
                if (!result.names().equals(Arrays.asList("c", "v", "writetime(v)", "s", "m")))
                    failures.add(via + "I14: result columns " + result.names());
                Object[][] rows = result.toObjectArrays();
                for (int i = 1; i < rows.length; i++)
                    if ((Integer) rows[i][0] <= (Integer) rows[i - 1][0])
                        failures.add(via + "I14: clustering " + rows[i][0] + " after " + rows[i - 1][0]);
                Map<Integer, Object[]> actual = raw(rows);
                // I5: every acknowledged write and delete; unknown outcomes either way
                Set<Integer> all = new TreeSet<>(model.keySet());
                all.addAll(actual.keySet());
                for (int c : all)
                {
                    if (c == POISON)
                        continue;
                    String why = judge(c, actual.get(c), model.get(c));
                    if (why != null)
                        failures.add(via + "I5: " + why);
                }
                if (actual.containsKey(POISON))
                    failures.add(via + "I10: the base-only row " + POISON + " was read");
                // I14: descending, and paged, give the same rows
                Object[][] desc = cluster.coordinator(node).execute(String.format(SELECT, t(), key) + " ORDER BY c DESC", ConsistencyLevel.QUORUM);
                for (int i = 1; i < desc.length; i++)
                    if ((Integer) desc[i][0] >= (Integer) desc[i - 1][0])
                        failures.add(via + "I14: descending read has " + desc[i][0] + " after " + desc[i - 1][0]);
                List<Integer> paged = new ArrayList<>();
                java.util.Iterator<Object[]> pages = cluster.coordinator(node).executeWithPaging(String.format(SELECT, t(), key), ConsistencyLevel.QUORUM, 13);
                while (pages.hasNext())
                    paged.add((Integer) pages.next()[0]);
                for (int i = 1; i < paged.size(); i++)
                    if (paged.get(i) <= paged.get(i - 1))
                        failures.add(via + "I14: paged read has " + paged.get(i) + " after " + paged.get(i - 1));
                Set<Integer> unpaged = settledOf(actual.keySet());
                Set<Integer> descSet = new TreeSet<>();
                for (Object[] row : desc)
                    descSet.add((Integer) row[0]);
                if (!settledOf(descSet).equals(unpaged) || !settledOf(new TreeSet<>(paged)).equals(unpaged))
                    failures.add(via + "I14: ascending, descending and paged reads differ: " + unpaged.size() + ", " + descSet.size() + ", " + paged.size());
                // I14: a scan returns the partition once, contiguously, with the same rows
                Object[][] scan = cluster.coordinator(node).execute("SELECT p, c FROM " + t(), ConsistencyLevel.QUORUM);
                Set<Integer> scanned = new TreeSet<>();
                boolean seen = false;
                boolean ended = false;
                for (Object[] row : scan)
                {
                    boolean mine = (Integer) row[0] == key;
                    if (mine && ended)
                        failures.add(via + "I14: the scan returned the partition in two pieces");
                    if (!mine && seen)
                        ended = true;
                    seen |= mine;
                    if (mine && !scanned.add((Integer) row[1]))
                        failures.add(via + "I14: the scan returned clustering " + row[1] + " twice");
                }
                if (!settledOf(scanned).equals(unpaged))
                    failures.add(via + "I14: the scan returned " + scanned.size() + " rows of the partition, the read " + actual.size());
            }
            // I7 on every live node
            for (int node : nodes)
            {
                String i7 = i7(cluster.get(node), table, key);
                if (i7.startsWith("violation"))
                    failures.add(label + ", node " + node + ": I7: " + i7);
            }
            // I10
            failures.addAll(phases.violations(label));
            boolean steady = true;
            for (int node : nodes)
                steady &= Phase.STEADY.name().equals(phaseOf(cluster.get(node), table, key));
            if (steady && poisonProbe)
                failures.addAll(poisonProbe(label));
            if (!failures.isEmpty())
                throw new AssertionError(failures.size() + " invariant violations at " + label + ":\n" + String.join("\n", failures.subList(0, Math.min(40, failures.size()))));
            if (!mismatches.isEmpty())
                throw new AssertionError("the workload saw " + mismatches.size() + " mismatches before " + label + ":\n"
                                         + String.join("\n", mismatches.subList(0, Math.min(20, mismatches.size()))));
        }

        private Set<Integer> settledOf(Set<Integer> cs)
        {
            Set<Integer> out = new TreeSet<>(cs);
            out.removeAll(uncertain.keySet());
            out.remove(POISON);
            return out;
        }

        /** I10 in STEADY: a row written only to the base table, on every live node, is not read through any coordinator. */
        private List<String> poisonProbe(String label)
        {
            List<String> failures = new ArrayList<>();
            int[] nodes = live;
            long ts = (System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1)) * 1000;
            for (int node : nodes)
                cluster.get(node).executeInternal("INSERT INTO " + t() + " (p, c, v) VALUES (?, ?, ?) USING TIMESTAMP ?", key, POISON, POISON, ts);
            for (int node : nodes)
            {
                if (cluster.coordinator(node).execute("SELECT c FROM " + t() + " WHERE p = ? AND c = ?", ConsistencyLevel.QUORUM, key, POISON).length > 0)
                    failures.add(label + ", node " + node + ": I10: a read in STEADY returned a row that only the base table holds");
                for (Object[] row : cluster.coordinator(node).execute("SELECT p, c FROM " + t(), ConsistencyLevel.QUORUM))
                    if ((Integer) row[0] == key && (Integer) row[1] == POISON)
                        failures.add(label + ", node " + node + ": I10: a scan in STEADY returned a row that only the base table holds");
            }
            for (int node : nodes)
                cluster.get(node).executeInternal("DELETE FROM " + t() + " USING TIMESTAMP ? WHERE p = ? AND c = ?", ts + 1, key, POISON);
            return failures;
        }

        void startRecordingPhases()
        {
            phases.start();
        }

        void stopRecordingPhases()
        {
            phases.stop();
        }

        String observedPhases()
        {
            return phases.toString();
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // I7, node-locally
    // ---------------------------------------------------------------------------------------------------------

    private static PartitionUpdate readUnpurged(TableMetadata table, DecoratedKey key)
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(table, 0, key);
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            if (!partitions.hasNext())
                return PartitionUpdate.emptyUpdate(table, key);
            try (UnfilteredRowIterator partition = partitions.next())
            {
                return PartitionUpdate.fromIterator(partition, ColumnFilter.all(table));
            }
        }
    }

    /**
     * I7 on the node: for every row both copies hold, every cell and the row's liveness that both hold with the same
     * timestamp have the same value, TTL and local deletion time. "compared N cells" or "violation: ..." or "no key".
     */
    static String i7(IInvokableInstance node, String table, int key)
    {
        return node.callOnInstance(() -> {
            TableMetadata logical = Schema.instance.getTableMetadata(KS, table);
            TableLayout layout = LayoutRegistry.of(ClusterMetadata.current()).layout(logical.id);
            KeyState state = layout == null ? null : layout.keyState(bytes(key));
            TableMetadata physical = layout == null ? null : Schema.instance.getTableMetadata(layout.physical);
            if (state == null || physical == null)
                return "no key";
            PartitionUpdate base = readUnpurged(logical, logical.partitioner.decorateKey(bytes(key)));
            List<PartitionUpdate> buckets = new ArrayList<>();
            for (int bucket = 0; bucket < layout.buckets; bucket++)
                buckets.add(RowMapper.toLogical(readUnpurged(physical, RowMapper.physicalKey(logical, physical, bytes(key), state.incarnation, bucket)), logical));
            PartitionUpdate overflow = PartitionUpdate.merge(buckets);
            List<String> violations = new ArrayList<>();
            int compared = 0;
            for (Row o : overflow)
            {
                Row b = base.getRow(o.clustering());
                if (b == null)
                    continue;
                String where = o.clustering().toString(logical);
                if (o.primaryKeyLivenessInfo().timestamp() == b.primaryKeyLivenessInfo().timestamp() && !o.primaryKeyLivenessInfo().isEmpty()
                    && !o.primaryKeyLivenessInfo().equals(b.primaryKeyLivenessInfo()))
                    violations.add(where + " liveness " + o.primaryKeyLivenessInfo() + " / " + b.primaryKeyLivenessInfo());
                for (Cell<?> oc : o.cells())
                {
                    Cell<?> bc = oc.column().isComplex() ? b.getCell(oc.column(), oc.path()) : b.getCell(oc.column());
                    if (bc == null || bc.timestamp() != oc.timestamp())
                        continue;
                    compared++;
                    if (!oc.buffer().equals(bc.buffer()) || oc.ttl() != bc.ttl() || oc.localDeletionTime() != bc.localDeletionTime())
                        violations.add(where + ' ' + oc.column().name + ": overflow " + oc + ", base " + bc);
                }
            }
            return violations.isEmpty() ? "compared " + compared + " cells" : "violation: " + String.join("; ", violations);
        });
    }

    // ---------------------------------------------------------------------------------------------------------
    // I10: the phases every node is seen in
    // ---------------------------------------------------------------------------------------------------------

    /** Samples the phase of the key on every live node every 100 ms. */
    static final class PhaseRecorder
    {
        private final Model model;
        private final Map<Integer, List<String>> seen = new ConcurrentHashMap<>();
        private volatile boolean stop;
        private Thread thread;

        PhaseRecorder(Model model)
        {
            this.model = model;
        }

        void start()
        {
            stop = false;
            thread = new Thread(() -> {
                while (!stop)
                {
                    for (int node : model.live)
                    {
                        try
                        {
                            String phase = phaseOf(model.cluster.get(node), model.table, model.key);
                            List<String> list = seen.computeIfAbsent(node, n -> new CopyOnWriteArrayList<>());
                            if (list.isEmpty() || !list.get(list.size() - 1).equals(phase))
                                list.add(phase);
                        }
                        catch (Throwable t)
                        {
                            // the node is going down or starting
                        }
                    }
                    sleep(100);
                }
            }, "phase-recorder");
            thread.setDaemon(true);
            thread.start();
        }

        void stop()
        {
            stop = true;
            if (thread != null)
            {
                try
                {
                    thread.join(10_000);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
            }
        }

        /**
         * I10 (amended by 11.6): once a node was seen in NEW_ONLY or later, it is never seen in an earlier class, or
         * absent, or going back, until it is seen in a reverse phase ({@code BeginUnsplit}); a reverse that aborts
         * returns to STEADY, from where the rule holds again. I21: once seen in BASE_ONLY or REVERSE_CLEANUP, the node is
         * only seen in those or with the key absent.
         */
        List<String> violations(String label)
        {
            List<String> out = new ArrayList<>();
            for (Map.Entry<Integer, List<String>> e : seen.entrySet())
            {
                int highest = -1;
                boolean reverse = false;
                boolean baseOnly = false;
                for (String phase : e.getValue())
                {
                    Phase p = "absent".equals(phase) ? null : Phase.valueOf(phase);
                    if (baseOnly && p != null && p.coordinatorClass > 0)
                        out.add(label + ": I21: node " + e.getKey() + " was seen in " + phase + " after BASE_ONLY: " + e.getValue());
                    if (p == Phase.BASE_ONLY || p == Phase.REVERSE_CLEANUP)
                        baseOnly = true;
                    if (p != null && p.isReverse())
                    {
                        reverse = true;
                        continue;
                    }
                    if (reverse && p == Phase.STEADY)
                    {
                        // an aborted reverse: STEADY again, the forward rule from here on
                        reverse = false;
                        highest = Phase.STEADY.id;
                        continue;
                    }
                    if (reverse || baseOnly)
                        continue;
                    if (highest >= Phase.NEW_ONLY.id && (p == null || p.coordinatorClass < 3 || p.id < highest))
                        out.add(label + ": I10: node " + e.getKey() + " was seen in " + phase + " after a C3 phase: " + e.getValue());
                    if (p != null && p.coordinatorClass == 3)
                        highest = Math.max(highest, p.id);
                }
            }
            return out;
        }

        @Override
        public String toString()
        {
            return new TreeMap<>(seen).toString();
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // the workload
    // ---------------------------------------------------------------------------------------------------------

    /**
     * One thread: writes (some with a TTL) and row deletes of clusterings below {@link #SCENARIO_ROWS} through the live
     * coordinators at QUORUM, each acknowledged one read back at once through another live coordinator and judged by
     * the model. Failures (timeouts around a node going down or a phase change) are allowed and leave the outcome
     * unknown; wrong reads are recorded in {@link Model#mismatches}.
     */
    static final class Workload
    {
        private final Model model;
        private final int range;
        private volatile boolean stop;
        private volatile boolean paused;
        private volatile boolean idle;
        private final Thread thread;
        final AtomicLong reads = new AtomicLong();
        final AtomicLong readFailures = new AtomicLong();
        volatile String firstReadFailure;

        @Override
        public String toString()
        {
            return reads + " reads checked, " + readFailures + " failed" + (firstReadFailure == null ? "" : " (first: " + firstReadFailure + ')');
        }

        private final long seed;

        Workload(Model model, int range, long seed)
        {
            this.model = model;
            this.range = range;
            this.seed = seed;
            thread = new Thread(this::run, "failure-workload");
            thread.setDaemon(true);
            thread.start();
        }

        private void run()
        {
            Random random = new Random(seed);
            while (!stop)
            {
                if (paused)
                {
                    idle = true;
                    sleep(10);
                    continue;
                }
                idle = false;
                int[] nodes = model.live;
                int node = nodes[random.nextInt(nodes.length)];
                int c = random.nextInt(range);
                Integer v = random.nextInt(10) < 8 ? random.nextInt(1_000_000) : null;
                if (model.write(node, c, v, ConsistencyLevel.QUORUM))
                {
                    int reader = nodes[random.nextInt(nodes.length)];
                    try
                    {
                        String why = model.pointCheck(reader, c);
                        reads.incrementAndGet();
                        if (why != null)
                            model.mismatches.add("read back through node " + reader + " after a write through node " + node + ": " + why);
                    }
                    catch (Throwable t)
                    {
                        readFailures.incrementAndGet();
                        if (firstReadFailure == null)
                            firstReadFailure = t.toString();
                        // a read may time out around a phase change or a node going down
                    }
                }
                sleep(5);
            }
        }

        void pause() throws InterruptedException
        {
            paused = true;
            await("the workload to pause", () -> idle || stop);
        }

        void resume()
        {
            paused = false;
        }

        /** Waits until n more statements were acknowledged. */
        void awaitAcked(long n) throws InterruptedException
        {
            long target = model.acked.get() + n;
            await(n + " acknowledged statements", () -> model.acked.get() >= target);
        }

        void stop() throws InterruptedException
        {
            stop = true;
            thread.join(60_000);
        }
    }

    static void sleep(long millis)
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    /** Checks the condition stays true for {@code polls} owner poll intervals (an observation period, not a wait). */
    static void assertStays(String what, int polls, BooleanSupplier condition) throws InterruptedException
    {
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(polls * POLL_MILLIS);
        while (System.nanoTime() < end)
        {
            assertTrue(what, condition.getAsBoolean());
            Thread.sleep(POLL_MILLIS / 2);
        }
    }

    static Set<Integer> nodes(int... ns)
    {
        Set<Integer> s = new HashSet<>();
        for (int n : ns)
            s.add(n);
        return s;
    }

    static Map<String, Object> settings(Object... kv)
    {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2)
            m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    /** The overflow table's name for a table split by hand. */
    static String physicalByHand(String table)
    {
        return PhysicalTables.physicalName(table, layoutId(table));
    }

    /** True if the node holds clustering c of the key, live, in the base table (node-local, not routed). */
    static boolean inBase(IInvokableInstance node, String table, int key, int c)
    {
        return node.executeInternal("SELECT v FROM " + KS + '.' + table + " WHERE p = ? AND c = ?", key, c).length > 0;
    }

    /** True if the node holds clustering c of the key, live, in the overflow table (node-local). */
    static boolean inOverflow(IInvokableInstance node, String physical, int key, int c)
    {
        for (Object[] row : node.executeInternal("SELECT p, c FROM " + KS + '.' + physical))
            if ((Integer) row[0] == key && (Integer) row[1] == c)
                return true;
        return false;
    }

    /** What a read through the node at QUORUM returns for clustering c of the key: v, or null if there is no row. */
    static Integer readV(Cluster cluster, int node, String table, int key, int c)
    {
        Object[][] rows = cluster.coordinator(node).execute("SELECT v FROM " + KS + '.' + table + " WHERE p = ? AND c = ?", ConsistencyLevel.QUORUM, key, c);
        return rows.length == 0 ? null : (Integer) rows[0][0];
    }
}
