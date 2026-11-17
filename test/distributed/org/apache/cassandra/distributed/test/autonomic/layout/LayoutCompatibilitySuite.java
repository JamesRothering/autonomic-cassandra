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
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator;
import org.apache.cassandra.autonomic.coord.TcmEnvironment;
import org.apache.cassandra.autonomic.layout.AdvanceLayout;
import org.apache.cassandra.autonomic.layout.EpochWatermarks;
import org.apache.cassandra.autonomic.layout.FinishKey;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.PhysicalTables;
import org.apache.cassandra.autonomic.layout.RemoveKey;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.Clock;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutReadsTest.page;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.K;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.LAYOUT_ID;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.PHYSICAL;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.register;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * T6.12: the compatibility suite of adaptive-schema design section 9. A fixed catalogue of statements, one entry for
 * each row of the tables of 3.3 (writes), 3.4 (single-partition reads), 3.5 (range and token scans) and 3.6 (LWT,
 * TRUNCATE, direct writes to the overflow table) plus the DDL rejections of section 5, is run against a table whose
 * partition 1 is moved through the migration phases and against a twin that is never split and receives the same
 * statements: the twin is the model, "what stock Cassandra returns".
 *
 * <p>The key is held in a phase by simply not advancing it (the registry is committed by hand, as in
 * {@link LayoutWritesTest}); a step of a script moves it to the next phase and, around that commit, runs
 * <ul>
 * <li>a workload of writes and reads through both coordinators while the commit happens (every acknowledged write
 * must read back, through either coordinator, before, during and after), and a paging state that is taken before the
 * commit and resumed after it;</li>
 * <li>optionally with node 2 held one class behind: its log delivery is dropped by a message filter, so node 1 is in
 * the next phase and node 2 in this one. The catalogue is then run through both, and rule O1 is asserted: the epoch
 * of the new phase is not acknowledged by every node, so an owner would not take the next transition.</li>
 * </ul>
 * Expectations (rejections) are computed per coordinator from the phase that coordinator sees.
 *
 * <p>Node 2 runs with the autonomic layer disabled: routing is unconditional (design 4.3). Counters (3.6) are not in
 * the catalogue: a table with counters is ineligible, so there is no counter statement on a table that can be split.
 * Machine limits: two nodes, one cluster start per test class.
 */
public abstract class LayoutCompatibilitySuite extends TestBaseImpl
{
    static final String COLUMNS = " (p int, c1 int, c2 int, v int, s text, l list<int>, m map<text, int>, PRIMARY KEY (p, c1, c2))"
                                  + " WITH CLUSTERING ORDER BY (c1 DESC, c2 ASC)";
    static final int PARTITIONS = 6;
    static final String ADAPTED = "Lightweight transactions are not supported on partition 0x00000001 of " + KEYSPACE + ".t while it is adapted";
    static final String MANAGED = PHYSICAL + " is managed by the adapted layout of " + KEYSPACE + ".t";

    /** A write of the catalogue: a statement of a table and a timestamp, the consistency level and node it runs at. */
    static final class W
    {
        final String row;
        final int node;
        final ConsistencyLevel cl;
        final boolean replay;
        final BiFunction<String, Long, String> cql;

        W(String row, int node, ConsistencyLevel cl, boolean replay, BiFunction<String, Long, String> cql)
        {
            this.row = row;
            this.node = node;
            this.cl = cl;
            this.replay = replay;
            this.cql = cql;
        }
    }

    /** One step of a script: hold the current phase, or move the key to {@code target} (null: the key is removed). */
    static final class Step
    {
        final Phase target;
        final boolean move;
        boolean hold;
        boolean behind;
        boolean quiet;

        private Step(Phase target, boolean move)
        {
            this.target = target;
            this.move = move;
        }

        /** Run the catalogue with the key where it is now (the start, before anything is registered). */
        static Step start()
        {
            Step s = new Step(null, false);
            s.hold = true;
            return s;
        }

        /** Move the key to the phase, with the workload around the commit, then hold it there. */
        static Step to(Phase target)
        {
            Step s = new Step(target, true);
            s.hold = true;
            return s;
        }

        /** Move the key to absent (RemoveKey after ABORTING), then hold it. */
        static Step removed()
        {
            Step s = new Step(null, true);
            s.hold = true;
            return s;
        }

        /** Move the key without running the catalogue or the workload: only to reach the phase the script starts at. */
        static Step reach(Phase target)
        {
            Step s = new Step(target, true);
            s.quiet = true;
            return s;
        }

        /** Hold node 2 one class behind across the commit. */
        Step behind()
        {
            this.behind = true;
            return this;
        }

        /** Do not run the catalogue after the transition (it still runs while node 2 is behind). */
        Step noHold()
        {
            this.hold = false;
            return this;
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // the cluster and the script
    // ---------------------------------------------------------------------------------------------------------

    private Cluster cluster;
    private String actionId;
    /** The phase of the key as the owner has committed it; null: not in the registry. */
    private Phase current;
    private int round;
    private int replicas;
    private final List<W> history = new ArrayList<>();
    private final List<String> failures = new ArrayList<>();

    private static final int[] RING = ringOrder();

    private static ByteBuffer key(int p)
    {
        return Int32Type.instance.decompose(p);
    }

    private static int[] ringOrder()
    {
        Integer[] order = new Integer[PARTITIONS];
        for (int p = 1; p <= PARTITIONS; p++)
            order[p - 1] = p;
        Arrays.sort(order, (a, b) -> Murmur3Partitioner.instance.getToken(key(a)).compareTo(Murmur3Partitioner.instance.getToken(key(b))));
        int[] r = new int[PARTITIONS];
        for (int i = 0; i < PARTITIONS; i++)
            r[i] = order[i];
        return r;
    }

    private static long token(int p)
    {
        return (Long) Murmur3Partitioner.instance.getToken(key(p)).getTokenValue();
    }

    private static ClusterCoordinator coordinator()
    {
        return new ClusterCoordinator(new TcmEnvironment(Clock.Global::currentTimeMillis, DatabaseDescriptor::getAutonomicConfig));
    }

    /** Runs the script on a fresh two-node cluster. */
    void run(Step... script) throws Throwable
    {
        try (Cluster c = init(Cluster.build(2)
                                     .withConfig(cfg -> cfg.with(NETWORK, GOSSIP)
                                                           .set("autonomic.enabled", cfg.num() == 1)
                                                           .set("hinted_handoff_enabled", false))
                                     .start()))
        {
            cluster = c;
            IInvokableInstance n1 = c.get(1);
            c.schemaChange("CREATE TABLE " + KEYSPACE + ".t" + COLUMNS);
            c.schemaChange("CREATE TABLE " + KEYSPACE + ".twin" + COLUMNS);
            String derived = n1.callOnInstance(() -> PhysicalTables.derive(Schema.instance.getTableMetadata(KEYSPACE, "t"), LAYOUT_ID, TableId.generate()));
            c.schemaChange(derived);
            ClusterUtils.waitForCMSToQuiesce(c, n1);
            initialData();
            replicas = 0;
            for (int node = 1; node <= 2; node++)
                if (c.get(node).executeInternal("SELECT c1 FROM " + KEYSPACE + ".t WHERE p = ?", K).length > 0)
                    replicas++;
            assertTrue(replicas > 0);

            for (Step s : script)
                step(s);
            assertTrue("disagreements between the code and the model:\n" + String.join("\n", failures), failures.isEmpty());
        }
    }

    private void step(Step s) throws Throwable
    {
        Phase from = current;
        Phase[] seen = { null, current, current };
        if (s.move)
        {
            WorkloadHandle workload = null;
            String[] paging = null;
            if (!s.quiet)
            {
                paging = takePagingState();
                workload = new WorkloadHandle();
                workload.awaitAcknowledged(8);
            }
            if (s.behind)
                cluster.filters().verbs(Verb.TCM_REPLICATION.id, Verb.TCM_NOTIFY_REQ.id,
                                        Verb.TCM_FETCH_CMS_LOG_RSP.id, Verb.TCM_FETCH_PEER_LOG_RSP.id).to(2).drop().on();
            Epoch before = ClusterUtils.getCurrentEpoch(cluster.get(1));
            Epoch phaseEpochBefore = phaseEpoch(cluster.get(1));
            commit(from, s.target);
            current = s.target;
            seen[1] = current;
            if (s.behind)
            {
                // node 2 is one class behind: it has not been told of the new phase, node 1 has
                Epoch after = ClusterUtils.getCurrentEpoch(cluster.get(1));
                assertTrue(after.getEpoch() > before.getEpoch());
                assertTrue("node 2 must be behind", ClusterUtils.getCurrentEpoch(cluster.get(2)).getEpoch() < after.getEpoch());
                assertEquals("node 2 still sees the old phase", from, phaseOf(cluster.get(2)));
                assertEquals(current, phaseOf(cluster.get(1)));
                if (current != null)
                {
                    Epoch newPhaseEpoch = phaseEpoch(cluster.get(1));
                    // O1: the owner may leave the new phase only when every node has answered with an epoch at or past it
                    assertFalse("O1: a node is behind, so the new phase is not acknowledged by every node",
                                cluster.get(1).callOnInstance(() -> EpochWatermarks.allAtLeast(newPhaseEpoch)));
                }
                if (from != null)
                    assertTrue("O1: the phase left was acknowledged by every node", cluster.get(1).callOnInstance(() -> EpochWatermarks.allAtLeast(phaseEpochBefore)));
                seen[2] = from;
                seen[1] = current;
            }
            if (workload != null)
            {
                workload.awaitAcknowledged(workload.acknowledged() + 8);
                workload.finish();
            }
            if (s.behind)
            {
                // the catalogue through the node that is behind and through the one that is current
                catalogue(current + " (node 2 one class behind, " + from + " seen by it)", seen);
                cluster.filters().reset();
                catchUp();
            }
            else
                ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
            if (current == Phase.BACKFILL)
                BackfillTest.awaitBackfill(cluster, 1, replicas, 120);
            if (paging != null)
                resumePagingState(paging, from + " -> " + current);
            if (s.hold)
                catalogue(String.valueOf(current), new Phase[]{ null, current, current });
        }
        else
            catalogue(String.valueOf(current), seen);
    }

    // ---------------------------------------------------------------------------------------------------------
    // driving the key by hand
    // ---------------------------------------------------------------------------------------------------------

    private void commit(Phase from, Phase to)
    {
        if (from == null)
        {
            assertEquals(Phase.ANNOUNCED, to);
            actionId = register(cluster.get(1), 1);
            return;
        }
        String id = actionId;
        String fromName = from.name();
        String toName = to == null ? null : to.name();
        cluster.get(1).runOnInstance(() -> {
            ClusterCoordinator c = coordinator();
            Claim now = c.ownClaim(UUID.fromString(id));
            TableId logical = Schema.instance.getTableMetadata(KEYSPACE, "t").id;
            ByteBuffer key = key(K);
            if (toName == null)
            {
                RemoveKey r = new RemoveKey(now.actionId, now.owner, now.step, logical, key, Clock.Global.currentTimeMillis(), 600_000);
                assertTrue(c.commitInstead(now, r.wrap(), "KEY_REMOVED", now.state).ok());
            }
            else if (Phase.valueOf(toName) == Phase.STEADY)
            {
                FinishKey f = new FinishKey(now.actionId, now.owner, now.step, logical, key, Clock.Global.currentTimeMillis(), 600_000);
                assertTrue(c.commitInstead(now, f.wrap(), toName, now.state).ok());
            }
            else
            {
                AdvanceLayout a = new AdvanceLayout(now.actionId, now.owner, now.step, logical, key, Phase.valueOf(fromName), Phase.valueOf(toName), 1,
                                                    Clock.Global.currentTimeMillis(), 600_000);
                assertTrue(c.commitInstead(now, a.wrap(), toName, now.state).ok());
            }
        });
    }

    /** The phase of the key as the node sees it; null if the key is not registered. */
    private static Phase phaseOf(IInvokableInstance node)
    {
        String name = node.callOnInstance(() -> {
            LayoutRegistry registry = LayoutRegistry.of(ClusterMetadata.current());
            if (registry.byLogical.isEmpty() || registry.byLogical.values().iterator().next().keyState(key(K)) == null)
                return null;
            return registry.byLogical.values().iterator().next().keyState(key(K)).phase.name();
        });
        return name == null ? null : Phase.valueOf(name);
    }

    private static Epoch phaseEpoch(IInvokableInstance node)
    {
        long epoch = node.callOnInstance(() -> {
            LayoutRegistry registry = LayoutRegistry.of(ClusterMetadata.current());
            if (registry.byLogical.isEmpty())
                return 0L;
            return registry.byLogical.values().iterator().next().keyState(key(K)).phaseEpoch.getEpoch();
        });
        return Epoch.create(epoch);
    }

    /** Lifts the filter's effect: node 2 learns the log on the next epoch request, as in LayoutServiceTest. */
    private void catchUp() throws InterruptedException
    {
        long deadline = System.nanoTime() + 60_000_000_000L;
        while (true)
        {
            long target = ClusterUtils.getCurrentEpoch(cluster.get(1)).getEpoch();
            boolean ok = cluster.get(1).callOnInstance(() -> EpochWatermarks.allAtLeast(Epoch.create(target)));
            if (ok && ClusterUtils.getCurrentEpoch(cluster.get(2)).getEpoch() >= target)
                return;
            if (System.nanoTime() > deadline)
                throw new AssertionError("node 2 did not catch up");
            Thread.sleep(300);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // the data and the catalogue
    // ---------------------------------------------------------------------------------------------------------

    /** Statement texts of one table, with the twin as the model. */
    private void both(int node, ConsistencyLevel cl, String cqlOfTable)
    {
        for (String table : new String[]{ "t", "twin" })
            cluster.coordinator(node).execute(cqlOfTable.replace("%T", KEYSPACE + '.' + table), cl);
    }

    private void initialData()
    {
        // partition 1 is written in a way that can be replayed after a partition delete; the others are written once
        for (int c1 = 0; c1 < 6; c1++)
        {
            int row = c1;
            history.add(new W("initial rows", 1, ConsistencyLevel.ALL, true, (t, ts) -> {
                StringBuilder b = new StringBuilder("BEGIN UNLOGGED BATCH ");
                for (int c2 = 0; c2 < 7; c2++)
                {
                    int v = row * 10 + c2;
                    b.append("INSERT INTO ").append(t).append(" (p, c1, c2, v, s, l, m) VALUES (1, ").append(row).append(", ").append(c2).append(", ")
                     .append(c2 == 6 ? "null" : String.valueOf(v)).append(", 's").append(v).append("', [").append(v).append(", 1, 2], {'a': ").append(v)
                     .append(", 'b': 2}) USING TIMESTAMP ").append(ts + c2).append(c2 == 5 ? " AND TTL 100000; " : "; ");
                }
                return b.append("APPLY BATCH").toString();
            }));
        }
        long ts = 1000;
        for (W w : new ArrayList<>(history))
        {
            exec(w, ts);
            ts += 10;
        }
        for (int p = 2; p <= PARTITIONS; p++)
            for (int c1 = 0; c1 < 4; c1++)
                for (int c2 = 0; c2 < 5; c2++)
                    both(1 + ((p + c1) % 2), ConsistencyLevel.ALL, "INSERT INTO %T (p, c1, c2, v) VALUES (" + p + ", " + c1 + ", " + c2 + ", "
                                                                    + (p * 100 + c1 * 10 + c2) % 47 + ") USING TIMESTAMP " + (1000 + c1 * 5 + c2));
    }

    private void exec(W w, long ts)
    {
        for (String table : new String[]{ "t", "twin" })
            cluster.coordinator(w.node).execute(w.cql.apply(KEYSPACE + '.' + table, ts), w.cl);
    }

    private List<W> writes(int r)
    {
        int x = 10 + r;
        int prev = r == 0 ? 5 : x - 1;
        int k = r % 6;
        int j = new int[]{ 1, 2, 4, 5, 6, 0 }[r % 6];
        ConsistencyLevel all = ConsistencyLevel.ALL;
        List<W> w = new ArrayList<>();
        // 3.3: insert with TTL and collections; plain inserts; an insert of a null (a cell tombstone)
        w.add(new W("insert, ttl, collections", 1, all, true, (t, ts) -> "INSERT INTO " + t + " (p, c1, c2, v, s, l, m) VALUES (1, " + x + ", 0, " + x * 10 + ", 's" + x
                                                                         + "', [" + x + ", 1, 2], {'a': " + x + ", 'b': 2}) USING TIMESTAMP " + ts + " AND TTL 100000"));
        w.add(new W("insert", 2, all, true, (t, ts) -> "INSERT INTO " + t + " (p, c1, c2, v, s) VALUES (1, " + x + ", 1, " + x + ", 'one') USING TIMESTAMP " + ts));
        w.add(new W("insert of a null", 1, all, true, (t, ts) -> "INSERT INTO " + t + " (p, c1, c2, v, s) VALUES (1, " + x + ", 2, null, 'two') USING TIMESTAMP " + ts));
        w.add(new W("update", 1, all, true, (t, ts) -> "UPDATE " + t + " USING TIMESTAMP " + (ts + 1) + " SET v = " + (x + 1) + ", s = 'upd' WHERE p = 1 AND c1 = " + x + " AND c2 = 1"));
        w.add(new W("collection append", 2, all, true, (t, ts) -> "UPDATE " + t + " USING TIMESTAMP " + ts + " SET l = l + [" + x + "], m['k" + x + "'] = " + x
                                                                   + " WHERE p = 1 AND c1 = 2 AND c2 = 3"));
        w.add(new W("list element (read before write)", 1, all, true, (t, ts) -> "UPDATE " + t + " USING TIMESTAMP " + (ts + 2) + " SET l[1] = " + x
                                                                                 + " WHERE p = 1 AND c1 = 2 AND c2 = 3"));
        w.add(new W("cell delete", 2, all, true, (t, ts) -> "DELETE v FROM " + t + " USING TIMESTAMP " + ts + " WHERE p = 1 AND c1 = 0 AND c2 = " + k));
        w.add(new W("row delete", 1, all, true, (t, ts) -> "DELETE FROM " + t + " USING TIMESTAMP " + ts + " WHERE p = 1 AND c1 = " + k + " AND c2 = " + j));
        w.add(new W("collection delete", 2, all, true, (t, ts) -> "DELETE m FROM " + t + " USING TIMESTAMP " + ts + " WHERE p = 1 AND c1 = " + ((k + 1) % 6) + " AND c2 = 4"));
        w.add(new W("map element delete", 1, all, true, (t, ts) -> "DELETE m['a'] FROM " + t + " USING TIMESTAMP " + ts + " WHERE p = 1 AND c1 = " + ((k + 2) % 6) + " AND c2 = 4"));
        // 3.3: range tombstones go to every bucket
        w.add(new W("range delete, one clustering prefix", 2, all, true, (t, ts) -> "DELETE FROM " + t + " USING TIMESTAMP " + ts + " WHERE p = 1 AND c1 = " + prev + " AND c2 > 1"));
        w.add(new W("range delete, closed", 1, all, true, (t, ts) -> "DELETE FROM " + t + " USING TIMESTAMP " + ts + " WHERE p = 1 AND c1 = " + k + " AND c2 >= 5 AND c2 <= 6"));
        w.add(new W("range delete older than the rows it covers", 2, all, true, (t, ts) -> "DELETE FROM " + t + " USING TIMESTAMP " + (ts - 500000) + " WHERE p = 1 AND c1 > 90 AND c1 <= 5000"));
        w.add(new W("insert shadowed by a range delete", 1, all, true, (t, ts) -> "INSERT INTO " + t + " (p, c1, c2, v) VALUES (1, " + prev + ", 2, 777) USING TIMESTAMP " + (ts - 300)));
        // 3.3: batches
        w.add(new W("unlogged batch, split and other partition", 2, all, true, (t, ts) -> "BEGIN UNLOGGED BATCH "
                                                                                          + "INSERT INTO " + t + " (p, c1, c2, v) VALUES (1, " + x + ", 4, 4) USING TIMESTAMP " + ts + "; "
                                                                                          + "INSERT INTO " + t + " (p, c1, c2, v) VALUES (1, " + x + ", 5, 5) USING TIMESTAMP " + ts + "; "
                                                                                          + "INSERT INTO " + t + " (p, c1, c2, v) VALUES (2, " + x + ", 0, 0) USING TIMESTAMP " + ts + "; "
                                                                                          + "APPLY BATCH"));
        w.add(new W("logged batch over several buckets and partitions, with a range delete", 1, all, true, (t, ts) -> "BEGIN BATCH USING TIMESTAMP " + ts + " "
                                                                                                                      + "INSERT INTO " + t + " (p, c1, c2, v) VALUES (1, " + x + ", 8, 8); "
                                                                                                                      + "INSERT INTO " + t + " (p, c1, c2, v) VALUES (1, " + x + ", 9, 9); "
                                                                                                                      + "INSERT INTO " + t + " (p, c1, c2, v) VALUES (1, " + (x + 100) + ", 9, 9); "
                                                                                                                      + "INSERT INTO " + t + " (p, c1, c2, v) VALUES (3, " + x + ", 0, 0); "
                                                                                                                      + "DELETE FROM " + t + " WHERE p = 1 AND c1 = " + prev + " AND c2 > 4; "
                                                                                                                      + "APPLY BATCH"));
        // 3.3: consistency ANY is raised to ONE for a routed write
        w.add(new W("write at ANY", 2, ConsistencyLevel.ANY, true, (t, ts) -> "INSERT INTO " + t + " (p, c1, c2, v) VALUES (1, " + x + ", 7, 7) USING TIMESTAMP " + ts));
        // 3.3: server-side timestamps and a TTL without a timestamp (the values are compared, not the write times)
        w.add(new W("insert with a server timestamp", 1, all, false, (t, ts) -> "INSERT INTO " + t + " (p, c1, c2, v, s) VALUES (1, " + (50 + r) + ", 0, " + r + ", 'server')"));
        w.add(new W("update with a ttl and a server timestamp", 2, all, false, (t, ts) -> "UPDATE " + t + " USING TTL 5000 SET v = " + r + " WHERE p = 1 AND c1 = " + (50 + r) + " AND c2 = 1"));
        // a partition that is not split
        w.add(new W("partition that is not split", 1, all, true, (t, ts) -> "INSERT INTO " + t + " (p, c1, c2, v) VALUES (2, " + x + ", 1, " + x + ") USING TIMESTAMP " + ts));
        return w;
    }

    /** The statements of one round; every statement is a row of the tables of 3.3 and is applied to the table and to its twin. */
    private void writeRound(int r)
    {
        long base = 1_000_000L * (r + 1);
        long ts = base;
        for (W w : writes(r))
        {
            exec(w, ts);
            if (w.replay)
                history.add(w);
            ts += 20;
        }
        // 3.3: partition delete (a range tombstone over the whole partition, in every bucket); the history is then
        // replayed with fresh timestamps so that later rounds still have data in every region
        exec(new W("partition delete", 2, ConsistencyLevel.ALL, false, (t, x) -> "DELETE FROM " + t + " USING TIMESTAMP " + x + " WHERE p = 1"), ts);
        ts += 20;
        for (W w : new ArrayList<>(history))
        {
            ts += 20;
            exec(w, ts);
        }
    }

    private static final int[] PAGE_SIZES = { 0, 13 };

    private static List<String> singlePartitionReads()
    {
        // 3.4, one entry for each row of the table
        return Arrays.asList(
        "SELECT * FROM %s WHERE p = 1 AND c1 = 2 AND c2 = 3",
        "SELECT * FROM %s WHERE p = 1 AND c1 = 77 AND c2 = 3",
        "SELECT * FROM %s WHERE p = 1 AND c1 = 2 AND c2 IN (5, 1, 3, 99)",
        "SELECT * FROM %s WHERE p = 1 AND (c1, c2) IN ((0, 1), (4, 4), (5, 6), (3, 6))",
        "SELECT * FROM %s WHERE p = 1",
        "SELECT * FROM %s WHERE p = 1 AND c1 = 2",
        "SELECT * FROM %s WHERE p = 1 AND c1 >= 1 AND c1 < 4",
        "SELECT * FROM %s WHERE p = 1 AND c1 = 2 AND c2 > 2",
        "SELECT * FROM %s WHERE p = 1 ORDER BY c1 ASC, c2 DESC",
        "SELECT * FROM %s WHERE p = 1 ORDER BY c1 ASC, c2 DESC LIMIT 5",
        "SELECT * FROM %s WHERE p = 1 LIMIT 1",
        "SELECT * FROM %s WHERE p = 1 LIMIT 9",
        "SELECT * FROM %s WHERE p = 1 PER PARTITION LIMIT 4",
        "SELECT * FROM %s WHERE p IN (1, 2)",
        "SELECT * FROM %s WHERE p IN (2, 1, 3) LIMIT 50",
        "SELECT * FROM %s WHERE p IN (1, 2) PER PARTITION LIMIT 3",
        "SELECT count(*) FROM %s WHERE p = 1",
        "SELECT count(*), sum(v), max(c2) FROM %s WHERE p = 1 AND c1 > 2",
        "SELECT c1, count(*), sum(v), max(c2) FROM %s WHERE p = 1 GROUP BY c1",
        "SELECT c1, count(*) FROM %s WHERE p = 1 GROUP BY c1 LIMIT 3",
        "SELECT p, c1, count(*) FROM %s WHERE p IN (1, 2) GROUP BY p, c1",
        "SELECT DISTINCT p FROM %s WHERE p IN (1, 2)",
        "SELECT c1, c2, writetime(v), writetime(s) FROM %s WHERE p = 1 AND c1 < 50",
        "SELECT c1, c2, l, m, m['a'] FROM %s WHERE p = 1 AND c1 = 2",
        "SELECT v FROM %s WHERE p = 1",
        "SELECT c1, c2, v FROM %s WHERE p = 1 AND v > 20 ALLOW FILTERING",
        "SELECT c1, c2, v FROM %s WHERE p = 1 AND v > 20 LIMIT 3 ALLOW FILTERING",
        "SELECT c1, c2 FROM %s WHERE p = 1 AND c2 = 3 ALLOW FILTERING");
    }

    private static List<String> rangeReads()
    {
        // 3.5: scans of the whole table and token ranges around the split partition, the first and the last of the ring
        List<String> cql = new ArrayList<>(Arrays.asList(
        "SELECT * FROM %s",
        "SELECT count(*) FROM %s",
        "SELECT p, count(*) FROM %s GROUP BY p",
        "SELECT p, c1, count(*) FROM %s GROUP BY p, c1",
        "SELECT DISTINCT p FROM %s",
        "SELECT * FROM %s PER PARTITION LIMIT 3",
        "SELECT * FROM %s WHERE v > 20 ALLOW FILTERING",
        "SELECT * FROM %s WHERE c1 = 2 ALLOW FILTERING",
        "SELECT * FROM %s PER PARTITION LIMIT 5 LIMIT 22"));
        for (int limit : new int[]{ 1, 7, 40, 133, 400 })
            cql.add("SELECT * FROM %s LIMIT " + limit);
        for (int p : new int[]{ K, RING[0], RING[PARTITIONS - 1] })
        {
            long t = token(p);
            cql.add("SELECT * FROM %s WHERE token(p) >= " + t);
            cql.add("SELECT * FROM %s WHERE token(p) > " + t);
            cql.add("SELECT * FROM %s WHERE token(p) <= " + t);
            cql.add("SELECT * FROM %s WHERE token(p) < " + t + " LIMIT 25");
        }
        long other = token(RING[0] == K ? RING[1] : RING[0]);
        long lo = Math.min(token(K), other);
        long hi = Math.max(token(K), other);
        cql.add("SELECT * FROM %s WHERE token(p) >= " + lo + " AND token(p) <= " + hi);
        cql.add("SELECT * FROM %s WHERE token(p) > " + lo + " AND token(p) < " + hi);
        cql.add("SELECT * FROM %s WHERE token(p) >= " + lo + " AND token(p) <= " + lo);
        cql.add("SELECT * FROM %s WHERE token(p) >= " + lo + " AND c1 > 1 AND c2 < 3 ALLOW FILTERING");
        return cql;
    }

    private static final List<String> READS = new ArrayList<>();

    static
    {
        READS.addAll(singlePartitionReads());
        READS.addAll(rangeReads());
    }

    private static final String[] TTL_READS = {
        "SELECT c1, c2, ttl(v) FROM %s WHERE p = 1 AND c2 = 0 ALLOW FILTERING",
        "SELECT c1, c2, ttl(v), ttl(s) FROM %s WHERE p = 1 AND c2 = 0 ALLOW FILTERING",
        "SELECT p, c1, c2, ttl(v) FROM %s WHERE c2 = 0 AND p IN (1, 2) ALLOW FILTERING" };

    /**
     * The catalogue in the state the cluster is in: {@code seen[n]} is the phase node n sees (null: not registered).
     * Writes first, through both nodes, then the rejections, then every read through both nodes at every page size.
     */
    private void catalogue(String label, Phase[] seen) throws Exception
    {
        int r = round++;
        writeRound(r);
        rejections(label, seen, r);
        reads(label, seen, false);
        if (seen[1] != null && seen[1].coordinatorClass >= 2)
            reads(label, seen, true);
    }

    private void reads(String label, Phase[] seen, boolean lazy)
    {
        if (lazy)
            for (int node = 1; node <= 2; node++)
                cluster.get(node).runOnInstance(() -> {
                    DatabaseDescriptor.getAutonomicConfig().layout.fanout_eager_rows = 5;
                    DatabaseDescriptor.getAutonomicConfig().layout.fanout_chunk_rows = 3;
                });
        try
        {
            int[] sizes = lazy ? new int[]{ 0 } : PAGE_SIZES;
            for (String cql : READS)
            {
                String expected = String.join("\n", LayoutReadsTest.rows(cluster, 1, cql, "twin", 0));
                String[] results = new String[3];
                for (int node = 1; node <= 2; node++)
                    for (int size : sizes)
                    {
                        String actual = String.join("\n", LayoutReadsTest.rows(cluster, node, cql, "t", size));
                        if (!expected.equals(actual))
                            failures.add(label + (lazy ? ", lazy fan-out" : "") + ", node " + node + " (sees " + seen[node] + "), page size " + size + ": " + cql
                                         + "\n  model:  " + abbreviate(expected) + "\n  actual: " + abbreviate(actual));
                        if (size == 0)
                            results[node] = actual;
                    }
                if (!results[1].equals(results[2]))
                    failures.add(label + ": the coordinators disagree with each other (node 1 sees " + seen[1] + ", node 2 sees " + seen[2] + "): " + cql);
            }
            for (String cql : TTL_READS)
                for (int node = 1; node <= 2; node++)
                    assertTtl(label, node, cql);
        }
        finally
        {
            if (lazy)
                for (int node = 1; node <= 2; node++)
                    cluster.get(node).runOnInstance(() -> {
                        DatabaseDescriptor.getAutonomicConfig().layout.fanout_eager_rows = 10_000;
                        DatabaseDescriptor.getAutonomicConfig().layout.fanout_chunk_rows = 1000;
                    });
        }
    }

    private static String abbreviate(String s)
    {
        return s.length() < 400 ? s : s.substring(0, 400) + "... (" + s.length() + " characters)";
    }

    /** The statements run at different times, so the remaining time of a ttl may differ by a little. */
    private void assertTtl(String label, int node, String cql)
    {
        Object[][] model = cluster.coordinator(1).execute(String.format(cql, KEYSPACE + ".twin"), ConsistencyLevel.QUORUM);
        Object[][] actual = cluster.coordinator(node).execute(String.format(cql, KEYSPACE + ".t"), ConsistencyLevel.QUORUM);
        boolean same = model.length == actual.length && model.length > 0;
        for (int i = 0; same && i < model.length; i++)
            for (int c = 0; same && c < model[i].length; c++)
            {
                Object a = model[i][c];
                Object b = actual[i][c];
                boolean ttl = cql.contains("ttl(") && c == model[i].length - 1 || (cql.contains("ttl(s)") && c >= model[i].length - 2);
                if (ttl && a instanceof Integer && b instanceof Integer)
                    same = Math.abs((Integer) a - (Integer) b) <= 30;
                else
                    same = java.util.Objects.equals(a, b);
            }
        if (!same)
            failures.add(label + ", node " + node + ": " + cql + "\n  model:  " + abbreviate(Arrays.deepToString(model)) + "\n  actual: " + abbreviate(Arrays.deepToString(actual)));
    }

    // ---------------------------------------------------------------------------------------------------------
    // 3.6 and section 5: the documented rejections, and the statements that are accepted where they are not documented
    // ---------------------------------------------------------------------------------------------------------

    /** The exception text, or null if the statement ran. */
    private String attempt(int node, String cql, ConsistencyLevel cl)
    {
        try
        {
            cluster.coordinator(node).execute(cql, cl);
            return null;
        }
        catch (Throwable t)
        {
            StringBuilder text = new StringBuilder(t.getClass().getName()).append(' ').append(t.getMessage());
            for (Throwable c = t.getCause(); c != null && c != c.getCause(); c = c.getCause())
                text.append(" / ").append(c.getClass().getName()).append(' ').append(c.getMessage());
            return text.toString();
        }
    }

    private void expectRejected(String label, int node, Phase seen, String cql, ConsistencyLevel cl, String message)
    {
        String text = attempt(node, cql, cl);
        if (text == null)
            failures.add(label + ", node " + node + " (sees " + seen + "): expected a rejection with \"" + message + "\" but the statement ran: " + cql);
        else if (!text.contains("InvalidRequestException") || !text.contains(message))
            failures.add(label + ", node " + node + " (sees " + seen + "): expected \"" + message + "\", got: " + text + " for " + cql);
    }

    private void rejections(String label, Phase[] seen, int r) throws Exception
    {
        String t = KEYSPACE + ".t";
        String twin = KEYSPACE + ".twin";
        int c = 70 + r;
        for (int node = 1; node <= 2; node++)
        {
            Phase p = seen[node];
            String where = label + ", node " + node;
            // LWT (3.6): rejected for the partition once the key is registered; accepted and equal to the model otherwise
            String[] lwt = {
                "INSERT INTO %s (p, c1, c2, v) VALUES (1, " + c + ", 0, 1) IF NOT EXISTS",
                "INSERT INTO %s (p, c1, c2, v) VALUES (1, " + c + ", 1, 1) IF NOT EXISTS USING TTL 100000",
                "UPDATE %s SET v = 7 WHERE p = 1 AND c1 = " + c + " AND c2 = 0 IF v = 1",
                "UPDATE %s SET v = 8 WHERE p = 1 AND c1 = " + c + " AND c2 = 0 IF v = 12345",
                "DELETE FROM %s WHERE p = 1 AND c1 = 9999 AND c2 = 0 IF EXISTS",
                "BEGIN BATCH INSERT INTO %s (p, c1, c2, v) VALUES (1, " + c + ", 5, 5) IF NOT EXISTS; "
                + "INSERT INTO %s (p, c1, c2, v) VALUES (1, " + c + ", 6, 6); APPLY BATCH" };
            for (String statement : lwt)
            {
                if (p != null)
                    expectRejected(where + " LWT", node, p, statement.replace("%s", t), ConsistencyLevel.QUORUM, ADAPTED);
                else
                {
                    // the twin first: it decides what the model returns
                    Object[][] model = cluster.coordinator(node).execute(statement.replace("%s", twin), ConsistencyLevel.QUORUM);
                    Object[][] actual = cluster.coordinator(node).execute(statement.replace("%s", t), ConsistencyLevel.QUORUM);
                    if (!Arrays.deepEquals(model, actual))
                        failures.add(where + " (sees " + p + ") LWT result: " + statement + "\n  model:  " + Arrays.deepToString(model) + "\n  actual: " + Arrays.deepToString(actual));
                }
                // a partition that is not split: always accepted, and equal to the model
                String other = statement.replace("p = 1", "p = 2").replace("(1, ", "(2, ").replace("%s", "%T");
                Object[][] model = cluster.coordinator(node).execute(other.replace("%T", twin), ConsistencyLevel.QUORUM);
                Object[][] actual = cluster.coordinator(node).execute(other.replace("%T", t), ConsistencyLevel.QUORUM);
                if (!Arrays.deepEquals(model, actual))
                    failures.add(where + " LWT on a partition that is not split: " + other + "\n  model:  " + Arrays.deepToString(model) + "\n  actual: " + Arrays.deepToString(actual));
            }
            c += 10;

            // SERIAL and LOCAL_SERIAL reads (3.4, 3.6): rejected where the overflow table is read (C2, C3), the model's answer elsewhere
            for (ConsistencyLevel cl : new ConsistencyLevel[]{ ConsistencyLevel.SERIAL, ConsistencyLevel.LOCAL_SERIAL })
                for (String read : new String[]{ "SELECT * FROM %s WHERE p = 1 AND c1 = 2 AND c2 = 3", "SELECT * FROM %s WHERE p = 1" })
                {
                    if (p != null && p.coordinatorClass >= 2)
                        expectRejected(where + " " + cl + " read", node, p, read.replace("%s", t), cl,
                                       cl + " reads are not supported on partition 0x00000001 of " + t + " while it is adapted");
                    else
                    {
                        String text = attempt(node, read.replace("%s", t), cl);
                        if (text != null)
                            failures.add(where + " (sees " + p + "): " + cl + " read of the partition is documented as accepted (3.4 replaces only C2 and C3 reads), got: " + text);
                        else if (!Arrays.deepEquals(cluster.coordinator(node).execute(read.replace("%s", twin), cl), cluster.coordinator(node).execute(read.replace("%s", t), cl)))
                            failures.add(where + " (sees " + p + "): " + cl + " read differs from the model: " + read);
                    }
                    // a partition that is not split can be read serially
                    String ok = read.replace("p = 1", "p = 2").replace("%s", t);
                    String text = attempt(node, ok, cl);
                    if (text != null)
                        failures.add(where + ": " + cl + " read of a partition that is not split: " + text);
                }

            if (p == null)
                continue;
            // 3.3 / section 5: direct writes to the overflow table, whatever their kind
            String[] direct = {
                "INSERT INTO " + KEYSPACE + '.' + PHYSICAL + " (p, \"__al_bucket\", c1, c2, v) VALUES (1, 256, 0, 0, 0)",
                "INSERT INTO " + KEYSPACE + '.' + PHYSICAL + " (p, \"__al_bucket\", c1, c2, v) VALUES (1, 256, 0, 0, 0) IF NOT EXISTS",
                "UPDATE " + KEYSPACE + '.' + PHYSICAL + " SET v = 1 WHERE p = 1 AND \"__al_bucket\" = 256 AND c1 = 0 AND c2 = 0",
                "DELETE FROM " + KEYSPACE + '.' + PHYSICAL + " WHERE p = 1 AND \"__al_bucket\" = 256 AND c1 = 0 AND c2 = 0",
                "BEGIN BATCH INSERT INTO " + KEYSPACE + '.' + PHYSICAL + " (p, \"__al_bucket\", c1, c2, v) VALUES (1, 256, 0, 0, 0); "
                + "INSERT INTO " + t + " (p, c1, c2, v) VALUES (2, 3, 3, 3); APPLY BATCH" };
            for (String statement : direct)
                expectRejected(where + " direct write", node, p, statement, ConsistencyLevel.QUORUM, statement.contains("IF NOT EXISTS") ? "is managed" : MANAGED);
            // client reads of the overflow table are allowed (5.1)
            String read = attempt(node, "SELECT * FROM " + KEYSPACE + '.' + PHYSICAL + " WHERE p = 1 AND \"__al_bucket\" = 256", ConsistencyLevel.QUORUM);
            if (read != null)
                failures.add(where + " (sees " + p + "): a read of the overflow table is allowed by section 5.1, got: " + read);
            // TRUNCATE (3.6): of the logical table only in STEADY; of the overflow table never
            if (p != Phase.STEADY)
                expectRejected(where + " truncate", node, p, "TRUNCATE " + t, ConsistencyLevel.ALL,
                               "cannot be truncated while a partition is being adapted (phase " + p + ')');
            expectRejected(where + " truncate of the overflow table", node, p, "TRUNCATE " + KEYSPACE + '.' + PHYSICAL, ConsistencyLevel.ALL, "is managed");
        }
        // DDL (section 5, D7, H21): judged by the owner, the node that commits it
        Phase p = seen[1];
        if (p != null)
        {
            expectRejected(label + " DDL", 2, p, "ALTER TABLE " + KEYSPACE + '.' + PHYSICAL + " ADD extra int", ConsistencyLevel.ALL, "is managed by the adapted layout");
            expectRejected(label + " DDL", 1, p, "DROP TABLE " + KEYSPACE + '.' + PHYSICAL, ConsistencyLevel.ALL, "is managed by the adapted layout");
            expectRejected(label + " DDL", 2, p, "ALTER TABLE " + KEYSPACE + ".t ADD extra int", ConsistencyLevel.ALL,
                           "columns cannot be added, dropped, renamed or altered until the layout is removed");
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // paging states across a transition (P5)
    // ---------------------------------------------------------------------------------------------------------

    private static final String PAGED = "SELECT * FROM %s WHERE p = 1";

    /** The first page of the partition through node 1 and node 2, compared with the model; returns {state1, state2, rows1, rows2}. */
    private String[] takePagingState()
    {
        String[] out = new String[4];
        for (int node = 1; node <= 2; node++)
        {
            String[] model = page(cluster, node, String.format(PAGED, KEYSPACE + ".twin"), 7, "");
            String[] actual = page(cluster, node, String.format(PAGED, KEYSPACE + ".t"), 7, "");
            if (!model[0].equals(actual[0]))
                failures.add("first page through node " + node + " in " + current + "\n  model:  " + model[0] + "\n  actual: " + actual[0]);
            out[node - 1] = actual[1];
            out[node + 1] = actual[0];
        }
        return out;
    }

    /** Continues each state through the other node, on the table and on the twin: the rest of the partition must be the model's. */
    private void resumePagingState(String[] taken, String transition)
    {
        for (int node = 1; node <= 2; node++)
        {
            String state = taken[node - 1];
            int through = 3 - node;
            String model = "";
            String actual = "";
            String modelState = state;
            String actualState = state;
            while (!modelState.isEmpty())
            {
                String[] next = page(cluster, through, String.format(PAGED, KEYSPACE + ".twin"), 7, modelState);
                model += next[0];
                modelState = next[1];
            }
            while (!actualState.isEmpty())
            {
                String[] next = page(cluster, through, String.format(PAGED, KEYSPACE + ".t"), 7, actualState);
                actual += next[0];
                actualState = next[1];
            }
            if (!model.equals(actual))
                failures.add("paging state taken before " + transition + " through node " + node + ", resumed through node " + through
                             + "\n  model:  " + abbreviate(model) + "\n  actual: " + abbreviate(actual));
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // the workload around a commit
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Writers and readers through both coordinators while the next phase is committed. A write is repeated, with the
     * same timestamp, until it is acknowledged (a timeout around a phase change is allowed, D4); once acknowledged it
     * must be read back through either coordinator, point and slice, from then on; it is then applied to the model.
     */
    private final class WorkloadHandle
    {
        private final ConcurrentSkipListSet<Integer> acknowledged = new ConcurrentSkipListSet<>();
        private final AtomicReference<String> mismatch = new AtomicReference<>();
        private volatile boolean stop;
        private final Thread writer;
        private final Thread reader;
        private final int first;

        WorkloadHandle()
        {
            first = 1000 + round * 1000;
            writer = new Thread(() -> {
                int seq = 0;
                while (!stop && mismatch.get() == null)
                {
                    int c1 = first + seq;
                    int node = 1 + (seq % 2);
                    String insert = "INSERT INTO %s (p, c1, c2, v) VALUES (1, " + c1 + ", 0, " + seq + ") USING TIMESTAMP " + (2_000_000_000L + c1);
                    boolean done = false;
                    for (int attempt = 0; attempt < 40 && !done; attempt++)
                    {
                        try
                        {
                            cluster.coordinator(node).execute(String.format(insert, KEYSPACE + ".t"), ConsistencyLevel.QUORUM);
                            done = true;
                        }
                        catch (Throwable t)
                        {
                            sleep(50);
                        }
                    }
                    if (!done)
                    {
                        mismatch.compareAndSet(null, "write of " + c1 + " through node " + node + " never succeeded");
                        return;
                    }
                    cluster.coordinator(node).execute(String.format(insert, KEYSPACE + ".twin"), ConsistencyLevel.QUORUM);
                    acknowledged.add(seq);
                    seq++;
                    sleep(20);
                }
            }, "compat-writer");
            reader = new Thread(() -> {
                Random random = new Random(11);
                while (!stop && mismatch.get() == null)
                {
                    List<Integer> snapshot = new ArrayList<>(acknowledged);
                    if (snapshot.isEmpty())
                    {
                        sleep(10);
                        continue;
                    }
                    int node = 1 + random.nextInt(2);
                    try
                    {
                        if (random.nextBoolean())
                        {
                            int seq = snapshot.get(random.nextInt(snapshot.size()));
                            Object[][] rows = cluster.coordinator(node).execute("SELECT v FROM " + KEYSPACE + ".t WHERE p = 1 AND c1 = ? AND c2 = 0",
                                                                                ConsistencyLevel.QUORUM, first + seq);
                            if (rows.length != 1 || !Integer.valueOf(seq).equals(rows[0][0]))
                                mismatch.compareAndSet(null, "acknowledged write " + (first + seq) + " read through node " + node + ": " + Arrays.deepToString(rows));
                        }
                        else
                        {
                            Object[][] rows = cluster.coordinator(node).execute("SELECT c1, v FROM " + KEYSPACE + ".t WHERE p = 1 AND c1 >= " + first
                                                                                + " AND c1 < " + (first + 1000), ConsistencyLevel.QUORUM);
                            java.util.Set<Integer> seen = new java.util.HashSet<>();
                            int previous = Integer.MAX_VALUE;
                            for (Object[] row : rows)
                            {
                                int c1 = (Integer) row[0];
                                if (c1 >= previous)
                                    mismatch.compareAndSet(null, "slice through node " + node + " out of order at " + c1);
                                previous = c1;
                                if (!Integer.valueOf(c1 - first).equals(row[1]))
                                    mismatch.compareAndSet(null, "slice through node " + node + ": row " + c1 + " has v " + row[1]);
                                seen.add(c1 - first);
                            }
                            for (int seq : snapshot)
                                if (!seen.contains(seq))
                                    mismatch.compareAndSet(null, "slice through node " + node + " misses acknowledged write " + (first + seq));
                        }
                    }
                    catch (Throwable t)
                    {
                        // a read that overlaps a phase change may time out (D4)
                    }
                    sleep(10);
                }
            }, "compat-reader");
            writer.setDaemon(true);
            reader.setDaemon(true);
            writer.start();
            reader.start();
        }

        int acknowledged()
        {
            return acknowledged.size();
        }

        void awaitAcknowledged(int n) throws InterruptedException
        {
            long deadline = System.nanoTime() + 120_000_000_000L;
            while (acknowledged.size() < n && mismatch.get() == null)
            {
                if (System.nanoTime() > deadline)
                    throw new AssertionError("the workload acknowledged " + acknowledged.size() + " writes, expected " + n);
                Thread.sleep(20);
            }
        }

        void finish() throws InterruptedException
        {
            stop = true;
            writer.join(60_000);
            reader.join(60_000);
            if (mismatch.get() != null)
                failures.add("workload around the commit to " + current + ": " + mismatch.get());
        }
    }

    private static void sleep(long millis)
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
}
