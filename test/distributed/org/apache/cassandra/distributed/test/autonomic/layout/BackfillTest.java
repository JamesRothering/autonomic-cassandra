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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator;
import org.apache.cassandra.autonomic.coord.TcmEnvironment;
import org.apache.cassandra.autonomic.layout.AdvanceLayout;
import org.apache.cassandra.autonomic.layout.BackfillTask;
import org.apache.cassandra.autonomic.layout.KeyState;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.PhysicalTables;
import org.apache.cassandra.autonomic.layout.RowMapper;
import org.apache.cassandra.config.DataRateSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.Clock;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.BUCKETS;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.K;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.LAYOUT_ID;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.PHYSICAL;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.advance;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.register;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * T6.7: the replica backfill on three nodes with replication factor 3. There is no actuator yet (T6.9): the registry
 * is moved by hand and the nodes do the rest. Nodes 2 and 3 run with the autonomic layer disabled; the backfill is
 * unconditional (design 4.3).
 *
 * <p>The partition holds live rows, rows with a TTL, row tombstones, a range tombstone with newer rows inside it and
 * an overwritten collection; one delete never reached node 3. Afterwards every node holds, in the overflow table,
 * exactly what node 1 holds in the base table: {@link #held} compares the unpurged local content cell for cell.
 */
public class BackfillTest extends TestBaseImpl
{
    static final String TABLE = "CREATE TABLE " + KEYSPACE + ".t (p int, c int, v int, s text, m map<int, text>, PRIMARY KEY (p, c))";

    private static ClusterCoordinator coordinator()
    {
        return new ClusterCoordinator(new TcmEnvironment(Clock.Global::currentTimeMillis, DatabaseDescriptor::getAutonomicConfig));
    }

    private static ByteBuffer key()
    {
        return Int32Type.instance.decompose(K);
    }

    /** Restarts the backfill generation of the key in its phase; the generation committed is {@code generation}. */
    static void restartGeneration(Cluster cluster, String id, Phase phase, int generation)
    {
        String phaseName = phase.name();
        cluster.get(1).runOnInstance(() -> {
            ClusterCoordinator c = coordinator();
            Claim now = c.ownClaim(UUID.fromString(id));
            TableId logical = Schema.instance.getTableMetadata(KEYSPACE, "t").id;
            Phase p = Phase.valueOf(phaseName);
            AdvanceLayout a = new AdvanceLayout(now.actionId, now.owner, now.step, logical, key(), p, p, generation,
                                                Clock.Global.currentTimeMillis(), 600_000);
            assertTrue(c.commitInstead(now, a.wrap(), p.name(), now.state).ok());
        });
    }

    /** {@code LayoutWritesTest.advance} at a generation other than 1. */
    static void advanceAt(Cluster cluster, String id, Phase from, Phase to, int generation)
    {
        String fromName = from.name();
        String toName = to.name();
        cluster.get(1).runOnInstance(() -> {
            ClusterCoordinator c = coordinator();
            Claim now = c.ownClaim(UUID.fromString(id));
            TableId logical = Schema.instance.getTableMetadata(KEYSPACE, "t").id;
            AdvanceLayout a = new AdvanceLayout(now.actionId, now.owner, now.step, logical, key(), Phase.valueOf(fromName), Phase.valueOf(toName),
                                                generation, Clock.Global.currentTimeMillis(), 600_000);
            assertTrue(c.commitInstead(now, a.wrap(), toName, now.state).ok());
        });
        ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
    }

    /** "generation:n" where n is the number of nodes in backfillDone, as the node sees the key. */
    static String progress(IInvokableInstance node)
    {
        return node.callOnInstance(() -> {
            KeyState state = LayoutRegistry.of(ClusterMetadata.current()).byLogical.values().iterator().next().keyState(key());
            return state.generation + ":" + state.backfillDone.size();
        });
    }

    static void awaitBackfill(Cluster cluster, int generation, int nodes, long timeoutSeconds) throws InterruptedException
    {
        String expected = generation + ":" + nodes;
        long deadline = System.nanoTime() + timeoutSeconds * 1_000_000_000L;
        while (!expected.equals(progress(cluster.get(1))))
        {
            if (System.nanoTime() > deadline)
                throw new AssertionError("backfill is at " + progress(cluster.get(1)) + ", expected " + expected);
            Thread.sleep(200);
        }
    }

    static long stat(IInvokableInstance node, String name)
    {
        return node.callOnInstance(() -> {
            switch (name)
            {
                case "chunks": return BackfillTask.stats.chunksWritten.get();
                case "bytes": return BackfillTask.stats.bytesWritten.get();
                case "failures": return BackfillTask.stats.writeFailures.get();
                case "throttled": return BackfillTask.stats.throttledMicros.get();
                case "resumed": return BackfillTask.stats.passesResumed.get();
                case "reports": return BackfillTask.stats.reportsCommitted.get();
                default: throw new IllegalArgumentException(name);
            }
        });
    }

    // ---- what a node holds, unpurged ----

    private static PartitionUpdate read(TableMetadata table, DecoratedKey key)
    {
        // nowInSec 0: the local read does not purge
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

    private static String describe(PartitionUpdate update, TableMetadata logical, int maxClustering)
    {
        StringBuilder sb = new StringBuilder("partition deletion ").append(update.partitionLevelDeletion()).append('\n');
        for (Row row : update)
            sb.append(row.toString(logical, true)).append('\n');
        // the range tombstone over every clustering, whatever pieces it is stored in
        for (int c = -2; c <= maxClustering + 2; c++)
        {
            Clustering<?> clustering = Clustering.make(Int32Type.instance.decompose(c));
            DeletionTime covering = DeletionTime.LIVE;
            for (Iterator<RangeTombstone> it = update.deletionInfo().rangeIterator(false); it.hasNext(); )
            {
                RangeTombstone range = it.next();
                if (range.deletedSlice().includes(logical.comparator, clustering) && range.deletionTime().supersedes(covering))
                    covering = range.deletionTime();
            }
            if (!covering.isLive())
                sb.append("range tombstone over ").append(c).append(": ").append(covering).append('\n');
        }
        return sb.toString();
    }

    /**
     * What the node holds of partition {@link LayoutWritesTest#K}: in the base table, or in the buckets of incarnation 1
     * of the overflow table mapped back to the logical table. Rows with all their cells, timestamps, TTLs and deletion
     * times, row and complex deletions, the partition deletion, and the range tombstone over each clustering.
     */
    static String held(IInvokableInstance node, boolean overflow, int maxClustering)
    {
        return node.callOnInstance(() -> {
            TableMetadata logical = Schema.instance.getTableMetadata(KEYSPACE, "t");
            if (!overflow)
                return describe(read(logical, logical.partitioner.decorateKey(key())), logical, maxClustering);
            TableMetadata physical = Schema.instance.getTableMetadata(KEYSPACE, PHYSICAL);
            List<PartitionUpdate> buckets = new ArrayList<>();
            for (int bucket = 0; bucket < BUCKETS; bucket++)
                buckets.add(RowMapper.toLogical(read(physical, RowMapper.physicalKey(logical, physical, key(), 1, bucket)), logical));
            return describe(PartitionUpdate.merge(buckets), logical, maxClustering);
        });
    }

    private static Set<Integer> overflowClusterings(IInvokableInstance node)
    {
        Set<Integer> cs = new TreeSet<>();
        for (Object[] row : node.executeInternal("SELECT p, c FROM " + KEYSPACE + '.' + PHYSICAL))
            if ((Integer) row[0] == K)
                cs.add((Integer) row[1]);
        return cs;
    }

    private static final String SELECT = "SELECT c, v, writetime(v), s, writetime(s), m, ttl(v) FROM " + KEYSPACE + ".t WHERE p = " + K;

    /** The rows without the remaining TTL, which moves with the clock, and the TTLs. */
    private static Object[][] withoutTtl(Object[][] rows)
    {
        Object[][] copy = new Object[rows.length][];
        for (int i = 0; i < rows.length; i++)
            copy[i] = java.util.Arrays.copyOf(rows[i], rows[i].length - 1);
        return copy;
    }

    @Test
    public void everyReplicaCopiesWhatItHolds() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                             .set("autonomic.enabled", c.num() == 1)
                                                             .set("hinted_handoff_enabled", false)
                                                             .set("autonomic.layout.backfill_chunk_rows", 7))
                                           .start()))
        {
            IInvokableInstance n1 = cluster.get(1);
            cluster.schemaChange(TABLE);
            String derived = n1.callOnInstance(() -> PhysicalTables.derive(Schema.instance.getTableMetadata(KEYSPACE, "t"), LAYOUT_ID, TableId.generate()));
            cluster.schemaChange(derived);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);

            // ---- the partition, before anything is registered ----
            String s = "s".repeat(600);
            String insert = "INSERT INTO " + KEYSPACE + ".t (p, c, v, s, m) VALUES (" + K + ", ?, ?, ?, {1: 'a', 2: 'b'}) USING TIMESTAMP ?";
            for (int c = 0; c < 40; c++)
                cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, c, c, s + c, 1000L + c);
            for (int c = 40; c < 45; c++)
                cluster.coordinator(2).execute("INSERT INTO " + KEYSPACE + ".t (p, c, v, s) VALUES (" + K + ", ?, ?, 'ttl') USING TIMESTAMP 1100 AND TTL 100000",
                                               ConsistencyLevel.ALL, c, c);
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".t (p, c, v) VALUES (2, 1, 1)", ConsistencyLevel.ALL);
            cluster.get(2).flush(KEYSPACE);
            // a row tombstone, a range tombstone with two newer rows inside it, a collection overwrite, a cell tombstone
            cluster.coordinator(1).execute("DELETE FROM " + KEYSPACE + ".t USING TIMESTAMP 2000 WHERE p = " + K + " AND c = 3", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("DELETE FROM " + KEYSPACE + ".t USING TIMESTAMP 2001 WHERE p = " + K + " AND c >= 10 AND c < 20", ConsistencyLevel.ALL);
            cluster.coordinator(3).execute(insert, ConsistencyLevel.ALL, 12, 120, "after", 3000L);
            cluster.coordinator(3).execute(insert, ConsistencyLevel.ALL, 15, 150, "after", 3000L);
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".t USING TIMESTAMP 2002 SET m = {9: 'nine'} WHERE p = " + K + " AND c = 5", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("DELETE s FROM " + KEYSPACE + ".t USING TIMESTAMP 2003 WHERE p = " + K + " AND c = 6", ConsistencyLevel.ALL);
            cluster.get(3).flush(KEYSPACE);
            // a row delete and a range delete that node 3 never sees
            cluster.filters().verbs(Verb.MUTATION_REQ.id).to(3).drop().on();
            cluster.coordinator(1).execute("DELETE FROM " + KEYSPACE + ".t USING TIMESTAMP 2004 WHERE p = " + K + " AND c = 4", ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute("DELETE FROM " + KEYSPACE + ".t USING TIMESTAMP 2005 WHERE p = " + K + " AND c >= 30 AND c <= 32", ConsistencyLevel.QUORUM);
            cluster.filters().reset();
            assertEquals("node 3 missed the delete", 1, cluster.get(3).executeInternal("SELECT c FROM " + KEYSPACE + ".t WHERE p = ? AND c = 4", K).length);
            assertEquals(0, n1.executeInternal("SELECT c FROM " + KEYSPACE + ".t WHERE p = ? AND c = 4", K).length);
            String base1 = held(n1, false, 50);
            assertEquals("nodes 1 and 2 hold the same", base1, held(cluster.get(2), false, 50));
            assertFalse(base1.equals(held(cluster.get(3), false, 50)));
            assertTrue(base1, base1.contains("range tombstone over 10:") && base1.contains("range tombstone over 31:"));

            // ---- backfill ----
            String id = register(n1, 1);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            advance(cluster, id, Phase.ANNOUNCED, Phase.DUAL_WRITE);
            for (int node = 1; node <= 3; node++)
                assertEquals("nothing is copied before BACKFILL", 0, stat(cluster.get(node), "chunks"));
            advance(cluster, id, Phase.DUAL_WRITE, Phase.BACKFILL);
            awaitBackfill(cluster, 1, 3, 120);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);

            for (int node = 1; node <= 3; node++)
            {
                IInvokableInstance n = cluster.get(node);
                // node 1 holds everything any replica holds; node 3 holds less, and its overflow copy has it all too
                assertEquals("overflow table of node " + node, base1, held(n, true, 50));
                assertTrue("several chunks on node " + node, stat(n, "chunks") >= 5);
                assertEquals(1, stat(n, "reports"));
                for (Object[] row : n.executeInternal("SELECT key FROM system_autonomic.local_state"))
                    assertFalse("the record is removed with the report: " + row[0], ((String) row[0]).startsWith("layout/"));
                Set<Integer> cs = overflowClusterings(n);
                assertFalse(cs.contains(3) || cs.contains(4) || cs.contains(10) || cs.contains(31));
                assertTrue(cs.contains(12) && cs.contains(15) && cs.contains(44) && cs.contains(0) && cs.contains(39));
            }

            // ---- a late write older than a tombstone stays dead, in both copies, on every node ----
            for (int c : new int[]{ 3, 4, 11, 31 })
                cluster.coordinator(1 + c % 3).execute(insert, ConsistencyLevel.ALL, c, 999, "late", 1500L);
            // and one that is not older than anything lands in both
            cluster.coordinator(2).execute(insert, ConsistencyLevel.ALL, 60, 60, "new", 1500L);
            for (int node = 1; node <= 3; node++)
            {
                Set<Integer> cs = overflowClusterings(cluster.get(node));
                assertFalse("node " + node + ": " + cs, cs.contains(3) || cs.contains(4) || cs.contains(11) || cs.contains(31));
                assertTrue(cs.contains(60));
            }
            Object[][] viaBase = cluster.coordinator(1).execute(SELECT, ConsistencyLevel.ALL);
            assertEquals(33, viaBase.length);

            // ---- throttle: a second generation at 8 kB/s ----
            long[] bytes = new long[4];
            long[] throttled = new long[4];
            for (int node = 1; node <= 3; node++)
            {
                cluster.get(node).runOnInstance(() -> DatabaseDescriptor.getAutonomicConfig().layout.backfill_throughput = new DataRateSpec.LongBytesPerSecondBound("8000B/s"));
                bytes[node] = stat(cluster.get(node), "bytes");
                throttled[node] = stat(cluster.get(node), "throttled");
            }
            long start = System.nanoTime();
            restartGeneration(cluster, id, Phase.BACKFILL, 2);
            awaitBackfill(cluster, 2, 3, 180);
            long elapsedMicros = (System.nanoTime() - start) / 1000;
            for (int node = 1; node <= 3; node++)
            {
                long copied = stat(cluster.get(node), "bytes") - bytes[node];
                long waited = stat(cluster.get(node), "throttled") - throttled[node];
                assertTrue("node " + node + " copied " + copied + " bytes", copied > 10_000);
                // nothing waits for the first chunk and nothing is written after the last: two chunks of 7 rows less
                long expectedMicros = (copied - 2 * 7 * 650) * 1_000_000L / 8000;
                assertTrue("node " + node + " copied " + copied + " bytes and waited " + waited + " us", waited >= expectedMicros * 7 / 10);
                assertTrue(elapsedMicros >= expectedMicros * 7 / 10);
                assertEquals(2, stat(cluster.get(node), "reports"));
            }
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);

            // ---- the copies read the same: through the base table now, through the overflow table after cutover ----
            // base replicas converge by the read at ALL; node 3's overflow copy was complete all along
            viaBase = cluster.coordinator(1).execute(SELECT, ConsistencyLevel.ALL);
            advanceAt(cluster, id, Phase.BACKFILL, Phase.VERIFY, 2);
            advanceAt(cluster, id, Phase.VERIFY, Phase.CUTOVER, 2);
            for (int node = 1; node <= 3; node++)
            {
                Object[][] viaOverflow = cluster.coordinator(node).execute(SELECT, ConsistencyLevel.ONE);
                assertArrayEquals("values and write times, read at ONE through node " + node, withoutTtl(viaBase), withoutTtl(viaOverflow));
                for (int i = 0; i < viaBase.length; i++)
                {
                    Integer before = (Integer) viaBase[i][6];
                    Integer after = (Integer) viaOverflow[i][6];
                    assertEquals(before == null, after == null);
                    if (before != null)
                        assertTrue("ttl " + before + " then " + after, after <= before && after >= before - 120);
                }
            }
        }
    }
}
