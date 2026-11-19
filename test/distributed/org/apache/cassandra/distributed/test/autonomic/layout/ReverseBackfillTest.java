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
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.coord.TcmEnvironment;
import org.apache.cassandra.autonomic.layout.AdvanceLayout;
import org.apache.cassandra.autonomic.layout.BackfillTask;
import org.apache.cassandra.autonomic.layout.BeginUnsplit;
import org.apache.cassandra.autonomic.layout.KeyState;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.NaturalReplicas;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.PhysicalTables;
import org.apache.cassandra.autonomic.layout.RowMapper;
import org.apache.cassandra.autonomic.layout.TableLayout;
import org.apache.cassandra.batchlog.BatchlogManager;
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
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.autonomic.layout.BackfillTest.stat;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.BUCKETS;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.K;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.LAYOUT_ID;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.PHYSICAL;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.advance;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.register;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * T6.17c: the reverse pass on three nodes with replication factor 2, so that the bucket partitions of the key sit on
 * different pairs of nodes. The registry is moved by hand (there is no reverse actuator yet): forward to
 * {@code NEW_ONLY}, the content written while split (it reaches the buckets only), {@code BeginUnsplit} in fast mode,
 * then the reverse phases. The buckets hold live rows, TTL rows, row, cell and range tombstones, a partition deletion
 * with newer rows, and a collection overwrite; one row delete and one range delete never reached node 3.
 *
 * <p>Node 2 is restarted in the middle of its pass and resumes from its per-bucket records. Afterwards every base
 * replica of the key holds exactly the merge of every node's buckets, cell for cell, read unpurged; and a read of the
 * base table returns the same values, write times and TTLs as a read of the overflow table did.
 */
public class ReverseBackfillTest extends TestBaseImpl
{
    private static final int MAX_CLUSTERING = 50;

    private static ClusterCoordinator coordinator()
    {
        return new ClusterCoordinator(new TcmEnvironment(Clock.Global::currentTimeMillis, DatabaseDescriptor::getAutonomicConfig));
    }

    private static ByteBuffer key()
    {
        return Int32Type.instance.decompose(K);
    }

    /** "phase:generation:reports" as node 1 sees the key. */
    private static String progress(IInvokableInstance node)
    {
        return node.callOnInstance(() -> {
            KeyState state = LayoutRegistry.of(ClusterMetadata.current()).byLogical.values().iterator().next().keyState(key());
            return state.phase + ":" + state.generation + ":" + state.backfillDone.size();
        });
    }

    private static int generation(IInvokableInstance node)
    {
        return node.callOnInstance(() -> LayoutRegistry.of(ClusterMetadata.current()).byLogical.values().iterator().next().keyState(key()).generation);
    }

    /** Releases the forward action's claim and begins a fast reverse from NEW_ONLY as a second action. */
    private static String beginReverse(Cluster cluster, String forward) throws InterruptedException
    {
        IInvokableInstance n1 = cluster.get(1);
        n1.runOnInstance(() -> {
            ClusterCoordinator c = coordinator();
            assertTrue(c.release(c.ownClaim(UUID.fromString(forward)), "COMMITTED").ok());
        });
        Thread.sleep(2500); // the cooldown of the forward action, configured to 1 s
        String id = n1.callOnInstance(() -> TimeUUID.Generator.nextTimeUUID().asUUID().toString());
        n1.runOnInstance(() -> {
            ClusterCoordinator c = coordinator();
            ClusterCoordinator.Outcome outcome = c.claim(Lane.SCHEMA_LAYOUT, UUID.fromString(id), "partition_split", "TABLE:t", "CLAIMED", ImmutableMap.of());
            assertTrue(String.valueOf(outcome.detail), outcome.ok());
            Claim own = c.ownClaim(UUID.fromString(id));
            TableId logical = Schema.instance.getTableMetadata(KEYSPACE, "t").id;
            BeginUnsplit begin = new BeginUnsplit(own.actionId, own.owner, own.step, logical, key(), KeyState.ReverseMode.FAST, UUID.fromString(forward),
                                                  Clock.Global.currentTimeMillis(), 600_000);
            assertTrue(c.commitInstead(own, begin.wrap(), Phase.REVERSE_ANNOUNCED.name(), own.state).ok());
        });
        ClusterUtils.waitForCMSToQuiesce(cluster, n1);
        return id;
    }

    /** Moves the key from {@code from} to {@code to}; {@code from == to} restarts the generation. */
    private static void advanceReverse(Cluster cluster, String id, Phase from, Phase to)
    {
        String fromName = from.name();
        String toName = to.name();
        cluster.get(1).runOnInstance(() -> {
            ClusterCoordinator c = coordinator();
            Claim now = c.ownClaim(UUID.fromString(id));
            TableId logical = Schema.instance.getTableMetadata(KEYSPACE, "t").id;
            KeyState state = LayoutRegistry.of(ClusterMetadata.current()).layout(logical).keyState(key());
            Phase f = Phase.valueOf(fromName);
            Phase t = Phase.valueOf(toName);
            boolean newGeneration = f == t || (f == Phase.REVERSE_VERIFY && t == Phase.REVERSE_BACKFILL);
            AdvanceLayout a = new AdvanceLayout(now.actionId, now.owner, now.step, logical, key(), f, t,
                                                state.generation + (newGeneration ? 1 : 0), Clock.Global.currentTimeMillis(), 600_000);
            assertTrue(c.commitInstead(now, a.wrap(), toName, now.state).ok());
        });
        ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
    }

    // ---- what a node holds, unpurged ----

    private static PartitionUpdate read(TableMetadata table, DecoratedKey key)
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

    private static String describe(PartitionUpdate update, TableMetadata logical)
    {
        StringBuilder sb = new StringBuilder("partition deletion ").append(update.partitionLevelDeletion()).append('\n');
        for (Row row : update)
            sb.append(row.toString(logical, true)).append('\n');
        for (int c = -2; c <= MAX_CLUSTERING + 2; c++)
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

    /** The node's bucket partitions of incarnation 1, mapped to the logical table, merged and serialized. */
    private static ByteBuffer buckets(IInvokableInstance node)
    {
        byte[] bytes = node.callOnInstance(() -> {
            TableMetadata logical = Schema.instance.getTableMetadata(KEYSPACE, "t");
            TableMetadata physical = Schema.instance.getTableMetadata(KEYSPACE, PHYSICAL);
            List<PartitionUpdate> buckets = new ArrayList<>();
            for (int bucket = 0; bucket < BUCKETS; bucket++)
                buckets.add(RowMapper.toLogical(read(physical, RowMapper.physicalKey(logical, physical, key(), 1, bucket)), logical));
            return ByteBufferUtil.getArray(PartitionUpdate.toBytes(PartitionUpdate.merge(buckets), MessagingService.current_version));
        });
        return ByteBuffer.wrap(bytes);
    }

    /** {@link #describe} of the merge of the serialized updates, computed on node 1. */
    private static String merged(IInvokableInstance node, List<ByteBuffer> serialized)
    {
        List<byte[]> arrays = new ArrayList<>();
        for (ByteBuffer b : serialized)
            arrays.add(ByteBufferUtil.getArray(b));
        return node.callOnInstance(() -> {
            List<PartitionUpdate> updates = new ArrayList<>();
            for (byte[] a : arrays)
                updates.add(PartitionUpdate.fromBytes(ByteBuffer.wrap(a), MessagingService.current_version));
            return describe(PartitionUpdate.merge(updates), Schema.instance.getTableMetadata(KEYSPACE, "t"));
        });
    }

    private static String base(IInvokableInstance node)
    {
        return node.callOnInstance(() -> {
            TableMetadata logical = Schema.instance.getTableMetadata(KEYSPACE, "t");
            return describe(read(logical, logical.partitioner.decorateKey(key())), logical);
        });
    }

    /** True if the node has a reverse resume record that is not DONE. */
    private static boolean inTheMiddle(IInvokableInstance node)
    {
        int done = 0;
        for (Object[] row : node.executeInternal("SELECT key, value FROM system_autonomic.local_state"))
        {
            String k = (String) row[0];
            if (!k.startsWith("layout/") || !k.contains("/r"))
                continue;
            if (((String) row[1]).endsWith(" DONE"))
                done++;
            else
                return true;
        }
        return done > 0;
    }

    private static final String SELECT = "SELECT c, v, writetime(v), s, writetime(s), m, ttl(v) FROM " + KEYSPACE + ".t WHERE p = " + K;

    @Test
    public void everyBucketReplicaCopiesIntoBaseAndARestartResumes() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                             .set("autonomic.enabled", c.num() == 1)
                                                             .set("hinted_handoff_enabled", false)
                                                             .set("autonomic.layout.backfill_chunk_rows", 4)
                                                             .set("autonomic.safety", Map.of("cooldown_after_commit", "1s", "cooldown_after_rollback", "1s")))
                                           .start(), 2))
        {
            IInvokableInstance n1 = cluster.get(1);
            IInvokableInstance n2 = cluster.get(2);
            cluster.schemaChange(BackfillTest.TABLE);
            String derived = n1.callOnInstance(() -> PhysicalTables.derive(Schema.instance.getTableMetadata(KEYSPACE, "t"), LAYOUT_ID, TableId.generate()));
            cluster.schemaChange(derived);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);

            // ---- split, with nothing in base ----
            String forward = register(n1, 1);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            Phase[] chain = { Phase.ANNOUNCED, Phase.DUAL_WRITE, Phase.BACKFILL, Phase.VERIFY, Phase.CUTOVER, Phase.NEW_ONLY };
            for (int i = 0; i + 1 < chain.length; i++)
                advance(cluster, forward, chain[i], chain[i + 1]);

            // ---- written while split: the buckets only ----
            String s = "s".repeat(250);
            String insert = "INSERT INTO " + KEYSPACE + ".t (p, c, v, s, m) VALUES (" + K + ", ?, ?, ?, {1: 'a', 2: 'b'}) USING TIMESTAMP ?";
            for (int c = 0; c < 40; c++)
                cluster.coordinator(1 + c % 3).execute(insert, ConsistencyLevel.ALL, c, c, s + c, 1000L + c);
            for (int c = 40; c < 45; c++)
                cluster.coordinator(2).execute("INSERT INTO " + KEYSPACE + ".t (p, c, v, s) VALUES (" + K + ", ?, ?, 'ttl') USING TIMESTAMP 1100 AND TTL 100000",
                                               ConsistencyLevel.ALL, c, c);
            cluster.forEach(n -> n.flush(KEYSPACE));
            // a partition deletion that shadows rows 0..5, two of them written again after it
            cluster.coordinator(1).execute("DELETE FROM " + KEYSPACE + ".t USING TIMESTAMP 1005 WHERE p = " + K, ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, 1, 101, "again", 3000L);
            cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, 2, 102, "again", 3000L);
            cluster.coordinator(1).execute("DELETE FROM " + KEYSPACE + ".t USING TIMESTAMP 2000 WHERE p = " + K + " AND c = 7", ConsistencyLevel.ALL);
            cluster.coordinator(2).execute("DELETE FROM " + KEYSPACE + ".t USING TIMESTAMP 2001 WHERE p = " + K + " AND c >= 10 AND c < 20", ConsistencyLevel.ALL);
            cluster.coordinator(3).execute(insert, ConsistencyLevel.ALL, 12, 120, "after", 3000L);
            cluster.coordinator(3).execute(insert, ConsistencyLevel.ALL, 15, 150, "after", 3000L);
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".t USING TIMESTAMP 2002 SET m = {9: 'nine'} WHERE p = " + K + " AND c = 8", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("DELETE s FROM " + KEYSPACE + ".t USING TIMESTAMP 2003 WHERE p = " + K + " AND c = 9", ConsistencyLevel.ALL);
            cluster.get(3).flush(KEYSPACE);
            // a row delete and a range delete (in every bucket) that node 3 never sees. The range delete spans four
            // bucket partitions, so it is a logged batch: batchlog replay, which hinted_handoff_enabled does not turn
            // off, would deliver it to node 3 about 4 to 14 s later. Replay stays paused for the rest of the test
            cluster.forEach(n -> n.runOnInstance(() -> BatchlogManager.instance.pauseReplay()));
            cluster.filters().verbs(Verb.MUTATION_REQ.id).to(3).drop().on();
            cluster.coordinator(1).execute("DELETE FROM " + KEYSPACE + ".t USING TIMESTAMP 2004 WHERE p = " + K + " AND c = 21", ConsistencyLevel.ONE);
            cluster.coordinator(1).execute("DELETE FROM " + KEYSPACE + ".t USING TIMESTAMP 2005 WHERE p = " + K + " AND c >= 30 AND c <= 32", ConsistencyLevel.ONE);
            cluster.filters().reset();

            List<ByteBuffer> all = new ArrayList<>();
            for (int node = 1; node <= 3; node++)
                all.add(buckets(cluster.get(node)));
            String expected = merged(n1, all);
            assertTrue(expected, expected.contains("range tombstone over 31:") && expected.contains("range tombstone over 10:"));
            assertTrue(expected, expected.contains("partition deletion deletedAt=1005"));
            String node3 = merged(n1, List.of(all.get(2)));
            assertFalse("node 3 missed the range delete: " + node3, node3.contains("range tombstone over 31:"));
            assertTrue(expected, expected.contains("del=deletedAt=2004"));
            assertFalse("node 3 missed the row delete: " + node3, node3.contains("del=deletedAt=2004"));
            for (int node = 1; node <= 3; node++)
                assertEquals("nothing in base on node " + node, "partition deletion LIVE\n", base(cluster.get(node)));

            // ---- the reverse ----
            String id = beginReverse(cluster, forward);
            advanceReverse(cluster, id, Phase.REVERSE_ANNOUNCED, Phase.REVERSE_DUAL_WRITE);
            advanceReverse(cluster, id, Phase.REVERSE_DUAL_WRITE, Phase.REVERSE_BACKFILL);
            Thread.sleep(2000);
            for (int node = 1; node <= 3; node++)
                assertEquals("the generation of BeginUnsplit is not copied", 0, stat(cluster.get(node), "chunks"));

            // slow enough to stop node 2 in the middle of its buckets
            for (int node = 1; node <= 3; node++)
                cluster.get(node).runOnInstance(() -> DatabaseDescriptor.getAutonomicConfig().layout.backfill_throughput = new DataRateSpec.LongBytesPerSecondBound("1100B/s"));
            long[] reportsBefore = new long[4];
            for (int node = 1; node <= 3; node++)
                reportsBefore[node] = stat(cluster.get(node), "reports");
            int generation = generation(n1) + 1;
            advanceReverse(cluster, id, Phase.REVERSE_BACKFILL, Phase.REVERSE_BACKFILL);
            int reporters = n1.callOnInstance(() -> {
                ClusterMetadata cm = ClusterMetadata.current();
                TableMetadata logical = Schema.instance.getTableMetadata(KEYSPACE, "t");
                TableLayout layout = LayoutRegistry.of(cm).layout(logical.id);
                return NaturalReplicas.ofBuckets(cm, logical, Schema.instance.getTableMetadata(KEYSPACE, PHYSICAL), key(), 1, layout.buckets).size();
            });
            assertEquals("every node replicates a bucket", 3, reporters);

            long deadline = System.nanoTime() + 120_000_000_000L;
            while (!inTheMiddle(n2))
            {
                assertTrue("node 2 does not start its pass", System.nanoTime() < deadline);
                Thread.sleep(100);
            }
            // nodes 1 and 3 may have reported already (node 1 has three chunks to copy); node 2 has not
            assertEquals("node 2 is in the middle of its pass", reportsBefore[2], stat(n2, "reports"));
            ClusterUtils.stopUnchecked(n2);
            Thread.sleep(2000);
            n2.startup();
            n2.runOnInstance(() -> BatchlogManager.instance.pauseReplay());

            String done = Phase.REVERSE_BACKFILL + ":" + generation + ":3";
            deadline = System.nanoTime() + 300_000_000_000L;
            while (!done.equals(progress(n1)))
            {
                if (System.nanoTime() > deadline)
                    fail("the reverse pass is at " + progress(n1) + ", expected " + done);
                Thread.sleep(200);
            }
            assertTrue("node 2 resumed from its records", stat(n2, "resumed") >= 1);
            assertNotNull(n2.callOnInstance(() -> BackfillTask.stats.lastResumeRecord));
            for (int node = 1; node <= 3; node++)
            {
                // node 2 counts from its restart
                assertEquals("reports of node " + node, node == 2 ? 1 : reportsBefore[node] + 1, stat(cluster.get(node), "reports"));
                for (Object[] row : cluster.get(node).executeInternal("SELECT key FROM system_autonomic.local_state"))
                    assertFalse("the records are removed with the report: " + row[0], ((String) row[0]).startsWith("layout/"));
            }

            // ---- base on every base replica of K: the merge of every node's buckets, cell for cell ----
            int baseReplicas = 0;
            for (int node = 1; node <= 3; node++)
            {
                String held = base(cluster.get(node));
                if (held.equals("partition deletion LIVE\n"))
                    continue;
                assertEquals("base table of node " + node, expected, held);
                baseReplicas++;
            }
            assertEquals(2, baseReplicas);

            // ---- and through CQL: overflow now (C2), base after CUTBACK (C1) ----
            Object[][] viaOverflow = cluster.coordinator(1).execute(SELECT, ConsistencyLevel.ALL);
            assertEquals("45 rows, 4 under the partition deletion, 1 + 8 + 1 + 3 deleted", 28, viaOverflow.length);
            advanceReverse(cluster, id, Phase.REVERSE_BACKFILL, Phase.REVERSE_VERIFY);
            advanceReverse(cluster, id, Phase.REVERSE_VERIFY, Phase.CUTBACK);
            for (int node = 1; node <= 3; node++)
            {
                Object[][] viaBase = cluster.coordinator(node).execute(SELECT, ConsistencyLevel.ALL);
                assertEquals(viaOverflow.length, viaBase.length);
                for (int i = 0; i < viaBase.length; i++)
                {
                    int last = viaBase[i].length - 1;
                    assertArrayEquals("values and write times through node " + node, java.util.Arrays.copyOf(viaOverflow[i], last),
                                      java.util.Arrays.copyOf(viaBase[i], last));
                    Integer before = (Integer) viaOverflow[i][last];
                    Integer after = (Integer) viaBase[i][last];
                    assertEquals(before == null, after == null);
                    if (before != null)
                        assertTrue("ttl " + before + " then " + after, after <= before && after >= before - 120);
                }
            }
        }
    }
}
