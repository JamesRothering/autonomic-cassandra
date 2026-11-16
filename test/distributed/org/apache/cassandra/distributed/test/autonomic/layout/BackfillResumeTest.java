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

import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

import org.apache.cassandra.autonomic.layout.BackfillTask;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.PhysicalTables;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.autonomic.layout.BackfillTest.awaitBackfill;
import static org.apache.cassandra.distributed.test.autonomic.layout.BackfillTest.held;
import static org.apache.cassandra.distributed.test.autonomic.layout.BackfillTest.progress;
import static org.apache.cassandra.distributed.test.autonomic.layout.BackfillTest.stat;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.K;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.LAYOUT_ID;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.PHYSICAL;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.advance;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.register;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * T6.7 on three nodes with replication factor 3: a replica that restarts in the middle of its backfill, at a point
 * inside a range tombstone, continues from its record; and while it is down, the other two, whose writes at ALL need
 * it as a replica of the overflow table, stall and then finish.
 *
 * <p>The partition is rows 0..9, a range tombstone over [10, 110) with a newer row at every even clustering inside it,
 * and rows 110..119. The backfill is slowed to about one chunk of four rows per second so that the test can stop a
 * node while it is inside the tombstone.
 */
public class BackfillResumeTest extends TestBaseImpl
{
    /** The clustering of the resume record of the node, or -1 if it has none or the record has no clustering. */
    private static int recorded(IInvokableInstance node)
    {
        for (Object[] row : node.executeInternal("SELECT key, value FROM system_autonomic.local_state"))
        {
            if (!((String) row[0]).startsWith("layout/"))
                continue;
            return clustering((String) row[1]);
        }
        return -1;
    }

    // the record "owner EXCL_START_BOUND x0000002a" is 42
    private static int clustering(String record)
    {
        String[] fields = record.split(" ");
        return fields.length == 3 && fields[2].startsWith("x") ? Integer.parseUnsignedInt(fields[2].substring(1), 16) : -1;
    }

    private static Set<Integer> overflowClusterings(IInvokableInstance node)
    {
        Set<Integer> cs = new TreeSet<>();
        for (Object[] row : node.executeInternal("SELECT p, c FROM " + KEYSPACE + '.' + PHYSICAL))
            if ((Integer) row[0] == K)
                cs.add((Integer) row[1]);
        return cs;
    }

    @Test
    public void restartInsideARangeTombstoneResumesAndTheOthersStallMeanwhile() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                             .set("autonomic.enabled", c.num() == 1)
                                                             .set("autonomic.layout.backfill_chunk_rows", 4)
                                                             .set("autonomic.layout.backfill_throughput", "1100B/s"))
                                           .start()))
        {
            IInvokableInstance n1 = cluster.get(1);
            IInvokableInstance n2 = cluster.get(2);
            IInvokableInstance n3 = cluster.get(3);
            cluster.schemaChange(BackfillTest.TABLE);
            String derived = n1.callOnInstance(() -> PhysicalTables.derive(Schema.instance.getTableMetadata(KEYSPACE, "t"), LAYOUT_ID, TableId.generate()));
            cluster.schemaChange(derived);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);

            String s = "s".repeat(250);
            String insert = "INSERT INTO " + KEYSPACE + ".t (p, c, v, s) VALUES (" + K + ", ?, ?, ?) USING TIMESTAMP ?";
            for (int c = 0; c < 120; c++)
                cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, c, c, s, 1000L + c);
            cluster.coordinator(1).execute("DELETE FROM " + KEYSPACE + ".t USING TIMESTAMP 2001 WHERE p = " + K + " AND c >= 10 AND c < 110", ConsistencyLevel.ALL);
            n2.flush(KEYSPACE);
            for (int c = 10; c < 110; c += 2)
                cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, c, c, s, 3000L);
            n3.flush(KEYSPACE);
            String base = held(n1, false, 120);
            assertEquals(base, held(n2, false, 120));
            assertEquals(base, held(n3, false, 120));
            assertTrue(base.contains("range tombstone over 10:") && base.contains("range tombstone over 109:"));
            assertFalse(base.contains("range tombstone over 9:") || base.contains("range tombstone over 110:"));

            String id = register(n1, 1);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            advance(cluster, id, Phase.ANNOUNCED, Phase.DUAL_WRITE);
            advance(cluster, id, Phase.DUAL_WRITE, Phase.BACKFILL);

            // ---- stop node 2 once its record is inside the tombstone ----
            long deadline = System.nanoTime() + 120_000_000_000L;
            int before;
            while ((before = recorded(n2)) < 20)
            {
                assertTrue("node 2 does not get into the tombstone", System.nanoTime() < deadline);
                Thread.sleep(100);
            }
            assertTrue("the backfill is still inside the tombstone: " + before, before < 60);
            ClusterUtils.stopUnchecked(n2);
            assertEquals("nobody is done", "1:0", progress(n1));

            // ---- an overflow replica is down: the writes of nodes 1 and 3 at ALL fail and are repeated ----
            for (IInvokableInstance node : new IInvokableInstance[]{ n1, n3 })
            {
                long failures = stat(node, "failures");
                deadline = System.nanoTime() + 120_000_000_000L;
                while (stat(node, "failures") < failures + 2)
                {
                    assertTrue("the backfill of a node does not notice that a replica is down", System.nanoTime() < deadline);
                    Thread.sleep(200);
                }
            }
            long stalledAt = stat(n1, "chunks");
            Thread.sleep(3000);
            assertEquals("no chunk is acknowledged while a replica is down", stalledAt, stat(n1, "chunks"));
            assertEquals("1:0", progress(n1));
            assertTrue("node 1 keeps its place", recorded(n1) > 0);

            // ---- node 2 comes back: it resumes inside the tombstone, the others recover ----
            n2.startup();
            awaitBackfill(cluster, 1, 3, 300);
            assertEquals("node 2 resumed once, after its restart", 1, stat(n2, "resumed"));
            String record = n2.callOnInstance(() -> BackfillTask.stats.lastResumeRecord);
            assertNotNull(record);
            int resumedAt = clustering(record);
            assertTrue("resumed at " + resumedAt + ", recorded " + before + " before the stop", resumedAt >= before && resumedAt < 110);
            // 70 rows and two markers in chunks of 4 are 18 chunks or more; node 2 did not start over after the restart
            assertTrue("chunks after the restart: " + stat(n2, "chunks"), stat(n2, "chunks") <= 18 - before / 8);
            assertTrue(stat(n1, "resumed") >= 1 && stat(n3, "resumed") >= 1);

            // ---- the copy: cell for cell, and the tombstone over exactly [10, 110) on every node ----
            for (int node = 1; node <= 3; node++)
                assertEquals("overflow table of node " + node, base, held(cluster.get(node), true, 120));

            // late writes older than the tombstone: dead inside it, alive right outside it
            for (int c : new int[]{ 9, 11, 55, before | 1, resumedAt | 1, 109, 110 })
                cluster.coordinator(1 + c % 3).execute("UPDATE " + KEYSPACE + ".t USING TIMESTAMP 1500 SET v = 777 WHERE p = " + K + " AND c = ?",
                                                       ConsistencyLevel.ALL, c);
            for (int node = 1; node <= 3; node++)
            {
                IInvokableInstance n = cluster.get(node);
                Set<Integer> cs = overflowClusterings(n);
                for (int c = 0; c < 120; c++)
                    assertEquals("node " + node + ", clustering " + c, c < 10 || c >= 110 || c % 2 == 0, cs.contains(c));
                for (Object[] row : n.executeInternal("SELECT p, c, v FROM " + KEYSPACE + '.' + PHYSICAL))
                    if ((Integer) row[0] == K)
                        assertEquals("clustering " + row[1], (int) row[1] == 9 || (int) row[1] == 110 ? 777 : (int) row[1], row[2]);
            }
        }
    }
}
