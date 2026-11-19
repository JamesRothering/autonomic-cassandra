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

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * T6.17b, ADR-036: a split key's base partition is not deleted at the end of the split. Three nodes, replication
 * factor 2, replicas nodes 1 and 3, owner node 1. After STEADY the base residue is still on the replicas and no read or
 * range scan reaches it (an update and a delete made after the split are what is read, not the residue). With
 * {@code reclaim_window: 60s} the policy then proposes the {@code reclaim} (approved by hand, as the cluster approval
 * is manual), which writes the base partition tombstone at ALL and releases the key {@code TOMBSTONED}.
 */
public class SplitReclaimTest extends SplitFailureSupport
{
    private static final int ROWS = 100;
    private static final int UPDATED = 0;
    private static final int DELETED = 1;
    private static final int NEW_V = 999;

    /** Rows of the key through a range scan (no partition key restriction) at QUORUM, as clustering to v. */
    private static Map<Integer, Integer> scan(Cluster cluster, int node, String table, int key)
    {
        Map<Integer, Integer> rows = new java.util.TreeMap<>();
        for (Object[] row : cluster.coordinator(node).execute("SELECT p, c, v FROM " + KS + '.' + table, ConsistencyLevel.QUORUM))
            if ((Integer) row[0] == key)
                assertNull("a row twice in the scan: " + row[1], rows.put((Integer) row[1], (Integer) row[2]));
        return rows;
    }

    private static String layoutRow(IInvokableInstance node, String column)
    {
        Object[][] rows = node.executeInternal("SELECT " + column + " FROM system_views.autonomic_layouts WHERE keyspace_name = ?", KS);
        return rows.length == 0 || rows[0][0] == null ? null : String.valueOf(rows[0][0]);
    }

    @Test
    public void theBaseResidueIsKeptUnreachableUntilTheReclaimWritesTheTombstone() throws Throwable
    {
        Consumer<IInstanceConfig> config = config(nodes(1), settings("reclaim_window", "60s"), settings());
        try (Cluster cluster = start(3, config))
        {
            IInvokableInstance n1 = cluster.get(1);
            String table = "reclaim";
            createTables(cluster, 2, table);
            int k = keyWithReplicas(cluster, table, true, false, true);
            arm(n1);
            Model model = new Model(cluster, table, k, 1, 2, 3);
            model.initialData(ROWS);

            String id = split(n1, table, k, 4);
            awaitPhase(n1, table, k, "STEADY");
            await("the split to be released", () -> "RESIDUE".equals(layoutRow(n1, "base_state")) && layoutRow(n1, "action_id") == null);
            assertTrue(trail(n1, id), history(n1).contains("COMMITTED"));
            Object frozen = n1.executeInternal("SELECT frozen_since FROM system_views.autonomic_layouts WHERE keyspace_name = ?", KS)[0][0];
            long freeze = frozen instanceof java.util.Date ? ((java.util.Date) frozen).getTime() : 0;
            assertTrue("the freeze is recorded", freeze > 0);

            // the residue is on the replicas of the base partition, and only there; it is not deleted
            for (int node : new int[]{ 1, 3 })
            {
                assertFalse("base residue on node " + node, baseEmpty(cluster.get(node), table, k));
                assertTrue(inBase(cluster.get(node), table, k, UPDATED));
            }
            assertTrue(baseEmpty(cluster.get(2), table, k));
            assertTrue(layoutStatus(n1), layoutStatus(n1).contains("base residue kept"));

            Map<Integer, Integer> expected = scan(cluster, 1, table, k);
            assertTrue(expected.containsKey(DELETED));
            expected.put(UPDATED, NEW_V);
            expected.remove(DELETED);

            // and unreachable: what is read, by key and by scan, is what was written after the split
            cluster.coordinator(2).execute("UPDATE " + KS + '.' + table + " SET v = ? WHERE p = ? AND c = ?", ConsistencyLevel.ALL, NEW_V, k, UPDATED);
            cluster.coordinator(2).execute("DELETE FROM " + KS + '.' + table + " WHERE p = ? AND c = ?", ConsistencyLevel.ALL, k, DELETED);
            for (int node = 1; node <= 3; node++)
            {
                assertEquals("update through node " + node, Integer.valueOf(NEW_V), readV(cluster, node, table, k, UPDATED));
                assertNull("delete through node " + node, readV(cluster, node, table, k, DELETED));
                Map<Integer, Integer> rows = scan(cluster, node, table, k);
                assertEquals("scan through node " + node, expected, rows);
            }
            // the residue itself is untouched: the old value and the deleted row are still in the base table of a replica
            assertTrue(inBase(cluster.get(1), table, k, DELETED));

            // the reclaim: proposed by the preferred proposer once the window has passed, approved by hand
            String proposal = proposedReclaim(n1);
            n1.nodetoolResult("autonomic", "approve", proposal).asserts().success();
            await("the reclaim's tombstone on both replicas", () -> baseEmpty(cluster.get(1), table, k) && baseEmpty(cluster.get(3), table, k));
            await("the key released as TOMBSTONED", () -> "TOMBSTONED".equals(layoutRow(n1, "base_state")) && layoutRow(n1, "action_id") == null);
            assertEquals("STEADY", phaseOf(n1, table, k));
            assertTrue(layoutStatus(n1), layoutStatus(n1).contains("base partition tombstoned"));
            for (int node = 1; node <= 3; node++)
            {
                assertEquals(Integer.valueOf(NEW_V), readV(cluster, node, table, k, UPDATED));
                assertNull(readV(cluster, node, table, k, DELETED));
                assertEquals(expected, scan(cluster, node, table, k));
            }
        }
    }

    /** The id of the proposed {@code reclaim} of the key, once the policy has proposed it. */
    private static String proposedReclaim(IInvokableInstance node) throws InterruptedException
    {
        String[] id = new String[1];
        await("the reclaim to be proposed", () -> {
            List<Map<String, String>> history = node.callOnInstance(() -> AutonomicService.instance.getHistory(50));
            for (Map<String, String> r : history)
                if ("PROPOSED".equals(r.get("state")) && r.get("params").contains("reclaim"))
                {
                    id[0] = r.get("id");
                    return true;
                }
            return false;
        });
        return id[0];
    }
}
