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

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.autonomic.layout.BackfillTask;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * T6.13, design 4.11 "delete racing with backfill", three nodes, replication factor 3, the key moved by hand. Every
 * node is a base replica and an overflow replica. Each node's backfill pass is stopped by the test seam
 * {@link BackfillTask#beforeChunkWriteForTesting} after it has read its (single) chunk and before it writes it; while
 * all three are stopped there, clients delete through C1 coordinators (row deletes through each node, a range delete,
 * a cell delete) and overwrite a row, all with timestamps newer than the rows. The tombstones reach the overflow
 * table first; the passes are then released and write the older rows they read. On every overflow replica, node-locally
 * and through every coordinator after cutover, the deletes win and the overwrite stays.
 */
public class SplitDeleteRaceTest extends SplitFailureSupport
{
    private static final int K = 1;
    private static final int ROWS = 100;
    /** On each node: 0 armed, 1 stopped between read and write, 2 released. */
    static volatile int gate;

    @Test
    public void deleteBetweenBackfillReadAndWriteWins() throws Throwable
    {
        try (Cluster cluster = start(3, handConfig(Collections.emptyMap())))
        {
            String table = "race";
            String physical = physicalByHand(table);
            createTables(cluster, 3, table);
            Model model = new Model(cluster, table, K, 1, 2, 3);
            model.initialData(ROWS);
            model.startRecordingPhases();
            try
            {
                String id = registerByHand(cluster, table, K, 4);
                ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
                advanceByHand(cluster, id, table, K, Phase.ANNOUNCED, Phase.DUAL_WRITE, 1);
                for (int node = 1; node <= 3; node++)
                    cluster.get(node).runOnInstance(() -> {
                        gate = 0;
                        BackfillTask.beforeChunkWriteForTesting = () -> {
                            if (gate != 0)
                                return;
                            gate = 1;
                            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
                            while (gate == 1 && System.nanoTime() < end)
                                sleep(5);
                        };
                    });
                advanceByHand(cluster, id, table, K, Phase.DUAL_WRITE, Phase.BACKFILL, 1);
                for (int node = 1; node <= 3; node++)
                {
                    IInvokableInstance n = cluster.get(node);
                    await("node " + node + " to stop between its backfill read and write", () -> n.callOnInstance(() -> gate == 1));
                }

                // ---- the deletes and an overwrite, newer than the rows the passes hold, through C1 coordinators ----
                assertTrue(model.write(1, 1, null, ConsistencyLevel.QUORUM));
                assertTrue(model.write(2, 2, null, ConsistencyLevel.QUORUM));
                assertTrue(model.write(3, 4, null, ConsistencyLevel.QUORUM));
                model.both(1, ConsistencyLevel.QUORUM, "DELETE FROM %T USING TIMESTAMP " + model.nextTimestamp() + " WHERE p = " + K + " AND c >= 40 AND c < 50");
                model.both(2, ConsistencyLevel.QUORUM, "DELETE v FROM %T USING TIMESTAMP " + model.nextTimestamp() + " WHERE p = " + K + " AND c = 62");
                assertTrue(model.write(3, 70, 777, ConsistencyLevel.QUORUM));
                for (int node = 1; node <= 3; node++)
                    assertEquals("node " + node + " is still stopped", "1:0", progress(cluster.get(node), table, K));
                model.check("race, BACKFILL, passes stopped after their read");

                // ---- the passes write what they read, older than the tombstones ----
                for (int node = 1; node <= 3; node++)
                    cluster.get(node).runOnInstance(() -> gate = 2);
                awaitBackfill(cluster.get(1), table, K, 1, 3);
                for (int node = 1; node <= 3; node++)
                {
                    IInvokableInstance n = cluster.get(node);
                    for (int c : new int[]{ 1, 2, 4, 40, 45, 49 })
                        assertFalse("node " + node + ": the delete of " + c + " won in the overflow table", inOverflow(n, physical, K, c));
                    assertTrue("node " + node + ": a row outside the deletes was copied", inOverflow(n, physical, K, 3) && inOverflow(n, physical, K, 61));
                    Object[] r62 = row(n, physical, 62);
                    Object[] r70 = row(n, physical, 70);
                    assertNull("node " + node + ": the cell delete of 62 won", r62[2]);
                    assertEquals("node " + node + ": the overwrite of 70 won", 777, r70[2]);
                }
                model.check("race, BACKFILL done");

                advanceByHand(cluster, id, table, K, Phase.BACKFILL, Phase.VERIFY, 1);
                advanceByHand(cluster, id, table, K, Phase.VERIFY, Phase.CUTOVER, 1);
                model.check("race, CUTOVER");
                for (int node = 1; node <= 3; node++)
                {
                    assertNull(readV(cluster, node, table, K, 1));
                    assertNull(readV(cluster, node, table, K, 45));
                    assertNull(readV(cluster, node, table, K, 62));
                    assertEquals(Integer.valueOf(777), readV(cluster, node, table, K, 70));
                }
                advanceByHand(cluster, id, table, K, Phase.CUTOVER, Phase.NEW_ONLY, 1);
                advanceByHand(cluster, id, table, K, Phase.NEW_ONLY, Phase.CLEANUP, 1);
                advanceByHand(cluster, id, table, K, Phase.CLEANUP, Phase.CLEANUP, 2);
                awaitBackfill(cluster.get(1), table, K, 2, 3);
                advanceByHand(cluster, id, table, K, Phase.CLEANUP, Phase.STEADY, 2);
                model.check("race, STEADY");
            }
            finally
            {
                model.stopRecordingPhases();
            }
            System.out.println("T6.13 race: phases seen " + model.observedPhases() + "; acknowledged " + model.acked);
        }
    }

    /** {p, c, v} of the overflow row of clustering c on the node, node-locally. */
    private static Object[] row(IInvokableInstance node, String physical, int c)
    {
        for (Object[] r : node.executeInternal("SELECT p, c, v FROM " + KS + '.' + physical))
            if ((Integer) r[0] == K && (Integer) r[1] == c)
                return r;
        throw new AssertionError("no overflow row " + c + " on node " + node.config().num());
    }
}
