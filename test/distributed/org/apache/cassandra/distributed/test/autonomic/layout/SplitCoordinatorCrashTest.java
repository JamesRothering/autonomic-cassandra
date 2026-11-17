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

import org.junit.Test;

import org.apache.cassandra.autonomic.layout.LayoutWrites;
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
 * T6.13, design 4.11 "coordinator crashes between the two writes", both orders, on three nodes with replication
 * factor 3. The key is moved by hand (as {@link LayoutWritesTest}) so that it can be held in class C1 and in class
 * C2. Node 3 coordinates a write and is stopped after its first copy was acknowledged: the test seam
 * {@link LayoutWrites#betweenCopiesForTesting} throws there, so the second copy is never sent and the client gets an
 * error, and the node is then shut down. The write's outcome is unknown to the model.
 *
 * <ul>
 * <li>Forward (table {@code fwd}): stopped in DUAL_WRITE (C1: overflow written, base not), then in CUTOVER (C2: base
 * written, overflow not), then the key goes on to STEADY through a reconcile pass.</li>
 * <li>Rollback (table {@code back}): stopped in BACKFILL (C1), then in CUTOVER (C2), then ROLLBACK_DUAL, ABORTING and
 * RemoveKey.</li>
 * </ul>
 * Asserted at each step: the invariants through the model, where each copy landed node-locally, and what 4.11 says a
 * read shows. One cluster start; node 3 is restarted four times.
 */
public class SplitCoordinatorCrashTest extends SplitFailureSupport
{
    private static final int K = 1;
    private static final int ROWS = 200;
    /** On each node: the seam fired. */
    static volatile boolean fired;

    private static void stopBetweenCopies(IInvokableInstance node)
    {
        node.runOnInstance(() -> {
            fired = false;
            LayoutWrites.betweenCopiesForTesting = () -> {
                fired = true;
                throw new RuntimeException("T6.13: the coordinator stops between the two copies");
            };
        });
    }

    /** Node 3 writes clustering c, is stopped between the copies, and is shut down. Returns the write's timestamp. */
    private static long writeAndKill(Cluster cluster, Model model, Workload workload, int c, int v) throws Exception
    {
        model.setLive(1, 2);
        workload.pause();
        stopBetweenCopies(cluster.get(3));
        long ts = model.begin(c, v);
        boolean acknowledged;
        try
        {
            cluster.coordinator(3).execute(model.statement(c, v, ts), ConsistencyLevel.QUORUM);
            acknowledged = true;
        }
        catch (Throwable t)
        {
            acknowledged = false;
        }
        workload.resume();
        assertFalse("the write must not be acknowledged", acknowledged);
        assertTrue("the seam fired", cluster.get(3).callOnInstance(() -> fired));
        cluster.get(3).shutdown().get();
        return ts;
    }

    private static void restart(Cluster cluster, Model model)
    {
        cluster.get(3).startup();
        ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
        model.setLive(1, 2, 3);
    }

    @Test
    public void coordinatorKilledBetweenTheTwoWrites() throws Throwable
    {
        try (Cluster cluster = start(3, handConfig(Collections.emptyMap())))
        {
            createTables(cluster, 3, "fwd", "back");
            forward(cluster);
            rollback(cluster);
        }
    }

    private void forward(Cluster cluster) throws Exception
    {
        String table = "fwd";
        String physical = physicalByHand(table);
        Model model = new Model(cluster, table, K, 1, 2, 3);
        model.initialData(ROWS);
        model.startRecordingPhases();
        Workload workload = new Workload(model, ROWS, 1);
        try
        {
            String id = registerByHand(cluster, table, K, 4);
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
            advanceByHand(cluster, id, table, K, Phase.ANNOUNCED, Phase.DUAL_WRITE, 1);
            workload.awaitAcked(20);
            model.check("fwd, DUAL_WRITE");

            // ---- C1: the overflow copy is written, the base copy is not ----
            writeAndKill(cluster, model, workload, 1001, 111);
            for (int node = 1; node <= 2; node++)
                assertFalse("C1 stop: no base copy on node " + node, inBase(cluster.get(node), table, K, 1001));
            assertTrue("C1 stop: the overflow copy is on node 1 or 2",
                       inOverflow(cluster.get(1), physical, K, 1001) || inOverflow(cluster.get(2), physical, K, 1001));
            // 4.11: reads (base, in C1) do not see the write
            for (int node = 1; node <= 2; node++)
                assertNull("C1 stop: a read in C1 through node " + node + " sees the write", readV(cluster, node, table, K, 1001));
            workload.awaitAcked(10);
            model.check("fwd, DUAL_WRITE, node 3 stopped between the copies");
            restart(cluster, model);

            advanceByHand(cluster, id, table, K, Phase.DUAL_WRITE, Phase.BACKFILL, 1);
            awaitBackfill(cluster.get(1), table, K, 1, 3);
            advanceByHand(cluster, id, table, K, Phase.BACKFILL, Phase.VERIFY, 1);
            advanceByHand(cluster, id, table, K, Phase.VERIFY, Phase.CUTOVER, 1);
            workload.awaitAcked(10);
            model.check("fwd, CUTOVER");
            // 4.11: the write becomes visible at cutover (the overflow copy is read from C2 on)
            for (int node = 1; node <= 3; node++)
                assertEquals("C1 stop: visible at cutover through node " + node, Integer.valueOf(111), readV(cluster, node, table, K, 1001));

            // ---- C2: the base copy is written, the overflow copy is not ----
            writeAndKill(cluster, model, workload, 1002, 222);
            assertTrue("C2 stop: the base copy is on node 1 or 2", inBase(cluster.get(1), table, K, 1002) || inBase(cluster.get(2), table, K, 1002));
            for (int node = 1; node <= 2; node++)
                assertFalse("C2 stop: no overflow copy on node " + node, inOverflow(cluster.get(node), physical, K, 1002));
            // 4.11: invisible (C2 reads the overflow copy)
            for (int node = 1; node <= 2; node++)
                assertNull("C2 stop: a read in C2 through node " + node + " sees the write", readV(cluster, node, table, K, 1002));
            workload.awaitAcked(10);
            model.check("fwd, CUTOVER, node 3 stopped between the copies");
            restart(cluster, model);

            advanceByHand(cluster, id, table, K, Phase.CUTOVER, Phase.NEW_ONLY, 1);
            advanceByHand(cluster, id, table, K, Phase.NEW_ONLY, Phase.CLEANUP, 1);
            workload.awaitAcked(10);
            model.check("fwd, CLEANUP before the reconcile pass");
            for (int node = 1; node <= 3; node++)
                assertNull("C2 stop: invisible before the reconcile pass, node " + node, readV(cluster, node, table, K, 1002));
            // the reconcile generation (design 4.6): the base-only copy is a straggler, copied like any other
            advanceByHand(cluster, id, table, K, Phase.CLEANUP, Phase.CLEANUP, 2);
            awaitBackfill(cluster.get(1), table, K, 2, 3);
            workload.awaitAcked(10);
            model.check("fwd, CLEANUP after the reconcile pass");
            for (int node = 1; node <= 3; node++)
                assertEquals("C2 stop: the reconcile pass copies the base-only write; read through node " + node,
                             Integer.valueOf(222), readV(cluster, node, table, K, 1002));
            advanceByHand(cluster, id, table, K, Phase.CLEANUP, Phase.STEADY, 2);
            workload.awaitAcked(20);
            model.check("fwd, STEADY");
            assertEquals(Integer.valueOf(111), readV(cluster, 2, table, K, 1001));
            releaseByHand(cluster, id, "COMMITTED");
        }
        finally
        {
            workload.stop();
            model.stopRecordingPhases();
        }
        model.check("fwd, end");
        System.out.println("T6.13 fwd: phases seen " + model.observedPhases() + "; acknowledged " + model.acked + ", failed " + model.failed
                           + ", unknown outcomes left " + model.uncertainCount() + "; workload: " + workload);
    }

    private void rollback(Cluster cluster) throws Exception
    {
        String table = "back";
        String physical = physicalByHand(table);
        Model model = new Model(cluster, table, K, 1, 2, 3);
        model.initialData(ROWS);
        model.startRecordingPhases();
        Workload workload = new Workload(model, ROWS, 2);
        try
        {
            String id = registerByHand(cluster, table, K, 4);
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
            advanceByHand(cluster, id, table, K, Phase.ANNOUNCED, Phase.DUAL_WRITE, 1);
            advanceByHand(cluster, id, table, K, Phase.DUAL_WRITE, Phase.BACKFILL, 1);
            awaitBackfill(cluster.get(1), table, K, 1, 3);
            workload.awaitAcked(20);
            model.check("back, BACKFILL");

            // ---- C1 in BACKFILL, after the backfill pass ----
            writeAndKill(cluster, model, workload, 1001, 111);
            assertTrue(inOverflow(cluster.get(1), physical, K, 1001) || inOverflow(cluster.get(2), physical, K, 1001));
            for (int node = 1; node <= 2; node++)
            {
                assertFalse(inBase(cluster.get(node), table, K, 1001));
                assertNull(readV(cluster, node, table, K, 1001));
            }
            model.check("back, BACKFILL, node 3 stopped between the copies");
            restart(cluster, model);
            advanceByHand(cluster, id, table, K, Phase.BACKFILL, Phase.VERIFY, 1);
            advanceByHand(cluster, id, table, K, Phase.VERIFY, Phase.CUTOVER, 1);
            model.check("back, CUTOVER");
            // observed: from CUTOVER on, the overflow-only write is read (ADR-030 tolerates it)
            assertEquals(Integer.valueOf(111), readV(cluster, 1, table, K, 1001));

            // ---- C2 in CUTOVER ----
            writeAndKill(cluster, model, workload, 1002, 222);
            assertTrue(inBase(cluster.get(1), table, K, 1002) || inBase(cluster.get(2), table, K, 1002));
            for (int node = 1; node <= 2; node++)
            {
                assertFalse(inOverflow(cluster.get(node), physical, K, 1002));
                assertNull(readV(cluster, node, table, K, 1002));
            }
            model.check("back, CUTOVER, node 3 stopped between the copies");
            restart(cluster, model);

            // ---- rollback: ROLLBACK_DUAL (C1), ABORTING (C0), removed ----
            advanceByHand(cluster, id, table, K, Phase.CUTOVER, Phase.ROLLBACK_DUAL, 1);
            workload.awaitAcked(10);
            model.check("back, ROLLBACK_DUAL");
            advanceByHand(cluster, id, table, K, Phase.ROLLBACK_DUAL, Phase.ABORTING, 1);
            workload.awaitAcked(10);
            model.check("back, ABORTING");
            advanceByHand(cluster, id, table, K, Phase.ABORTING, null, 1);
            workload.awaitAcked(20);
            model.check("back, removed");
            for (int node = 1; node <= 3; node++)
            {
                // 4.11: the C1 write is discarded by the abort, the C2 write is visible after the rollback
                assertNull("C1 stop: discarded after the abort, node " + node, readV(cluster, node, table, K, 1001));
                assertEquals("C2 stop: visible after the rollback, node " + node, Integer.valueOf(222), readV(cluster, node, table, K, 1002));
            }
            releaseByHand(cluster, id, "ROLLED_BACK");
        }
        finally
        {
            workload.stop();
            model.stopRecordingPhases();
        }
        model.check("back, end");
        System.out.println("T6.13 back: phases seen " + model.observedPhases() + "; acknowledged " + model.acked + ", failed " + model.failed
                           + ", unknown outcomes left " + model.uncertainCount() + "; workload: " + workload);
    }
}
