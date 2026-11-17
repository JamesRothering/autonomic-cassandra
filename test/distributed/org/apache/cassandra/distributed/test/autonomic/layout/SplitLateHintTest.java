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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.Verb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * T6.13, design 4.6 and 4.11 "late hint or batchlog replay of a pre-migration base write", with the real actuator,
 * three nodes, replication factor 2, owner node 1, which is not a replica of the key. Before the split, node 1
 * coordinates a write and a delete of the key while its mutations to both replicas are dropped by a message filter:
 * neither is acknowledged (outcome unknown), and node 1 keeps them as hints, whose delivery is paused. The split runs
 * through BACKFILL, whose passes cannot see them, and is held in CUTOVER by {@code commit_approval: manual}; then the
 * hints are delivered: they land in the base table only, after every replica's backfill pass. Approved, the key goes
 * through CLEANUP, whose reconcile pass copies them, so in STEADY the write is read and the delete holds: the
 * stragglers converge to "applied" (4.6).
 */
public class SplitLateHintTest extends SplitFailureSupport
{
    private static final int ROWS = 100;
    private static final int LATE_WRITE = 1001;
    private static final int LATE_DELETE = 99;

    @Test
    public void lateHintIsReconciledBeforeCleanup() throws Throwable
    {
        try (Cluster cluster = start(3, config(nodes(1), settings("commit_approval", "manual"), settings())))
        {
            IInvokableInstance n1 = cluster.get(1);
            String table = "hint";
            createTables(cluster, 2, table);
            int k = keyWithReplicas(cluster, table, false, true, true);
            arm(n1);
            Model model = new Model(cluster, table, k, 1, 2, 3);
            model.initialData(ROWS);
            model.startRecordingPhases();
            Workload workload = null;
            try
            {
                // ---- two writes that reach no replica and are kept as hints on node 1 ----
                n1.runOnInstance(() -> HintsService.instance.pauseDispatch());
                long hintsBefore = n1.callOnInstance(() -> StorageMetrics.totalHints.getCount());
                cluster.filters().verbs(Verb.MUTATION_REQ.id).from(1).to(2, 3).drop().on();
                assertFalse("the late write is not acknowledged", model.write(1, LATE_WRITE, 11, ConsistencyLevel.ONE));
                assertFalse("the late delete is not acknowledged", model.write(1, LATE_DELETE, null, ConsistencyLevel.ONE));
                await("hints for both replicas on node 1", () -> n1.callOnInstance(() -> StorageMetrics.totalHints.getCount()) >= hintsBefore + 4);
                cluster.filters().reset();
                for (int node = 2; node <= 3; node++)
                {
                    assertFalse(inBase(cluster.get(node), table, k, LATE_WRITE));
                    assertTrue(inBase(cluster.get(node), table, k, LATE_DELETE));
                }

                workload = new Workload(model, ROWS - 10, 7);
                String id = split(n1, table, k, 4);
                awaitPhase(n1, table, k, "CUTOVER");
                await("the commit point waits for approval", () -> layoutStatus(n1).contains("waits for `nodetool autonomic approve"));
                workload.awaitAcked(20);
                model.check("hint, CUTOVER, hints pending");

                // ---- the hints arrive after every replica's backfill pass ----
                n1.runOnInstance(() -> HintsService.instance.resumeDispatch());
                for (int node = 2; node <= 3; node++)
                {
                    IInvokableInstance n = cluster.get(node);
                    await("the hints delivered to node " + node, () -> inBase(n, table, k, LATE_WRITE) && !inBase(n, table, k, LATE_DELETE));
                }
                for (int node = 1; node <= 3; node++)
                {
                    // observed: in CUTOVER the overflow copy is read, which the late hints did not reach
                    assertNull("hint, CUTOVER: the late write is not read yet through node " + node, readV(cluster, node, table, k, LATE_WRITE));
                    assertEquals("hint, CUTOVER: the late delete is not applied yet through node " + node, Integer.valueOf(LATE_DELETE),
                                 readV(cluster, node, table, k, LATE_DELETE));
                }
                model.check("hint, CUTOVER, hints delivered to the base table");

                // ---- the reconcile pass of CLEANUP picks them up ----
                n1.nodetoolResult("autonomic", "approve", id).asserts().success();
                awaitPhase(n1, table, k, "STEADY");
                await("the action committed", () -> history(n1).contains("state=COMMITTED"));
                for (int node = 1; node <= 3; node++)
                {
                    assertEquals("hint, STEADY: the late write is reconciled, read through node " + node, Integer.valueOf(11),
                                 readV(cluster, node, table, k, LATE_WRITE));
                    assertNull("hint, STEADY: the late delete is reconciled, read through node " + node, readV(cluster, node, table, k, LATE_DELETE));
                }
                workload.awaitAcked(20);
                model.check("hint, STEADY");
                String trail = trail(n1, id);
                System.out.println("T6.13 hint: trail\n" + trail);
            }
            finally
            {
                if (workload != null)
                    workload.stop();
                model.stopRecordingPhases();
            }
            model.check("hint, end");
            System.out.println("T6.13 hint: phases seen " + model.observedPhases() + "; acknowledged " + model.acked + ", failed " + model.failed
                               + ", unknown outcomes left " + model.uncertainCount());
        }
    }
}
