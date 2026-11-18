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

import java.util.function.Consumer;

import org.junit.Test;

import org.apache.cassandra.autonomic.layout.KeyState;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.TableLayout;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * T6.13, design 4.6 and 4.11 "late hint of a base-only write", with the real actuator, three nodes, replication factor
 * 2, owner node 1, replicas nodes 1 and 3. In ANNOUNCED, after node 2 has made its hint report of ADR-035 (so the hints
 * below are not ones the owner waits for), node 2 coordinates a write and a delete of the key while its mutations to
 * both replicas are dropped by a message filter: neither is acknowledged (outcome unknown), and node 2 keeps them as
 * hints, whose delivery is paused. The key is in class C0, so they are base-only writes. Node 1, the owner, has a long
 * {@code cas_drain}, which holds ANNOUNCED open for this. The split runs through BACKFILL, whose passes cannot see
 * them, and is held in CUTOVER by {@code commit_approval: manual}; then the hints are delivered: they land in the base
 * table only, after every replica's backfill pass. Approved, the key goes through CLEANUP, whose reconcile pass copies
 * them, so in STEADY the write is read and the delete holds: the stragglers converge to "applied" (4.6).
 *
 * <p>Before ADR-035 the hints were created before the split; such hints now hold the split in ANNOUNCED until they
 * are delivered ({@link SplitPendingHintsTest}).
 */
public class SplitLateHintTest extends SplitFailureSupport
{
    /** True if the node has made its hint report of ADR-035 for the key, as the given node sees the registry. */
    static boolean hintsReported(IInvokableInstance node, String table, int key, int reporter)
    {
        return node.callOnInstance(() -> {
            TableMetadata logical = Schema.instance.getTableMetadata(KS, table);
            TableLayout layout = LayoutRegistry.of(ClusterMetadata.current()).layout(logical.id);
            KeyState state = layout == null ? null : layout.keyState(bytes(key));
            if (state == null)
                return false;
            for (NodeId id : state.hintsClear)
                if (ClusterMetadata.current().directory.endpoint(id).getAddress().getHostAddress().equals("127.0.0." + reporter))
                    return true;
            return false;
        });
    }

    private static final int ROWS = 100;
    private static final int LATE_WRITE = 1001;
    private static final int LATE_DELETE = 99;

    @Test
    public void lateHintIsReconciledBeforeCleanup() throws Throwable
    {
        Consumer<IInstanceConfig> others = config(nodes(1), settings("commit_approval", "manual"), settings());
        Consumer<IInstanceConfig> owner = config(nodes(1), settings("commit_approval", "manual", "cas_drain", "60s"), settings());
        try (Cluster cluster = start(3, c -> (c.num() == 1 ? owner : others).accept(c)))
        {
            IInvokableInstance n1 = cluster.get(1);
            IInvokableInstance n2 = cluster.get(2);
            String table = "hint";
            createTables(cluster, 2, table);
            int k = keyWithReplicas(cluster, table, true, false, true);
            arm(n1);
            Model model = new Model(cluster, table, k, 1, 2, 3);
            model.initialData(ROWS);
            model.startRecordingPhases();
            Workload workload = null;
            try
            {
                String id = split(n1, table, k, 4);
                awaitPhase(n1, table, k, "ANNOUNCED");
                await("node 2's hint report", () -> hintsReported(n1, table, k, 2));

                // ---- in ANNOUNCED, two writes that reach no replica and are kept as hints on node 2 ----
                n2.runOnInstance(() -> HintsService.instance.pauseDispatch());
                long hintsBefore = n2.callOnInstance(() -> StorageMetrics.totalHints.getCount());
                cluster.filters().verbs(Verb.MUTATION_REQ.id).from(2).to(1, 3).drop().on();
                assertFalse("the late write is not acknowledged", model.write(2, LATE_WRITE, 11, ConsistencyLevel.ONE));
                assertFalse("the late delete is not acknowledged", model.write(2, LATE_DELETE, null, ConsistencyLevel.ONE));
                await("hints for both replicas on node 2", () -> n2.callOnInstance(() -> StorageMetrics.totalHints.getCount()) >= hintsBefore + 4);
                cluster.filters().reset();
                assertEquals("still in ANNOUNCED: the hints were made in time for the scenario", "ANNOUNCED", phaseOf(n1, table, k));
                for (int node = 1; node <= 3; node += 2)
                {
                    assertFalse(inBase(cluster.get(node), table, k, LATE_WRITE));
                    assertTrue(inBase(cluster.get(node), table, k, LATE_DELETE));
                }

                workload = new Workload(model, ROWS - 10, 7);
                awaitPhase(n1, table, k, "CUTOVER");
                await("the commit point waits for approval", () -> layoutStatus(n1).contains("waits for `nodetool autonomic approve"));
                workload.awaitAcked(20);
                model.check("hint, CUTOVER, hints pending");

                // ---- the hints arrive after every replica's backfill pass ----
                n2.runOnInstance(() -> HintsService.instance.resumeDispatch());
                for (int node = 1; node <= 3; node += 2)
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
