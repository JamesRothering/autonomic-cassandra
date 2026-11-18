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

import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.config.DataRateSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * T6.13, design 4.11 "owner crashes mid-backfill", and the owner lost past the commit point, with the real actuator on
 * three nodes, replication factor 3, the CMS on all three (so it stays available with one node down). Nodes 1 and 2
 * run the layer, node 3 only its data plane. Only the lowest live member may start a cluster action, so node 1
 * proposes and owns each split; node 2 is the lowest live member once node 1 is down. The owner is shut down
 * (instance shutdown, the way a node goes down in these tests) while the key is
 *
 * <ul>
 * <li>in BACKFILL (backfill slowed to a few kB/s): before the commit point the stopping owner takes the abort path
 * (it commits ABORTING); the abort cannot finish while the owner is down, because every abort step obeys O1; node 2
 * takes the expired claim over and finishes the abort once the owner is back;</li>
 * <li>in CLEANUP (during the straggler window): past the commit point the stop releases the claim as
 * COMMITTED_CLEANUP_PENDING, the key stays in CLEANUP, routed to the overflow copy, and is orphaned
 * ({@code layout.orphaned}). {@code nodetool autonomic layout resume} on node 2 is refused while the owner is down (a
 * SCHEMA_LAYOUT claim needs every member alive); the adoption the preferred proposer proposes waits for approval
 * (manual by default), so nothing moves on its own. Once the owner is back, resume on the lowest live member (which
 * returns that pending adoption if there is one) finishes the migration: it continues committed work and is exempt
 * from the cooldown the stopped action left.</li>
 * </ul>
 * Throughout, a workload through the live coordinators is checked against the model, and every acknowledged write and
 * delete is checked again at each step (I5, I7, I10, I14). Lease 15 s; every wait is on a condition. One cluster per test method.
 */
public class SplitOwnerCrashTest extends SplitFailureSupport
{
    private static final int K = 1;
    private static final int ROWS = 300;

    private static void slowBackfill(Cluster cluster, String rate, int chunkRows)
    {
        for (int node = 1; node <= 3; node++)
            if (!cluster.get(node).isShutdown())
                cluster.get(node).runOnInstance(() -> {
                    DatabaseDescriptor.getAutonomicConfig().layout.backfill_throughput = new DataRateSpec.LongBytesPerSecondBound(rate);
                    DatabaseDescriptor.getAutonomicConfig().layout.backfill_chunk_rows = chunkRows;
                });
    }

    private static void stragglerWindow(IInvokableInstance node, String window)
    {
        node.runOnInstance(() -> DatabaseDescriptor.getAutonomicConfig().layout.straggler_window = new DurationSpec.LongMillisecondsBound(window));
    }

    private static boolean layoutGone(IInvokableInstance node, String table)
    {
        return node.callOnInstance(() -> LayoutRegistry.of(ClusterMetadata.current()).layout(Schema.instance.getTableMetadata(KS, table).id) == null);
    }

    private static Cluster cluster() throws java.io.IOException
    {
        Map<String, Object> extra = settings("autonomic.coordination", Map.of("lease", "15s", "lease_safety_margin", "2s"));
        Cluster cluster = start(3, config(nodes(1, 2), settings(), extra));
        // the CMS on every node, so that it stays available while one node is down
        cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
        return cluster;
    }

    @Test
    public void ownerLostInBackfill() throws Throwable
    {
        try (Cluster cluster = cluster())
        {
            createTables(cluster, 3, "bf");
            arm(cluster.get(1), cluster.get(2));
            backfill(cluster);
        }
    }

    /**
     * T6.13 found that resume was refused here with COOLDOWN for a day: the stop ended the action COMMITTED
     * (COMMITTED_CLEANUP_PENDING), which put the irreversible-actuator cooldown on the dedupe key the resume has. Since
     * the T6.9 review a resume of a key past its commit point is a continuation ({@code Actuator.continuation}),
     * exempt from it; a new split of the key is not.
     */
    @Test
    public void ownerLostInCleanup() throws Throwable
    {
        try (Cluster cluster = cluster())
        {
            createTables(cluster, 3, "cl");
            arm(cluster.get(1), cluster.get(2));
            cleanup(cluster);
        }
    }

    private void backfill(Cluster cluster) throws Exception
    {
        IInvokableInstance standby = cluster.get(2);
        IInvokableInstance owner = cluster.get(1);
        String table = "bf";
        Model model = new Model(cluster, table, K, 1, 2, 3);
        model.initialData(ROWS);
        model.startRecordingPhases();
        Workload workload = new Workload(model, ROWS, 3);
        try
        {
            slowBackfill(cluster, "3KiB/s", 20);
            String id = split(owner, table, K, 4);
            awaitPhase(standby, table, K, "BACKFILL");
            workload.awaitAcked(20);
            model.check("bf, BACKFILL");
            assertFalse("the backfill is still running", progress(standby, table, K).endsWith(":3"));

            // ---- the owner goes down mid-backfill ----
            model.setLive(2, 3);
            owner.shutdown().get();
            String afterStop = phaseOf(standby, table, K);
            System.out.println("T6.13 bf: after the owner stopped, node 2 sees " + afterStop + "; status:\n" + layoutStatus(standby));
            // before the commit point: never forward past VERIFY, and with the owner down nothing completes (O1)
            assertStays("bf: the key is held before the commit point while the owner is down", 25, () -> {
                String p = phaseOf(standby, table, K);
                return p.equals("BACKFILL") || p.equals("VERIFY") || p.equals("ABORTING");
            });
            workload.awaitAcked(20);
            model.check("bf, owner down");
            String whileDown = phaseOf(standby, table, K);
            String statusDown = layoutStatus(standby);
            System.out.println("T6.13 bf: while the owner is down, node 2 sees " + whileDown + "; status:\n" + statusDown);

            // ---- the owner comes back: the abort path completes ----
            owner.startup();
            ClusterUtils.waitForCMSToQuiesce(cluster, standby);
            model.setLive(1, 2, 3);
            await("bf: the key removed by the abort path", () -> "absent".equals(phaseOf(standby, table, K)));
            await("bf: the overflow table dropped", () -> layoutGone(standby, table));
            workload.awaitAcked(20);
            model.check("bf, aborted");
            System.out.println("T6.13 bf: history on node 2:\n" + history(standby) + "\nhistory on the owner:\n" + history(owner) + "\ntrail of " + id + " on the owner:\n" + trail(owner, id));
            String trail = trail(owner, id) + history(standby) + history(owner);
            assertFalse("bf: the migration never reached the commit point", trail.contains("phase NEW_ONLY") || trail.contains("phase CUTOVER"));
        }
        finally
        {
            workload.stop();
            model.stopRecordingPhases();
        }
        model.check("bf, end");
        System.out.println("T6.13 bf: phases seen " + model.observedPhases() + "; acknowledged " + model.acked + ", failed " + model.failed
                           + ", unknown outcomes left " + model.uncertainCount() + "; workload: " + workload);
    }

    private void cleanup(Cluster cluster) throws Exception
    {
        IInvokableInstance standby = cluster.get(2);
        IInvokableInstance owner = cluster.get(1);
        String table = "cl";
        Model model = new Model(cluster, table, K, 1, 2, 3);
        model.initialData(ROWS);
        model.startRecordingPhases();
        Workload workload = new Workload(model, ROWS, 4);
        try
        {
            slowBackfill(cluster, "0MiB/s", 1000);
            stragglerWindow(owner, "20s");
            String id = split(owner, table, K, 4);
            awaitPhase(standby, table, K, "CLEANUP");
            String generation = progress(standby, table, K);
            workload.awaitAcked(10);
            model.check("cl, CLEANUP");

            // ---- the owner goes down past the commit point, in the straggler window ----
            model.setLive(2, 3);
            owner.shutdown().get();
            assertEquals("cl: the reconcile pass had not started", generation, progress(standby, table, K));
            await("cl: the key has no owner", () -> layoutStatus(standby).contains("no owner, past the commit point"));
            System.out.println("T6.13 cl: after the owner stopped, status on node 2:\n" + layoutStatus(standby) + "\nhistory on node 2:\n" + history(standby));
            // the key stays in CLEANUP, routed to the overflow copy; its adoption waits for an approval
            assertStays("cl: the key stays in CLEANUP without an owner", 25, () -> "CLEANUP".equals(phaseOf(standby, table, K))
                                                                                && layoutStatus(standby).contains("no owner, past the commit point"));
            workload.awaitAcked(20);
            model.check("cl, CLEANUP, no owner");

            // ---- resume from node 2 while the owner is still down: refused, a SCHEMA_LAYOUT claim needs every member alive ----
            String refused = proposeResume(standby, table, K);
            System.out.println("T6.13 cl: resume on node 2 while the owner is down: " + refused);
            assertTrue(refused, refused.contains("state=REJECTED") && refused.contains("NODE_STATE"));
            assertEquals("CLEANUP", phaseOf(standby, table, K));

            // ---- the owner comes back; still nobody adopts the key; resume ----
            owner.startup();
            ClusterUtils.waitForCMSToQuiesce(cluster, standby);
            model.setLive(1, 2, 3);
            assertStays("cl: the key stays in CLEANUP without an owner after the restart", 25, () -> "CLEANUP".equals(phaseOf(standby, table, K))
                                                                                                && layoutStatus(standby).contains("no owner, past the commit point"));
            // only the lowest live member may start a cluster action: with the owner back, that is node 1 again
            IInvokableInstance resumer = owner;
            String resumed = resume(owner, table, K);
            awaitPhase(standby, table, K, "STEADY");
            await("cl: the resumed action committed", () -> history(resumer).contains("state=COMMITTED"));
            for (int node = 1; node <= 3; node++)
            {
                IInvokableInstance n = cluster.get(node);
                await("cl: the base partition tombstone on node " + node, () -> baseEmpty(n, table, K));
            }
            workload.awaitAcked(20);
            model.check("cl, STEADY");
            String trail = trail(resumer, resumed);
            System.out.println("T6.13 cl: trail of the resume " + resumed + " on node " + resumer.config().num() + ":\n" + trail + "\ntrail of " + id + " on the owner:\n" + trail(owner, id));
            assertTrue(trail, trail.contains("phase STEADY"));
        }
        finally
        {
            workload.stop();
            model.stopRecordingPhases();
        }
        model.check("cl, end");
        System.out.println("T6.13 cl: phases seen " + model.observedPhases() + "; acknowledged " + model.acked + ", failed " + model.failed
                           + ", unknown outcomes left " + model.uncertainCount() + "; workload: " + workload);
    }
}
