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
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator;
import org.apache.cassandra.autonomic.coord.TcmEnvironment;
import org.apache.cassandra.autonomic.execute.actuators.PartitionSplitActuator;
import org.apache.cassandra.autonomic.execute.actuators.PartitionUnsplitActuator;
import org.apache.cassandra.autonomic.layout.Eligibility;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.ReleaseKey;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.Clock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * T6.17k: slow-mode unsplits with the real actuators on three nodes, replication factor 2, owner node 1, base
 * replicas nodes 1 and 3. The tables have {@code gc_grace_seconds = 120} from the start (the eligibility minimum is
 * lowered for the test): long enough for the forward and the reverse windows of 11.5 with the short windows
 * configured here, short enough for the replicas' purge. One cluster, three keys in turn:
 *
 * <ul>
 * <li>a key split under the old forward cleanup (driven by hand as T6.9 built it: base partition tombstone written
 * after FinishKey, {@code ReleaseKey} in its format 1 form, so the key is {@code TOMBSTONED} with an unknown time),
 * unsplit in slow mode;</li>
 * <li>a key split by the real actuator, its residue reclaimed by the {@code reclaim} continuation ({@code TOMBSTONED(Tt)}),
 * written and deleted while split, then unsplit in slow mode once {@code Tt + gc_grace} has passed: R1 to R4 against the
 * model;</li>
 * <li>the same with a base cell newer than {@code Tt}: the purge leaves it, the replicas publish
 * {@code RESIDUE_ABOVE_TT} (gossip {@code X1}), and on {@code purge_timeout} the owner aborts to {@code STEADY} with
 * {@code layout.unsplit_residue} naming each base replica's reason and the largest timestamp.</li>
 * </ul>
 */
public class UnsplitSlowTest extends UnsplitSupport
{
    private static final int GC_GRACE = 120;
    private static final int ROWS = 150;
    private static final int WORKLOAD_RANGE = 100;
    private static final int DELETED_WHILE_SPLIT = 120;
    private static final int UPDATED_WHILE_SPLIT = 121;

    private static ClusterCoordinator coordinator()
    {
        return new ClusterCoordinator(new TcmEnvironment(Clock.Global::currentTimeMillis, DatabaseDescriptor::getAutonomicConfig));
    }

    @Test
    public void slowUnsplits() throws Throwable
    {
        try (Cluster cluster = start(3, unsplitConfig(nodes(1), settings("reconcile_budget", "5s", "reclaim_window", "10s", "baseline_window", "1s",
                                                                         "purge_timeout", "30s"), settings())))
        {
            cluster.forEach(node -> node.runOnInstance(() -> Eligibility.setMinGcGraceSecondsForTesting(1)));
            createTables(cluster, 2, "legacy", "slow", "residue");
            for (String table : new String[]{ "legacy", "slow", "residue" })
                cluster.schemaChange("ALTER TABLE " + KS + '.' + table + " WITH gc_grace_seconds = " + GC_GRACE);
            arm(cluster.get(1));
            legacy(cluster);
            slow(cluster);
            residue(cluster);
        }
    }

    /** Proposes the unsplit as soon as validate allows it (slow mode: from Tt + gc_grace), and approves it. */
    private static String unsplitWhenAllowed(IInvokableInstance n1, String table, int k) throws InterruptedException
    {
        String[] out = new String[1];
        await("the unsplit of " + table + " to be allowed", () -> {
            NodeToolResult r = n1.nodetoolResult("autonomic", "layout", "unsplit", KS, table, hex(k));
            out[0] = r.getStdout() + r.getStderr();
            if (!out[0].contains("state=PROPOSED"))
            {
                sleep(1000);
                return false;
            }
            return true;
        });
        assertTrue(out[0], out[0].contains(PartitionUnsplitActuator.NAME));
        return approveProposal(n1, out[0]);
    }

    private static String unsplitHistory(IInvokableInstance node, String id)
    {
        for (String line : history(node).split("\n"))
            if (line.contains(id))
                return line;
        return "";
    }

    /** While split: a delete and an update (with a TTL) of rows written before the split, through the model. */
    private static void writeWhileSplit(Model model)
    {
        int k = model.key;
        model.both(2, ConsistencyLevel.ALL, "DELETE FROM %T USING TIMESTAMP " + model.nextTimestamp() + " WHERE p = " + k + " AND c = " + DELETED_WHILE_SPLIT);
        model.both(3, ConsistencyLevel.ALL, "UPDATE %T USING TIMESTAMP " + model.nextTimestamp() + " AND TTL 60000 SET v = 777 WHERE p = " + k
                                             + " AND c = " + UPDATED_WHILE_SPLIT);
    }

    private static void assertUnsplit(Cluster cluster, Model model, String label)
    {
        assertWritetimeAndTtl(model, label);
        assertRow(cluster, model.table, model.key, DELETED_WHILE_SPLIT, null);
        assertRow(cluster, model.table, model.key, UPDATED_WHILE_SPLIT, 777);
    }

    // ---------------------------------------------------------------------------------------------------------
    // a key of the old forward cleanup
    // ---------------------------------------------------------------------------------------------------------

    private void legacy(Cluster cluster) throws Exception
    {
        IInvokableInstance n1 = cluster.get(1);
        String table = "legacy";
        int k = keyWithReplicas(cluster, table, true, false, true);
        Model model = new Model(cluster, table, k, 1, 2, 3);
        model.poisonProbe = false;
        model.initialData(ROWS);
        model.startRecordingPhases();
        try
        {
            // ---- the forward migration as T6.9 built it, by hand; the replicas' passes are the real ones ----
            String forward = registerByHand(cluster, table, k, 4);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            advanceByHand(cluster, forward, table, k, Phase.ANNOUNCED, Phase.DUAL_WRITE, 1);
            advanceByHand(cluster, forward, table, k, Phase.DUAL_WRITE, Phase.BACKFILL, 1);
            awaitBackfill(n1, table, k, 1, 2);
            advanceByHand(cluster, forward, table, k, Phase.BACKFILL, Phase.VERIFY, 1);
            advanceByHand(cluster, forward, table, k, Phase.VERIFY, Phase.CUTOVER, 1);
            advanceByHand(cluster, forward, table, k, Phase.CUTOVER, Phase.NEW_ONLY, 1);
            advanceByHand(cluster, forward, table, k, Phase.NEW_ONLY, Phase.CLEANUP, 1);
            advanceByHand(cluster, forward, table, k, Phase.CLEANUP, Phase.CLEANUP, 2);
            awaitBackfill(n1, table, k, 2, 2);
            advanceByHand(cluster, forward, table, k, Phase.CLEANUP, Phase.STEADY, 2);
            // the old cleanup: the base partition tombstone at ALL right after FinishKey, then ReleaseKey in its first form
            long tt = System.currentTimeMillis() * 1000;
            for (int node : new int[]{ 1, 3 })
                cluster.get(node).executeInternal("DELETE FROM " + KS + '.' + table + " USING TIMESTAMP ? WHERE p = ?", tt, k);
            n1.runOnInstance(() -> {
                ClusterCoordinator c = coordinator();
                Claim now = c.ownClaim(UUID.fromString(forward));
                TableId logical = Schema.instance.getTableMetadata(KS, table).id;
                ReleaseKey release = new ReleaseKey(now.actionId, now.owner, now.step, logical, bytes(k), Clock.Global.currentTimeMillis(), 600_000);
                assertTrue(c.commitInstead(now, release.wrap(), ReleaseKey.CLAIM_PHASE, now.state).ok());
            });
            releaseByHand(cluster, forward, "COMMITTED");
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            assertEquals("still format 1", LayoutRegistry.FORMAT_VERSION, (int) n1.callOnInstance(() -> LayoutRegistry.of(ClusterMetadata.current()).formatVersion));
            awaitReleased(n1, table, "TOMBSTONED");
            assertTrue(baseEmpty(cluster.get(1), table, k));
            writeWhileSplit(model);
            model.check("legacy, split");

            // ---- the unsplit: slow mode at once (the tombstone's time is unknown); each replica waits its own gc_grace ----
            sleep(2000); // the cooldown of the hand action
            String id = unsplitWhenAllowed(n1, table, k);
            awaitPhase(n1, table, k, "REVERSE_ANNOUNCED");
            assertEquals("SLOW", layoutRow(n1, table, "reverse_mode"));
            awaitPhase(n1, table, k, "absent");
            await("the unsplit committed", () -> unsplitHistory(n1, id).contains("state=COMMITTED"));
            model.check("legacy, unsplit");
            assertUnsplit(cluster, model, "legacy, unsplit");
            System.out.println("T6.17k legacy: trail\n" + trail(n1, id));
        }
        finally
        {
            model.stopRecordingPhases();
        }
        System.out.println("T6.17k legacy: phases seen " + model.observedPhases());
    }

    // ---------------------------------------------------------------------------------------------------------
    // split, reclaim, unsplit slowly
    // ---------------------------------------------------------------------------------------------------------

    /** Splits the key with the real actuator and has its residue reclaimed; returns when it is TOMBSTONED. */
    private static void splitAndReclaim(IInvokableInstance n1, String table, int k) throws InterruptedException
    {
        split(n1, table, k, 4);
        awaitReleased(n1, table, "RESIDUE");
        String reclaim = proposed(n1, PartitionSplitActuator.NAME, "reclaim");
        n1.nodetoolResult("autonomic", "approve", reclaim).asserts().success();
        awaitReleased(n1, table, "TOMBSTONED");
    }

    private void slow(Cluster cluster) throws Exception
    {
        IInvokableInstance n1 = cluster.get(1);
        String table = "slow";
        int k = keyWithReplicas(cluster, table, true, false, true);
        Model model = new Model(cluster, table, k, 1, 2, 3);
        model.poisonProbe = false;
        model.initialData(ROWS);
        model.startRecordingPhases();
        Workload workload = null;
        try
        {
            splitAndReclaim(n1, table, k);
            assertTrue("the reclaim's tombstone", baseEmpty(cluster.get(1), table, k) && baseEmpty(cluster.get(3), table, k));
            writeWhileSplit(model);
            workload = new Workload(model, WORKLOAD_RANGE, 21);
            workload.awaitAcked(20);
            model.check("slow, split and reclaimed");

            String id = unsplitWhenAllowed(n1, table, k);
            awaitPhase(n1, table, k, "REVERSE_ANNOUNCED");
            assertEquals("SLOW", layoutRow(n1, table, "reverse_mode"));
            workload.awaitAcked(10);
            checkWhileMoving(model, "slow, REVERSE_ANNOUNCED");
            awaitPhase(n1, table, k, "absent");
            await("the unsplit committed", () -> unsplitHistory(n1, id).contains("state=COMMITTED"));
            workload.awaitAcked(20);
            model.check("slow, unsplit");
            workload.pause();
            assertUnsplit(cluster, model, "slow, unsplit");
            workload.resume();
            System.out.println("T6.17k slow: trail\n" + trail(n1, id));
        }
        finally
        {
            if (workload != null)
                workload.stop();
            model.stopRecordingPhases();
        }
        model.check("slow, end");
        System.out.println("T6.17k slow: phases seen " + model.observedPhases() + "; workload: " + workload);
    }

    private void residue(Cluster cluster) throws Exception
    {
        IInvokableInstance n1 = cluster.get(1);
        String table = "residue";
        int k = keyWithReplicas(cluster, table, true, false, true);
        Model model = new Model(cluster, table, k, 1, 2, 3);
        model.poisonProbe = false;
        model.initialData(ROWS);
        model.startRecordingPhases();
        try
        {
            splitAndReclaim(n1, table, k);
            // a base cell with a client timestamp in the future: above Tt, never purged (11.9 hazard 2)
            long future = 4_102_444_800_000_000L;
            for (int node : new int[]{ 1, 3 })
                cluster.get(node).executeInternal("INSERT INTO " + KS + '.' + table + " (p, c, v) VALUES (?, ?, ?) USING TIMESTAMP ?", k, 9999, 9, future);
            model.check("residue, split");

            String id = unsplitWhenAllowed(n1, table, k);
            await("the unsplit rolled back", () -> unsplitHistory(n1, id).contains("state=ROLLED_BACK"));
            awaitReleased(n1, table, "TOMBSTONED");
            String trail = trail(n1, id);
            System.out.println("T6.17k residue: trail\n" + trail);
            assertTrue(trail, trail.contains(PartitionUnsplitActuator.UNSPLIT_RESIDUE));
            List<String> raised = diagnoses(n1, PartitionUnsplitActuator.UNSPLIT_RESIDUE);
            await("the diagnosis", () -> !diagnoses(n1, PartitionUnsplitActuator.UNSPLIT_RESIDUE).isEmpty());
            raised = diagnoses(n1, PartitionUnsplitActuator.UNSPLIT_RESIDUE);
            System.out.println("T6.17k residue: diagnosis " + raised);
            String d = raised.get(0);
            // both base replicas' reasons: node 1's own, node 3's through gossip
            assertEquals(d, 2, d.split("RESIDUE_ABOVE_TT", -1).length - 1);
            assertTrue(d, d.contains("largest_residue_timestamp=" + future));
            assertTrue(d, d.contains("operator decision"));
            // the key is split as before
            assertEquals("STEADY", phaseOf(cluster.get(3), table, k));
            model.check("residue, aborted to STEADY");
        }
        finally
        {
            model.stopRecordingPhases();
        }
        System.out.println("T6.17k residue: phases seen " + model.observedPhases());
    }
}
