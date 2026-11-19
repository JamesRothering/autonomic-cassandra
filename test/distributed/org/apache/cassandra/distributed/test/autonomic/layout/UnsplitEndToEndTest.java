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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.autonomic.layout.EpochWatermarks;
import org.apache.cassandra.autonomic.layout.LayoutWrites;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.Epoch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * T6.17k: the reverse migration end to end, driven by the real actuators through {@code nodetool autonomic layout
 * split} and {@code unsplit} and their approvals, on three nodes with replication factor 2, owner node 1.
 *
 * <ul>
 * <li>{@link #fastUnsplitKeepsEveryWriteAndAFailedVerificationAbortsToSteady}: a key split, written and deleted while
 * split, then unsplit in fast mode under a background workload: R1 to R4 against the model with {@code writetime} and
 * {@code ttl}; the overflow table is dropped with the last key. Then a second table whose base residue holds a delete
 * newer than the overflow copy: the reverse verification fails, the re-backfill cannot repair it, and the unsplit aborts
 * from {@code REVERSE_VERIFY} back to {@code STEADY}, released, with the data read from the overflow copy as before.</li>
 * <li>{@link #aCoordinatorOneClassBehindAndALateOverflowHint}: with {@code commit_approval: manual}, a coordinator is
 * held at {@code CUTBACK} while the owner commits {@code BASE_ONLY} (the owner does not move, the paused write executes
 * again under C0); and a write and a delete made in {@code REVERSE_ANNOUNCED} that reached no overflow replica arrive
 * as hints after the reverse passes, and the reconcile pass of {@code REVERSE_CLEANUP} copies them.</li>
 * </ul>
 */
public class UnsplitEndToEndTest extends UnsplitSupport
{
    private static final int ROWS = 200;
    /** The workload writes clusterings below this; the scenario's own rows are above. */
    private static final int WORKLOAD_RANGE = 140;
    private static final int DELETED_WHILE_SPLIT = 150;
    private static final int UPDATED_WHILE_SPLIT = 151;
    private static final int WRITTEN_WHILE_SPLIT = 1500;
    private static final int RANGE_FROM = 160;
    private static final int RANGE_TO = 165;

    private static String unsplitHistory(IInvokableInstance node, String id)
    {
        for (String line : history(node).split("\n"))
            if (line.contains(id))
                return line;
        return "";
    }

    @Test
    public void fastUnsplitKeepsEveryWriteAndAFailedVerificationAbortsToSteady() throws Throwable
    {
        try (Cluster cluster = start(3, unsplitConfig(nodes(1), settings("baseline_window", "1s"), settings())))
        {
            IInvokableInstance n1 = cluster.get(1);
            createTables(cluster, 2, "fast", "verify");
            arm(n1);
            fast(cluster);
            verificationFails(cluster);
        }
    }

    private void fast(Cluster cluster) throws Exception
    {
        IInvokableInstance n1 = cluster.get(1);
        String table = "fast";
        int k = keyWithReplicas(cluster, table, true, false, true);
        Model model = new Model(cluster, table, k, 1, 2, 3);
        model.initialData(ROWS);
        model.startRecordingPhases();
        Workload workload = null;
        try
        {
            split(n1, table, k, 4);
            awaitReleased(n1, table, "RESIDUE");
            String physical = physicalOf(n1, table);
            model.check("fast, split");

            // ---- while split: a delete and an update of rows written before, a range delete, a new row with a TTL ----
            model.both(2, ConsistencyLevel.ALL, "DELETE FROM %T USING TIMESTAMP " + model.nextTimestamp() + " WHERE p = " + k + " AND c = " + DELETED_WHILE_SPLIT);
            model.both(3, ConsistencyLevel.ALL, "UPDATE %T USING TIMESTAMP " + model.nextTimestamp() + " AND TTL 50000 SET v = 4242 WHERE p = " + k
                                                 + " AND c = " + UPDATED_WHILE_SPLIT);
            model.both(1, ConsistencyLevel.ALL, "DELETE FROM %T USING TIMESTAMP " + model.nextTimestamp() + " WHERE p = " + k + " AND c >= " + RANGE_FROM
                                                 + " AND c < " + RANGE_TO);
            model.both(2, ConsistencyLevel.ALL, "INSERT INTO %T (p, c, v, s) VALUES (" + k + ", " + WRITTEN_WHILE_SPLIT + ", 7, 'split') USING TIMESTAMP "
                                                 + model.nextTimestamp() + " AND TTL 70000");
            // the residue keeps the old row: only the overflow copy has the delete
            assertTrue(inBase(cluster.get(1), table, k, DELETED_WHILE_SPLIT));
            assertNull(readV(cluster, 1, table, k, DELETED_WHILE_SPLIT));
            workload = new Workload(model, WORKLOAD_RANGE, 11);
            workload.awaitAcked(20);
            model.check("fast, written while split");

            // ---- the unsplit, under the workload ----
            String id = unsplit(n1, table, k);
            await("the unsplit past REVERSE_BACKFILL", () -> quietly(() -> Set.of("REVERSE_VERIFY", "CUTBACK", "BASE_ONLY", "REVERSE_CLEANUP", "absent")
                                                                           .contains(phaseOf(n1, table, k))));
            workload.awaitAcked(10);
            checkWhileMoving(model, "fast, during the unsplit");
            awaitPhase(n1, table, k, "absent");
            await("the unsplit committed", () -> unsplitHistory(n1, id).contains("state=COMMITTED"));
            await("the overflow table dropped with the last key", () -> !tableExists(n1, physical));
            assertTrue(layoutGone(n1, table));
            workload.awaitAcked(20);
            model.check("fast, unsplit");

            // ---- R1, R2 with writetime and ttl ----
            workload.pause();
            int compared = assertWritetimeAndTtl(model, "fast, unsplit");
            assertTrue("rows compared: " + compared, compared > 100);
            assertRow(cluster, table, k, DELETED_WHILE_SPLIT, null);
            assertRow(cluster, table, k, RANGE_FROM + 2, null);
            assertRow(cluster, table, k, WRITTEN_WHILE_SPLIT, 7);
            if (model.alternatives(UPDATED_WHILE_SPLIT).isEmpty())
                assertRow(cluster, table, k, UPDATED_WHILE_SPLIT, 4242);
            // the key takes the stock path again: a lightweight transaction is accepted (and its row deleted again)
            cluster.coordinator(1).execute("INSERT INTO " + KS + '.' + table + " (p, c, v) VALUES (" + k + ", 1999, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
            model.both(1, ConsistencyLevel.ALL, "DELETE FROM %T USING TIMESTAMP " + (model.nextTimestamp() + 1_000_000) + " WHERE p = " + k + " AND c = 1999");
            workload.resume();
            System.out.println("T6.17k fast: trail\n" + trail(n1, id));
        }
        finally
        {
            if (workload != null)
                workload.stop();
            model.stopRecordingPhases();
        }
        model.check("fast, end");
        System.out.println("T6.17k fast: phases seen " + model.observedPhases() + "; acknowledged " + model.acked + ", failed " + model.failed
                           + "; workload: " + workload);
    }

    /** Design 11.4, abort from REVERSE_VERIFY: the base residue holds a delete newer than every copy of a row. */
    private void verificationFails(Cluster cluster) throws Exception
    {
        IInvokableInstance n1 = cluster.get(1);
        String table = "verify";
        int k = keyWithReplicas(cluster, table, true, false, true);
        Model model = new Model(cluster, table, k, 1, 2, 3);
        model.initialData(100);
        model.startRecordingPhases();
        try
        {
            split(n1, table, k, 4);
            awaitReleased(n1, table, "RESIDUE");
            model.both(2, ConsistencyLevel.ALL, "UPDATE %T USING TIMESTAMP " + model.nextTimestamp() + " SET v = 31 WHERE p = " + k + " AND c = 4");
            long future = (System.currentTimeMillis() + TimeUnit.DAYS.toMillis(2)) * 1000;
            for (int node : new int[]{ 1, 3 })
                cluster.get(node).executeInternal("DELETE FROM " + KS + '.' + table + " USING TIMESTAMP ? WHERE p = ? AND c = ?", future, k, 4);
            // the model's base-only probe writes into the residue too; it is not needed here
            model.poisonProbe = false;
            model.check("verify, split");

            String id = unsplit(n1, table, k);
            await("the unsplit rolled back", () -> unsplitHistory(n1, id).contains("state=ROLLED_BACK"));
            awaitReleased(n1, table, "RESIDUE");
            String trail = trail(n1, id);
            System.out.println("T6.17k verify: trail\n" + trail);
            assertTrue(trail, trail.contains("reverse verification failed 2 times"));
            assertTrue(trail, trail.contains("REVERSE_VERIFY") && !trail.contains("phase CUTBACK"));
            assertEquals("STEADY", phaseOf(cluster.get(3), table, k));
            // the overflow copy is read as before, through every coordinator
            assertRow(cluster, table, k, 4, 31);
            model.poisonProbe = true;
            model.check("verify, aborted to STEADY");
        }
        finally
        {
            model.stopRecordingPhases();
        }
        model.check("verify, end");
        System.out.println("T6.17k verify: phases seen " + model.observedPhases());
    }

    // ---------------------------------------------------------------------------------------------------------
    // a coordinator one class behind, and a late overflow hint
    // ---------------------------------------------------------------------------------------------------------

    /** On node 3: 0 armed, 1 paused in the seam, 2 released. */
    static volatile int seam;

    @Test
    public void aCoordinatorOneClassBehindAndALateOverflowHint() throws Throwable
    {
        // REVERSE_ANNOUNCED is held for the baseline window, long enough to make the hints
        try (Cluster cluster = start(3, unsplitConfig(nodes(1), settings("commit_approval", "manual", "baseline_window", "20s"),
                                                      settings("write_request_timeout", "40000ms"))))
        {
            IInvokableInstance n1 = cluster.get(1);
            createTables(cluster, 2, "held", "hint");
            arm(n1);
            held(cluster);
            lateHint(cluster);
        }
    }

    /** Splits the key with the manual commit approval: approves the proposal, then the commit point. */
    private static String splitApproved(IInvokableInstance n1, String table, int k) throws InterruptedException
    {
        String id = split(n1, table, k, 4);
        awaitPhase(n1, table, k, "CUTOVER");
        await("the commit point waits for approval", () -> layoutStatus(n1).contains("waits for `nodetool autonomic approve"));
        n1.nodetoolResult("autonomic", "approve", id).asserts().success();
        awaitReleased(n1, table, "RESIDUE");
        return id;
    }

    private static void awaitCutbackApproval(IInvokableInstance n1, String table, int k) throws InterruptedException
    {
        awaitPhase(n1, table, k, "CUTBACK");
        // the soak is 1 s; the approval is then all that is missing
        assertStays("the reverse commit point waits for its approval", 15, () -> "CUTBACK".equals(phaseOf(n1, table, k)));
    }

    private void held(Cluster cluster) throws Exception
    {
        IInvokableInstance n1 = cluster.get(1);
        IInvokableInstance n3 = cluster.get(3);
        String table = "held";
        int k = 1;
        Model model = new Model(cluster, table, k, 1, 2, 3);
        model.initialData(ROWS);
        model.startRecordingPhases();
        Workload workload = new Workload(model, ROWS, 5);
        try
        {
            splitApproved(n1, table, k);
            String id = unsplit(n1, table, k);
            awaitCutbackApproval(n1, table, k);
            workload.awaitAcked(20);
            model.check("held, CUTBACK");

            // ---- node 3 pauses inside a write, after its first copy (C1: the overflow copy) ----
            workload.pause();
            n3.runOnInstance(() -> {
                seam = 0;
                LayoutWrites.betweenCopiesForTesting = () -> {
                    if (seam != 0)
                        return;
                    seam = 1;
                    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
                    while (seam == 1 && System.nanoTime() < end)
                        sleep(5);
                };
            });
            long reexecutedBefore = n3.callOnInstance(() -> LayoutWrites.reexecutions.get());
            int c = 1001;
            long ts = model.begin(c, 4343);
            CompletableFuture<Throwable> paused = CompletableFuture.supplyAsync(() -> {
                try
                {
                    cluster.coordinator(3).execute(model.statement(c, 4343, ts), ConsistencyLevel.QUORUM);
                    return null;
                }
                catch (Throwable t)
                {
                    return t;
                }
            });
            await("node 3 to pause inside the write", () -> n3.callOnInstance(() -> seam == 1));
            workload.resume();

            // ---- node 3 is held at CUTBACK while the owner commits BASE_ONLY ----
            holdLog(cluster, 3);
            assertEquals("the second approval (owner answer 5)", "CUTBACK", phaseOf(n1, table, k));
            n1.nodetoolResult("autonomic", "approve", id).asserts().success();
            awaitPhase(n1, table, k, "BASE_ONLY");
            assertEquals("node 3 is one class behind", "CUTBACK", phaseOf(n3, table, k));
            long baseOnly = phaseEpoch(n1, table, k);
            assertFalse("O1: node 3 has not acknowledged BASE_ONLY", n1.callOnInstance(() -> EpochWatermarks.allAtLeast(Epoch.create(baseOnly))));
            assertStays("O1: the owner does not leave BASE_ONLY while node 3 is behind", 25,
                        () -> "BASE_ONLY".equals(phaseOf(n1, table, k)) && "CUTBACK".equals(phaseOf(n3, table, k)));
            workload.awaitAcked(10);

            // ---- release: node 3 catches up, the paused write executes again under C0 ----
            cluster.filters().reset();
            await("node 3 to catch up", () -> {
                long target = ClusterUtils.getCurrentEpoch(n1).getEpoch();
                return n1.callOnInstance(() -> EpochWatermarks.allAtLeast(Epoch.create(target))) && ClusterUtils.getCurrentEpoch(n3).getEpoch() >= target;
            });
            assertFalse("the write is still paused", paused.isDone());
            n3.runOnInstance(() -> seam = 2);
            Throwable outcome = paused.get(60, TimeUnit.SECONDS);
            long reexecuted = n3.callOnInstance(() -> LayoutWrites.reexecutions.get()) - reexecutedBefore;
            assertEquals("the paused write is acknowledged", null, outcome);
            assertTrue("C1: the write was executed again under the new routing", reexecuted >= 1);
            model.acknowledged(c, 4343, ts);
            n3.runOnInstance(() -> LayoutWrites.betweenCopiesForTesting = null);
            assertRow(cluster, table, k, c, 4343);
            checkWhileMoving(model, "held, after the release");

            awaitPhase(n1, table, k, "absent");
            await("the unsplit committed", () -> unsplitHistory(n1, id).contains("state=COMMITTED"));
            workload.awaitAcked(20);
            model.check("held, unsplit");
            workload.pause();
            assertWritetimeAndTtl(model, "held, unsplit");
            workload.resume();
        }
        finally
        {
            workload.stop();
            model.stopRecordingPhases();
        }
        model.check("held, end");
        System.out.println("T6.17k held: phases seen " + model.observedPhases() + "; workload: " + workload);
    }

    private static final int LATE_WRITE = 1002;

    private void lateHint(Cluster cluster) throws Exception
    {
        IInvokableInstance n1 = cluster.get(1);
        IInvokableInstance n2 = cluster.get(2);
        String table = "hint";
        int k = 1;
        Model model = new Model(cluster, table, k, 1, 2, 3);
        model.initialData(ROWS);
        model.startRecordingPhases();
        Workload workload = null;
        try
        {
            splitApproved(n1, table, k);
            // a new row and an existing row whose bucket partitions node 2 does not replicate
            int lateWrite = LATE_WRITE;
            while (bucketReplicasOf(n1, table, k, lateWrite).contains(2))
                lateWrite++;
            int lateDelete = WORKLOAD_RANGE + 1;
            while (bucketReplicasOf(n1, table, k, lateDelete).contains(2) || lateDelete % 11 == 5 || !inOverflowAnywhere(cluster, n1, table, k, lateDelete))
                lateDelete++;
            assertTrue(lateDelete < ROWS);

            String id = unsplit(n1, table, k);
            awaitPhase(n1, table, k, "REVERSE_ANNOUNCED");
            // ---- in REVERSE_ANNOUNCED (class C3, overflow only): a write and a delete that reach no replica ----
            n2.runOnInstance(() -> HintsService.instance.pauseDispatch());
            // node 2's writes fail fast (the 40 s of this cluster are for the paused write of the other scenario)
            n2.runOnInstance(() -> DatabaseDescriptor.setWriteRpcTimeout(2000L));
            long hintsBefore = n2.callOnInstance(() -> StorageMetrics.totalHints.getCount());
            cluster.filters().verbs(Verb.MUTATION_REQ.id).from(2).to(1, 3).drop().on();
            assertFalse("the late write is not acknowledged", model.write(2, lateWrite, 11, ConsistencyLevel.ONE));
            assertFalse("the late delete is not acknowledged", model.write(2, lateDelete, null, ConsistencyLevel.ONE));
            await("hints for both overflow replicas on node 2", () -> n2.callOnInstance(() -> StorageMetrics.totalHints.getCount()) >= hintsBefore + 4);
            cluster.filters().reset();
            n2.runOnInstance(() -> DatabaseDescriptor.setWriteRpcTimeout(40000L));
            assertEquals("still in REVERSE_ANNOUNCED", "REVERSE_ANNOUNCED", phaseOf(n1, table, k));
            String physical = physicalOf(n1, table);

            workload = new Workload(model, WORKLOAD_RANGE, 7);
            awaitCutbackApproval(n1, table, k);
            workload.awaitAcked(20);
            model.check("hint, CUTBACK, hints pending");

            // ---- the hints arrive after every reverse pass: they land in the overflow copy only ----
            n2.runOnInstance(() -> HintsService.instance.resumeDispatch());
            int lw = lateWrite;
            int ld = lateDelete;
            for (int node : new int[]{ 1, 3 })
            {
                IInvokableInstance n = cluster.get(node);
                await("the hints delivered to node " + node, () -> inOverflow(n, physical, k, lw) && !inOverflow(n, physical, k, ld));
            }
            // C1 reads the base copy, which the hints did not reach
            assertRow(cluster, table, k, lateWrite, null);
            assertTrue(readV(cluster, 1, table, k, lateDelete) != null);
            model.check("hint, CUTBACK, hints delivered to the overflow copy");

            // ---- the reconcile pass of REVERSE_CLEANUP copies them ----
            n1.nodetoolResult("autonomic", "approve", id).asserts().success();
            awaitPhase(n1, table, k, "absent");
            await("the unsplit committed", () -> unsplitHistory(n1, id).contains("state=COMMITTED"));
            assertRow(cluster, table, k, lateWrite, 11);
            assertRow(cluster, table, k, lateDelete, null);
            workload.awaitAcked(20);
            model.check("hint, unsplit");
            System.out.println("T6.17k hint: trail\n" + trail(n1, id));
        }
        finally
        {
            if (workload != null)
                workload.stop();
            model.stopRecordingPhases();
        }
        model.check("hint, end");
        System.out.println("T6.17k hint: phases seen " + model.observedPhases());
    }

    private static boolean inOverflowAnywhere(Cluster cluster, IInvokableInstance n1, String table, int k, int c)
    {
        String physical = physicalOf(n1, table);
        for (int node = 1; node <= 3; node++)
            if (inOverflow(cluster.get(node), physical, k, c))
                return true;
        return false;
    }
}
