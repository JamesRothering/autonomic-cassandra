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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.autonomic.execute.actuators.PartitionUnsplitActuator;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * T6.17k: failures during the reverse migration, with the real actuators.
 *
 * <ul>
 * <li>{@link #ownerLostInReverseBackfillAndInReverseCleanup}: three nodes, replication factor 3, the CMS on all three,
 * nodes 1 and 2 running the layer, lease 15 s. The owner (node 1) is shut down in {@code REVERSE_BACKFILL} (the
 * reverse pass slowed): before the reverse commit point the stop takes the abort path, which waits for every node
 * (O1); node 2 takes the expired claim over; once node 1 is back the abort completes, back to {@code STEADY}, released.
 * Then the owner of a second key is shut down in {@code REVERSE_CLEANUP} (a long {@code straggler_window}): past the
 * commit point the claim is released as COMMITTED_CLEANUP_PENDING and the key stays in {@code REVERSE_CLEANUP}, routed
 * to the base copy; once the owner is back, the preferred proposer proposes {@code mode resume}
 * ({@code layout.unsplit_orphaned}), which is approved and finishes the unsplit.</li>
 * <li>{@link #decommissionOfABucketReplicaDuringTheReverse}: three nodes, replication factor 2; node 3 replicates a
 * bucket of the key and is decommissioned while the key is in {@code REVERSE_BACKFILL}: the owner holds while the
 * sequence runs (4.7), then restarts the reverse generation over the new bucket replicas and the unsplit completes.</li>
 * </ul>
 */
public class UnsplitFailureTest extends UnsplitSupport
{
    private static final int K = 1;
    private static final int ROWS = 300;

    private static void stragglerWindow(IInvokableInstance node, String window)
    {
        node.runOnInstance(() -> DatabaseDescriptor.getAutonomicConfig().layout.straggler_window = new DurationSpec.LongMillisecondsBound(window));
    }

    /**
     * After the restart of a node whose action was stopped, its layer comes up paused (a persisted flag); the operator
     * lets it act again.
     */
    private static void resumeLayer(IInvokableInstance node) throws InterruptedException
    {
        await("the layer to start on node " + node.config().num(), () -> quietly(() -> node.nodetoolResult("autonomic", "status").getStdout().contains("State:")));
        node.nodetoolResult("autonomic", "resume").asserts().success();
        arm(node);
    }

    private static String unsplitHistory(IInvokableInstance node, String id)
    {
        for (String line : history(node).split("\n"))
            if (line.contains(id))
                return line;
        return "";
    }

    @Test
    public void ownerLostInReverseBackfillAndInReverseCleanup() throws Throwable
    {
        Map<String, Object> extra = settings("autonomic.coordination", Map.of("lease", "15s", "lease_safety_margin", "2s"));
        try (Cluster cluster = start(3, unsplitConfig(nodes(1, 2), settings("baseline_window", "1s"), extra)))
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            createTables(cluster, 3, "rbf", "rcl");
            arm(cluster.get(1), cluster.get(2));
            lostInReverseBackfill(cluster);
            lostInReverseCleanup(cluster);
        }
    }

    private void lostInReverseBackfill(Cluster cluster) throws Exception
    {
        IInvokableInstance owner = cluster.get(1);
        IInvokableInstance standby = cluster.get(2);
        String table = "rbf";
        Model model = new Model(cluster, table, K, 1, 2, 3);
        model.initialData(ROWS);
        model.startRecordingPhases();
        Workload workload = new Workload(model, ROWS, 31);
        try
        {
            split(owner, table, K, 4);
            awaitReleased(standby, table, "RESIDUE");
            workload.awaitAcked(20);
            model.check("rbf, split");

            backfillRate(cluster, "3KiB/s", 20);
            String id = unsplit(owner, table, K);
            awaitPhase(standby, table, K, "REVERSE_BACKFILL");
            await("the reverse pass of the new generation", () -> !progress(standby, table, K).endsWith(":3") && quietly(() -> Integer.parseInt(progress(standby, table, K).split(":")[0]) > 3));
            workload.awaitAcked(10);
            checkWhileMoving(model, "rbf, REVERSE_BACKFILL");

            // ---- the owner goes down mid-copy ----
            model.setLive(2, 3);
            owner.shutdown().get();
            assertStays("rbf: never past the reverse commit point while the owner is down", 25, () -> {
                String p = phaseOf(standby, table, K);
                return p.equals("REVERSE_BACKFILL") || p.equals("REVERSE_VERIFY") || p.equals("STEADY");
            });
            workload.awaitAcked(20);
            checkWhileMoving(model, "rbf, owner down");
            System.out.println("T6.17k rbf: while the owner is down, node 2 sees " + phaseOf(standby, table, K) + "; status:\n" + layoutStatus(standby));

            // ---- the owner comes back: the abort completes ----
            owner.startup();
            ClusterUtils.waitForCMSToQuiesce(cluster, standby);
            model.setLive(1, 2, 3);
            resumeLayer(owner);
            backfillRate(cluster, "0MiB/s", 1000);
            awaitReleased(standby, table, "RESIDUE");
            workload.awaitAcked(20);
            model.check("rbf, aborted to STEADY");
            String trail = trail(owner, id) + history(standby) + history(owner);
            System.out.println("T6.17k rbf: history on node 2:\n" + history(standby) + "\ntrail of " + id + ":\n" + trail(owner, id));
            assertFalse("rbf: never past the reverse commit point", trail.contains("phase BASE_ONLY") || trail.contains("phase CUTBACK"));
        }
        finally
        {
            workload.stop();
            model.stopRecordingPhases();
        }
        model.check("rbf, end");
        System.out.println("T6.17k rbf: phases seen " + model.observedPhases() + "; workload: " + workload);
    }

    private void lostInReverseCleanup(Cluster cluster) throws Exception
    {
        IInvokableInstance owner = cluster.get(1);
        IInvokableInstance standby = cluster.get(2);
        String table = "rcl";
        Model model = new Model(cluster, table, K, 1, 2, 3);
        model.initialData(ROWS);
        model.startRecordingPhases();
        Workload workload = new Workload(model, ROWS, 32);
        try
        {
            split(owner, table, K, 4);
            awaitReleased(standby, table, "RESIDUE");
            stragglerWindow(owner, "30s");
            String id = unsplit(owner, table, K);
            awaitPhase(standby, table, K, "REVERSE_CLEANUP");
            String generation = progress(standby, table, K);
            workload.awaitAcked(10);
            checkWhileMoving(model, "rcl, REVERSE_CLEANUP");

            // ---- the owner goes down past the reverse commit point ----
            model.setLive(2, 3);
            owner.shutdown().get();
            assertStays("rcl: the key stays in REVERSE_CLEANUP without its owner", 25, () -> "REVERSE_CLEANUP".equals(phaseOf(standby, table, K)));
            assertTrue("rcl: the reconcile pass had not started", generation.equals(progress(standby, table, K)));
            workload.awaitAcked(20);
            checkWhileMoving(model, "rcl, owner down");

            // ---- the owner comes back; the orphan's resume is proposed and approved ----
            owner.startup();
            ClusterUtils.waitForCMSToQuiesce(cluster, standby);
            model.setLive(1, 2, 3);
            resumeLayer(owner);
            String resume = proposed(owner, PartitionUnsplitActuator.NAME, PartitionUnsplitActuator.MODE_RESUME);
            owner.nodetoolResult("autonomic", "approve", resume).asserts().success();
            awaitPhase(standby, table, K, "absent");
            await("rcl: the resume committed", () -> unsplitHistory(owner, resume).contains("state=COMMITTED"));
            workload.awaitAcked(20);
            model.check("rcl, unsplit");
            workload.pause();
            assertWritetimeAndTtl(model, "rcl, unsplit");
            workload.resume();
            System.out.println("T6.17k rcl: trail of the resume:\n" + trail(owner, resume) + "\ntrail of " + id + ":\n" + trail(owner, id));
        }
        finally
        {
            workload.stop();
            model.stopRecordingPhases();
        }
        model.check("rcl, end");
        System.out.println("T6.17k rcl: phases seen " + model.observedPhases() + "; workload: " + workload);
    }

    @Test
    public void decommissionOfABucketReplicaDuringTheReverse() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(CassandraRelevantProperties.SKIP_AUTH_SETUP, "true");
             Cluster cluster = start(3, unsplitConfig(nodes(1), settings("baseline_window", "1s"),
                                                      settings("progress_barrier_timeout", "1000ms", "progress_barrier_backoff", "100ms"))))
        {
            IInvokableInstance n1 = cluster.get(1);
            String table = "rdecom";
            createTables(cluster, 2, table);
            arm(n1);
            Model model = new Model(cluster, table, K, 1, 2, 3);
            model.initialData(ROWS);
            model.startRecordingPhases();
            Workload workload = new Workload(model, ROWS, 33);
            try
            {
                split(n1, table, K, 4);
                awaitReleased(n1, table, "RESIDUE");
                boolean replicatesABucket = false;
                for (int c = 0; c < 64 && !replicatesABucket; c++)
                    replicatesABucket = bucketReplicasOf(n1, table, K, c).contains(3);
                assertTrue("node 3 replicates a bucket of the key", replicatesABucket);
                // slow enough that the decommission's sequence starts before every bucket replica has copied
                backfillRate(cluster, "512B/s", 5);
                String id = unsplit(n1, table, K);
                awaitPhase(n1, table, K, "REVERSE_BACKFILL");
                await("the reverse pass of the new generation", () -> quietly(() -> Integer.parseInt(progress(n1, table, K).split(":")[0]) > 3));
                String before = progress(n1, table, K);
                workload.awaitAcked(20);
                checkWhileMoving(model, "rdecom, REVERSE_BACKFILL");

                // ---- decommission node 3, sampling what the owner does meanwhile ----
                List<String> samples = new ArrayList<>();
                CompletableFuture<Boolean> decommission = CompletableFuture.supplyAsync(() -> cluster.get(3).nodetoolResult("decommission", "--force").getRc() == 0);
                while (!decommission.isDone())
                {
                    boolean inProgress = n1.callOnInstance(() -> !ClusterMetadata.current().inProgressSequences.isEmpty());
                    samples.add(inProgress + "/" + phaseOf(n1, table, K) + "/" + progress(n1, table, K));
                    Thread.sleep(100);
                }
                assertTrue("the decommission succeeded", decommission.get(10, TimeUnit.SECONDS));
                model.setLive(1, 2);
                System.out.println("T6.17k rdecom: samples (in progress/phase/generation:reports): " + samples);
                for (int i = 1; i < samples.size(); i++)
                {
                    String[] a = samples.get(i - 1).split("/");
                    String[] b = samples.get(i).split("/");
                    if (a[0].equals("true") && b[0].equals("true"))
                        assertTrue("the owner moved while the decommission was in progress: " + samples.get(i - 1) + " -> " + samples.get(i),
                                   a[1].equals(b[1]) && a[2].split(":")[0].equals(b[2].split(":")[0]));
                }
                assertTrue("a sequence was seen in progress in REVERSE_BACKFILL: " + samples, samples.stream().anyMatch(s -> s.startsWith("true/REVERSE_BACKFILL/")));
                backfillRate(cluster, "0MiB/s", 1000);
                awaitPhase(n1, table, K, "absent");
                await("the unsplit committed", () -> unsplitHistory(n1, id).contains("state=COMMITTED"));
                String trail = trail(n1, id);
                System.out.println("T6.17k rdecom: trail\n" + trail + "\nbefore the decommission " + before);
                // entered from REVERSE_DUAL_WRITE, its own generation on entry (T6.17f), and the restart for the new replicas
                assertTrue("the reverse generation was restarted for the new bucket replicas: " + trail,
                           trail.split("phase REVERSE_BACKFILL \\(step", -1).length - 1 >= 3);
                workload.awaitAcked(20);
                model.check("rdecom, unsplit");
                workload.pause();
                assertWritetimeAndTtl(model, "rdecom, unsplit");
                workload.resume();
            }
            finally
            {
                workload.stop();
                model.stopRecordingPhases();
            }
            model.check("rdecom, end");
            System.out.println("T6.17k rdecom: phases seen " + model.observedPhases() + "; workload: " + workload);
        }
    }
}
