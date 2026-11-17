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

import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.config.DataRateSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.net.Verb;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * T6.13, design 4.4 and 4.11 "CMS unavailable", with the real actuator, three nodes, replication factor 3, the CMS on
 * all three nodes, owner node 1. The CMS is made unavailable without stopping any node: a message filter drops every
 * Paxos message (the CMS appends to its log by Paxos) and every commit request, so no node can commit, while every
 * node stays up and answers epoch requests, so rule O1 alone would let the owner go on. Two outages in one migration:
 *
 * <ol>
 * <li>in BACKFILL (slowed): no phase change and no backfill report on any node; routing goes on from each node's
 * local metadata (the workload through all three coordinators keeps being acknowledged and read back);</li>
 * <li>in CUTOVER, during the soak: the key stays in CUTOVER after the soak has passed.</li>
 * </ol>
 * After each outage the migration goes on, and it finishes without taking the abort path. LWT is not used by the
 * workload (it is rejected for the key anyway), so the filter does not touch client operations.
 */
public class SplitCmsUnavailableTest extends SplitFailureSupport
{
    private static final int K = 1;
    private static final int ROWS = 300;

    private static void backfillRate(Cluster cluster, String rate, int chunkRows, int... nodes)
    {
        for (int node : nodes)
            cluster.get(node).runOnInstance(() -> {
                DatabaseDescriptor.getAutonomicConfig().layout.backfill_throughput = new DataRateSpec.LongBytesPerSecondBound(rate);
                DatabaseDescriptor.getAutonomicConfig().layout.backfill_chunk_rows = chunkRows;
            });
    }

    private static void cmsUnavailable(Cluster cluster)
    {
        int[] verbs = Arrays.stream(Verb.values()).filter(v -> v.name().startsWith("PAXOS") || v == Verb.TCM_COMMIT_REQ).mapToInt(v -> v.id).toArray();
        cluster.filters().verbs(verbs).drop().on();
    }

    @Test
    public void migrationHoldsWhileTheCmsIsUnavailable() throws Throwable
    {
        try (Cluster cluster = start(3, config(nodes(1), settings("cutover_soak", "3s"), settings())))
        {
            IInvokableInstance n1 = cluster.get(1);
            String table = "cms";
            n1.nodetoolResult("cms", "reconfigure", "3").asserts().success();
            createTables(cluster, 3, table);
            arm(n1);
            Model model = new Model(cluster, table, K, 1, 2, 3);
            model.initialData(ROWS);
            model.startRecordingPhases();
            Workload workload = new Workload(model, ROWS, 8);
            try
            {
                backfillRate(cluster, "3KiB/s", 20, 1, 2, 3);
                String id = split(n1, table, K, 4);
                awaitPhase(n1, table, K, "BACKFILL");
                workload.awaitAcked(20);
                model.check("cms, BACKFILL");

                // ---- 1. in BACKFILL ----
                cmsUnavailable(cluster);
                String progress = progress(n1, table, K);
                long acked = model.acked.get();
                assertStays("CMS unavailable in BACKFILL: no phase change and no report", 25, () -> allIn(cluster, table, "BACKFILL")
                                                                                                    && progress.equals(progress(n1, table, K)));
                workload.awaitAcked(20);
                assertTrue("routing goes on while the CMS is unavailable", model.acked.get() > acked);
                model.check("cms, CMS unavailable, BACKFILL");
                cluster.filters().reset();
                backfillRate(cluster, "0MiB/s", 1000, 1, 2, 3);

                // ---- 2. in CUTOVER, during the soak ----
                awaitPhase(n1, table, K, "CUTOVER");
                cmsUnavailable(cluster);
                acked = model.acked.get();
                assertStays("CMS unavailable in CUTOVER: the key stays there past the soak", 30, () -> allIn(cluster, table, "CUTOVER"));
                workload.awaitAcked(20);
                assertTrue("routing goes on while the CMS is unavailable", model.acked.get() > acked);
                model.check("cms, CMS unavailable, CUTOVER");
                System.out.println("T6.13 cms: status while the CMS is unavailable:\n" + layoutStatus(n1));
                cluster.filters().reset();

                awaitPhase(n1, table, K, "STEADY");
                await("the action committed", () -> history(n1).contains("state=COMMITTED"));
                workload.awaitAcked(20);
                model.check("cms, STEADY");
                String trail = trail(n1, id);
                System.out.println("T6.13 cms: trail\n" + trail);
                assertFalse("the owner did not take the abort path: " + trail, trail.contains("ROLLBACK_DUAL") || trail.contains("ABORTING"));
            }
            finally
            {
                workload.stop();
                model.stopRecordingPhases();
            }
            model.check("cms, end");
            System.out.println("T6.13 cms: phases seen " + model.observedPhases() + "; acknowledged " + model.acked + ", failed " + model.failed
                               + ", unknown outcomes left " + model.uncertainCount() + "; workload: " + workload);
        }
    }

    private static boolean allIn(Cluster cluster, String table, String phase)
    {
        for (int node = 1; node <= 3; node++)
            if (!phase.equals(phaseOf(cluster.get(node), table, K)))
                return false;
        return true;
    }
}
