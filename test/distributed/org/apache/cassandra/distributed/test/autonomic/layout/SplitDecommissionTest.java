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
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DataRateSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * T6.13, design 4.7 and 4.11 "node decommission during migration", with the real actuator. Shrunk to fit this
 * machine: three nodes with replication factor <b>2</b> (with RF 3 on three nodes a decommission is impossible), owner
 * node 1. The key is chosen so that node 3 is one of its two base replicas; the backfill is slowed so that the key is
 * in BACKFILL when node 3 is decommissioned.
 *
 * <p>Asserted: while the decommission's sequence is in progress the owner does not change the key's phase or
 * generation (it holds, 4.7); after it, the owner restarts the backfill generation because the key's replica set
 * changed; the migration completes; every node of the new replica set holds, node-locally in the overflow table,
 * every row the model has; and the invariants hold throughout.
 */
public class SplitDecommissionTest extends SplitFailureSupport
{
    private static final int ROWS = 300;

    private static void backfillRate(Cluster cluster, String rate, int chunkRows, int... nodes)
    {
        for (int node : nodes)
            cluster.get(node).runOnInstance(() -> {
                DatabaseDescriptor.getAutonomicConfig().layout.backfill_throughput = new DataRateSpec.LongBytesPerSecondBound(rate);
                DatabaseDescriptor.getAutonomicConfig().layout.backfill_chunk_rows = chunkRows;
            });
    }

    @Test
    public void decommissionDuringBackfill() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(CassandraRelevantProperties.SKIP_AUTH_SETUP, "true");
             Cluster cluster = start(3, config(nodes(1), settings(),
                                               settings("progress_barrier_timeout", "1000ms", "progress_barrier_backoff", "100ms"))))
        {
            IInvokableInstance n1 = cluster.get(1);
            String table = "decom";
            createTables(cluster, 2, table);
            int key = 1;
            while (!isReplica(cluster.get(3), table, key))
                key++;
            int k = key;
            arm(n1);
            Model model = new Model(cluster, table, k, 1, 2, 3);
            model.initialData(ROWS);
            model.startRecordingPhases();
            Workload workload = new Workload(model, ROWS, 6);
            try
            {
                backfillRate(cluster, "3KiB/s", 20, 1, 2, 3);
                String id = split(n1, table, k, 4);
                awaitPhase(n1, table, k, "BACKFILL");
                workload.awaitAcked(20);
                model.check("decom, BACKFILL");
                String before = progress(n1, table, k);
                assertTrue("the backfill is still running: " + before, !before.endsWith(":2"));

                // ---- decommission node 3, sampling what the owner does meanwhile ----
                List<String> samples = new ArrayList<>();
                CompletableFuture<Boolean> decommission = CompletableFuture.supplyAsync(() -> cluster.get(3).nodetoolResult("decommission", "--force").getRc() == 0);
                while (!decommission.isDone())
                {
                    boolean inProgress = n1.callOnInstance(() -> !ClusterMetadata.current().inProgressSequences.isEmpty());
                    samples.add(inProgress + "/" + phaseOf(n1, table, k) + "/" + progress(n1, table, k));
                    Thread.sleep(100);
                }
                assertTrue("the decommission succeeded", decommission.get(10, TimeUnit.SECONDS));
                model.setLive(1, 2);
                System.out.println("T6.13 decom: samples during the decommission (in progress/phase/generation:reports): " + samples);
                // 4.7: while a sequence is in progress the phase and the generation do not change
                for (int i = 1; i < samples.size(); i++)
                {
                    String[] a = samples.get(i - 1).split("/");
                    String[] b = samples.get(i).split("/");
                    if (a[0].equals("true") && b[0].equals("true"))
                        assertTrue("the owner moved while the decommission was in progress: " + samples.get(i - 1) + " -> " + samples.get(i),
                                   a[1].equals(b[1]) && a[2].split(":")[0].equals(b[2].split(":")[0]));
                }
                assertTrue("a sequence was seen in progress", samples.stream().anyMatch(s -> s.startsWith("true")));
                backfillRate(cluster, "0MiB/s", 1000, 1, 2);
                int generationBefore = Integer.parseInt(before.split(":")[0]);
                await("a new backfill generation for the new replica set",
                      () -> quietly(() -> !progress(n1, table, k).equals("absent") && Integer.parseInt(progress(n1, table, k).split(":")[0]) > generationBefore)
                            || "STEADY".equals(phaseOf(n1, table, k)));
                awaitPhase(n1, table, k, "STEADY");
                await("the action committed", () -> history(n1).contains("state=COMMITTED"));
                String trail = trail(n1, id);
                System.out.println("T6.13 decom: trail\n" + trail);
                assertTrue("the generation was restarted: " + trail, trail.contains("phase BACKFILL (step") && trail.indexOf("phase BACKFILL (step") != trail.lastIndexOf("phase BACKFILL (step"));
                workload.awaitAcked(20);
                model.check("decom, STEADY");

                // ---- complete on the new replica set: nodes 1 and 2 each hold every bucket ----
                workload.pause();
                Set<Integer> expected = new TreeSet<>();
                for (Object[] row : cluster.coordinator(1).execute("SELECT c FROM " + KS + '.' + table + "_twin WHERE p = ?", ConsistencyLevel.ALL, k))
                    if (model.alternatives((Integer) row[0]).isEmpty())
                        expected.add((Integer) row[0]);
                String physical = (String) n1.executeInternal("SELECT physical_table FROM system_views.autonomic_layouts WHERE keyspace_name = ?", KS)[0][0];
                physical = physical.substring(physical.indexOf('.') + 1);
                for (int node = 1; node <= 2; node++)
                {
                    Set<Integer> held = new TreeSet<>();
                    for (Object[] row : cluster.get(node).executeInternal("SELECT p, c FROM " + KS + '.' + physical))
                        if ((Integer) row[0] == k)
                            held.add((Integer) row[1]);
                    Set<Integer> missing = new TreeSet<>(expected);
                    missing.removeAll(held);
                    assertEquals("node " + node + " misses rows in the overflow table", new TreeSet<>(), missing);
                }
                workload.resume();
            }
            finally
            {
                workload.stop();
                model.stopRecordingPhases();
            }
            model.check("decom, end");
            System.out.println("T6.13 decom: phases seen " + model.observedPhases() + "; acknowledged " + model.acked + ", failed " + model.failed
                               + ", unknown outcomes left " + model.uncertainCount() + "; workload: " + workload);
        }
    }
}
