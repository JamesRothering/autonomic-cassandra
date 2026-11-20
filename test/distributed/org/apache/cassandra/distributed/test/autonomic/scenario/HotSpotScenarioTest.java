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
package org.apache.cassandra.distributed.test.autonomic.scenario;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.actions;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.await;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.describe;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.diagnoses;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.inState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * D7 scenario, injected hot spot: a wide partition (1500 rows) takes over 98% of the writes and point reads of its table
 * on a two-node cluster with replication factor 1, so the buckets can land on both nodes. Everything is the production
 * path: the sampler, the {@code hot_partition} diagnosis, {@code PartitionSplitPolicy} (probe, shape profile, benefit),
 * the safety governor, and the split actuator through every phase to STEADY and its stage B verification. Intervals,
 * windows and thresholds are shortened through configuration; approvals are automatic.
 *
 * <p>Asserts the diagnosis (subject and key), the action ({@code partition_split}, split mode, buckets, every phase in
 * its audit trail), the verification outcome ({@code COMMITTED} with the verdict {@code IMPROVED}) and that a client of
 * the old schema, which keeps using {@code ks.t} unchanged, reads exactly what it wrote: all rows, the single rows and a
 * slice, while the buckets of the overflow table hold the partition. The base rows are removed later by the reclaim, which is not part of this scenario.
 */
public class HotSpotScenarioTest extends TestBaseImpl
{
    private static final String KS = "scn_hot";
    private static final int HOT = 1;
    private static final int ROWS = 1500;
    // The workload is mostly point reads, with a few writes. The profile needs min_samples operations, which the reads
    // provide, while the write latency of the verification window (a fixed 25% over its baseline) is judged only from
    // min_samples writes, which the few writes never reach: in-JVM latencies of microseconds swing by factors with the
    // load of the three phases, and the scenario is about the loop, not about a latency budget. The read guard is kept
    // with a wide tolerance for the same reason.

    private static void configure(IInstanceConfig c)
    {
        c.with(NETWORK, GOSSIP)
         .set("write_request_timeout", "2000ms")
         .set("range_request_timeout", "5000ms")
         .set("autonomic.layout", Map.ofEntries(Map.entry("ack_poll_interval", "200ms"), Map.entry("cas_drain", "100ms"),
                                                Map.entry("cutover_soak", "1s"), Map.entry("straggler_window", "1s"),
                                                Map.entry("commit_approval", "auto"), Map.entry("verify_recheck_delay", "200ms"),
                                                Map.entry("backfill_throughput", "0MiB/s"), Map.entry("for_evaluations", 3),
                                                Map.entry("min_rows_for_split", 500), Map.entry("profile_window", "3s"),
                                                Map.entry("min_samples", 50), Map.entry("read_regression_tolerance", 20.0), Map.entry("baseline_window", "2s"),
                                                Map.entry("verify_window", "4s")));
        if (c.num() != 1)
            return;
        c.set("autonomic.enabled", true)
         .set("autonomic.dry_run", false)
         .set("autonomic.startup_grace", "1s")
         .set("autonomic.monitor_interval", "500ms")
         .set("autonomic.analyze_interval", "500ms")
         .set("autonomic.sampling.window", "1s")
         .set("autonomic.sampling.interval", "1s")
         .set("autonomic.sampling.min_ops_per_sec", 1)
         .set("autonomic.safety.cluster_approval", "auto")
         .set("autonomic.detectors", Map.of("hot_partition", Map.of("for_evaluations", 2)))
         .set("autonomic.actuators", Map.of("partition_split", Map.of("enabled", true)));
    }

    @Test
    public void aHotWidePartitionIsSplitAndTheOldSchemaStillReadsTheSame() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).withConfig(HotSpotScenarioTest::configure).start()))
        {
            cluster.schemaChange("CREATE KEYSPACE " + KS + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            cluster.schemaChange("CREATE TABLE " + KS + ".t (id int, seq int, v int, PRIMARY KEY (id, seq))");
            String insert = "INSERT INTO " + KS + ".t (id, seq, v) VALUES (?, ?, ?)";
            TreeMap<Integer, Integer> model = new TreeMap<>();
            Set<Integer> uncertain = new HashSet<>();
            for (int start = 0; start < ROWS; start += 250)
            {
                StringBuilder batch = new StringBuilder("BEGIN UNLOGGED BATCH ");
                for (int seq = start; seq < start + 250; seq++)
                {
                    batch.append("INSERT INTO ").append(KS).append(".t (id, seq, v) VALUES (").append(HOT).append(", ").append(seq).append(", ").append(seq).append("); ");
                    model.put(seq, seq);
                }
                cluster.coordinator(1).execute(batch.append("APPLY BATCH").toString(), ConsistencyLevel.ALL);
            }
            await("the layer to be armed", () -> cluster.get(1).nodetoolResult("autonomic", "status").getStdout().contains("Armed: true"));
            cluster.get(1).nodetoolResult("autonomic", "cluster", "enable", "--yes").asserts().success();

            // the hot spot: over 98% of the operations on one key, writes and point reads, through node 1; the client is this thread
            int[] counter = { 0 };
            Object[] done = null;
            long end = System.currentTimeMillis() + 3 * ScenarioSupport.DEADLINE_MILLIS;
            while (System.currentTimeMillis() < end && done == null)
            {
                for (int n = 0; n < 40; n++)
                {
                    int seq = counter[0]++ % (ROWS + 200);
                    try
                    {
                        if (n == 0 && counter[0] % 160 < 40)
                            cluster.coordinator(1).execute(insert, ConsistencyLevel.ONE, 2 + (counter[0] % 18), 0, n);
                        else if (n == 5 && counter[0] % 80 < 40)
                        {
                            int v = counter[0];
                            uncertain.add(seq);
                            cluster.coordinator(1).execute(insert, ConsistencyLevel.ONE, HOT, seq, v);
                            model.put(seq, v);
                            uncertain.remove(seq);
                        }
                        else
                            cluster.coordinator(1).execute("SELECT v FROM " + KS + ".t WHERE id = ? AND seq = ?", ConsistencyLevel.ONE, HOT, seq);
                    }
                    catch (Throwable t)
                    {
                        // a timeout around a phase change is allowed; the outcome of that write is unknown
                    }
                }
                done = inState(cluster, 1, "partition_split", "COMMITTED");
                for (String bad : new String[]{ "FAILED", "ROLLED_BACK", "ABORTED", "REJECTED" })
                    assertTrue(bad + ": " + describe(actions(cluster, 1, "partition_split")), inState(cluster, 1, "partition_split", bad) == null);
            }
            assertNotNull("no COMMITTED split within the deadline: " + describe(actions(cluster, 1, "partition_split")), done);

            // diagnosis: the hot partition, named by its key
            String hex = ByteBufferUtil.bytesToHex(Int32Type.instance.decompose(HOT));
            Object[] diagnosis = null;
            for (Object[] row : diagnoses(cluster, 1, "hot_partition"))
                if (("PARTITION:" + KS + ".t:" + hex).equals(row[0]))
                    diagnosis = row;
            assertNotNull("no hot_partition diagnosis of the key: " + describe(diagnoses(cluster, 1, "hot_partition")), diagnosis);
            @SuppressWarnings("unchecked")
            Map<String, String> evidence = (Map<String, String>) diagnosis[3];
            assertEquals(hex, evidence.get("key_hex"));

            // action and verification outcome
            @SuppressWarnings("unchecked")
            Map<String, String> params = (Map<String, String>) done[4];
            assertEquals("split", params.get("mode"));
            assertTrue("buckets: " + params, Integer.parseInt(params.get("buckets")) >= 4);
            TimeUUID id = (TimeUUID) done[0];
            StringBuilder trail = new StringBuilder();
            List<String> events = new ArrayList<>();
            for (Object[] e : cluster.coordinator(1).execute("SELECT from_state, to_state, detail FROM system_autonomic.action_events WHERE action_id = ?",
                                                             ConsistencyLevel.ONE, id.asUUID()))
                events.add(e[0] + "->" + e[1] + " " + e[2]);
            for (String e : events)
                trail.append(e).append('\n');
            assertEquals(trail.toString() + describe(actions(cluster, 1, "partition_split")), "IMPROVED", done[3]);
            for (String p : new String[]{ "ANNOUNCED", "DUAL_WRITE", "BACKFILL", "VERIFY", "CUTOVER", "NEW_ONLY", "CLEANUP", "STEADY" })
                assertTrue(p + " is not in the audit trail:\n" + trail, trail.toString().contains("phase " + p + " "));

            // the buckets of the overflow table hold the partition (the base rows go later, with the reclaim after reclaim_window)
            String physical = (String) cluster.get(1).executeInternal("SELECT physical_table FROM system_views.autonomic_layouts WHERE keyspace_name = ?", KS)[0][0];
            String overflow = physical.substring(physical.indexOf('.') + 1);
            Set<Object> buckets = new HashSet<>();
            for (int node = 1; node <= 2; node++)
                for (Object[] row : cluster.get(node).executeInternal("SELECT DISTINCT id, \"__al_bucket\" FROM " + KS + '.' + overflow))
                    buckets.add(row[1]);
            assertEquals(buckets.toString(), Integer.parseInt(params.get("buckets")), buckets.size());

            // the old schema: the same table, the same queries, the same results
            for (int node = 1; node <= 2; node++)
            {
                Object[][] rows = cluster.coordinator(node).execute("SELECT seq, v FROM " + KS + ".t WHERE id = ?", ConsistencyLevel.ALL, HOT);
                int previous = -1;
                int checked = 0;
                for (Object[] row : rows)
                {
                    int seq = (Integer) row[0];
                    assertTrue("rows out of order at " + seq + " on node " + node, seq > previous);
                    previous = seq;
                    if (!uncertain.contains(seq))
                    {
                        assertEquals("row " + seq + " on node " + node, model.get(seq), row[1]);
                        checked++;
                    }
                }
                assertTrue("rows on node " + node + ": " + rows.length + " of " + model.size(), rows.length >= model.size() - uncertain.size() && checked > ROWS - 1);
                Object[][] one = cluster.coordinator(node).execute("SELECT v FROM " + KS + ".t WHERE id = ? AND seq = ?", ConsistencyLevel.ALL, HOT, 700);
                if (!uncertain.contains(700))
                    assertEquals(model.get(700), one[0][0]);
                Object[][] slice = cluster.coordinator(node).execute("SELECT seq FROM " + KS + ".t WHERE id = ? AND seq >= 100 AND seq < 140", ConsistencyLevel.ALL, HOT);
                assertEquals(40, slice.length);
            }

            // wait for the node with the busy layer to be down before the cluster closes, or the thread check of the harness can see it stopping
            cluster.get(1).shutdown().get();
        }
    }
}
