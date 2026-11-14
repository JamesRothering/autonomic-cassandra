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
package org.apache.cassandra.distributed.test.autonomic;

import java.util.Map;
import java.util.function.BooleanSupplier;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * D3 acceptance on one node: an injected hot partition yields a named {@code hot_partition} diagnosis in
 * {@code system_autonomic.diagnoses} and in {@code nodetool autonomic history --diagnoses}; tombstone-heavy reads
 * yield {@code tombstone_overload}. Intervals are shortened through configuration, thresholds are the defaults.
 */
public class HotPartitionDiagnosisTest extends TestBaseImpl
{
    private static final long DEADLINE_MILLIS = 75_000;

    private static void fast(IInstanceConfig c)
    {
        c.set("autonomic.enabled", true)
         .set("autonomic.startup_grace", "1s")
         .set("autonomic.monitor_interval", "1s")
         .set("autonomic.analyze_interval", "1s")
         .set("autonomic.sampling.window", "1s")
         .set("autonomic.sampling.interval", "1s")
         .set("autonomic.sampling.min_ops_per_sec", 1);
    }

    private static void await(String what, BooleanSupplier step) throws InterruptedException
    {
        long end = System.currentTimeMillis() + DEADLINE_MILLIS;
        while (System.currentTimeMillis() < end)
        {
            if (step.getAsBoolean())
                return;
            Thread.sleep(200);
        }
        throw new AssertionError("timed out waiting for " + what);
    }

    private static Object[][] diagnoses(Cluster cluster, String type)
    {
        return cluster.coordinator(1).execute("SELECT subject, state, summary, evidence FROM system_autonomic.diagnoses WHERE type = '" + type + "' ALLOW FILTERING",
                                              ConsistencyLevel.ONE);
    }

    @Test
    public void hotPartitionIsDiagnosedAndNamed() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).withConfig(HotPartitionDiagnosisTest::fast).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".hot (k text PRIMARY KEY, v int)");
            String insert = "INSERT INTO " + KEYSPACE + ".hot (k, v) VALUES (?, ?)";
            int[] i = { 0 };
            Object[][][] found = new Object[1][][];
            await("a hot_partition diagnosis", () -> {
                // 95% of the writes go to one key
                for (int n = 0; n < 40; n++)
                    cluster.coordinator(1).execute(insert, ConsistencyLevel.ONE, n % 20 == 0 ? "cold" + (i[0]++) : "hotkey", n);
                found[0] = diagnoses(cluster, "hot_partition");
                return found[0].length > 0;
            });
            Object[] row = found[0][0];
            String hex = ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes("hotkey"));
            assertEquals("PARTITION:" + KEYSPACE + ".hot:" + hex, row[0]);
            assertTrue("summary should name the key: " + row[2], ((String) row[2]).contains("hotkey"));
            @SuppressWarnings("unchecked")
            Map<String, String> evidence = (Map<String, String>) row[3];
            assertEquals(hex, evidence.get("key_hex"));
            assertNotNull(evidence.get("partition.write_share"));

            NodeToolResult history = cluster.get(1).nodetoolResult("autonomic", "history", "--diagnoses");
            history.asserts().success().stdoutContains("type=hot_partition").stdoutContains(hex).stdoutContains("hotkey");
        }
    }

    @Test
    public void tombstoneHeavyReadsAreDiagnosed() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).withConfig(HotPartitionDiagnosisTest::fast).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tomb (k int, c int, v int, PRIMARY KEY (k, c))");
            for (int c = 0; c < 1500; c++)
                cluster.coordinator(1).execute("DELETE FROM " + KEYSPACE + ".tomb WHERE k = 1 AND c = ?", ConsistencyLevel.ONE, c);
            Object[][][] found = new Object[1][][];
            await("a tombstone_overload diagnosis", () -> {
                for (int n = 0; n < 5; n++)
                    cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tomb WHERE k = 1", ConsistencyLevel.ONE);
                found[0] = diagnoses(cluster, "tombstone_overload");
                return found[0].length > 0;
            });
            assertEquals("TABLE:" + KEYSPACE + ".tomb", found[0][0][0]);
        }
    }
}
