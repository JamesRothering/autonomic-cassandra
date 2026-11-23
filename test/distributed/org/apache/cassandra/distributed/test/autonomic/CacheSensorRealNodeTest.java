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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** The cache sensor reports the key cache's hit rate of a real node serving repeated reads (ADR-046). */
public class CacheSensorRealNodeTest extends TestBaseImpl
{
    private static final long DEADLINE_MILLIS = 60_000;

    @Test
    public void keyCacheHitRateIsEmitted() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).withConfig(c -> c.set("autonomic.enabled", true)
                                                                      .set("autonomic.startup_grace", "1s")
                                                                      .set("autonomic.monitor_interval", "1s")
                                                                      .set("autonomic.analyze_interval", "1s")
                                                                      .set("autonomic.cache.min_requests_per_sec", 1)).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".c (k int PRIMARY KEY, v int)");
            for (int k = 0; k < 20; k++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".c (k, v) VALUES (?, ?)", ConsistencyLevel.ONE, k, k);
            cluster.get(1).flush(KEYSPACE);

            double hitRate = -1;
            long end = System.currentTimeMillis() + DEADLINE_MILLIS;
            while (System.currentTimeMillis() < end && hitRate < 0)
            {
                for (int k = 0; k < 20; k++)
                    cluster.coordinator(1).execute("SELECT v FROM " + KEYSPACE + ".c WHERE k = ?", ConsistencyLevel.ONE, k);
                Object[][] rows = cluster.coordinator(1).execute("SELECT value FROM system_views.autonomic_signals WHERE signal = 'cache.key_hit_rate'",
                                                                 ConsistencyLevel.ONE);
                if (rows.length > 0)
                    hitRate = (Double) rows[0][0];
                else
                    Thread.sleep(100);
            }
            assertTrue("no cache.key_hit_rate signal", hitRate >= 0);
            assertTrue("hit rate " + hitRate, hitRate <= 1);
            // repeated reads of the same 20 partitions are served from the key cache
            assertTrue("hit rate " + hitRate, hitRate > 0.5);
            assertEquals(1, cluster.coordinator(1).execute("SELECT value FROM system_views.autonomic_signals WHERE signal = 'cache.key_requests_per_sec'",
                                                           ConsistencyLevel.ONE).length);
        }
    }
}
