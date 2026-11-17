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

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.autonomic.layout.LayoutProfiler;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.Schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * T6.11 on one node: the profiler counts the shapes of the operations on a watched key (named-row reads, slice reads,
 * row writes, range and partition deletes, {@code cas}, range scans of the table), times the reads and writes, and
 * leaves the data as it was; and while no key is watched the hooks never go on to look at a watch (FC8).
 */
public class LayoutProfilerTest extends TestBaseImpl
{
    private static final int K = 7;

    private static long[] shape(IInvokableInstance node)
    {
        return node.callOnInstance(() -> {
            ByteBuffer key = Int32Type.instance.decompose(K);
            LayoutProfiler.Watch watch = LayoutProfiler.get(Schema.instance.getTableMetadata(KEYSPACE, "t").id, key);
            LayoutProfiler.Shape s = watch.shape();
            return new long[]{ s.namedReads, s.sliceReads, s.rowWrites, s.deletes, s.cas, s.scans, s.timedReads, s.timedWrites, s.readErrors + s.writeErrors };
        });
    }

    @Test
    public void countsShapesForAWatchedKeyAndCostsNothingWhenNotWatching() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            IInvokableInstance node = cluster.get(1);
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (p int, c int, v int, PRIMARY KEY (p, c))");
            String t = KEYSPACE + ".t";

            // nothing is watched: the hooks do not look at the profiler at all
            assertTrue(node.callOnInstance(() -> LayoutProfiler.active() == null));
            long before = node.callOnInstance(LayoutProfiler::hookCalls);
            for (int c = 0; c < 20; c++)
                cluster.coordinator(1).execute("INSERT INTO " + t + " (p, c, v) VALUES (" + K + ", ?, ?)", ConsistencyLevel.ONE, c, c);
            for (int c = 0; c < 20; c++)
                cluster.coordinator(1).execute("SELECT v FROM " + t + " WHERE p = " + K + " AND c = ?", ConsistencyLevel.ONE, c);
            cluster.coordinator(1).execute("SELECT * FROM " + t, ConsistencyLevel.ONE);
            assertEquals("FC8: no work was done for 41 requests", before, (long) node.callOnInstance(LayoutProfiler::hookCalls));
            assertEquals(0L, before);

            node.runOnInstance(() -> LayoutProfiler.watch(Schema.instance.getTableMetadata(KEYSPACE, "t").id, Int32Type.instance.decompose(K), 600_000));
            for (int c = 100; c < 110; c++)
                cluster.coordinator(1).execute("INSERT INTO " + t + " (p, c, v) VALUES (" + K + ", ?, ?)", ConsistencyLevel.ONE, c, c);
            for (int c = 0; c < 5; c++)
                assertEquals(1, cluster.coordinator(1).execute("SELECT v FROM " + t + " WHERE p = " + K + " AND c = ?", ConsistencyLevel.ONE, c).length);
            assertEquals(30, cluster.coordinator(1).execute("SELECT v FROM " + t + " WHERE p = " + K, ConsistencyLevel.ONE).length);
            assertEquals(30, cluster.coordinator(1).execute("SELECT v FROM " + t + " WHERE p = " + K + " AND c >= 0", ConsistencyLevel.ONE).length);
            // another key of the same table is not counted
            cluster.coordinator(1).execute("INSERT INTO " + t + " (p, c, v) VALUES (8, 1, 1)", ConsistencyLevel.ONE);
            cluster.coordinator(1).execute("SELECT v FROM " + t + " WHERE p = 8", ConsistencyLevel.ONE);
            cluster.coordinator(1).execute("SELECT * FROM " + t, ConsistencyLevel.ONE);
            cluster.coordinator(1).execute("DELETE FROM " + t + " WHERE p = " + K + " AND c >= 100 AND c < 105", ConsistencyLevel.ONE);
            assertEquals(25, cluster.coordinator(1).execute("SELECT v FROM " + t + " WHERE p = " + K, ConsistencyLevel.ONE).length);
            cluster.coordinator(1).execute("DELETE FROM " + t + " WHERE p = " + K, ConsistencyLevel.ONE);
            assertEquals(0, cluster.coordinator(1).execute("SELECT v FROM " + t + " WHERE p = " + K, ConsistencyLevel.ONE).length);

            long[] s = shape(node);
            assertEquals("named-row reads", 5, s[0]);
            assertEquals("slice reads", 2 + 1 + 1, s[1]);
            assertEquals("row writes", 10, s[2]);
            assertEquals("range and partition deletes", 2, s[3]);
            assertEquals("cas", 0, s[4]);
            assertEquals("range scans of the table", 1, s[5]);
            assertEquals("timed reads", 5 + 4, s[6]);
            assertEquals("timed writes", 12, s[7]);
            assertEquals("errors", 0, s[8]);

            cluster.coordinator(1).execute("INSERT INTO " + t + " (p, c, v) VALUES (" + K + ", 500, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
            assertEquals("cas", 1, shape(node)[4]);

            node.runOnInstance(() -> LayoutProfiler.unwatch(Schema.instance.getTableMetadata(KEYSPACE, "t").id, Int32Type.instance.decompose(K)));
            assertTrue(node.callOnInstance(() -> LayoutProfiler.active() == null));
            long hooks = node.callOnInstance(LayoutProfiler::hookCalls);
            cluster.coordinator(1).execute("SELECT * FROM " + t + " WHERE p = 8", ConsistencyLevel.ONE);
            assertEquals(hooks, (long) node.callOnInstance(LayoutProfiler::hookCalls));
        }
    }
}
