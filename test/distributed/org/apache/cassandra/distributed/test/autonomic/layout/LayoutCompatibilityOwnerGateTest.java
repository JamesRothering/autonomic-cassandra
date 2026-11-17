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

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * T6.12, rule O1 with the real actuator: node 2's log delivery is held back before the split starts, so it never
 * learns of ANNOUNCED. The owner must stay in ANNOUNCED (it polls every 200 ms) for as long as the node is behind, and
 * must go on to DUAL_WRITE once the node has caught up.
 */
public class LayoutCompatibilityOwnerGateTest extends TestBaseImpl
{
    private static final String KS = "gate_ks";

    private static String phase(IInvokableInstance node)
    {
        Object[][] rows = node.executeInternal("SELECT phase FROM system_views.autonomic_layouts WHERE keyspace_name = ?", KS);
        return rows.length == 0 ? null : (String) rows[0][0];
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (!condition.getAsBoolean())
        {
            if (System.nanoTime() > deadline)
                throw new AssertionError("timed out waiting for " + what);
            Thread.sleep(100);
        }
    }

    @Test
    public void theOwnerDoesNotAdvanceWhileANodeIsBehind() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).withConfig(PartitionSplitEndToEndTest::configure).start()))
        {
            IInvokableInstance n1 = cluster.get(1);
            cluster.schemaChange("CREATE KEYSPACE " + KS + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + KS + ".t (id int, seq int, v int, PRIMARY KEY (id, seq))");
            for (int seq = 0; seq < 200; seq++)
                cluster.coordinator(1).execute("INSERT INTO " + KS + ".t (id, seq, v) VALUES (1, ?, ?)", org.apache.cassandra.distributed.api.ConsistencyLevel.ALL, seq, seq);
            await("the layer to be armed", () -> n1.nodetoolResult("autonomic", "status").getStdout().contains("Armed: true"));
            n1.nodetoolResult("autonomic", "cluster", "enable", "--yes").asserts().success();

            cluster.filters().verbs(Verb.TCM_REPLICATION.id, Verb.TCM_NOTIFY_REQ.id,
                                    Verb.TCM_FETCH_CMS_LOG_RSP.id, Verb.TCM_FETCH_PEER_LOG_RSP.id).to(2).drop().on();
            String keyHex = ByteBufferUtil.bytesToHex(Int32Type.instance.decompose(1));
            org.apache.cassandra.distributed.api.NodeToolResult split = n1.nodetoolResult("autonomic", "layout", "split", KS, "t", keyHex, "--buckets", "4");
            split.asserts().success();
            String out = split.getStdout();
            assertTrue(out, out.contains("state=PROPOSED"));
            String id = out.substring(out.indexOf("id=") + 3, out.indexOf("id=") + 3 + 36);
            n1.nodetoolResult("autonomic", "approve", id).asserts().success();

            await("ANNOUNCED", () -> "ANNOUNCED".equals(phase(n1)));
            // many ack polls: the owner does not move while node 2 has not answered with the epoch of ANNOUNCED
            Thread.sleep(6000);
            assertEquals("O1: the owner advanced while a node was behind", "ANNOUNCED", phase(n1));

            cluster.filters().reset();
            // the next epoch request makes node 2 catch up; the owner polls it
            await("DUAL_WRITE once node 2 has caught up", () -> {
                String p = phase(n1);
                return p != null && !"ANNOUNCED".equals(p);
            });
        }
    }
}
