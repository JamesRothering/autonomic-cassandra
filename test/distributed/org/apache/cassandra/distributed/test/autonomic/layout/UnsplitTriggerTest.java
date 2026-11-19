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

import java.util.Map;
import java.util.function.Consumer;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * T6.17g: on one node, {@code nodetool autonomic layout unsplit} of a split key creates a proposed
 * {@code partition_unsplit} action that stays proposed (manual approval); a second call returns the same proposal; a key
 * that is not split is not proposed.
 */
public class UnsplitTriggerTest extends SplitFailureSupport
{
    private static final int KEY = 1;

    @Test
    public void theCommandCreatesAProposedAction() throws Throwable
    {
        Consumer<IInstanceConfig> base = config(nodes(1), settings("reclaim_window", "60s"), settings());
        Consumer<IInstanceConfig> config = c -> {
            base.accept(c);
            if (c.num() == 1)
                c.set("autonomic.actuators", Map.of("partition_split", Map.of("enabled", true), "partition_unsplit", Map.of("enabled", true)));
        };
        try (Cluster cluster = start(1, config))
        {
            IInvokableInstance n1 = cluster.get(1);
            String table = "trigger";
            createTables(cluster, 1, table);
            for (int c = 0; c < 50; c++)
                cluster.coordinator(1).execute("INSERT INTO " + KS + '.' + table + " (p, c, v) VALUES (?, ?, ?)", ConsistencyLevel.ALL, KEY, c, c);
            arm(n1);

            // a key that is not split is refused
            NodeToolResult refused = n1.nodetoolResult("autonomic", "layout", "unsplit", KS, table, hex(KEY));
            assertTrue(refused.getStdout() + refused.getStderr(), !refused.getStdout().contains("state=PROPOSED"));

            split(n1, table, KEY, 4);
            awaitPhase(n1, table, KEY, "STEADY");
            await("the split to be released", () -> "RESIDUE".equals(layoutRow(n1)));

            NodeToolResult result = n1.nodetoolResult("autonomic", "layout", "unsplit", KS, table, hex(KEY));
            result.asserts().success();
            String out = result.getStdout();
            assertTrue(out, out.contains("state=PROPOSED"));
            assertTrue(out, out.contains("partition_unsplit"));
            String id = out.substring(out.indexOf("id=") + 3, out.indexOf("id=") + 3 + 36);

            // manual approval: it stays proposed, and asking again returns it
            Thread.sleep(2000);
            NodeToolResult again = n1.nodetoolResult("autonomic", "layout", "unsplit", KS, table, hex(KEY), "--when-ready");
            again.asserts().success();
            assertTrue(again.getStdout(), again.getStdout().contains(id));
            assertEquals("STEADY", phaseOf(n1, table, KEY));
        }
    }

    private static String layoutRow(IInvokableInstance node)
    {
        Object[][] rows = node.executeInternal("SELECT base_state FROM system_views.autonomic_layouts WHERE keyspace_name = ?", KS);
        return rows.length == 0 || rows[0][0] == null ? null : String.valueOf(rows[0][0]);
    }
}
