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

import org.junit.Test;

import org.apache.cassandra.autonomic.execute.actuators.PartitionSplitActuator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * T6.13 style, ADR-035: three nodes, replication factor 3, owner node 1. Before the split, node 1 coordinates a write
 * of the key whose mutation to node 3 is dropped by a message filter; it is acknowledged by nodes 1 and 2, and node 1
 * keeps a hint for node 3 with handoff paused. The split then waits in ANNOUNCED, well past the drain of ADR-028,
 * because node 1 does not report its hints. With handoff resumed the hint is delivered first and the split goes on to
 * STEADY, the row readable. With handoff kept paused, the migration aborts on {@code hint_drain_timeout} with a
 * diagnosis naming node 1, and nothing is lost.
 */
public class SplitPendingHintsTest extends SplitFailureSupport
{
    private static final int ROWS = 100;
    private static final int HINTED = 1001;

    /** The key, its model, and a hint for node 3 on node 1 that is not delivered. */
    private static Model hintForNode3(Cluster cluster, String table) throws InterruptedException
    {
        IInvokableInstance n1 = cluster.get(1);
        createTables(cluster, 3, table);
        int k = keyWithReplicas(cluster, table, true, true, true);
        arm(n1);
        Model model = new Model(cluster, table, k, 1, 2, 3);
        model.initialData(ROWS);
        n1.runOnInstance(() -> HintsService.instance.pauseDispatch());
        long hintsBefore = n1.callOnInstance(() -> StorageMetrics.totalHints.getCount());
        cluster.filters().verbs(Verb.MUTATION_REQ.id).from(1).to(3).drop().on();
        assertTrue("acknowledged by nodes 1 and 2", model.write(1, HINTED, 11, ConsistencyLevel.ONE));
        await("hints for node 3 on node 1", () -> n1.callOnInstance(() -> StorageMetrics.totalHints.getCount()) >= hintsBefore + 2);
        cluster.filters().reset();
        assertTrue(inBase(cluster.get(2), table, k, HINTED));
        assertFalse(inBase(cluster.get(3), table, k, HINTED));
        n1.runOnInstance(() -> HintsService.instance.flushAndFsyncBlockingly());
        assertTrue("node 1 holds a hint file for node 3", hintFilesFor(n1, hostId(cluster.get(3))) > 0);
        return model;
    }

    private static String hostId(IInvokableInstance node)
    {
        return node.callOnInstance(() -> ClusterMetadata.current().myNodeId().toUUID().toString());
    }

    /** Hints files in the node's hints directory for the host, the open one included. */
    private static int hintFilesFor(IInvokableInstance node, String host)
    {
        return node.callOnInstance(() -> {
            int n = 0;
            for (String name : DatabaseDescriptor.getHintsDirectory().listNamesUnchecked())
                if (name.startsWith(host) && name.endsWith(".hints"))
                    n++;
            return n;
        });
    }

    @Test
    public void splitWaitsInAnnouncedUntilTheHintIsDelivered() throws Throwable
    {
        try (Cluster cluster = start(3, config(nodes(1), settings(), settings())))
        {
            IInvokableInstance n1 = cluster.get(1);
            String table = "hinted";
            Model model = hintForNode3(cluster, table);
            int k = model.key;
            model.startRecordingPhases();
            try
            {
                String id = split(n1, table, k, 4);
                awaitPhase(n1, table, k, "ANNOUNCED");
                await("nodes 2 and 3 reported, node 1 did not", () -> layoutStatus(n1).contains("waits for hint reports (ADR-035), 2 of 3 nodes reported"));
                // the drain of ADR-028 is 3 s here: 10 s is well past it
                assertStays("held in ANNOUNCED while node 1 holds the hint", 50, () -> "ANNOUNCED".equals(phaseOf(n1, table, k)));
                assertFalse(inBase(cluster.get(3), table, k, HINTED));

                n1.runOnInstance(() -> HintsService.instance.resumeDispatch());
                String host3 = hostId(cluster.get(3));
                await("past ANNOUNCED", () -> !"ANNOUNCED".equals(phaseOf(n1, table, k)));
                assertEquals("the hint was delivered before DUAL_WRITE", 0, hintFilesFor(n1, host3));
                // the base partition tombstone comes in STEADY only
                if (!"STEADY".equals(phaseOf(n1, table, k)))
                    assertTrue("the hint is in node 3's base table", inBase(cluster.get(3), table, k, HINTED));

                awaitPhase(n1, table, k, "STEADY");
                await("the action committed", () -> history(n1).contains("state=COMMITTED"));
                for (int node = 1; node <= 3; node++)
                    assertEquals("the hinted row, read through node " + node, Integer.valueOf(11), readV(cluster, node, table, k, HINTED));
                model.check("hints, STEADY");
                System.out.println("ADR-035 delivered: trail\n" + trail(n1, id));
            }
            finally
            {
                model.stopRecordingPhases();
            }
        }
    }

    @Test
    public void splitAbortsWhenHandoffStaysPaused() throws Throwable
    {
        try (Cluster cluster = start(3, config(nodes(1), settings("hint_drain_timeout", "10s"), settings())))
        {
            IInvokableInstance n1 = cluster.get(1);
            String table = "paused";
            Model model = hintForNode3(cluster, table);
            int k = model.key;
            String id = split(n1, table, k, 4);
            await("the migration rolled back", () -> history(n1).contains("state=ROLLED_BACK"));
            assertEquals("absent", phaseOf(n1, table, k));
            String trail = trail(n1, id);
            System.out.println("ADR-035 paused: trail\n" + trail);
            assertTrue(trail, trail.contains(PartitionSplitActuator.HINTS_NOT_DRAINED + ": hint_drain_timeout 10s passed in ANNOUNCED; node(s) "));
            assertTrue("node 1 is named", trail.contains("127.0.0.1"));
            assertFalse("nodes that reported are not", trail.contains("127.0.0.2") || trail.contains("127.0.0.3"));
            assertFalse("the key never reached DUAL_WRITE", trail.contains("DUAL_WRITE"));

            // nothing is lost: the hint is still there and is delivered to the base table, where the key now lives
            n1.runOnInstance(() -> HintsService.instance.resumeDispatch());
            await("the hint delivered to node 3", () -> inBase(cluster.get(3), table, k, HINTED));
            for (int node = 1; node <= 3; node++)
                assertEquals("the hinted row, read through node " + node, Integer.valueOf(11), readV(cluster, node, table, k, HINTED));
            model.check("hints, aborted");
        }
    }
}
