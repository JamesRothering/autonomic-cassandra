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

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.autonomic.coord.SetClusterActionsEnabled;
import org.apache.cassandra.autonomic.layout.EpochWatermarks;
import org.apache.cassandra.autonomic.layout.LayoutService;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * T6.6: the all-node epoch watermark (rule O1) on a real cluster. Nodes 2 and 3 run with the layer disabled and still
 * answer (FC1, A2). Note that "answers and routes" (A2) is covered here only as far as "answers": that a node which
 * answers also routes by the registry is T6.3's to establish.
 */
public class LayoutServiceTest extends TestBaseImpl
{
    private static long commit(IInvokableInstance node)
    {
        // any committed transformation advances the epoch; this one is harmless
        return node.callOnInstance(() -> {
            ClusterMetadataService.instance().commit(new SetClusterActionsEnabled(false).wrap());
            return ClusterMetadata.current().epoch.getEpoch();
        });
    }

    private static boolean allAtLeast(IInvokableInstance node, long epoch)
    {
        return node.callOnInstance(() -> EpochWatermarks.allAtLeast(Epoch.create(epoch)));
    }

    @Test
    public void watermarkNeedsEveryNode() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP).set("autonomic.enabled", c.num() == 1))
                                           .start()))
        {
            IInvokableInstance n1 = cluster.get(1);
            IInvokableInstance n2 = cluster.get(2);
            IInvokableInstance n3 = cluster.get(3);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            assertFalse(n3.callOnInstance(() -> AutonomicService.instance.isEnabled()));
            // FC1: the data-plane service is started although the layer is disabled
            assertTrue(n3.callOnInstance(() -> LayoutService.instance != null));

            // all nodes caught up: true, including nodes with the layer disabled
            long e0 = ClusterUtils.getCurrentEpoch(n1).getEpoch();
            assertTrue(allAtLeast(n1, e0));
            assertTrue("a disabled node answers", allAtLeast(n3, e0));

            // isolate node 3 from the log
            cluster.filters().verbs(Verb.TCM_REPLICATION.id, Verb.TCM_NOTIFY_REQ.id,
                                    Verb.TCM_FETCH_CMS_LOG_RSP.id, Verb.TCM_FETCH_PEER_LOG_RSP.id).to(3).drop().on();
            long e1 = commit(n1);
            assertTrue(e1 > e0);
            long wait = System.nanoTime() + 30_000_000_000L;
            while (ClusterUtils.getCurrentEpoch(n2).getEpoch() < e1 && System.nanoTime() < wait)
                Thread.sleep(100);
            assertTrue("node 2 follows the log", ClusterUtils.getCurrentEpoch(n2).getEpoch() >= e1);
            assertTrue("node 3 has not seen it", ClusterUtils.getCurrentEpoch(n3).getEpoch() < e1);
            assertFalse("no quorum fallback: two of three is not enough", allAtLeast(n1, e1));
            assertFalse(allAtLeast(n2, e1));
            assertTrue("the old epoch is still acknowledged by all", allAtLeast(n1, e0));

            // remove the isolation; node 3 catches up on the next request
            cluster.filters().reset();
            long deadline = System.nanoTime() + 60_000_000_000L;
            boolean ok = false;
            while (!ok && System.nanoTime() < deadline)
            {
                ok = allAtLeast(n1, e1);
                if (!ok)
                    Thread.sleep(500);
            }
            assertTrue(ok);

            // A1: a restarted node never reports a lower epoch than before
            long before = ClusterUtils.getCurrentEpoch(n2).getEpoch();
            ClusterUtils.restartUnchecked(n2);
            assertTrue(ClusterUtils.getCurrentEpoch(n2).getEpoch() >= before);
            assertTrue(allAtLeast(n1, before));
            assertTrue(allAtLeast(n2, before));
            ClusterUtils.restartUnchecked(n3);
            assertTrue(allAtLeast(n1, e1));
        }
    }
}
