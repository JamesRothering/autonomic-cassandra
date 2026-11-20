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
package org.apache.cassandra.distributed.test.autonomic.fault;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.events;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.localState;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * T7.2 fault 3, the CMS unreachable during a cluster-ledger claim (overview 6.2, 6.4, 7.5). Node 1 is the CMS; node 2
 * is cut off from it, sees it down, so it is the preferred proposer and claims a lane for an approved cluster action.
 * <ol>
 * <li>Request lost: the claim never reaches the CMS. The commit gives up at {@code action_step_timeout}; the action is
 *     REJECTED (CLAIM_REJECTED) with its {@code inflight} key removed, the ledger has no claim, the actuator never
 *     steps.</li>
 * <li>Response lost (an ambiguous commit): the CMS applies the claim but node 2 never hears of it. The action is
 *     REJECTED the same way; the claim that landed regardless is released as ABANDONED by node 2 once it catches up
 *     with the log, before its lease could expire, and nothing ran under it.</li>
 * </ol>
 * Two nodes suffice: the proposer must be a non-CMS node that does not see the CMS alive, which is exactly what a
 * partition from a single-member CMS gives.
 */
public class CmsUnreachableClaimFaultTest extends TestBaseImpl
{
    private static final String ACTUATOR = "fault_cluster";

    private static String propose(IInvokableInstance node, String target)
    {
        String held = node.callOnInstance(() -> FaultSupport.inPropose(ACTUATOR, "CLUSTER", target, Map.of(), 1000, 1000));
        assertTrue(held, held.startsWith("PROPOSED|null|"));
        String id = held.substring(held.lastIndexOf('|') + 1);
        // cluster actions need an operator's approval (overview 7.1 check 13)
        node.runOnInstance(() -> AutonomicService.instance.approve(id));
        return id;
    }

    private static void partition(IInvokableInstance cms, IInvokableInstance proposer) throws InterruptedException
    {
        InetSocketAddress address = cms.broadcastAddress();
        proposer.runOnInstance(() -> FaultSupport.inConvict(address));
        await("node 2 to see the CMS down", () -> !proposer.callOnInstance(() -> FaultSupport.inAlive(address)));
    }

    private static void assertNoStep(Cluster cluster)
    {
        for (int i = 1; i <= 2; i++)
            assertEquals("node " + i, List.of(), cluster.get(i).callOnInstance(() -> FaultSupport.inClusterCalls("step")));
    }

    @Test
    public void aClaimTheCmsCannotConfirmChangesNothing() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).withConfig(c -> FaultSupport.clusterConfig(c, "60s")).start()))
        {
            IInvokableInstance cms = cluster.get(1);
            IInvokableInstance proposer = cluster.get(2);
            for (IInvokableInstance node : List.of(cms, proposer))
                node.runOnInstance(() -> FaultSupport.inStartClusterLayer(ACTUATOR));
            for (IInvokableInstance node : List.of(cms, proposer))
                await("layer armed", () -> node.callOnInstance(() -> AutonomicService.instance.isArmed()));
            cms.runOnInstance(() -> AutonomicService.instance.enableClusterActions(true));
            ClusterUtils.waitForCMSToQuiesce(cluster, cms);
            await("node 2 to see cluster actions enabled", () -> proposer.callOnInstance(() -> AutonomicService.instance.isClusterActionsEnabled()));
            InetSocketAddress cmsAddress = cms.broadcastAddress();

            // ---- 1. the claim never reaches the CMS ----
            IMessageFilters.Filter in = cluster.filters().allVerbs().from(1).to(2).drop();
            IMessageFilters.Filter out = cluster.filters().allVerbs().from(2).to(1).drop();
            partition(cms, proposer);
            String lost = propose(proposer, Subject.keyspace("fault_a").asString());
            await("the claim to give up", () -> String.valueOf(proposer.callOnInstance(() -> FaultSupport.inMemoryRecord(UUID.fromString(lost))))
                                                       .startsWith("REJECTED"));
            assertEquals("REJECTED:CLAIM_REJECTED:null", proposer.callOnInstance(() -> FaultSupport.inMemoryRecord(UUID.fromString(lost))));
            assertEquals(List.of("PROPOSED", "APPROVED", "REJECTED"), events(cluster, 2, UUID.fromString(lost)));
            assertEquals(null, localState(cluster, 2, "inflight/" + lost));
            assertEquals(List.of(), cms.callOnInstance(() -> FaultSupport.inClaims()));
            assertEquals(null, cms.callOnInstance(() -> FaultSupport.inRecentFinalState(lost)));
            assertNoStep(cluster);
            assertEquals("RUNNING", proposer.callOnInstance(() -> AutonomicService.instance.getState()));
            in.off();
            out.off();
            await("node 2 to see the CMS again", () -> proposer.callOnInstance(() -> FaultSupport.inAlive(cmsAddress)));

            // ---- 2. the CMS applies the claim, node 2 never hears back ----
            in = cluster.filters().allVerbs().from(1).to(2).drop();
            partition(cms, proposer);
            String ambiguous = propose(proposer, Subject.keyspace("fault_b").asString());
            await("the claim to give up", () -> String.valueOf(proposer.callOnInstance(() -> FaultSupport.inMemoryRecord(UUID.fromString(ambiguous))))
                                                       .startsWith("REJECTED"));
            assertEquals("REJECTED:CLAIM_REJECTED:null", proposer.callOnInstance(() -> FaultSupport.inMemoryRecord(UUID.fromString(ambiguous))));
            int proposerId = proposer.callOnInstance(() -> ClusterMetadata.current().myNodeId().id());
            assertEquals("the claim landed in the CMS", List.of("MAINTENANCE|" + ambiguous + '|' + proposerId + "|CLAIMED"),
                         cms.callOnInstance(() -> FaultSupport.inClaims()));
            assertEquals(null, localState(cluster, 2, "inflight/" + ambiguous));
            in.off();

            // node 2 catches up, finds a claim of its own it gave up on, and releases it; nobody acted under it
            for (IInvokableInstance node : List.of(cms, proposer))
                await("the abandoned claim to be released", () -> "ABANDONED".equals(node.callOnInstance(() -> FaultSupport.inRecentFinalState(ambiguous))));
            for (IInvokableInstance node : List.of(cms, proposer))
                assertEquals(List.of(), node.callOnInstance(() -> FaultSupport.inClaims()));
            assertNoStep(cluster);
            for (IInvokableInstance node : List.of(cms, proposer))
                assertEquals(List.of(), node.callOnInstance(() -> FaultSupport.inClusterCalls("recover")));
            assertEquals("REJECTED", cluster.coordinator(2).execute("SELECT state FROM system_autonomic.actions WHERE id = ? ALLOW FILTERING",
                                                                    ConsistencyLevel.ONE, UUID.fromString(ambiguous))[0][0]);
            assertEquals("RUNNING", proposer.callOnInstance(() -> AutonomicService.instance.getState()));
        }
    }
}
