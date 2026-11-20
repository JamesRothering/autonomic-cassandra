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

import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.localState;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * T7.7 (T7.2 finding 3, ADR-038): an expired claim whose owner is the preferred proposer but paused is still taken
 * over. Node 1 owns a claim in phase P1 and dies without releasing it; nobody takes it over while node 1 is down
 * (nodes 2 and 3 are paused, standing in for peers whose failure detector has not convicted it yet). Node 1 restarts
 * past its lease, records its action FAILED with CLAIM_LOST and pauses itself. Nodes 2 and 3 see node 1 alive, so
 * node 1 is the preferred proposer of every node. Node 2, the next node in id order, is resumed and takes the
 * expired claim over (one {@code lease_safety_margin} after expiry); its actuator's {@code recover} undoes the
 * action. Node 1 and node 3 call nothing.
 *
 * <p>Three nodes with the CMS on all three, as in {@link OwnerKilledFaultTest}: the claim must outlive its owner.
 */
public class ExpiredClaimTakeoverFaultTest extends TestBaseImpl
{
    private static final String ACTUATOR = "fault_cluster";

    private static void startLayer(IInvokableInstance node) throws InterruptedException
    {
        node.runOnInstance(() -> FaultSupport.inStartClusterLayer(ACTUATOR));
        await("layer armed", () -> node.callOnInstance(() -> AutonomicService.instance.isArmed()));
    }

    private static String recentFinalState(IInvokableInstance node)
    {
        return node.callOnInstance(() -> {
            for (Map<String, String> r : AutonomicService.instance.getClusterRecentActions())
                if (ACTUATOR.equals(r.get("actuator")))
                    return r.get("final_state");
            return null;
        });
    }

    @Test
    public void anExpiredClaimOfAPausedPreferredProposerIsTakenOver() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3).withConfig(c -> FaultSupport.clusterConfig(c, "20s")).start()))
        {
            IInvokableInstance owner = cluster.get(1);
            owner.nodetoolResult("cms", "reconfigure", "3").asserts().success();
            await("a CMS of three", () -> owner.callOnInstance(() -> ClusterMetadata.current().fullCMSMembers().size()) == 3);
            for (int i = 1; i <= 3; i++)
                startLayer(cluster.get(i));
            owner.runOnInstance(() -> AutonomicService.instance.enableClusterActions(true));
            for (int i = 1; i <= 3; i++)
            {
                IInvokableInstance node = cluster.get(i);
                await("cluster actions enabled", () -> node.callOnInstance(() -> AutonomicService.instance.isClusterActionsEnabled()));
            }
            int ownerId = owner.callOnInstance(() -> ClusterMetadata.current().myNodeId().id());

            String target = Subject.keyspace("fault_a").asString();
            String held = owner.callOnInstance(() -> FaultSupport.inPropose(ACTUATOR, "CLUSTER", target, Map.of(), 1000, 1000));
            assertTrue(held, held.startsWith("PROPOSED|null|"));
            String id = held.substring(held.lastIndexOf('|') + 1);
            owner.runOnInstance(() -> AutonomicService.instance.approve(id));
            await("the action in P1", () -> cluster.get(2).callOnInstance(() -> FaultSupport.inClaims())
                                                   .equals(List.of("MAINTENANCE|" + id + '|' + ownerId + "|P1")));

            // nobody takes the claim over while its owner is down
            for (int i = 2; i <= 3; i++)
                cluster.get(i).runOnInstance(() -> AutonomicService.instance.pause("T7.7"));
            ClusterUtils.stopAbrupt(cluster, owner);
            await("the lease to expire", () -> cluster.get(2).callOnInstance(() -> {
                List<Map<String, String>> claims = AutonomicService.instance.getClusterClaims();
                return claims.size() == 1 && "true".equals(claims.get(0).get("expired"));
            }));

            // the owner is back past its lease: its action is lost and its layer pauses
            owner.startup();
            startLayer(owner);
            await("restart recovery", () -> localState(cluster, 1, "inflight/" + id) == null);
            Map<String, String> status = owner.callOnInstance(() -> AutonomicService.instance.getStatus());
            assertEquals(status.toString(), "PAUSED", status.get("state"));
            assertEquals(status.toString(), "ACTION_FAILED", status.get("pause_reason"));

            // its peers see it alive, so node 1 stays the preferred proposer of all three; node 2 runs again. Node 3
            // stays paused until the takeover: once both are past their turn, either may win the takeover commit
            InetSocketAddress address = owner.broadcastAddress();
            for (int i = 2; i <= 3; i++)
            {
                IInvokableInstance node = cluster.get(i);
                await("node " + i + " to see node 1 alive", () -> node.callOnInstance(() -> FaultSupport.inAlive(address)));
            }
            for (int i = 1; i <= 3; i++)
                assertEquals("node " + i, String.valueOf(i == 1), cluster.get(i).callOnInstance(() -> AutonomicService.instance.getClusterStatus().get("preferred_proposer")));
            cluster.get(2).runOnInstance(() -> AutonomicService.instance.resume());

            // in flight restored by the next node in id order: node 2 takes over and undoes the action
            await("the takeover", () -> "ROLLED_BACK".equals(recentFinalState(cluster.get(2))));
            cluster.get(3).runOnInstance(() -> AutonomicService.instance.resume());
            assertEquals(List.of("recover:RECOVERING:0"), cluster.get(2).callOnInstance(() -> FaultSupport.inClusterCalls("recover")));
            assertEquals(List.of(), cluster.get(3).callOnInstance(() -> FaultSupport.inClusterCalls("recover")));
            for (int i = 1; i <= 3; i++)
            {
                IInvokableInstance node = cluster.get(i);
                assertEquals("node " + i, List.of(), node.callOnInstance(() -> FaultSupport.inClusterCalls("step")));
                await("node " + i + " to see the lane free", () -> node.callOnInstance(() -> FaultSupport.inClaims()).isEmpty());
            }
            assertEquals(List.of(), owner.callOnInstance(() -> FaultSupport.inClusterCalls("recover")));
            assertEquals(List.of(), owner.callOnInstance(() -> FaultSupport.inClusterCalls("abort")));
            assertEquals("PAUSED", owner.callOnInstance(() -> AutonomicService.instance.getState()));
            List<Map<String, String>> history = cluster.get(2).callOnInstance(() -> AutonomicService.instance.getHistory(10));
            assertTrue(history.toString(), history.stream().anyMatch(h -> "ROLLED_BACK".equals(h.get("state")) && "RECOVERY".equals(h.get("state_reason"))
                                                                          && "takeover".equals(h.get("policy")) && h.get("params").contains(id)
                                                                          && h.get("params").contains("expired_owner=" + ownerId)));
        }
    }
}
