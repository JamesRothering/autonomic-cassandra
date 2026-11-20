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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.events;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.localState;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * T7.2 fault 4, the owner of a cluster action killed in the middle of it (overview 6.4, 7.6). Node 1, the preferred
 * proposer, owns a claim in phase P1 of a cluster action that has more work to do, and dies without releasing it
 * ({@link ClusterUtils#stopAbrupt}: its shutdown cannot reach anybody).
 * <ol>
 * <li>Down past the lease: node 2, now the preferred proposer, takes the expired claim over and its actuator's
 *     {@code recover} undoes the action (ROLLED_BACK). When node 1 restarts, its action is FAILED with CLAIM_LOST
 *     (recorded by the dying owner once its lease ran out, or else by restart recovery, 7.6 step 3), its layer is
 *     paused, and it calls nothing.</li>
 * <li>Back within the lease: node 1 restarts while its claim is still valid; restart recovery calls {@code recover}
 *     on node 1 itself, releases the claim as ROLLED_BACK and records reason RESTART. Nobody else touches it.</li>
 * </ol>
 * Three nodes, with the CMS on all three: the owner must be node 1 (the lowest node id is the preferred proposer),
 * and the claim can only outlive it and be taken over if the CMS keeps a quorum without it. With a single-member CMS
 * on node 1 its own shutdown would release the claim locally, and nobody could commit the takeover.
 */
public class OwnerKilledFaultTest extends TestBaseImpl
{
    private static final String ACTUATOR = "fault_cluster";

    private static String proposeAndApprove(IInvokableInstance node, String target)
    {
        String held = node.callOnInstance(() -> FaultSupport.inPropose(ACTUATOR, "CLUSTER", target, Map.of(), 1000, 1000));
        assertTrue(held, held.startsWith("PROPOSED|null|"));
        String id = held.substring(held.lastIndexOf('|') + 1);
        node.runOnInstance(() -> AutonomicService.instance.approve(id));
        return id;
    }

    private static void startLayer(IInvokableInstance node) throws InterruptedException
    {
        node.runOnInstance(() -> FaultSupport.inStartClusterLayer(ACTUATOR));
        await("layer armed", () -> node.callOnInstance(() -> AutonomicService.instance.isArmed()));
    }

    private static String actionState(Cluster cluster, int node, String id)
    {
        Object[][] rows = cluster.coordinator(node).execute("SELECT state, state_reason FROM system_autonomic.actions WHERE id = ? ALLOW FILTERING",
                                                            ConsistencyLevel.ONE, UUID.fromString(id));
        return rows.length == 0 ? null : rows[0][0] + ":" + rows[0][1];
    }

    /** The number of persisted {@code autonomic.action_failed} diagnoses on a node that name the action. */
    private static int actionFailedDiagnoses(Cluster cluster, int node, String actionId)
    {
        int found = 0;
        for (Object[] row : cluster.coordinator(node).execute("SELECT type, evidence FROM system_autonomic.diagnoses ALLOW FILTERING", ConsistencyLevel.ONE))
        {
            @SuppressWarnings("unchecked")
            Map<String, String> evidence = (Map<String, String>) row[1];
            if (FaultSupport.ACTION_FAILED.equals(row[0]) && evidence != null && actionId.equals(evidence.get("action")))
                found++;
        }
        return found;
    }

    /** The owner dies: nothing it sends on the way down arrives, and its peers' failure detectors convict it. */
    private static void kill(Cluster cluster, IInvokableInstance owner)
    {
        InetSocketAddress address = owner.broadcastAddress();
        ClusterUtils.stopAbrupt(cluster, owner);
        for (int i = 2; i <= 3; i++)
            cluster.get(i).runOnInstance(() -> FaultSupport.inConvict(address));
    }

    @Test
    public void aClaimWhoseOwnerDiesIsRecoveredAndNeverResumedByTheOldOwner() throws Throwable
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

            // ---- 1. the owner dies in P1 and stays down past its lease ----
            String first = proposeAndApprove(owner, Subject.keyspace("fault_a").asString());
            await("the action in P1", () -> cluster.get(2).callOnInstance(() -> FaultSupport.inClaims())
                                                   .equals(List.of("MAINTENANCE|" + first + '|' + ownerId + "|P1")));
            kill(cluster, owner);

            // in flight restored: node 2 takes the claim over and undoes the action; nobody steps it on
            await("the takeover", () -> "ROLLED_BACK".equals(cluster.get(2).callOnInstance(() -> {
                for (Map<String, String> r : AutonomicService.instance.getClusterRecentActions())
                    if (ACTUATOR.equals(r.get("actuator")))
                        return r.get("final_state");
                return null;
            })));
            for (int i = 2; i <= 3; i++)
            {
                IInvokableInstance node = cluster.get(i);
                assertEquals(List.of(), node.callOnInstance(() -> FaultSupport.inClaims()));
                assertEquals("node " + i, List.of(), node.callOnInstance(() -> FaultSupport.inClusterCalls("step")));
            }
            assertEquals(List.of("recover:RECOVERING:0"), cluster.get(2).callOnInstance(() -> FaultSupport.inClusterCalls("recover")));
            assertEquals(List.of(), cluster.get(3).callOnInstance(() -> FaultSupport.inClusterCalls("recover")));
            List<Map<String, String>> history = cluster.get(2).callOnInstance(() -> AutonomicService.instance.getHistory(10));
            assertTrue(history.toString(), history.stream().anyMatch(h -> "ROLLED_BACK".equals(h.get("state")) && "RECOVERY".equals(h.get("state_reason"))
                                                                          && "takeover".equals(h.get("policy")) && h.get("params").contains(first)
                                                                          && h.get("params").contains("expired_owner=" + ownerId)));

            // the old owner comes back: its action is FAILED (CLAIM_LOST), its layer paused, and it calls nothing. An
            // in-JVM shutdown takes long enough for the dying owner to see its lease run out and record the loss itself
            // (6.4, "owner alive but cannot commit"); otherwise restart recovery (7.6 step 3) records the same
            owner.startup();
            startLayer(owner);
            await("restart recovery", () -> localState(cluster, 1, "inflight/" + first) == null);
            assertEquals("FAILED:CLAIM_LOST", actionState(cluster, 1, first));
            assertEquals(List.of(), owner.callOnInstance(() -> FaultSupport.inClusterCalls("step")));
            assertEquals(List.of(), owner.callOnInstance(() -> FaultSupport.inClusterCalls("recover")));
            assertEquals(List.of(), owner.callOnInstance(() -> FaultSupport.inClusterCalls("abort")));
            Map<String, String> status = owner.callOnInstance(() -> AutonomicService.instance.getStatus());
            assertEquals(status.toString(), "PAUSED", status.get("state"));
            assertEquals(status.toString(), "ACTION_FAILED", status.get("pause_reason"));
            // the action_failed diagnosis raised while the owner shut down is persisted (T7.5), not lost with the
            // control thread that was stopping: it is there after the restart, and the restart sweep may have cleared it
            assertTrue("the action_failed diagnosis of " + first, actionFailedDiagnoses(cluster, 1, first) >= 1);
            // the audit trail of the action ends FAILED
            List<String> trail = events(cluster, 1, UUID.fromString(first));
            assertEquals(trail.toString(), "FAILED", trail.get(trail.size() - 1));
            assertEquals(List.of(), owner.callOnInstance(() -> FaultSupport.inClaims()));

            // ---- 2. the owner dies in P1 and is back within its lease ----
            owner.runOnInstance(() -> {
                AutonomicService.instance.resume();
                DatabaseDescriptor.getAutonomicConfig().coordination.lease = new DurationSpec.IntSecondsBound("180s");
            });
            String second = proposeAndApprove(owner, Subject.keyspace("fault_b").asString());
            await("the action in P1", () -> cluster.get(2).callOnInstance(() -> FaultSupport.inClaims())
                                                   .equals(List.of("MAINTENANCE|" + second + '|' + ownerId + "|P1")));
            kill(cluster, owner);
            owner.startup();
            owner.runOnInstance(() -> DatabaseDescriptor.getAutonomicConfig().coordination.lease = new DurationSpec.IntSecondsBound("180s"));
            startLayer(owner);

            // in flight restored by the owner itself: recover, release ROLLED_BACK, reason RESTART
            await("the owner's recovery", () -> "ROLLED_BACK".equals(owner.callOnInstance(() -> FaultSupport.inRecentFinalState(second))));
            await("the record", () -> "ROLLED_BACK:RESTART".equals(actionState(cluster, 1, second)));
            List<String> recovered = owner.callOnInstance(() -> FaultSupport.inClusterCalls("recover"));
            assertEquals(recovered.toString(), 1, recovered.size());
            assertTrue(recovered.toString(), recovered.get(0).startsWith("recover:P1:"));
            assertEquals(List.of(), owner.callOnInstance(() -> FaultSupport.inClusterCalls("step")));
            for (int i = 1; i <= 3; i++)
                assertEquals(List.of(), cluster.get(i).callOnInstance(() -> FaultSupport.inClaims()));
            assertEquals("no second takeover", List.of("recover:RECOVERING:0"), cluster.get(2).callOnInstance(() -> FaultSupport.inClusterCalls("recover")));
            assertEquals(List.of(), cluster.get(3).callOnInstance(() -> FaultSupport.inClusterCalls("recover")));
            assertEquals("RUNNING", owner.callOnInstance(() -> AutonomicService.instance.getState()));
            assertEquals(null, localState(cluster, 1, "inflight/" + second));
        }
    }
}
