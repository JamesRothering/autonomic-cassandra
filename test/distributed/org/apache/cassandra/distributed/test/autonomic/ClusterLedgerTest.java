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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator;
import org.apache.cassandra.autonomic.coord.ClusterLedger;
import org.apache.cassandra.autonomic.coord.ClusterLedgerValue;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.coord.TcmEnvironment;
import org.apache.cassandra.autonomic.execute.ActionContext;
import org.apache.cassandra.autonomic.execute.ClusterActuator;
import org.apache.cassandra.autonomic.execute.StepResult;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * The cluster ledger on a real metadata log: the operator gate, concurrent claims, takeover of an expired lease,
 * and (risk R1) restart, snapshot and catch-up with ledger entries in the log.
 */
public class ClusterLedgerTest extends TestBaseImpl
{
    private static final String ACTUATOR = "fake_recover";

    /** Registered on node 1 only; recovery undoes. Calls are recorded per instance (one class loader each). */
    public static class RecoveringActuator implements ClusterActuator
    {
        public static final List<String> CALLS = Collections.synchronizedList(new ArrayList<>());

        public String name()
        {
            return ACTUATOR;
        }

        public Lane lane()
        {
            return Lane.MAINTENANCE;
        }

        public boolean reversible()
        {
            return true;
        }

        public Optional<String> validate(Action action, KnowledgeView knowledge)
        {
            return Optional.empty();
        }

        public StepResult step(Action action, Claim claim, ActionContext ctx)
        {
            CALLS.add("step:" + claim.phase);
            return StepResult.stay(1000);
        }

        public StepResult recover(Claim claim, ActionContext ctx)
        {
            CALLS.add("recover:" + claim.phase + ':' + claim.state.get("request") + ":owner=" + claim.owner.id());
            return StepResult.rolledBack("undone by the test actuator");
        }

        public void onPhase(Claim claim, ActionContext ctx)
        {
            CALLS.add("onPhase:" + claim.phase);
        }
    }

    // ---- helpers that run inside an instance; everything they touch is public API of the layer ----

    private static ClusterCoordinator coordinator()
    {
        return new ClusterCoordinator(new TcmEnvironment(Clock.Global::currentTimeMillis, DatabaseDescriptor::getAutonomicConfig));
    }

    private static String ledger(IInvokableInstance node)
    {
        return node.callOnInstance(() -> ClusterLedger.of(ClusterMetadata.current()).toString());
    }

    private static boolean ledgerPresent(IInvokableInstance node)
    {
        return node.callOnInstance(() -> ClusterMetadata.current().extensions.containsKey(ClusterLedgerValue.KEY));
    }

    private static long epoch(IInvokableInstance node)
    {
        return ClusterUtils.getCurrentEpoch(node).getEpoch();
    }

    /** Claims the lane from the node; returns the owner's node id if the ledger shows the claim, or -1. */
    private static int claim(IInvokableInstance node, String lane, String id, String actuator)
    {
        return node.callOnInstance(() -> {
            ClusterCoordinator.Outcome outcome = coordinator().claim(Lane.valueOf(lane), UUID.fromString(id), actuator, "CLUSTER", "WORKING",
                                                                    ImmutableMap.of("request", "r1"));
            return outcome.ok() ? outcome.claim.owner.id() : -1;
        });
    }

    private static void advance(IInvokableInstance node, String id, String phase)
    {
        node.runOnInstance(() -> {
            ClusterCoordinator c = coordinator();
            Claim own = c.ownClaim(UUID.fromString(id));
            assertNotNull("claim of " + id, own);
            assertTrue(c.advance(own, phase, ImmutableMap.of("request", "r1", "phase", phase)).ok());
        });
    }

    private static void release(IInvokableInstance node, String id, String finalState)
    {
        node.runOnInstance(() -> {
            ClusterCoordinator c = coordinator();
            assertTrue(c.release(c.ownClaim(UUID.fromString(id)), finalState).ok());
        });
    }

    private static String newId(IInvokableInstance node)
    {
        return node.callOnInstance(() -> TimeUUID.Generator.nextTimeUUID().asUUID().toString());
    }

    private static void awaitTrue(String what, java.util.function.BooleanSupplier condition) throws InterruptedException
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!condition.getAsBoolean())
        {
            if (System.nanoTime() > deadline)
                throw new AssertionError("timed out waiting for " + what);
            Thread.sleep(200);
        }
    }

    // ---------------------------------------------------------------------------------------------------------

    /**
     * Node 1 runs the layer with a cluster actuator; node 2 has {@code autonomic.enabled: false} throughout and
     * still applies every ledger entry.
     */
    @Test
    public void gateConcurrentClaimsAndTakeover() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2).withConfig(c -> c.with(NETWORK, GOSSIP)).start()))
        {
            IInvokableInstance n1 = cluster.get(1);
            IInvokableInstance n2 = cluster.get(2);

            // node 1: short lease, no startup grace, a fake cluster actuator, then the layer is enabled at runtime
            n1.runOnInstance(() -> {
                AutonomicConfig config = DatabaseDescriptor.getAutonomicConfig();
                config.startup_grace = new DurationSpec.IntMillisecondsBound("0ms");
                config.analyze_interval = new DurationSpec.IntMillisecondsBound("200ms");
                config.coordination.lease = new DurationSpec.IntSecondsBound("4s");
                config.coordination.lease_safety_margin = new DurationSpec.IntSecondsBound("1s");
                AutonomicService.instance.registerActuator(new RecoveringActuator());
                AutonomicService.instance.setActuatorEnabled(ACTUATOR, true);
                AutonomicService.instance.setEnabled(true);
            });
            n2.runOnInstance(() -> DatabaseDescriptor.getAutonomicConfig().coordination.lease = new DurationSpec.IntSecondsBound("4s"));
            awaitTrue("layer armed on node 1", () -> n1.callOnInstance(() -> AutonomicService.instance.isArmed()));
            assertFalse(n2.callOnInstance(() -> AutonomicService.instance.isEnabled()));

            // ---- 1. nothing is committed before `cluster enable` ----
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            long before = epoch(n1);
            String id0 = newId(n1);
            for (IInvokableInstance node : List.of(n1, n2))
            {
                assertEquals(-1, claim(node, "MAINTENANCE", id0, ACTUATOR));
                node.runOnInstance(() -> {
                    AutonomicService service = AutonomicService.instance;
                    assertFalse(service.isClusterActionsEnabled());
                    assertEquals("false", service.getClusterStatus().get("ledger_present"));
                    assertTrue(service.getClusterClaims().isEmpty());
                    // disabling what was never enabled writes nothing
                    service.enableClusterActions(false);
                    try
                    {
                        service.releaseClaim("MAINTENANCE");
                        throw new AssertionError("nothing to release");
                    }
                    catch (IllegalStateException expected)
                    {
                        // lane is free
                    }
                    ClusterCoordinator c = coordinator();
                    Claim imagined = new Claim(UUID.randomUUID(), ACTUATOR, "CLUSTER", ClusterMetadata.current().myNodeId(), "P", 0, 1, Long.MAX_VALUE, ImmutableMap.of());
                    assertFalse(c.advance(imagined, "P1", ImmutableMap.of()).ok());
                    assertFalse(c.release(imagined, "COMMITTED").ok());
                    assertEquals(0, c.releaseAbandoned());
                });
            }
            // the armed layer on node 1 has ticked meanwhile
            Thread.sleep(1500);
            for (IInvokableInstance node : List.of(n1, n2))
            {
                assertFalse("ledger extension must be absent", ledgerPresent(node));
                assertEquals("epoch must not move through autonomic activity", before, epoch(node));
            }

            // ---- 2. the operator enables cluster actions, from the node where the layer is disabled ----
            n2.runOnInstance(() -> AutonomicService.instance.enableClusterActions(true));
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            assertEquals(before + 1, epoch(n1));
            for (IInvokableInstance node : List.of(n1, n2))
            {
                assertTrue(ledgerPresent(node));
                assertTrue(node.callOnInstance(() -> AutonomicService.instance.isClusterActionsEnabled()));
            }

            // ---- 3. two concurrent ClaimAction commits: exactly one owner ----
            String idA = newId(n1);
            String idB = newId(n2);
            Future<Integer> f1 = n1.asyncCallsOnInstance(() -> claim(Lane.TOPOLOGY, idA)).call();
            Future<Integer> f2 = n2.asyncCallsOnInstance(() -> claim(Lane.TOPOLOGY, idB)).call();
            int r1 = f1.get(1, TimeUnit.MINUTES);
            int r2 = f2.get(1, TimeUnit.MINUTES);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            assertTrue("exactly one claim must win: " + r1 + ", " + r2, (r1 == -1) != (r2 == -1));
            String winner = r1 != -1 ? idA : idB;
            for (IInvokableInstance node : List.of(n1, n2))
            {
                List<Map<String, String>> claims = node.callOnInstance(() -> AutonomicService.instance.getClusterClaims());
                assertEquals(1, claims.size());
                assertEquals("TOPOLOGY", claims.get(0).get("lane"));
                assertEquals(winner, claims.get(0).get("action_id"));
                assertEquals(String.valueOf(Math.max(r1, r2)), claims.get(0).get("owner"));
            }
            assertEquals(ledger(n1), ledger(n2));
            // the virtual table shows the claim on both nodes, also where the layer is disabled
            for (IInvokableInstance node : List.of(n1, n2))
            {
                Object[][] rows = node.executeInternal("SELECT lane, actuator, phase FROM system_views.autonomic_cluster_ledger");
                assertEquals(1, rows.length);
                assertEquals("TOPOLOGY", rows[0][0]);
                assertEquals("WORKING", rows[0][2]);
            }
            // forced release by the operator
            assertEquals(winner, n2.callOnInstance(() -> AutonomicService.instance.releaseClaim("topology")));
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            assertTrue(n1.callOnInstance(() -> AutonomicService.instance.getClusterClaims()).isEmpty());
            assertEquals(ClusterCoordinator.FORCE_RELEASED, n1.callOnInstance(() -> AutonomicService.instance.getClusterRecentActions()).get(0).get("final_state"));

            // ---- 4. an expired lease is taken over and recover runs ----
            // node 2 claims and never renews (its layer is not running): the claim expires after 4s
            String idC = newId(n2);
            int owner = claim(n2, "MAINTENANCE", idC, ACTUATOR);
            assertTrue(owner > 0);
            awaitTrue("recover on node 1", () -> n1.callOnInstance(() -> RecoveringActuator.CALLS.stream().anyMatch(c -> c.startsWith("recover:"))));
            awaitTrue("release after recovery", () -> n1.callOnInstance(() -> AutonomicService.instance.getClusterClaims().isEmpty()));
            List<String> calls = n1.callOnInstance(() -> new ArrayList<>(RecoveringActuator.CALLS));
            int n1Id = n1.callOnInstance(() -> ClusterMetadata.current().myNodeId().id());
            assertTrue(calls.toString(), calls.contains("recover:" + Claim.RECOVERING + ":r1:owner=" + n1Id));
            assertFalse("never stepped: the action was not node 1's", calls.stream().anyMatch(c -> c.startsWith("step:")));
            // phase changes committed by either node were dispatched to node 1's actuator
            assertTrue(calls.toString(), calls.contains("onPhase:WORKING"));
            assertTrue(calls.toString(), calls.contains("onPhase:" + Claim.RECOVERING));

            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            for (IInvokableInstance node : List.of(n1, n2))
            {
                Map<String, String> recent = node.callOnInstance(() -> AutonomicService.instance.getClusterRecentActions()).get(0);
                assertEquals("ROLLED_BACK", recent.get("final_state"));
                assertEquals(ACTUATOR, recent.get("actuator"));
                assertFalse("the takeover claim has its own action id", idC.equals(recent.get("action_id")));
            }
            // node 1 has the audit trail of the recovery
            List<Map<String, String>> history = n1.callOnInstance(() -> AutonomicService.instance.getHistory(10));
            assertTrue(history.toString(), history.stream().anyMatch(h -> "ROLLED_BACK".equals(h.get("state")) && h.get("params").contains(idC)));
            // the returning owner finds its claim gone
            n2.runOnInstance(() -> assertEquals(null, coordinator().ownClaim(UUID.fromString(idC))));
            assertEquals(ledger(n1), ledger(n2));
        }
    }

    private static int claim(Lane lane, String id)
    {
        ClusterCoordinator.Outcome outcome = coordinator().claim(lane, UUID.fromString(id), "none", "CLUSTER", "WORKING", ImmutableMap.of("request", "r1"));
        return outcome.ok() ? outcome.claim.owner.id() : -1;
    }

    /**
     * Risk R1: the CUSTOM transformation and ExtensionValue path through restart replay, a metadata snapshot, and a
     * new node catching up. Node 2 (and node 3) have the layer disabled.
     */
    @Test
    public void restartSnapshotAndCatchUpWithLedgerEntries() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3))
                                           .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(3, "dc0", "rack0"))
                                           .withConfig(c -> c.with(NETWORK, GOSSIP).set("autonomic.enabled", c.num() == 1))
                                           .start()))
        {
            IInvokableInstance n1 = cluster.get(1);
            IInvokableInstance n2 = cluster.get(2);
            assertTrue(n1.callOnInstance(() -> AutonomicService.instance.isEnabled()));
            assertFalse(n2.callOnInstance(() -> AutonomicService.instance.isEnabled()));

            // every kind of ledger entry: enable, claim, advance, release, a claim that stays, disable and enable
            n1.runOnInstance(() -> AutonomicService.instance.enableClusterActions(true));
            String idA = newId(n1);
            String idB = newId(n1);
            assertTrue(claim(n1, "TOPOLOGY", idA, "none") > 0);
            advance(n1, idA, "P1");
            release(n1, idA, "COMMITTED");
            assertTrue(claim(n2, "MAINTENANCE", idB, "none") > 0);
            advance(n2, idB, "P1");
            n1.runOnInstance(() -> AutonomicService.instance.enableClusterActions(false));
            n1.runOnInstance(() -> AutonomicService.instance.enableClusterActions(true));
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            String expected = ledger(n1);
            assertTrue(expected, expected.contains(idB) && expected.contains("phase=P1") && expected.contains("COMMITTED") && expected.contains("enabled=true"));
            assertEquals(expected, ledger(n2));

            // ---- a node with the layer disabled restarts and replays the log ----
            ClusterUtils.restartUnchecked(n2);
            assertEquals(expected, ledger(n2));
            assertFalse(n2.callOnInstance(() -> AutonomicService.instance.isEnabled()));
            // ---- the CMS node, with the layer enabled, restarts and replays the log ----
            ClusterUtils.restartUnchecked(n1);
            assertEquals(expected, ledger(n1));
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            assertEquals(epoch(n1), epoch(n2));

            // ---- snapshot with a non-empty ledger, then more entries, then restarts ----
            Epoch snapshot = ClusterUtils.snapshotClusterMetadata(n1);
            String inSnapshot = n1.callOnInstance(() -> ClusterLedger.of(org.apache.cassandra.tcm.ClusterMetadataService.instance().snapshotManager()
                                                                                .getSnapshot(ClusterMetadata.current().epoch)).toString());
            assertEquals(expected, inSnapshot);
            advance(n2, idB, "P2");
            String idC = newId(n1);
            // another actuator: "none" on this target is in its cooldown after the release above
            assertTrue(claim(n1, "SCHEMA_LAYOUT", idC, "other") > 0);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            String afterSnapshot = ledger(n1);
            assertTrue(afterSnapshot, afterSnapshot.contains("phase=P2") && afterSnapshot.contains(idC));
            assertTrue(epoch(n1) > snapshot.getEpoch());
            assertEquals(afterSnapshot, ledger(n2));

            ClusterUtils.restartUnchecked(n2);
            assertEquals(afterSnapshot, ledger(n2));
            ClusterUtils.restartUnchecked(n1);
            assertEquals(afterSnapshot, ledger(n1));
            // the ledger still works after being loaded from a snapshot plus replay
            advance(n2, idB, "P3");
            release(n1, idC, "ROLLED_BACK");
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            String afterRestart = ledger(n1);
            assertTrue(afterRestart, afterRestart.contains("phase=P3") && afterRestart.contains("ROLLED_BACK"));
            assertEquals(afterRestart, ledger(n2));

            // ---- a new node catches up on a snapshot and log that contain ledger entries ----
            ClusterUtils.snapshotClusterMetadata(n1);
            advance(n2, idB, "P4");
            IInstanceConfig config = cluster.newInstanceConfig().set("auto_bootstrap", true);
            IInvokableInstance n3 = cluster.bootstrap(config);
            n3.startup();
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            String finalLedger = ledger(n1);
            assertTrue(finalLedger, finalLedger.contains("phase=P4"));
            assertEquals(finalLedger, ledger(n2));
            assertEquals(finalLedger, ledger(n3));
            assertFalse(n3.callOnInstance(() -> AutonomicService.instance.isEnabled()));
            assertEquals(epoch(n1), epoch(n3));
            // and the joined node can take part: a release committed from it
            n3.runOnInstance(() -> assertEquals(idB, AutonomicService.instance.releaseClaim("MAINTENANCE")));
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            assertEquals(ledger(n3), ledger(n1));
        }
    }
}
