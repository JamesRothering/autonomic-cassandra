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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.autonomic.monitor.InjectedSensor;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * D5 acceptance (docs/deliverables/D5-elasticity.md, criterion 3): a disk trend opens {@code capacity_exhausted}, the
 * policy proposes a scale-out, and after the operator has enabled cluster actions and approved it the
 * {@link SimulatedProvider} starts a fourth node that joins. The ledger shows the phases and the claim on the
 * TOPOLOGY lane is released at the end. Only node 1 runs the layer; the disk signal is injected, the rest is the
 * production path.
 */
public class ScaleOutTest extends TestBaseImpl
{
    private static final long DEADLINE_MILLIS = 100_000;
    private static final int MEMBERS = 3;
    private static InjectedSensor injected;

    private static void fast(IInstanceConfig c)
    {
        c.with(NETWORK, GOSSIP);
        if (c.num() != 1)
            return;
        c.set("autonomic.enabled", true)
         .set("autonomic.dry_run", false)
         .set("autonomic.startup_grace", "1s")
         .set("autonomic.monitor_interval", "500ms")
         .set("autonomic.analyze_interval", "500ms")
         .set("autonomic.sampling.enabled", false)
         .set("autonomic.detectors", Map.of("capacity_exhausted", Map.of("for_evaluations", 3, "trend_window_ms", 6000, "min_samples", 5,
                                                                         "horizon_hours", 12, "full_fraction", 0.9, "min_used_fraction", 0.5),
                                            "pool_saturation", Map.of("enabled", false)))
         .set("autonomic.actuators", Map.of("scale_out", Map.of("enabled", true, "baseline_window", "2s", "observation_window", "2s")))
         .set("autonomic.scale_out", Map.of("provider", SimulatedProvider.class.getName(),
                                            "parameters", Map.of(SimulatedProvider.MAX_NODES, "4"),
                                            "provision_timeout", "60s", "join_timeout", "120s", "verify_window", "3s",
                                            "provider_call_timeout", "10s", "poll_interval", "500ms"));
    }

    private static void noProvider(IInstanceConfig c)
    {
        fast(c);
        c.set("autonomic.scale_out", Map.of("poll_interval", "500ms"));
    }

    private static Cluster cluster(int nodes, java.util.function.Consumer<IInstanceConfig> config) throws Exception
    {
        return init(Cluster.build(nodes)
                           .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(MEMBERS + 1))
                           .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(MEMBERS + 1, "dc0", "rack0"))
                           .withConfig(config::accept)
                           .start());
    }

    /** Replaces the disk sensor, and pushes a disk usage that grows until stopped or {@link #level} is set. */
    private static final class Disk implements AutoCloseable
    {
        private final Cluster cluster;
        private final AtomicBoolean stop = new AtomicBoolean();
        private volatile double flat = -1;
        private final Thread thread;

        Disk(Cluster cluster)
        {
            this.cluster = cluster;
            cluster.get(1).runOnInstance(() -> {
                AutonomicService.instance.removeSensor("disk");
                injected = new InjectedSensor();
                AutonomicService.instance.registerSensor(injected);
            });
            thread = new Thread(() -> {
                double used = 0.6;
                while (!stop.get())
                {
                    double value = flat >= 0 ? flat : (used = Math.min(0.88, used + 0.002));
                    try
                    {
                        cluster.get(1).runOnInstance(() -> injected.push(Signals.DISK_USED_FRACTION, Subject.node(), value));
                        Thread.sleep(500);
                    }
                    catch (Throwable e)
                    {
                        return;
                    }
                }
            }, "disk-trend");
            thread.setDaemon(true);
            thread.start();
        }

        void level(double fraction)
        {
            flat = fraction;
        }

        @Override
        public void close() throws Exception
        {
            stop.set(true);
            thread.join(10_000);
        }
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException
    {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DEADLINE_MILLIS);
        while (!condition.getAsBoolean())
        {
            if (System.nanoTime() > deadline)
                throw new AssertionError("timed out waiting for " + what);
            Thread.sleep(200);
        }
    }

    private static Object[][] actions(Cluster cluster)
    {
        return cluster.coordinator(1).execute("SELECT id, state, state_reason, verdict FROM system_autonomic.actions WHERE actuator = 'scale_out' ALLOW FILTERING",
                                              ConsistencyLevel.ONE);
    }

    private static UUID actionIn(Cluster cluster, String state)
    {
        for (Object[] row : actions(cluster))
            if (state.equals(row[1]))
                return ((org.apache.cassandra.utils.TimeUUID) row[0]).asUUID();
        return null;
    }

    private static String describe(Cluster cluster)
    {
        return java.util.Arrays.deepToString(actions(cluster)) + " claims=" + cluster.get(1).callOnInstance(() -> AutonomicService.instance.getClusterClaims());
    }

    private static boolean diagnosed(Cluster cluster)
    {
        return cluster.get(1).nodetoolResult("autonomic", "history", "--diagnoses").getStdout().contains("capacity_exhausted");
    }

    /** The phase of the TOPOLOGY claim as the virtual table of the given node shows it, or null. */
    private static String phase(IInvokableInstance node)
    {
        Object[][] rows = node.executeInternal("SELECT phase FROM system_views.autonomic_cluster_ledger WHERE lane = 'TOPOLOGY'");
        return rows.length == 0 ? null : (String) rows[0][0];
    }

    private static int members(IInvokableInstance node)
    {
        return node.callOnInstance(() -> {
            ClusterMetadata metadata = ClusterMetadata.current();
            int n = 0;
            for (NodeId id : metadata.directory.peerIds())
                if (metadata.directory.peerState(id) == NodeState.JOINED)
                    n++;
            return n;
        });
    }

    /** Up to the proposal that waits for the operator: the layer is diagnosing, the gate is closed, nothing is requested. */
    private static UUID proposed(Cluster cluster, SimulatedProvider.Cloud cloud) throws Exception
    {
        IInvokableInstance n1 = cluster.get(1);
        await("capacity_exhausted", () -> diagnosed(cluster));
        // cluster actions are off: whatever was proposed is not approved, and the provider has not been asked
        assertFalse(n1.callOnInstance(() -> AutonomicService.instance.isClusterActionsEnabled()));
        Thread.sleep(2000);
        assertEquals(describe(cluster), List.of(), cloud.calls());
        assertNull(describe(cluster), actionIn(cluster, "APPROVED"));
        assertNull(describe(cluster), actionIn(cluster, "APPLYING"));
        // `enable` without --yes only prints the warning
        n1.nodetoolResult("autonomic", "cluster", "enable").asserts().success();
        assertFalse(n1.callOnInstance(() -> AutonomicService.instance.isClusterActionsEnabled()));
        n1.nodetoolResult("autonomic", "cluster", "enable", "--yes").asserts().success();
        assertTrue(n1.callOnInstance(() -> AutonomicService.instance.isClusterActionsEnabled()));
        await("a proposal awaiting approval: " + describe(cluster), () -> actionIn(cluster, "PROPOSED") != null);
        UUID id = actionIn(cluster, "PROPOSED");
        // manual approval is the default: nothing is requested until the operator says so
        Thread.sleep(2000);
        assertEquals(describe(cluster), List.of(), cloud.calls());
        assertEquals(0, cloud.requests());
        assertNull(phase(n1));
        return id;
    }

    @Test
    public void anApprovedScaleOutAddsOneNode() throws Throwable
    {
        SimulatedProvider.Cloud cloud = null;
        try (Cluster cluster = cluster(MEMBERS, ScaleOutTest::fast))
        {
            cloud = new SimulatedProvider.Cloud(cluster).register();
            IInvokableInstance n1 = cluster.get(1);
            try (Disk disk = new Disk(cluster))
            {
                UUID id = proposed(cluster, cloud);
                disk.level(0.7);

                // the claim's phases, as the ledger shows them on a node that does not run the layer
                Set<String> phases = new LinkedHashSet<>();
                n1.nodetoolResult("autonomic", "approve", id.toString()).asserts().success();
                SimulatedProvider.Cloud c = cloud;
                await("the action to end: " + describe(cluster), () -> {
                    String phase = phase(cluster.get(2));
                    if (phase != null)
                        phases.add(phase);
                    return actionIn(cluster, "COMMITTED") != null || actionIn(cluster, "FAILED") != null || actionIn(cluster, "ROLLED_BACK") != null;
                });
                assertNull(String.valueOf(c.failure()), c.failure());
                assertEquals(describe(cluster), id, actionIn(cluster, "COMMITTED"));

                // 4 nodes, one request, one started instance
                cloud.await();
                assertEquals(1, cloud.requests());
                assertEquals(1, cloud.started());
                assertEquals(0, cloud.cancelled());
                ClusterUtils.waitForCMSToQuiesce(cluster, n1);
                await("four joined members", () -> members(n1) == MEMBERS + 1);
                assertEquals(MEMBERS + 1, cluster.size());
                assertEquals(NodeState.JOINED.name(), cluster.get(MEMBERS + 1).callOnInstance(() -> ClusterMetadata.current().directory
                                                                                                    .peerState(ClusterMetadata.current().myNodeId()).name()));
                assertEquals(1, cloud.calls().stream().filter(call -> call.startsWith("request|")).count());

                // the phases are in the ledger and in the audit trail; the claim on the TOPOLOGY lane is gone
                assertTrue(phases.toString(), phases.contains("PROVISIONING"));
                for (IInvokableInstance node : List.of(n1, cluster.get(2), cluster.get(3)))
                {
                    assertNull(phase(node));
                    List<Map<String, String>> recent = node.callOnInstance(() -> AutonomicService.instance.getClusterRecentActions());
                    assertEquals("COMMITTED", recent.get(0).get("final_state"));
                    assertEquals("scale_out", recent.get(0).get("actuator"));
                }
                assertTrue(n1.callOnInstance(() -> AutonomicService.instance.getClusterClaims()).isEmpty());
                n1.nodetoolResult("autonomic", "cluster", "status").asserts().success().stdoutContains("Claims:");
                List<String> audit = new ArrayList<>();
                for (Object[] row : cluster.coordinator(1).execute("SELECT to_state, detail FROM system_autonomic.action_events WHERE action_id = " + id,
                                                                   ConsistencyLevel.ONE))
                    audit.add(row[0] + ":" + row[1]);
                String trail = audit.toString();
                for (String phase : List.of("REQUESTED", "PROVISIONING", "JOINING", "VERIFYING"))
                    assertTrue(phase + " is not in " + trail, trail.contains(phase));
                assertTrue(trail, trail.contains("COMMITTED"));
            }
        }
        finally
        {
            if (cloud != null)
                cloud.unregister();
        }
    }

    @Test
    public void withoutAProviderThatCanAddNodesThereIsOnlyADiagnosis() throws Throwable
    {
        try (Cluster cluster = cluster(1, ScaleOutTest::noProvider); Disk disk = new Disk(cluster))
        {
            IInvokableInstance n1 = cluster.get(1);
            n1.nodetoolResult("autonomic", "cluster", "enable", "--yes").asserts().success();
            await("capacity_exhausted", () -> diagnosed(cluster));
            Thread.sleep(8000);
            assertEquals(describe(cluster), 0, actions(cluster).length);
            assertTrue(n1.callOnInstance(() -> AutonomicService.instance.getClusterClaims()).isEmpty());
            assertTrue(diagnosed(cluster));
            // the diagnosis is the audit of the refusal
            Object[][] rows = cluster.coordinator(1).execute("SELECT type, state FROM system_autonomic.diagnoses WHERE day = toDate(now()) ALLOW FILTERING",
                                                             ConsistencyLevel.ONE);
            assertTrue(java.util.Arrays.deepToString(rows), java.util.Arrays.deepToString(rows).contains("capacity_exhausted"));
        }
    }

    @Test
    public void shutdownOfTheOwnerBeforeTheNodeStartedCancelsTheRequest() throws Throwable
    {
        SimulatedProvider.Cloud cloud = null;
        try (Cluster cluster = cluster(MEMBERS, ScaleOutTest::fast))
        {
            cloud = new SimulatedProvider.Cloud(cluster).register().hold(true);
            IInvokableInstance n1 = cluster.get(1);
            Disk disk = new Disk(cluster);
            UUID id = proposed(cluster, cloud);
            disk.level(0.7);
            n1.nodetoolResult("autonomic", "approve", id.toString()).asserts().success();
            SimulatedProvider.Cloud c = cloud;
            await("the request to be provisioning: " + describe(cluster), () -> "PROVISIONING".equals(phase(cluster.get(2))));
            assertEquals(1, c.requests());
            assertEquals(0, c.started());
            disk.close();

            n1.shutdown().get(60, TimeUnit.SECONDS);

            // the request was cancelled, the claim released, nothing joined
            assertEquals(1, cloud.cancelled());
            assertEquals(0, cloud.started());
            assertTrue(cloud.calls().toString(), cloud.calls().stream().anyMatch(call -> call.startsWith("cancel|")));
            await("the claim to be released", () -> phase(cluster.get(2)) == null);
            List<Map<String, String>> recent = cluster.get(2).callOnInstance(() -> AutonomicService.instance.getClusterRecentActions());
            assertEquals("ROLLED_BACK", recent.get(0).get("final_state"));
            assertEquals(MEMBERS, cluster.size());
        }
        finally
        {
            if (cloud != null)
                cloud.unregister();
        }
    }

    /** The provider contract as the actuator relies on it, without a cluster. */
    @Test
    public void theSimulatedCloudKeepsTheProviderContract() throws Throwable
    {
        SimulatedProvider.Cloud cloud = new SimulatedProvider.Cloud(null).register().hold(true);
        try
        {
            SimulatedProvider provider = new SimulatedProvider(Map.of(SimulatedProvider.MAX_NODES, "5"));
            assertTrue(provider.capabilities().canAdd);
            assertEquals(5, provider.capabilities().maxNodes);
            org.apache.cassandra.autonomic.elastic.ScaleRequest request = new org.apache.cassandra.autonomic.elastic.ScaleRequest(UUID.randomUUID(), "dc0", "rack0", "test");
            org.apache.cassandra.autonomic.elastic.ProvisionTicket first = provider.requestNode(request);
            assertEquals(first, provider.requestNode(request));
            assertEquals(1, cloud.requests());
            assertEquals(org.apache.cassandra.autonomic.elastic.ProvisionStatus.State.PENDING, provider.status(first).state);
            provider.cancel(first);
            provider.cancel(first);
            assertEquals(1, cloud.cancelled());
            assertEquals(org.apache.cassandra.autonomic.elastic.ProvisionStatus.State.FAILED, provider.status(first).state);
            assertEquals(0, cloud.started());
        }
        finally
        {
            cloud.unregister();
        }
    }
}
