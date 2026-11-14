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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.coord.TcmEnvironment;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * D4 acceptance for auto-repair integration: the {@code repair_overdue} diagnosis becomes a {@code repair_priority}
 * action in {@code system_distributed.auto_repair_priority} only under a MAINTENANCE claim and only inside the rate
 * limit; with auto-repair disabled only the diagnosis appears. The scheduler is enabled with a one hour initial delay,
 * so it never repairs during the test and repair history is written by the test.
 */
public class AutoRepairPriorityTest extends TestBaseImpl
{
    private static final long DEADLINE_MILLIS = 60_000;
    private static final String ACTIONS = "SELECT id, state, state_reason, target, verdict FROM system_autonomic.actions "
                                          + "WHERE actuator = 'repair_priority' ALLOW FILTERING";
    private static final String PRIORITY = "SELECT repair_priority FROM system_distributed.auto_repair_priority WHERE repair_type = 'FULL'";

    private static void fast(IInstanceConfig c, boolean autoRepair)
    {
        if (autoRepair)
        {
            c.set("auto_repair", ImmutableMap.of("repair_type_overrides",
                                                 ImmutableMap.of("full", ImmutableMap.of("enabled", "true",
                                                                                         "initial_scheduler_delay", "1h",
                                                                                         "min_repair_interval", "1h"))))
             .set("auto_repair.enabled", "true");
        }
        // the layer runs on node 1 only; node 2 applies the ledger and holds claims the test makes for it
        c.set("autonomic.enabled", c.num() == 1)
         .set("autonomic.dry_run", false)
         .set("autonomic.startup_grace", "1s")
         .set("autonomic.monitor_interval", "1s")
         .set("autonomic.analyze_interval", "1s")
         .set("autonomic.sampling.enabled", false)
         .set("autonomic.safety.cluster_approval", "auto")
         .set("autonomic.safety.max_cluster_actions_per_hour", 1)
         .set("autonomic.detectors", Map.of("repair_overdue", Map.of("for_evaluations", 2, "refresh_ms", 500, "repair_type", "FULL")))
         .set("autonomic.actuators", Map.of("repair_priority", Map.of("enabled", true)));
    }

    private static void await(String what, BooleanSupplier step) throws InterruptedException
    {
        long end = System.currentTimeMillis() + DEADLINE_MILLIS;
        while (System.currentTimeMillis() < end)
        {
            if (step.getAsBoolean())
                return;
            Thread.sleep(200);
        }
        throw new AssertionError("timed out waiting for " + what);
    }

    private static Object[][] actions(Cluster cluster)
    {
        return cluster.coordinator(1).execute(ACTIONS, ConsistencyLevel.ONE);
    }

    private static boolean hasAction(Cluster cluster, String state, String reason)
    {
        for (Object[] row : actions(cluster))
            if (state.equals(row[1]) && (reason == null || reason.equals(row[2])))
                return true;
        return false;
    }

    private static int actionsIn(Cluster cluster, String state)
    {
        int n = 0;
        for (Object[] row : actions(cluster))
            if (state.equals(row[1]))
                n++;
        return n;
    }

    @SuppressWarnings("unchecked")
    private static Set<UUID> priority(Cluster cluster)
    {
        Object[][] rows = cluster.coordinator(1).execute(PRIORITY, cluster.size() > 1 ? ConsistencyLevel.QUORUM : ConsistencyLevel.ONE);
        return rows.length == 0 || rows[0][0] == null ? Set.of() : (Set<UUID>) rows[0][0];
    }

    private static Object[][] diagnoses(Cluster cluster)
    {
        return cluster.coordinator(1).execute("SELECT subject, severity, evidence FROM system_autonomic.diagnoses "
                                              + "WHERE type = 'repair_overdue' ALLOW FILTERING", ConsistencyLevel.ONE);
    }

    private static boolean allDiagnoses(Cluster cluster, int count, String severity, String autoRepairEnabled)
    {
        Object[][] rows = diagnoses(cluster);
        if (rows.length != count)
            return false;
        for (Object[] row : rows)
            if (!severity.equals(row[1]) || !autoRepairEnabled.equals(((Map<?, ?>) row[2]).get("auto_repair_enabled")))
                return false;
        return true;
    }

    private static String hostId(IInvokableInstance node)
    {
        return node.callOnInstance(() -> StorageService.instance.getLocalHostUUID().toString());
    }

    private static void armed(IInvokableInstance node) throws InterruptedException
    {
        await("the layer to be armed", () -> node.callOnInstance(() -> AutonomicService.instance.isArmed()));
    }

    private static ClusterCoordinator coordinator()
    {
        return new ClusterCoordinator(new TcmEnvironment(Clock.Global::currentTimeMillis, DatabaseDescriptor::getAutonomicConfig));
    }

    /** Both criteria of T4.4 and the rate and claim limits of D4 criterion 3, on two nodes. */
    @Test
    public void priorityIsSetOnceUnderAClaimAndWithinTheRateLimit() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).withConfig(c -> fast(c.with(NETWORK, GOSSIP), true)).start()))
        {
            IInvokableInstance n1 = cluster.get(1);
            IInvokableInstance n2 = cluster.get(2);
            armed(n1);
            Set<String> hosts = Set.of(hostId(n1), hostId(n2));
            // node 1 repaired ten intervals ago, node 2 has no history: both are overdue
            cluster.coordinator(1).execute("INSERT INTO system_distributed.auto_repair_history (repair_type, host_id, repair_start_ts, repair_finish_ts) "
                                           + "VALUES ('FULL', ?, ?, ?)", ConsistencyLevel.QUORUM, UUID.fromString(hostId(n1)),
                                           new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(11)),
                                           new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(10)));

            // ---- 1. the diagnosis names both hosts; nothing can be done before `cluster enable` ----
            await("a CRITICAL repair_overdue diagnosis per host", () -> allDiagnoses(cluster, 2, "CRITICAL", "true"));
            Set<String> subjects = new java.util.HashSet<>();
            for (Object[] row : diagnoses(cluster))
                subjects.add(((String) row[0]).substring("PEER:".length()));
            assertEquals(hosts, subjects);
            await("proposals refused while cluster actions are off", () -> hasAction(cluster, "REJECTED", "CLUSTER_DISABLED"));
            assertTrue(priority(cluster).isEmpty());

            // ---- 2. a claim held by another node refuses the action ----
            n1.runOnInstance(() -> AutonomicService.instance.setActuatorEnabled("repair_priority", false));
            n1.nodetoolResult("autonomic", "cluster", "enable", "--yes").asserts().success();
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            UUID foreign = n2.callOnInstance(() -> {
                UUID id = TimeUUID.Generator.nextTimeUUID().asUUID();
                ClusterCoordinator.Outcome outcome = coordinator().claim(Lane.MAINTENANCE, id, "other", "CLUSTER", "WORKING", ImmutableMap.of());
                assertTrue(outcome.detail, outcome.ok());
                return id;
            });
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            n1.runOnInstance(() -> AutonomicService.instance.setActuatorEnabled("repair_priority", true));
            await("proposals refused while node 2 holds the lane", () -> hasAction(cluster, "REJECTED", "CONCURRENCY"));
            Thread.sleep(2000);
            assertEquals("nothing may start without the claim", 0, actionsIn(cluster, "COMMITTED") + actionsIn(cluster, "APPLYING")
                                                                  + actionsIn(cluster, "VERIFYING") + actionsIn(cluster, "PROPOSED"));
            assertTrue(priority(cluster).isEmpty());

            // ---- 3. the lane is free: exactly one host gets the priority, under its own claim ----
            assertEquals(foreign.toString(), n2.callOnInstance(() -> AutonomicService.instance.releaseClaim("MAINTENANCE")));
            await("a committed repair_priority action", () -> hasAction(cluster, "COMMITTED", null));
            Object[] committed = null;
            for (Object[] row : actions(cluster))
                if ("COMMITTED".equals(row[1]))
                    committed = row;
            assertEquals("IMPROVED", committed[4]);
            String target = ((String) committed[3]).substring("PEER:".length());
            assertTrue(hosts.contains(target));
            assertEquals(Set.of(UUID.fromString(target)), priority(cluster));
            TimeUUID id = (TimeUUID) committed[0];
            List<String> transitions = new ArrayList<>();
            for (Object[] e : cluster.coordinator(1).execute("SELECT to_state FROM system_autonomic.action_events WHERE action_id = ?",
                                                             ConsistencyLevel.ONE, id.asUUID()))
                transitions.add(e[0].toString());
            assertTrue("audit rows: " + transitions, transitions.containsAll(List.of("APPLYING", "COMMITTED")));
            n1.nodetoolResult("autonomic", "events", id.toString()).asserts().success().stdoutContains("COMMITTED");
            // the claim was released into the ledger's recent list
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            List<Map<String, String>> recent = n1.callOnInstance(() -> AutonomicService.instance.getClusterRecentActions());
            assertTrue(recent.toString(), recent.stream().anyMatch(r -> "repair_priority".equals(r.get("actuator")) && "COMMITTED".equals(r.get("final_state"))
                                                                        && id.toString().equals(r.get("action_id"))));
            assertTrue(n1.callOnInstance(() -> AutonomicService.instance.getClusterClaims()).isEmpty());

            // ---- 4. the other host is refused by the rate limit, the same host by its cooldown; the priority stays at one ----
            await("the second host refused by the rate limit", () -> hasAction(cluster, "REJECTED", "RATE"));
            await("the same host refused by the cooldown", () -> hasAction(cluster, "REJECTED", "COOLDOWN"));
            Thread.sleep(3000);
            assertEquals(1, actionsIn(cluster, "COMMITTED"));
            assertEquals(Set.of(UUID.fromString(target)), priority(cluster));
            assertTrue(n1.callOnInstance(() -> AutonomicService.instance.getClusterClaims()).isEmpty());

            // ---- 5. auto-repair is switched off: the diagnoses stay, at INFO, and nothing is proposed any more ----
            n1.runOnInstance(() -> AutoRepairService.instance.getAutoRepairConfig().setAutoRepairSchedulingEnabled(false));
            await("INFO diagnoses that say auto-repair is disabled", () -> allDiagnoses(cluster, 2, "INFO", "false"));
            Thread.sleep(2000);
            int before = actions(cluster).length;
            Thread.sleep(4000);
            assertEquals("no plan while auto-repair is disabled", before, actions(cluster).length);
            assertEquals(1, actionsIn(cluster, "COMMITTED"));
        }
    }

    /** Auto-repair disabled from the start: its statements were never prepared, so there is no history, and only a diagnosis appears. */
    @Test
    public void withAutoRepairDisabledOnlyADiagnosisAppears() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).withConfig(c -> fast(c.with(NETWORK, GOSSIP), false)).start()))
        {
            IInvokableInstance n1 = cluster.get(1);
            armed(n1);
            n1.nodetoolResult("autonomic", "cluster", "enable", "--yes").asserts().success();
            await("an INFO diagnosis that says auto-repair is disabled", () -> allDiagnoses(cluster, 1, "INFO", "false"));
            Thread.sleep(6000);
            assertEquals("no action of any state", 0, actions(cluster).length);
            assertTrue(priority(cluster).isEmpty());
            assertFalse(n1.callOnInstance(() -> AutonomicService.instance.getInFlightActions().stream().anyMatch(a -> "repair_priority".equals(a.get("actuator")))));
        }
    }
}
