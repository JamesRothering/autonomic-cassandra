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
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.autonomic.monitor.InjectedSensor;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * D4 acceptance on one node: a backlog that the policy answers by raising compaction throughput, and that then gets
 * worse, ends {@code ROLLED_BACK} with the knob restored and the transitions in {@code system_autonomic.action_events}.
 * The compaction and client sensors are replaced by an injected one so that the signals are exact; everything else,
 * from the detector to the actuator, is the production path.
 */
public class SelfHealingRollbackTest extends TestBaseImpl
{
    private static final long DEADLINE_MILLIS = 60_000;
    private static final SignalId PENDING = Signals.COMPACTION_PENDING_TASKS;
    private static InjectedSensor injected;

    private static void fast(IInstanceConfig c)
    {
        c.set("autonomic.enabled", true)
         .set("autonomic.dry_run", false)
         .set("autonomic.startup_grace", "1s")
         .set("autonomic.monitor_interval", "1s")
         .set("autonomic.analyze_interval", "1s")
         .set("autonomic.sampling.enabled", false)
         .set("autonomic.detectors", Map.of("compaction_backlog", Map.of("pending_tasks", 10, "for_evaluations", 3)))
         .set("autonomic.actuators", Map.of("compaction_throughput", Map.of("enabled", true, "baseline_window", "10s", "observation_window", "10s")));
    }

    private static void push(Cluster cluster, double pending)
    {
        cluster.get(1).runOnInstance(() -> {
            Subject node = Subject.node();
            injected.push(PENDING, node, pending)
                    .push(Signals.CLIENT_READ_LATENCY_P99_MICROS, node, 1000)
                    .push(Signals.CLIENT_WRITE_LATENCY_P99_MICROS, node, 1000);
        });
    }

    private static Object[][] actions(Cluster cluster)
    {
        return cluster.coordinator(1).execute("SELECT id, state, state_reason, verdict, undo, baseline, params FROM system_autonomic.actions "
                                              + "WHERE actuator = 'compaction_throughput' ALLOW FILTERING", ConsistencyLevel.ONE);
    }

    /** Later proposals of the same actuator are rejected (cooldown), so look for the row by state. */
    private static Object[] inState(Cluster cluster, String state)
    {
        for (Object[] row : actions(cluster))
            if (state.equals(row[1]))
                return row;
        return null;
    }

    @Test
    public void aRegressingChangeIsRolledBackAndAudited() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).withConfig(SelfHealingRollbackTest::fast).start()))
        {
            int configured = cluster.get(1).callOnInstance(() -> StorageService.instance.getCompactionThroughputMbPerSec());
            cluster.get(1).runOnInstance(() -> {
                AutonomicService.instance.removeSensor("compaction");
                AutonomicService.instance.removeSensor("client_request");
                injected = new InjectedSensor();
                AutonomicService.instance.registerSensor(injected);
            });
            cluster.get(1).nodetoolResult("autonomic", "actuators").asserts().success()
                   .stdoutContains("compaction_throughput").stdoutContains("garbage_collect").stdoutContains("advertised_severity");

            // backlog at 20 tasks, twice the threshold: the policy raises the throughput, then the backlog grows
            boolean worsened = false;
            boolean sawRaised = false;
            TimeUUID id = null;
            long end = System.currentTimeMillis() + DEADLINE_MILLIS;
            while (System.currentTimeMillis() < end && id == null)
            {
                if (inState(cluster, "VERIFYING") != null)
                {
                    worsened = true;
                    sawRaised |= cluster.get(1).callOnInstance(() -> StorageService.instance.getCompactionThroughputMbPerSec()) > configured;
                }
                Object[] done = inState(cluster, "ROLLED_BACK");
                if (done != null)
                    id = (TimeUUID) done[0];
                push(cluster, worsened ? 40 : 20);
                Thread.sleep(500);
            }
            assertTrue("no ROLLED_BACK action within the deadline: " + java.util.Arrays.deepToString(actions(cluster)), id != null);
            assertTrue("the raised throughput was not observed while verifying", sawRaised);

            Object[] row = inState(cluster, "ROLLED_BACK");
            assertEquals("REGRESSED", row[3]);
            assertEquals("REGRESSED", row[2]);
            @SuppressWarnings("unchecked")
            Map<String, String> undo = (Map<String, String>) row[4];
            assertEquals(String.valueOf(configured), undo.get("previous"));
            assertFalse(((Map<?, ?>) row[5]).isEmpty());
            assertEquals(configured, (int) cluster.get(1).callOnInstance(() -> StorageService.instance.getCompactionThroughputMbPerSec()));

            List<String> transitions = new ArrayList<>();
            for (Object[] e : cluster.coordinator(1).execute("SELECT from_state, to_state FROM system_autonomic.action_events WHERE action_id = ?",
                                                             ConsistencyLevel.ONE, id.asUUID()))
                transitions.add(e[1].toString());
            assertTrue("audit rows: " + transitions, transitions.containsAll(List.of("APPLYING", "VERIFYING", "ROLLING_BACK", "ROLLED_BACK")));
            cluster.get(1).nodetoolResult("autonomic", "events", id.toString()).asserts().success().stdoutContains("ROLLED_BACK");
        }
    }
}
