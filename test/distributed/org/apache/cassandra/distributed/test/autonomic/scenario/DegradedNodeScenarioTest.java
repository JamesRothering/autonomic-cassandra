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
package org.apache.cassandra.distributed.test.autonomic.scenario;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.autonomic.monitor.InjectedSensor;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.actions;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.describe;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.diagnoses;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.inState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * D7 scenario, degraded node: two nodes run the layer, node 2 builds a compaction backlog (its compaction sensor is
 * replaced by an injected one, so the signal is exact) and node 1 stays healthy. Asserts the diagnosis
 * {@code compaction_backlog} on node 2 and none on node 1, the action (compaction throughput raised on node 2, verified
 * while it is raised), the verdict {@code IMPROVED} with the state {@code COMMITTED} when the backlog shrinks, the audit
 * trail, and that node 1 was neither diagnosed nor touched. The rollback of a change that makes things worse is
 * {@code SelfHealingRollbackTest}.
 */
public class DegradedNodeScenarioTest extends TestBaseImpl
{
    private static final String ACTUATOR = "compaction_throughput";
    private static InjectedSensor injected;

    private static void fast(IInstanceConfig c)
    {
        c.set("autonomic.enabled", true)
         .set("autonomic.dry_run", false)
         .set("autonomic.startup_grace", "1s")
         .set("autonomic.monitor_interval", "500ms")
         .set("autonomic.analyze_interval", "500ms")
         .set("autonomic.sampling.enabled", false)
         .set("autonomic.detectors", Map.of("compaction_backlog", Map.of("pending_tasks", 10, "for_evaluations", 3)))
         .set("autonomic.actuators", Map.of(ACTUATOR, Map.of("enabled", true, "baseline_window", "4s", "observation_window", "4s")));
    }

    private static void push(Cluster cluster, double pending)
    {
        cluster.get(2).runOnInstance(() -> {
            Subject node = Subject.node();
            injected.push(Signals.COMPACTION_PENDING_TASKS, node, pending)
                    .push(Signals.CLIENT_READ_LATENCY_P99_MICROS, node, 1000)
                    .push(Signals.CLIENT_WRITE_LATENCY_P99_MICROS, node, 1000);
        });
    }

    private static int throughput(Cluster cluster, int node)
    {
        return cluster.get(node).callOnInstance(() -> StorageService.instance.getCompactionThroughputMbPerSec());
    }

    @Test
    public void aBacklogOnOneNodeIsDiagnosedHealedAndVerified() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).withConfig(DegradedNodeScenarioTest::fast).start()))
        {
            int configured = throughput(cluster, 2);
            cluster.get(2).runOnInstance(() -> {
                AutonomicService.instance.removeSensor("compaction");
                AutonomicService.instance.removeSensor("client_request");
                injected = new InjectedSensor();
                AutonomicService.instance.registerSensor(injected);
            });

            // degraded: twice the threshold; once the throughput is raised the backlog falls
            boolean raisedSeen = false;
            long end = System.currentTimeMillis() + ScenarioSupport.DEADLINE_MILLIS;
            Object[] done = null;
            while (System.currentTimeMillis() < end && done == null)
            {
                boolean raised = throughput(cluster, 2) > configured;
                raisedSeen |= raised;
                push(cluster, raised ? 4 : 20);
                done = inState(cluster, 2, ACTUATOR, "COMMITTED");
                Thread.sleep(300);
            }
            assertTrue("no COMMITTED action within the deadline: " + describe(actions(cluster, 2, ACTUATOR)), done != null);
            assertTrue("the raised throughput was never observed", raisedSeen);

            // diagnosis
            Object[][] diagnosed = diagnoses(cluster, 2, "compaction_backlog");
            assertTrue("no compaction_backlog diagnosis on node 2", diagnosed.length > 0);
            assertEquals("NODE", ((String) diagnosed[0][0]).substring(0, 4));
            assertEquals("no diagnosis on the healthy node", 0, diagnoses(cluster, 1, "compaction_backlog").length);

            // action and verdict
            assertEquals("IMPROVED", done[3]);
            assertTrue("throughput was raised: " + throughput(cluster, 2) + " vs " + configured, throughput(cluster, 2) > configured);
            @SuppressWarnings("unchecked")
            Map<String, String> undo = (Map<String, String>) done[5];
            assertEquals(String.valueOf(configured), undo.get("previous"));

            // audit trail: applied, verified, committed
            TimeUUID id = (TimeUUID) done[0];
            List<String> transitions = new ArrayList<>();
            for (Object[] e : cluster.coordinator(2).execute("SELECT to_state FROM system_autonomic.action_events WHERE action_id = ?",
                                                             ConsistencyLevel.ONE, id.asUUID()))
                transitions.add(e[0].toString());
            assertTrue("audit rows: " + transitions, transitions.containsAll(List.of("APPLYING", "VERIFYING", "COMMITTED")));
            cluster.get(2).nodetoolResult("autonomic", "events", id.toString()).asserts().success().stdoutContains("COMMITTED");

            // the healthy node was left alone
            assertEquals("node 1 has actions", 0, actions(cluster, 1, ACTUATOR).length);
            assertEquals(configured, throughput(cluster, 1));
        }
    }
}
