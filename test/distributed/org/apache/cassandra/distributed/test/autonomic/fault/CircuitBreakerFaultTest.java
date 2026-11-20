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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.CIRCUIT_BREAKER;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.KNOB;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.applied;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.events;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.localState;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.push;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.throughput;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.DEADLINE_MILLIS;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.await;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.describe;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.diagnoses;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * T7.2 fault 6, repeated regressions (overview 7.5): four raises of the compaction throughput in a row each make the
 * pending compactions worse and are rolled back REGRESSED. The fourth trips the circuit breaker (4 of the last 6, the
 * defaults): the layer pauses itself with reason CIRCUIT_BREAKER, persists the pause and raises
 * {@code autonomic.circuit_breaker}; the next proposal is rejected PAUSED. The actions are proposed through the
 * executor as the control thread's plan sink would, each with its own diagnosis id, because the production policy's
 * direction lock would (rightly) not raise the knob again for the same diagnosis; the production compaction
 * backlog detector is kept out of the way with a high threshold. Cooldown, quarantine and rate limits are relaxed so
 * that the breaker is the limit that stops the layer.
 */
public class CircuitBreakerFaultTest extends TestBaseImpl
{
    private static final long BASELINE_MILLIS = 5000;
    private static final long OBSERVATION_MILLIS = 4000;
    private static final int LOW = 5;
    private static final int HIGH = 9;

    @Test
    public void repeatedRegressionsTripTheBreakerAndNothingIsAppliedAfter() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(c -> FaultSupport.nodeConfig(c)
                                                                        .set("autonomic.detectors", Map.of("compaction_backlog", Map.of("pending_tasks", 1000)))
                                                                        .set("autonomic.safety.cooldown_after_rollback", "1s")
                                                                        .set("autonomic.safety.quarantine_after_rollbacks", 10)
                                                                        .set("autonomic.safety.max_actions_per_hour", 20)
                                                                        .set("autonomic.safety.max_actions_per_actuator_per_hour", 20))
                                           .start()))
        {
            IInvokableInstance node = cluster.get(1);
            int configured = throughput(node);
            String value = String.valueOf(configured + 1);
            FaultSupport.injectSensors(node);

            List<UUID> rolledBack = new ArrayList<>();
            long lastHigh = 0;
            UUID current = null;
            long end = System.currentTimeMillis() + 4 * DEADLINE_MILLIS / 3;
            while (rolledBack.size() < 4)
            {
                assertTrue("rolled back so far " + rolledBack + ": " + describe(applied(cluster, 1, KNOB).toArray(new Object[0][])),
                           System.currentTimeMillis() < end);
                if (current == null)
                {
                    push(node, LOW);
                    // propose once the baseline window holds only the low value
                    if (System.currentTimeMillis() - lastHigh > BASELINE_MILLIS + 1000)
                    {
                        String diagnosis = UUID.randomUUID().toString();
                        String r = node.callOnInstance(() -> FaultSupport.inPropose(KNOB, "NODE", "NODE", Map.of("value", value, "diagnosis_id", diagnosis),
                                                                                    BASELINE_MILLIS, OBSERVATION_MILLIS));
                        if (r != null && r.startsWith("APPROVED|"))
                            current = UUID.fromString(r.substring(r.lastIndexOf('|') + 1));
                    }
                }
                else
                {
                    // the change makes the backlog worse
                    push(node, HIGH);
                    lastHigh = System.currentTimeMillis();
                    UUID id = current;
                    String state = node.callOnInstance(() -> FaultSupport.inMemoryRecord(id));
                    if (state != null && state.startsWith("ROLLED_BACK"))
                    {
                        assertEquals("ROLLED_BACK:REGRESSED:REGRESSED", state);
                        rolledBack.add(id);
                        current = null;
                    }
                }
                Thread.sleep(500);
            }

            // the breaker tripped on the fourth rollback: paused by itself, persisted, diagnosed
            await("the layer to pause itself", () -> "PAUSED".equals(node.callOnInstance(() -> AutonomicService.instance.getState())));
            Map<String, String> status = node.callOnInstance(() -> AutonomicService.instance.getStatus());
            assertEquals("CIRCUIT_BREAKER", status.get("pause_reason"));
            assertEquals("true", status.get("paused"));
            assertEquals("false", status.get("killed"));
            assertEquals("0", status.get("in_flight_actions"));
            assertTrue(String.valueOf(localState(cluster, 1, "paused")), String.valueOf(localState(cluster, 1, "paused")).endsWith("CIRCUIT_BREAKER"));
            assertTrue(node.callOnInstance(() -> FaultSupport.inDiagnosed(CIRCUIT_BREAKER)));
            await("circuit_breaker to be persisted", () -> diagnoses(cluster, 1, CIRCUIT_BREAKER).length > 0);
            assertTrue(String.valueOf(diagnoses(cluster, 1, CIRCUIT_BREAKER)[0][3]), String.valueOf(diagnoses(cluster, 1, CIRCUIT_BREAKER)[0][3]).contains("rollbacks=4"));

            // each change was restored, with its audit trail
            for (UUID id : rolledBack)
                assertEquals(List.of("PROPOSED", "APPROVED", "APPLYING", "VERIFYING", "ROLLING_BACK", "ROLLED_BACK"), events(cluster, 1, id));
            assertEquals(configured, throughput(node));

            // nothing new: the next proposal is rejected because the layer is paused (check 1, before any other)
            String diagnosis = UUID.randomUUID().toString();
            String r = node.callOnInstance(() -> FaultSupport.inPropose(KNOB, "NODE", "NODE", Map.of("value", value, "diagnosis_id", diagnosis),
                                                                        BASELINE_MILLIS, OBSERVATION_MILLIS));
            assertTrue(r, r.startsWith("REJECTED|PAUSED|"));
            assertEquals(4, applied(cluster, 1, KNOB).size());
            assertEquals(configured, throughput(node));
        }
    }
}
