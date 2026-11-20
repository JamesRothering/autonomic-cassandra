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

import java.util.List;

import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.COMPONENT_FAILED;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.KNOB;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.applied;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.assertContains;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.events;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.throughput;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.await;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.describe;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.diagnoses;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.inState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * T7.2 fault 1, a throwing sensor (overview 7.5): the sensor that feeds an action under verification starts to throw.
 * It is disabled after {@code component_failure_threshold} consecutive failures and {@code autonomic.component_failed}
 * is raised; the action, now without observations, is judged INCONCLUSIVE and rolled back; nothing new is applied.
 */
public class ThrowingSensorFaultTest extends TestBaseImpl
{
    @Test
    public void aThrowingSensorIsDisabledAndTheActionItFedIsRolledBack() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(c -> FaultSupport.nodeConfig(c).set("autonomic.safety.component_failure_threshold", 3))
                                           .start()))
        {
            IInvokableInstance node = cluster.get(1);
            int configured = throughput(node);
            FaultSupport.injectSensors(node);
            TimeUUID id = (TimeUUID) FaultSupport.driveToVerifying(cluster, 1, configured)[0];

            // the fault: every collect throws from now on
            node.runOnInstance(() -> FaultSupport.sensor.failing = true);
            await("the sensor to be disabled", () -> node.callOnInstance(() -> FaultSupport.inComponentDisabled("sensor", "injected")));
            await("the action to be rolled back", () -> inState(cluster, 1, KNOB, "ROLLED_BACK") != null);

            // in flight restored: the knob, the record and its audit trail
            Object[] row = inState(cluster, 1, KNOB, "ROLLED_BACK");
            assertEquals(id, row[0]);
            assertEquals("INCONCLUSIVE", row[3]);
            assertEquals("INCONCLUSIVE", row[2]);
            assertEquals(configured, throughput(node));
            assertContains("audit trail", events(cluster, 1, id.asUUID()), "APPLYING", "VERIFYING", "ROLLING_BACK", "ROLLED_BACK");

            // the fault is visible: the component is disabled and the self-diagnosis is persisted
            Object[][] rows = node.executeInternal("SELECT enabled, disabled_reason FROM system_views.autonomic_components WHERE kind = 'sensor' AND name = 'injected'");
            assertEquals(1, rows.length);
            assertEquals(Boolean.FALSE, rows[0][0]);
            assertTrue(String.valueOf(rows[0][1]), String.valueOf(rows[0][1]).contains("injected sensor fault"));
            await("component_failed to be persisted", () -> diagnoses(cluster, 1, COMPONENT_FAILED).length > 0);
            Object[] failed = diagnoses(cluster, 1, COMPONENT_FAILED)[0];
            assertTrue(String.valueOf(failed[3]), String.valueOf(failed[3]).contains("sensor:injected"));

            // nothing new: without observations the backlog diagnosis clears (max_signal_age_ms), and no other action was applied
            await("the backlog diagnosis to clear", () -> !node.callOnInstance(() -> FaultSupport.inDiagnosed("compaction_backlog")));
            List<Object[]> appliedRows = applied(cluster, 1, KNOB);
            assertEquals("only the one action was applied: " + describe(appliedRows.toArray(new Object[0][])), 1, appliedRows.size());
            assertEquals(configured, throughput(node));
            assertEquals("0", node.callOnInstance(() -> AutonomicService.instance.getStatus().get("in_flight_actions")));
        }
    }
}
