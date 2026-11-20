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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.KNOB;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.LOOP_STALLED;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.applied;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.assertContains;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.events;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.push;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.throughput;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.await;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.describe;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.diagnoses;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.inState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * T7.2 fault 5, a stalled control thread (overview 7.5): a detector blocks the control thread while an action is
 * verifying. The monitor thread raises {@code autonomic.loop_stalled}; nothing triggers verification any more, so the
 * dead-man deadline on the actuator thread rolls the action back (rule 9 of 7.3). When the thread is released it
 * finishes its stale tick without applying anything, and the diagnosis clears.
 */
public class StalledControlThreadFaultTest extends TestBaseImpl
{
    @Test
    public void theDeadManDeadlineRestoresWhatTheStalledControlThreadCannotVerify() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(c -> FaultSupport.nodeConfig(c).set("autonomic.safety.verify_grace", "1s"))
                                           .start()))
        {
            IInvokableInstance node = cluster.get(1);
            int configured = throughput(node);
            FaultSupport.injectSensors(node);
            node.runOnInstance(() -> {
                FaultSupport.inRegisterDetector(new FaultSupport.StallingDetector());
                FaultSupport.inRecordTransitions();
            });
            TimeUUID id = (TimeUUID) FaultSupport.driveToVerifying(cluster, 1, configured)[0];

            // the fault: the next control tick blocks inside a detector; the backlog stays, so a running loop would act
            node.runOnInstance(FaultSupport.StallingDetector::stall);
            await("the control thread to stall", () -> node.callOnInstance(() -> FaultSupport.StallingDetector.entered));
            long end = System.currentTimeMillis() + 60_000;
            while (inState(cluster, 1, KNOB, "ROLLED_BACK") == null)
            {
                assertTrue("no rollback by the dead-man deadline: " + describe(FaultSupport.applied(cluster, 1, KNOB).toArray(new Object[0][])),
                           System.currentTimeMillis() < end);
                push(node, 20);
                Thread.sleep(500);
            }
            assertTrue("the stall was not diagnosed", node.callOnInstance(() -> FaultSupport.inDiagnosed(LOOP_STALLED)));
            assertTrue("still stalled", node.callOnInstance(() -> FaultSupport.StallingDetector.stalled != null));

            // in flight restored, by the deadline and not by a verdict
            Object[] row = inState(cluster, 1, KNOB, "ROLLED_BACK");
            assertEquals(id, row[0]);
            assertEquals("DEADLINE", row[2]);
            assertEquals(null, row[3]);
            assertEquals(configured, throughput(node));
            assertContains("audit trail", events(cluster, 1, id.asUUID()), "APPLYING", "VERIFYING", "ROLLING_BACK", "ROLLED_BACK");

            // the stalled tick resumes with what it saw before the stall, and the loop goes on: nothing is applied
            int ticksBefore = node.callOnInstance(() -> FaultSupport.StallingDetector.ticks.get());
            node.runOnInstance(FaultSupport.StallingDetector::release);
            await("the stall to clear", () -> !node.callOnInstance(() -> FaultSupport.inDiagnosed(LOOP_STALLED)));
            // the control thread drains the diagnosis writes it queued while stalled at the end of its tick
            await("loop_stalled to be persisted", () -> diagnoses(cluster, 1, LOOP_STALLED).length > 0);
            // three more complete ticks, each of which saw the backlog
            while (node.callOnInstance(() -> FaultSupport.StallingDetector.ticks.get()) < ticksBefore + 4)
            {
                assertTrue("the control thread did not resume", System.currentTimeMillis() < end + 60_000);
                push(node, 20);
                Thread.sleep(500);
            }
            List<Object[]> appliedRows = applied(cluster, 1, KNOB);
            assertEquals("only the one action was applied: " + describe(appliedRows.toArray(new Object[0][])), 1, appliedRows.size());
            assertEquals(configured, throughput(node));
            List<String> trail = node.callOnInstance(() -> new ArrayList<>(FaultSupport.trail));
            for (String t : trail)
                assertTrue("a transition of another action reached the actuator thread: " + trail,
                           t.startsWith(id.asUUID().toString()) || t.contains(":REJECTED:") || t.contains(":PROPOSED:"));
        }
    }
}
