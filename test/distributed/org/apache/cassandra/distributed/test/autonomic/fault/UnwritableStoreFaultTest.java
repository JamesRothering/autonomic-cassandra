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
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.KNOB;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.applied;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.events;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.localState;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.push;
import static org.apache.cassandra.distributed.test.autonomic.fault.FaultSupport.throughput;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.DEADLINE_MILLIS;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.await;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.describe;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * T7.2 fault 2, an unwritable knowledge store (overview 7.5, 7.2): the store fails exactly between the write-ahead of
 * an action (APPLYING with its {@code inflight/<id>} key) and the write of VERIFYING, after the knob was changed. The
 * undo is not durable, so the action is rolled back at once; while the store stays unwritable no action is approved
 * (governor check 6) and diagnoses go on in memory. Once the store is writable again, the next rejection finds it so;
 * the layer then rewrites the row left at APPLYING and deletes its inflight key (T7.5).
 */
public class UnwritableStoreFaultTest extends TestBaseImpl
{
    private static List<String> trailOf(IInvokableInstance node, String id)
    {
        return node.callOnInstance(() -> new ArrayList<>(FaultSupport.trail)).stream()
                   .filter(t -> t.startsWith(id))
                   .map(t -> t.substring(id.length() + 1))
                   .collect(Collectors.toList());
    }

    private static String proposeRaise(IInvokableInstance node, int configured)
    {
        String value = String.valueOf(configured + 1);
        String diagnosis = UUID.randomUUID().toString();
        return node.callOnInstance(() -> FaultSupport.inPropose(KNOB, "NODE", "NODE", Map.of("value", value, "diagnosis_id", diagnosis), 5000, 10_000));
    }

    @Test
    public void anUnwritableStoreRestoresTheUnrecordedChangeAndApprovesNothing() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).withConfig(FaultSupport::nodeConfig).start()))
        {
            IInvokableInstance node = cluster.get(1);
            int configured = throughput(node);
            FaultSupport.injectSensors(node);
            node.runOnInstance(() -> {
                FaultSupport.inRecordTransitions();
                FaultSupport.StoreFault.downAtWriteOf = "VERIFYING";
                FaultSupport.StoreFault.install(AutonomicService.instance.knowledgeStore());
            });

            // the policy raises the throughput; the store fails at the VERIFYING write
            String id = null;
            long end = System.currentTimeMillis() + DEADLINE_MILLIS;
            while (id == null)
            {
                assertTrue("no rollback: " + node.callOnInstance(() -> new ArrayList<>(FaultSupport.trail)), System.currentTimeMillis() < end);
                push(node, 20);
                Thread.sleep(500);
                for (String t : node.callOnInstance(() -> new ArrayList<>(FaultSupport.trail)))
                    if (t.contains(":ROLLED_BACK:"))
                        id = t.substring(0, t.indexOf(':'));
            }
            assertTrue(node.callOnInstance(() -> FaultSupport.StoreFault.down));

            // in flight restored: the change was made and undone, in memory, without a durable VERIFYING
            List<String> trail = trailOf(node, id);
            List<String> states = trail.stream().map(t -> t.substring(0, t.lastIndexOf(':'))).collect(Collectors.toList());
            assertEquals("in-memory trail " + trail, List.of("PROPOSED:null", "APPROVED:null", "APPLYING:null",
                                                             "ROLLING_BACK:STORE_UNAVAILABLE", "ROLLED_BACK:STORE_UNAVAILABLE"), states);
            int whileRollingBack = Integer.parseInt(trail.get(3).substring(trail.get(3).lastIndexOf(':') + 1));
            assertTrue("the knob was raised before the rollback: " + trail, whileRollingBack > configured);
            assertEquals(configured, throughput(node));
            String uuid = id;
            assertEquals("ROLLED_BACK:STORE_UNAVAILABLE:null", node.callOnInstance(() -> FaultSupport.inMemoryRecord(UUID.fromString(uuid))));
            assertEquals("false", node.callOnInstance(() -> AutonomicService.instance.getStatus().get("store_available")));
            // on disk only the write-ahead: the row at APPLYING and the inflight key
            List<Object[]> rows = applied(cluster, 1, KNOB);
            assertEquals(describe(rows.toArray(new Object[0][])), 1, rows.size());
            assertEquals("APPLYING", rows.get(0)[1]);
            assertNotNull(localState(cluster, 1, "inflight/" + id));

            // nothing new while unwritable: a new proposal is rejected by check 6, and the backlog is still diagnosed
            String rejected = proposeRaise(node, configured);
            assertTrue(rejected, rejected.startsWith("REJECTED|STORE_UNAVAILABLE|"));
            assertTrue(node.callOnInstance(() -> FaultSupport.inDiagnosed("compaction_backlog")));
            assertEquals(configured, throughput(node));
            assertEquals(1, applied(cluster, 1, KNOB).size());

            // the store comes back: the next rejection is the probe that finds it writable
            node.runOnInstance(() -> {
                FaultSupport.StoreFault.downAtWriteOf = null;
                FaultSupport.StoreFault.down = false;
            });
            String probe = proposeRaise(node, configured);
            assertTrue(probe, probe.startsWith("REJECTED|STORE_UNAVAILABLE|"));
            assertEquals("true", node.callOnInstance(() -> AutonomicService.instance.getStatus().get("store_available")));
            // the probe's write succeeded, so the layer rewrote the latest state of the restored action (T7.5): the
            // row is closed and its inflight key deleted without a restart
            await("the stale row is written", () -> "ROLLED_BACK".equals(applied(cluster, 1, KNOB).get(0)[1]) && localState(cluster, 1, "inflight/" + uuid) == null);
            rows = applied(cluster, 1, KNOB);
            assertEquals(describe(rows.toArray(new Object[0][])), 1, rows.size());
            assertEquals("STORE_UNAVAILABLE", rows.get(0)[2]);
            assertEquals(List.of("PROPOSED", "APPROVED", "APPLYING", "ROLLED_BACK"), events(cluster, 1, UUID.fromString(uuid)));

            // a restart finds nothing left to recover
            ClusterUtils.restartUnchecked(node);
            rows = applied(cluster, 1, KNOB);
            assertEquals(describe(rows.toArray(new Object[0][])), 1, rows.size());
            assertEquals("ROLLED_BACK", rows.get(0)[1]);
            assertEquals("STORE_UNAVAILABLE", rows.get(0)[2]);
            assertEquals(null, localState(cluster, 1, "inflight/" + uuid));
            assertEquals(List.of("PROPOSED", "APPROVED", "APPLYING", "ROLLED_BACK"), events(cluster, 1, UUID.fromString(uuid)));
            assertEquals(configured, throughput(node));
        }
    }
}
