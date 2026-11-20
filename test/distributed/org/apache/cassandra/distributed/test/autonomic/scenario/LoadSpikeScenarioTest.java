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

import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.autonomic.monitor.InjectedSensor;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.ClientResourceLimits;

import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.actions;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.await;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.describe;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.diagnoses;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.inState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * D7 scenario, load spike on node 1 of two (node 2 stays idle): the read queue grows and the coordinator latency
 * breaches its SLO. The load sensors are injected, so the spike is exact; the signals follow the live knobs (the queue
 * falls with the read pool size, the latency with the rate limit). Asserts the load spike ladder of overview 8.1:
 * the {@code pool_saturation} diagnosis, then the first action, {@code read_pool} grown inside its envelope and
 * committed with the verdict {@code IMPROVED}; with the pool at the top of its envelope and the latency still above
 * its SLO, the next step, {@code request_throttle}, enabled below the demand and committed with the verdict
 * {@code IMPROVED}; and, when the spike is over, the pool back at its configured size and the limiter released.
 * Node 2 is not touched.
 */
public class LoadSpikeScenarioTest extends TestBaseImpl
{
    private static final int CONFIGURED = 4;
    private static final int DEMAND = 4000;
    private static final int FLOOR = 2048;
    private static final int CONFIGURED_LIMIT = 1_000_000;
    private static InjectedSensor injected;
    private static double backlog;

    private static void fast(IInstanceConfig c)
    {
        c.set("concurrent_reads", CONFIGURED)
         .set("autonomic.enabled", true)
         .set("autonomic.dry_run", false)
         .set("autonomic.startup_grace", "1s")
         .set("autonomic.monitor_interval", "500ms")
         .set("autonomic.analyze_interval", "500ms")
         .set("autonomic.sampling.enabled", false)
         .set("autonomic.safety.cooldown_after_commit", "1s")
         .set("autonomic.safety.max_actions_per_hour", 100)
         .set("autonomic.safety.max_actions_per_actuator_per_hour", 100)
         .set("autonomic.detectors", Map.of("pool_saturation", Map.of("pending_tasks", 10, "for_evaluations", 3),
                                            "latency_slo_breach", Map.of("for_evaluations", 3)))
         .set("autonomic.actuators", Map.of("read_pool", Map.of("enabled", true, "baseline_window", "2s", "observation_window", "2s"),
                                            "request_throttle", Map.of("enabled", true, "min_requests_per_second", FLOOR, "increase_step", 512,
                                                                       "baseline_window", "2s", "observation_window", "2s")));
    }

    private static int readPool(Cluster cluster, int node)
    {
        return cluster.get(node).callOnInstance(() -> StorageService.instance.getConcurrency(List.of("ReadStage")).get("ReadStage").get(1));
    }

    /** {@code enabled@limit} of the rate limiter of node 1. */
    private static String limiter(Cluster cluster)
    {
        return cluster.get(1).callOnInstance(() -> StorageService.instance.getNativeTransportRateLimitingEnabled() + "@"
                                                   + ClientResourceLimits.getNativeTransportMaxRequestsPerSecond());
    }

    /** One tick of the spike: the queue falls with the cube of the pool size and then grows once the pool is at its limit, the latency falls with the limit. */
    private static void spike(Cluster cluster, boolean over)
    {
        int size = readPool(cluster, 1);
        String state = limiter(cluster);
        int limit = state.startsWith("true") ? Integer.parseInt(state.substring(5)) : DEMAND;
        // at the top of the envelope the queue keeps growing: the pool cannot absorb the spike, which is what calls for the throttle
        // once the last growth step is verified
        boolean settled = size == 2 * CONFIGURED && inState(cluster, 1, "read_pool", "APPLYING") == null && inState(cluster, 1, "read_pool", "VERIFYING") == null;
        backlog = over ? 0 : settled ? backlog + 5 : 0;
        double pending = over ? 0 : 100.0 * Math.pow((double) CONFIGURED / size, 3) + backlog;
        double served = over ? 500 : Math.min(DEMAND, limit);
        double latency = over ? 10_000 : 600_000.0 * served / DEMAND;
        cluster.get(1).runOnInstance(() -> {
            Subject node = Subject.node();
            injected.push(Signals.POOL_PENDING_TASKS, Subject.threadPool("ReadStage"), pending)
                    .push(Signals.OS_LOAD_PER_PROCESSOR, node, 0.2)
                    .push(Signals.CLIENT_READ_OPS_PER_SEC, node, served)
                    .push(Signals.CLIENT_READ_LATENCY_P99_MICROS, node, latency)
                    .push(Signals.CLIENT_WRITE_LATENCY_P99_MICROS, node, 1000);
        });
    }

    private static int committedImproved(Cluster cluster, String actuator)
    {
        int n = 0;
        for (Object[] row : actions(cluster, 1, actuator))
            if ("COMMITTED".equals(row[1]) && "IMPROVED".equals(row[3]))
                n++;
        return n;
    }

    @Test
    public void aLoadSpikeGrowsThePoolThenThrottlesAndRecovers() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).withConfig(LoadSpikeScenarioTest::fast).start()))
        {
            cluster.get(1).runOnInstance(() -> {
                AutonomicService.instance.removeSensor("thread_pool");
                AutonomicService.instance.removeSensor("client_request");
                AutonomicService.instance.removeSensor("cpu");
                injected = new InjectedSensor();
                AutonomicService.instance.registerSensor(injected);
            });
            assertEquals(CONFIGURED, readPool(cluster, 1));
            assertEquals("false@" + CONFIGURED_LIMIT, limiter(cluster));

            // the spike, until the pool has grown and the limiter has an improving committed action
            int highest = CONFIGURED;
            int lowestLimit = CONFIGURED_LIMIT;
            long end = System.currentTimeMillis() + 4 * ScenarioSupport.DEADLINE_MILLIS / 3;
            while (System.currentTimeMillis() < end && !(committedImproved(cluster, "read_pool") > 0 && committedImproved(cluster, "request_throttle") > 0
                                                       && highest == 2 * CONFIGURED))
            {
                int size = readPool(cluster, 1);
                assertTrue("pool size " + size + " is outside the envelope", size >= CONFIGURED / 2 && size <= 2 * CONFIGURED);
                highest = Math.max(highest, size);
                String state = limiter(cluster);
                if (state.startsWith("true"))
                    lowestLimit = Math.min(lowestLimit, Integer.parseInt(state.substring(5)));
                spike(cluster, false);
                Thread.sleep(300);
            }
            String trail = "read_pool " + describe(actions(cluster, 1, "read_pool")) + "\nrequest_throttle " + describe(actions(cluster, 1, "request_throttle"));
            assertEquals("the pool did not reach the top of its envelope:\n" + trail, 2 * CONFIGURED, highest);
            assertTrue("no improving committed read_pool action:\n" + trail, committedImproved(cluster, "read_pool") > 0);
            assertTrue("no improving committed request_throttle action:\n" + trail, committedImproved(cluster, "request_throttle") > 0);
            assertTrue("the limiter went below the floor: " + lowestLimit, lowestLimit >= FLOOR && lowestLimit < DEMAND);

            // diagnoses of the spike
            assertTrue("no pool_saturation diagnosis", diagnoses(cluster, 1, "pool_saturation").length > 0);
            assertTrue("no latency_slo_breach diagnosis", diagnoses(cluster, 1, "latency_slo_breach").length > 0);

            // the spike is over: the limiter is released and the pool returns to its configured size
            await("the limiter to be released and the pool to return to " + CONFIGURED, () -> {
                spike(cluster, true);
                return limiter(cluster).startsWith("false") && readPool(cluster, 1) == CONFIGURED;
            });
            assertEquals("false@" + CONFIGURED_LIMIT, limiter(cluster));

            // node 2 saw none of it
            assertEquals(CONFIGURED, readPool(cluster, 2));
            assertEquals(0, actions(cluster, 2, "read_pool").length);
            assertEquals(0, actions(cluster, 2, "request_throttle").length);
        }
    }
}
