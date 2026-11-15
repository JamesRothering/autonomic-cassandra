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
import org.apache.cassandra.transport.ClientResourceLimits;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * D5 adaptive throttling on one node: an injected coordinator latency far above its SLO with a growing queue makes the
 * layer enable the native transport rate limiter below the observed request rate and lower it step by step, never below
 * the floor; when the latency recovers the limit is raised and finally the limiter is disabled with its configured
 * limit restored. The client sensor is replaced by an injected one whose latency follows the live limit.
 */
public class RequestThrottleTest extends TestBaseImpl
{
    private static final long DEADLINE_MILLIS = 60_000;
    private static final int FLOOR = 2048;
    private static final int DEMAND = 4000;
    private static final int CONFIGURED_LIMIT = 1_000_000;
    private static InjectedSensor injected;

    private static void fast(IInstanceConfig c)
    {
        c.set("autonomic.enabled", true)
         .set("autonomic.dry_run", false)
         .set("autonomic.startup_grace", "1s")
         .set("autonomic.monitor_interval", "500ms")
         .set("autonomic.analyze_interval", "500ms")
         .set("autonomic.sampling.enabled", false)
         .set("autonomic.safety.cooldown_after_commit", "1s")
         .set("autonomic.safety.max_actions_per_hour", 100)
         .set("autonomic.safety.max_actions_per_actuator_per_hour", 100)
         .set("autonomic.detectors", Map.of("latency_slo_breach", Map.of("for_evaluations", 3),
                                            "pool_saturation", Map.of("enabled", false)))
         .set("autonomic.actuators", Map.of("request_throttle", Map.of("enabled", true, "min_requests_per_second", FLOOR, "increase_step", 512,
                                                                       "baseline_window", "2s", "observation_window", "2s")));
    }

    /** {@code enabled@limit} of the rate limiter. */
    private static String limiter(Cluster cluster)
    {
        return cluster.get(1).callOnInstance(() -> StorageService.instance.getNativeTransportRateLimitingEnabled() + "@"
                                                   + ClientResourceLimits.getNativeTransportMaxRequestsPerSecond());
    }

    private static void push(Cluster cluster, double pending, double demand, double latencyMicros)
    {
        String state = limiter(cluster);
        double served = state.startsWith("true") ? Math.min(demand, Integer.parseInt(state.substring(5))) : demand;
        cluster.get(1).runOnInstance(() -> {
            Subject node = Subject.node();
            injected.push(Signals.POOL_PENDING_TASKS, Subject.threadPool("ReadStage"), pending)
                    .push(Signals.CLIENT_READ_OPS_PER_SEC, node, served)
                    .push(Signals.CLIENT_READ_LATENCY_P99_MICROS, node, latencyMicros)
                    .push(Signals.CLIENT_WRITE_LATENCY_P99_MICROS, node, 1000);
        });
    }

    @Test
    public void limiterIsLoweredToTheFloorOnBreachAndReleasedOnRecovery() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).withConfig(RequestThrottleTest::fast).start()))
        {
            cluster.get(1).runOnInstance(() -> {
                AutonomicService.instance.removeSensor("client_request");
                AutonomicService.instance.removeSensor("thread_pool");
                injected = new InjectedSensor();
                AutonomicService.instance.registerSensor(injected);
            });
            assertEquals("false@" + CONFIGURED_LIMIT, limiter(cluster));
            cluster.get(1).nodetoolResult("autonomic", "actuators").asserts().success().stdoutContains("request_throttle");

            // breach: latency is three times the floor's worth of demand over the SLO and falls with the limit
            int lowest = CONFIGURED_LIMIT;
            int pending = 0;
            long end = System.currentTimeMillis() + DEADLINE_MILLIS;
            String state = limiter(cluster);
            while (System.currentTimeMillis() < end && !state.equals("true@" + FLOOR))
            {
                if (state.startsWith("true"))
                    lowest = Math.min(lowest, Integer.parseInt(state.substring(5)));
                assertTrue("limit below the floor: " + state, lowest >= FLOOR);
                int limit = state.startsWith("true") ? Integer.parseInt(state.substring(5)) : DEMAND;
                pending += 5;
                push(cluster, pending, DEMAND, 600_000.0 * Math.min(limit, DEMAND) / DEMAND);
                Thread.sleep(500);
                state = limiter(cluster);
            }
            assertEquals("the limiter did not reach the floor: " + actions(cluster), "true@" + FLOOR, state);
            for (int i = 0; i < 12; i++)
            {
                pending += 5;
                push(cluster, pending, DEMAND, 600_000.0 * FLOOR / DEMAND);
                Thread.sleep(500);
                assertEquals("true@" + FLOOR, limiter(cluster));
            }

            // recovery: healthy latency; with demand above the limit it is raised, with demand below it the limiter is released
            boolean raised = false;
            end = System.currentTimeMillis() + DEADLINE_MILLIS;
            while (System.currentTimeMillis() < end && !limiter(cluster).startsWith("false"))
            {
                state = limiter(cluster);
                raised |= state.startsWith("true") && Integer.parseInt(state.substring(5)) > FLOOR;
                assertTrue("limit below the floor: " + state, !state.startsWith("true") || Integer.parseInt(state.substring(5)) >= FLOOR);
                push(cluster, 0, raised ? 500 : 3000, 10_000);
                Thread.sleep(500);
            }
            assertTrue("the limit was never raised before it was released: " + actions(cluster), raised);
            assertEquals("the limiter was not released: " + actions(cluster), "false@" + CONFIGURED_LIMIT, limiter(cluster));

            int committed = 0;
            for (Object[] row : cluster.coordinator(1).execute("SELECT state, params FROM system_autonomic.actions WHERE actuator = 'request_throttle' ALLOW FILTERING",
                                                               ConsistencyLevel.ONE))
            {
                @SuppressWarnings("unchecked")
                int target = Integer.parseInt(((Map<String, String>) row[1]).get("value"));
                assertTrue("target " + target, target >= FLOOR);
                if ("COMMITTED".equals(row[0]))
                    committed++;
            }
            assertTrue("committed actions: " + committed, committed >= 3);
        }
    }

    private static String actions(Cluster cluster)
    {
        return java.util.Arrays.deepToString(cluster.coordinator(1).execute("SELECT state, state_reason, verdict, params, baseline, result FROM system_autonomic.actions "
                                                                            + "WHERE actuator = 'request_throttle' ALLOW FILTERING", ConsistencyLevel.ONE));
    }
}
