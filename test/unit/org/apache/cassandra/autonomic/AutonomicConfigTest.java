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
package org.apache.cassandra.autonomic;

import java.util.Map;

import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.YamlConfigurationLoader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AutonomicConfigTest
{
    private static final String YAML =
    "autonomic:\n" +
    "  enabled: true\n" +
    "  dry_run: false\n" +
    "  monitor_interval: 7s\n" +
    "  analyze_interval: 15s\n" +
    "  startup_grace: 10m\n" +
    "  observation_retention: 30m\n" +
    "  max_series: 1234\n" +
    "  history_ttl: 30d\n" +
    "  slo:\n" +
    "    read_latency_p99: 80ms\n" +
    "    write_latency_p99: 20ms\n" +
    "  sampling:\n" +
    "    enabled: true\n" +
    "    window: 10s\n" +
    "    interval: 60s\n" +
    "    min_ops_per_sec: 100\n" +
    "    capacity: 256\n" +
    "  safety:\n" +
    "    max_actions_per_hour: 3\n" +
    "    max_step_fraction: 0.5\n" +
    "    cooldown_after_commit: 10m\n" +
    "    cooldown_max: 6h\n" +
    "    node_approval: manual\n" +
    "    cluster_approval: manual\n" +
    "    action_step_timeout: 30s\n" +
    "    sensor_budget: 200ms\n" +
    "  coordination:\n" +
    "    lease: 10m\n" +
    "    lease_safety_margin: 60s\n" +
    "  detectors:\n" +
    "    hot_partition: { enabled: true, share_of_table_ops: 0.3, for_evaluations: 4 }\n" +
    "    compaction_backlog: { enabled: false, pending_tasks: 100 }\n" +
    "  actuators:\n" +
    "    compaction_throughput: { enabled: true, min_factor: 0.25, max_factor: 4.0 }\n" +
    "  scale_out:\n" +
    "    provider: org.apache.cassandra.autonomic.elastic.NoopProvider\n" +
    "    parameters: {}\n" +
    "    provision_timeout: 15m\n" +
    "    join_timeout: 6h\n";

    @Test
    public void defaults()
    {
        AutonomicConfig c = new Config().autonomic;
        assertFalse(c.enabled);
        assertTrue(c.dry_run);
        assertEquals(5000, c.monitor_interval.toMilliseconds());
        assertEquals(300_000, c.startup_grace.toMilliseconds());
        assertEquals(5000, c.max_series);
        assertEquals(50, c.slo.read_latency_p99.toMilliseconds());
        assertEquals("manual", c.safety.cluster_approval);
        assertFalse((Boolean) c.actuators.get("scale_out").get("enabled"));
    }

    @Test
    public void parsesDocumentedSection()
    {
        Map<String, Object> map = new Yaml().load(YAML);
        AutonomicConfig c = YamlConfigurationLoader.fromMap(map, true, Config.class).autonomic;
        assertTrue(c.enabled);
        assertFalse(c.dry_run);
        assertEquals(7000, c.monitor_interval.toMilliseconds());
        assertEquals(600_000, c.startup_grace.toMilliseconds());
        assertEquals(1234, c.max_series);
        assertEquals(30L * 24 * 3600, c.history_ttl.toSeconds());
        assertEquals(80, c.slo.read_latency_p99.toMilliseconds());
        assertEquals(256, c.sampling.capacity);
        assertEquals(3, c.safety.max_actions_per_hour);
        assertEquals(0.5, c.safety.max_step_fraction, 0.0);
        assertEquals("manual", c.safety.node_approval);
        assertEquals(6 * 3600, c.safety.cooldown_max.toSeconds());
        assertEquals(200, c.safety.sensor_budget.toMilliseconds());
        assertEquals(60, c.coordination.lease_safety_margin.toSeconds());
        assertFalse((Boolean) c.detectors.get("compaction_backlog").get("enabled"));
        assertEquals(4, ((Number) c.detectors.get("hot_partition").get("for_evaluations")).intValue());
        assertTrue((Boolean) c.actuators.get("compaction_throughput").get("enabled"));
        assertEquals(6 * 3600, c.scale_out.join_timeout.toSeconds());
    }
}
