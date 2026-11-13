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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.cassandra.config.DurationSpec;

/**
 * Configuration of the {@code autonomic:} section of cassandra.yaml. Plain public fields, loaded as a nested section.
 * Only {@link #enabled} and {@link #dry_run} change at runtime. New blocks (for example {@code layout:}) are added as
 * further nested static classes with a public final field here.
 */
public class AutonomicConfig
{
    /** Master switch; runtime-settable. */
    public volatile boolean enabled = false;
    /** When true no action is applied; runtime-settable. */
    public volatile boolean dry_run = true;

    public DurationSpec.IntSecondsBound monitor_interval = new DurationSpec.IntSecondsBound("5s");
    public DurationSpec.IntSecondsBound analyze_interval = new DurationSpec.IntSecondsBound("15s");
    public DurationSpec.IntSecondsBound startup_grace = new DurationSpec.IntSecondsBound("5m");
    public DurationSpec.IntSecondsBound observation_retention = new DurationSpec.IntSecondsBound("30m");
    public int max_series = 5000;
    public DurationSpec.IntSecondsBound history_ttl = new DurationSpec.IntSecondsBound("30d");

    public final SloConfig slo = new SloConfig();
    public final SamplingConfig sampling = new SamplingConfig();
    public final SafetyConfig safety = new SafetyConfig();
    public final CoordinationConfig coordination = new CoordinationConfig();

    /** Detector name to options; all enabled when the layer is enabled. */
    public Map<String, Map<String, Object>> detectors = defaultDetectors();
    /** Actuator name to options; each disabled by default. */
    public Map<String, Map<String, Object>> actuators = defaultActuators();

    public final ScaleOutConfig scale_out = new ScaleOutConfig();

    /** Inputs to detectors. */
    public static class SloConfig
    {
        public DurationSpec.IntMillisecondsBound read_latency_p99 = new DurationSpec.IntMillisecondsBound("50ms");
        public DurationSpec.IntMillisecondsBound write_latency_p99 = new DurationSpec.IntMillisecondsBound("20ms");
    }

    public static class SamplingConfig
    {
        public boolean enabled = true;
        public DurationSpec.IntSecondsBound window = new DurationSpec.IntSecondsBound("10s");
        public DurationSpec.IntSecondsBound interval = new DurationSpec.IntSecondsBound("60s");
        public int min_ops_per_sec = 100;
        public int capacity = 256;
    }

    public static class SafetyConfig
    {
        public static final String APPROVAL_AUTO = "auto";
        public static final String APPROVAL_MANUAL = "manual";

        public int max_actions_per_hour = 6;
        public int max_actions_per_actuator_per_hour = 2;
        public int max_cluster_actions_per_hour = 2;
        public double max_step_fraction = 0.25;
        public DurationSpec.IntSecondsBound cooldown_after_commit = new DurationSpec.IntSecondsBound("10m");
        public DurationSpec.IntSecondsBound cooldown_after_rollback = new DurationSpec.IntSecondsBound("30m");
        public DurationSpec.IntSecondsBound cooldown_max = new DurationSpec.IntSecondsBound("6h");
        public int quarantine_after_rollbacks = 3;
        public DurationSpec.IntSecondsBound quarantine_window = new DurationSpec.IntSecondsBound("1h");
        public DurationSpec.IntSecondsBound quarantine_duration = new DurationSpec.IntSecondsBound("6h");
        public int breaker_rollbacks = 4;
        public int breaker_window = 6;
        /** {@link #APPROVAL_AUTO} or {@link #APPROVAL_MANUAL}. */
        public String node_approval = APPROVAL_AUTO;
        public String cluster_approval = APPROVAL_MANUAL;
        public DurationSpec.IntSecondsBound proposal_ttl = new DurationSpec.IntSecondsBound("1h");
        public DurationSpec.IntSecondsBound verify_grace = new DurationSpec.IntSecondsBound("30s");
        public DurationSpec.IntSecondsBound action_step_timeout = new DurationSpec.IntSecondsBound("30s");
        public int component_failure_threshold = 5;
        public DurationSpec.IntMillisecondsBound sensor_budget = new DurationSpec.IntMillisecondsBound("200ms");
    }

    public static class CoordinationConfig
    {
        public DurationSpec.IntSecondsBound lease = new DurationSpec.IntSecondsBound("10m");
        public DurationSpec.IntSecondsBound lease_safety_margin = new DurationSpec.IntSecondsBound("60s");
    }

    public static class ScaleOutConfig
    {
        public String provider = "org.apache.cassandra.autonomic.elastic.NoopProvider";
        public Map<String, String> parameters = new LinkedHashMap<>();
        public DurationSpec.IntSecondsBound provision_timeout = new DurationSpec.IntSecondsBound("15m");
        public DurationSpec.IntSecondsBound join_timeout = new DurationSpec.IntSecondsBound("6h");
    }

    private static Map<String, Map<String, Object>> defaultDetectors()
    {
        Map<String, Map<String, Object>> m = new LinkedHashMap<>();
        m.put("hot_partition", options("enabled", true, "share_of_table_ops", 0.3, "for_evaluations", 4));
        m.put("compaction_backlog", options("enabled", true, "pending_tasks", 100));
        return m;
    }

    private static Map<String, Map<String, Object>> defaultActuators()
    {
        Map<String, Map<String, Object>> m = new LinkedHashMap<>();
        m.put("compaction_throughput", options("enabled", false, "min_factor", 0.25, "max_factor", 4.0));
        m.put("request_pools", options("enabled", false, "min_factor", 0.5, "max_factor", 2.0));
        m.put("request_throttle", options("enabled", false, "min_requests_per_second", 1000));
        m.put("scale_out", options("enabled", false));
        return m;
    }

    private static Map<String, Object> options(Object... kv)
    {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2)
            m.put((String) kv[i], kv[i + 1]);
        return m;
    }
}
