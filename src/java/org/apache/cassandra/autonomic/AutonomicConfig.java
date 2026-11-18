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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.DataRateSpec;
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

    public DurationSpec.IntMillisecondsBound monitor_interval = new DurationSpec.IntMillisecondsBound("5s");
    public DurationSpec.IntMillisecondsBound analyze_interval = new DurationSpec.IntMillisecondsBound("15s");
    public DurationSpec.IntMillisecondsBound startup_grace = new DurationSpec.IntMillisecondsBound("5m");
    public DurationSpec.IntMillisecondsBound observation_retention = new DurationSpec.IntMillisecondsBound("30m");
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
    /** Adaptive schema (D6). Read on every node, whatever {@link #enabled} says: routing is unconditional. */
    public final LayoutConfig layout = new LayoutConfig();

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

    public static class LayoutConfig
    {
        /** A read of an adapted partition that asks each bucket for at most this many rows holds them in memory. */
        public int fanout_eager_rows = 10000;
        /** Larger reads fetch each bucket in chunks of this many rows and merge them as they are consumed. */
        public int fanout_chunk_rows = 1000;
        /** Bytes per second this node copies into the overflow table while it backfills a partition; 0 is unlimited. */
        public DataRateSpec.LongBytesPerSecondBound backfill_throughput = new DataRateSpec.LongBytesPerSecondBound("1MiB/s");
        /** Rows and range tombstone markers a backfill reads, writes at ALL and records as copied in one step. */
        public int backfill_chunk_rows = 1000;

        // the owner of a partition split (T6.9); read only on the node that drives the migration
        /** Buckets of a split when the operator names none. */
        public int default_buckets = 8;
        /** Tables ({@code keyspace.table}) that are never split (ADR-024, decision 1). */
        public List<String> excluded_tables = new ArrayList<>();
        /** How often the owner asks every node for its epoch (rule O1) and re-checks what it waits for. */
        public DurationSpec.IntMillisecondsBound ack_poll_interval = new DurationSpec.IntMillisecondsBound("5s");
        /** Lower bound of the wait in ANNOUNCED after every node acknowledged it (design 4.1, ADR-028). */
        public DurationSpec.IntMillisecondsBound cas_drain = new DurationSpec.IntMillisecondsBound("15s");
        /** Rows per page of the equivalence check in VERIFY. */
        public int verify_page_rows = 1000;
        /** Delay before a row that differed is read again. */
        public DurationSpec.IntMillisecondsBound verify_recheck_delay = new DurationSpec.IntMillisecondsBound("2s");
        /** Backfill generations started again after a failed verification before the migration is aborted (ADR-030). */
        public int verify_retries = 1;
        /** How long CUTOVER is held after every node acknowledged it, before the commit point. */
        public DurationSpec.IntMillisecondsBound cutover_soak = new DurationSpec.IntMillisecondsBound("10m");
        /** {@code manual}: the commit point also waits for {@code nodetool autonomic approve <id>} (ADR-024, decision 8). */
        public String commit_approval = "manual";
        /** How long CLEANUP waits for late base-only writes before the reconcile pass (design 4.6). */
        public DurationSpec.LongMillisecondsBound straggler_window = new DurationSpec.LongMillisecondsBound("3h");
        /**
         * What the commit point keeps free for the reconcile pass of CLEANUP, its acknowledgement and FinishKey: the
         * commit is taken only if {@code straggler_window} plus this (and two acknowledgement polls) still ends before
         * the replicas' tombstone deadline (ADR-031); otherwise the migration rolls back from CUTOVER.
         */
        public DurationSpec.LongMillisecondsBound reconcile_budget = new DurationSpec.LongMillisecondsBound("30m");
        /** Longest a migration may spend before the commit point, counted from DUAL_WRITE (and in ANNOUNCED from registration). */
        public DurationSpec.LongMillisecondsBound max_migration_duration = new DurationSpec.LongMillisecondsBound("24h");

        // triggering (T6.11); read on the node that proposes and on the node that owns the action
        /** Evaluations a {@code hot_partition} diagnosis is open before the policy looks at it (design 7.1). */
        public int for_evaluations = 20;
        /** A partition with fewer rows than this is a small hot partition (shape C): not split (design 7.2). */
        public int min_rows_for_split = 10000;
        /** Rows the size probe counts at most; a larger partition is not proposed automatically. */
        public int max_probe_rows = 5_000_000;
        /** Smallest fraction of a key's operations that touch a single bucket (named-row reads and row writes). */
        public double min_point_op_fraction = 0.8;
        /** The share of the table's operations a bucket should stay below when the number of buckets is chosen. */
        public double target_share = 0.05;
        /** How long the profiler counts the shape of the key's operations before the policy decides. */
        public DurationSpec.IntMillisecondsBound profile_window = new DurationSpec.IntMillisecondsBound("5m");
        /** Operations the profile, and each evaluation window of design 7.3, needs before it is judged. */
        public int min_samples = 100;
        /** Stage A and B: the key's read latency may grow by this fraction of its baseline (1.0 is 2x). */
        public double read_regression_tolerance = 1.0;
        /** Stage B: the busiest partition's share of the table's operations must fall by this fraction. */
        public double min_improvement = 0.5;
        /** Stage B: the window, ending when the key is STEADY, over which the signals are compared with the baseline. */
        public DurationSpec.IntMillisecondsBound verify_window = new DurationSpec.IntMillisecondsBound("10m");
    }

    public static class ScaleOutConfig
    {
        public String provider = "org.apache.cassandra.autonomic.elastic.NoopProvider";
        public Map<String, String> parameters = new LinkedHashMap<>();
        public DurationSpec.IntSecondsBound provision_timeout = new DurationSpec.IntSecondsBound("15m");
        public DurationSpec.IntSecondsBound join_timeout = new DurationSpec.IntSecondsBound("6h");
        /** How long after the join the goal signal is compared with its baseline. */
        public DurationSpec.IntSecondsBound verify_window = new DurationSpec.IntSecondsBound("1h");
        /** Bound of one call of the provider; a call that overruns is interrupted. */
        public DurationSpec.IntMillisecondsBound provider_call_timeout = new DurationSpec.IntMillisecondsBound("10s");
        /** How often the provider's status and the directory are read while an action waits. */
        public DurationSpec.IntMillisecondsBound poll_interval = new DurationSpec.IntMillisecondsBound("15s");
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
        m.put("partition_split", options("enabled", false));
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
