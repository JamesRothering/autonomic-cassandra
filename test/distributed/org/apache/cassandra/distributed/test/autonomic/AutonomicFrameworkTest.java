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

import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end demonstration of the three D2 acceptance criteria on single-node clusters.
 * Criterion 2 (kill stops actuators within one control interval) is verified at unit level by
 * {@code AutonomicExecutionTest#killRollsBackInFlightActionsAndSurvivesRestart}; this class adds the
 * operator-visible kill check.
 */
public class AutonomicFrameworkTest extends TestBaseImpl
{
    private static final Set<String> EXPECTED_AUTONOMIC_TABLES = Set.of("system_autonomic.diagnoses",
                                                                         "system_autonomic.actions",
                                                                         "system_autonomic.action_events",
                                                                         "system_autonomic.local_state",
                                                                         "system_views.autonomic_status",
                                                                         "system_views.autonomic_components",
                                                                         "system_views.autonomic_signals",
                                                                         "system_views.autonomic_diagnoses",
                                                                         "system_views.autonomic_actions",
                                                                         "system_views.autonomic_cluster_ledger");

    /** Criterion 1: nothing active and no tables beyond the knowledge store and virtual tables. */
    @Test
    public void defaultConfigurationHasNothingActive() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            Set<String> threads = cluster.get(1).callOnInstance(() -> Thread.getAllStackTraces().keySet().stream()
                                                                           .map(Thread::getName)
                                                                           .filter(n -> n.contains("Autonomic"))
                                                                           .collect(Collectors.toCollection(TreeSet::new)));
            assertTrue("unexpected threads: " + threads, threads.isEmpty());

            cluster.get(1).nodetoolResult("autonomic", "status").asserts().success()
                   .stdoutContains("Enabled: false").stdoutContains("Armed: false");

            Set<String> tables = new TreeSet<>();
            for (Object[] row : cluster.coordinator(1).execute("SELECT keyspace_name, table_name FROM system_schema.tables", ConsistencyLevel.ONE))
                tables.add(row[0] + "." + row[1]);
            // local system keyspaces are not listed in system_schema, so read them from the in-memory schema
            tables.addAll(cluster.get(1).callOnInstance(() -> org.apache.cassandra.schema.Schema.instance
                                                              .getKeyspaceMetadata("system_autonomic").tables.stream()
                                                              .map(t -> t.keyspace + "." + t.name)
                                                              .collect(Collectors.toSet())));
            for (Object[] row : cluster.coordinator(1).execute("SELECT keyspace_name, table_name FROM system_virtual_schema.tables", ConsistencyLevel.ONE))
                tables.add(row[0] + "." + row[1]);
            Set<String> autonomic = tables.stream()
                                          .filter(t -> t.startsWith("system_autonomic.") || t.contains("autonomic"))
                                          .collect(Collectors.toCollection(TreeSet::new));
            assertEquals(new TreeSet<>(EXPECTED_AUTONOMIC_TABLES), autonomic);

            for (String t : new String[]{ "diagnoses", "actions", "action_events", "local_state" })
                assertEquals(0, cluster.coordinator(1).execute("SELECT * FROM system_autonomic." + t, ConsistencyLevel.ONE).length);
        }
    }

    /** Criteria 2 (operator view) and 3: status, pause, resume, history and kill through nodetool. */
    @Test
    public void operatorCommandsAndKill() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(c -> c.set("autonomic.enabled", true))
                                           .start()))
        {
            cluster.get(1).nodetoolResult("autonomic", "status").asserts().success()
                   .stdoutContains("Enabled: true").stdoutContains("Paused: false").stdoutContains("Killed: false");

            cluster.get(1).nodetoolResult("autonomic", "pause", "framework test").asserts().success();
            cluster.get(1).nodetoolResult("autonomic", "status").asserts().success()
                   .stdoutContains("Paused: true (reason: framework test)");

            cluster.get(1).nodetoolResult("autonomic", "resume").asserts().success();
            cluster.get(1).nodetoolResult("autonomic", "status").asserts().success().stdoutContains("Paused: false");

            cluster.get(1).nodetoolResult("autonomic", "history").asserts().success();

            cluster.get(1).nodetoolResult("autonomic", "kill", "framework test").asserts().success();
            cluster.get(1).nodetoolResult("autonomic", "status").asserts().success()
                   .stdoutContains("Killed: true").stdoutContains("Paused: true").stdoutContains("In-flight actions: 0");
        }
    }
}
