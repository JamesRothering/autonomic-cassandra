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

import java.util.Arrays;
import java.util.function.BooleanSupplier;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

/**
 * What the D7 scenario tests share: polling with a deadline and reading the node-local audit tables of one node.
 */
final class ScenarioSupport
{
    /** Per wait; a scenario as a whole stays well under the five minutes of the D7 acceptance. */
    static final long DEADLINE_MILLIS = 90_000;

    private ScenarioSupport()
    {
    }

    static void await(String what, BooleanSupplier condition) throws InterruptedException
    {
        long end = System.currentTimeMillis() + DEADLINE_MILLIS;
        while (!condition.getAsBoolean())
        {
            if (System.currentTimeMillis() > end)
                throw new AssertionError("timed out waiting for " + what);
            Thread.sleep(200);
        }
    }

    /** Rows of {@code system_autonomic.diagnoses} of the type on the node: subject, state, summary, evidence. */
    static Object[][] diagnoses(Cluster cluster, int node, String type)
    {
        return cluster.coordinator(node).execute("SELECT subject, state, summary, evidence FROM system_autonomic.diagnoses WHERE type = '" + type
                                                 + "' ALLOW FILTERING", ConsistencyLevel.ONE);
    }

    /** Rows of {@code system_autonomic.actions} of the actuator on the node: id, state, state_reason, verdict, params, undo. */
    static Object[][] actions(Cluster cluster, int node, String actuator)
    {
        return cluster.coordinator(node).execute("SELECT id, state, state_reason, verdict, params, undo FROM system_autonomic.actions WHERE actuator = '"
                                                 + actuator + "' ALLOW FILTERING", ConsistencyLevel.ONE);
    }

    /** The first action of the actuator in the state, or null. */
    static Object[] inState(Cluster cluster, int node, String actuator, String state)
    {
        for (Object[] row : actions(cluster, node, actuator))
            if (state.equals(row[1]))
                return row;
        return null;
    }

    static String describe(Object[][] rows)
    {
        return Arrays.deepToString(rows);
    }
}
