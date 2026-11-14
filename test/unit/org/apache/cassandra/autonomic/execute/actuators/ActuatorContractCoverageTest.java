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
package org.apache.cassandra.autonomic.execute.actuators;

import java.util.HashSet;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.execute.AbstractActuatorContractTest;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * D4 criterion 1 as a guard: every actuator the layer builds has a unit test class that applies a change and restores
 * the previous value on rollback (the shared contract of overview 7.3), so a new actuator without one fails here.
 * The irreversible actuators have no rollback by design and are named, so that a new irreversible one is a decision.
 */
public class ActuatorContractCoverageTest
{
    private static final Set<String> IRREVERSIBLE = Set.of(GarbageCollectActuator.NAME, RepairPriorityActuator.NAME);

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void everyBuiltActuatorHasItsContractTest() throws Exception
    {
        Set<String> tested = new HashSet<>();
        for (Actuator actuator : Actuators.build(new AutonomicConfig()))
        {
            Class<?> test = Class.forName(actuator.getClass().getName() + "Test");
            if (actuator.reversible())
                assertTrue(test + " must extend the contract test", AbstractActuatorContractTest.class.isAssignableFrom(test));
            else
                assertTrue(actuator.name() + " is irreversible and must be listed", IRREVERSIBLE.contains(actuator.name()));
            tested.add(actuator.name());
        }
        assertEquals(Set.of("compaction_throughput", "concurrent_compactors", "stream_throughput", "hint_throttle", "key_cache_capacity",
                            "row_cache_capacity", "counter_cache_capacity", "advertised_severity", "garbage_collect", "repair_priority"),
                     tested);
    }
}
