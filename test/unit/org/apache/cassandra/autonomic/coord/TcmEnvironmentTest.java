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
package org.apache.cassandra.autonomic.coord;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TcmEnvironmentTest
{
    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.toolInitialization();
    }

    /**
     * A step whose ledger commit gives up must end before the actuator-stall check, whose threshold is
     * {@code action_step_timeout}, pauses the layer: the commit leaves half of the step for the store writes around it.
     */
    @Test
    public void commitBoundLeavesHalfOfTheStallThreshold()
    {
        AutonomicConfig defaults = new AutonomicConfig();
        assertBound(defaults, 30_000);

        AutonomicConfig small = new AutonomicConfig();
        small.safety.action_step_timeout = new DurationSpec.IntSecondsBound("1s");
        assertBound(small, 1_000);

        AutonomicConfig fault = new AutonomicConfig();
        fault.safety.action_step_timeout = new DurationSpec.IntSecondsBound("5s");
        assertBound(fault, 5_000);
    }

    private static void assertBound(AutonomicConfig config, long stallThresholdMillis)
    {
        assertEquals(stallThresholdMillis, config.safety.action_step_timeout.toMilliseconds());
        long bound = new TcmEnvironment(() -> 0L, () -> config).commitTimeoutMillis();
        assertTrue("bound " + bound + " of " + stallThresholdMillis, bound > 0 && 2 * bound <= stallThresholdMillis);
    }
}
