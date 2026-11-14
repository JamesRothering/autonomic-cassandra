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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;

public class AdvertisedSeverityActuatorTest extends AbstractKnobContractTest
{
    private final FakeHandle handle = new FakeHandle(0);
    private final KnobActuator knob = new AdvertisedSeverityActuator(handle);

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Override
    protected KnobActuator knob()
    {
        return knob;
    }

    @Override
    protected FakeHandle handle()
    {
        return handle;
    }

    @Test
    public void stepsInTenthsInsideZeroToOne()
    {
        assertEquals(0, knob.min(), 0);
        assertEquals(1, knob.max(), 0);
        assertEquals(java.util.Optional.of(0.1), knob.nextTarget(KnobActuator.Direction.UP, 0.25, null));
        handle.value = 0.95;
        assertEquals(java.util.Optional.of(1.0), knob.nextTarget(KnobActuator.Direction.UP, 0.25, null));
        handle.value = 1.0;
        assertEquals(java.util.Optional.empty(), knob.nextTarget(KnobActuator.Direction.UP, 0.25, null));
        handle.value = 0.5;
        assertEquals(java.util.Optional.of(0.375), knob.nextTarget(KnobActuator.Direction.DOWN, 0.25, null));
        handle.value = 0.05;
        assertEquals(java.util.Optional.of(0.0), knob.nextTarget(KnobActuator.Direction.DOWN, 0.25, null));
    }
}
