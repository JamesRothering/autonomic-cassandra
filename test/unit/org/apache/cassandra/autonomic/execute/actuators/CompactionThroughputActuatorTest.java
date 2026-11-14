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

import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CompactionThroughputActuatorTest extends AbstractKnobContractTest
{
    private final FakeHandle handle = new FakeHandle(64);
    private final KnobActuator knob = new CompactionThroughputActuator(handle, Map.of());

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
    public void envelopeIsFactorsOfConfiguredValue()
    {
        assertEquals(16, knob.min(), 0);
        assertEquals(256, knob.max(), 0);
        KnobActuator custom = new CompactionThroughputActuator(new FakeHandle(64), Map.of("min_factor", 0.5, "max_factor", 2.0));
        assertEquals(32, custom.min(), 0);
        assertEquals(128, custom.max(), 0);
    }

    @Test
    public void unthrottledIsNotManaged()
    {
        KnobActuator unlimited = new CompactionThroughputActuator(new FakeHandle(0), Map.of());
        assertFalse(unlimited.managed());
    }

    /** The production handle reads and writes the live value; revert restores the one configured at start. */
    @Test
    public void realHandleIsRevertedToItsConfiguredValue() throws Exception
    {
        KnobActuator real = CompactionThroughputActuator.create(Map.of());
        int configured = StorageService.instance.getCompactionThroughputMbPerSec();
        assertEquals(configured, real.configured(), 0);
        StorageService.instance.setCompactionThroughputMbPerSec(configured + 7);
        assertTrue(real.drifted());
        assertTrue(real.revert());
        assertEquals(configured, StorageService.instance.getCompactionThroughputMbPerSec());
        assertFalse(real.revert());
    }
}
