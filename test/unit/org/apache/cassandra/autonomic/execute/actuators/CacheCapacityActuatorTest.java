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
import org.apache.cassandra.service.CacheService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CacheCapacityActuatorTest extends AbstractKnobContractTest
{
    private final FakeHandle handle = new FakeHandle(100);
    private final KnobActuator knob = new CacheCapacityActuator(CacheCapacityActuator.KEY, handle, Map.of());

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
    public void envelopeAndDisabledRowCache()
    {
        assertEquals(50, knob.min(), 0);
        assertEquals(200, knob.max(), 0);
        assertFalse(new CacheCapacityActuator(CacheCapacityActuator.ROW, new FakeHandle(0), Map.of()).managed());
    }

    @Test
    public void realHandlesAreRevertedToTheirConfiguredValues() throws Exception
    {
        KnobActuator real = CacheCapacityActuator.key(Map.of());
        long configured = CacheService.instance.keyCache.getCapacity() / (1024 * 1024);
        assertEquals(configured, real.configured(), 0);
        CacheService.instance.setKeyCacheCapacityInMB(configured + 1);
        assertTrue(real.revert());
        assertEquals(configured, CacheService.instance.keyCache.getCapacity() / (1024 * 1024));
        assertEquals("row_cache_capacity", CacheCapacityActuator.row(Map.of()).name());
        assertEquals("counter_cache_capacity", CacheCapacityActuator.counter(Map.of()).name());
    }
}
