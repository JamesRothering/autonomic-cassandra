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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RequestPoolActuatorTest extends AbstractKnobContractTest
{
    private final FakeHandle handle = new FakeHandle(10);
    private final RequestPoolActuator knob = new RequestPoolActuator(RequestPoolActuator.READ, handle, Map.of(), 16);

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
    public void envelopeIsHalfToDoubleAndNeverAboveEightTimesTheProcessors()
    {
        assertEquals(5, knob.min(), 0);
        assertEquals(20, knob.max(), 0);
        assertEquals("ReadStage", knob.stage());
        assertEquals(10, new RequestPoolActuator(RequestPoolActuator.READ, new FakeHandle(10), Map.of("max_factor", 4.0), 1).max(), 0);
        assertEquals(24, new RequestPoolActuator(RequestPoolActuator.READ, new FakeHandle(10), Map.of("max_factor", 4.0), 3).max(), 0);
        assertEquals("never below the configured size", 10, new RequestPoolActuator(RequestPoolActuator.READ, new FakeHandle(10), Map.of(), 1).max(), 0);
        assertEquals(1, new RequestPoolActuator(RequestPoolActuator.READ, new FakeHandle(1), Map.of(), 4).min(), 0);
    }

    @Test
    public void aSmallPoolNeverStepsFurtherThanTheStepLimitAllows()
    {
        handle.value = 6;
        assertEquals(7.0, knob.nextTarget(KnobActuator.Direction.UP, 0.2, null).get(), 0);
        assertEquals(5.0, knob.nextTarget(KnobActuator.Direction.DOWN, 0.2, null).get(), 0);
        handle.value = 7;
        assertEquals(8.0, knob.nextTarget(KnobActuator.Direction.UP, 0.2, null).get(), 0);
    }

    @Test
    public void stepsBackTowardsTheConfiguredSizeWithoutPassingIt()
    {
        assertEquals(Optional.empty(), knob.nextTargetTowardConfigured(0.2));
        handle.value = 20;
        assertEquals(16.0, knob.nextTargetTowardConfigured(0.2).get(), 0);
        handle.value = 11;
        assertEquals(10.0, knob.nextTargetTowardConfigured(0.5).get(), 0);
    }

    @Test
    public void realHandleMovesTheMaximumOnlyAndIsRevertedToTheConfiguredSize() throws Exception
    {
        RequestPoolActuator real = RequestPoolActuator.create(RequestPoolActuator.MUTATION, Map.of());
        int configured = (int) real.configured();
        List<Integer> sizes = StorageService.instance.getConcurrency(List.of("MutationStage")).get("MutationStage");
        StorageService.instance.setConcurrency("MutationStage", -1, configured + 2);
        assertEquals(configured + 2, StorageService.instance.getConcurrency(List.of("MutationStage")).get("MutationStage").get(1).intValue());
        assertEquals(sizes.get(0), StorageService.instance.getConcurrency(List.of("MutationStage")).get("MutationStage").get(0));
        assertTrue(real.revert());
        assertEquals(configured, StorageService.instance.getConcurrency(List.of("MutationStage")).get("MutationStage").get(1).intValue());
    }
}
