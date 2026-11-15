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
import java.util.Optional;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.autonomic.execute.AbstractActuatorContractTest;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.ClientResourceLimits;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RequestThrottleActuatorTest extends AbstractActuatorContractTest
{
    /** Stand-in for the limiter settings. */
    private static final class FakeLimiter implements RequestThrottleActuator.Limiter
    {
        boolean enabled;
        int limit = 1_000_000;

        FakeLimiter(boolean enabled, int limit)
        {
            this.enabled = enabled;
            this.limit = limit;
        }

        public boolean enabled()
        {
            return enabled;
        }

        public int limit()
        {
            return limit;
        }

        public void set(int limit, boolean enabled)
        {
            this.limit = limit;
            this.enabled = enabled;
        }

        String state()
        {
            return enabled + "@" + limit;
        }
    }

    private final FakeLimiter limiter = new FakeLimiter(false, 1_000_000);
    private final RequestThrottleActuator knob = new RequestThrottleActuator(limiter, Map.of());

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Override
    protected Actuator actuator()
    {
        return knob;
    }

    private static Action action(double target)
    {
        return new Action(TimeUUID.Generator.nextTimeUUID().asUUID(), RequestThrottleActuator.NAME, ActionScope.NODE, Subject.node(),
                          KnobActuator.params(target, null), null);
    }

    @Override
    protected Action newAction()
    {
        return action(knob.engageAt(4000).orElseThrow(AssertionError::new));
    }

    @Override
    protected Object snapshot()
    {
        return limiter.state();
    }

    @Override
    protected void operatorOverride()
    {
        limiter.limit += 1;
    }

    private void apply(double target) throws Exception
    {
        Action a = action(target);
        assertEquals(Optional.empty(), knob.validate(a, null));
        assertEquals(Optional.empty(), knob.blastRadius(a, null, 0.25));
        knob.apply(a, context());
    }

    @Test
    public void envelopeIsTheFloorToTheUnthrottledRate()
    {
        assertEquals(1000, knob.min(), 0);
        assertEquals(1_000_000, knob.max(), 0);
        assertFalse(knob.engaged());
        assertEquals(1_000_000, knob.current(), 0);
        assertEquals(Optional.empty(), knob.decrease(null));
        assertEquals("an unknown or idle rate engages nothing", Optional.empty(), knob.engageAt(0));
    }

    @Test
    public void multiplicativeDecreaseThenAdditiveIncreaseThenReleaseRestoresTheLimiter() throws Exception
    {
        // engaged at 80% of the observed rate, then 20% less per step, never below the floor
        assertEquals(3200, knob.engageAt(4000).get(), 0);
        apply(3200);
        assertTrue(limiter.enabled);
        assertEquals(3200, limiter.limit);
        double[] expected = { 2560, 2048, 1638, 1310, 1048, 1000 };
        for (double e : expected)
        {
            double next = knob.decrease(null).get();
            assertEquals(e, next, 0);
            assertTrue("never below the floor", next >= 1000);
            apply(next);
        }
        assertEquals("at the floor nothing is left to take", Optional.empty(), knob.decrease(null));

        // additive increase while the limit still binds, by increase_step (a tenth of the floor)
        assertEquals(1100, knob.increase(3000, 1.2, null).get(), 0);
        apply(1100);
        assertEquals(1200, knob.increase(3000, 1.2, null).get(), 0);
        apply(1200);
        // the demand fell: a limit of 1200 no longer binds against 900 requests per second, so it is released
        assertEquals(knob.ceiling(), knob.increase(900, 1.2, null).get(), 0);
        apply(knob.ceiling());
        assertEquals("previous state restored", "false@1000000", limiter.state());
        assertFalse(knob.engaged());
        assertEquals(Optional.empty(), knob.increase(900, 1.2, null));
    }

    @Test
    public void aLimiterConfiguredOnIsNeverRaisedAboveItsConfiguredLimitAndComesBackToIt() throws Exception
    {
        FakeLimiter on = new FakeLimiter(true, 5000);
        RequestThrottleActuator configuredOn = new RequestThrottleActuator(on, Map.of("min_requests_per_second", 2000));
        assertEquals(5000, configuredOn.max(), 0);
        assertFalse(configuredOn.engaged());
        // limiting at 5000 already: the first step goes below it, from the observed rate
        assertEquals(3200, configuredOn.engageAt(4000).get(), 0);
        configuredOn.apply(new Action(UUID.randomUUID(), RequestThrottleActuator.NAME, ActionScope.NODE, Subject.node(), KnobActuator.params(3200, null), null), context());
        assertEquals("true@3200", on.state());
        assertEquals(5000, configuredOn.increase(100, 1.2, null).get(), 0);
        assertTrue(configuredOn.revert());
        assertEquals("true@5000", on.state());
    }

    @Test
    public void everyStepWhileEngagedStaysWithinTheStepLimitButEngagingAndReleasingAreExempt() throws Exception
    {
        Action engage = action(3200);
        assertEquals("engaging from the unthrottled rate is not limited by a step fraction", Optional.empty(), knob.blastRadius(engage, null, 0.25));
        knob.apply(engage, context());
        assertTrue(knob.blastRadius(action(1500), null, 0.25).isPresent());
        assertEquals(Optional.empty(), knob.blastRadius(action(2560), null, 0.25));
        assertEquals("releasing returns to the ceiling in one step", Optional.empty(), knob.blastRadius(action(knob.ceiling()), null, 0.25));
        assertTrue(knob.blastRadius(action(500), null, 0.25).isPresent());
    }

    @Test
    public void aRolledBackIncreaseIsNotRepeatedForTheSameDiagnosis() throws Exception
    {
        apply(3200);
        Action up = new Action(TimeUUID.Generator.nextTimeUUID().asUUID(), RequestThrottleActuator.NAME, ActionScope.NODE, Subject.node(),
                               KnobActuator.params(3300, "d1"), null);
        org.apache.cassandra.autonomic.execute.Applied applied = knob.apply(up, context());
        knob.rollback(up, applied, context());
        assertEquals(3200, knob.current(), 0);
        assertEquals(Optional.empty(), knob.increase(9000, 1.2, "d1"));
        assertEquals(3300, knob.increase(9000, 1.2, "d2").get(), 0);
    }

    @Test
    public void realLimiterIsRevertedToTheConfiguredState() throws Exception
    {
        RequestThrottleActuator real = RequestThrottleActuator.create(Map.of());
        boolean enabled = StorageService.instance.getNativeTransportRateLimitingEnabled();
        int limit = ClientResourceLimits.getNativeTransportMaxRequestsPerSecond();
        real.apply(new Action(UUID.randomUUID(), RequestThrottleActuator.NAME, ActionScope.NODE, Subject.node(), KnobActuator.params(2000, null), null), context());
        assertTrue(StorageService.instance.getNativeTransportRateLimitingEnabled());
        assertEquals(2000, ClientResourceLimits.getNativeTransportMaxRequestsPerSecond());
        assertTrue(real.revert());
        assertEquals(enabled, StorageService.instance.getNativeTransportRateLimitingEnabled());
        assertEquals(limit, ClientResourceLimits.getNativeTransportMaxRequestsPerSecond());
    }
}
