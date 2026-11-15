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
package org.apache.cassandra.autonomic.elastic;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** The provider SPI around the actuator: the default provider, loading by class name, and the bound on a call. */
public class ScaleOutProviderTest
{
    private static final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
    private static final ProviderCalls calls = new ProviderCalls((task, delayMillis) -> {
        ScheduledFuture<?> f = timer.schedule(task, delayMillis, TimeUnit.MILLISECONDS);
        return () -> f.cancel(false);
    });

    @AfterClass
    public static void stop()
    {
        timer.shutdownNow();
    }

    public static class WithParameters extends FakeScaleOutProvider
    {
        final Map<String, String> parameters;

        public WithParameters(Map<String, String> parameters)
        {
            this.parameters = parameters;
        }
    }

    public static class Throwing extends FakeScaleOutProvider
    {
        public Throwing()
        {
            throw new IllegalStateException("cannot reach the cloud");
        }
    }

    public static class NoCapabilities extends FakeScaleOutProvider
    {
        @Override
        public Capabilities capabilities()
        {
            throw new IllegalStateException("not ready");
        }
    }

    private static AutonomicConfig.ScaleOutConfig config(String provider)
    {
        AutonomicConfig.ScaleOutConfig config = new AutonomicConfig().scale_out;
        config.provider = provider;
        return config;
    }

    @Test
    public void noopIsTheDefaultCannotAddAndReportsFailed() throws Exception
    {
        ScaleOutProvider provider = ScaleOutProviders.load(new AutonomicConfig().scale_out);
        assertEquals(NoopProvider.class, provider.getClass());
        assertFalse(provider.capabilities().canAdd);
        assertEquals(0, provider.capabilities().maxNodes);
        UUID id = UUID.randomUUID();
        ProvisionTicket ticket = provider.requestNode(new ScaleRequest(id, "dc1", "rack1", "test"));
        assertEquals("idempotent on the action id", ticket, provider.requestNode(new ScaleRequest(id, "dc1", "rack1", "again")));
        ProvisionStatus status = provider.status(ticket);
        assertEquals(ProvisionStatus.State.FAILED, status.state);
        assertEquals("no provider configured", status.reason);
        provider.cancel(ticket);
        provider.cancel(ticket);
    }

    @Test
    public void providerIsLoadedByClassNameWithItsParameters()
    {
        AutonomicConfig.ScaleOutConfig config = config(WithParameters.class.getName());
        config.parameters.put("region", "north");
        ScaleOutProvider provider = ScaleOutProviders.load(config);
        assertEquals(WithParameters.class, provider.getClass());
        assertEquals(Map.of("region", "north"), ((WithParameters) provider).parameters);
        assertEquals(FakeScaleOutProvider.class, ScaleOutProviders.load(config(FakeScaleOutProvider.class.getName())).getClass());
    }

    @Test
    public void unknownOrFailingProviderFallsBackToNoopAndNeverThrows()
    {
        for (String name : new String[]{ "org.example.DoesNotExist", String.class.getName(), Throwing.class.getName(),
                                         NoCapabilities.class.getName(), "", null })
            assertEquals(String.valueOf(name), NoopProvider.class, ScaleOutProviders.load(config(name)).getClass());
        assertEquals(NoopProvider.class, ScaleOutProviders.load(null).getClass());
    }

    @Test
    public void aCallThatOverrunsIsInterruptedAndTheInterruptDoesNotLeak() throws Exception
    {
        CountDownLatch never = new CountDownLatch(1);
        try
        {
            calls.call("status", 50, () -> {
                never.await();
                return null;
            });
            fail("the call must time out");
        }
        catch (ProviderException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("status timed out after 50ms"));
            assertFalse("a timeout says nothing about what the provider did", e.isRejected());
        }
        assertFalse("the watchdog's interrupt is cleared", Thread.currentThread().isInterrupted());
        assertEquals("a quick call is not disturbed", "ok", calls.call("status", 10_000, () -> "ok"));
        assertFalse(Thread.currentThread().isInterrupted());
    }

    @Test
    public void aCallThatBlocksTheActuatorThreadIsVisibleAsStalled() throws Exception
    {
        long[] now = { 1000 };
        ProviderCalls clocked = new ProviderCalls((task, delayMillis) -> () -> {}, () -> now[0]);
        assertEquals(null, clocked.stalled(30_000));
        String seen = clocked.call("requestNode", 10_000, () -> {
            now[0] += 30_000;
            String before = clocked.stalled(30_000);
            now[0] += 1;
            return before + "/" + clocked.stalled(30_000);
        });
        assertEquals("null/requestNode", seen);
        assertEquals("nothing is running after the call", null, clocked.stalled(0));
    }

    @Test
    public void whateverACallThrowsBecomesAProviderException()
    {
        ProviderException rejected = ProviderException.rejected("quota");
        try
        {
            calls.call("requestNode", 10_000, () -> {
                throw rejected;
            });
            fail();
        }
        catch (ProviderException e)
        {
            assertSame(rejected, e);
            assertTrue(e.isRejected());
        }
        try
        {
            calls.call("requestNode", 10_000, () -> {
                throw new IllegalStateException("boom");
            });
            fail();
        }
        catch (ProviderException e)
        {
            assertFalse(e.isRejected());
            assertTrue(e.getMessage().contains("requestNode failed") && e.getMessage().contains("boom"));
        }
    }

    @Test
    public void anInterruptFromElsewhereIsKeptForTheCaller()
    {
        try
        {
            calls.call("status", 10_000, () -> {
                throw new InterruptedException("shutdown");
            });
            fail();
        }
        catch (ProviderException e)
        {
            assertTrue("not the watchdog: the thread's owner still has to see it", Thread.interrupted());
        }
    }
}
