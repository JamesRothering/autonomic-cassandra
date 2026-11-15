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
package org.apache.cassandra.autonomic;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AutonomicServiceTest
{
    private final AtomicLong now = new AtomicLong(1_000_000L);
    private final AtomicBoolean joined = new AtomicBoolean(false);
    private AutonomicService service;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        DatabaseDescriptor.getAutonomicConfig().enabled = false;
        service = new AutonomicService(now::get, joined::get);
    }

    @After
    public void teardown() throws Exception
    {
        service.shutdownBlocking();
        DatabaseDescriptor.getAutonomicConfig().enabled = false;
    }

    private static Set<String> autonomicThreads()
    {
        return Thread.getAllStackTraces().keySet().stream()
                     .map(Thread::getName)
                     .filter(n -> n.startsWith("Autonomic"))
                     .map(n -> n.replaceAll(":\\d+$", ""))
                     .collect(Collectors.toSet());
    }

    /** Production knob actuators are built when the layer starts; revert restores their configured values. */
    @Test
    public void revertRestoresEveryKnobToItsConfiguredValue()
    {
        assertTrue(service.getActuators().isEmpty());
        DatabaseDescriptor.getAutonomicConfig().enabled = true;
        service.start();
        assertEquals("NODE disabled", service.getActuators().get("compaction_throughput"));
        assertTrue(service.getActuators().containsKey("concurrent_compactors"));

        int throughput = StorageService.instance.getCompactionThroughputMbPerSec();
        int compactors = StorageService.instance.getConcurrentCompactors();
        StorageService.instance.setCompactionThroughputMbPerSec(throughput + 5);
        StorageService.instance.setConcurrentCompactors(compactors + 1);
        service.revert();
        assertEquals(throughput, StorageService.instance.getCompactionThroughputMbPerSec());
        assertEquals(compactors, StorageService.instance.getConcurrentCompactors());
    }

    /** Scale-out exists only once the layer starts, is off, and a provider class that cannot be loaded never fails the start. */
    @Test
    public void scaleOutIsBuiltWithTheLayerOffByDefaultAndSurvivesABrokenProvider()
    {
        AutonomicConfig c = DatabaseDescriptor.getAutonomicConfig();
        assertEquals("org.apache.cassandra.autonomic.elastic.NoopProvider", c.scale_out.provider);
        assertEquals(Boolean.FALSE, c.actuators.get("scale_out").get("enabled"));
        assertFalse(service.getActuators().containsKey("scale_out"));
        String provider = c.scale_out.provider;
        c.scale_out.provider = "org.example.NoSuchProvider";
        try
        {
            c.enabled = true;
            service.start();
            assertEquals(AutonomicService.State.STARTING, service.state());
            assertEquals("CLUSTER irreversible disabled", service.getActuators().get("scale_out"));
        }
        finally
        {
            c.scale_out.provider = provider;
        }
    }

    @Test
    public void defaultsAreSafe()
    {
        AutonomicConfig c = DatabaseDescriptor.getAutonomicConfig();
        assertFalse(c.enabled);
        assertTrue(c.dry_run);
    }

    @Test
    public void disabledRegistersMBeanAndCreatesNoThreads()
    {
        AutonomicService.instance.start();
        assertTrue(MBeanWrapper.instance.isRegistered(AutonomicServiceMBean.MBEAN_NAME));
        assertEquals(AutonomicService.State.DISABLED, AutonomicService.instance.state());
        assertTrue(autonomicThreads().isEmpty());
    }

    @Test
    public void enabledCreatesThreeThreadsAndShutdownRemovesThem() throws Exception
    {
        DatabaseDescriptor.getAutonomicConfig().enabled = true;
        service.start();
        assertEquals(AutonomicService.State.STARTING, service.state());
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (autonomicThreads().size() < 3 && System.nanoTime() < deadline)
            Thread.sleep(10);
        assertEquals(Set.of("AutonomicMonitor", "AutonomicControl", "AutonomicActuator"), autonomicThreads());

        service.shutdownBlocking();
        assertEquals(AutonomicService.State.DISABLED, service.state());
        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!autonomicThreads().isEmpty() && System.nanoTime() < deadline)
            Thread.sleep(10);
        assertTrue(autonomicThreads().isEmpty());
    }

    @Test
    public void runtimeEnableDisable()
    {
        service.setEnabled(true);
        assertTrue(service.isEnabled());
        assertEquals(AutonomicService.State.STARTING, service.state());
        service.setEnabled(false);
        assertEquals(AutonomicService.State.DISABLED, service.state());
    }

    @Test
    public void armedOnlyAfterJoinedAndGrace()
    {
        DatabaseDescriptor.getAutonomicConfig().enabled = true;
        service.start();
        long graceMs = DatabaseDescriptor.getAutonomicConfig().startup_grace.toMilliseconds();

        // joined but grace not elapsed
        joined.set(true);
        assertFalse(service.evaluateArmed());
        now.addAndGet(graceMs - 1);
        assertFalse(service.evaluateArmed());
        assertEquals(AutonomicService.State.STARTING, service.state());

        // grace elapsed but not joined
        joined.set(false);
        now.addAndGet(10);
        assertFalse(service.evaluateArmed());

        joined.set(true);
        assertTrue(service.evaluateArmed());
        assertTrue(service.isArmed());
        assertEquals(AutonomicService.State.RUNNING, service.state());
    }

    @Test
    public void pauseAndResume()
    {
        DatabaseDescriptor.getAutonomicConfig().enabled = true;
        service.start();
        joined.set(true);
        now.addAndGet(TimeUnit.HOURS.toMillis(1));
        service.evaluateArmed();
        assertEquals(AutonomicService.State.RUNNING, service.state());

        service.pause("test");
        assertEquals(AutonomicService.State.PAUSED, service.state());
        assertEquals("test", service.getStatus().get("pause_reason"));
        service.resume();
        assertEquals(AutonomicService.State.RUNNING, service.state());
    }

    @Test
    public void pauseBeforeArmedIsHonoured()
    {
        DatabaseDescriptor.getAutonomicConfig().enabled = true;
        service.start();
        service.pause("early");
        assertEquals(AutonomicService.State.STARTING, service.state());
        joined.set(true);
        now.addAndGet(TimeUnit.HOURS.toMillis(1));
        service.evaluateArmed();
        assertEquals(AutonomicService.State.PAUSED, service.state());
    }

    @Test
    public void pauseWhenDisabledFails()
    {
        try
        {
            service.pause("x");
            fail();
        }
        catch (IllegalStateException expected)
        {
        }
    }
}
