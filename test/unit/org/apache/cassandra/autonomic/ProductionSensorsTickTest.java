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

import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** All registered production sensors run through monitor ticks on a live node without a failure being recorded. */
public class ProductionSensorsTickTest extends CQLTester
{
    private static final String[] SENSORS = { "table", "client_request", "thread_pool", "compaction", "disk", "gc", "peer", "hot_partition" };

    private AutonomicService service;
    private DurationSpec.IntMillisecondsBound savedBudget;

    @After
    public void shutdown() throws Exception
    {
        if (service != null)
            service.shutdownBlocking();
        AutonomicConfig config = DatabaseDescriptor.getAutonomicConfig();
        config.enabled = false;
        if (savedBudget != null)
            config.safety.sensor_budget = savedBudget;
    }

    @Test
    public void monitorTickRunsEverySensorWithoutFailure() throws Throwable
    {
        AutonomicConfig config = DatabaseDescriptor.getAutonomicConfig();
        savedBudget = config.safety.sensor_budget;
        // the sensors read real metrics but the loop clock below is simulated; keep the budget check out of the way
        config.safety.sensor_budget = new DurationSpec.IntMillisecondsBound("5s");
        config.enabled = true;
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        AtomicLong clock = new AtomicLong(1_000_000);
        service = new AutonomicService(clock::get, () -> true);
        service.start();

        for (int round = 0; round < 3; round++)
        {
            for (int i = 0; i < 20; i++)
                execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, round);
            execute("SELECT * FROM %s WHERE k = 1");
            clock.addAndGet(60_000);
            service.monitorTick();
        }

        for (String sensor : SENSORS)
        {
            AutonomicService.ComponentHealth h = service.componentHealth("sensor", sensor);
            assertNotNull("sensor " + sensor + " never ran", h);
            assertEquals(sensor + ": " + h.lastError, 0, h.totalFailures);
            assertFalse(sensor + " disabled", h.disabled);
        }
        assertTrue(service.view().subjects(Signals.TABLE_WRITE_OPS_PER_SEC).contains(Subject.table(keyspace(), currentTable())));
        assertFalse(service.view().subjects(Signals.GC_HEAP_USED_FRACTION).isEmpty());
    }
}
