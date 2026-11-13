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
package org.apache.cassandra.autonomic.monitor.sensors;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.metrics.Sampler;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HotPartitionSensorTest extends CQLTester
{
    private static final long T0 = 100_000;
    private static final long WINDOW_MILLIS = 10_000;

    private boolean savedEnabled;
    private DurationSpec.IntSecondsBound savedWindow;
    private DurationSpec.IntSecondsBound savedInterval;
    private int savedMinOps;

    @Before
    public void configure()
    {
        AutonomicConfig.SamplingConfig c = DatabaseDescriptor.getAutonomicConfig().sampling;
        savedEnabled = c.enabled;
        savedWindow = c.window;
        savedInterval = c.interval;
        savedMinOps = c.min_ops_per_sec;
        c.enabled = true;
        c.window = new DurationSpec.IntSecondsBound("10s");
        c.interval = new DurationSpec.IntSecondsBound("1s");
        c.min_ops_per_sec = 1;
    }

    @After
    public void restore()
    {
        AutonomicConfig.SamplingConfig c = DatabaseDescriptor.getAutonomicConfig().sampling;
        c.enabled = savedEnabled;
        c.window = savedWindow;
        c.interval = savedInterval;
        c.min_ops_per_sec = savedMinOps;
    }

    private void drainSampler() throws Exception
    {
        Sampler.samplerExecutor.submit(() -> {}).get(10, TimeUnit.SECONDS);
    }

    private void writes(int key, int count) throws Throwable
    {
        for (int i = 0; i < count; i++)
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", key, i);
    }

    /** Baseline collection, then traffic so that the next collection sees a rate and opens the window. */
    private void arm(HotPartitionSensor sensor, RecordingSink sink) throws Throwable
    {
        sensor.start();
        sensor.collect(sink, T0);
        writes(1, 20);
        sensor.collect(sink, T0 + 1_000);
    }

    @Test
    public void concentratedWritesReportTheKeyWithItsBytes() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        HotPartitionSensor sensor = new HotPartitionSensor();
        RecordingSink sink = new RecordingSink();
        arm(sensor, sink);

        writes(7, 190);
        writes(8, 10);
        drainSampler();
        sensor.collect(sink, T0 + 1_000 + WINDOW_MILLIS);

        List<Observation> shares = sink.of(Signals.PARTITION_WRITE_SHARE);
        assertFalse(shares.isEmpty());
        Observation top = shares.get(0);
        for (Observation o : shares)
            if (o.value > top.value)
                top = o;
        assertEquals(Subject.Kind.PARTITION, top.subject.kind);
        assertEquals(keyspace(), top.subject.keyspace);
        assertEquals(table, top.subject.table);
        assertTrue("share " + top.value, top.value >= 0.9 && top.value <= 1.0);
        assertTrue(sink.value(Signals.PARTITION_WRITE_OPS_PER_SEC, top.subject) > 0);

        // the raw key bytes round trip, through hex and through the subject string
        ByteBuffer expected = Int32Type.instance.decompose(7);
        assertEquals(ByteBufferUtil.bytesToHex(expected), PartitionKeys.hex(top.subject));
        assertEquals(expected, PartitionKeys.bytes(top.subject));
        assertEquals("7", PartitionKeys.cql(top.subject));
        assertTrue(top.subject.asString().endsWith(':' + ByteBufferUtil.bytesToHex(expected)));
        assertTrue(sink.of(Signals.PARTITION_READ_SHARE).isEmpty());
    }

    @Test
    public void concentratedReadsReportTheKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        writes(3, 1);
        HotPartitionSensor sensor = new HotPartitionSensor();
        RecordingSink sink = new RecordingSink();
        sensor.start();
        sensor.collect(sink, T0);
        for (int i = 0; i < 20; i++)
            execute("SELECT * FROM %s WHERE k = 3");
        sensor.collect(sink, T0 + 1_000);
        for (int i = 0; i < 100; i++)
            execute("SELECT * FROM %s WHERE k = 3");
        drainSampler();
        sensor.collect(sink, T0 + 1_000 + WINDOW_MILLIS);

        Subject partition = sink.of(Signals.PARTITION_READ_SHARE).get(0).subject;
        assertTrue(sink.value(Signals.PARTITION_READ_SHARE, partition) >= 0.9);
        assertEquals(Int32Type.instance.decompose(3), PartitionKeys.bytes(partition));
    }

    @Test
    public void quietTableIsNotSampled() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        DatabaseDescriptor.getAutonomicConfig().sampling.min_ops_per_sec = 100_000;
        HotPartitionSensor sensor = new HotPartitionSensor();
        RecordingSink sink = new RecordingSink();
        arm(sensor, sink);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertFalse(cfs.metric.topWritePartitionFrequency.isEnabled());
        sensor.collect(sink, T0 + 1_000 + WINDOW_MILLIS);
        assertTrue(sink.observations.isEmpty());
    }

    @Test
    public void yieldsToAnActiveOperatorSampler() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.beginLocalSampling("WRITES", 64, 60_000); // an operator's nodetool profileload
        HotPartitionSensor sensor = new HotPartitionSensor();
        RecordingSink sink = new RecordingSink();
        arm(sensor, sink);
        writes(5, 30);
        sensor.collect(sink, T0 + 1_000 + WINDOW_MILLIS);
        sensor.collect(sink, T0 + 2_000 + WINDOW_MILLIS);
        sensor.stop();

        assertTrue(sink.observations.isEmpty());
        assertTrue("operator sampling must stay active", cfs.metric.topWritePartitionFrequency.isActive());
        drainSampler();
        // the operator's window still collects and is finished by the operator
        assertFalse(cfs.finishLocalSampling("WRITES", 5).isEmpty());
    }

    @Test
    public void doesNotFinishAnOperatorWindowOpenedAfterOurs() throws Throwable
    {
        DatabaseDescriptor.getAutonomicConfig().sampling.window = new DurationSpec.IntSecondsBound("1s");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        HotPartitionSensor sensor = new HotPartitionSensor();
        RecordingSink sink = new RecordingSink();
        arm(sensor, sink);
        assertTrue(cfs.metric.topWritePartitionFrequency.isActive());

        Thread.sleep(1_300); // our window expires; an operator starts theirs
        cfs.beginLocalSampling("WRITES", 64, 60_000);
        writes(5, 10);
        sensor.collect(sink, T0 + 3_000);

        assertTrue(sink.observations.isEmpty());
        assertTrue(cfs.metric.topWritePartitionFrequency.isActive());
        drainSampler();
        assertFalse(cfs.finishLocalSampling("WRITES", 5).isEmpty());
    }

    @Test
    public void disabledSamplingOpensNothing() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        DatabaseDescriptor.getAutonomicConfig().sampling.enabled = false;
        HotPartitionSensor sensor = new HotPartitionSensor();
        arm(sensor, new RecordingSink());
        assertFalse(getCurrentColumnFamilyStore().metric.topWritePartitionFrequency.isEnabled());
    }

    @Test
    public void tableDroppedDuringWindowDoesNotThrow() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        HotPartitionSensor sensor = new HotPartitionSensor();
        RecordingSink sink = new RecordingSink();
        arm(sensor, sink);
        writes(9, 30);
        dropTable("DROP TABLE %s");
        sensor.collect(sink, T0 + 1_000 + WINDOW_MILLIS);
        sensor.collect(sink, T0 + 2_000 + WINDOW_MILLIS);
        sensor.stop();
    }
}
