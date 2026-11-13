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

import java.net.InetAddress;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Node-scoped sensors on a live node: client requests, thread pools, compaction, disk, GC, peers. */
public class NodeSensorsTest extends CQLTester
{
    @BeforeClass
    public static void startNetwork() throws Throwable
    {
        requireNetwork(); // client request metrics are only updated by requests that go through the coordinator
    }

    private static void assertAllNonNegative(RecordingSink sink)
    {
        for (Observation o : sink.observations)
            assertTrue(o + " negative", o.value >= 0);
    }

    @Test
    public void clientRequestSensor() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ClientRequestSensor sensor = new ClientRequestSensor();
        RecordingSink sink = new RecordingSink();
        Subject node = Subject.node();
        sensor.collect(sink, 1_000);
        assertNotNull(sink.value(Signals.CLIENT_WRITE_LATENCY_P99_MICROS, node));
        assertNull(sink.value(Signals.CLIENT_WRITE_OPS_PER_SEC, node));

        for (int i = 0; i < 20; i++)
            executeNet("INSERT INTO %s (k, v) VALUES (?, ?)", i, i);
        for (int i = 0; i < 10; i++)
            executeNet("SELECT * FROM %s WHERE k = ?", i);
        sink.clear();
        sensor.collect(sink, 2_000);
        assertAllNonNegative(sink);
        assertTrue(sink.observations.toString(), sink.value(Signals.CLIENT_WRITE_OPS_PER_SEC, node) >= 20.0);
        assertTrue(sink.value(Signals.CLIENT_READ_OPS_PER_SEC, node) >= 10.0);
        assertTrue(sink.value(Signals.CLIENT_WRITE_LATENCY_MEAN_MICROS, node) > 0);
        assertTrue(sink.value(Signals.CLIENT_READ_LATENCY_MEAN_MICROS, node) > 0);
        assertEquals(0.0, sink.value(Signals.CLIENT_WRITE_TIMEOUTS_PER_SEC, node), 0.0);
        for (SignalId id : sensor.signals())
            if (!id.name.contains("mean"))
                assertNotNull(id + " missing", sink.value(id, node));

        sink.clear();
        sensor.collect(sink, 3_000);
        assertEquals(0.0, sink.value(Signals.CLIENT_WRITE_OPS_PER_SEC, node), 0.0);
        assertEquals(0.0, sink.value(Signals.CLIENT_READ_OPS_PER_SEC, node), 0.0);
        assertEquals(0.0, sink.value(Signals.CLIENT_READ_FAILURES_PER_SEC, node), 0.0);
        assertNull(sink.value(Signals.CLIENT_WRITE_LATENCY_MEAN_MICROS, node));
    }

    @Test
    public void threadPoolSensor() throws Throwable
    {
        ThreadPoolSensor sensor = new ThreadPoolSensor();
        RecordingSink sink = new RecordingSink();
        sensor.collect(sink, 1_000);
        sensor.collect(sink, 2_000);
        assertAllNonNegative(sink);
        Subject read = Subject.threadPool("GossipStage");
        for (SignalId id : sensor.signals())
            assertNotNull(id + " missing for GossipStage " + sink.observations, sink.value(id, read));
        assertTrue(sink.value(Signals.POOL_MAX_POOL_SIZE, read) > 0);
        assertEquals(0.0, sink.value(Signals.POOL_BLOCKED_TASKS_DELTA, read), 0.0);
    }

    @Test
    public void compactionSensor() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        CompactionSensor sensor = new CompactionSensor();
        RecordingSink sink = new RecordingSink();
        Subject node = Subject.node();
        sensor.collect(sink, 1_000);
        assertNotNull(sink.value(Signals.COMPACTION_PENDING_TASKS, node));

        for (int sstable = 0; sstable < 2; sstable++)
        {
            for (int i = 0; i < 50; i++)
                execute("INSERT INTO %s (k, v) VALUES (?, ?)", i + sstable * 1000, i);
            flush();
        }
        compact();
        sink.clear();
        sensor.collect(sink, 2_000);
        assertAllNonNegative(sink);
        assertTrue(sink.value(Signals.COMPACTION_BYTES_PER_SEC, node) > 0);

        sink.clear();
        sensor.collect(sink, 3_000);
        assertEquals(0.0, sink.value(Signals.COMPACTION_BYTES_PER_SEC, node), 0.0);
    }

    @Test
    public void diskSensor() throws Throwable
    {
        RecordingSink sink = new RecordingSink();
        new DiskSensor().collect(sink, 1_000);
        Subject node = Subject.node();
        double used = sink.value(Signals.DISK_USED_FRACTION, node);
        assertTrue(used >= 0 && used <= 1);
        double free = sink.value(Signals.DISK_FREE_BYTES, node);
        double total = sink.value(Signals.DISK_TOTAL_BYTES, node);
        assertTrue(free > 0 && free <= total);
    }

    @Test
    public void gcSensor() throws Throwable
    {
        GcSensor sensor = new GcSensor();
        RecordingSink sink = new RecordingSink();
        Subject node = Subject.node();
        sensor.collect(sink, 1_000);
        double heap = sink.value(Signals.GC_HEAP_USED_FRACTION, node);
        assertTrue(heap > 0 && heap <= 1);
        assertNull(sink.value(Signals.GC_TIME_FRACTION, node));

        System.gc();
        sink.clear();
        sensor.collect(sink, 6_000);
        assertAllNonNegative(sink);
        assertTrue(sink.value(Signals.GC_COLLECTIONS_DELTA, node) >= 1);
        assertTrue(sink.value(Signals.GC_TIME_FRACTION, node) <= 1);
        assertNotNull(sink.value(Signals.GC_TIME_MILLIS_DELTA, node));
    }

    @Test
    public void peerSensorReadsGossipState() throws Throwable
    {
        PeerSensor sensor = new PeerSensor();
        RecordingSink sink = new RecordingSink();
        sensor.collect(sink, 1_000);
        // a single node has no remote peers
        assertTrue(sink.observations.isEmpty());

        InetAddressAndPort fake = InetAddressAndPort.getByAddress(InetAddress.getByAddress(new byte[]{ 127, 9, 9, 9 }));
        EndpointState state = new EndpointState(HeartBeatState.empty());
        state.addApplicationState(ApplicationState.LOAD, VersionedValue.unsafeMakeVersionedValue("1234.0", 1));
        state.addApplicationState(ApplicationState.SEVERITY, VersionedValue.unsafeMakeVersionedValue("0.5", 2));
        state.markAlive();
        Gossiper.instance.endpointStateMap.put(fake, state);
        try
        {
            sink.clear();
            sensor.collect(sink, 2_000);
            Subject peer = Subject.peer(fake.getHostAddressAndPort());
            assertEquals(1234.0, sink.value(Signals.PEER_LOAD_BYTES, peer), 0.0);
            assertEquals(0.5, sink.value(Signals.PEER_SEVERITY, peer), 0.0);
            assertEquals(1.0, sink.value(Signals.PEER_ALIVE, peer), 0.0);

            state.markDead();
            sink.clear();
            sensor.collect(sink, 3_000);
            assertEquals(0.0, sink.value(Signals.PEER_ALIVE, peer), 0.0);
        }
        finally
        {
            Gossiper.instance.endpointStateMap.remove(fake);
        }
    }

    @Test
    public void everySensorSignalIsDocumented()
    {
        Set<SignalId> all = Signals.all();
        Set<SignalId> claimed = new java.util.HashSet<>();
        for (org.apache.cassandra.autonomic.monitor.Sensor s : new org.apache.cassandra.autonomic.monitor.Sensor[]{
            new TableSensor(), new ClientRequestSensor(), new ThreadPoolSensor(), new CompactionSensor(),
            new DiskSensor(), new GcSensor(), new PeerSensor(), new HotPartitionSensor() })
        {
            for (SignalId id : s.signals())
            {
                claimed.add(id);
                assertTrue(id + " not in Signals", all.contains(id));
                assertNotNull(Signals.unit(id));
                assertFalse(Signals.meaning(id).isEmpty());
            }
        }
        Set<SignalId> unclaimed = new java.util.HashSet<>(all);
        unclaimed.removeAll(claimed);
        assertEquals("signals no sensor declares", Set.of(), unclaimed);
    }
}
