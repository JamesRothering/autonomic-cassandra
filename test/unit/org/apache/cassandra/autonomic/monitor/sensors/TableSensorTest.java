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

import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TableSensorTest extends CQLTester
{
    /** Signals a table with traffic must have after two collections. */
    private static final Set<SignalId> REQUIRED = Set.of(
        Signals.TABLE_READ_LATENCY_P99_MICROS, Signals.TABLE_WRITE_LATENCY_P99_MICROS, Signals.TABLE_RANGE_LATENCY_P99_MICROS,
        Signals.TABLE_COORDINATOR_READ_LATENCY_P99_MICROS, Signals.TABLE_COORDINATOR_WRITE_LATENCY_P99_MICROS,
        Signals.TABLE_SSTABLES_PER_READ_P99, Signals.TABLE_TOMBSTONES_SCANNED_P99, Signals.TABLE_LIVE_CELLS_SCANNED_P99,
        Signals.TABLE_PENDING_COMPACTIONS, Signals.TABLE_PENDING_FLUSHES, Signals.TABLE_MAX_PARTITION_SIZE_BYTES,
        Signals.TABLE_MEAN_PARTITION_SIZE_BYTES, Signals.TABLE_TOMBSTONE_WARNINGS_DELTA, Signals.TABLE_TOMBSTONE_FAILURES_DELTA);

    @Test
    public void emitsTableSignalsAndDeltasAreZeroWhenIdle() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");
        Subject subject = Subject.table(keyspace(), table);
        TableSensor sensor = new TableSensor();
        RecordingSink sink = new RecordingSink();

        sensor.collect(sink, 10_000);
        // first collection has no baseline: gauges and snapshots only
        assertNull(sink.value(Signals.TABLE_READ_OPS_PER_SEC, subject));
        assertNotNull(sink.value(Signals.TABLE_LIVE_SSTABLE_COUNT, subject));

        for (int i = 0; i < 20; i++)
            execute("INSERT INTO %s (k, c, v) VALUES (1, ?, ?)", i, i);
        flush();
        for (int i = 0; i < 10; i++)
            execute("SELECT * FROM %s WHERE k = 1");
        execute("SELECT * FROM %s");
        sink.clear();
        sensor.collect(sink, 11_000);

        for (Observation o : sink.on(subject))
            assertTrue(o.signal + " negative: " + o.value, o.value >= 0);
        assertEquals(20.0, sink.value(Signals.TABLE_WRITE_OPS_PER_SEC, subject), 0.0001);
        assertTrue(sink.value(Signals.TABLE_READ_OPS_PER_SEC, subject) >= 10.0);
        assertTrue(sink.value(Signals.TABLE_RANGE_OPS_PER_SEC, subject) >= 1.0);
        assertTrue(sink.value(Signals.TABLE_WRITE_LATENCY_MEAN_MICROS, subject) > 0);
        assertTrue(sink.value(Signals.TABLE_READ_LATENCY_MEAN_MICROS, subject) > 0);
        assertEquals(1.0, sink.value(Signals.TABLE_LIVE_SSTABLE_COUNT, subject), 0.0);
        assertTrue(sink.value(Signals.TABLE_COORDINATOR_WRITE_OPS_PER_SEC, subject) >= 0.0); // only some write paths update the coordinator write timer
        assertTrue(sink.value(Signals.TABLE_COORDINATOR_READ_OPS_PER_SEC, subject) >= 0.0);
        for (SignalId id : REQUIRED)
            assertNotNull(id + " missing", sink.value(id, subject));

        // idle second interval: rates and deltas are zero, mean latencies absent
        sink.clear();
        sensor.collect(sink, 12_000);
        assertEquals(0.0, sink.value(Signals.TABLE_READ_OPS_PER_SEC, subject), 0.0);
        assertEquals(0.0, sink.value(Signals.TABLE_WRITE_OPS_PER_SEC, subject), 0.0);
        assertEquals(0.0, sink.value(Signals.TABLE_RANGE_OPS_PER_SEC, subject), 0.0);
        assertEquals(0.0, sink.value(Signals.TABLE_COORDINATOR_READ_OPS_PER_SEC, subject), 0.0);
        assertEquals(0.0, sink.value(Signals.TABLE_TOMBSTONE_WARNINGS_DELTA, subject), 0.0);
        assertEquals(0.0, sink.value(Signals.TABLE_TOMBSTONE_FAILURES_DELTA, subject), 0.0);
        assertNull(sink.value(Signals.TABLE_READ_LATENCY_MEAN_MICROS, subject));
    }

    @Test
    public void skipsSystemKeyspaces() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY)");
        RecordingSink sink = new RecordingSink();
        new TableSensor().collect(sink, 1_000);
        for (Observation o : sink.observations)
        {
            assertEquals(Subject.Kind.TABLE, o.subject.kind);
            assertFalse(o.subject.keyspace.startsWith("system"));
        }
    }

    @Test
    public void tableDroppedBetweenCollectionsDoesNotThrow() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        Subject subject = Subject.table(keyspace(), table);
        TableSensor sensor = new TableSensor();
        RecordingSink sink = new RecordingSink();
        execute("INSERT INTO %s (k, v) VALUES (1, 1)");
        sensor.collect(sink, 1_000);
        assertNotNull(sink.value(Signals.TABLE_LIVE_SSTABLE_COUNT, subject));

        dropTable("DROP TABLE %s");
        sink.clear();
        sensor.collect(sink, 2_000);
        assertTrue(sink.on(subject).isEmpty());
    }
}
