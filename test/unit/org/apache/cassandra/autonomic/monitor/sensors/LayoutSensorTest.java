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

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.autonomic.layout.LayoutProfiler;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** {@link LayoutSensor}: deltas of the profiler's counters of a watched key; nothing for a key that is not watched. */
public class LayoutSensorTest extends CQLTester
{
    @After
    public void restore()
    {
        LayoutProfiler.clear();
    }

    @Test
    public void emitsMeansAndErrorRateOfTheIntervalForAWatchedKeyOnly() throws Throwable
    {
        String t = createTable("CREATE TABLE %s (p int, c int, v int, PRIMARY KEY (p, c))");
        TableMetadata table = Schema.instance.getTableMetadata(keyspace(), t);
        ByteBuffer key = Int32Type.instance.decompose(7);
        LayoutSensor sensor = new LayoutSensor();
        RecordingSink sink = new RecordingSink();

        sensor.collect(sink, 1000);
        assertTrue("nothing is watched, nothing is emitted", sink.observations.isEmpty());
        assertNull(LayoutProfiler.active());

        LayoutProfiler.Watch watch = LayoutProfiler.watch(table.id, key, 600_000);
        Subject subject = PartitionKeys.subject(keyspace(), t, key);
        sensor.collect(sink, 2000);
        assertTrue("no baseline for the first collection", sink.observations.isEmpty());

        LayoutProfiler.Watches watches = LayoutProfiler.active();
        record(watches, true, key, table, 3_000_000);
        record(watches, true, key, table, 1_000_000);
        record(watches, false, key, table, 500_000);
        failed(watches, true, key, table);
        sensor.collect(sink, 7000);
        assertEquals("mean of the two reads that succeeded, in us", 2000.0, sink.value(Signals.KEY_READ_LATENCY_MICROS, subject), 0);
        assertEquals(500.0, sink.value(Signals.KEY_WRITE_LATENCY_MICROS, subject), 0);
        assertEquals(3.0, sink.value(Signals.KEY_READ_OPS_DELTA, subject), 0);
        assertEquals(1.0, sink.value(Signals.KEY_WRITE_OPS_DELTA, subject), 0);
        assertEquals("1 of 4", 0.25, sink.value(Signals.KEY_ERROR_RATE, subject), 0);

        sink.clear();
        sensor.collect(sink, 12000);
        assertEquals("nothing happened in the interval", 0.0, sink.value(Signals.KEY_READ_OPS_DELTA, subject), 0);
        assertNull(sink.value(Signals.KEY_READ_LATENCY_MICROS, subject));
        assertNull(sink.value(Signals.KEY_ERROR_RATE, subject));
        assertTrue(watch.shape().timedReads == 3);
    }

    /** ADR-040: the requests to the table of a watched key that touch no watched key are timed as the rest of the table. */
    @Test
    public void emitsTheRestOfTheTableOfAWatchedKey() throws Throwable
    {
        String t = createTable("CREATE TABLE %s (p int, c int, v int, PRIMARY KEY (p, c))");
        String other = createTable("CREATE TABLE %s (p int, c int, v int, PRIMARY KEY (p, c))");
        TableMetadata table = Schema.instance.getTableMetadata(keyspace(), t);
        TableMetadata otherTable = Schema.instance.getTableMetadata(keyspace(), other);
        ByteBuffer key = Int32Type.instance.decompose(7);
        ByteBuffer rest = Int32Type.instance.decompose(8);
        LayoutSensor sensor = new LayoutSensor();
        RecordingSink sink = new RecordingSink();
        LayoutProfiler.watch(table.id, key, 600_000);
        sensor.collect(sink, 1000);

        LayoutProfiler.Watches watches = LayoutProfiler.active();
        record(watches, true, key, table, 9_000_000);
        record(watches, true, rest, table, 1_000_000);
        record(watches, true, rest, table, 3_000_000);
        record(watches, false, rest, table, 600_000);
        assertNull("another table is not timed", watches.write(java.util.List.of(new org.apache.cassandra.db.Mutation(write(otherTable, 8)))));
        sensor.collect(sink, 6000);
        Subject subject = Subject.table(keyspace(), t);
        assertEquals(2000.0, sink.value(Signals.REST_READ_LATENCY_MICROS, subject), 0);
        assertEquals(600.0, sink.value(Signals.REST_WRITE_LATENCY_MICROS, subject), 0);
        assertEquals(2.0, sink.value(Signals.REST_READ_OPS_DELTA, subject), 0);
        assertEquals(1.0, sink.value(Signals.REST_WRITE_OPS_DELTA, subject), 0);
        assertEquals("the watched key's read is its own", 9000.0, sink.value(Signals.KEY_READ_LATENCY_MICROS, PartitionKeys.subject(keyspace(), t, key)), 0);
        assertNull(sink.value(Signals.REST_READ_OPS_DELTA, Subject.table(keyspace(), other)));
        assertEquals(2, LayoutProfiler.rest(table.id).timedReads);

        LayoutProfiler.unwatch(table.id, key);
        assertNull("nothing is timed once no key of the table is watched", LayoutProfiler.rest(table.id));
    }

    private static void record(LayoutProfiler.Watches watches, boolean read, ByteBuffer key, TableMetadata table, long nanos)
    {
        LayoutProfiler.Probe probe = read ? watches.read(org.apache.cassandra.db.SinglePartitionReadCommand.Group.one(
            org.apache.cassandra.db.SinglePartitionReadCommand.create(table, 0, org.apache.cassandra.db.filter.ColumnFilter.all(table),
                                                                      org.apache.cassandra.db.filter.RowFilter.none(), org.apache.cassandra.db.filter.DataLimits.NONE,
                                                                      table.partitioner.decorateKey(key),
                                                                      new org.apache.cassandra.db.filter.ClusteringIndexSliceFilter(org.apache.cassandra.db.Slices.ALL, false))))
                                          : watches.write(java.util.List.of(new org.apache.cassandra.db.Mutation(write(table, Int32Type.instance.compose(key)))));
        probe.done(nanos);
    }

    private static org.apache.cassandra.db.partitions.PartitionUpdate write(TableMetadata table, int key)
    {
        org.apache.cassandra.db.partitions.PartitionUpdate.SimpleBuilder b = org.apache.cassandra.db.partitions.PartitionUpdate.simpleBuilder(table, key).timestamp(1);
        b.row(1).add("v", 1);
        return b.build();
    }

    private static void failed(LayoutProfiler.Watches watches, boolean read, ByteBuffer key, TableMetadata table)
    {
        LayoutProfiler.Probe probe = watches.read(org.apache.cassandra.db.SinglePartitionReadCommand.Group.one(
            org.apache.cassandra.db.SinglePartitionReadCommand.create(table, 0, org.apache.cassandra.db.filter.ColumnFilter.all(table),
                                                                      org.apache.cassandra.db.filter.RowFilter.none(), org.apache.cassandra.db.filter.DataLimits.NONE,
                                                                      table.partitioner.decorateKey(key),
                                                                      new org.apache.cassandra.db.filter.ClusteringIndexSliceFilter(org.apache.cassandra.db.Slices.ALL, false))));
        probe.failed();
    }
}
