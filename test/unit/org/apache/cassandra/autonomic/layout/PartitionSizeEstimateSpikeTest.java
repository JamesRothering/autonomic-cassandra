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
package org.apache.cassandra.autonomic.layout;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Random;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

/**
 * SPIKE CODE, NOT A TEST OF PRODUCTION BEHAVIOUR (docs/spikes/partition-size-estimate.md).
 *
 * Compares a cheap, index-only estimate of one partition's size on this node (data file position of the key and of
 * the next key in each live sstable, plus the key's rows in the live memtables) with {@link PartitionProbe} and with
 * the true size, for a few partition shapes, in the BIG format with the skiplist memtable and in the BTI format with
 * the trie memtable. It writes a few hundred MiB and takes minutes, so it is skipped unless SPIKE_PARTITION_SIZE is
 * set in the environment; the table goes to SPIKE_OUT (default build/test/spike-partition-size.txt).
 */
public class PartitionSizeEstimateSpikeTest extends CQLTester
{
    private static final int KEY = 0;
    private static final int FILLER_KEYS = 2000;
    private static final int REPEATS = 20;

    private String originalFormat;
    private PrintWriter out;

    @Before
    public void setUpSpike() throws IOException
    {
        Assume.assumeTrue("spike code, set SPIKE_PARTITION_SIZE to run", System.getenv("SPIKE_PARTITION_SIZE") != null);
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat().name();
        String path = System.getenv().getOrDefault("SPIKE_OUT", "build/test/spike-partition-size.txt");
        out = new PrintWriter(new FileWriter(path, true));
    }

    @After
    public void tearDownSpike()
    {
        if (originalFormat != null)
            DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
        if (out != null)
            out.close();
    }

    /** What the cheap estimate saw. */
    static final class Estimate
    {
        long sstableBytes;      // uncompressed serialized bytes, summed over the key's fragments
        long compressedBytes;   // the compressed chunks those fragments touch (chunk granularity)
        int fragments;
        double approxRows;      // fragment bytes scaled by each sstable's totalRows / data length
        long memtableRows;
        long memtableBytes;     // Row.dataSize() of the key's rows in the live memtables

        long bytes()
        {
            return sstableBytes + memtableBytes;
        }
    }

    /** The spike's cheap estimate: index lookups only for sstables, an in-memory walk for memtables. */
    static Estimate estimate(ColumnFamilyStore cfs, DecoratedKey key)
    {
        Estimate e = new Estimate();
        try (ColumnFamilyStore.RefViewFragment view = cfs.selectAndReference(View.select(SSTableSet.LIVE, key)))
        {
            for (SSTableReader sstable : view.sstables)
            {
                long start = sstable.getPosition(key, SSTableReader.Operator.EQ, false);
                if (start < 0)
                    continue;
                long next = sstable.getPosition(key, SSTableReader.Operator.GT, false);
                long end = next < 0 ? sstable.uncompressedLength() : next;
                e.fragments++;
                e.sstableBytes += end - start;
                e.compressedBytes += sstable.onDiskSizeForPartitionPositions(Collections.singletonList(new SSTableReader.PartitionPositionBounds(start, end)));
                StatsMetadata stats = sstable.getSSTableMetadata();
                if (stats.totalRows > 0)
                    e.approxRows += (double) (end - start) * stats.totalRows / sstable.uncompressedLength();
            }
            for (Memtable memtable : view.memtables)
            {
                try (UnfilteredRowIterator rows = memtable.rowIterator(key))
                {
                    if (rows == null)
                        continue;
                    while (rows.hasNext())
                    {
                        Unfiltered u = rows.next();
                        if (u.isRow())
                        {
                            e.memtableRows++;
                            e.memtableBytes += ((Row) u).dataSize();
                        }
                    }
                }
            }
        }
        return e;
    }

    /** The truth on this node: every live row, read locally. */
    static long[] liveRowsAndDataSize(ColumnFamilyStore cfs, DecoratedKey key)
    {
        SinglePartitionReadCommand cmd = SinglePartitionReadCommand.fullPartitionRead(cfs.metadata(), FBUtilities.nowInSeconds(), key);
        long rows = 0, bytes = 0;
        try (ReadExecutionController ec = cmd.executionController(); PartitionIterator pi = cmd.executeInternal(ec))
        {
            while (pi.hasNext())
                try (RowIterator ri = pi.next())
                {
                    while (ri.hasNext())
                    {
                        rows++;
                        bytes += ri.next().dataSize();
                    }
                }
        }
        return new long[]{ rows, bytes };
    }

    interface Loader
    {
        void load(ColumnFamilyStore cfs, TableMetadata t) throws Throwable;
    }

    private static ByteBuffer value(Random r, int size)
    {
        // a quarter random, the rest zeros: compresses about 3-4x, so compressed and uncompressed sizes differ
        byte[] b = new byte[size];
        byte[] rnd = new byte[Math.max(1, size / 4)];
        r.nextBytes(rnd);
        System.arraycopy(rnd, 0, b, 0, Math.min(rnd.length, size));
        return ByteBuffer.wrap(b);
    }

    /** Rows [from, to) of the key, with values of valueSize bytes, in mutations of about 1 MiB. */
    private static void write(TableMetadata t, int from, int to, int valueSize, long timestamp, Random r)
    {
        int perMutation = Math.max(1, (1 << 20) / (valueSize + 16));
        for (int c = from; c < to; )
        {
            PartitionUpdate.SimpleBuilder b = PartitionUpdate.simpleBuilder(t, KEY).timestamp(timestamp);
            for (int i = 0; i < perMutation && c < to; i++, c++)
                b.row(c).add("v", value(r, valueSize));
            b.buildAsMutation().applyUnsafe();
        }
    }

    private static void fillers(TableMetadata t, Random r)
    {
        for (int k = 1; k <= FILLER_KEYS; k++)
        {
            PartitionUpdate.SimpleBuilder b = PartitionUpdate.simpleBuilder(t, k);
            for (int c = 0; c < 3; c++)
                b.row(c).add("v", value(r, 100));
            b.buildAsMutation().applyUnsafe();
        }
    }

    private void scenario(String name, String format, Loader loader) throws Throwable
    {
        DatabaseDescriptor.setSelectedSSTableFormat(format);
        String memtable = format.equals("bti") ? " AND memtable = 'trie'" : "";
        createTable("CREATE TABLE %s (k int, c int, v blob, PRIMARY KEY (k, c)) WITH gc_grace_seconds = 0" + memtable);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        TableMetadata t = cfs.metadata();
        loader.load(cfs, t);
        DecoratedKey key = t.partitioner.decorateKey(Int32Type.instance.decompose(KEY));

        long t0 = System.nanoTime();
        Estimate first = estimate(cfs, key);
        long firstNanos = System.nanoTime() - t0;
        long[] times = new long[REPEATS];
        for (int i = 0; i < REPEATS; i++)
        {
            long s = System.nanoTime();
            estimate(cfs, key);
            times[i] = System.nanoTime() - s;
        }
        Arrays.sort(times);
        long sstableNanos = 0;
        {
            // the sstable part alone, without the memtable walk
            long s = System.nanoTime();
            try (ColumnFamilyStore.RefViewFragment view = cfs.selectAndReference(View.select(SSTableSet.LIVE, key)))
            {
                for (SSTableReader sstable : view.sstables)
                    if (sstable.getPosition(key, SSTableReader.Operator.EQ, false) >= 0)
                        sstable.getPosition(key, SSTableReader.Operator.GT, false);
            }
            sstableNanos = System.nanoTime() - s;
        }

        long p0 = System.nanoTime();
        PartitionProbe.Result probe = PartitionProbe.run(t, Int32Type.instance.decompose(KEY), 5_000_000);
        long probeNanos = System.nanoTime() - p0;
        long pauseMillis = (probe.rows / PartitionProbe.PAGE_ROWS) * PartitionProbe.PAUSE_MILLIS;

        long[] truth = liveRowsAndDataSize(cfs, key);

        // compacted truth: one fragment, tombstones purged (gc_grace 0), memtable flushed
        Thread.sleep(1100);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        cfs.forceMajorCompaction();
        Estimate compacted = estimate(cfs, key);

        String row = String.format(Locale.ROOT,
                                   "| %s | %s | %d+%s | %d | %.0f | %d | %.1f | %.1f | %.1f | %.1f | %.1f | %.3f / %.3f / %.3f | %d | %.0f | %.0f | %.1f |",
                                   format, name, first.fragments, first.memtableRows > 0 ? "mem" : "0",
                                   truth[0], first.approxRows + first.memtableRows, probe.rows,
                                   mib(first.bytes()), mib(first.compressedBytes), mib(compacted.sstableBytes),
                                   mib(truth[1]), mib(probe.estimatedBytes()),
                                   firstNanos / 1e6, times[REPEATS / 2] / 1e6, sstableNanos / 1e6,
                                   probeNanos / 1_000_000, (double) pauseMillis, probe.meanRowBytes, compactedRowBytes(compacted, truth[0]));
        out.println(row);
        out.flush();
        System.out.println("SPIKE " + row);
    }

    private static double compactedRowBytes(Estimate compacted, long rows)
    {
        return rows == 0 ? 0 : (double) compacted.sstableBytes / rows;
    }

    private static double mib(double bytes)
    {
        return bytes / (1024.0 * 1024.0);
    }

    private static final String[] FORMATS = { "big", "bti" };

    @Test
    public void narrowRowsFourFragments() throws Throwable
    {
        for (String f : FORMATS)
            scenario("narrow 1M rows x 8B", f, (cfs, t) -> {
                Random r = new Random(1);
                fillers(t, r);
                for (int part = 0; part < 4; part++)
                {
                    write(t, part * 250_000, (part + 1) * 250_000, 8, FBUtilities.timestampMicros(), r);
                    cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
                }
            });
    }

    @Test
    public void wideRows() throws Throwable
    {
        for (String f : FORMATS)
            scenario("wide 1500 rows x 32KiB", f, (cfs, t) -> {
                Random r = new Random(2);
                fillers(t, r);
                write(t, 0, 1500, 32 * 1024, FBUtilities.timestampMicros(), r);
                cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            });
    }

    @Test
    public void overwrites() throws Throwable
    {
        for (String f : FORMATS)
            scenario("50k rows x 200B written 3 times", f, (cfs, t) -> {
                Random r = new Random(3);
                fillers(t, r);
                for (int pass = 0; pass < 3; pass++)
                {
                    write(t, 0, 50_000, 200, FBUtilities.timestampMicros(), r);
                    cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
                }
            });
    }

    @Test
    public void deletes() throws Throwable
    {
        for (String f : FORMATS)
            scenario("50k rows x 200B, 60% deleted", f, (cfs, t) -> {
                Random r = new Random(4);
                fillers(t, r);
                write(t, 0, 50_000, 200, FBUtilities.timestampMicros(), r);
                cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
                // every other row of [0, 40000) deleted by a row tombstone, [40000, 50000) by one range tombstone
                for (int c = 0; c < 40_000; )
                {
                    PartitionUpdate.SimpleBuilder b = PartitionUpdate.simpleBuilder(t, KEY);
                    for (int i = 0; i < 5000; i++, c += 2)
                        b.row(c).delete();
                    b.buildAsMutation().applyUnsafe();
                }
                PartitionUpdate.SimpleBuilder b = PartitionUpdate.simpleBuilder(t, KEY);
                b.addRangeTombstone().start(40_000).end(50_000);
                b.buildAsMutation().applyUnsafe();
                cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            });
    }

    @Test
    public void splitAcrossSSTablesAndMemtable() throws Throwable
    {
        for (String f : FORMATS)
            scenario("200k rows x 100B, 4 sstables + memtable", f, (cfs, t) -> {
                Random r = new Random(5);
                fillers(t, r);
                for (int part = 0; part < 4; part++)
                {
                    write(t, part * 45_000, (part + 1) * 45_000, 100, FBUtilities.timestampMicros(), r);
                    cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
                }
                write(t, 180_000, 200_000, 100, FBUtilities.timestampMicros(), r);
            });
    }
}
