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

package org.apache.cassandra.db.memtable.rust;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The oracle for the Rust memtable spike: random operation sequences are applied to Cassandra's own memtable
 * (SkipListMemtable or TrieMemtable, created by the table's factory) and to the Rust memtable, and every read and
 * every flush must produce identical bytes in the record format of {@link PartitionRecords}.
 * <p>
 * Timestamps, deletion times and values come from tiny domains so that ties, which exercise the subtle
 * reconciliation rules, are common. Run with {@code -Dcassandra.rust_memtable.library=...} or after
 * {@code cargo build --release} in {@code native/memtable-rs}. The seed is printed; set
 * {@code -Drust_memtable.seed=N} to replay one.
 */
@RunWith(Parameterized.class)
public class RustMemtableDifferentialTest extends CQLTester
{
    private static final int SEQUENCES = SpikeProperties.getInt("rust_memtable.sequences", 40);
    private static final int OPS_PER_SEQUENCE = SpikeProperties.getInt("rust_memtable.ops", 400);
    private static final long BASE_SEED = SpikeProperties.getLong("rust_memtable.seed", System.nanoTime());
    /** The Rust implementation under test: arena (default) or boxed (spike 1's). */
    private static final RustMemtable.Kind RUST_KIND = RustMemtable.Kind.valueOf(SpikeProperties.get("rust_memtable.kind", "arena").toUpperCase());
    private static final String[] CK2 = { "", "a", "ab", "b" };
    private static final byte[][] VALUES = { {}, { 0 }, { 1 }, { 1, 0 }, { (byte) 0x80 }, { (byte) 0xff, 1 } };
    private static final long[] LDTS = { 10, 20, 30 };

    @Parameterized.Parameter
    public String memtableConfig;

    @Parameterized.Parameters(name = "{0}")
    public static List<Object> parameters()
    {
        return List.of("skiplist", "trie");
    }

    /** Replaces CQLTester's: the production partitioner, so keys carry Murmur3 tokens and the Rust side has 4 shards. */
    @BeforeClass
    public static void setUpClass()
    {
        JavaMemtableHarness.setLibraryPathIfUnset();
        ServerTestUtils.daemonInitialization();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        CQLTester.prepareServer();
    }

    private JavaMemtableHarness createTable()
    {
        createTable("CREATE TABLE %s (pk blob, ck1 int, ck2 text, v1 blob, v2 blob, v3 blob, PRIMARY KEY (pk, ck1, ck2))" +
                    " WITH CLUSTERING ORDER BY (ck1 DESC, ck2 ASC) AND memtable = '" + memtableConfig + "'");
        return new JavaMemtableHarness(getCurrentColumnFamilyStore());
    }

    private static RustMemtable createRust(TableMetadata metadata)
    {
        List<byte[]> boundaries = RustMemtable.tokenBoundaries(metadata.partitioner, 4);
        assertEquals("expected a Murmur3 table with 4 shards", 3, boundaries.size());
        return RustMemtable.create(RUST_KIND, boundaries);
    }

    @Test
    public void randomSequencesMatchCassandra() throws Throwable
    {
        JavaMemtableHarness java = createTable();
        TableMetadata metadata = java.metadata();
        long baseline = NativeMemtable.liveObjects();
        Stats stats = new Stats();
        for (int s = 0; s < SEQUENCES; s++)
        {
            long seed = BASE_SEED + s;
            // Profiles: 0 = wide domains; 1 = tiny domains (two keys, four rows, three timestamps) with many
            // deletions; 2 = tiny domains, two timestamps, mostly row liveness and shadowable deletions. Profiles 1
            // and 2 make rare ties common; each was added after a mutant of one tie rule survived without it.
            Generator gen = new Generator(new Random(seed), metadata, s % 3);
            RustMemtable rust = createRust(metadata);
            List<String> log = new ArrayList<>();
            try
            {
                for (int op = 0; op < OPS_PER_SEQUENCE; op++)
                {
                    int dice = gen.random.nextInt(100);
                    if (dice < 60)
                    {
                        PartitionUpdate update = gen.update();
                        log.add("put " + update);
                        java.put(update);
                        rust.put(update);
                        stats.puts++;
                    }
                    else if (dice < 98)
                    {
                        DecoratedKey key = gen.key();
                        Slice slice = dice < 85 ? Slice.ALL : gen.slice();
                        log.add("read " + key + ' ' + slice.toString(metadata.comparator));
                        ByteBuffer expected = java.read(key, Slices.with(metadata.comparator, slice));
                        ByteBuffer actual = slice == Slice.ALL
                                            ? rust.read(key)
                                            : rust.read(key, metadata, slice.start(), slice.end());
                        check(seed, log, expected, actual);
                        stats.reads++;
                        if (expected != null)
                            stats.hits++;
                    }
                    else
                    {
                        log.add("flush");
                        rust = flushAndCompare(seed, log, java, rust, metadata);
                        stats.flushes++;
                    }
                }
                rust = flushAndCompare(seed, log, java, rust, metadata);
                stats.flushes++;
            }
            finally
            {
                rust.close();
            }
        }
        assertEquals("native objects leaked", baseline, NativeMemtable.liveObjects());
        logger.info(String.format("RustMemtableDifferentialTest[%s vs Rust %s] seed %d: %d sequences, %d puts, %d reads (%d hits), %d flushes, all identical",
                                  memtableConfig, RUST_KIND, BASE_SEED, SEQUENCES, stats.puts, stats.reads, stats.hits, stats.flushes));
    }

    private RustMemtable flushAndCompare(long seed, List<String> log, JavaMemtableHarness java, RustMemtable rust, TableMetadata metadata) throws Exception
    {
        rust.freeze();
        check(seed, log, java.readAll(), JavaMemtableHarness.readAll(rust));
        rust.close();
        java.discardAndRenew();
        return createRust(metadata);
    }

    private static void check(long seed, List<String> log, ByteBuffer expected, ByteBuffer actual)
    {
        if (expected == null && actual == null)
            return;
        String e = expected == null ? "absent" : RecordPrinter.describe(expected);
        String a = actual == null ? "absent" : RecordPrinter.describe(actual);
        if (expected == null || !expected.equals(actual))
        {
            int from = Math.max(0, log.size() - 30);
            fail("Mismatch (seed " + seed + ") after:\n  " + String.join("\n  ", log.subList(from, log.size())) +
                 "\nCassandra: " + e + "\nRust:      " + a);
        }
    }

    /** Cell-only writes from four threads into one Rust memtable equal the same writes applied serially to Cassandra's. */
    @Test
    public void concurrentWritesMatchSequentialCassandra() throws Throwable
    {
        JavaMemtableHarness java = createTable();
        TableMetadata metadata = java.metadata();
        int threads = 4;
        int perThread = 5000;
        List<List<PartitionUpdate>> work = new ArrayList<>();
        for (int t = 0; t < threads; t++)
        {
            Generator gen = new Generator(new Random(BASE_SEED * 31 + t), metadata, 0);
            List<PartitionUpdate> updates = new ArrayList<>();
            for (int i = 0; i < perThread; i++)
                updates.add(gen.cellOnlyUpdate());
            work.add(updates);
        }
        try (RustMemtable rust = createRust(metadata))
        {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            try
            {
                List<Future<?>> futures = new ArrayList<>();
                for (List<PartitionUpdate> updates : work)
                    futures.add(pool.submit(() -> updates.forEach(rust::put)));
                for (Future<?> f : futures)
                    f.get();
            }
            finally
            {
                pool.shutdown();
            }
            for (List<PartitionUpdate> updates : work)
                updates.forEach(java::put);
            rust.freeze();
            check(BASE_SEED, List.of("concurrent cell-only writes"), java.readAll(), JavaMemtableHarness.readAll(rust));
        }
    }

    @Test
    public void boundaryErrorsBecomeJavaExceptions() throws Throwable
    {
        JavaMemtableHarness java = createTable();
        TableMetadata metadata = java.metadata();
        long baseline = NativeMemtable.liveObjects();
        Generator gen = new Generator(new Random(BASE_SEED), metadata, 0);

        try
        {
            NativeMemtable.panicForTest();
            fail("expected an exception");
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("deliberate panic"));
        }

        RustMemtable rust = createRust(metadata);
        PartitionUpdate update = gen.update();
        rust.put(update);
        expect(IllegalArgumentException.class, () -> NativeMemtable.put(1, new byte[]{ 1, 2, 3 }, 3), "unknown handle with bad bytes");
        expect(IllegalStateException.class, () -> NativeMemtable.ownedBytes(-5), "unknown handle");
        expect(IllegalArgumentException.class, () -> NativeMemtable.put(1, new byte[4], 5), "length beyond the array");
        expect(IllegalStateException.class, rust::openFlush, "flush before freeze");

        rust.freeze();
        expect(IllegalStateException.class, () -> rust.put(update), "write after freeze");
        assertNotNull(rust.read(update.partitionKey()));

        // The iterator keeps the data alive after the owner releases the memtable.
        RustMemtable.FlushIterator it = rust.openFlush();
        rust.close();
        expect(IllegalStateException.class, rust::ownedBytes, "use after release");
        ByteBuffer batch = it.nextBatch();
        assertNotNull(batch);
        assertTrue(RecordPrinter.describe(batch).startsWith("partition"));
        assertNull(it.nextBatch());
        it.close();
        expect(IllegalStateException.class, it::nextBatch, "use after close");
        assertEquals("native objects leaked", baseline, NativeMemtable.liveObjects());
    }

    private static void expect(Class<? extends Throwable> type, Runnable call, String what)
    {
        try
        {
            call.run();
        }
        catch (Throwable t)
        {
            assertTrue(what + ": got " + t, type.isInstance(t));
            return;
        }
        fail(what + ": expected " + type.getSimpleName());
    }

    private static final class Stats
    {
        long puts, reads, hits, flushes;
    }

    /** Random updates over tiny domains, so that keys, clusterings, timestamps and values collide often. */
    private static final class Generator
    {
        final Random random;
        final TableMetadata metadata;
        final ClusteringComparator comparator;
        final List<ColumnMetadata> columns = new ArrayList<>();
        final boolean dense;
        final boolean livenessHeavy;

        Generator(Random random, TableMetadata metadata, int profile)
        {
            this.random = random;
            this.dense = profile > 0;
            this.livenessHeavy = profile == 2;
            this.metadata = metadata;
            this.comparator = metadata.comparator;
            metadata.regularColumns().forEach(columns::add);
        }

        DecoratedKey key()
        {
            int k = random.nextInt(dense ? 2 : 10);
            byte[] bytes = k < 8 ? new byte[]{ (byte) k } : new byte[]{ (byte) k, (byte) random.nextInt(2) };
            return metadata.partitioner.decorateKey(ByteBuffer.wrap(bytes));
        }

        Clustering<?> clustering()
        {
            return Clustering.make(Int32Type.instance.decompose(random.nextInt(dense ? 2 : 5) - (dense ? 0 : 2)),
                                   UTF8Type.instance.decompose(CK2[random.nextInt(dense ? 2 : CK2.length)]));
        }

        long timestamp()
        {
            return random.nextInt(livenessHeavy ? 2 : dense ? 3 : 4);
        }

        long ldt()
        {
            return LDTS[random.nextInt(dense ? 2 : LDTS.length)];
        }

        DeletionTime deletion()
        {
            return DeletionTime.build(timestamp(), ldt());
        }

        Cell<?> cell(ColumnMetadata column)
        {
            int kind = random.nextInt(10);
            long ts = timestamp();
            if (kind < 6)
                return BufferCell.live(column, ts, ByteBuffer.wrap(VALUES[random.nextInt(VALUES.length)]));
            if (kind < 8)
                return new BufferCell(column, ts, 1 + random.nextInt(2), ldt(), ByteBuffer.wrap(VALUES[random.nextInt(VALUES.length)]), null);
            return BufferCell.tombstone(column, ts, ldt());
        }

        LivenessInfo liveness()
        {
            int kind = random.nextInt(10);
            if (kind < (livenessHeavy ? 4 : 7))
                return LivenessInfo.create(timestamp());
            if (kind < 9)
                return LivenessInfo.withExpirationTime(timestamp(), 1 + random.nextInt(2), ldt());
            return LivenessInfo.withExpirationTime(timestamp(), LivenessInfo.EXPIRED_LIVENESS_TTL, ldt());
        }

        @SuppressWarnings("deprecation")
        Row row(Clustering<?> clustering, boolean cellsOnly)
        {
            while (true)
            {
                Row.Builder builder = BTreeRow.sortedBuilder();
                builder.newRow(clustering);
                if (!cellsOnly && random.nextInt(10) < (livenessHeavy ? 8 : 3))
                    builder.addPrimaryKeyLivenessInfo(liveness());
                if (!cellsOnly && random.nextInt(100) < (livenessHeavy ? 20 : dense ? 30 : 15))
                {
                    DeletionTime dt = deletion();
                    boolean shadowable = random.nextInt(10) < (livenessHeavy ? 6 : 2);
                    builder.addRowDeletion(shadowable ? Row.Deletion.shadowable(dt) : Row.Deletion.regular(dt));
                }
                for (ColumnMetadata column : columns)
                    if (random.nextBoolean())
                        builder.addCell(cell(column));
                Row row = builder.build();
                if (!row.isEmpty())
                    return row;
            }
        }

        PartitionUpdate update()
        {
            PartitionUpdate.Builder builder = new PartitionUpdate.Builder(metadata, key(), metadata.regularAndStaticColumns(), 4);
            boolean partitionDeletion = random.nextInt(10) == 0;
            if (partitionDeletion)
                builder.addPartitionDeletion(deletion());
            int rows = random.nextInt(4);
            if (rows == 0 && !partitionDeletion)
                rows = 1;
            TreeMap<Clustering<?>, Boolean> clusterings = new TreeMap<>(comparator);
            for (int i = 0; i < rows; i++)
                clusterings.put(clustering(), Boolean.TRUE);
            for (Clustering<?> c : clusterings.keySet())
                builder.add(row(c, false));
            return builder.build();
        }

        PartitionUpdate cellOnlyUpdate()
        {
            PartitionUpdate.Builder builder = new PartitionUpdate.Builder(metadata, key(), metadata.regularAndStaticColumns(), 1);
            builder.add(row(clustering(), true));
            return builder.build();
        }

        Slice slice()
        {
            while (true)
            {
                ClusteringBound<?> start = random.nextInt(4) == 0 ? ClusteringBound.BOTTOM : bound(true);
                ClusteringBound<?> end = random.nextInt(4) == 0 ? ClusteringBound.TOP : bound(false);
                if (!Slice.isEmpty(comparator, start, end))
                    return Slice.make(start, end);
            }
        }

        private ClusteringBound<?> bound(boolean isStart)
        {
            boolean inclusive = random.nextBoolean();
            Object[] values = random.nextBoolean()
                              ? new Object[]{ random.nextInt(5) - 2 }
                              : new Object[]{ random.nextInt(5) - 2, CK2[random.nextInt(CK2.length)] };
            return ClusteringBound.create(comparator, isStart, inclusive, values);
        }
    }
}
