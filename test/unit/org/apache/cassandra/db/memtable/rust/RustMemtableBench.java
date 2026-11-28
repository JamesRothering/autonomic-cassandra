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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.MemtableParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * Benchmarks for the Rust memtable spike; not a unit test. Run through {@code native/memtable-rs/bench/run-java-bench.sh}.
 * <pre>
 *   overhead                      per-call costs, single thread
 *   throughput java|rust T SECS   T writer threads for SECS seconds
 *   gc java|rust SECS             2 writers, flush every N updates, for SECS seconds (run with -Xlog:gc*)
 * </pre>
 * System properties: {@code bench.memtable} (memtable configuration for the Java side: skiplist or trie),
 * {@code bench.allocation} (heap_buffers, offheap_objects, ...), {@code bench.flush_every} (updates per memtable).
 * <p>
 * Every update is one row of three 32-byte blob cells with a random 8-byte partition key out of 2^20 and a
 * clustering out of 16, built as a {@link PartitionUpdate} on both sides, so the per-update allocation before the
 * memtable is the same; what differs is what the memtable keeps.
 */
public final class RustMemtableBench
{
    private static final int VALUE_SIZE = 32;
    private static final long FLUSH_EVERY = SpikeProperties.getLong("bench.flush_every", 1_000_000);
    /** Rust shards: 1024 by default, where the Rust-only benchmark showed shard-lock contention to vanish at 4 threads. */
    private static final int RUST_SHARDS = SpikeProperties.getInt("bench.rust_shards", 1024);

    private static TableMetadata metadata;
    private static ColumnFamilyStore cfs;

    public static void main(String[] args) throws Exception
    {
        setUp(SpikeProperties.get("bench.memtable", "skiplist"),
              Config.MemtableAllocationType.valueOf(SpikeProperties.get("bench.allocation", "heap_buffers")));
        switch (args[0])
        {
            case "overhead":
                overhead();
                break;
            case "throughput":
                run(args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]), true);
                break;
            case "gc":
                run(args[1], 2, Integer.parseInt(args[2]), false);
                break;
            default:
                throw new IllegalArgumentException(Arrays.toString(args));
        }
        System.exit(0);
    }

    private static void setUp(String memtable, Config.MemtableAllocationType allocation) throws Exception
    {
        JavaMemtableHarness.setLibraryPathIfUnset();
        ServerTestUtils.daemonInitialization();
        // As MemtableSizeTestBase does: the memtable pools are built from this config on first use.
        Field confField = DatabaseDescriptor.class.getDeclaredField("conf");
        confField.setAccessible(true);
        Config conf = (Config) confField.get(null);
        conf.memtable_allocation_type = allocation;
        conf.memtable_cleanup_threshold = 0.99f; // no pool-triggered flushes: this benchmark flushes itself
        long heapMiB = Runtime.getRuntime().maxMemory() >> 20;
        conf.memtable_heap_space = new DataStorageSpec.IntMebibytesBound(heapMiB / 2 + "MiB");
        conf.memtable_offheap_space = new DataStorageSpec.IntMebibytesBound(heapMiB / 2 + "MiB");
        // The production default partitioner (test/conf uses ByteOrderedPartitioner, which cannot be sharded).
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("bench", KeyspaceParams.simple(1),
                                    TableMetadata.builder("bench", "t")
                                                 .addPartitionKeyColumn("pk", BytesType.instance)
                                                 .addClusteringColumn("ck", Int32Type.instance)
                                                 .addRegularColumn("v1", BytesType.instance)
                                                 .addRegularColumn("v2", BytesType.instance)
                                                 .addRegularColumn("v3", BytesType.instance)
                                                 .memtable(MemtableParams.get(memtable)));
        cfs = Keyspace.open("bench").getColumnFamilyStore("t");
        metadata = cfs.metadata();
        System.out.println(String.format("setup: jdk=%s gc=%s heap=%dMiB memtable=%s allocation=%s flush_every=%d rust_shards=%d partitioner=%s",
                          Runtime.version().toString(), gcNames(), heapMiB, memtable, allocation, FLUSH_EVERY, RUST_SHARDS,
                          metadata.partitioner.getClass().getSimpleName()));
    }

    /** One update of one row with three cells. */
    static PartitionUpdate update(SplittableRandom random, long timestamp)
    {
        byte[] key = new byte[8];
        long k = random.nextInt(1 << 20);
        for (int i = 0; i < 8; i++)
            key[i] = (byte) (k >>> (56 - 8 * i));
        DecoratedKey dk = metadata.partitioner.decorateKey(ByteBuffer.wrap(key));
        Row.Builder row = BTreeRow.sortedBuilder();
        row.newRow(Clustering.make(Int32Type.instance.decompose(random.nextInt(16))));
        for (ColumnMetadata column : metadata.regularColumns())
        {
            byte[] value = new byte[VALUE_SIZE];
            for (int i = 0; i < VALUE_SIZE; i += 8)
            {
                long v = random.nextLong();
                for (int j = 0; j < 8; j++)
                    value[i + j] = (byte) (v >>> (8 * j));
            }
            row.addCell(BufferCell.live(column, timestamp, ByteBuffer.wrap(value)));
        }
        return PartitionUpdate.singleRowUpdate(metadata, dk, row.build());
    }

    // ---------------------------------------------------------------- overhead

    private static void overhead() throws Exception
    {
        int n = 200_000;
        SplittableRandom random = new SplittableRandom(1);
        PartitionUpdate[] updates = new PartitionUpdate[n];
        for (int i = 0; i < n; i++)
            updates[i] = update(random, i);
        DataOutputBuffer out = new DataOutputBuffer(4096);
        long sink = 0;

        for (int round = 0; round < 7; round++)
        {
            boolean report = round >= 2; // rounds 0-1 are JIT warm-up
            // 1. a native call that does nothing but look up its handle
            try (RustMemtable empty = RustMemtable.create(List.of()))
            {
                long t0 = System.nanoTime();
                for (int i = 0; i < 5_000_000; i++)
                    sink += empty.ownedBytes();
                print(report, "native call (handle lookup only)", System.nanoTime() - t0, 5_000_000);
            }
            // 2. encoding alone
            long t0 = System.nanoTime();
            for (PartitionUpdate u : updates)
            {
                out.clear();
                PartitionRecords.write(out, u);
                sink += out.getLength();
            }
            print(report, "encode update to record (Java)", System.nanoTime() - t0, n);
            // 3. Rust put of fresh rows, including encoding
            List<byte[]> boundaries = RustMemtable.tokenBoundaries(metadata.partitioner, RUST_SHARDS);
            try (RustMemtable rust = RustMemtable.create(boundaries))
            {
                t0 = System.nanoTime();
                for (PartitionUpdate u : updates)
                    sink += rust.put(u);
                print(report, "Rust put, new rows (encode + JNI + decode + insert)", System.nanoTime() - t0, n);
                t0 = System.nanoTime();
                for (int i = 0; i < n; i++)
                    sink += rust.read(updates[(i * 7919) % n].partitionKey()).remaining();
                print(report, "Rust read, whole partition (encode key + JNI + copy out)", System.nanoTime() - t0, n);
            }
            // 4. Java memtable put and read
            JavaMemtableHarness java = new JavaMemtableHarness(cfs);
            t0 = System.nanoTime();
            for (PartitionUpdate u : updates)
                java.put(u);
            print(report, "Java put, new rows (" + cfs.metadata().params.memtable.configurationKey() + ")", System.nanoTime() - t0, n);
            t0 = System.nanoTime();
            for (int i = 0; i < n; i++)
            {
                try (UnfilteredRowIterator p = java.memtable().rowIterator(updates[(i * 7919) % n].partitionKey(), Slices.ALL,
                                                                           ColumnFilter.all(metadata), false,
                                                                           SSTableReadsListener.NOOP_LISTENER))
                {
                    while (p.hasNext())
                        sink += ((Row) p.next()).columnCount();
                }
            }
            print(report, "Java read, whole partition (iterate rows)", System.nanoTime() - t0, n);
            java.discardAndRenew();
            java.discardAndRenew();
        }
        System.out.println("sink " + sink);
    }

    private static void print(boolean report, String what, long nanos, long ops)
    {
        if (report)
            System.out.println(String.format("overhead: %-60s %8.1f ns/op", what, (double) nanos / ops));
    }

    // ---------------------------------------------------------------- throughput and gc

    /** A memtable both sides can write to and flush, with a switch between memtables like Cassandra's flush. */
    interface Target
    {
        void put(PartitionUpdate update);

        /** Replaces the current memtable, then flushes (iterates) and discards the old one; returns its size. */
        long switchAndFlush();

        void close();
    }

    static final class JavaTarget implements Target
    {
        private final OpOrder order = new OpOrder();
        private volatile Memtable current = newMemtable();

        private static Memtable newMemtable()
        {
            return metadata.params.memtable.factory().create(new AtomicReference<>(CommitLogPosition.NONE), cfs.metadata, cfs);
        }

        public void put(PartitionUpdate update)
        {
            try (OpOrder.Group group = order.start())
            {
                current.put(update, UpdateTransaction.NO_OP, group, false);
            }
        }

        public long switchAndFlush()
        {
            Memtable old = current;
            current = newMemtable();
            OpOrder.Barrier barrier = order.newBarrier();
            old.switchOut(barrier, new AtomicReference<>(CommitLogPosition.NONE));
            barrier.issue();
            barrier.await();
            Memtable.MemoryUsage usage = Memtable.getMemoryUsage(old);
            long cells = 0;
            try (UnfilteredPartitionIterator partitions = old.partitionIterator(ColumnFilter.all(metadata), DataRange.allData(metadata.partitioner),
                                                                                SSTableReadsListener.NOOP_LISTENER))
            {
                while (partitions.hasNext())
                {
                    try (UnfilteredRowIterator partition = partitions.next())
                    {
                        while (partition.hasNext())
                        {
                            Unfiltered u = partition.next();
                            cells += ((Row) u).columnCount();
                        }
                    }
                }
            }
            old.discard();
            if (cells < 0)
                throw new AssertionError();
            return usage.ownsOnHeap + usage.ownsOffHeap;
        }

        public void close()
        {
            switchAndFlush();
        }
    }

    static final class RustTarget implements Target
    {
        private static final List<byte[]> BOUNDARIES = RustMemtable.tokenBoundaries(metadata.partitioner, RUST_SHARDS);
        private volatile RustMemtable current = RustMemtable.create(BOUNDARIES);

        public void put(PartitionUpdate update)
        {
            while (true)
            {
                RustMemtable m = current;
                try
                {
                    m.put(update);
                    return;
                }
                catch (IllegalStateException e)
                {
                    // Frozen (or released) under us by a switch: retry on the new memtable.
                    if (m == current)
                        throw e;
                }
            }
        }

        public long switchAndFlush()
        {
            RustMemtable old = current;
            current = RustMemtable.create(BOUNDARIES);
            old.freeze();
            long owned = old.ownedBytes();
            long bytes = 0;
            try (RustMemtable.FlushIterator it = old.openFlush())
            {
                ByteBuffer batch;
                while ((batch = it.nextBatch()) != null)
                    bytes += batch.remaining();
            }
            old.close();
            if (bytes < 0)
                throw new AssertionError();
            return owned;
        }

        public void close()
        {
            current.close();
        }
    }

    private static void run(String impl, int threads, int seconds, boolean warmup) throws Exception
    {
        Target target = impl.equals("rust") ? new RustTarget() : new JavaTarget();
        if (warmup)
        {
            measure(target, threads, 5, false);
            target.switchAndFlush();
        }
        measure(target, threads, seconds, true);
        target.close();
    }

    private static void measure(Target target, int threads, int seconds, boolean report) throws Exception
    {
        LongAdder ops = new LongAdder();
        AtomicBoolean stop = new AtomicBoolean();
        List<Thread> writers = new ArrayList<>();
        for (int t = 0; t < threads; t++)
        {
            int id = t;
            Thread w = new Thread(() -> {
                SplittableRandom random = new SplittableRandom(id * 1_000_003L + System.nanoTime());
                long ts = 0;
                while (!stop.get())
                {
                    target.put(update(random, ts++));
                    ops.increment();
                }
            }, "writer-" + t);
            writers.add(w);
        }
        long start = System.nanoTime();
        long[] gcBefore = gcTotals();
        writers.forEach(Thread::start);
        long deadline = start + TimeUnit.SECONDS.toNanos(seconds);
        long nextFlushAt = FLUSH_EVERY;
        int flushes = 0;
        long lastSize = 0;
        while (System.nanoTime() < deadline)
        {
            Thread.sleep(5);
            if (ops.sum() >= nextFlushAt)
            {
                lastSize = target.switchAndFlush();
                flushes++;
                nextFlushAt = ops.sum() + FLUSH_EVERY;
            }
        }
        stop.set(true);
        for (Thread w : writers)
            w.join();
        double elapsed = (System.nanoTime() - start) / 1e9;
        long[] gcAfter = gcTotals();
        if (report)
            System.out.println(String.format("result: impl=%s threads=%d seconds=%.1f ops=%d ops_per_sec=%.0f flushes=%d memtable_bytes_at_flush=%d gc_count=%d gc_ms=%d vm_hwm_kb=%s",
                              target instanceof RustTarget ? "rust" : "java", threads, elapsed, ops.sum(), ops.sum() / elapsed, flushes,
                              lastSize, gcAfter[0] - gcBefore[0], gcAfter[1] - gcBefore[1], vmHwm()));
    }

    private static long[] gcTotals()
    {
        long count = 0, time = 0;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans())
        {
            count += Math.max(0, gc.getCollectionCount());
            time += Math.max(0, gc.getCollectionTime());
        }
        return new long[]{ count, time };
    }

    private static String gcNames()
    {
        List<String> names = new ArrayList<>();
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans())
            names.add(gc.getName());
        return String.join("+", names).replace(' ', '_');
    }

    private static String vmHwm() throws Exception
    {
        for (String line : Files.readAllLines(Paths.get("/proc/self/status")))
            if (line.startsWith("VmHWM:"))
                return line.replaceAll("[^0-9]", "");
        return "?";
    }
}
