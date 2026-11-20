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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Clock;

/**
 * The shape profile of design 7.2 step 3 and the key signals of 7.3: while a key is <em>watched</em>, the layout hooks
 * of this node count, for that key, named-row reads, slice reads, row writes, range and partition deletes and
 * {@code cas} calls, record the latency of the reads and writes that this node coordinated, and count failures. The
 * range scans of the key's table are counted too. Node-local and in memory; nothing goes through cluster metadata.
 *
 * <p><b>Cost when nothing is watched (FC8).</b> {@link #active} is one read of a volatile field that is null; the hooks
 * do nothing else, so a request allocates nothing, takes no lock and counts nothing. {@link #hookCalls} counts the
 * times a hook went on to look at a watch; it stays at zero while nothing is watched (asserted by the tests).
 *
 * <p>While a key is watched, a request that touches it is executed by the hook itself (the stock tail, under the
 * hook's re-entrancy flag) so that its latency can be taken. So is a request to the table of a watched key that touches
 * no watched key: its latency is counted for the <em>rest of the table</em> ({@link #rest}), the control of the
 * collateral guard of ADR-040. Requests to other tables take the stock path as before. A watch has an expiry; {@link #sweep} removes the expired ones and, with the last, deactivates the
 * hooks. The sensor of this node keeps the keys of running migrations watched.
 */
public final class LayoutProfiler
{
    /** Times a hook looked at a watch (a request that found the profiler active). Zero while nothing is watched. */
    private static final AtomicLong HOOK_CALLS = new AtomicLong();

    /** Immutable once published; null while nothing is watched. */
    private static volatile Watches current;

    private LayoutProfiler()
    {
    }

    /** Paxos operations on the table on this node since it started; 0 if the table is not open here. */
    public static long paxosOperations(TableMetadata table)
    {
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(table.id);
        if (cfs == null)
            return 0;
        return cfs.metric.casPrepare.latency.getCount() + cfs.metric.casPropose.latency.getCount() + cfs.metric.casCommit.latency.getCount();
    }

    /** {@link Eligibility#runtime(long, long)} with the Paxos counters of the table on this node and the key's profile, if it has one. */
    public static String runtime(TableMetadata table, Shape profile)
    {
        return Eligibility.runtime(paxosOperations(table), profile == null ? 0 : profile.cas);
    }

    /** The watches, or null if nothing is watched. The only cost of the hooks when no key is watched. */
    public static Watches active()
    {
        return current;
    }

    public static long hookCalls()
    {
        return HOOK_CALLS.get();
    }

    /**
     * Starts counting for the key, or extends the watch to {@code windowMillis} from now. The counters of a watch
     * that is extended are kept.
     */
    public static synchronized Watch watch(TableId table, ByteBuffer key, long windowMillis)
    {
        long now = Clock.Global.currentTimeMillis();
        Map<TableId, Map<ByteBuffer, Watch>> next = copy(current);
        Map<ByteBuffer, Watch> keys = next.computeIfAbsent(table, t -> new HashMap<>());
        ByteBuffer k = key.duplicate();
        Watch watch = keys.get(k);
        if (watch == null)
        {
            watch = new Watch(table, k, now);
            keys.put(k, watch);
        }
        watch.expiresMillis = Math.max(watch.expiresMillis, now + windowMillis);
        current = new Watches(next);
        return watch;
    }

    public static synchronized void unwatch(TableId table, ByteBuffer key)
    {
        Map<TableId, Map<ByteBuffer, Watch>> next = copy(current);
        Map<ByteBuffer, Watch> keys = next.get(table);
        if (keys != null && keys.remove(key) != null && keys.isEmpty())
            next.remove(table);
        current = next.isEmpty() ? null : new Watches(next);
    }

    /** The watch of the key, or null. */
    public static Watch get(TableId table, ByteBuffer key)
    {
        Watches w = current;
        return w == null ? null : w.find(table, key);
    }

    /** Removes the watches that expired at {@code nowMillis}. */
    public static synchronized void sweep(long nowMillis)
    {
        Watches w = current;
        if (w == null)
            return;
        Map<TableId, Map<ByteBuffer, Watch>> next = copy(w);
        boolean changed = false;
        for (Map<ByteBuffer, Watch> keys : next.values())
            changed |= keys.values().removeIf(watch -> watch.expiresMillis <= nowMillis);
        if (!changed)
            return;
        next.values().removeIf(Map::isEmpty);
        current = next.isEmpty() ? null : new Watches(next);
    }

    /** All watches now. */
    public static List<Watch> all()
    {
        Watches w = current;
        return w == null ? List.of() : w.all();
    }

    public static synchronized void clear()
    {
        current = null;
    }

    private static Map<TableId, Map<ByteBuffer, Watch>> copy(Watches w)
    {
        Map<TableId, Map<ByteBuffer, Watch>> out = new HashMap<>();
        if (w != null)
            for (Map.Entry<TableId, Map<ByteBuffer, Watch>> e : w.byTable.entrySet())
                out.put(e.getKey(), new HashMap<>(e.getValue()));
        return out;
    }

    /** What one watched key has seen; counters only grow. */
    public static final class Watch
    {
        public final TableId table;
        public final ByteBuffer key;
        public final long startedMillis;
        volatile long expiresMillis;

        private final LongAdder namedReads = new LongAdder();
        private final LongAdder sliceReads = new LongAdder();
        private final LongAdder rowWrites = new LongAdder();
        private final LongAdder deletes = new LongAdder();
        private final LongAdder cas = new LongAdder();
        private final Timing timing = new Timing();

        Watch(TableId table, ByteBuffer key, long startedMillis)
        {
            this.table = table;
            this.key = key;
            this.startedMillis = startedMillis;
            this.expiresMillis = startedMillis;
        }

        public long expiresMillis()
        {
            return expiresMillis;
        }

        /** The counters now. {@code scans} is the table's, which the caller reads from {@link Watches#scans}. */
        public Shape shape()
        {
            Watches w = current;
            return new Shape(namedReads.sum(), sliceReads.sum(), rowWrites.sum(), deletes.sum(), cas.sum(), w == null ? 0 : w.scans(table),
                             timing.timedReads.sum(), timing.readNanos.sum() / 1000, timing.timedWrites.sum(), timing.writeNanos.sum() / 1000,
                             timing.readErrors.sum(), timing.writeErrors.sum());
        }
    }

    /** The timed reads and writes of a watched key, or of the rest of its table; counters only grow. */
    static final class Timing
    {
        private final LongAdder readNanos = new LongAdder();
        private final LongAdder timedReads = new LongAdder();
        private final LongAdder writeNanos = new LongAdder();
        private final LongAdder timedWrites = new LongAdder();
        private final LongAdder readErrors = new LongAdder();
        private final LongAdder writeErrors = new LongAdder();

        /** The counters now, as a shape with only the timed fields set. */
        Shape shape()
        {
            return new Shape(0, 0, 0, 0, 0, 0, timedReads.sum(), readNanos.sum() / 1000, timedWrites.sum(), writeNanos.sum() / 1000,
                             readErrors.sum(), writeErrors.sum());
        }
    }

    /**
     * The timed requests to the table that touched no watched key since its first key was watched (ADR-040): only the
     * timed fields of the shape are set. Null if no key of the table is watched.
     */
    public static Shape rest(TableId table)
    {
        Watches w = current;
        Timing timing = w == null ? null : w.rest(table);
        return timing == null ? null : timing.shape();
    }

    /** The tables with a watched key now. */
    public static java.util.Set<TableId> watchedTables()
    {
        Watches w = current;
        return w == null ? java.util.Set.of() : java.util.Set.copyOf(w.byTable.keySet());
    }

    /** A copy of the counters of one watch. */
    public static final class Shape
    {
        public final long namedReads;
        public final long sliceReads;
        public final long rowWrites;
        /** Range tombstones and partition deletions. */
        public final long deletes;
        public final long cas;
        /** Range scans of the key's table since the profiler started counting them. */
        public final long scans;
        public final long timedReads;
        public final long readMicros;
        public final long timedWrites;
        public final long writeMicros;
        public final long readErrors;
        public final long writeErrors;

        public Shape(long namedReads, long sliceReads, long rowWrites, long deletes, long cas, long scans,
                     long timedReads, long readMicros, long timedWrites, long writeMicros, long readErrors, long writeErrors)
        {
            this.namedReads = namedReads;
            this.sliceReads = sliceReads;
            this.rowWrites = rowWrites;
            this.deletes = deletes;
            this.cas = cas;
            this.scans = scans;
            this.timedReads = timedReads;
            this.readMicros = readMicros;
            this.timedWrites = timedWrites;
            this.writeMicros = writeMicros;
            this.readErrors = readErrors;
            this.writeErrors = writeErrors;
        }

        /** Operations that count towards the fan-out fraction: everything but {@code cas}. */
        public long operations()
        {
            return namedReads + sliceReads + rowWrites + deletes;
        }

        /** The fraction of operations that touch a single bucket (design 7.2 step 3); 0 with no operation. */
        public double pointFraction()
        {
            long ops = operations();
            return ops == 0 ? 0 : (double) (namedReads + rowWrites) / ops;
        }

        public long timedOperations()
        {
            return timedReads + timedWrites;
        }

        public double meanReadMicros()
        {
            return timedReads == 0 ? Double.NaN : (double) readMicros / timedReads;
        }

        public double meanWriteMicros()
        {
            return timedWrites == 0 ? Double.NaN : (double) writeMicros / timedWrites;
        }

        public double errorRate()
        {
            long n = timedOperations();
            return n == 0 ? Double.NaN : (double) (readErrors + writeErrors) / n;
        }

        @Override
        public String toString()
        {
            return "named_reads=" + namedReads + " slice_reads=" + sliceReads + " row_writes=" + rowWrites + " deletes=" + deletes
                   + " cas=" + cas + " range_scans=" + scans + " p=" + String.format("%.2f", pointFraction());
        }
    }

    /** The published set of watches, read by the hooks. */
    public static final class Watches
    {
        private final Map<TableId, Map<ByteBuffer, Watch>> byTable;
        private final Map<TableId, LongAdder> scans = new HashMap<>();
        private final Map<TableId, Timing> rest = new HashMap<>();

        Watches(Map<TableId, Map<ByteBuffer, Watch>> byTable)
        {
            this.byTable = byTable;
            // range scans and the rest of the table are counted per table and carried over a republication
            Watches previous = current;
            for (TableId table : byTable.keySet())
            {
                LongAdder adder = previous == null ? null : previous.scans.get(table);
                scans.put(table, adder == null ? new LongAdder() : adder);
                Timing timing = previous == null ? null : previous.rest.get(table);
                rest.put(table, timing == null ? new Timing() : timing);
            }
        }

        Watch find(TableId table, ByteBuffer key)
        {
            Map<ByteBuffer, Watch> keys = byTable.get(table);
            return keys == null ? null : keys.get(key);
        }

        List<Watch> all()
        {
            List<Watch> out = new java.util.ArrayList<>();
            for (Map<ByteBuffer, Watch> keys : byTable.values())
                out.addAll(keys.values());
            return out;
        }

        long scans(TableId table)
        {
            LongAdder adder = scans.get(table);
            return adder == null ? 0 : adder.sum();
        }

        /**
         * Hook H17: a probe if a command of the group reads a watched key, or else, if the group reads the table of a
         * watched key, a probe of the rest of that table; else null.
         */
        public Probe read(SinglePartitionReadCommand.Group group)
        {
            HOOK_CALLS.incrementAndGet();
            Probe probe = null;
            Timing others = null;
            for (SinglePartitionReadCommand command : group.queries)
            {
                Watch watch = find(command.metadata().id, command.partitionKey().getKey());
                if (watch == null)
                {
                    others = others == null ? rest.get(command.metadata().id) : others;
                    continue;
                }
                ClusteringIndexFilter filter = command.clusteringIndexFilter();
                (filter instanceof ClusteringIndexNamesFilter ? watch.namedReads : watch.sliceReads).increment();
                probe = probe == null ? new Probe(true) : probe;
                probe.timings.add(watch.timing);
            }
            return probe != null ? probe : others(true, others);
        }

        /** Hook H16: a probe if a mutation writes a watched key, else one of the rest of a watched key's table it writes, else null. */
        public Probe write(List<? extends IMutation> mutations)
        {
            HOOK_CALLS.incrementAndGet();
            Probe probe = null;
            Timing others = null;
            for (IMutation m : mutations)
            {
                if (!(m instanceof Mutation))
                    continue;
                for (PartitionUpdate update : ((Mutation) m).getPartitionUpdates())
                {
                    Watch watch = find(update.metadata().id, update.partitionKey().getKey());
                    if (watch == null)
                    {
                        others = others == null ? rest.get(update.metadata().id) : others;
                        continue;
                    }
                    if (!update.partitionLevelDeletion().isLive() || update.deletionInfo().hasRanges())
                        watch.deletes.increment();
                    watch.rowWrites.add(update.rowCount());
                    probe = probe == null ? new Probe(false) : probe;
                    probe.timings.add(watch.timing);
                }
            }
            return probe != null ? probe : others(false, others);
        }

        private static Probe others(boolean read, Timing others)
        {
            if (others == null)
                return null;
            Probe probe = new Probe(read);
            probe.timings.add(others);
            return probe;
        }

        /** The requests to the table that touched no watched key, or null if no key of the table is watched. */
        Timing rest(TableId table)
        {
            return rest.get(table);
        }

        /** Hook H19. */
        public void cas(String keyspace, String table, ByteBuffer key)
        {
            HOOK_CALLS.incrementAndGet();
            if (byTable.isEmpty())
                return;
            TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
            Watch watch = metadata == null ? null : find(metadata.id, key);
            if (watch != null)
                watch.cas.increment();
        }

        /** Hook H18. */
        public void scan(TableId table)
        {
            HOOK_CALLS.incrementAndGet();
            LongAdder adder = scans.get(table);
            if (adder != null)
                adder.increment();
        }
    }

    /** One timed request that touched watched keys. */
    public static final class Probe
    {
        private final boolean read;
        private final java.util.ArrayList<Timing> timings = new java.util.ArrayList<>(1);

        private Probe(boolean read)
        {
            this.read = read;
        }

        public void done(long nanos)
        {
            for (Timing t : timings)
            {
                (read ? t.timedReads : t.timedWrites).increment();
                (read ? t.readNanos : t.writeNanos).add(nanos);
            }
        }

        public void failed()
        {
            for (Timing t : timings)
            {
                (read ? t.timedReads : t.timedWrites).increment();
                (read ? t.readErrors : t.writeErrors).increment();
            }
        }
    }
}
