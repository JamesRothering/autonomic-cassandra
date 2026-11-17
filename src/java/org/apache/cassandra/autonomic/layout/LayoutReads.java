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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.transform.EmptyPartitionsDiscarder;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.pager.SinglePartitionPager;
import org.apache.cassandra.service.reads.ReadCoordinator;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;

import io.netty.util.concurrent.FastThreadLocal;

/**
 * Read routing for adapted partitions (design 3.4): hook H17 of {@code StorageProxy}. Runs on every node, whatever the
 * state of the autonomic layer. With an empty registry the hook costs one lookup of the registry and returns.
 *
 * <h3>What reaches the hook (finding F5, confirmed by reading the code)</h3>
 * Every coordinator read of a single partition of a regular table ends in
 * {@code StorageProxy.read(SinglePartitionReadCommand.Group, ConsistencyLevel, RequestTime)}, and only through
 * {@code SinglePartitionReadCommand.execute} (a group of one) or {@code SinglePartitionReadCommand.Group.execute}:
 * <ul>
 * <li>{@code SelectStatement.execute} without paging calls {@code ReadQuery.execute} on the command or group;</li>
 * <li>with paging, and for every aggregate, it goes through a {@code QueryPager}: {@code SinglePartitionPager},
 *     {@code MultiPartitionPager} (one {@code SinglePartitionPager} per partition) and {@code AggregationQueryPager}
 *     (which drives one of the former) all reach {@code AbstractQueryPager.fetchPage}, which calls
 *     {@code ReadQuery.execute} on the command of the next page;</li>
 * <li>{@code ModificationStatement.readRequiredLists}, the read before a write of a list element, calls
 *     {@code Group.execute};</li>
 * <li>the reads of a lightweight transaction call {@code StorageProxy.readOne} or {@code read} from inside
 *     {@code StorageProxy.cas}, which hook H19 rejects first for an adapted key.</li>
 * </ul>
 * Not reaching it: {@code executeInternal} and {@code executeLocally} (node-local), virtual tables
 * ({@code VirtualTableGroup} overrides {@code execute}), a command whose clustering filter selects nothing
 * ({@code SinglePartitionReadCommand.execute} returns an empty result first), and {@code StorageProxy.fetchRows}
 * called directly by the Accord interoperability code, whose tables are not eligible.
 *
 * <h3>Routing</h3>
 * A command is <em>routed</em> if its key is in the registry, in any phase. In classes C0 and C1 it runs unchanged on
 * the base table; in C2 and C3 it is replaced by a fan-out over the buckets of the overflow table. Either way the
 * result is handed to the caller only if the routing state of every routed key ({@link RoutedKey}) is the same after
 * the read as before it (rule C1 of design 4.3); otherwise the result is discarded and the read executed again under
 * the new metadata, at most {@link #MAX_ATTEMPTS} times, then it fails with a read timeout. A group that routes
 * nothing takes the stock path and is not checked.
 *
 * <p>So that no row leaves before that check, results are held in memory: all of them when the read asks each
 * bucket for at most {@code fanout_eager_rows} rows (<em>eager</em>, every paged query), and always for a routed read of
 * the base table. A larger fan-out is <em>lazy</em>: each bucket is fetched through its own {@code SinglePartitionPager}
 * in chunks of {@code fanout_chunk_rows} rows. The first chunk of every bucket is part of the checked attempt; every
 * later chunk is checked after it was fetched and before any of its rows is returned, and a change then fails the
 * read with a read timeout, because rows of earlier chunks have already been returned.
 */
public final class LayoutReads
{
    public static final int MAX_ATTEMPTS = 3;

    /** How this class reaches {@code StorageProxy}; replaced by tests. Called with the re-entrancy flag set. */
    @VisibleForTesting
    interface Sink
    {
        PartitionIterator read(SinglePartitionReadCommand.Group group, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime);
    }

    /** How the range path reaches {@code StorageProxy.getRangeSlice}; replaced by tests. Called with the re-entrancy flag set. */
    @VisibleForTesting
    interface RangeSink
    {
        PartitionIterator read(PartitionRangeReadCommand command, ConsistencyLevel consistencyLevel, ReadCoordinator readCoordinator,
                               Dispatcher.RequestTime requestTime);
    }

    private static final Sink STORAGE_PROXY = StorageProxy::read;
    private static final RangeSink STORAGE_PROXY_RANGE = StorageProxy::getRangeSlice;
    private static final Supplier<ClusterMetadata> CURRENT = ClusterMetadata::current;

    private static Sink sink = STORAGE_PROXY;
    private static RangeSink rangeSink = STORAGE_PROXY_RANGE;
    private static Supplier<ClusterMetadata> metadata = CURRENT;

    /** True while this thread executes a read made by this class: the hook then lets it through untouched. */
    private static final FastThreadLocal<Boolean> reentrant = new FastThreadLocal<>()
    {
        @Override
        protected Boolean initialValue()
        {
            return Boolean.FALSE;
        }
    };

    private LayoutReads()
    {
    }

    @VisibleForTesting
    static void setForTesting(Sink testSink, Supplier<ClusterMetadata> testMetadata)
    {
        sink = testSink == null ? STORAGE_PROXY : testSink;
        metadata = testMetadata == null ? CURRENT : testMetadata;
    }

    @VisibleForTesting
    static void setRangeSinkForTesting(RangeSink testSink)
    {
        rangeSink = testSink == null ? STORAGE_PROXY_RANGE : testSink;
    }

    /** True while this thread executes a read made by this class. */
    @VisibleForTesting
    static boolean isReentrant()
    {
        return reentrant.get();
    }

    // ---------------------------------------------------------------------------------------------------------
    // H17
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Hook H17. Returns null if the caller must continue on the stock path, else the result of the group.
     *
     * @throws InvalidRequestException for a serial read of a key in class C2 or C3
     * @throws ReadTimeoutException if the routing of a key changed during each of {@link #MAX_ATTEMPTS} executions
     */
    public static PartitionIterator read(SinglePartitionReadCommand.Group group, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
    {
        // FC8: one read of a field that is null unless a key is profiled
        LayoutProfiler.Watches watches = LayoutProfiler.active();
        if (watches == null || reentrant.get())
            return readRegistered(group, consistencyLevel, requestTime);
        LayoutProfiler.Probe probe = watches.read(group);
        if (probe == null)
            return readRegistered(group, consistencyLevel, requestTime);
        long start = Clock.Global.nanoTime();
        try
        {
            // taken by the hook so that its latency is known
            PartitionIterator result = readRegistered(group, consistencyLevel, requestTime);
            if (result == null)
                result = stock(group, consistencyLevel, requestTime);
            probe.done(Clock.Global.nanoTime() - start);
            return result;
        }
        catch (RuntimeException e)
        {
            probe.failed();
            throw e;
        }
    }

    private static PartitionIterator readRegistered(SinglePartitionReadCommand.Group group, ConsistencyLevel consistencyLevel,
                                                    Dispatcher.RequestTime requestTime)
    {
        ClusterMetadata cm = metadata.get();
        LayoutRegistry registry = LayoutRegistry.of(cm);
        if (registry.byLogical.isEmpty() || reentrant.get())
            return null;

        Validation validation = null;
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++)
        {
            // the commands of a group differ in their partition key only
            if (registry.layout(group.queries.get(0).metadata().id) == null)
                return null;
            RoutedKey[] routed = new RoutedKey[group.queries.size()];
            List<RoutedKey> keys = new ArrayList<>(1);
            for (int i = 0; i < routed.length; i++)
            {
                SinglePartitionReadCommand command = group.queries.get(i);
                routed[i] = RoutedKey.of(cm, registry, command.metadata().id, command.partitionKey().getKey());
                if (routed[i] != null)
                    keys.add(routed[i]);
            }
            if (keys.isEmpty())
                return null;

            validation = new Validation(keys, consistencyLevel);
            PartitionIterator result = execute(group, routed, cm, consistencyLevel, requestTime, validation);
            ClusterMetadata after = metadata.get();
            if (after == cm || RoutedKey.unchanged(keys, after))
            {
                validation.armed = true;
                return result;
            }
            result.close();
            cm = after;
            registry = LayoutRegistry.of(cm);
        }
        throw validation.timeout();
    }

    // ---------------------------------------------------------------------------------------------------------
    // H18: range and token scans (design 3.5)
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Hook H18. Returns null if the caller must continue on the stock path, else the result of the scan.
     *
     * <p>A scan is routed if its key range contains a key that is in the registry, in any phase. The range is cut at
     * every such key: the stretches between them (the <em>gaps</em>) hold no routed key and are read by the stock
     * range read, with bounds that exclude the routed keys; each routed key is a segment of its own, read like a
     * single-partition read of design 3.4 (a fan-out in C2 and C3, a read of the base table in C0 and C1) and checked
     * (rule C1) before its rows are returned. Excluding the key from the gaps is what hides the stray rows the base
     * table may hold for it. The segments are concatenated in ring order, and the limits of the command are applied
     * over the concatenation.
     *
     * <p>Deviation from design 3.5, which cuts the range only at keys of class C2 and C3 and leaves C0 and C1 keys to
     * the stock scan: a stock scan streams, so a key in class C0 or C1 that moved on while it was being scanned could
     * not be detected before its rows were returned. As a segment it can.
     *
     * <p>As for single-partition reads, a scan whose limit is at most {@code fanout_eager_rows} rows (every paged
     * scan) is held in memory and checked as a whole, and repeated up to {@link #MAX_ATTEMPTS} times; a larger one is
     * read lazily, its first segment is part of the checked attempt and each later routed segment is checked before
     * its rows are returned.
     */
    public static PartitionIterator range(PartitionRangeReadCommand command, ConsistencyLevel consistencyLevel, ReadCoordinator readCoordinator,
                                          Dispatcher.RequestTime requestTime)
    {
        LayoutProfiler.Watches watches = LayoutProfiler.active();
        if (watches != null && !reentrant.get())
            watches.scan(command.metadata().id);
        ClusterMetadata cm = metadata.get();
        LayoutRegistry registry = LayoutRegistry.of(cm);
        if (registry.byLogical.isEmpty() || reentrant.get() || registry.layout(command.metadata().id) == null)
            return null;

        Validation validation = null;
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++)
        {
            List<RoutedInRange> inRange = routedInRange(cm, registry, command);
            if (inRange.isEmpty())
                return null;
            List<RoutedKey> keys = new ArrayList<>(inRange.size());
            for (RoutedInRange r : inRange)
                keys.add(r.routed);

            validation = new Validation(keys, consistencyLevel);
            PartitionIterator result = scan(command, inRange, cm, consistencyLevel, readCoordinator, requestTime, validation);
            ClusterMetadata after = metadata.get();
            if (after == cm || RoutedKey.unchanged(keys, after))
            {
                validation.armed = true;
                return result;
            }
            result.close();
            cm = after;
            registry = LayoutRegistry.of(cm);
        }
        throw validation.timeout();
    }

    /** A routed key inside the key range of a scan. */
    static final class RoutedInRange
    {
        final DecoratedKey key;
        final RoutedKey routed;

        RoutedInRange(DecoratedKey key, RoutedKey routed)
        {
            this.key = key;
            this.routed = routed;
        }
    }

    /** The routed keys of the table that the key range of the command contains, in ring order. */
    static List<RoutedInRange> routedInRange(ClusterMetadata cm, LayoutRegistry registry, PartitionRangeReadCommand command)
    {
        TableMetadata table = command.metadata();
        TableLayout layout = registry.layout(table.id);
        AbstractBounds<PartitionPosition> range = command.dataRange().keyRange();
        List<RoutedInRange> found = new ArrayList<>();
        if (layout == null)
            return found;
        for (ByteBuffer key : layout.keys.keySet())
        {
            DecoratedKey decorated = table.partitioner.decorateKey(key);
            if (!range.contains(decorated))
                continue;
            RoutedKey routed = RoutedKey.of(cm, registry, table.id, key);
            if (routed != null)
                found.add(new RoutedInRange(decorated, routed));
        }
        found.sort(Comparator.comparing(r -> r.key));
        return found;
    }

    /** One step of the plan of a scan, in ring order. */
    static final class Piece
    {
        enum Kind
        {
            /** A stock range read of a stretch that holds no routed key; streamed. */
            GAP,
            /** A stock range read of the base table that holds routed keys of class C0 or C1 only; held and checked. */
            BASE,
            /** The fan-out of one routed key of class C2 or C3. */
            FAN_OUT
        }

        final Kind kind;
        final AbstractBounds<PartitionPosition> bounds;
        final RoutedInRange key;

        Piece(Kind kind, AbstractBounds<PartitionPosition> bounds, RoutedInRange key)
        {
            this.kind = kind;
            this.bounds = bounds;
            this.key = key;
        }

        @Override
        public String toString()
        {
            return kind + (key == null ? " " + bounds : " " + ByteBufferUtil.bytesToHex(key.key.getKey()));
        }
    }

    /** One end of a range: a position and whether it belongs to the range. */
    private static final class End
    {
        final PartitionPosition position;
        final boolean inclusive;

        End(PartitionPosition position, boolean inclusive)
        {
            this.position = position;
            this.inclusive = inclusive;
        }
    }

    /**
     * The plan of a scan: the key range cut around the routed keys.
     *
     * <p>The stretches between the keys are bounded by the <em>tokens</em> of the keys, never by the keys themselves:
     * the stock coordinator merges the per-vnode pieces of a range and gives the merged range the right bound of its
     * last piece but the inclusiveness of its first one, so an exclusive right bound that is a partition key can come
     * back inclusive and return the very partition that was to be excluded. A token bound cannot hold a key on either
     * side, so it survives that. What lies inside the token of a routed key is then read by ranges that fit into one
     * piece, where a key bound is exact: the keys of one token are cut at the keys themselves (a collision of two
     * keys in one token is not a case that the plan may ignore, only one that costs two small reads).
     *
     * <p>The keys of one token that are all in class C0 or C1 are read from the base table as one range, the token.
     */
    @VisibleForTesting
    static List<Piece> plan(AbstractBounds<PartitionPosition> range, List<RoutedInRange> routed)
    {
        List<Piece> plan = new ArrayList<>();
        // the routed keys by token, in ring order; the keys of one token in key order
        TreeMap<Token, List<RoutedInRange>> byToken = new TreeMap<>();
        for (RoutedInRange r : routed)
            byToken.computeIfAbsent(r.key.getToken(), t -> new ArrayList<>()).add(r);

        End cursor = new End(range.left, range.inclusiveLeft());
        for (Map.Entry<Token, List<RoutedInRange>> entry : byToken.entrySet())
        {
            PartitionPosition min = entry.getKey().minKeyBound();
            PartitionPosition max = entry.getKey().maxKeyBound();
            List<RoutedInRange> keys = entry.getValue();
            keys.sort(Comparator.comparing(r -> r.key));
            add(plan, Piece.Kind.GAP, range, cursor, new End(min, false), null);

            boolean baseOnly = true;
            for (RoutedInRange r : keys)
                baseOnly &= r.routed.coordinatorClass() < 2;
            if (baseOnly)
            {
                add(plan, Piece.Kind.BASE, range, new End(min, true), new End(max, true), null);
            }
            else
            {
                End from = new End(min, true);
                for (RoutedInRange r : keys)
                {
                    add(plan, Piece.Kind.GAP, range, from, new End(r.key, false), null);
                    if (r.routed.coordinatorClass() < 2)
                        add(plan, Piece.Kind.BASE, range, new End(r.key, true), new End(r.key, true), null);
                    else
                        plan.add(new Piece(Piece.Kind.FAN_OUT, null, r));
                    from = new End(r.key, false);
                }
                add(plan, Piece.Kind.GAP, range, from, new End(max, true), null);
            }
            cursor = new End(max, false);
        }
        add(plan, Piece.Kind.GAP, range, cursor, new End(range.right, range.inclusiveRight()), null);
        return plan;
    }

    /** Adds the piece for [from, to] cut to the range, unless nothing is left of it. */
    private static void add(List<Piece> plan, Piece.Kind kind, AbstractBounds<PartitionPosition> range, End from, End to, RoutedInRange key)
    {
        boolean unboundedEnd = range.right.isMinimum();
        PartitionPosition left = from.position;
        boolean leftInclusive = from.inclusive;
        int c = left.compareTo(range.left);
        if (c < 0 || (c == 0 && !range.inclusiveLeft()))
        {
            left = range.left;
            leftInclusive = range.inclusiveLeft();
        }
        PartitionPosition right = to.position;
        boolean rightInclusive = to.inclusive;
        if (!unboundedEnd)
        {
            int d = right.compareTo(range.right);
            if (d > 0 || (d == 0 && !range.inclusiveRight()))
            {
                right = range.right;
                rightInclusive = range.inclusiveRight();
            }
        }
        // the end of the range as the end of a piece: its minimum bound means the end of the ring
        boolean toRingEnd = to.position == range.right && unboundedEnd;
        if (!toRingEnd)
        {
            int e = left.compareTo(right);
            if (e > 0 || (e == 0 && !(leftInclusive && rightInclusive)))
                return;
        }
        plan.add(new Piece(kind, AbstractBounds.bounds(left, leftInclusive, right, rightInclusive), key));
    }

    /** The pieces of a scan in ring order, each executed when it is reached. */
    private static PartitionIterator scan(PartitionRangeReadCommand command, List<RoutedInRange> inRange, ClusterMetadata cm,
                                          ConsistencyLevel consistencyLevel, ReadCoordinator readCoordinator,
                                          Dispatcher.RequestTime requestTime, Validation validation)
    {
        DataLimits limits = command.limits();
        boolean eager = !limits.isGroupByLimit() && !limits.isDistinct()
                        && limits.count() <= DatabaseDescriptor.getAutonomicConfig().layout.fanout_eager_rows;

        List<Supplier<PartitionIterator>> segments = new ArrayList<>();
        for (Piece piece : plan(command.dataRange().keyRange(), inRange))
        {
            boolean first = segments.isEmpty();
            switch (piece.kind)
            {
                case GAP:
                    segments.add(() -> stockRange(command.forSubRange(piece.bounds, first), consistencyLevel, readCoordinator, requestTime));
                    break;
                case BASE:
                    segments.add(() -> held(stockRange(command.forSubRange(piece.bounds, first), consistencyLevel, readCoordinator, requestTime), validation));
                    break;
                default:
                    segments.add(() -> {
                        DecoratedKey key = piece.key.key;
                        // the single-partition command of this key: the clustering filter carries the paging position when the scan resumes in it
                        SinglePartitionReadCommand single = SinglePartitionReadCommand.create(command.metadata(), command.nowInSec(), command.columnFilter(),
                                                                                              command.rowFilter(), first ? command.limits() : command.limits().withoutState(),
                                                                                              key, command.dataRange().clusteringIndexFilter(key));
                        return fanOut(single, piece.key.routed, cm, consistencyLevel, requestTime, validation);
                    });
            }
        }

        PartitionIterator result = limits.filter(new Segments(segments), command.nowInSec(), command.selectsFullPartition(),
                                                 command.metadata().enforceStrictLiveness());
        if (eager)
            return materialize(result);
        result.hasNext(); // the first segment is part of the checked attempt
        return result;
    }

    /** The base table is authoritative here; the result is held so that the keys can be checked before any row leaves. */
    private static PartitionIterator held(PartitionIterator read, Validation validation)
    {
        PartitionIterator held = materialize(read);
        try
        {
            validation.run();
        }
        catch (Throwable t)
        {
            held.close();
            throw t;
        }
        return held;
    }

    private static PartitionIterator stockRange(PartitionRangeReadCommand command, ConsistencyLevel consistencyLevel, ReadCoordinator readCoordinator,
                                                Dispatcher.RequestTime requestTime)
    {
        boolean outer = reentrant.get();
        reentrant.set(Boolean.TRUE);
        try
        {
            return rangeSink.read(command, consistencyLevel, readCoordinator, requestTime);
        }
        finally
        {
            reentrant.set(outer);
        }
    }

    /** Partitions of a list of segments, each created when the previous one is exhausted. */
    private static final class Segments implements PartitionIterator
    {
        private final List<Supplier<PartitionIterator>> segments;
        private int next;
        private PartitionIterator current;

        Segments(List<Supplier<PartitionIterator>> segments)
        {
            this.segments = segments;
        }

        @Override
        public boolean hasNext()
        {
            while (current == null || !current.hasNext())
            {
                if (current != null)
                {
                    current.close();
                    current = null;
                }
                if (next >= segments.size())
                    return false;
                current = segments.get(next++).get();
            }
            return true;
        }

        @Override
        public RowIterator next()
        {
            if (!hasNext())
                throw new NoSuchElementException();
            return current.next();
        }

        @Override
        public void close()
        {
            if (current != null)
                current.close();
            current = null;
            next = segments.size();
        }
    }

    /**
     * The check of rule C1 for the chunks a lazy fan-out fetches after the read was returned to its caller. Not armed
     * during an attempt: the attempt is checked as a whole and can still be repeated.
     */
    static final class Validation implements Runnable
    {
        private final List<RoutedKey> keys;
        private final ConsistencyLevel consistencyLevel;
        private boolean armed;

        Validation(List<RoutedKey> keys, ConsistencyLevel consistencyLevel)
        {
            this.keys = keys;
            this.consistencyLevel = consistencyLevel;
        }

        @Override
        public void run()
        {
            if (armed && !RoutedKey.unchanged(keys, metadata.get()))
                throw timeout();
        }

        ReadTimeoutException timeout()
        {
            int blockFor;
            try
            {
                blockFor = consistencyLevel.blockFor(Keyspace.open(keys.get(0).layout.keyspace).getReplicationStrategy());
            }
            catch (RuntimeException e)
            {
                blockFor = 1;
            }
            return new ReadTimeoutException(consistencyLevel, 0, blockFor, false,
                                            armed ? "The layout of an adapted partition changed while it was read"
                                                  : "The layout of an adapted partition changed during each of " + MAX_ATTEMPTS + " attempts to read it");
        }
    }

    /**
     * One attempt, entirely under {@code cm}. Commands of keys in class C2 or C3 are replaced by a fan-out; the others
     * run on the stock path, in runs that keep the order of the group, and their results are held in memory.
     */
    private static PartitionIterator execute(SinglePartitionReadCommand.Group group, RoutedKey[] routed, ClusterMetadata cm,
                                             ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime, Validation validation)
    {
        boolean replaced = false;
        for (int i = 0; i < routed.length; i++)
        {
            if (routed[i] == null || routed[i].coordinatorClass() < 2)
                continue;
            if (consistencyLevel.isSerialConsistency())
            {
                SinglePartitionReadCommand command = group.queries.get(i);
                throw new InvalidRequestException(consistencyLevel + " reads are not supported on partition 0x"
                                                  + ByteBufferUtil.bytesToHex(command.partitionKey().getKey()) + " of " + command.metadata()
                                                  + " while it is adapted");
            }
            replaced = true;
        }
        if (!replaced)
            return materialize(stock(group, consistencyLevel, requestTime));

        List<PartitionIterator> parts = new ArrayList<>();
        try
        {
            List<SinglePartitionReadCommand> run = new ArrayList<>();
            for (int i = 0; i < routed.length; i++)
            {
                SinglePartitionReadCommand command = group.queries.get(i);
                if (routed[i] == null || routed[i].coordinatorClass() < 2)
                {
                    run.add(command);
                    continue;
                }
                if (!run.isEmpty())
                {
                    parts.add(materialize(stock(SinglePartitionReadCommand.Group.create(run, group.limits()), consistencyLevel, requestTime)));
                    run = new ArrayList<>();
                }
                parts.add(fanOut(command, routed[i], cm, consistencyLevel, requestTime, validation));
            }
            if (!run.isEmpty())
                parts.add(materialize(stock(SinglePartitionReadCommand.Group.create(run, group.limits()), consistencyLevel, requestTime)));
        }
        catch (Throwable t)
        {
            for (PartitionIterator part : parts)
                part.close();
            throw t;
        }
        if (group.queries.size() == 1)
            return parts.get(0);
        // as StorageProxy.maybeEnforceLimits: each command honoured the limits, the group as a whole may not
        return group.limits().filter(PartitionIterators.concat(parts), group.nowInSec(), group.selectsFullPartition(),
                                     group.queries.get(0).metadata().enforceStrictLiveness());
    }

    // ---------------------------------------------------------------------------------------------------------
    // fan-out of one command
    // ---------------------------------------------------------------------------------------------------------

    /**
     * The result of one logical command of a key in class C2 or C3, read from the overflow table (design 3.4, steps 1
     * to 6): at most one partition, with the logical metadata, key and columns. When this returns, an eager fan-out
     * is complete and in memory and a lazy one has fetched the first chunk of every bucket; {@code validation} is run
     * after every later chunk.
     */
    static PartitionIterator fanOut(SinglePartitionReadCommand command, RoutedKey routed, ClusterMetadata cm,
                                    ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime, Runnable validation)
    {
        TableLayout layout = routed.layout;
        TableMetadata logical = command.metadata();
        TableMetadata physical = RoutedKey.physical(cm, layout);
        ClusteringIndexFilter filter = command.clusteringIndexFilter();
        DataLimits limits = command.limits();

        // A row filter is applied here, on the merged rows; it is not translated to the overflow table (design 3.4,
        // step 2, takes that path when a filter cannot be translated). Then, and for limits that do not count rows,
        // nothing can be cut per bucket.
        boolean filtered = !command.rowFilter().isEmpty();
        int perBucket = filtered || limits.isGroupByLimit() || limits.isDistinct() ? DataLimits.NO_LIMIT
                                                                                 : Math.min(limits.count(), limits.perPartitionCount());
        // A per-partition limit, not a row limit: StorageProxy enforces the limits of a group over the whole group,
        // which for a row limit would cut the buckets in the order of the group instead of the order of the rows.
        DataLimits bucketLimits = DataLimits.cqlLimits(DataLimits.NO_LIMIT, perBucket);

        List<SinglePartitionReadCommand> commands = new ArrayList<>();
        for (Map.Entry<Integer, ClusteringIndexFilter> bucket : buckets(filter, layout, logical).entrySet())
        {
            DecoratedKey key = RowMapper.physicalKey(logical, physical, command.partitionKey().getKey(), routed.state.incarnation, bucket.getKey());
            commands.add(SinglePartitionReadCommand.create(physical, command.nowInSec(), ColumnFilter.all(physical), RowFilter.none(), bucketLimits,
                                                           key, bucket.getValue()));
        }

        boolean eager = perBucket <= DatabaseDescriptor.getAutonomicConfig().layout.fanout_eager_rows;
        List<FanOutIterator.Source> sources = eager ? eager(commands, bucketLimits, consistencyLevel, requestTime)
                                                    : lazy(commands, consistencyLevel, requestTime, validation);
        FanOutIterator merged = new FanOutIterator(logical, command.partitionKey(), filter.isReversed(), logical.regularAndStaticColumns(),
                                                   Rows.EMPTY_STATIC_ROW, sources, true);
        merged.hasNext(); // looks at every bucket: a lazy fan-out fetches its first chunks here

        PartitionIterator result = PartitionIterators.singletonIterator(merged);
        if (filtered)
            result = command.rowFilter().filter(result, logical, command.nowInSec());
        // the limits of the logical command over the merged rows, so that they are exact
        result = limits.filter(result, command.nowInSec(), command.selectsFullPartition(), logical.enforceStrictLiveness());
        result = Transformation.apply(result, new EmptyPartitionsDiscarder());
        return eager ? materialize(result) : result;
    }

    /**
     * Step 1: the buckets to read, each with its clustering filter. A names filter reads the buckets of the named
     * rows, each with the names that hash to it; a slice filter reads every bucket with the same slices.
     */
    private static Map<Integer, ClusteringIndexFilter> buckets(ClusteringIndexFilter filter, TableLayout layout, TableMetadata logical)
    {
        Map<Integer, ClusteringIndexFilter> buckets = new TreeMap<>();
        if (filter instanceof ClusteringIndexNamesFilter)
        {
            Map<Integer, NavigableSet<Clustering<?>>> names = new TreeMap<>();
            for (Clustering<?> clustering : ((ClusteringIndexNamesFilter) filter).requestedRows())
                names.computeIfAbsent(BucketFunction.bucket(clustering, layout.hashedClusteringColumns, layout.buckets),
                                      b -> new TreeSet<>(logical.comparator))
                     .add(clustering);
            names.forEach((bucket, set) -> buckets.put(bucket, new ClusteringIndexNamesFilter(set, filter.isReversed())));
        }
        else
        {
            for (int bucket = 0; bucket < layout.buckets; bucket++)
                buckets.put(bucket, filter);
        }
        return buckets;
    }

    /** Step 4, eager: the bucket commands as one group, read in parallel, at the consistency level of the client. */
    private static List<FanOutIterator.Source> eager(List<SinglePartitionReadCommand> commands, DataLimits bucketLimits,
                                                     ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
    {
        List<FanOutIterator.Source> sources = new ArrayList<>(commands.size());
        if (commands.isEmpty())
            return sources;
        // a bucket without live rows is not in the result, so the partitions are matched by key
        Map<DecoratedKey, List<Row>> byKey = new HashMap<>();
        try (PartitionIterator partitions = stock(SinglePartitionReadCommand.Group.create(commands, bucketLimits), consistencyLevel, requestTime))
        {
            while (partitions.hasNext())
            {
                try (RowIterator partition = partitions.next())
                {
                    byKey.put(partition.partitionKey(), drain(partition));
                }
            }
        }
        for (SinglePartitionReadCommand command : commands)
        {
            List<Row> rows = byKey.get(command.partitionKey());
            if (rows != null)
                sources.add(FanOutIterator.Source.eager(rows));
        }
        return sources;
    }

    /** Step 4, lazy: one pager per bucket; a chunk is checked after it was fetched and before its rows are used. */
    private static List<FanOutIterator.Source> lazy(List<SinglePartitionReadCommand> commands, ConsistencyLevel consistencyLevel,
                                                    Dispatcher.RequestTime requestTime, Runnable validation)
    {
        PageSize chunk = PageSize.inRows(Math.max(1, DatabaseDescriptor.getAutonomicConfig().layout.fanout_chunk_rows));
        List<FanOutIterator.Source> sources = new ArrayList<>(commands.size());
        for (SinglePartitionReadCommand command : commands)
        {
            SinglePartitionPager pager = new SinglePartitionPager(command, null, ProtocolVersion.CURRENT);
            sources.add(FanOutIterator.Source.lazy(() -> {
                if (pager.isExhausted())
                    return null;
                List<Row> rows = new ArrayList<>();
                boolean outer = reentrant.get();
                reentrant.set(Boolean.TRUE);
                try (PartitionIterator page = pager.fetchPage(chunk, consistencyLevel, null, requestTime))
                {
                    while (page.hasNext())
                    {
                        try (RowIterator partition = page.next())
                        {
                            rows.addAll(drain(partition));
                        }
                    }
                }
                finally
                {
                    reentrant.set(outer);
                }
                validation.run();
                return rows;
            }));
        }
        return sources;
    }

    // ---------------------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------------------

    private static PartitionIterator stock(SinglePartitionReadCommand.Group group, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
    {
        boolean outer = reentrant.get();
        reentrant.set(Boolean.TRUE);
        try
        {
            return sink.read(group, consistencyLevel, requestTime);
        }
        finally
        {
            reentrant.set(outer);
        }
    }

    private static List<Row> drain(RowIterator partition)
    {
        List<Row> rows = new ArrayList<>();
        while (partition.hasNext())
            rows.add(partition.next());
        return rows;
    }

    /**
     * Reads the whole result into memory and closes it. Iterating a stock result can still read from replicas (short
     * read protection, read repair), so a result is only as old as its last row.
     */
    private static PartitionIterator materialize(PartitionIterator result)
    {
        List<RowIterator> partitions = new ArrayList<>();
        boolean outer = reentrant.get();
        reentrant.set(Boolean.TRUE);
        try (PartitionIterator source = result)
        {
            while (source.hasNext())
            {
                try (RowIterator partition = source.next())
                {
                    Row staticRow = partition.staticRow();
                    List<Row> rows = drain(partition);
                    partitions.add(new FanOutIterator(partition.metadata(), partition.partitionKey(), partition.isReverseOrder(), partition.columns(),
                                                      staticRow, List.of(FanOutIterator.Source.eager(rows)), false));
                }
            }
        }
        finally
        {
            reentrant.set(outer);
        }
        return new Materialized(partitions);
    }

    private static final class Materialized implements PartitionIterator
    {
        private final List<RowIterator> partitions;
        private int next;

        Materialized(List<RowIterator> partitions)
        {
            this.partitions = partitions;
        }

        @Override
        public boolean hasNext()
        {
            return next < partitions.size();
        }

        @Override
        public RowIterator next()
        {
            if (!hasNext())
                throw new NoSuchElementException();
            return partitions.get(next++);
        }

        @Override
        public void close()
        {
        }
    }
}
