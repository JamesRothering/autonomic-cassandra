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

import java.util.ArrayList;
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
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.pager.SinglePartitionPager;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

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

    private static final Sink STORAGE_PROXY = StorageProxy::read;
    private static final Supplier<ClusterMetadata> CURRENT = ClusterMetadata::current;

    private static Sink sink = STORAGE_PROXY;
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
