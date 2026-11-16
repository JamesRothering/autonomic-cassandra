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
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.PreserveTimestamp;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;

import io.netty.util.concurrent.FastThreadLocal;

/**
 * Write routing for adapted partitions (design 3.3, 3.6): hooks H16 ({@link #route}), H19 ({@link #checkCas}) and H20
 * ({@link #beforeTruncate}) of {@code StorageProxy}. Runs on every node, whatever the state of the autonomic layer.
 *
 * <p>With an empty registry each hook costs one lookup of the registry in the current metadata and returns.
 *
 * <p>A write is <em>routed</em> if it updates a key that is in the registry, in any phase. A routed write executes
 * entirely under the metadata read before it started and is acknowledged only if, afterwards, every key it routed is
 * still in the same routing state (rule C1 of design 4.3); otherwise it is executed again under the new metadata, at
 * most {@link #MAX_ATTEMPTS} times. The routing state of a key is defined by {@link RoutedKey}: backfill reports and
 * generation restarts do not change it and do not cause a re-execution. A write that routes nothing takes the stock
 * path and is not checked.
 *
 * <p>Order of the copies (design 3.2): the copy that reads use is written last, after the other copy acknowledged at
 * the client's consistency level. Every send goes through the stock tail of {@code StorageProxy.mutateWithTriggers}
 * under the re-entrancy flag: not atomic it is {@code StorageProxy.mutate} with the stock retry on a coordinator that
 * is behind, atomic it is {@code StorageProxy.mutateAtomically}.
 */
public final class LayoutWrites
{
    public static final int MAX_ATTEMPTS = 3;

    /** How this class reaches {@code StorageProxy}; replaced by tests to record the calls. */
    @VisibleForTesting
    interface Sink
    {
        /** The stock tail of {@code StorageProxy.mutateWithTriggers}; called with the re-entrancy flag set. */
        void write(List<? extends IMutation> mutations, ConsistencyLevel consistencyLevel, boolean atomic,
                   Dispatcher.RequestTime requestTime, PreserveTimestamp preserveTimestamps);

        void truncate(String keyspace, String table) throws UnavailableException, TimeoutException;
    }

    private static final Sink STORAGE_PROXY = new Sink()
    {
        @Override
        public void write(List<? extends IMutation> mutations, ConsistencyLevel consistencyLevel, boolean atomic,
                          Dispatcher.RequestTime requestTime, PreserveTimestamp preserveTimestamps)
        {
            StorageProxy.mutateWithTriggers(mutations, consistencyLevel, atomic, requestTime, preserveTimestamps);
        }

        @Override
        public void truncate(String keyspace, String table) throws UnavailableException, TimeoutException
        {
            StorageProxy.truncateBlocking(keyspace, table);
        }
    };

    private static final Supplier<ClusterMetadata> CURRENT = ClusterMetadata::current;

    private static Sink sink = STORAGE_PROXY;
    private static Supplier<ClusterMetadata> metadata = CURRENT;

    /** True while this thread executes a call made by this class: the hooks then let it through untouched. */
    private static final FastThreadLocal<Boolean> reentrant = new FastThreadLocal<>()
    {
        @Override
        protected Boolean initialValue()
        {
            return Boolean.FALSE;
        }
    };

    private LayoutWrites()
    {
    }

    @VisibleForTesting
    static void setForTesting(Sink testSink, Supplier<ClusterMetadata> testMetadata)
    {
        sink = testSink == null ? STORAGE_PROXY : testSink;
        metadata = testMetadata == null ? CURRENT : testMetadata;
    }

    // ---------------------------------------------------------------------------------------------------------
    // H16
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Hook H16. Returns false if the caller must continue on the stock path, true if the write was executed and may be
     * acknowledged.
     *
     * @throws InvalidRequestException if a mutation writes to an overflow table directly
     * @throws WriteTimeoutException if the routing of a key changed during each of {@link #MAX_ATTEMPTS} executions
     */
    public static boolean route(List<? extends IMutation> mutations, ConsistencyLevel consistencyLevel, boolean atomic,
                                Dispatcher.RequestTime requestTime, PreserveTimestamp preserveTimestamps)
    {
        ClusterMetadata cm = metadata.get();
        LayoutRegistry registry = LayoutRegistry.of(cm);
        if (registry.byLogical.isEmpty() || reentrant.get())
            return false;

        Plan plan = null;
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++)
        {
            plan = plan(mutations, consistencyLevel, cm, registry);
            if (plan.routed.isEmpty())
                return false;
            execute(plan, atomic, requestTime, preserveTimestamps);
            ClusterMetadata after = metadata.get();
            if (after == cm || RoutedKey.unchanged(plan.routed, after))
                return true;
            cm = after;
            registry = LayoutRegistry.of(cm);
        }
        // the outcome is unknown to the client, as after any write timeout
        throw new WriteTimeoutException(atomic ? WriteType.BATCH : mutations.size() <= 1 ? WriteType.SIMPLE : WriteType.UNLOGGED_BATCH,
                                        plan.consistencyLevel, 0, plan.blockFor(),
                                        "The layout of an adapted partition changed during each of " + MAX_ATTEMPTS + " attempts to write it");
    }

    private static final class Plan
    {
        final ConsistencyLevel consistencyLevel;
        final List<RoutedKey> routed = new ArrayList<>();
        /** Shadows of keys in class C1: written before the original. */
        final List<IMutation> shadowsFirst = new ArrayList<>();
        /** The caller's mutations; for keys in class C3 the base updates are replaced by their shadows. */
        List<? extends IMutation> main;
        boolean replaced;
        /** Shadows of keys in class C2: written after the original. */
        final List<IMutation> shadowsLast = new ArrayList<>();
        String keyspace;

        Plan(ConsistencyLevel consistencyLevel)
        {
            this.consistencyLevel = consistencyLevel;
        }

        int blockFor()
        {
            try
            {
                return consistencyLevel.blockFor(Keyspace.open(keyspace).getReplicationStrategy());
            }
            catch (RuntimeException e)
            {
                return 1;
            }
        }
    }

    /**
     * {@code split} of design 3.3. Reads nothing but its arguments, so a plan is entirely a function of one metadata.
     *
     * @throws InvalidRequestException if a mutation writes to an overflow table
     */
    private static Plan plan(List<? extends IMutation> mutations, ConsistencyLevel consistencyLevel, ClusterMetadata cm, LayoutRegistry registry)
    {
        // a hint-only acknowledgement would leave the order of the two copies nothing to stand on
        Plan plan = new Plan(consistencyLevel == ConsistencyLevel.ANY ? ConsistencyLevel.ONE : consistencyLevel);
        List<IMutation> main = null;
        int index = 0;
        for (IMutation mutation : mutations)
        {
            IMutation kept = mutation;
            List<IMutation> c3Shadows = null;
            if (mutation instanceof Mutation)
            {
                for (PartitionUpdate update : ((Mutation) mutation).getPartitionUpdates())
                {
                    TableId table = update.metadata().id;
                    RoutedKey routed = RoutedKey.of(cm, registry, table, update.partitionKey().getKey());
                    if (routed == null)
                    {
                        rejectDirectWrite(registry, table);
                        continue;
                    }
                    TableLayout layout = routed.layout;
                    plan.routed.add(routed);
                    plan.keyspace = layout.keyspace;
                    int coordinatorClass = routed.coordinatorClass();
                    if (coordinatorClass == 0)
                        continue;
                    List<IMutation> shadows = coordinatorClass == 1 ? plan.shadowsFirst
                                              : coordinatorClass == 2 ? plan.shadowsLast
                                              : c3Shadows == null ? c3Shadows = new ArrayList<>() : c3Shadows;
                    for (PartitionUpdate shadow : RowMapper.toPhysical(update, RoutedKey.physical(cm, layout), layout, routed.state.incarnation).values())
                        shadows.add(new Mutation(shadow));
                    if (coordinatorClass == 3 && kept != null)
                        kept = ((Mutation) kept).without(table);
                }
            }
            if (c3Shadows != null && main == null)
                main = new ArrayList<>(mutations.subList(0, index));
            if (main != null)
            {
                if (kept != null)
                    main.add(kept);
                if (c3Shadows != null)
                    main.addAll(c3Shadows);
            }
            index++;
        }
        plan.main = main == null ? mutations : main;
        plan.replaced = main != null;
        return plan;
    }

    /**
     * The class table of design 3.3. Not atomic: shadows of C1 keys, then the caller's mutations (with the base updates
     * of C3 keys replaced by their shadows), then shadows of C2 keys; each step returns once it is acknowledged at the
     * consistency level. A step of several shadow mutations goes through the batchlog so that a delete cannot stay
     * half applied across buckets. Atomic: everything in one logged batch, which is acknowledged once every mutation is.
     */
    private static void execute(Plan plan, boolean atomic, Dispatcher.RequestTime requestTime, PreserveTimestamp preserveTimestamps)
    {
        boolean outer = reentrant.get();
        reentrant.set(Boolean.TRUE);
        try
        {
            if (atomic)
            {
                List<? extends IMutation> all = plan.main;
                if (!plan.shadowsFirst.isEmpty() || !plan.shadowsLast.isEmpty())
                {
                    List<IMutation> joined = new ArrayList<>(plan.main);
                    joined.addAll(plan.shadowsFirst);
                    joined.addAll(plan.shadowsLast);
                    all = joined;
                }
                if (!all.isEmpty())
                    sink.write(all, plan.consistencyLevel, true, requestTime, preserveTimestamps);
                return;
            }
            if (!plan.shadowsFirst.isEmpty())
                sink.write(plan.shadowsFirst, plan.consistencyLevel, plan.shadowsFirst.size() > 1, requestTime, preserveTimestamps);
            if (!plan.main.isEmpty())
                sink.write(plan.main, plan.consistencyLevel, plan.replaced && plan.main.size() > 1, requestTime, preserveTimestamps);
            if (!plan.shadowsLast.isEmpty())
                sink.write(plan.shadowsLast, plan.consistencyLevel, plan.shadowsLast.size() > 1, requestTime, preserveTimestamps);
        }
        finally
        {
            reentrant.set(outer);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // H19, H20
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Hook H19: a lightweight transaction on a key that is in the registry, in any phase, is rejected (design 3.6), and
     * so is one on an overflow table.
     */
    public static void checkCas(String keyspace, String table, DecoratedKey key)
    {
        ClusterMetadata cm = metadata.get();
        LayoutRegistry registry = LayoutRegistry.of(cm);
        if (registry.byLogical.isEmpty())
            return;
        for (TableLayout layout : registry.byLogical.values())
        {
            if (!layout.keyspace.equals(keyspace))
                continue;
            if (layout.physicalName.equals(table))
                throw managed(layout);
            if (layout.logicalName.equals(table) && layout.keyState(key.getKey()) != null && RoutedKey.logical(cm, layout) != null)
                throw new InvalidRequestException("Lightweight transactions are not supported on partition 0x" + ByteBufferUtil.bytesToHex(key.getKey())
                                                  + " of " + keyspace + '.' + table + " while it is adapted");
        }
    }

    /**
     * Hook H20 (design 3.6). A table with a key in a phase other than {@code STEADY} cannot be truncated. Otherwise
     * the overflow table is truncated here, before the stock truncate of the base table that follows. An overflow
     * table cannot be truncated directly.
     */
    public static void beforeTruncate(String keyspace, String table) throws UnavailableException, TimeoutException
    {
        ClusterMetadata cm = metadata.get();
        LayoutRegistry registry = LayoutRegistry.of(cm);
        if (registry.byLogical.isEmpty() || reentrant.get())
            return;
        for (TableLayout layout : registry.byLogical.values())
        {
            if (!layout.keyspace.equals(keyspace))
                continue;
            if (layout.physicalName.equals(table))
                throw managed(layout);
            if (!layout.logicalName.equals(table) || RoutedKey.logical(cm, layout) == null)
                continue;
            for (KeyState state : layout.keys.values())
                if (state.phase != Phase.STEADY)
                    throw new InvalidRequestException(keyspace + '.' + table + " cannot be truncated while a partition is being adapted (phase "
                                                      + state.phase + "); retry after the migration");
            boolean outer = reentrant.get();
            reentrant.set(Boolean.TRUE);
            try
            {
                sink.truncate(keyspace, layout.physicalName);
            }
            finally
            {
                reentrant.set(outer);
            }
            return;
        }
    }

    private static void rejectDirectWrite(LayoutRegistry registry, TableId table)
    {
        for (TableLayout layout : registry.byLogical.values())
            if (layout.physical.equals(table))
                throw managed(layout);
    }

    private static InvalidRequestException managed(TableLayout layout)
    {
        return new InvalidRequestException(layout.keyspace + '.' + layout.physicalName + " is managed by the adapted layout of "
                                           + layout.keyspace + '.' + layout.logicalName + " and cannot be written directly");
    }
}
