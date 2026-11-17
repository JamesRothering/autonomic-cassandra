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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.autonomic.knowledge.KnowledgeStore;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.MD5Digest;

/**
 * The replica backfill of an adapted partition (design 4.5, ADR-018): while a key is in {@code BACKFILL} or
 * {@code CLEANUP}, every natural replica of the key's token copies what it holds locally for the partition in the base
 * table into the overflow table, and then commits {@link ReportBackfill}. It is the work of the {@code LayoutBackfill}
 * thread of {@link LayoutService} and runs on every node, whatever the state of the autonomic layer.
 *
 * <p><b>What is copied.</b> The partition is read locally in chunks, each by its own slice command, with
 * {@code nowInSec = 0}: the stock local read then skips its purge of gc-able tombstones, and nothing is judged
 * expired. The chunk is therefore the merge of everything the replica holds in memtables and sstables for the slice:
 * live cells, expiring cells whether expired or not, cell tombstones, row deletions, complex deletions, range
 * tombstones and the partition deletion, each with its timestamp, TTL and local deletion time. The only data the
 * replica holds that is not copied is data the merge itself removes because a deletion that <em>is</em> copied
 * shadows it. Cells are re-labelled to the columns of the overflow table by {@link RowMapper}, never rebuilt.
 *
 * <p><b>Chunks and range tombstones.</b> A chunk ends after a row or after the closing marker of a range tombstone,
 * never right after an opening one. If a range tombstone is open where the chunk ends, the chunk carries it up to and
 * including the last row of the chunk, and the next chunk, whose slice starts strictly after that row, receives the
 * rest from the storage engine, which opens a covering tombstone at the start of a slice. The union is the range
 * the base table holds, neither wider nor narrower. The position recorded for resumption is that slice start, a
 * clustering bound; it is recorded only after the chunk was acknowledged by every replica of the overflow table.
 *
 * <p><b>Writes.</b> Each chunk is written through the stock write path at {@code ALL}, outside the routing of
 * {@link LayoutWrites}. A failed write is repeated, with backoff, for as long as the key stays in the generation the
 * pass was started for; repeating is harmless because the cells and their timestamps are the same.
 *
 * <p><b>A pass belongs to one generation</b> of one attempt: layout, incarnation, phase, generation number and the
 * epoch at which the generation started. Before every chunk and before the report the pass looks at the current
 * metadata and ends without reporting if any of them changed or this node is no longer a natural replica. A key that
 * enters {@code CLEANUP} brings the generation of its backfill with it; only a generation started in {@code CLEANUP},
 * the reconcile pass, is run there ({@link KeyState#backfillDue}).
 *
 * <p><b>The tombstone deadline</b> (invariant I9, ADR-031). A delete made while the key is split may exist in the
 * overflow table only, and is purged there {@code gc_grace_seconds} after it was made at the earliest; a base row
 * copied after that would come back to life. So nothing is copied later than half of {@code gc_grace_seconds} after
 * the key entered {@code DUAL_WRITE}, by this node's clock: before every chunk write and before the report the pass
 * compares the clock with {@link KeyState#backfillLimitMillis} less the write timeout, the longest a write handed to
 * the stock path can still take to reach a replica that is up. Past it, the pass writes nothing more and does not
 * report, for as long as the key stays in that incarnation.
 */
public final class BackfillTask
{
    private static final Logger logger = LoggerFactory.getLogger(BackfillTask.class);

    /** Prefix of the resume records in {@code system_autonomic.local_state}. */
    static final String STATE_PREFIX = "layout/";

    /**
     * The local read time of a backfill chunk. With it {@code ReadCommand.executeLocally} does not purge and nothing
     * counts as expired, see the class comment.
     */
    static final long NO_PURGE = 0;

    /** What the task needs from the node; replaced in unit tests. */
    @VisibleForTesting
    interface Env
    {
        ClusterMetadata metadata();

        /** The node id of this node in the metadata, or null if it has none yet. */
        NodeId self(ClusterMetadata cm);

        /** True if this node is a natural replica of the key of the logical table. */
        boolean isNaturalReplica(ClusterMetadata cm, TableMetadata logical, DecoratedKey key);

        /** Writes at ALL on the stock path; returns when every replica acknowledged, throws otherwise. */
        void write(List<Mutation> mutations);

        /** True if the report was committed (or was already), false if it was rejected; throws if that is unknown. */
        boolean report(ReportBackfill report);

        Optional<String> getState(String key);

        boolean putState(String key, String value);

        void deleteState(String key);

        Map<String, String> scanState(String prefix);

        long throughputBytesPerSecond();

        int chunkRows();

        /** Returns false if the thread was interrupted. */
        boolean sleep(long millis);

        /** This node's wall clock. */
        long nowMillis();

        /** The longest a write of {@link #write} can take to reach a live replica after it was handed over. */
        long writeTimeoutMillis();
    }

    private static final class NodeEnv implements Env
    {
        private final KnowledgeStore store = new KnowledgeStore();

        @Override
        public ClusterMetadata metadata()
        {
            return ClusterMetadata.current();
        }

        @Override
        public NodeId self(ClusterMetadata cm)
        {
            return cm.myNodeId();
        }

        @Override
        public boolean isNaturalReplica(ClusterMetadata cm, TableMetadata logical, DecoratedKey key)
        {
            return BackfillTask.isNaturalReplica(cm, logical, key);
        }

        @Override
        public void write(List<Mutation> mutations)
        {
            LayoutWrites.writeUnrouted(mutations, ConsistencyLevel.ALL, Dispatcher.RequestTime.forImmediateExecution());
        }

        @Override
        public boolean report(ReportBackfill report)
        {
            return ClusterMetadataService.instance().commit(report.wrap(), metadata -> true, (code, message) -> {
                logger.info("{} was rejected: {}", report, message);
                return false;
            });
        }

        @Override
        public Optional<String> getState(String key)
        {
            return store.getState(key);
        }

        @Override
        public boolean putState(String key, String value)
        {
            return store.putState(key, value, Clock.Global.currentTimeMillis());
        }

        @Override
        public void deleteState(String key)
        {
            store.deleteState(key);
        }

        @Override
        public Map<String, String> scanState(String prefix)
        {
            return store.scanState(prefix);
        }

        @Override
        public long throughputBytesPerSecond()
        {
            return (long) DatabaseDescriptor.getAutonomicConfig().layout.backfill_throughput.toBytesPerSecond();
        }

        @Override
        public int chunkRows()
        {
            return DatabaseDescriptor.getAutonomicConfig().layout.backfill_chunk_rows;
        }

        @Override
        public long nowMillis()
        {
            return Clock.Global.currentTimeMillis();
        }

        @Override
        public long writeTimeoutMillis()
        {
            return DatabaseDescriptor.getWriteRpcTimeout(TimeUnit.MILLISECONDS);
        }

        @Override
        public boolean sleep(long millis)
        {
            try
            {
                Thread.sleep(millis);
                return true;
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    /** Totals since the process started, for tests and logs. */
    public static final class Stats
    {
        public final AtomicLong chunksWritten = new AtomicLong();
        public final AtomicLong bytesWritten = new AtomicLong();
        public final AtomicLong writeFailures = new AtomicLong();
        public final AtomicLong throttledMicros = new AtomicLong();
        public final AtomicLong passesResumed = new AtomicLong();
        public final AtomicLong passesAbandoned = new AtomicLong();
        public final AtomicLong reportsCommitted = new AtomicLong();
        /** Times a pass was not started, or was stopped before a chunk or its report, because the deadline had passed. */
        public final AtomicLong refusedPastDeadline = new AtomicLong();
        /** The record the last resumed pass continued from. */
        public volatile String lastResumeRecord;
    }

    public static final Stats stats = new Stats();

    /** Test seam (T6.13): if set, run after a chunk was read and before it is written; a failure test blocks in it. */
    @VisibleForTesting
    public static volatile Runnable beforeChunkWriteForTesting;

    static final long DEFAULT_MIN_BACKOFF_MILLIS = 500;
    static final long DEFAULT_MAX_BACKOFF_MILLIS = 15_000;

    private static final Object LOCK = new Object();
    private static final BackfillTask instance = new BackfillTask(new NodeEnv(), DEFAULT_MIN_BACKOFF_MILLIS, DEFAULT_MAX_BACKOFF_MILLIS);

    private final Env env;
    private final long minBackoffMillis;
    private final long maxBackoffMillis;
    private RateLimiter limiter;
    private long limiterRate;
    /** Layout, key and incarnation of the keys whose deadline was seen to have passed; each is logged once. */
    private final Set<String> pastDeadline = new HashSet<>();

    @VisibleForTesting
    BackfillTask(Env env, long minBackoffMillis, long maxBackoffMillis)
    {
        this.env = env;
        this.minBackoffMillis = minBackoffMillis;
        this.maxBackoffMillis = maxBackoffMillis;
    }

    /**
     * The backfill hook of {@link LayoutService}: called on the backfill thread once per registry change. The registry
     * passed may be old by the time this runs, so the current metadata is what decides.
     */
    static void onRegistryChange(LayoutRegistry ignored)
    {
        // one pass at a time, also if a released thread is still finishing a chunk when its successor starts
        synchronized (LOCK)
        {
            instance.runPending();
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // the loop over keys
    // ---------------------------------------------------------------------------------------------------------

    enum Outcome
    {
        /** The pass finished and the report is committed. */
        REPORTED,
        /** Nothing to do for this key on this node: not a replica, already reported, or the pass went stale. */
        NOTHING,
        /** A write, the report or the record failed; the pass continues from its record after the backoff. */
        RETRY,
        INTERRUPTED
    }

    /** One generation of one attempt of one key, as the metadata showed it when the pass started. */
    static final class Pass
    {
        final TableLayout layout;
        final ByteBuffer key;
        final KeyState state;

        Pass(TableLayout layout, ByteBuffer key, KeyState state)
        {
            this.layout = layout;
            this.key = key;
            this.state = state;
        }

        /** The key of the resume record (design 4.5). */
        String stateKey()
        {
            return statePrefix() + state.generation;
        }

        /** The prefix of the records of every generation of this key. */
        String statePrefix()
        {
            return STATE_PREFIX + layout.layoutId + '/' + MD5Digest.compute(ByteBufferUtil.getArray(key)).toString().substring(0, 16) + '/';
        }

        /**
         * The first field of the record. The key of the record names layout, key and generation; an attempt that was
         * abandoned and started again reuses all three, so the record also names the incarnation and the epoch.
         */
        String stateOwner()
        {
            return state.incarnation + "." + state.generationEpoch.getEpoch();
        }

        Object identity()
        {
            return layout.layoutId + "/" + ByteBufferUtil.bytesToHex(key) + '/' + stateOwner() + '/' + state.generation + '/' + state.phase;
        }

        @Override
        public String toString()
        {
            return "backfill of key 0x" + ByteBufferUtil.bytesToHex(key) + " of " + layout.keyspace + '.' + layout.logicalName
                   + " (" + state.phase + ", incarnation " + state.incarnation + ", generation " + state.generation + ')';
        }
    }

    /**
     * Runs every pass this node owes, key after key, until none is left that could make progress now. A pass that
     * fails does not hold up the others: it is taken up again, from its record, after the backoff.
     */
    @VisibleForTesting
    void runPending()
    {
        Set<Object> reported = new HashSet<>();
        long backoff = minBackoffMillis;
        boolean pruned = false;
        while (!Thread.currentThread().isInterrupted())
        {
            ClusterMetadata cm = env.metadata();
            List<Pass> owed = owed(cm, reported);
            if (!pruned)
            {
                prune(cm);
                pruned = true;
            }
            if (owed.isEmpty())
                return;
            boolean retry = false;
            Set<Object> attempted = new HashSet<>();
            for (Pass pass : owed)
            {
                attempted.add(pass.identity());
                Outcome outcome;
                try
                {
                    outcome = run(pass);
                }
                catch (RuntimeException e)
                {
                    if (Thread.currentThread().isInterrupted())
                        return;
                    logger.warn("The {} failed and will be continued", pass, e);
                    outcome = Outcome.RETRY;
                }
                if (outcome == Outcome.INTERRUPTED)
                    return;
                if (outcome == Outcome.REPORTED)
                    reported.add(pass.identity());
                retry |= outcome == Outcome.RETRY;
            }
            if (!retry)
            {
                // anything that becomes due later comes with a registry change, and so with another call
                List<Pass> left = owed(env.metadata(), reported);
                if (left.isEmpty())
                    return;
                boolean fresh = false;
                for (Pass pass : left)
                    fresh |= !attempted.contains(pass.identity());
                if (fresh)
                {
                    backoff = minBackoffMillis;
                    continue;
                }
                // a pass ended without a report and is still owed, for example its report was rejected: not at once
            }
            if (!env.sleep(backoff))
                return;
            backoff = Math.min(maxBackoffMillis, backoff * 2);
        }
    }

    /** The passes of the metadata that this node has to run: it is a natural replica and has not reported. */
    private List<Pass> owed(ClusterMetadata cm, Set<Object> reported)
    {
        List<Pass> owed = new ArrayList<>();
        NodeId self = env.self(cm);
        if (self == null)
            return owed;
        for (TableLayout layout : LayoutRegistry.of(cm).byLogical.values())
        {
            TableMetadata logical = RoutedKey.logical(cm, layout);
            if (logical == null)
                continue;
            for (Map.Entry<ByteBuffer, KeyState> e : layout.keys.entrySet())
            {
                KeyState state = e.getValue();
                if (!state.backfillDue() || state.backfillDone.contains(self))
                    continue;
                Pass pass = new Pass(layout, e.getKey(), state);
                if (reported.contains(pass.identity()) || pastDeadline(pass, logical, false))
                    continue;
                if (env.isNaturalReplica(cm, logical, logical.partitioner.decorateKey(e.getKey())))
                    owed.add(pass);
            }
        }
        return owed;
    }

    /** Removes the resume records that belong to no generation that is current. Best effort. */
    private void prune(ClusterMetadata cm)
    {
        try
        {
            Set<String> current = new HashSet<>();
            for (TableLayout layout : LayoutRegistry.of(cm).byLogical.values())
                for (Map.Entry<ByteBuffer, KeyState> e : layout.keys.entrySet())
                    if (e.getValue().backfillDue())
                        current.add(new Pass(layout, e.getKey(), e.getValue()).stateKey());
            for (String key : env.scanState(STATE_PREFIX).keySet())
                if (!current.contains(key))
                    env.deleteState(key);
        }
        catch (RuntimeException e)
        {
            logger.debug("Could not prune the backfill records", e);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // one pass
    // ---------------------------------------------------------------------------------------------------------

    /**
     * True if the pass is still the one the metadata asks this node for: same layout and overflow table, the key in
     * the same phase, incarnation and generation (number and starting epoch), and this node a natural replica.
     */
    private boolean current(ClusterMetadata cm, Pass pass)
    {
        TableLayout now = LayoutRegistry.of(cm).layout(pass.layout.logical);
        if (now == null || !now.layoutId.equals(pass.layout.layoutId) || !now.physical.equals(pass.layout.physical))
            return false;
        TableMetadata logical = RoutedKey.logical(cm, now);
        KeyState state = now.keyState(pass.key);
        if (logical == null || state == null)
            return false;
        return state.phase == pass.state.phase && state.incarnation == pass.state.incarnation
               && state.generation == pass.state.generation && state.generationEpoch.equals(pass.state.generationEpoch)
               && env.isNaturalReplica(cm, logical, logical.partitioner.decorateKey(pass.key));
    }

    /**
     * True if this node's clock is past the time after which nothing may be copied for the key (ADR-031). The limit
     * moves with {@code gc_grace_seconds} of the logical table as it is now.
     *
     * @param counted false for the look that decides whether a pass is owed at all, which is repeated
     */
    private boolean pastDeadline(Pass pass, TableMetadata logical, boolean counted)
    {
        long limit = pass.state.backfillLimitMillis(logical.params.gcGraceSeconds);
        if (limit != Long.MIN_VALUE)
            limit -= env.writeTimeoutMillis();
        long now = env.nowMillis();
        if (now < limit)
            return false;
        if (counted)
            stats.refusedPastDeadline.incrementAndGet();
        String key = pass.layout.layoutId + "/" + ByteBufferUtil.bytesToHex(pass.key) + '/' + pass.state.incarnation;
        if (pastDeadline.add(key))
        {
            if (!counted)
                stats.refusedPastDeadline.incrementAndGet();
            logger.warn("Not copying for the {}: {}. Nothing is written or reported for this key any more; it needs an operator",
                        pass, limit == Long.MIN_VALUE
                              ? "the registry does not say when dual write started"
                              : "the clock (" + now + ") is past the tombstone deadline (" + limit + "), half of gc_grace_seconds after dual write started");
        }
        return true;
    }

    private Outcome run(Pass pass)
    {
        String stateKey = pass.stateKey();
        ClusterMetadata cm = env.metadata();
        if (!current(cm, pass))
            return Outcome.NOTHING;
        TableMetadata base = RoutedKey.logical(cm, pass.layout);

        // where to start: the record of this very generation, or the beginning
        ClusteringBound<?> from = null;
        boolean started = false;
        boolean copied = false;
        Optional<String> record = env.getState(stateKey);
        if (record.isPresent())
        {
            Resume resume = Resume.parse(record.get(), pass.stateOwner(), base);
            if (resume != null)
            {
                from = resume.from;
                started = true;
                copied = from == null;
                stats.passesResumed.incrementAndGet();
                stats.lastResumeRecord = record.get();
                logger.info("Resuming the {} {}", pass, copied ? "at its report" : "at " + from.toString(base));
            }
        }
        if (!started)
            logger.info("Starting the {}", pass);

        long chunks = 0;
        long bytes = 0;
        while (!copied)
        {
            if (Thread.currentThread().isInterrupted())
                return Outcome.INTERRUPTED;
            cm = env.metadata();
            if (!current(cm, pass))
                return abandoned(pass);
            base = RoutedKey.logical(cm, pass.layout);
            TableMetadata physical = RoutedKey.physical(cm, pass.layout);
            DecoratedKey key = base.partitioner.decorateKey(pass.key);

            Chunk chunk = readChunk(base, key, from, !started, Math.max(1, env.chunkRows()), maxChunkBytes());
            if (chunk.update != null)
            {
                List<Mutation> mutations = new ArrayList<>();
                for (PartitionUpdate update : RowMapper.toPhysical(chunk.update, physical, pass.layout, pass.state.incarnation).values())
                    mutations.add(new Mutation(update));
                // judged here, with the chunk in hand: the read may have taken long
                if (pastDeadline(pass, base, true))
                    return Outcome.NOTHING;
                Runnable seam = beforeChunkWriteForTesting;
                if (seam != null)
                    seam.run();
                try
                {
                    env.write(mutations);
                }
                catch (RuntimeException e)
                {
                    if (Thread.currentThread().isInterrupted())
                        return Outcome.INTERRUPTED;
                    stats.writeFailures.incrementAndGet();
                    logger.info("A write of the {} was not acknowledged by every replica and will be repeated: {}", pass, e.toString());
                    return Outcome.RETRY;
                }
                chunks++;
                bytes += chunk.bytes;
                stats.chunksWritten.incrementAndGet();
                stats.bytesWritten.addAndGet(chunk.bytes);
            }
            from = chunk.next;
            started = true;
            copied = from == null;
            // a record that cannot be written costs a repeated chunk after a restart, nothing else. The last record
            // says that all is copied: a report that has to be repeated does not copy the partition again
            env.putState(stateKey, new Resume(pass.stateOwner(), from).format());
            if (!copied)
                throttle(chunk.bytes);
        }

        // the report is for the generation the copy was made for and nothing else
        cm = env.metadata();
        if (!current(cm, pass))
            return abandoned(pass);
        // a pass that could not finish in time is not vouched for, even if its last chunk was written in time
        if (pastDeadline(pass, RoutedKey.logical(cm, pass.layout), true))
            return Outcome.NOTHING;
        ReportBackfill report = new ReportBackfill(pass.layout.logical, pass.key, pass.state.generation, pass.state.generationEpoch,
                                                   env.self(cm));
        if (!env.report(report))
            return abandoned(pass);
        stats.reportsCommitted.incrementAndGet();
        logger.info("Finished the {}: {} chunks, {} bytes", pass, chunks, bytes);
        for (String key : env.scanState(pass.statePrefix()).keySet())
            env.deleteState(key);
        return Outcome.REPORTED;
    }

    private static Outcome abandoned(Pass pass)
    {
        stats.passesAbandoned.incrementAndGet();
        logger.info("Abandoning the {}: it is no longer what the layout registry asks of this node", pass);
        return Outcome.NOTHING;
    }

    private void throttle(long bytes)
    {
        long rate = env.throughputBytesPerSecond();
        if (rate <= 0 || bytes <= 0)
            return;
        if (limiter == null || limiterRate != rate)
        {
            limiter = RateLimiter.create(rate);
            limiterRate = rate;
        }
        // the permits of this chunk are paid for by the wait of the next: the first chunk of a pass is not delayed
        double waited = limiter.acquire((int) Math.min(Integer.MAX_VALUE, bytes));
        stats.throttledMicros.addAndGet((long) (waited * 1_000_000));
    }

    /** A chunk is one mutation per bucket; none may come near the size the commit log refuses. */
    private static long maxChunkBytes()
    {
        return Math.max(1, DatabaseDescriptor.getMaxMutationSize() / 4);
    }

    /** True if this node is a natural replica of the key in the placements of the metadata. */
    static boolean isNaturalReplica(ClusterMetadata cm, TableMetadata logical, DecoratedKey key)
    {
        KeyspaceMetadata keyspace = cm.schema.getKeyspaces().getNullable(logical.keyspace);
        if (keyspace == null)
            return false;
        return cm.placement(keyspace.params.replication).reads.forToken(key.getToken()).get()
                            .contains(FBUtilities.getBroadcastAddressAndPort());
    }

    // ---------------------------------------------------------------------------------------------------------
    // reading a chunk
    // ---------------------------------------------------------------------------------------------------------

    /** What one local read produced. */
    static final class Chunk
    {
        /** The update of the base table that holds the chunk, or null if there is nothing to write. */
        final PartitionUpdate update;
        /** Where the next chunk starts, or null if the partition is exhausted. */
        final ClusteringBound<?> next;
        final long bytes;

        Chunk(PartitionUpdate update, ClusteringBound<?> next, long bytes)
        {
            this.update = update;
            this.next = next;
            this.bytes = bytes;
        }
    }

    /**
     * Reads what this node holds of the partition from {@code from} (null: from the beginning) on, unfiltered and
     * unpurged, and stops after about {@code maxUnfiltered} rows and markers or {@code maxBytes} of row data.
     *
     * @param withPartitionDeletion true for the first chunk of a pass: the partition deletion goes with it and is not
     *                              repeated in every chunk (it would be written to every bucket each time)
     */
    static Chunk readChunk(TableMetadata base, DecoratedKey key, ClusteringBound<?> from, boolean withPartitionDeletion, int maxUnfiltered, long maxBytes)
    {
        Slice slice = Slice.make(from == null ? ClusteringBound.BOTTOM : from, ClusteringBound.TOP);
        SinglePartitionReadCommand command =
            SinglePartitionReadCommand.create(base, NO_PURGE, key, ColumnFilter.all(base),
                                              new ClusteringIndexSliceFilter(Slices.with(base.comparator, slice), false));
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            if (!partitions.hasNext())
                return new Chunk(null, null, 0);
            try (UnfilteredRowIterator partition = partitions.next())
            {
                if (!partition.staticRow().isEmpty())
                    throw new IllegalStateException("static columns cannot be split");
                PartitionUpdate.Builder builder = new PartitionUpdate.Builder(base, key, partition.columns(), Math.min(maxUnfiltered, 1024));
                boolean any = false;
                if (withPartitionDeletion && !partition.partitionLevelDeletion().isLive())
                {
                    builder.addPartitionDeletion(partition.partitionLevelDeletion());
                    any = true;
                }

                ClusteringBound<?> openBound = null;
                DeletionTime openDeletion = null;
                Row lastRow = null;
                ClusteringBound<?> next = null;
                boolean canStop = true;
                boolean exhausted = true;
                int count = 0;
                long bytes = 0;
                while (partition.hasNext())
                {
                    if (canStop && (count >= maxUnfiltered || bytes >= maxBytes))
                    {
                        exhausted = false;
                        break;
                    }
                    Unfiltered unfiltered = partition.next();
                    count++;
                    any = true;
                    if (unfiltered.isRow())
                    {
                        lastRow = (Row) unfiltered;
                        builder.add(lastRow);
                        bytes += lastRow.dataSize();
                        next = ClusteringBound.exclusiveStartOf(lastRow.clustering());
                        canStop = true;
                        continue;
                    }
                    RangeTombstoneMarker marker = (RangeTombstoneMarker) unfiltered;
                    if (marker.isClose(false))
                    {
                        if (openBound == null)
                            throw new IllegalStateException("range tombstone closed but not open at " + marker.toString(base));
                        ClusteringBound<?> close = marker.closeBound(false);
                        builder.add(new RangeTombstone(Slice.make(openBound, close), openDeletion));
                        openBound = null;
                        openDeletion = null;
                        next = close.invert();
                        canStop = true;
                    }
                    if (marker.isOpen(false))
                    {
                        openBound = marker.openBound(false);
                        openDeletion = marker.openDeletionTime(false);
                        // a chunk never ends on an opening marker: there would be nothing of the tombstone to carry
                        canStop = false;
                    }
                }
                if (openBound != null)
                {
                    if (exhausted || lastRow == null)
                        throw new IllegalStateException("range tombstone opened at " + openBound.toString(base) + " is never closed");
                    // the tombstone goes on: this chunk has it up to its last row, the next one from there
                    builder.add(new RangeTombstone(Slice.make(openBound, ClusteringBound.inclusiveEndOf(lastRow.clustering())), openDeletion));
                }
                return new Chunk(any ? builder.build() : null, exhausted ? null : next, bytes);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // the resume record
    // ---------------------------------------------------------------------------------------------------------

    /**
     * The value of a resume record: {@code <incarnation>.<generation epoch> <kind> <value hex or -> ...}, where the
     * bound is the start of the slice of the next chunk, or {@code <incarnation>.<generation epoch> DONE} when the
     * whole partition is copied and only the report is missing.
     */
    static final class Resume
    {
        static final String DONE = "DONE";

        final String owner;
        /** Null: everything is copied. */
        final ClusteringBound<?> from;

        Resume(String owner, ClusteringBound<?> from)
        {
            this.owner = owner;
            this.from = from;
        }

        String format()
        {
            if (from == null)
                return owner + ' ' + DONE;
            StringBuilder sb = new StringBuilder(owner).append(' ').append(from.kind().name());
            for (int i = 0; i < from.size(); i++)
            {
                ByteBuffer value = from.bufferAt(i);
                sb.append(' ').append(value == null ? "-" : 'x' + ByteBufferUtil.bytesToHex(value));
            }
            return sb.toString();
        }

        /** Null if the record is of another attempt or generation start, or cannot be read: the pass starts over. */
        static Resume parse(String record, String owner, TableMetadata base)
        {
            try
            {
                String[] fields = record.split(" ");
                if (fields.length < 2 || !fields[0].equals(owner))
                    return null;
                if (fields.length == 2 && fields[1].equals(DONE))
                    return new Resume(owner, null);
                ClusteringPrefix.Kind kind = ClusteringPrefix.Kind.valueOf(fields[1]);
                if (!kind.isStart() || kind.isBoundary() || fields.length - 2 > base.comparator.size())
                    return null;
                ByteBuffer[] values = new ByteBuffer[fields.length - 2];
                for (int i = 0; i < values.length; i++)
                    values[i] = fields[i + 2].equals("-") ? null : ByteBuffer.wrap(Hex.hexToBytes(fields[i + 2].substring(1)));
                return new Resume(owner, BufferClusteringBound.create(kind, values));
            }
            catch (RuntimeException e)
            {
                logger.warn("Unreadable backfill record '{}'; the pass starts from the beginning", record);
                return null;
            }
        }
    }
}
