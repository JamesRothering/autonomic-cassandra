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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

/**
 * The replica's work of a slow-mode reverse migration in {@link Phase#REVERSE_ANNOUNCED} (design 11.4, 11.5, ADR-036,
 * T6.17d): get the base partition of the key empty, so that the reverse copy lands in a clean target (invariant I17).
 *
 * <p>For each key in {@code REVERSE_ANNOUNCED} in slow mode that this node replicates and has not reported:
 * <ol>
 * <li>Wait until the local clock passes the local deletion time of this node's partition deletion of the key plus
 *     {@code gc_grace_seconds}; a node that holds no partition deletion has nothing to wait for.</li>
 * <li>Flush the table, then (unless {@code force_purge_compaction} is false, owner answer 7) run
 *     {@code CompactionManager.forceCompactionForKey}, which rewrites every sstable that may hold the key.</li>
 * <li>Run the <b>residue probe</b>: a local read of the key at {@code nowInSec = 0} (nothing is purged or judged
 *     expired). Clean means no row, no range tombstone marker and no partition deletion.</li>
 * <li>If clean, commit {@link ReportBackfill} for the current generation. Else retry with backoff, and record why
 *     ({@link Reason}).</li>
 * </ol>
 *
 * <p><b>What the forced compaction can and cannot purge</b> (design 11.5, confirmed against the code). The tasks of
 * {@code forceCompactionForKey} are made per strategy holder, so sstables that are repaired, unrepaired and pending
 * repair are compacted in separate tasks, never together. A tombstone is dropped only if its timestamp is below the
 * smallest timestamp of every other sstable that may hold the key and every memtable that does
 * ({@code CompactionController.getPurgeEvaluator}), and not at all by a task of unrepaired sstables if
 * {@code only_purge_repaired_tombstones} is set. So a partition deletion in one repair state and the data it shadows in
 * another stay as they are until repair puts them in one state, and with that option an unrepaired deletion stays
 * until it is repaired. The probe then fails ({@link Reason#STILL_SHADOWED}, {@link Reason#ONLY_PURGE_REPAIRED_TOMBSTONES}),
 * and the node retries.
 *
 * <p><b>Residue above Tt</b> (a cell newer than the base partition deletion, which the deletion never shadowed) cannot
 * be purged by anything but a delete at a higher timestamp. It is never fixed here: the node keeps reporting it, at the
 * slowest backoff, and the operator decides (owner answer 3).
 */
public final class BasePurge
{
    private static final Logger logger = LoggerFactory.getLogger(BasePurge.class);

    public static final long DEFAULT_MIN_BACKOFF_MILLIS = 1000;
    public static final long DEFAULT_MAX_BACKOFF_MILLIS = TimeUnit.MINUTES.toMillis(5);

    /** Why the probe of a key is not clean, or that the node waits. */
    public enum Reason
    {
        /** The local clock has not passed the deletion time of the partition deletion plus gc_grace_seconds. */
        WAITING_GC_GRACE,
        /** The partition deletion, or data at or below its timestamp, is still there: not purged. */
        STILL_SHADOWED,
        /** A cell or marker newer than the base tombstone survived: never purgeable, held for the operator. */
        RESIDUE_ABOVE_TT,
        /** The table purges only repaired tombstones and the key's sstables are not all repaired. */
        ONLY_PURGE_REPAIRED_TOMBSTONES
    }

    /** What the residue probe found. */
    public static final class Probe
    {
        public final boolean partitionDeletion;
        public final long partitionDeletionTimestamp;
        /** Local deletion time of the partition deletion, in seconds. */
        public final long partitionDeletionLocalTime;
        public final int rows;
        public final int markers;
        /** The largest timestamp of anything found, {@link Long#MIN_VALUE} if nothing. */
        public final long maxTimestamp;

        public Probe(boolean partitionDeletion, long partitionDeletionTimestamp, long partitionDeletionLocalTime, int rows, int markers, long maxTimestamp)
        {
            this.partitionDeletion = partitionDeletion;
            this.partitionDeletionTimestamp = partitionDeletionTimestamp;
            this.partitionDeletionLocalTime = partitionDeletionLocalTime;
            this.rows = rows;
            this.markers = markers;
            this.maxTimestamp = maxTimestamp;
        }

        public boolean clean()
        {
            return !partitionDeletion && rows == 0 && markers == 0;
        }

        @Override
        public String toString()
        {
            return clean() ? "clean"
                           : (partitionDeletion ? "partition deletion at " + partitionDeletionTimestamp + " (local deletion time " + partitionDeletionLocalTime + "), " : "")
                             + rows + " rows, " + markers + " range tombstone markers, largest timestamp " + maxTimestamp;
        }
    }

    /** How many sstables that may hold the key are in each repair state. */
    public static final class RepairStates
    {
        public final int repaired;
        public final int unrepaired;
        public final int pendingRepair;

        public RepairStates(int repaired, int unrepaired, int pendingRepair)
        {
            this.repaired = repaired;
            this.unrepaired = unrepaired;
            this.pendingRepair = pendingRepair;
        }

        public boolean mixed()
        {
            return (repaired > 0 ? 1 : 0) + (unrepaired > 0 ? 1 : 0) + (pendingRepair > 0 ? 1 : 0) > 1;
        }

        @Override
        public String toString()
        {
            return repaired + " repaired, " + unrepaired + " unrepaired, " + pendingRepair + " pending repair sstables";
        }
    }

    /** What the work needs from the node; replaced in unit tests. */
    @VisibleForTesting
    interface Env
    {
        ClusterMetadata metadata();

        NodeId self(ClusterMetadata cm);

        boolean isNaturalReplica(ClusterMetadata cm, TableMetadata logical, DecoratedKey key);

        long nowMillis();

        /** The local residue probe of the key in the base table. */
        Probe probe(TableMetadata base, DecoratedKey key);

        void flush(TableMetadata base);

        void compact(TableMetadata base, DecoratedKey key);

        RepairStates repairStates(TableMetadata base, DecoratedKey key);

        boolean onlyPurgeRepairedTombstones(TableMetadata base);

        /** False to wait for natural compaction and only probe (owner answer 7). */
        boolean forceCompaction();

        /** True if the report was committed (or was already), false if it was rejected. */
        boolean report(ReportBackfill report);

        void schedule(Runnable task, long delayMillis);

        /** Releases the thread; a later {@link #schedule} makes a new one. */
        void shutdown();
    }

    private static final class NodeEnv implements Env
    {
        private ScheduledExecutorPlus executor;

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
        public long nowMillis()
        {
            return Clock.Global.currentTimeMillis();
        }

        @Override
        public Probe probe(TableMetadata base, DecoratedKey key)
        {
            return BasePurge.probe(base, key);
        }

        @Override
        public void flush(TableMetadata base)
        {
            cfs(base).forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        }

        @Override
        public void compact(TableMetadata base, DecoratedKey key)
        {
            CompactionManager.instance.forceCompactionForKey(cfs(base), key);
        }

        @Override
        public RepairStates repairStates(TableMetadata base, DecoratedKey key)
        {
            int repaired = 0;
            int unrepaired = 0;
            int pending = 0;
            for (SSTableReader sstable : cfs(base).getTracker().getView().liveSSTablesInBounds(key.getToken().minKeyBound(), key.getToken().maxKeyBound()))
            {
                if (!sstable.mayContainAssumingKeyIsInRange(key))
                    continue;
                if (sstable.isPendingRepair())
                    pending++;
                else if (sstable.isRepaired())
                    repaired++;
                else
                    unrepaired++;
            }
            return new RepairStates(repaired, unrepaired, pending);
        }

        @Override
        public boolean onlyPurgeRepairedTombstones(TableMetadata base)
        {
            return cfs(base).getCompactionStrategyManager().onlyPurgeRepairedTombstones();
        }

        @Override
        public boolean forceCompaction()
        {
            return DatabaseDescriptor.getAutonomicConfig().layout.force_purge_compaction;
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
        public synchronized void schedule(Runnable task, long delayMillis)
        {
            // compaction can take long: not on the shared optional-tasks thread
            if (executor == null)
                executor = executorFactory().scheduled(false, "LayoutPurge");
            executor.schedule(task, delayMillis, TimeUnit.MILLISECONDS);
        }

        @Override
        public synchronized void shutdown()
        {
            if (executor != null)
                executor.shutdownNow();
            executor = null;
        }

        private static ColumnFamilyStore cfs(TableMetadata table)
        {
            return Keyspace.open(table.keyspace).getColumnFamilyStore(table.id);
        }
    }

    public static final BasePurge instance = new BasePurge(new NodeEnv(), DEFAULT_MIN_BACKOFF_MILLIS, DEFAULT_MAX_BACKOFF_MILLIS);

    /** One key this node owes a purge report for. */
    private static final class Pending
    {
        final TableId logical;
        final ByteBuffer key;
        int attempts;
        long dueMillis;
        boolean compacted;
        Reason reason;
        String detail = "";

        Pending(TableId logical, ByteBuffer key)
        {
            this.logical = logical;
            this.key = key;
        }
    }

    private final Env env;
    private final long minBackoffMillis;
    private final long maxBackoffMillis;
    /** By {@link #id}; guarded by {@code this}. */
    private final Map<String, Pending> pending = new HashMap<>();
    private final AtomicBoolean scheduled = new AtomicBoolean();

    @VisibleForTesting
    BasePurge(Env env, long minBackoffMillis, long maxBackoffMillis)
    {
        this.env = env;
        this.minBackoffMillis = minBackoffMillis;
        this.maxBackoffMillis = maxBackoffMillis;
    }

    /** Called by {@link LayoutService} on every registry change: does no work itself, only schedules a poll. */
    public void onRegistryChange(ClusterMetadata metadata)
    {
        if (anySlowAnnounced(LayoutRegistry.of(metadata)) || !isEmpty())
            schedule(0);
    }

    static boolean anySlowAnnounced(LayoutRegistry registry)
    {
        for (TableLayout layout : registry.byLogical.values())
            for (KeyState key : layout.keys.values())
                if (owes(key))
                    return true;
        return false;
    }

    /** True if the key is in the phase and mode in which replicas purge the base partition. */
    static boolean owes(KeyState key)
    {
        return key.phase == Phase.REVERSE_ANNOUNCED && key.reverseMode == KeyState.ReverseMode.SLOW;
    }

    /** Called by {@link LayoutService#shutdownBlocking}: drops what is scheduled and releases the thread. */
    public void shutdownBlocking()
    {
        env.shutdown();
        scheduled.set(false);
        synchronized (this)
        {
            pending.clear();
        }
    }

    private synchronized boolean isEmpty()
    {
        return pending.isEmpty();
    }

    private void schedule(long delayMillis)
    {
        if (scheduled.compareAndSet(false, true))
            env.schedule(this::pollQuietly, delayMillis);
    }

    private void pollQuietly()
    {
        try
        {
            poll();
        }
        catch (Throwable t)
        {
            logger.error("Layout base purge failed", t);
            scheduled.set(false);
            schedule(minBackoffMillis);
        }
    }

    private static String id(TableId logical, ByteBuffer key, KeyState state)
    {
        return logical + "/" + ByteBufferUtil.bytesToHex(key) + '/' + state.incarnation + '/' + state.generation + '/' + state.generationEpoch.getEpoch();
    }

    /**
     * One round over the keys this node owes a purge report for; schedules the next round while there is one. Returns
     * the number of reports committed.
     */
    @VisibleForTesting
    synchronized int poll()
    {
        scheduled.set(false);
        ClusterMetadata cm = env.metadata();
        NodeId self = env.self(cm);
        Map<String, Pending> still = new HashMap<>();
        int reported = 0;
        long now = env.nowMillis();
        if (self != null)
        {
            for (TableLayout layout : LayoutRegistry.of(cm).byLogical.values())
            {
                TableMetadata logical = RoutedKey.logical(cm, layout);
                TableMetadata base = logical;
                if (logical == null)
                    continue;
                for (Map.Entry<ByteBuffer, KeyState> e : layout.keys.entrySet())
                {
                    KeyState state = e.getValue();
                    DecoratedKey key = logical.partitioner.decorateKey(e.getKey());
                    if (!owes(state) || state.backfillDone.contains(self) || !env.isNaturalReplica(cm, logical, key))
                        continue;
                    String id = id(layout.logical, e.getKey(), state);
                    Pending p = pending.get(id);
                    if (p == null)
                        p = new Pending(layout.logical, e.getKey());
                    still.put(id, p);
                    if (p.dueMillis > now)
                        continue;
                    if (step(cm, self, layout, state, base, key, p, now))
                    {
                        still.remove(id);
                        reported++;
                    }
                    now = env.nowMillis();
                }
            }
        }
        pending.clear();
        pending.putAll(still);
        if (!pending.isEmpty())
        {
            long next = Long.MAX_VALUE;
            for (Pending p : pending.values())
                next = Math.min(next, p.dueMillis);
            schedule(Math.max(0, next - env.nowMillis()));
        }
        return reported;
    }

    /** One attempt for one key. True if the report was committed. */
    private boolean step(ClusterMetadata cm, NodeId self, TableLayout layout, KeyState state, TableMetadata base, DecoratedKey key, Pending p, long now)
    {
        Probe before = env.probe(base, key);
        if (!before.clean())
        {
            if (before.partitionDeletion)
            {
                long due = TimeUnit.SECONDS.toMillis(before.partitionDeletionLocalTime + base.params.gcGraceSeconds);
                if (now < due)
                {
                    wait(p, Reason.WAITING_GC_GRACE, "the partition deletion at " + before.partitionDeletionTimestamp + " may be purged from "
                                                      + due + " (local deletion time " + before.partitionDeletionLocalTime + " plus gc_grace_seconds "
                                                      + base.params.gcGraceSeconds + ')', due);
                    return false;
                }
            }
            if (env.forceCompaction() && (before.partitionDeletion || !p.compacted))
            {
                env.flush(base);
                env.compact(base, key);
                p.compacted = true;
            }
        }
        Probe after = env.probe(base, key);
        if (after.clean())
        {
            // the registry may have moved while the purge ran
            ClusterMetadata current = env.metadata();
            TableLayout now2 = LayoutRegistry.of(current).layout(layout.logical);
            KeyState key2 = now2 == null ? null : now2.keyState(p.key);
            if (key2 == null || !owes(key2) || key2.generation != state.generation || !key2.generationEpoch.equals(state.generationEpoch))
                return false;
            Epoch epoch = state.generationEpoch;
            if (!env.report(new ReportBackfill(layout.logical, p.key, state.generation, epoch, self)))
                return false;
            logger.info("The base partition 0x{} of {}.{} is clean on this node (generation {}); reported", ByteBufferUtil.bytesToHex(p.key), base.keyspace,
                        base.name, state.generation);
            return true;
        }

        RepairStates states = env.repairStates(base, key);
        Reason reason;
        String detail;
        if (after.partitionDeletion)
        {
            if (env.onlyPurgeRepairedTombstones(base) && (states.unrepaired > 0 || states.pendingRepair > 0))
            {
                reason = Reason.ONLY_PURGE_REPAIRED_TOMBSTONES;
                detail = "only_purge_repaired_tombstones is set and the key is in sstables that are not repaired (" + states + "); " + after;
            }
            else
            {
                reason = Reason.STILL_SHADOWED;
                detail = "the partition deletion was not purged" + (env.forceCompaction() ? "" : " (waiting for natural compaction)")
                         + (states.mixed() ? "; the key is in sstables of different repair states, which are compacted apart (" + states + ')' : " (" + states + ')')
                         + "; " + after;
            }
        }
        else
        {
            long tt = state.baseTombstoneMicros;
            if (tt > 0 && after.maxTimestamp <= tt)
            {
                reason = Reason.STILL_SHADOWED;
                detail = "data at or below the base tombstone (" + tt + ") remains: " + after;
            }
            else
            {
                reason = Reason.RESIDUE_ABOVE_TT;
                detail = "residue above the base tombstone" + (tt > 0 ? " (" + tt + ')' : "") + ": largest timestamp " + after.maxTimestamp + ", " + after.rows
                         + " rows, " + after.markers + " range tombstone markers; it is never purged and needs the operator";
            }
        }
        long backoff = Math.min(maxBackoffMillis, minBackoffMillis << Math.min(20, p.attempts));
        p.attempts++;
        wait(p, reason, detail, now + backoff);
        logger.warn("The base partition 0x{} of {}.{} is not clean on this node: {}: {}", ByteBufferUtil.bytesToHex(p.key), base.keyspace, base.name, reason, detail);
        return false;
    }

    private static void wait(Pending p, Reason reason, String detail, long dueMillis)
    {
        p.reason = reason;
        p.detail = detail;
        p.dueMillis = dueMillis;
    }

    /** The reasons keys are held for, as "keyHex: REASON: detail"; for operators and tests. */
    public synchronized List<String> reasons()
    {
        List<String> out = new ArrayList<>();
        for (Pending p : pending.values())
            out.add(ByteBufferUtil.bytesToHex(p.key) + ": " + p.reason + ": " + p.detail);
        return out;
    }

    // ---------------------------------------------------------------------------------------------------------
    // the probe
    // ---------------------------------------------------------------------------------------------------------

    /**
     * The residue probe: what this node holds of the partition, read locally at {@code nowInSec = 0}, which neither
     * purges nor judges anything expired (as {@link BackfillTask} reads).
     */
    public static Probe probe(TableMetadata base, DecoratedKey key)
    {
        SinglePartitionReadCommand command =
            SinglePartitionReadCommand.create(base, BackfillTask.NO_PURGE, key, ColumnFilter.all(base), new ClusteringIndexSliceFilter(Slices.ALL, false));
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            if (!partitions.hasNext())
                return new Probe(false, 0, 0, 0, 0, Long.MIN_VALUE);
            try (UnfilteredRowIterator partition = partitions.next())
            {
                long max = Long.MIN_VALUE;
                boolean pd = !partition.partitionLevelDeletion().isLive();
                long pdTs = pd ? partition.partitionLevelDeletion().markedForDeleteAt() : 0;
                long pdLdt = pd ? partition.partitionLevelDeletion().localDeletionTime() : 0;
                if (pd)
                    max = pdTs;
                int rows = 0;
                int markers = 0;
                if (!partition.staticRow().isEmpty())
                {
                    rows++;
                    max = Math.max(max, maxTimestamp(partition.staticRow()));
                }
                while (partition.hasNext())
                {
                    Unfiltered unfiltered = partition.next();
                    if (unfiltered.isRow())
                    {
                        rows++;
                        max = Math.max(max, maxTimestamp((Row) unfiltered));
                    }
                    else
                    {
                        markers++;
                        RangeTombstoneMarker marker = (RangeTombstoneMarker) unfiltered;
                        if (marker.isOpen(false))
                            max = Math.max(max, marker.openDeletionTime(false).markedForDeleteAt());
                        if (marker.isClose(false))
                            max = Math.max(max, marker.closeDeletionTime(false).markedForDeleteAt());
                    }
                }
                return new Probe(pd, pdTs, pdLdt, rows, markers, max);
            }
        }
    }

    private static long maxTimestamp(Row row)
    {
        long max = Long.MIN_VALUE;
        if (!row.primaryKeyLivenessInfo().isEmpty())
            max = row.primaryKeyLivenessInfo().timestamp();
        if (!row.deletion().isLive())
            max = Math.max(max, row.deletion().time().markedForDeleteAt());
        for (ColumnData data : row.columnData())
        {
            if (data instanceof ComplexColumnData)
            {
                ComplexColumnData complex = (ComplexColumnData) data;
                if (!complex.complexDeletion().isLive())
                    max = Math.max(max, complex.complexDeletion().markedForDeleteAt());
                for (Cell<?> cell : complex)
                    max = Math.max(max, cell.timestamp());
            }
            else
            {
                max = Math.max(max, ((Cell<?>) data).timestamp());
            }
        }
        return max;
    }
}
