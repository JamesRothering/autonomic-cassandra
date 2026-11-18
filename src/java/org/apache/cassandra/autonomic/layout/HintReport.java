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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;

/**
 * The node's side of the hint wait of ADR-035: while a key is in {@link Phase#ANNOUNCED}, every node commits
 * {@link ReportHints} once it holds no hint, created before the split, for any natural replica of the key. The owner
 * moves the key to {@code DUAL_WRITE} only when every directory member that is not LEFT has reported. Part of the
 * unconditional data plane of {@link LayoutService}: it runs on every node, whatever the state of the autonomic layer,
 * and needs no messaging verb; the report is a metadata transformation, as {@link ReportBackfill} is.
 *
 * <p><b>The cut.</b> A hint that matters was created by a write that took the stock path, so it started before this
 * node saw the key registered. Such a write ends, or times out and leaves its hint, within the drain of ADR-028
 * ({@link #drainMillis}). So the node waits that long after it first saw the ANNOUNCED epoch, then flushes the hint
 * buffers of the key's replicas into their hint files ({@code HintsService.flushAndFsyncBlockingly}). From then on
 * every hint that matters is in a file that exists. A file's name carries its host and the time it was opened, which
 * grows with every new file of a host, so the cut of a host is the later of the flush time and the newest of its
 * files at that moment: every file at or before the cut may hold a hint that matters, every later file holds none.
 *
 * <p><b>The check</b> is by host and by age, never by content: the node lists the names of the files in its hints
 * directory and waits until none of a replica's files is at or before that replica's cut. A file is deleted only once
 * its hints were all delivered (or expired, or truncated by an operator). Hints written later, for this key or any
 * other, are in later files and do not hold the report up; a file still open at the cut does, until the dispatch
 * trigger closes it and it is delivered. Hint delivery paused, a replica refusing hints, or a corrupt file keeps the
 * report from being made; the owner's {@code hint_drain_timeout} then aborts the migration.
 *
 * <p>The cut is kept in memory. A node that restarts takes a new one, later than the first, which can only make it
 * wait for more.
 */
public final class HintReport
{
    private static final Logger logger = LoggerFactory.getLogger(HintReport.class);

    /** Added to the ADR-028 drain. */
    public static final long DRAIN_MARGIN_MILLIS = 1000;
    /** How often a node that waits for its hints says so in its log. */
    static final long LOG_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(1);

    /** {@code <host id>-<timestamp>-<version>.hints}, the name {@code HintsDescriptor} gives a hints file. */
    private static final Pattern HINT_FILE = Pattern.compile("^([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12})-(\\d+)-(\\d+)\\.hints$");

    public static final HintReport instance = new HintReport(new NodeEnv());

    /** ADR-028: the wait after which every operation that started on the stock path has ended or timed out. */
    public static long drainMillis(long casDrainMillis, long requestTimeoutsMillis)
    {
        return Math.max(casDrainMillis, requestTimeoutsMillis) + DRAIN_MARGIN_MILLIS;
    }

    /** ADR-028: the largest of the write, counter write, CAS contention, range and truncate request timeouts. */
    public static long requestTimeoutsMillis()
    {
        long max = DatabaseDescriptor.getWriteRpcTimeout(TimeUnit.MILLISECONDS);
        max = Math.max(max, DatabaseDescriptor.getCounterWriteRpcTimeout(TimeUnit.MILLISECONDS));
        max = Math.max(max, DatabaseDescriptor.getCasContentionTimeout(TimeUnit.MILLISECONDS));
        max = Math.max(max, DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS));
        return Math.max(max, DatabaseDescriptor.getTruncateRpcTimeout(TimeUnit.MILLISECONDS));
    }

    // ---------------------------------------------------------------------------------------------------------
    // the age and host filter: pure
    // ---------------------------------------------------------------------------------------------------------

    /** A hints file as its name describes it: the host its hints are for and the time it was opened. */
    static final class HintFile
    {
        final UUID host;
        final long timestamp;

        HintFile(UUID host, long timestamp)
        {
            this.host = host;
            this.timestamp = timestamp;
        }

        /** Null for a name that is not a hints file (a checksum file, a temporary file). */
        static HintFile parse(String name)
        {
            Matcher m = HINT_FILE.matcher(name);
            if (!m.matches())
                return null;
            try
            {
                return new HintFile(UUID.fromString(m.group(1)), Long.parseLong(m.group(2)));
            }
            catch (IllegalArgumentException e)
            {
                return null;
            }
        }

        @Override
        public String toString()
        {
            return host + "@" + timestamp;
        }
    }

    static List<HintFile> parse(Collection<String> names)
    {
        List<HintFile> files = new ArrayList<>();
        for (String name : names)
        {
            HintFile f = HintFile.parse(name);
            if (f != null)
                files.add(f);
        }
        return files;
    }

    /**
     * The cut of each host (see the class comment): the later of {@code flushedAtMillis} and the newest file of the
     * host among {@code files}, listed after the flush.
     */
    static Map<UUID, Long> cuts(Collection<HintFile> files, Collection<UUID> hosts, long flushedAtMillis)
    {
        Map<UUID, Long> cuts = new TreeMap<>();
        for (UUID host : hosts)
            cuts.put(host, flushedAtMillis);
        for (HintFile f : files)
            cuts.computeIfPresent(f.host, (h, cut) -> Math.max(cut, f.timestamp));
        return cuts;
    }

    /** The hosts that still have a file at or before their cut, each with the time of its oldest such file. */
    static Map<UUID, Long> blocking(Collection<HintFile> files, Map<UUID, Long> cuts)
    {
        Map<UUID, Long> out = new TreeMap<>();
        for (HintFile f : files)
        {
            Long cut = cuts.get(f.host);
            if (cut != null && f.timestamp <= cut)
                out.merge(f.host, f.timestamp, Math::min);
        }
        return out;
    }

    /** The host ids hints for the key's natural replicas are written under (both forms a directory can give). */
    static Set<UUID> hosts(ClusterMetadata cm, TableMetadata logical, ByteBuffer key)
    {
        Set<UUID> hosts = new TreeSet<>();
        for (NodeId id : NaturalReplicas.of(cm, logical, key))
        {
            hosts.add(id.toUUID());
            hosts.add(cm.directory.hostId(id));
        }
        return hosts;
    }

    // ---------------------------------------------------------------------------------------------------------
    // the node
    // ---------------------------------------------------------------------------------------------------------

    /** What the report needs from the node; replaced in unit tests. */
    @VisibleForTesting
    interface Env
    {
        ClusterMetadata metadata();

        /** This node's id in the metadata, or null if it has none yet. */
        NodeId self(ClusterMetadata cm);

        /** The clock hints files are named with. */
        long nowMillis();

        /** {@link #drainMillis} with this node's configuration. */
        long drainMillis();

        long pollMillis();

        /** Writes the buffered hints for the hosts into their files; returns when they are there. */
        void flush(Collection<UUID> hosts);

        /** The names of the files in the hints directory. */
        Collection<String> hintFileNames();

        /** True if the report was committed (or was already), false if it was rejected. */
        boolean report(ReportHints report);

        void schedule(Runnable task, long delayMillis);
    }

    private static final class NodeEnv implements Env
    {
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
        public long nowMillis()
        {
            return Clock.Global.currentTimeMillis();
        }

        @Override
        public long drainMillis()
        {
            return HintReport.drainMillis(DatabaseDescriptor.getAutonomicConfig().layout.cas_drain.toMilliseconds(), requestTimeoutsMillis());
        }

        @Override
        public long pollMillis()
        {
            return Math.max(1, DatabaseDescriptor.getAutonomicConfig().layout.ack_poll_interval.toMilliseconds());
        }

        @Override
        public void flush(Collection<UUID> hosts)
        {
            HintsService.instance.flushAndFsyncBlockingly(hosts);
        }

        @Override
        public Collection<String> hintFileNames()
        {
            File directory = DatabaseDescriptor.getHintsDirectory();
            if (directory == null || !directory.exists())
                return Collections.emptyList();
            String[] names = directory.listNamesUnchecked();
            return names == null ? Collections.emptyList() : List.of(names);
        }

        @Override
        public boolean report(ReportHints report)
        {
            return ClusterMetadataService.instance().commit(report.wrap(), metadata -> true, (code, message) -> {
                logger.info("{} was rejected: {}", report, message);
                return false;
            });
        }

        @Override
        public void schedule(Runnable task, long delayMillis)
        {
            ScheduledExecutors.optionalTasks.schedule(task, delayMillis, TimeUnit.MILLISECONDS);
        }
    }

    /** One key in ANNOUNCED that this node has not reported yet. */
    private static final class Pending
    {
        final long seenAtMillis;
        /** Null until the cut is taken. */
        Map<UUID, Long> cuts;
        long loggedAtMillis = Long.MIN_VALUE;

        Pending(long seenAtMillis)
        {
            this.seenAtMillis = seenAtMillis;
        }
    }

    private final Env env;
    /** By {@link #id}; guarded by {@code this}. */
    private final Map<String, Pending> pending = new HashMap<>();
    private final AtomicBoolean scheduled = new AtomicBoolean();

    @VisibleForTesting
    HintReport(Env env)
    {
        this.env = env;
    }

    /** Called by {@link LayoutService} on every registry change: does no work itself, only schedules a poll. */
    public void onRegistryChange(ClusterMetadata metadata)
    {
        if (anyAnnounced(LayoutRegistry.of(metadata)))
            schedule(0);
    }

    static boolean anyAnnounced(LayoutRegistry registry)
    {
        for (TableLayout layout : registry.byLogical.values())
            for (KeyState key : layout.keys.values())
                if (key.phase == Phase.ANNOUNCED)
                    return true;
        return false;
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
            logger.error("Layout hint report failed", t);
            scheduled.set(false);
            schedule(env.pollMillis());
        }
    }

    private static String id(TableId logical, ByteBuffer key, Epoch phaseEpoch)
    {
        return logical + "/" + ByteBufferUtil.bytesToHex(key) + '/' + phaseEpoch.getEpoch();
    }

    /**
     * One round over the keys in ANNOUNCED that this node has not reported; schedules the next round while there is
     * one. Returns the number of reports committed.
     */
    @VisibleForTesting
    synchronized int poll()
    {
        scheduled.set(false);
        ClusterMetadata cm = env.metadata();
        NodeId self = env.self(cm);
        Map<String, Pending> still = new HashMap<>();
        int reported = 0;
        List<HintFile> files = null;
        if (self != null)
        {
            for (TableLayout layout : LayoutRegistry.of(cm).byLogical.values())
            {
                for (Map.Entry<ByteBuffer, KeyState> e : layout.keys.entrySet())
                {
                    KeyState key = e.getValue();
                    if (key.phase != Phase.ANNOUNCED || key.hintsClear.contains(self))
                        continue;
                    String id = id(layout.logical, e.getKey(), key.phaseEpoch);
                    long now = env.nowMillis();
                    Pending p = pending.get(id);
                    if (p == null)
                        p = new Pending(now);
                    still.put(id, p);
                    if (now < p.seenAtMillis + env.drainMillis())
                        continue;
                    TableMetadata logical = cm.schema.getTableMetadata(layout.logical);
                    if (logical == null)
                        continue;
                    if (p.cuts == null)
                    {
                        Set<UUID> hosts = hosts(cm, logical, e.getKey());
                        env.flush(hosts);
                        long flushedAt = env.nowMillis();
                        files = parse(env.hintFileNames());
                        p.cuts = cuts(files, hosts, flushedAt);
                    }
                    if (files == null)
                        files = parse(env.hintFileNames());
                    Map<UUID, Long> blocking = blocking(files, p.cuts);
                    String name = layout.keyspace + '.' + layout.logicalName + " key 0x" + ByteBufferUtil.bytesToHex(e.getKey());
                    if (!blocking.isEmpty())
                    {
                        if (now >= p.loggedAtMillis + LOG_INTERVAL_MILLIS)
                        {
                            p.loggedAtMillis = now;
                            logger.info("Autonomic partition split of {} waits in ANNOUNCED until this node has delivered the hints created before the split "
                                        + "for its replicas (ADR-035): oldest file by host {}", name, blocking);
                        }
                        continue;
                    }
                    if (env.report(new ReportHints(layout.logical, e.getKey(), key.phaseEpoch, self)))
                    {
                        reported++;
                        logger.debug("Autonomic partition split of {}: this node holds no hint created before the split for its replicas", name);
                    }
                    // committed or rejected: the next round reads the result from the metadata
                }
            }
        }
        pending.keySet().retainAll(still.keySet());
        pending.putAll(still);
        if (!pending.isEmpty())
            schedule(env.pollMillis());
        return reported;
    }

    @VisibleForTesting
    synchronized int pendingCount()
    {
        return pending.size();
    }
}
