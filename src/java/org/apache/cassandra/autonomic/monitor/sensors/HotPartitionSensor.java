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
package org.apache.cassandra.autonomic.monitor.sensors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.monitor.ObservationSink;
import org.apache.cassandra.autonomic.monitor.Sensor;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.metrics.Sampler;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.Clock;

import static org.apache.cassandra.autonomic.monitor.sensors.SensorSupport.emit;

/**
 * Hot partition detection from the existing windowed top-K samplers (overview 5.4). The monitor thread never waits:
 * a window is opened in one {@link #collect} and closed in a later one once {@code sampling.window} has passed.
 *
 * <ul>
 * <li>Windows open at most every {@code sampling.interval}, only for user tables whose read or write rate (from this
 * sensor's own counter deltas between collections) is at least {@code sampling.min_ops_per_sec}, and for at most
 * {@link #MAX_TABLES_PER_WINDOW} tables (busiest first). Only the READS and/or WRITES sampler that is busy is opened.</li>
 * <li>A sampler that is already active (an operator's {@code nodetool profileload}) is skipped, as is one whose
 * {@code beginLocalSampling} throws; neither is a failure of the sensor. When a window closes and the sampler is
 * active although our window has expired by the real clock, someone else re-armed it: it is left alone.</li>
 * <li>Results are read from the sampler directly, not through {@code finishLocalSampling}, because that renders the
 * key as a string; the raw key bytes are what FC6 needs. The subject detail is the key as hex, see {@link PartitionKeys}.</li>
 * </ul>
 *
 * Share is the sampled count divided by the table's operation count over the window (the latency counter delta,
 * scaled down if the window was closed late). Only partitions with at least {@link #MIN_EMIT_SHARE} are emitted.
 * Sampler counts are upper bounds (StreamSummary), so a share can slightly overstate and is clamped to 1.
 */
public class HotPartitionSensor implements Sensor
{
    /** Partitions reported per table and sampler. */
    public static final int TOP_K = 5;
    /** Tables sampled in one window. */
    public static final int MAX_TABLES_PER_WINDOW = 4;
    /** Partitions below this share of the table's operations are not emitted. */
    public static final double MIN_EMIT_SHARE = 0.01;

    private static final Set<SignalId> SIGNALS = Set.of(
        Signals.PARTITION_READ_SHARE, Signals.PARTITION_WRITE_SHARE,
        Signals.PARTITION_READ_OPS_PER_SEC, Signals.PARTITION_WRITE_OPS_PER_SEC);

    private static final class Window
    {
        final ColumnFamilyStore cfs;
        final boolean reads;
        final boolean writes;
        final long readsBase;
        final long writesBase;

        Window(ColumnFamilyStore cfs, boolean reads, boolean writes)
        {
            this.cfs = cfs;
            this.reads = reads;
            this.writes = writes;
            this.readsBase = cfs.metric.readLatency.latency.getCount();
            this.writesBase = cfs.metric.writeLatency.latency.getCount();
        }
    }

    private static final class Candidate
    {
        final ColumnFamilyStore cfs;
        final double readRate;
        final double writeRate;

        Candidate(ColumnFamilyStore cfs, double readRate, double writeRate)
        {
            this.cfs = cfs;
            this.readRate = readRate;
            this.writeRate = writeRate;
        }

        double max()
        {
            return Math.max(readRate, writeRate);
        }
    }

    // table key -> {reads, writes} counts at the previous collection
    private final Map<String, long[]> counts = new HashMap<>();
    private long lastCollect = -1;
    private long lastWindowStart = Long.MIN_VALUE;

    private List<Window> open = new ArrayList<>();
    private long windowStartMillis;
    private long windowStartNanos;
    private long windowMillis;

    @Override
    public String name()
    {
        return "hot_partition";
    }

    @Override
    public Set<SignalId> signals()
    {
        return SIGNALS;
    }

    @Override
    public long minIntervalMillis()
    {
        return 1_000;
    }

    @Override
    public void start()
    {
        counts.clear();
        lastCollect = -1;
        lastWindowStart = Long.MIN_VALUE;
        open = new ArrayList<>();
    }

    @Override
    public void stop()
    {
        abandon();
    }

    @Override
    public void collect(ObservationSink sink, long now)
    {
        AutonomicConfig.SamplingConfig cfg = DatabaseDescriptor.getAutonomicConfig().sampling;
        List<Candidate> candidates = rates(now, cfg.min_ops_per_sec);

        if (!open.isEmpty())
        {
            if (!cfg.enabled || now - windowStartMillis >= windowMillis)
                finishWindows(sink, now);
            return;
        }
        if (!cfg.enabled || candidates.isEmpty())
            return;
        if (lastWindowStart != Long.MIN_VALUE && now - lastWindowStart < cfg.interval.toMilliseconds())
            return;
        beginWindows(candidates, now, cfg);
    }

    /** Updates the counter baselines and returns the tables at or above the rate threshold, busiest first. */
    private List<Candidate> rates(long now, int minOpsPerSec)
    {
        long elapsed = lastCollect < 0 ? -1 : now - lastCollect;
        lastCollect = now;
        List<Candidate> result = new ArrayList<>();
        Set<String> live = new HashSet<>();
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            if (SchemaConstants.isSystemKeyspace(cfs.getKeyspaceName()) || cfs.isIndex())
                continue;
            try
            {
                String key = cfs.getKeyspaceName() + '.' + cfs.getTableName();
                live.add(key);
                long reads = cfs.metric.readLatency.latency.getCount();
                long writes = cfs.metric.writeLatency.latency.getCount();
                long[] prev = counts.put(key, new long[]{ reads, writes });
                if (prev == null || elapsed <= 0 || reads < prev[0] || writes < prev[1])
                    continue;
                double readRate = (reads - prev[0]) * 1000.0 / elapsed;
                double writeRate = (writes - prev[1]) * 1000.0 / elapsed;
                if (Math.max(readRate, writeRate) >= minOpsPerSec)
                    result.add(new Candidate(cfs, readRate, writeRate));
            }
            catch (RuntimeException e)
            {
                // table dropped while reading its metrics
            }
        }
        counts.keySet().retainAll(live);
        result.sort(Comparator.comparingDouble(Candidate::max).reversed());
        return result;
    }

    private void beginWindows(List<Candidate> candidates, long now, AutonomicConfig.SamplingConfig cfg)
    {
        int duration = (int) cfg.window.toMilliseconds();
        List<Window> started = new ArrayList<>();
        for (Candidate c : candidates)
        {
            if (started.size() >= MAX_TABLES_PER_WINDOW)
                break;
            boolean reads = c.readRate >= cfg.min_ops_per_sec && begin(c.cfs, Sampler.SamplerType.READS, cfg.capacity, duration);
            boolean writes = c.writeRate >= cfg.min_ops_per_sec && begin(c.cfs, Sampler.SamplerType.WRITES, cfg.capacity, duration);
            if (reads || writes)
                started.add(new Window(c.cfs, reads, writes));
        }
        // the interval counts from the attempt, so a table that an operator is always profiling is not retried every tick
        lastWindowStart = now;
        if (started.isEmpty())
            return;
        open = started;
        windowStartMillis = now;
        windowStartNanos = Clock.Global.nanoTime();
        windowMillis = duration;
    }

    /** True if a window was opened. Yields to an active sampler and treats "already in progress" as skipped. */
    private static boolean begin(ColumnFamilyStore cfs, Sampler.SamplerType type, int capacity, int durationMillis)
    {
        try
        {
            if (cfs.metric.samplers.get(type).isActive())
                return false;
            cfs.beginLocalSampling(type.name(), capacity, durationMillis);
            return true;
        }
        catch (RuntimeException e)
        {
            return false;
        }
    }

    private void finishWindows(ObservationSink sink, long now)
    {
        List<Window> windows = open;
        open = new ArrayList<>();
        long realElapsedMillis = TimeUnit.NANOSECONDS.toMillis(Clock.Global.nanoTime() - windowStartNanos);
        long elapsedMillis = now - windowStartMillis;
        // if the window was closed late the sampler only saw the first windowMillis of the counted operations
        double scale = elapsedMillis > windowMillis ? (double) windowMillis / elapsedMillis : 1.0;
        for (Window w : windows)
        {
            try
            {
                if (w.reads)
                    finish(sink, now, w.cfs, w.cfs.metric.topReadPartitionFrequency, realElapsedMillis,
                           (long) ((w.cfs.metric.readLatency.latency.getCount() - w.readsBase) * scale),
                           Signals.PARTITION_READ_SHARE, Signals.PARTITION_READ_OPS_PER_SEC);
                if (w.writes)
                    finish(sink, now, w.cfs, w.cfs.metric.topWritePartitionFrequency, realElapsedMillis,
                           (long) ((w.cfs.metric.writeLatency.latency.getCount() - w.writesBase) * scale),
                           Signals.PARTITION_WRITE_SHARE, Signals.PARTITION_WRITE_OPS_PER_SEC);
            }
            catch (RuntimeException e)
            {
                // table dropped during the window: nothing to report
            }
        }
    }

    private void finish(ObservationSink sink, long now, ColumnFamilyStore cfs, Sampler<ByteBuffer> sampler,
                        long realElapsedMillis, long tableOps, SignalId share, SignalId opsPerSec)
    {
        if (ownedByOther(sampler, realElapsedMillis))
            return;
        List<Sampler.Sample<ByteBuffer>> samples = sampler.finishSampling(TOP_K);
        if (tableOps <= 0)
            return;
        for (Sampler.Sample<ByteBuffer> sample : samples)
        {
            double s = Math.min(1.0, (double) sample.count / tableOps);
            if (s < MIN_EMIT_SHARE)
                continue;
            Subject partition = PartitionKeys.subject(cfs.getKeyspaceName(), cfs.getTableName(), sample.value);
            emit(sink, share, partition, now, s);
            emit(sink, opsPerSec, partition, now, sample.count * 1000.0 / windowMillis);
        }
    }

    /**
     * Our window has expired by the real clock, so a sampler that is active now was re-armed by someone else
     * (an operator's profileload) and must not be finished by us.
     */
    private boolean ownedByOther(Sampler<?> sampler, long realElapsedMillis)
    {
        return realElapsedMillis >= windowMillis && sampler.isActive();
    }

    /** Closes open windows without reporting, only touching samplers that are still ours. */
    private void abandon()
    {
        long realElapsedMillis = TimeUnit.NANOSECONDS.toMillis(Clock.Global.nanoTime() - windowStartNanos);
        List<Window> windows = open;
        open = new ArrayList<>();
        for (Window w : windows)
        {
            try
            {
                if (w.reads && !ownedByOther(w.cfs.metric.topReadPartitionFrequency, realElapsedMillis))
                    w.cfs.metric.topReadPartitionFrequency.finishSampling(0);
                if (w.writes && !ownedByOther(w.cfs.metric.topWritePartitionFrequency, realElapsedMillis))
                    w.cfs.metric.topWritePartitionFrequency.finishSampling(0);
            }
            catch (RuntimeException e)
            {
                // table dropped
            }
        }
    }
}
