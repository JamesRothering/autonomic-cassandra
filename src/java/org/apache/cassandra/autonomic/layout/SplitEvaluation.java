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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.execute.Verdict;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.knowledge.Series;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;

/**
 * The verdict tables of adaptive-schema design 7.3, as pure functions of numbers: stage A (CUTOVER, reversible) and
 * stage B (after the commit point). The numbers come from {@link LayoutProfiler} through {@code LayoutSensor}; the
 * baseline is the key's profile when the policy proposed the split, carried by the action's parameters
 * ({@link Baseline#toParams}) and then by the claim's state.
 *
 * <p>A check that has too little data ({@code min_samples} operations) is not judged: it neither fails a stage nor
 * counts for it, and the result says so.
 */
public final class SplitEvaluation
{
    /** Added to the baseline error rate in stage A. */
    public static final double ERROR_RATE_TOLERANCE = 0.01;
    /** Stage B guard: the write latency may exceed its baseline by this fraction. */
    public static final double WRITE_TOLERANCE = 0.25;
    /** C1 guard: the table's coordinator write latency may reach this multiple of its baseline. */
    public static final double TABLE_WRITE_FACTOR = 3.0;
    /** Why a migration is aborted in ANNOUNCED when the baseline has too few operations (design 7.3). */
    public static final String INSUFFICIENT_BASELINE = "INSUFFICIENT_BASELINE";

    public static final String P_PREFIX = "baseline.";
    public static final String P_READ_MICROS = P_PREFIX + "read_micros";
    public static final String P_WRITE_MICROS = P_PREFIX + "write_micros";
    public static final String P_ERROR_RATE = P_PREFIX + "error_rate";
    public static final String P_READS = P_PREFIX + "reads";
    public static final String P_WRITES = P_PREFIX + "writes";
    public static final String P_OPS_SHARE = P_PREFIX + "ops_share";
    /** The table's coordinator write latency (p99, mean over the window), for the C1 guard. */
    public static final String P_TABLE_WRITE_MICROS = P_PREFIX + "table_write_micros";
    /** {@code partition_size_only}: the split bounds the partition's size and moves no load (design 7.2 step 5). */
    public static final String P_SIZE_ONLY = P_PREFIX + "size_only";
    /**
     * The claim state entry that holds the whole baseline ({@link Baseline#encode}): one entry instead of one per
     * number, because a claim holds at most 16.
     */
    public static final String S_BASELINE = "baseline";

    private SplitEvaluation()
    {
    }

    /** What the key looked like before the split; NaN for what was not measured. */
    public static final class Baseline
    {
        public final double readMicros;
        public final double writeMicros;
        public final double errorRate;
        public final long reads;
        public final long writes;
        /** The busiest partition's share of the table's operations. */
        public final double opsShare;
        public final boolean sizeOnly;
        public final double tableWriteMicros;

        public Baseline(double readMicros, double writeMicros, double errorRate, long reads, long writes, double opsShare, boolean sizeOnly)
        {
            this(readMicros, writeMicros, errorRate, reads, writes, opsShare, sizeOnly, Double.NaN);
        }

        public Baseline(double readMicros, double writeMicros, double errorRate, long reads, long writes, double opsShare, boolean sizeOnly,
                        double tableWriteMicros)
        {
            this.tableWriteMicros = tableWriteMicros;
            this.readMicros = readMicros;
            this.writeMicros = writeMicros;
            this.errorRate = errorRate;
            this.reads = reads;
            this.writes = writes;
            this.opsShare = opsShare;
            this.sizeOnly = sizeOnly;
        }

        /**
         * The baseline taken in ANNOUNCED (design 7.3): the key's signals over the window and the table's write
         * latency; the busiest share and {@code partition_size_only} stay those of the policy's profile, if any.
         */
        public static Baseline announced(Window window, double tableWriteMicros, Baseline proposed)
        {
            return new Baseline(window.readMicros, window.writeMicros, window.errorRate, window.reads, window.writes,
                                proposed == null ? Double.NaN : proposed.opsShare, proposed != null && proposed.sizeOnly, tableWriteMicros);
        }

        public static Baseline of(LayoutProfiler.Shape profile, double opsShare, boolean sizeOnly)
        {
            return new Baseline(profile.meanReadMicros(), profile.meanWriteMicros(), profile.errorRate(), profile.timedReads, profile.timedWrites,
                                opsShare, sizeOnly);
        }

        /** {@code name=value} pairs of {@link #toParams} without the prefix, separated by {@code ;}. */
        public String encode()
        {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> e : toParams().entrySet())
                sb.append(sb.length() == 0 ? "" : ";").append(e.getKey().substring(P_PREFIX.length())).append('=').append(e.getValue());
            return sb.toString();
        }

        /**
         * Null if the map carries no baseline: the action parameters (one {@link #P_PREFIX} entry per number) or the
         * claim state ({@link #S_BASELINE}).
         */
        public static Baseline fromMap(Map<String, String> map)
        {
            String encoded = map.get(S_BASELINE);
            if (encoded != null)
            {
                Map<String, String> params = new LinkedHashMap<>();
                for (String pair : encoded.split(";"))
                {
                    int eq = pair.indexOf('=');
                    if (eq > 0)
                        params.put(P_PREFIX + pair.substring(0, eq), pair.substring(eq + 1));
                }
                map = params;
            }
            if (!map.containsKey(P_READS) && !map.containsKey(P_WRITES) && !map.containsKey(P_OPS_SHARE))
                return null;
            return new Baseline(number(map, P_READ_MICROS), number(map, P_WRITE_MICROS), number(map, P_ERROR_RATE),
                                (long) orZero(number(map, P_READS)), (long) orZero(number(map, P_WRITES)), number(map, P_OPS_SHARE),
                                "true".equals(map.get(P_SIZE_ONLY)), number(map, P_TABLE_WRITE_MICROS));
        }

        public Map<String, String> toParams()
        {
            Map<String, String> m = new LinkedHashMap<>();
            put(m, P_READ_MICROS, readMicros);
            put(m, P_WRITE_MICROS, writeMicros);
            put(m, P_ERROR_RATE, errorRate);
            m.put(P_READS, Long.toString(reads));
            m.put(P_WRITES, Long.toString(writes));
            put(m, P_OPS_SHARE, opsShare);
            if (sizeOnly)
                m.put(P_SIZE_ONLY, "true");
            put(m, P_TABLE_WRITE_MICROS, tableWriteMicros);
            return m;
        }

        private static void put(Map<String, String> m, String key, double v)
        {
            if (!Double.isNaN(v))
                m.put(key, Double.toString(v));
        }

        private static double number(Map<String, String> m, String key)
        {
            String v = m.get(key);
            if (v == null)
                return Double.NaN;
            try
            {
                return Double.parseDouble(v);
            }
            catch (NumberFormatException e)
            {
                return Double.NaN;
            }
        }

        private static double orZero(double v)
        {
            return Double.isNaN(v) ? 0 : v;
        }
    }

    /** The key's signals over one window; NaN where there were none. */
    public static final class Window
    {
        public final double readMicros;
        public final double writeMicros;
        public final double errorRate;
        public final long reads;
        public final long writes;

        public Window(double readMicros, double writeMicros, double errorRate, long reads, long writes)
        {
            this.readMicros = readMicros;
            this.writeMicros = writeMicros;
            this.errorRate = errorRate;
            this.reads = reads;
            this.writes = writes;
        }

        /** The signals of {@code LayoutSensor} for {@code key} at times in {@code [fromMillis, toMillis]}, weighted by their operation counts. */
        public static Window of(KnowledgeView knowledge, Subject key, long fromMillis, long toMillis)
        {
            long lookback = Math.max(0, knowledge.nowMillis() - fromMillis);
            Series reads = knowledge.series(Signals.KEY_READ_OPS_DELTA, key, lookback);
            Series writes = knowledge.series(Signals.KEY_WRITE_OPS_DELTA, key, lookback);
            double[] read = weighted(knowledge.series(Signals.KEY_READ_LATENCY_MICROS, key, lookback), reads, toMillis);
            double[] write = weighted(knowledge.series(Signals.KEY_WRITE_LATENCY_MICROS, key, lookback), writes, toMillis);
            double[] error = weighted(knowledge.series(Signals.KEY_ERROR_RATE, key, lookback), sum(reads, writes), toMillis);
            return new Window(read[1] == 0 ? Double.NaN : read[0] / read[1], write[1] == 0 ? Double.NaN : write[0] / write[1],
                              error[1] == 0 ? Double.NaN : error[0] / error[1], total(reads, toMillis), total(writes, toMillis));
        }

        private static long total(Series ops, long toMillis)
        {
            long n = 0;
            for (int i = 0; i < ops.size(); i++)
                if (ops.times[i] <= toMillis)
                    n += (long) ops.values[i];
            return n;
        }

        /** The sum of two aligned operation count series. */
        private static Series sum(Series a, Series b)
        {
            Map<Long, Double> m = new LinkedHashMap<>();
            for (int i = 0; i < a.size(); i++)
                m.merge(a.times[i], a.values[i], Double::sum);
            for (int i = 0; i < b.size(); i++)
                m.merge(b.times[i], b.values[i], Double::sum);
            long[] times = new long[m.size()];
            double[] values = new double[m.size()];
            int i = 0;
            for (Map.Entry<Long, Double> e : m.entrySet())
            {
                times[i] = e.getKey();
                values[i++] = e.getValue();
            }
            return new Series(times, values);
        }

        /** {sum of value * weight, sum of weight} of the points of {@code values} that have a weight at the same time. */
        private static double[] weighted(Series values, Series weights, long toMillis)
        {
            Map<Long, Double> w = new LinkedHashMap<>();
            for (int i = 0; i < weights.size(); i++)
                w.merge(weights.times[i], weights.values[i], Double::sum);
            double num = 0;
            double den = 0;
            for (int i = 0; i < values.size(); i++)
            {
                Double weight = w.get(values.times[i]);
                if (weight == null || values.times[i] > toMillis)
                    continue;
                num += values.values[i] * weight;
                den += weight;
            }
            return new double[]{ num, den };
        }
    }

    /** The mean of the signal's points at times in {@code [fromMillis, toMillis]}; NaN if there are none. */
    public static double mean(KnowledgeView knowledge, SignalId signal, Subject subject, long fromMillis, long toMillis)
    {
        Series series = knowledge.series(signal, subject, Math.max(0, knowledge.nowMillis() - fromMillis));
        double sum = 0;
        int n = 0;
        for (int i = 0; i < series.size(); i++)
            if (series.times[i] >= fromMillis && series.times[i] <= toMillis)
            {
                sum += series.values[i];
                n++;
            }
        return n == 0 ? Double.NaN : sum / n;
    }

    /** True if the window has the {@code min_samples} operations a baseline needs (design 7.3). */
    public static boolean sufficient(Window window, AutonomicConfig.LayoutConfig lc)
    {
        return window.reads + window.writes >= lc.min_samples;
    }

    /**
     * The guard of class C1 (design 7.3, DUAL_WRITE to VERIFY): the key's error rate not above the baseline's plus
     * {@link #ERROR_RATE_TOLERANCE}; the table's coordinator write latency not above {@link #TABLE_WRITE_FACTOR} times
     * its baseline; no CRITICAL saturation diagnosis that opened since dual write started. Fails only on a check that
     * was judged.
     *
     * @param newSaturation the types of such diagnoses
     */
    public static Result guardC1(Baseline baseline, Window window, double tableWriteMicros, Collection<String> newSaturation,
                                 AutonomicConfig.LayoutConfig lc)
    {
        StringBuilder detail = new StringBuilder();
        boolean failed = false;
        if (baseline != null)
        {
            failed |= errorCheck(baseline, window, lc, detail);
            if (!Double.isNaN(baseline.tableWriteMicros) && !Double.isNaN(tableWriteMicros))
            {
                double limit = baseline.tableWriteMicros * TABLE_WRITE_FACTOR;
                boolean over = tableWriteMicros > limit;
                failed |= over;
                detail.append(over ? "table write latency " : "table write latency ok ").append(format(tableWriteMicros)).append(over ? " above " : " within ")
                      .append(format(limit)).append("us; ");
            }
        }
        if (!newSaturation.isEmpty())
        {
            failed = true;
            detail.append("new CRITICAL saturation diagnosis ").append(newSaturation).append("; ");
        }
        if (detail.length() == 0)
            detail.append(baseline == null ? "C1 guard: no baseline; " : "C1 guard not judged: too few operations; ");
        return new Result(!failed, null, detail.toString());
    }

    /** The outcome of a stage; {@code verdict} is stage B's, stage A's is {@link #ok}. */
    public static final class Result
    {
        /** False: stage A failed. */
        public final boolean ok;
        public final Verdict verdict;
        public final String detail;

        Result(boolean ok, Verdict verdict, String detail)
        {
            this.ok = ok;
            this.verdict = verdict;
            this.detail = detail;
        }

        @Override
        public String toString()
        {
            return (verdict == null ? (ok ? "PASS" : "FAIL") : verdict.name()) + ": " + detail;
        }
    }

    /**
     * Stage A (design 7.3): key read latency mean not above {@code baseline * (1 + read_regression_tolerance)}, error rate
     * not above the baseline's plus {@link #ERROR_RATE_TOLERANCE}. Fails only on a check that was judged.
     *
     * @param baseline null if the split was not proposed by the policy and has no profile
     */
    public static Result stageA(Baseline baseline, Window window, AutonomicConfig.LayoutConfig lc)
    {
        StringBuilder detail = new StringBuilder();
        if (baseline == null)
            return new Result(true, null, "stage A not judged: no baseline (the split was not proposed with a profile)");
        boolean failed = false;
        failed |= readCheck(baseline, window, lc, detail);
        failed |= errorCheck(baseline, window, lc, detail);
        if (detail.length() == 0)
            detail.append("stage A not judged: fewer than ").append(lc.min_samples).append(" operations in the window");
        return new Result(!failed, null, detail.toString());
    }

    /**
     * Stage B (design 7.3). Guards: write latency not above baseline + 25%, read latency within the stage A
     * tolerance. Goal: the busiest partition's share of the table's operations fell by at least {@code min_improvement};
     * for {@code partition_size_only} it is not measured. A breached guard is REGRESSED, whatever the goal; a goal that
     * was not met is NEUTRAL; a goal that cannot be judged is INCONCLUSIVE.
     *
     * @param afterShare the largest share among the base key and the bucket partitions now, NaN if unknown
     */
    public static Result stageB(Baseline baseline, Window window, double afterShare, AutonomicConfig.LayoutConfig lc)
    {
        if (baseline == null)
            return new Result(true, Verdict.INCONCLUSIVE, "stage B not judged: no baseline (the split was not proposed with a profile)");
        StringBuilder detail = new StringBuilder();
        boolean breached = readCheck(baseline, window, lc, detail);
        if (window.writes >= lc.min_samples && !Double.isNaN(baseline.writeMicros) && baseline.writes >= lc.min_samples && !Double.isNaN(window.writeMicros))
        {
            double limit = baseline.writeMicros * (1 + WRITE_TOLERANCE);
            boolean over = window.writeMicros > limit;
            breached |= over;
            detail.append(over ? "write latency " : "write latency ok ").append(format(window.writeMicros)).append(over ? " above " : " within ")
                  .append(format(limit)).append("us; ");
        }
        if (breached)
            return new Result(false, Verdict.REGRESSED, detail + "a guard was breached");
        if (baseline.sizeOnly)
            return new Result(true, Verdict.INCONCLUSIVE, detail + "partition_size_only: bucket row counts are not measured");
        if (Double.isNaN(baseline.opsShare) || Double.isNaN(afterShare))
            return new Result(true, Verdict.INCONCLUSIVE, detail + "the goal cannot be judged: share before "
                                                           + format(baseline.opsShare) + ", after " + format(afterShare));
        double reduction = baseline.opsShare <= 0 ? 0 : 1 - afterShare / baseline.opsShare;
        detail.append("busiest partition share ").append(format(baseline.opsShare)).append(" -> ").append(format(afterShare))
              .append(" (reduction ").append(format(reduction)).append(", required ").append(format(lc.min_improvement)).append("); ");
        return reduction >= lc.min_improvement ? new Result(true, Verdict.IMPROVED, detail + "goal met")
                                               : new Result(true, Verdict.NEUTRAL, detail + "goal not met");
    }

    /** True if the read latency check was judged and failed. */
    private static boolean readCheck(Baseline baseline, Window window, AutonomicConfig.LayoutConfig lc, StringBuilder detail)
    {
        if (window.reads < lc.min_samples || baseline.reads < lc.min_samples || Double.isNaN(baseline.readMicros) || Double.isNaN(window.readMicros))
            return false;
        double limit = baseline.readMicros * (1 + lc.read_regression_tolerance);
        boolean over = window.readMicros > limit;
        detail.append(over ? "read latency " : "read latency ok ").append(format(window.readMicros)).append(over ? " above " : " within ")
              .append(format(limit)).append("us; ");
        return over;
    }

    private static boolean errorCheck(Baseline baseline, Window window, AutonomicConfig.LayoutConfig lc, StringBuilder detail)
    {
        if (window.reads + window.writes < lc.min_samples || Double.isNaN(window.errorRate))
            return false;
        double limit = (Double.isNaN(baseline.errorRate) ? 0 : baseline.errorRate) + ERROR_RATE_TOLERANCE;
        boolean over = window.errorRate > limit;
        detail.append(over ? "error rate " : "error rate ok ").append(format(window.errorRate)).append(over ? " above " : " within ")
              .append(format(limit)).append("; ");
        return over;
    }

    public static String format(double v)
    {
        return Double.isNaN(v) ? "n/a" : String.format("%.3g", v);
    }

    /**
     * The largest share of its table's operations that the base key or any partition of the overflow table had in the
     * last {@code windowMillis} (from the sampler of {@code HotPartitionSensor}); NaN if there was no sample.
     */
    public static double busiestShare(KnowledgeView knowledge, Subject baseKey, String keyspace, String overflowTable, long windowMillis)
    {
        double max = Double.NaN;
        for (SignalId signal : new SignalId[]{ Signals.PARTITION_READ_SHARE, Signals.PARTITION_WRITE_SHARE })
            for (Subject subject : knowledge.subjects(signal))
            {
                boolean ours = subject.equals(baseKey)
                               || (subject.kind == Subject.Kind.PARTITION && keyspace.equals(subject.keyspace) && overflowTable.equals(subject.table));
                if (!ours)
                    continue;
                Series series = knowledge.series(signal, subject, windowMillis);
                for (int i = 0; i < series.size(); i++)
                    max = Double.isNaN(max) ? series.values[i] : Math.max(max, series.values[i]);
            }
        return max;
    }
}
