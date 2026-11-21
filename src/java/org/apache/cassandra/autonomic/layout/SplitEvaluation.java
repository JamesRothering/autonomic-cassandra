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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.execute.Verdict;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.knowledge.Series;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.HotPartitionSensor;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;

/**
 * The verdict tables of adaptive-schema design 7.3, as pure functions of numbers: stage A (CUTOVER, reversible) and
 * stage B (after the commit point). The numbers come from {@link LayoutProfiler} through {@code LayoutSensor}; the
 * baseline is the key's profile when the policy proposed the split, carried by the action's parameters
 * ({@link Baseline#toParams}) and then by the claim's state.
 *
 * <p>The latency guards of stages A and B are those of ADR-040. The split key's own latency is not compared with its
 * latency before the split, which describes another placement and another path: it is bounded by its expected
 * overhead over a reference, the larger of its baseline and the concurrent latency of the rest of its table (the
 * requests that touch no watched key). The rest of the table is the collateral guard: its latency is compared with
 * its own baseline.
 *
 * <p>A check that has too little data ({@code min_samples} operations of its class in the baseline and in the window)
 * is not judged: it neither fails a stage nor counts for it, and the result says so.
 */
public final class SplitEvaluation
{
    /** Added to the baseline error rate in stage A. */
    public static final double ERROR_RATE_TOLERANCE = 0.01;
    /**
     * Stages A and B: the key's write latency may exceed its reference, per copy written, by this fraction (ADR-040):
     * the routing of the write (plan, mapping to the bucket, validation, the stock path run again under the hook).
     */
    public static final double WRITE_TOLERANCE = 0.5;
    /** Stage A: CUTOVER writes both copies, the base copy first (design 4.4). */
    public static final int CUTOVER_WRITE_COPIES = 2;
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
    /** The fraction of the key's reads in the policy's profile that were slices (read from every bucket after the split). */
    public static final String P_SLICE_FRACTION = P_PREFIX + "slice_fraction";
    /** The rest of the table (ADR-040): the mean latencies and operation counts of the requests that touched no watched key. */
    public static final String P_REST_READ_MICROS = P_PREFIX + "rest_read_micros";
    public static final String P_REST_WRITE_MICROS = P_PREFIX + "rest_write_micros";
    public static final String P_REST_READS = P_PREFIX + "rest_reads";
    public static final String P_REST_WRITES = P_PREFIX + "rest_writes";
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
        /** The fraction of the key's reads that were slices; NaN if unknown (counted as none). */
        public final double sliceFraction;
        /** The rest of the table; NaN and 0 where it was not measured. */
        public final Window rest;

        public Baseline(double readMicros, double writeMicros, double errorRate, long reads, long writes, double opsShare, boolean sizeOnly)
        {
            this(readMicros, writeMicros, errorRate, reads, writes, opsShare, sizeOnly, Double.NaN);
        }

        public Baseline(double readMicros, double writeMicros, double errorRate, long reads, long writes, double opsShare, boolean sizeOnly,
                        double tableWriteMicros)
        {
            this(readMicros, writeMicros, errorRate, reads, writes, opsShare, sizeOnly, tableWriteMicros, Double.NaN, Window.NONE);
        }

        public Baseline(double readMicros, double writeMicros, double errorRate, long reads, long writes, double opsShare, boolean sizeOnly,
                        double tableWriteMicros, double sliceFraction, Window rest)
        {
            this.tableWriteMicros = tableWriteMicros;
            this.readMicros = readMicros;
            this.writeMicros = writeMicros;
            this.errorRate = errorRate;
            this.reads = reads;
            this.writes = writes;
            this.opsShare = opsShare;
            this.sizeOnly = sizeOnly;
            this.sliceFraction = sliceFraction;
            this.rest = rest == null ? Window.NONE : rest;
        }

        /**
         * The baseline taken in ANNOUNCED (design 7.3): the key's signals and those of the rest of its table over the
         * window, and the table's write latency; the busiest share, the slice fraction and {@code partition_size_only}
         * stay those of the policy's profile, if any.
         */
        public static Baseline announced(Window window, Window rest, double tableWriteMicros, Baseline proposed)
        {
            return new Baseline(window.readMicros, window.writeMicros, window.errorRate, window.reads, window.writes,
                                proposed == null ? Double.NaN : proposed.opsShare, proposed != null && proposed.sizeOnly, tableWriteMicros,
                                proposed == null ? Double.NaN : proposed.sliceFraction, rest);
        }

        public static Baseline of(LayoutProfiler.Shape profile, double opsShare, boolean sizeOnly)
        {
            long reads = profile.namedReads + profile.sliceReads;
            return new Baseline(profile.meanReadMicros(), profile.meanWriteMicros(), profile.errorRate(), profile.timedReads, profile.timedWrites,
                                opsShare, sizeOnly, Double.NaN, reads == 0 ? Double.NaN : (double) profile.sliceReads / reads, Window.NONE);
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
            Window rest = new Window(number(map, P_REST_READ_MICROS), number(map, P_REST_WRITE_MICROS), Double.NaN,
                                     (long) orZero(number(map, P_REST_READS)), (long) orZero(number(map, P_REST_WRITES)));
            return new Baseline(number(map, P_READ_MICROS), number(map, P_WRITE_MICROS), number(map, P_ERROR_RATE),
                                (long) orZero(number(map, P_READS)), (long) orZero(number(map, P_WRITES)), number(map, P_OPS_SHARE),
                                "true".equals(map.get(P_SIZE_ONLY)), number(map, P_TABLE_WRITE_MICROS), number(map, P_SLICE_FRACTION), rest);
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
            put(m, P_SLICE_FRACTION, sliceFraction);
            put(m, P_REST_READ_MICROS, rest.readMicros);
            put(m, P_REST_WRITE_MICROS, rest.writeMicros);
            if (rest.reads > 0)
                m.put(P_REST_READS, Long.toString(rest.reads));
            if (rest.writes > 0)
                m.put(P_REST_WRITES, Long.toString(rest.writes));
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

    /** The signals of a key, or of the rest of its table, over one window; NaN where there were none. */
    public static final class Window
    {
        /** Nothing measured. */
        public static final Window NONE = new Window(Double.NaN, Double.NaN, Double.NaN, 0, 0);

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
            double[] error = weighted(knowledge.series(Signals.KEY_ERROR_RATE, key, lookback), sum(reads, writes), toMillis);
            return of(knowledge, Signals.KEY_READ_LATENCY_MICROS, Signals.KEY_WRITE_LATENCY_MICROS, key, reads, writes, lookback, toMillis,
                      error[1] == 0 ? Double.NaN : error[0] / error[1]);
        }

        /** The signals of {@code LayoutSensor} for the rest of the table at times in {@code [fromMillis, toMillis]} (ADR-040); no error rate. */
        public static Window rest(KnowledgeView knowledge, Subject table, long fromMillis, long toMillis)
        {
            long lookback = Math.max(0, knowledge.nowMillis() - fromMillis);
            Series reads = knowledge.series(Signals.REST_READ_OPS_DELTA, table, lookback);
            Series writes = knowledge.series(Signals.REST_WRITE_OPS_DELTA, table, lookback);
            return of(knowledge, Signals.REST_READ_LATENCY_MICROS, Signals.REST_WRITE_LATENCY_MICROS, table, reads, writes, lookback, toMillis, Double.NaN);
        }

        private static Window of(KnowledgeView knowledge, SignalId readLatency, SignalId writeLatency, Subject subject, Series reads, Series writes,
                                 long lookback, long toMillis, double errorRate)
        {
            double[] read = weighted(knowledge.series(readLatency, subject, lookback), reads, toMillis);
            double[] write = weighted(knowledge.series(writeLatency, subject, lookback), writes, toMillis);
            return new Window(read[1] == 0 ? Double.NaN : read[0] / read[1], write[1] == 0 ? Double.NaN : write[0] / write[1],
                              errorRate, total(reads, toMillis), total(writes, toMillis));
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
            // not judged against a baseline without data: an idle table reports a latency of 0 (a limit of 0 would fail any write),
            // and a baseline of fewer than min_samples operations says nothing (design 7.3: a check without min_samples is not judged)
            if (baseline.writes >= lc.min_samples && !Double.isNaN(baseline.tableWriteMicros) && baseline.tableWriteMicros > 0 && !Double.isNaN(tableWriteMicros))
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
     * Stage A (design 7.3, ADR-040), over CUTOVER's soak, where reads come from the buckets and writes go to both copies:
     * the collateral guard, the key's read and write latency within their expected overhead (two copies written), the
     * key's error rate within the baseline's plus {@link #ERROR_RATE_TOLERANCE}. Fails only on a check that was judged.
     *
     * @param baseline null if the split was not proposed by the policy and has no profile
     * @param rest the rest of the key's table over the same window
     * @param buckets the number of buckets of the split
     */
    public static Result stageA(Baseline baseline, Window window, Window rest, int buckets, AutonomicConfig.LayoutConfig lc)
    {
        if (baseline == null)
            return new Result(true, null, "stage A not judged: no baseline (the split was not proposed with a profile)");
        StringBuilder detail = new StringBuilder();
        boolean failed = errorCheck(baseline, window, lc, detail);
        failed |= latencyGuards(baseline, window, rest, buckets, CUTOVER_WRITE_COPIES, lc, detail);
        return new Result(!failed, null, detail.toString());
    }

    /**
     * Stage B (design 7.3, ADR-040). Guards: the collateral guard, and the key's read and write latency within their
     * expected overhead (one copy written). Goal: the busiest partition's share of the table's operations fell by at
     * least {@code min_improvement}; for {@code partition_size_only} it is not measured. A breached guard is REGRESSED,
     * whatever the goal; a goal that was not met is NEUTRAL; a goal that cannot be judged is INCONCLUSIVE.
     *
     * @param rest the rest of the key's table over the same window
     * @param afterShare the largest share among the base key and the bucket partitions now, NaN if unknown
     * @param buckets the number of buckets of the split
     */
    public static Result stageB(Baseline baseline, Window window, Window rest, double afterShare, int buckets, AutonomicConfig.LayoutConfig lc)
    {
        if (baseline == null)
            return new Result(true, Verdict.INCONCLUSIVE, "stage B not judged: no baseline (the split was not proposed with a profile)");
        StringBuilder detail = new StringBuilder();
        boolean breached = latencyGuards(baseline, window, rest, buckets, 1, lc, detail);
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

    /**
     * The four latency guards of ADR-040, each judged only with {@code min_samples} operations of its class in the
     * baseline and in the window; true if one that was judged failed.
     * <ul>
     * <li>collateral read and write: the rest of the table not above its baseline * (1 + {@code collateral_tolerance});</li>
     * <li>key read: not above reference * (1 + slice fraction * (buckets - 1)) * (1 + {@code read_regression_tolerance}),
     * a slice reading every bucket;</li>
     * <li>key write: not above reference * copies * (1 + {@link #WRITE_TOLERANCE}).</li>
     * </ul>
     * The reference of a key guard is the larger of the key's baseline and the rest of the table in the window (or, with
     * too few operations there, in the baseline): the split moves the key to other replicas and another path, and the
     * rest of the table, over the same nodes and at the same time, is what an ordinary operation costs there.
     */
    private static boolean latencyGuards(Baseline baseline, Window window, Window rest, int buckets, int copies, AutonomicConfig.LayoutConfig lc,
                                         StringBuilder detail)
    {
        rest = rest == null ? Window.NONE : rest;
        Window restBefore = baseline.rest;
        boolean failed = false;
        double collateral = 1 + lc.collateral_tolerance;
        failed |= check("collateral read latency", restBefore.reads, rest.reads, restBefore.readMicros, rest.readMicros,
                        restBefore.readMicros * collateral, "", lc, detail);
        failed |= check("collateral write latency", restBefore.writes, rest.writes, restBefore.writeMicros, rest.writeMicros,
                        restBefore.writeMicros * collateral, "", lc, detail);

        double readReference = reference(baseline.readMicros, rest.readMicros, rest.reads, restBefore.readMicros, restBefore.reads, lc);
        double slices = Double.isNaN(baseline.sliceFraction) ? 0 : Math.max(0, Math.min(1, baseline.sliceFraction));
        double fanOut = 1 + slices * (Math.max(1, buckets) - 1);
        failed |= check("key read latency", baseline.reads, window.reads, baseline.readMicros, window.readMicros,
                        readReference * fanOut * (1 + lc.read_regression_tolerance),
                        " (baseline " + format(baseline.readMicros) + "us, reference " + format(readReference) + "us, fan-out " + format(fanOut) + ')',
                        lc, detail);

        double writeReference = reference(baseline.writeMicros, rest.writeMicros, rest.writes, restBefore.writeMicros, restBefore.writes, lc);
        failed |= check("key write latency", baseline.writes, window.writes, baseline.writeMicros, window.writeMicros,
                        writeReference * copies * (1 + WRITE_TOLERANCE),
                        " (baseline " + format(baseline.writeMicros) + "us, reference " + format(writeReference) + "us, "
                        + copies + (copies == 1 ? " copy" : " copies") + ')', lc, detail);
        return failed;
    }

    /**
     * The larger of the key's baseline and the rest of the table: now if that has {@code min_samples} operations, else
     * its baseline if that has them, else the key's baseline alone.
     */
    private static double reference(double keyBaseline, double restNow, long restOps, double restBaseline, long restBaselineOps,
                                    AutonomicConfig.LayoutConfig lc)
    {
        if (restOps >= lc.min_samples && !Double.isNaN(restNow))
            return Math.max(keyBaseline, restNow);
        if (restBaselineOps >= lc.min_samples && !Double.isNaN(restBaseline))
            return Math.max(keyBaseline, restBaseline);
        return keyBaseline;
    }

    /** One latency guard; true if it was judged and failed. What was judged, and what was not, goes to {@code detail}, a breach first. */
    private static boolean check(String name, long baselineOps, long windowOps, double before, double after, double limit, String basis,
                                 AutonomicConfig.LayoutConfig lc, StringBuilder detail)
    {
        if (baselineOps < lc.min_samples || windowOps < lc.min_samples || Double.isNaN(before) || Double.isNaN(after) || before <= 0)
        {
            detail.append(name).append(" not judged (").append(baselineOps).append('/').append(windowOps).append(" ops, min ")
                  .append(lc.min_samples).append("); ");
            return false;
        }
        boolean over = after > limit;
        String result = name + (over ? " " : " ok ") + format(after) + (over ? " above " : " within ") + format(limit) + "us" + basis + "; ";
        // a breach first: an abort's reason is kept to a bounded length
        if (over)
            detail.insert(0, result);
        else
            detail.append(result);
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
     * The busiest share of the logical table after a split (stage B's goal, ADR-041): the largest, over the base key and
     * the partitions of the overflow table, of the reads (or writes) it took in the last {@code windowMillis} divided by
     * the reads (or writes) of the base and overflow tables together over the sampling windows of {@code HotPartitionSensor}
     * in that time. The counts are added up over the windows; one window's share, decided by a handful of operations, is
     * not used. In a window that did not report a partition, its count is bounded by the smallest count reported for its
     * table if {@link HotPartitionSensor#TOP_K} were reported, else by {@link HotPartitionSensor#MIN_EMIT_SHARE} of the
     * table's operations; the same bound stands for a partition of the overflow table that no window reported. A table
     * not sampled in a window counts for neither side. A class (reads, writes) with fewer than {@code minOps} operations
     * is not judged; NaN if neither is.
     */
    public static double busiestShare(KnowledgeView knowledge, Subject baseKey, String keyspace, String overflowTable, long windowMillis, long minOps)
    {
        double reads = busiestShare(knowledge, Signals.PARTITION_READ_SHARE, Signals.SAMPLED_TABLE_READS, baseKey, keyspace, overflowTable, windowMillis, minOps);
        double writes = busiestShare(knowledge, Signals.PARTITION_WRITE_SHARE, Signals.SAMPLED_TABLE_WRITES, baseKey, keyspace, overflowTable, windowMillis, minOps);
        return Double.isNaN(reads) ? writes : Double.isNaN(writes) ? reads : Math.max(reads, writes);
    }

    private static double busiestShare(KnowledgeView knowledge, SignalId shareSignal, SignalId opsSignal, Subject baseKey, String keyspace,
                                       String overflowTable, long windowMillis, long minOps)
    {
        Subject baseTable = Subject.table(baseKey.keyspace, baseKey.table);
        Subject overflow = Subject.table(keyspace, overflowTable);
        Map<Subject, Map<Long, Double>> tableOps = new LinkedHashMap<>();
        double total = 0;
        for (Subject table : new Subject[]{ baseTable, overflow })
        {
            Map<Long, Double> ops = points(knowledge.series(opsSignal, table, windowMillis));
            tableOps.put(table, ops);
            for (double v : ops.values())
                total += v;
        }
        if (total <= 0 || total < minOps)
            return Double.NaN;

        // the counts reported per partition and window, and every count reported per table and window
        Map<Subject, Map<Long, Double>> counts = new LinkedHashMap<>();
        Map<Subject, Map<Long, Collection<Double>>> reported = new LinkedHashMap<>();
        counts.put(baseKey, new LinkedHashMap<>());
        for (Subject subject : knowledge.subjects(shareSignal))
        {
            if (subject.kind != Subject.Kind.PARTITION)
                continue;
            Subject table = Subject.table(subject.keyspace, subject.table);
            Map<Long, Double> ops = tableOps.get(table);
            if (ops == null)
                continue;
            Series shares = knowledge.series(shareSignal, subject, windowMillis);
            for (int i = 0; i < shares.size(); i++)
            {
                Double n = ops.get(shares.times[i]);
                if (n == null)
                    continue;
                double count = shares.values[i] * n;
                reported.computeIfAbsent(table, k -> new LinkedHashMap<>()).computeIfAbsent(shares.times[i], k -> new ArrayList<>()).add(count);
                if (subject.equals(baseKey) || table.equals(overflow))
                    counts.computeIfAbsent(subject, k -> new LinkedHashMap<>()).put(shares.times[i], count);
            }
        }
        // a partition of the overflow table that no window reported
        Subject unseen = Subject.partition(keyspace, overflowTable, null);
        counts.put(unseen, new LinkedHashMap<>());

        double max = 0;
        for (Map.Entry<Subject, Map<Long, Double>> e : counts.entrySet())
        {
            Subject table = e.getKey().equals(baseKey) ? baseTable : overflow;
            Map<Long, Collection<Double>> byWindow = reported.getOrDefault(table, Map.of());
            double sum = 0;
            for (Map.Entry<Long, Double> window : tableOps.get(table).entrySet())
            {
                Double count = e.getValue().get(window.getKey());
                sum += count != null ? count : unreportedBound(byWindow.get(window.getKey()), window.getValue());
            }
            max = Math.max(max, sum);
        }
        return Math.min(1, max / total);
    }

    /** The most an unreported partition can have taken in a window with these reported counts and table operations. */
    private static double unreportedBound(Collection<Double> reported, double tableOps)
    {
        if (reported == null || reported.size() < HotPartitionSensor.TOP_K)
            return HotPartitionSensor.MIN_EMIT_SHARE * tableOps;
        double min = Double.MAX_VALUE;
        for (double c : reported)
            min = Math.min(min, c);
        return min;
    }

    /** The points of a series by time, the values of equal times added. */
    private static Map<Long, Double> points(Series series)
    {
        Map<Long, Double> m = new LinkedHashMap<>();
        for (int i = 0; i < series.size(); i++)
            m.merge(series.times[i], series.values[i], Double::sum);
        return m;
    }
}
