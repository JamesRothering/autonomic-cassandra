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
package org.apache.cassandra.autonomic.execute;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.autonomic.plan.VerifySpec;
import org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.FreeRunningClock;

/**
 * SPIKE CODE, NOT A TEST OF PRODUCTION BEHAVIOUR (docs/spikes/guard-traffic-floor.md).
 *
 * Simulates a latency guard of ADR-045 at a given request rate: Poisson arrivals feed the real
 * {@link DecayingEstimatedHistogramReservoir} (as {@code CassandraMetricsRegistry.createReservoir} builds it for a
 * microsecond timer) on a simulated clock, the p99 is read every 5 s as {@code ClientRequestSensor} reads it, and the
 * samples go through {@link MetricVerifier} (baseline, rule 8 every 15 s, rule 6 after the observation window), with
 * the traffic floor off. It counts false alarms (no change) and detections (latency scaled by 1.5 and 2 from apply).
 * Skipped unless SPIKE_GUARD_FLOOR is set; SPIKE_TRIALS (default 500) trials per point; the tables go to SPIKE_OUT
 * (default build/test/spike-guard-floor.txt). With SPIKE_GUARD_FLOOR_REAL naming the file written by
 * {@code GuardTrafficFloorRealNodeSpikeTest}, {@link #compareWithRealNode} replays the node's latency distribution
 * through the same pipeline and compares the p99 noise.
 */
public class GuardTrafficFloorSpikeTest
{
    private static final SignalId GUARD = Signals.CLIENT_WRITE_LATENCY_P99_MICROS;
    /** Constant goal, so that a verdict other than NEUTRAL comes from the guard alone. */
    private static final SignalId GOAL = Signals.CLIENT_READ_LATENCY_MEAN_MICROS;
    private static final Subject NODE = Subject.node();
    private static final String GUARD_KEY = MetricVerifier.key(GUARD, NODE);

    private static final long SENSOR_MILLIS = 5_000;    // ClientRequestSensor.minIntervalMillis, monitor_interval 5 s
    private static final long CONTROL_MILLIS = 15_000;  // analyze_interval
    private static final long BASELINE_WINDOW = 120_000; // Windows.DEFAULT_BASELINE_MILLIS
    private static final long OBSERVATION = 300_000;    // Windows.DEFAULT_OBSERVATION_MILLIS
    private static final double TOLERANCE = 0.25;       // KnobPolicy.LATENCY_TOLERANCE
    private static final long WARMUP = 360_000;
    private static final double[] RATES = { 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, 100, 200 };
    private static final long[] OFFSETS = EstimatedHistogram.newOffsets(DecayingEstimatedHistogramReservoir.LOW_BUCKET_COUNT, false);

    /** Latency of one operation in microseconds. */
    interface Shape
    {
        double sample(Random r);
    }

    static Shape lognormal(double median, double sigma)
    {
        return r -> median * Math.exp(sigma * r.nextGaussian());
    }

    /** {@code slowShare} of the operations from a slow lognormal mode, the rest from a fast one. */
    static Shape bimodal(double fastMedian, double fastSigma, double slowShare, double slowMedian, double slowSigma)
    {
        return r -> {
            boolean slow = r.nextDouble() < slowShare;
            double g = r.nextGaussian();
            return slow ? slowMedian * Math.exp(slowSigma * g) : fastMedian * Math.exp(fastSigma * g);
        };
    }

    /** Draws a bucket by its count, then a value inside the bucket, so the reservoir puts it back in that bucket. */
    static Shape empirical(long[] counts)
    {
        long[] cumulative = new long[counts.length];
        long sum = 0;
        for (int i = 0; i < counts.length; i++)
            cumulative[i] = sum += Math.max(0, counts[i]);
        long total = sum;
        return r -> {
            long pick = (long) (r.nextDouble() * total);
            // first bucket whose cumulative count exceeds pick
            int i = Arrays.binarySearch(cumulative, pick + 1);
            i = i >= 0 ? i : -i - 1;
            while (i > 0 && cumulative[i - 1] == cumulative[i])
                i--;
            long hi = i < OFFSETS.length ? OFFSETS[i] : OFFSETS[OFFSETS.length - 1];
            long lo = i == 0 ? 0 : OFFSETS[i - 1];
            return lo + 1 + r.nextDouble() * (hi - lo - 1);
        };
    }

    /** How the baseline period is chosen. */
    enum BaselineMode
    {
        /** ADR-044 with defaults: first breaching check to apply, 45 to 60 s. */
        PROBLEM_PERIOD,
        /** The full baseline_window (2 min). */
        FULL_WINDOW,
        /** The least that the policy accepts: MetricVerifier.MIN_SAMPLES samples (15 s). */
        MIN_SAMPLES,
        /** RealLoadSpikeScenarioTest's windows: baseline and observation 17 s, control loop every 500 ms. */
        SCENARIO_17S;

        long observationMillis()
        {
            return this == SCENARIO_17S ? 17_000 : OBSERVATION;
        }

        long controlMillis()
        {
            return this == SCENARIO_17S ? 500 : CONTROL_MILLIS;
        }
    }

    static final class Outcome
    {
        boolean aborted;
        Verdict verdict;
        double ratio = Double.NaN;      // observation mean / baseline mean (rule 6)
        int baselineSamples;
        double baselineMean;
        double maxSinceApplyRatio;      // worst rule-8 ratio seen

        boolean alarm()
        {
            return aborted || verdict == Verdict.REGRESSED;
        }
    }

    static long baselinePeriod(BaselineMode mode, Random r)
    {
        switch (mode)
        {
            case FULL_WINDOW: return BASELINE_WINDOW;
            case MIN_SAMPLES: return MetricVerifier.MIN_SAMPLES * SENSOR_MILLIS;
            case SCENARIO_17S: return 17_000;
            default: return 45_000 + (long) (r.nextDouble() * 15_000);
        }
    }

    static Outcome trial(Shape shape, double rate, double factor, BaselineMode mode, long seed)
    {
        Random r = new Random(seed);
        long apply = 600_000 + (long) (r.nextDouble() * 1_800_000); // any phase of the reservoir's 30 min landmark
        long baselinePeriod = baselinePeriod(mode, r);
        long sensorPhase = (long) (r.nextDouble() * SENSOR_MILLIS);
        long controlEvery = mode.controlMillis();
        long controlPhase = (long) (r.nextDouble() * controlEvery);

        FreeRunningClock clock = new FreeRunningClock(0);
        DecayingEstimatedHistogramReservoir reservoir = new DecayingEstimatedHistogramReservoir(DecayingEstimatedHistogramReservoir.DEFAULT_ZERO_CONSIDERATION,
                                                                                                DecayingEstimatedHistogramReservoir.LOW_BUCKET_COUNT,
                                                                                                DecayingEstimatedHistogramReservoir.DEFAULT_STRIPE_COUNT,
                                                                                                clock,
                                                                                                DecayingEstimatedHistogramReservoir.LANDMARK_RESET_INTERVAL_IN_NS);
        AtomicLong now = new AtomicLong();
        ObservationStore store = new ObservationStore(1024, 4);
        InMemoryKnowledgeView view = new InMemoryKnowledgeView(store, now::get);
        VerifySpec spec = new VerifySpec(GOAL, NODE, true, 0.10,
                                         List.of(new VerifySpec.Guard(GUARD, NODE, true, TOLERANCE)),
                                         BASELINE_WINDOW, mode.observationMillis(), apply - baselinePeriod, 0);

        long start = apply - baselinePeriod - WARMUP;
        long nextSensor = start - Math.floorMod(start - sensorPhase, SENSOR_MILLIS) + SENSOR_MILLIS;
        long nextControl = apply - Math.floorMod(apply - controlPhase, controlEvery) + controlEvery;
        double nextArrivalNanos = start * 1e6 + exponential(r, rate);
        boolean applied = false;
        Map<String, Double> baseline = null;
        Outcome out = new Outcome();
        long clockNanos = 0;

        while (true)
        {
            long eventMillis = Math.min(nextSensor, Math.min(nextControl, applied ? Long.MAX_VALUE : apply));
            if (nextArrivalNanos < eventMillis * 1e6)
            {
                long t = (long) nextArrivalNanos;
                clock.advance(t - clockNanos, TimeUnit.NANOSECONDS);
                clockNanos = t;
                double micros = shape.sample(r) * (t >= apply * 1_000_000L ? factor : 1);
                reservoir.update((long) micros); // ScalingReservoir truncates nanos to micros
                nextArrivalNanos += exponential(r, rate);
                continue;
            }
            long t = eventMillis * 1_000_000L;
            clock.advance(t - clockNanos, TimeUnit.NANOSECONDS);
            clockNanos = t;
            now.set(eventMillis);
            if (!applied && eventMillis == apply)
            {
                applied = true;
                Optional<Map<String, Double>> b = MetricVerifier.baseline(spec, view);
                if (!b.isPresent())
                    throw new IllegalStateException("no baseline");
                baseline = b.get();
                out.baselineMean = baseline.get(GUARD_KEY);
                out.baselineSamples = view.series(GUARD, NODE, baselinePeriod).size();
            }
            else if (eventMillis == nextSensor)
            {
                store.accept(new Observation(GUARD, NODE, eventMillis, reservoir.getSnapshot().get99thPercentile()));
                store.accept(new Observation(GOAL, NODE, eventMillis, 1000));
                nextSensor += SENSOR_MILLIS;
            }
            else
            {
                long sinceApply = eventMillis - apply;
                if (sinceApply >= spec.observationWindowMillis)
                {
                    Map<String, Double> result = MetricVerifier.means(spec, view, spec.observationWindowMillis);
                    out.verdict = MetricVerifier.verdict(spec, baseline, result);
                    Double after = result.get(GUARD_KEY);
                    out.ratio = after == null ? Double.NaN : after / out.baselineMean;
                    return out;
                }
                if (MetricVerifier.guardBreached(spec, baseline, view, sinceApply).isPresent())
                    out.aborted = true;
                if (view.series(GUARD, NODE, sinceApply).size() >= MetricVerifier.MIN_SAMPLES)
                    out.maxSinceApplyRatio = Math.max(out.maxSinceApplyRatio, mean(view.series(GUARD, NODE, sinceApply).values) / out.baselineMean);
                nextControl += controlEvery;
            }
        }
    }

    private static double exponential(Random r, double ratePerSec)
    {
        return -Math.log(1 - r.nextDouble()) / ratePerSec * 1e9;
    }

    private static double mean(double[] v)
    {
        double s = 0;
        for (double x : v)
            s += x;
        return s / v.length;
    }

    private static double quantile(double[] sorted, double q)
    {
        if (sorted.length == 0)
            return Double.NaN;
        return sorted[Math.min(sorted.length - 1, (int) Math.floor(q * sorted.length))];
    }

    private static long seed(String shape, BaselineMode mode, double rate, int trial)
    {
        return (shape.hashCode() * 31L + mode.ordinal()) * 1_000_003L + (long) (rate * 10) * 100_003L + trial;
    }

    private static Outcome[] run(String name, Shape shape, double rate, double factor, BaselineMode mode, int trials)
    {
        return IntStream.range(0, trials).parallel()
                        .mapToObj(i -> trial(shape, rate, factor, mode, seed(name, mode, rate, i)))
                        .toArray(Outcome[]::new);
    }

    private static double share(Outcome[] os, java.util.function.Predicate<Outcome> p)
    {
        int n = 0;
        for (Outcome o : os)
            if (p.test(o))
                n++;
        return 100.0 * n / os.length;
    }

    private static String env(String key, String def)
    {
        String v = System.getenv(key); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        return v == null ? def : v;
    }

    @Test
    public void simulate() throws IOException
    {
        // spike-only switches, read from the environment on purpose
        Assume.assumeTrue("spike code, set SPIKE_GUARD_FLOOR to run", env("SPIKE_GUARD_FLOOR", null) != null);
        int trials = Integer.parseInt(env("SPIKE_TRIALS", "500"));
        String only = env("SPIKE_SHAPES", "lognormal,tight,bimodal");
        List<String> modes = Arrays.asList(env("SPIKE_MODES", "PROBLEM_PERIOD,FULL_WINDOW,MIN_SAMPLES,SCENARIO_17S").split(","));
        try (PrintWriter out = new PrintWriter(new FileWriter(env("SPIKE_OUT", "build/test/spike-guard-floor.txt"), true)))
        {
            Object[][] shapes = {
                { "lognormal", lognormal(2000, 0.5), "median 2 ms, sigma 0.5 (p99 about 6.4 ms)" },
                { "tight", lognormal(2000, 0.2), "median 2 ms, sigma 0.2 (p99 about 3.2 ms)" },
                { "bimodal", bimodal(1500, 0.3, 0.03, 15000, 0.5), "97% median 1.5 ms sigma 0.3, 3% median 15 ms sigma 0.5" },
            };
            for (Object[] s : shapes)
            {
                String name = (String) s[0];
                if (!Arrays.asList(only.split(",")).contains(name))
                    continue;
                for (BaselineMode mode : BaselineMode.values())
                {
                    if (!modes.contains(mode.name()) || (mode != BaselineMode.PROBLEM_PERIOD && "bimodal".equals(name)))
                        continue;
                    table(out, name, (Shape) s[1], (String) s[2], mode, trials);
                }
            }
        }
    }

    private static void table(PrintWriter out, String name, Shape shape, String description, BaselineMode mode, int trials)
    {
        print(out, String.format(Locale.ROOT, "%n## shape %s (%s), baseline %s, %d trials per point", name, description, mode, trials));
        print(out, "| rate ops/s | baseline samples | baseline p99 median us | A/A ratio p1 / p50 / p99 | false alarm: abort | verdict | total | detect +50% (abort) | detect +100% (abort) |");
        print(out, "|---|---|---|---|---|---|---|---|---|");
        for (double rate : RATES)
        {
            long begin = System.nanoTime();
            Outcome[] aa = run(name, shape, rate, 1.0, mode, trials);
            Outcome[] d50 = run(name, shape, rate, 1.5, mode, trials);
            Outcome[] d100 = run(name, shape, rate, 2.0, mode, trials);
            double[] ratios = Arrays.stream(aa).mapToDouble(o -> o.ratio).filter(x -> !Double.isNaN(x)).sorted().toArray();
            double[] base = Arrays.stream(aa).mapToDouble(o -> o.baselineMean).sorted().toArray();
            double[] samples = Arrays.stream(aa).mapToDouble(o -> o.baselineSamples).sorted().toArray();
            print(out, String.format(Locale.ROOT, "| %s | %.0f | %.0f | %.2f / %.2f / %.2f | %.1f%% | %.1f%% | **%.1f%%** | %.1f%% (%.1f%%) | %.1f%% (%.1f%%) |",
                                     fmt(rate), quantile(samples, 0.5), quantile(base, 0.5),
                                     quantile(ratios, 0.01), quantile(ratios, 0.5), quantile(ratios, 0.99),
                                     share(aa, o -> o.aborted), share(aa, o -> !o.aborted && o.verdict == Verdict.REGRESSED), share(aa, Outcome::alarm),
                                     share(d50, Outcome::alarm), share(d50, o -> o.aborted),
                                     share(d100, Outcome::alarm), share(d100, o -> o.aborted)));
            System.err.printf(Locale.ROOT, "%s %s rate %s took %d s%n", name, mode, fmt(rate), TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - begin));
        }
        out.flush();
    }

    private static String fmt(double rate)
    {
        return rate == Math.rint(rate) ? Long.toString((long) rate) : Double.toString(rate);
    }

    private static void print(PrintWriter out, String line)
    {
        out.println(line);
        System.out.println(line);
    }

    // ---- comparison with a real node ---------------------------------------------------------------------------

    /** Consecutive sensor samples of the baseline-like and observation-like windows of the comparison statistic. */
    private static final int BEFORE = 10;
    private static final int AFTER = 24;

    /** p99 samples every 5 s at {@code rate} from {@code shape}, after a warm-up, as the sensor would report them. */
    static double[] series(Shape shape, double rate, int samples, long seed)
    {
        Random r = new Random(seed);
        FreeRunningClock clock = new FreeRunningClock(0);
        DecayingEstimatedHistogramReservoir reservoir = new DecayingEstimatedHistogramReservoir(DecayingEstimatedHistogramReservoir.DEFAULT_ZERO_CONSIDERATION,
                                                                                                DecayingEstimatedHistogramReservoir.LOW_BUCKET_COUNT,
                                                                                                DecayingEstimatedHistogramReservoir.DEFAULT_STRIPE_COUNT,
                                                                                                clock,
                                                                                                DecayingEstimatedHistogramReservoir.LANDMARK_RESET_INTERVAL_IN_NS);
        long startNanos = TimeUnit.MINUTES.toNanos(5) + (long) (r.nextDouble() * TimeUnit.MINUTES.toNanos(30));
        long firstSample = startNanos + TimeUnit.MILLISECONDS.toNanos(WARMUP);
        double arrival = startNanos + exponential(r, rate);
        long clockNanos = 0;
        double[] out = new double[samples];
        for (int i = 0; i < samples; i++)
        {
            long sampleAt = firstSample + i * TimeUnit.MILLISECONDS.toNanos(SENSOR_MILLIS);
            while (arrival < sampleAt)
            {
                long t = (long) arrival;
                clock.advance(t - clockNanos, TimeUnit.NANOSECONDS);
                clockNanos = t;
                reservoir.update((long) shape.sample(r));
                arrival += exponential(r, rate);
            }
            clock.advance(sampleAt - clockNanos, TimeUnit.NANOSECONDS);
            clockNanos = sampleAt;
            out[i] = reservoir.getSnapshot().get99thPercentile();
        }
        return out;
    }

    /** mean(next AFTER samples) / mean(previous BEFORE samples), at every split point of the series. */
    static List<Double> windowRatios(double[] s)
    {
        List<Double> out = new ArrayList<>();
        for (int k = BEFORE; k + AFTER <= s.length; k++)
        {
            double before = mean(Arrays.copyOfRange(s, k - BEFORE, k));
            double after = mean(Arrays.copyOfRange(s, k, k + AFTER));
            out.add(after / before);
        }
        return out;
    }

    /** Each sample divided by the median of its series. */
    static List<Double> relativeSamples(double[] s)
    {
        double[] sorted = s.clone();
        Arrays.sort(sorted);
        double median = quantile(sorted, 0.5);
        List<Double> out = new ArrayList<>();
        for (double v : s)
            out.add(v / median);
        return out;
    }

    private static String stats(List<Double> values)
    {
        double[] v = values.stream().mapToDouble(Double::doubleValue).sorted().toArray();
        long over = values.stream().filter(x -> x > 1 + TOLERANCE).count();
        return String.format(Locale.ROOT, "p5 %.2f, p50 %.2f, p95 %.2f, max %.2f, > 1.25: %.1f%% (n=%d)",
                             quantile(v, 0.05), quantile(v, 0.5), quantile(v, 0.95), v.length == 0 ? Double.NaN : v[v.length - 1],
                             100.0 * over / Math.max(1, v.length), v.length);
    }

    @Test
    public void compareWithRealNode() throws IOException
    {
        String path = env("SPIKE_GUARD_FLOOR_REAL", null);
        Assume.assumeTrue("spike code, set SPIKE_GUARD_FLOOR_REAL to the real node's file", path != null);
        List<String> lines = Files.readAllLines(Paths.get(path));
        try (PrintWriter out = new PrintWriter(new FileWriter(env("SPIKE_OUT", "build/test/spike-guard-floor.txt"), true)))
        {
            for (int i = 0; i + 3 < lines.size(); i++)
            {
                if (!lines.get(i).startsWith("rate "))
                    continue;
                double rate = Double.parseDouble(lines.get(i).substring(5).trim());
                double[] real = parse(lines.get(i + 1), "p99 ");
                double[] ops = parse(lines.get(i + 2), "ops ");
                long[] buckets = Arrays.stream(parse(lines.get(i + 3), "buckets ")).mapToLong(d -> (long) d).toArray();
                Shape shape = empirical(buckets);
                List<Double> simRatios = new ArrayList<>();
                List<Double> simRelative = new ArrayList<>();
                for (int rep = 0; rep < 300; rep++)
                {
                    double[] s = series(shape, rate, real.length, 77_000L + rep);
                    simRatios.addAll(windowRatios(s));
                    simRelative.addAll(relativeSamples(s));
                }
                print(out, String.format(Locale.ROOT, "%n## real node, %s writes/s (measured mean %.2f ops/s), %d p99 samples, %d operations in the analysed period",
                                         fmt(rate), mean(ops), real.length, Arrays.stream(buckets).sum()));
                print(out, "real p99 samples us: " + Arrays.toString(Arrays.stream(real).mapToLong(d -> (long) d).toArray()));
                print(out, "sample / series median, real: " + stats(relativeSamples(real)));
                print(out, "sample / series median, sim:  " + stats(simRelative));
                print(out, String.format(Locale.ROOT, "mean(next %d) / mean(previous %d), real: %s", AFTER, BEFORE, stats(windowRatios(real))));
                print(out, String.format(Locale.ROOT, "mean(next %d) / mean(previous %d), sim:  %s", AFTER, BEFORE, stats(simRatios)));
                // the guard pipeline with this node's own latency distribution
                if (Arrays.stream(buckets).sum() >= 1000)
                    table(out, "real" + fmt(rate), shape, "the real node's write latency distribution at " + fmt(rate) + " ops/s",
                          BaselineMode.PROBLEM_PERIOD, Integer.parseInt(env("SPIKE_TRIALS", "500")));
            }
        }
    }

    private static double[] parse(String line, String prefix)
    {
        if (!line.startsWith(prefix))
            throw new IllegalArgumentException("expected " + prefix + ": " + line);
        String body = line.substring(prefix.length()).trim();
        return body.isEmpty() ? new double[0] : Arrays.stream(body.split(" ")).mapToDouble(Double::parseDouble).toArray();
    }
}
