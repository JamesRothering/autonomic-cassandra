# Spike: is 10 ops/s the right traffic floor for latency guards?

Status: spike result, 2026-11-22. No production code changed. Decision left to the owner.

## The question

ADR-045 skips a latency guard (`client.read_latency_p99_micros`, `client.write_latency_p99_micros`,
`table.read_latency_p99_micros`) when the mean traffic of its request class over the baseline period is below
`safety.guard_min_ops_per_sec`, default 10. Set too low, noise on a lightly used class undoes correct fixes (the
observed failure: write p99 2759 to 3568 us, +29% against a 25% tolerance, on a node with almost no writes). Set too
high, a real slowdown of light but real traffic is not guarded. Is 10 a good default?

## Short answer

**Yes, keep 10.** It is close to the lowest safe value, not a generous one. Using at most 1% false alarms as the
limit, with production windows:

- With this node's **real** write latency distributions replayed through the real histogram and verifier, 10 ops/s
  gives **0.1 to 0.4%** false alarms and 5 ops/s gives **2.6 to 4.6%**. Just above the floor real slowdowns are
  caught: +50% in 93 to 97% of trials, +100% in 100%.
- A smooth (unimodal) synthetic distribution is quiet from about 5 ops/s (0.1%), 0.0% at 10.
- If the p99 comes from a rare slow mode (3% of operations, as with GC pauses or cache misses) the guard is
  noisier: **1.8% at 10 ops/s, 0.3% at 20**. If such workloads matter, 20 is the safer default. It costs guard
  coverage for request classes between 10 and 20 ops/s on a node.
- Below 2 ops/s the guard is unusable for every realistic shape tested, with 14 to 60% false alarms (only a very
  tight synthetic distribution stays quiet there). The floor is needed.

The noise depends on the **rate**, not on how many operations fall in the baseline period. The p99 the sensor
reports is computed over the histogram's own memory, about 90 s of operations, whatever the baseline length. So
the floor is best kept in ops/s, and a sample-count floor would be the wrong form (section 5).

The real node matched the simulation where it can be compared: at 2 and 10 ops/s the spread of its p99 samples is
what the simulation predicts for its own latency distribution. At 50 ops/s the real p99 drifted (3311 to 1916 us
in three minutes) while the simulation predicts a flat line. That drift is a real change in service time, not
sampling noise, so a floor cannot remove it.

## 1. How the guard's numbers are produced

**Which metric.** `ClientRequestSensor` emits `client.read/write_latency_p99_micros` as
`m.latency.getSnapshot().get99thPercentile()` of `ClientRequestsMetricsHolder.readMetrics`/`writeMetrics`
(`ClientRequestSensor.java:94`); `TableSensor` emits `table.read_latency_p99_micros` the same way from
`TableMetrics.readLatency` (`TableSensor.java:142`, `TableMetrics.java:702`). Both are a `LatencyMetrics` timer whose
reservoir is `CassandraMetricsRegistry.createReservoir(MICROSECONDS)` (`LatencyMetrics.java:89`): a
`DecayingEstimatedHistogramReservoir` with 127 buckets and no zero bucket, behind a `ScalingReservoir` that
truncates nanoseconds to microseconds (`CassandraMetricsRegistry.java:407-416`). The traffic signals
(`client.*_ops_per_sec`, `table.read_ops_per_sec`) are count deltas over the sensor interval.

**How that histogram behaves** (`DecayingEstimatedHistogramReservoir.java`):

- Buckets grow by about 1.2x (..., 1916, 2299, 2759, 3311, 3973, ... us). A p99 can only take bucket upper bounds,
  so it moves in steps of about 20%. One step is below the 25% tolerance; two steps (+44%) are above it.
- Forward decay with a half-life of 60 s (`HALF_TIME_IN_S`, line 161), so an operation weighs 1/e after 86.6 s
  (`MEAN_LIFETIME_IN_S`, line 162). The p99 therefore describes roughly the last 1.5 minutes of operations:
  about `rate x 87` operations, of which about `rate x 0.87` are above the p99.
- The weights are whole numbers: `round(exp(whole seconds since landmark / 86.6))` (line 479), and a snapshot
  divides every bucket by the current weight and rounds again (line 699). The landmark is reset every 30 minutes
  (line 163). At low counts this rounding makes old operations vanish in steps rather than fade.
- The percentile is the upper bound of the first bucket where the cumulative count reaches `ceil(count x 0.99)`
  (line 515). With fewer than 100 effective operations that is the bucket of the slowest recent operation: at low
  rates the "p99" is a decaying maximum.

**How often.** Both sensors run at most every 5 s (`minIntervalMillis`, `ClientRequestSensor.java:61-63`,
`TableSensor.java:69-71`) from the monitor loop, `monitor_interval` 5 s with a fixed delay
(`AutonomicConfig.java:40`, `AutonomicService.java:488`, `:542`). So one p99 sample every 5 s.

**Baseline.** ADR-044: from the first breaching check of the diagnosis (`breach_since`, `Policies.java:55-79`) to
apply, capped at `baseline_window` (`MetricVerifier.java:113`), default 2 min (`Windows.java:30`). A diagnosis opens
after `for_evaluations` (4) checks 15 s apart (`analyze_interval`, `AutonomicConfig.java:41`), so with defaults the
baseline is the 45 to 60 s before apply: 9 to 12 samples. The executor takes it just before apply
(`ActionExecutor.java:680-684`). A window with fewer than `MetricVerifier.MIN_SAMPLES` = 3 samples has no mean
(`MetricVerifier.java:42`).

**Verdict, rule 6.** At the first control tick (every 15 s) at least `observation_window` after apply
(`ActionExecutor.java:565`), default 5 min (`Windows.java:31`), the mean over the last 5 min (60 samples) is
compared with the baseline mean; the guard regresses if it is more than 25% worse (`KnobPolicy.LATENCY_TOLERANCE`,
`KnobPolicy.java:64`; `MetricVerifier.java:169`).

**Early abort, rule 8.** At every control tick before that, the mean since apply (once it has 3 samples) is compared
with the baseline; more than `ABORT_FACTOR` (2) x 25% = 50% worse rolls the action back (`MetricVerifier.java:44`,
`:199`; `ActionExecutor.java:567`).

**The floor.** `safety.guard_min_ops_per_sec` = 10 (`AutonomicConfig.java:106`), compared with the mean of the
traffic signal over the same baseline period (`MetricVerifier.java:74`).

## 2. Method

`GuardTrafficFloorSpikeTest.simulate` (unit test, runs only with `SPIKE_GUARD_FLOOR` set):

- The **real** `DecayingEstimatedHistogramReservoir`, built exactly as `createReservoir` builds it for a microsecond
  timer, on a simulated clock (`FreeRunningClock`). The apply time is placed uniformly at random over the
  reservoir's 30-minute landmark cycle, so the rounding behaviour is sampled at every phase. Six minutes of traffic
  before the baseline fill the histogram.
- Poisson arrivals at rate r; each operation's latency from one of three shapes:
  - `lognormal`: median 2 ms, sigma 0.5 (p99 about 6.4 ms), a smooth heavy-ish tail;
  - `tight`: median 2 ms, sigma 0.2 (p99 about 3.2 ms), like a fast local write path;
  - `bimodal`: 97% median 1.5 ms sigma 0.3, 3% median 15 ms sigma 0.5 (the p99 falls inside the rare slow mode).
- A p99 sample every 5 s through `getSnapshot().get99thPercentile()`, stored in an `ObservationStore` behind an
  `InMemoryKnowledgeView`; then the real `MetricVerifier`: `baseline` at apply (with `baselineSinceMillis`),
  `guardBreached` at every 15 s control tick, `means` + `verdict` at the end of the observation window. The goal is
  a constant signal, so every non-NEUTRAL verdict comes from the guard. The traffic floor is set to 0 in the spec,
  so the guard is always judged: the tables show what the guard would do at each rate.
- Rates 0.1 to 200 ops/s, 1000 seeded trials per point.
- **False alarm**: no change at all (A/A), yet the guard aborts early or the verdict is REGRESSED.
- **Detection**: every latency after apply multiplied by 1.5 or 2 (the whole distribution scaled), and the guard
  aborts or regresses. In brackets the share caught by the early abort.
- Sensitivity: the baseline period as with defaults (45 to 60 s, uniform), the full `baseline_window` (2 min), and
  the least a policy accepts (3 samples, 15 s); plus the windows of `RealLoadSpikeScenarioTest` (17 s baseline, 17 s
  observation, 500 ms control loop) to check against the failure that started this.

With 1000 trials a rate of 1% has a 95% interval of about 0.5 to 1.8%; 0.0% means fewer than about 0.3%.

## 3. Results

### Production windows (baseline 45 to 60 s, observation 5 min)

False alarms (A/A), total of early abort and verdict:

| rate ops/s | lognormal | tight | bimodal |
|---|---|---|---|
| 0.1 | 26.4% | 1.9% | 55.0% |
| 0.2 | 21.9% | 0.6% | 63.0% |
| 0.5 | 13.9% | 0.4% | 54.7% |
| 1 | 14.0% | 0.0% | 41.0% |
| 2 | 2.7% | 0.0% | 26.3% |
| 5 | 0.1% | 0.0% | 9.4% |
| **10** | **0.0%** | **0.0%** | **1.8%** |
| 20 | 0.0% | 0.0% | 0.3% |
| 50 | 0.0% | 0.0% | 0.0% |
| 100 | 0.0% | 0.0% | 0.0% |
| 200 | 0.0% | 0.0% | 0.0% |

Detection of a real slowdown, +50% / +100% (share by early abort in brackets):

| rate ops/s | lognormal | tight | bimodal |
|---|---|---|---|
| 2 | 86% (38%) / 99.5% (95%) | 98% (48%) / 100% | 68% (51%) / 88% (77%) |
| 5 | 96% (19%) / 100% (99%) | 99% (40%) / 100% | 73% (32%) / 97% (82%) |
| 10 | 97% (9%) / 100% | 100% (44%) / 100% | 81% (22%) / 99.8% (87%) |
| 20 | 99.6% (2%) / 100% | 100% (35%) / 100% | 87% (11%) / 100% (95%) |
| 50 | 99.9% / 100% | 100% / 100% | 98% (3%) / 100% (99%) |
| 100 | 100% / 100% | 100% / 100% | 100% / 100% |

Spread of the A/A ratio (observation mean / baseline mean), 1st / 50th / 99th percentile, for the two
extreme shapes:

| rate ops/s | lognormal | bimodal |
|---|---|---|
| 2 | 0.76 / 1.01 / 1.31 | 0.48 / 1.01 / 3.08 |
| 5 | 0.86 / 1.00 / 1.19 | 0.69 / 1.00 / 1.51 |
| 10 | 0.86 / 1.00 / 1.11 | 0.78 / 1.00 / 1.31 |
| 20 | 0.93 / 1.00 / 1.04 | 0.84 / 0.99 / 1.21 |
| 50 | 1.00 / 1.00 / 1.00 | 0.90 / 1.00 / 1.17 |

What the tables say:

- The shape matters more than anything else. What counts is how many operations land above the p99 inside the
  histogram's 90 s memory and how far apart they are. A tight unimodal distribution is quiet even at 0.5 ops/s
  (its tail spans one or two buckets); a sparse slow mode needs 20 ops/s.
- At high rates the p99 locks onto one bucket and the A/A ratio is exactly 1.00. The bucket steps also mean a
  slowdown is seen as 0%, 20% or 44%, not as its real size (a +50% slowdown can read as +44% and miss the abort).
- Both rules contribute. For the bimodal shape most false alarms are early aborts: one slow operation lifts the
  short mean since apply by more than 50%. For the lognormal shape the 5-minute verdict contributes more, because
  the 45 to 60 s baseline is itself noisy and a low baseline makes an ordinary observation look 25% worse.
- Detection of +50% is not 100% even at moderate rates because the decaying histogram still holds pre-apply
  operations for the first minute or two of the observation window, which drags the observation mean down; +100%
  is caught almost always from 5 ops/s (unimodal) or 10 ops/s (bimodal).

### Sensitivity to the baseline length (lognormal)

| rate ops/s | 3 samples (15 s) | 45 to 60 s (default) | full window (2 min) |
|---|---|---|---|
| 0.5 | 17.2% | 13.9% | 14.0% |
| 1 | 13.4% | 14.0% | 9.9% |
| 2 | 5.3% | 2.7% | 2.3% |
| 5 | 0.3% | 0.1% | 0.0% |
| 10 | 0.0% | 0.0% | 0.0% |

A baseline eight times longer changes the false alarm rate far less than doubling the rate does. Consecutive p99
samples are strongly correlated (each one is a view of the same 90 s of operations), so more samples in the
baseline add little information; more operations per second do.

### Reproducing the observed failure (RealLoadSpikeScenarioTest windows: 17 s / 17 s)

| rate ops/s | lognormal false alarm | tight false alarm | lognormal detect +100% | tight detect +100% |
|---|---|---|---|---|
| 0.1 | 9.1% | 0.9% | 33% | 53% |
| 0.5 | 4.4% | 0.4% | 41% | 89% |
| 2 | 1.7% | 0.0% | 50% | 99% |
| 10 | 0.0% | 0.0% | 61% | 100% |
| 50 | 0.0% | 0.0% | 86% | 100% |

The simulation gives false alarms of a few percent at under 1 op/s with these windows, not the 50% of the T7.11
run. In that run the node had essentially **no** client writes after the setup inserts: the p99 came from a few
hundred old operations whose decayed counts were rounded away bucket by bucket, which is the limit "rate 0" that a
Poisson simulation does not reach (with no new operations the p99 can only move by rounding, and every move is a
whole bucket). The 17 s windows also show the other side: the decaying histogram reacts slowly, so with a 17 s
observation window even a doubling of latency is missed 15 to 60% of the time. That is a property of those test
windows, not of the defaults.

## 4. Real node

One node, layer in dry run with production sensor timing (`GuardTrafficFloorRealNodeSpikeTest`, an in-JVM dtest
that runs only with `SPIKE_GUARD_FLOOR` set, about 13 minutes). A client wrote through the node's coordinator at
Poisson rates of 2, 10 and 50 ops/s, about four minutes each. After a minute of settling the test recorded the
layer's own `client.write_latency_p99_micros` and `client.write_ops_per_sec` samples (38 per rate), plus the
non-decaying bucket counts of the write latency histogram over the same period. That gives the node's real
latency distribution. `GuardTrafficFloorSpikeTest.compareWithRealNode` then replays that distribution through the
same simulated pipeline, both for the same short series (300 repetitions) and for the full guard simulation.

The measured rates were 2.00, 10.21 and 49.85 ops/s. Write latency on this node, from the bucket counts: median 1.1, 0.64
and 0.2 ms (it got faster as the node warmed up), p99 2.8, 3.3 and 1.9 ms, and a few operations at 6 to 60 ms.

**Spread of individual p99 samples** (sample divided by its series' median):

| rate | real p95 / max / share > 1.25 | simulated with the node's distribution |
|---|---|---|
| 2 | 2.49 / 4.30 / 15.8% | 2.49 / 3.58 / 12.0% |
| 10 | 1.00 / 1.20 / 0.0% | 1.20 / 1.44 / 0.1% |
| 50 | 1.44 / 1.44 / 7.9% | 1.00 / 1.20 / 0.0% |

At 2 ops/s the real series looks like the simulated one: long flat runs on one bucket (2759 us), broken by single
slow operations that lift the p99 to 6866 or 11864 us for a minute or more. At 10 ops/s the real series stays on
3311 us with a few one-bucket dips, slightly quieter than simulated. The window statistic (mean of the next 24
samples over the previous 10) has only 5 split points per rate in a four-minute series, so it is weak evidence.
It agrees at 2 and 10 ops/s (real 0.76 to 1.29 and 0.97 to 0.98, simulated p5 to p95 0.54 to 1.96 and 0.88 to
1.13).

At 50 ops/s the real p99 fell steadily from 3311 to 1916 us over the three minutes (four buckets), while the
simulation predicts it locked on one bucket. That is a real change in the node's service time (a fresh node
warming up), not sampling noise. It shows that at high rates the guard measures real drift, which is what it is for.

**The guard simulation with the node's own write latency distribution** (production windows, 1000 trials):

| rate ops/s | false alarm (distribution at 10 ops/s) | false alarm (distribution at 50 ops/s) | detect +50% / +100% (at 10 / at 50) |
|---|---|---|---|
| 1 | 31.6% | 45.1% | 71 / 90%, 74 / 86% |
| 2 | 15.0% | 21.5% | 77 / 96%, 78 / 94% |
| 5 | 4.6% | 2.6% | 89 / 100%, 87 / 100% |
| **10** | **0.4%** | **0.1%** | 93 / 100%, 97 / 100% |
| 20 | 0.0% | 0.0% | 98 / 100%, 99 / 100% |
| 50 | 0.0% | 0.0% | 100 / 100%, 100 / 100% |

The real distributions behave between the smooth and the bimodal synthetic shapes. They have a sparse tail of
slow writes (0.5 to 1% of operations several buckets above the p99), so they are noisier than the lognormal at 2
to 5 ops/s, but at 10 ops/s they are under 1%. The distribution measured at 10 ops/s rests on 1939 operations, so
its tail is itself rough.

## 5. Recommendation

**Acceptable false alarm rate: at most 1% per guarded action.** A false alarm undoes a correct fix and lengthens
the policy's cooldown. A node action carries one or two latency guards, so 1% per guard means noise undoes about
one correct action in 50 to 100. That is well below the other reasons an action fails, and it is the level at
which each extra ops/s of floor stops buying much: going from 1% to 0.1% would need about 20 ops/s for the real
distributions and 50 for the bimodal one.

**Keep the default at 10 ops/s.** The evidence:

- the node's real write latency distributions: 0.4% and 0.1% at 10 (5 ops/s fails, with 2.6 to 4.6%);
- smooth distributions: 0.0% at 10. 5 would do, so 10 leaves a factor of two of margin there;
- just above the floor, detection of a real slowdown is 93 to 97% for +50% and 100% for +100% with the real
  distributions (bimodal: 81% and 99.8%).

The one case that 10 does not cover with margin is a p99 set by a rare slow mode (bimodal: 1.8% at 10, 0.3% at
20). If the owner expects such workloads to be common (read-heavy tables with cache misses, GC-heavy nodes), 20 is
the safer default. The price is that request classes between 10 and 20 ops/s on a node lose their latency guard.
Operators can already raise the floor per cluster (`safety.guard_min_ops_per_sec`). A simple rule for the
operations guide does not fall out of the data: the real distribution at 50 ops/s has a p99 nine times its median
and is fine at 10. What hurts is a p99 that falls inside a sparse slow mode, which a ratio does not show.

The answer depends on the windows only weakly. A baseline from 15 s to 2 min moves the crossing point by less than
a factor of two in rate, so the default fits every baseline length ADR-044 produces. A much shorter observation
window, as in the scenario tests, does not raise false alarms much, but it blinds the guard to real slowdowns
(section 3). That is a test setting, not a floor question.

**Form of the floor (for the owner only).** Keep it a rate. A count of operations in the baseline period would
make a short baseline need a high rate and let a long baseline pass with a low one, while the table above shows the
noise barely depends on the baseline length: the p99 is computed over the histogram's fixed ~90 s memory, so the
right quantity is "operations in that memory", which is the rate times a constant. For example, 300 operations in the baseline
give 0.0% false alarms as 20 ops/s over 15 s, but about 2% as 2.5 ops/s over 2 minutes (lognormal tables). If a
better form is wanted, it is the number of operations expected above the p99 in that memory, `rate x 87 s x 1%`:
about 9 at 10 ops/s and 17 at 20 ops/s. That is still a rate, just explained.

## 6. Open risks

- **Shapes.** Three synthetic shapes; a real workload can have a sparser tail than `bimodal`, which would need a
  higher floor. The floor would need to be set per workload where that matters, which it already can be
  (`safety.guard_min_ops_per_sec`).
- **Traffic that changes with the action.** The simulation keeps the rate constant across apply. An action that
  cuts a class's traffic (a throttle) makes its p99 noisier after apply than the baseline shows; ADR-045 already
  accepts this (the guard is judged on baseline traffic).
- **Near-idle classes below the Poisson range.** As in the T7.11 failure, a class with almost no new operations
  has a p99 that moves only by rounding of old counts. The floor removes these from judgement, which is the point.
- **Drift.** The real node at 50 ops/s showed its p99 moving four buckets in three minutes with no change made.
  A real drift upwards in the observation window would roll back a correct action whatever the floor; that is the
  guard working as designed, but it means the residual false alarm rate on a real node is not zero above the floor.
- **One node, one workload.** The real-node check used in-JVM writes on a small shared machine, four minutes per
  rate; its tail estimates rest on 400 to 9500 operations.
- **Bucket steps.** At high rates a slowdown of less than about 25% can be invisible and one of 44 to 50% can read
  as just under the abort threshold. Not a floor question, but it bounds what the guard can see.
- **Table guard.** `table.read_latency_p99_micros` uses the same reservoir and sensor timing, so the same numbers
  apply; its rate is per table, so a table guard reaches the floor less often than the node-wide client guards.

## How to rerun

Each run must stay under the unit test timeout (8 minutes), so the shapes and baseline modes are split:

```
export ANT_ARGS=-Dno-build-accord=true SPIKE_GUARD_FLOOR=1 SPIKE_TRIALS=1000
SPIKE_SHAPES=lognormal SPIKE_MODES=PROBLEM_PERIOD .build/run-tests.sh -a test -t '/GuardTrafficFloorSpikeTest\.java$'
# likewise tight / bimodal, and FULL_WINDOW, MIN_SAMPLES, SCENARIO_17S; tables are appended to build/test/spike-guard-floor.txt
.build/run-tests.sh -a jvm-dtest -t '/GuardTrafficFloorRealNodeSpikeTest\.java$'   # about 13 minutes, writes build/test/spike-guard-floor-real.txt
SPIKE_GUARD_FLOOR_REAL=build/test/spike-guard-floor-real.txt .build/run-tests.sh -a test -t '/GuardTrafficFloorSpikeTest\.java$'
```

The trials are seeded, so the synthetic tables are reproduced exactly. The real-node numbers vary from run to run.
