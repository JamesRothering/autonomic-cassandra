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
package org.apache.cassandra.autonomic.plan.policies;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.analyze.detectors.CompactionBacklogDetector;
import org.apache.cassandra.autonomic.analyze.detectors.DiskPressureDetector;
import org.apache.cassandra.autonomic.analyze.detectors.GcPressureDetector;
import org.apache.cassandra.autonomic.analyze.detectors.HotPartitionDetector;
import org.apache.cassandra.autonomic.analyze.detectors.PoolSaturationDetector;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.actuators.PartitionSplitActuator;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.knowledge.Series;
import org.apache.cassandra.autonomic.layout.Eligibility;
import org.apache.cassandra.autonomic.layout.KeyState;
import org.apache.cassandra.autonomic.layout.LayoutAnnotations;
import org.apache.cassandra.autonomic.layout.LayoutProfiler;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.PartitionProbe;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.SplitEvaluation;
import org.apache.cassandra.autonomic.layout.TableLayout;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.autonomic.plan.Policy;
import org.apache.cassandra.autonomic.plan.VerifySpec;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

/**
 * {@code hot_partition}: propose splitting the partition into buckets of an overflow table (adaptive-schema design
 * 7.1 and 7.2). The policy keeps a little state per partition because the qualification takes minutes: the diagnosis
 * must have been open for {@code layout.for_evaluations} evaluations, on the preferred proposer; the row-count probe
 * runs on a background thread; then the shape profile is taken for {@code layout.profile_window}. Each failure ends
 * with no plan and a note on the diagnosis ({@link LayoutAnnotations}, shown as evidence {@code layout}):
 * {@code not_applicable(<reason>)} with the reasons {@code excluded}, {@code overflow_table}, {@code blocked(...)},
 * {@code lwt}, {@code in_layout}, {@code registry_full}, {@code cooldown}, {@code saturated}, {@code node_down},
 * {@code topology}, {@code small}, {@code too_large}, {@code insufficient_profile}, {@code fan_out}, {@code no_spread},
 * {@code backfill_too_long}, {@code disk} and {@code probe_failed}.
 *
 * <p>The key is rebuilt from the diagnosis's evidence {@code key_hex} (FC6), never from the subject's detail. The
 * plan is cluster scoped: the governor applies the ledger, rate and approval rules (manual by default), and its
 * {@link PartitionSplitActuator#validate} runs the same {@link Eligibility} as {@code nodetool autonomic layout split}.
 * Whether the actuator is enabled, {@code dry_run} and whether cluster actions are enabled are the governor's.
 *
 * <p>Deviation from design 7.2: the checks of step 7 that cost nothing (headroom, cooldown) run before the probe, which
 * reads the partition and must not be started on a system that is already saturated.
 */
public class PartitionSplitPolicy implements Policy
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionSplitPolicy.class);

    public static final String NAME = "partition_split";
    public static final int MIN_BUCKETS = 4;
    public static final int MAX_BUCKETS = 32;
    /** An estimate at or above this is a large partition: a split bounds its size even where it moves no load. */
    public static final long LARGE_PARTITION_BYTES = 100L * 1024 * 1024;
    /** ADR-043: the cheap size undercounts the probe's byte estimate (in-memory units) by up to this factor, for narrow rows. */
    public static final double SIZE_UNDERCOUNT = 2.5;
    /** ADR-043: fewest serialized bytes a row can take on disk (flags, a clustering value, one cell); the spike's narrowest rows took 21.5. */
    public static final long MIN_ROW_DISK_BYTES = 8;
    /** ADR-043: after a probe that did not qualify, the next one waits until the cheap size is this many times the size at that probe. */
    public static final double REPROBE_GROWTH = 2.0;
    static final long LOOKBACK_MILLIS = 24L * 3_600_000;
    private static final long STATE_TTL_MILLIS = 3_600_000;
    private static final Set<String> SATURATION = Set.of(PoolSaturationDetector.NAME, CompactionBacklogDetector.NAME,
                                                         DiskPressureDetector.NAME, GcPressureDetector.NAME);

    /** What the policy needs from the node; replaced in unit tests. */
    public interface Env
    {
        ClusterMetadata metadata();

        /** True if this node is the first live node of the directory (the proposer of cluster actions). */
        boolean preferredProposer();

        /** Directory members that gossip does not consider alive. */
        Collection<NodeId> membersNotAlive();

        /** Starts the row-count probe of 7.2 steps 2 and 6; not on the calling thread. */
        Future<PartitionProbe.Result> probe(TableMetadata table, ByteBuffer key, long maxRows);

        /** The shape profile of the key, or null if it is not watched. */
        default LayoutProfiler.Shape profile(TableMetadata table, ByteBuffer key)
        {
            LayoutProfiler.Watch watch = LayoutProfiler.get(table.id, key);
            return watch == null ? null : watch.shape();
        }

        /** Starts counting the shape of the key's operations until {@code windowMillis} from now. */
        default void watch(TableMetadata table, ByteBuffer key, long windowMillis)
        {
            LayoutProfiler.watch(table.id, key, windowMillis);
        }

        default void unwatch(TableMetadata table, ByteBuffer key)
        {
            LayoutProfiler.unwatch(table.id, key);
        }

        /** Paxos operations on the table on this node since it started. */
        default long paxosOperations(TableMetadata table)
        {
            return LayoutProfiler.paxosOperations(table);
        }

        long writeTimeoutMillis();
    }

    private static final class State
    {
        int evaluations;
        long lastSeenMillis;
        Future<PartitionProbe.Result> probe;
        PartitionProbe.Result probed;
        long profileStartMillis;
        /** Cheap size when the probe was started; NaN if there was no size signal. */
        double sizeAtProbe = Double.NaN;
        long probeStartMillis;
        /** The last result of the probe was refused for a reason that depends on the partition's size. */
        boolean sizeRefused;
        /** The reason of that refusal. */
        String refusal = "small_by_size";
    }

    private final AutonomicConfig config;
    private final Env env;
    private final Map<Subject, State> states = new HashMap<>();

    public PartitionSplitPolicy(AutonomicConfig config, Env env)
    {
        this.config = config;
        this.env = env;
    }

    /** The policy of a node: the coordinator decides who proposes, and probes run on a thread that exists only while one runs. */
    public static PartitionSplitPolicy create(AutonomicConfig config, ClusterCoordinator coordinator)
    {
        return new PartitionSplitPolicy(config, new NodeEnv(coordinator));
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<String> handles()
    {
        return Set.of(HotPartitionDetector.NAME, PartitionSplitActuator.ORPHANED, PartitionSplitActuator.RECLAIM_DUE);
    }

    @Override
    public Set<SignalId> inputs()
    {
        return Set.of(Signals.PARTITION_READ_SHARE, Signals.PARTITION_WRITE_SHARE, Signals.PARTITION_SIZE_BYTES,
                      Signals.DISK_FREE_BYTES, Signals.TABLE_COORDINATOR_READ_LATENCY_P99_MICROS);
    }

    /** Split, resume and reclaim are all actions of the split actuator. */
    @Override
    public Set<String> actuations()
    {
        return Set.of(Policy.actuation(PartitionSplitActuator.NAME, Policy.APPLY));
    }

    /** The number of buckets of design 7.2 step 4: {@code clamp(nextPowerOfTwo(ceil(share / target_share)), 4, 32)}. */
    public static int buckets(double share, double targetShare, int defaultBuckets)
    {
        if (Double.isNaN(share) || share <= 0 || targetShare <= 0)
            return defaultBuckets;
        double wanted = Math.ceil(share / targetShare);
        long n = wanted >= MAX_BUCKETS ? MAX_BUCKETS : (long) wanted;
        long pow = 1;
        while (pow < n)
            pow <<= 1;
        return (int) Math.max(MIN_BUCKETS, Math.min(MAX_BUCKETS, pow));
    }

    /** Design 7.2 step 5: some datacenter has more nodes than its replication factor, so the buckets land on different nodes. */
    public static boolean spreadsLoad(ClusterMetadata cm, KeyspaceMetadata keyspace)
    {
        Map<String, Integer> nodes = new HashMap<>();
        for (NodeId id : cm.directory.peerIds())
            if (cm.directory.peerState(id) == NodeState.JOINED)
                nodes.merge(cm.directory.location(id).datacenter, 1, Integer::sum);
        if (keyspace.replicationStrategy instanceof NetworkTopologyStrategy)
        {
            NetworkTopologyStrategy nts = (NetworkTopologyStrategy) keyspace.replicationStrategy;
            for (String dc : nts.getDatacenters())
                if (nodes.getOrDefault(dc, 0) > nts.getReplicationFactor(dc).allReplicas)
                    return true;
            return false;
        }
        int total = nodes.values().stream().mapToInt(Integer::intValue).sum();
        return total > keyspace.replicationStrategy.getReplicationFactor().allReplicas;
    }

    @Override
    public Optional<Plan> plan(Diagnosis diagnosis, KnowledgeView knowledge)
    {
        if (PartitionSplitActuator.ORPHANED.equals(diagnosis.type))
            return adoption(diagnosis, knowledge);
        if (PartitionSplitActuator.RECLAIM_DUE.equals(diagnosis.type))
            return reclaim(diagnosis, knowledge);
        long now = knowledge.nowMillis();
        states.values().removeIf(s -> now - s.lastSeenMillis > STATE_TTL_MILLIS);
        AutonomicConfig.LayoutConfig lc = config.layout;
        Subject subject = diagnosis.subject;
        State st = states.computeIfAbsent(subject, s -> new State());
        st.lastSeenMillis = now;
        if (++st.evaluations < lc.for_evaluations || !env.preferredProposer())
            return Optional.empty();

        String keyHex = diagnosis.evidence.get("key_hex");
        ByteBuffer key;
        try
        {
            key = ByteBufferUtil.hexToBytes(keyHex);
        }
        catch (RuntimeException e)
        {
            return no(subject, "invalid_key");
        }
        ClusterMetadata cm = env.metadata();
        KeyspaceMetadata ksm = cm.schema.getKeyspaces().getNullable(subject.keyspace);
        TableMetadata table = ksm == null ? null : ksm.tables.getNullable(subject.table);
        if (table == null)
            return no(subject, "table_dropped");
        Subject target = PartitionSplitActuator.target(table.keyspace, table.name, ByteBufferUtil.bytesToHex(key));

        if (busy(cm, knowledge, target))
            return Optional.empty();
        // a pending unsplit takes the lane before a new split (T6.17g, design 11.8)
        for (Diagnosis pending : knowledge.activeDiagnoses())
            if ("layout.unsplit_pending".equals(pending.type))
                return Optional.empty();

        // 1. eligibility: the same checks as the operator's split
        LayoutRegistry registry = LayoutRegistry.of(cm);
        TableLayout layout = registry.layout(table.id);
        if (lc.excluded_tables != null && lc.excluded_tables.contains(table.keyspace + '.' + table.name))
            return no(subject, "excluded");
        if (registry.byLogical.values().stream().anyMatch(l -> l.physical.equals(table.id)))
            return no(subject, "overflow_table");
        List<Eligibility.Blocker> blockers = Eligibility.structural(table, ksm);
        if (!blockers.isEmpty())
            return no(subject, "blocked(" + blockers.toString().replaceAll("[\\[\\] ]", "") + ')');
        if (layout != null && layout.keyState(key) != null)
            return no(subject, "in_layout");
        if ((layout == null && registry.byLogical.size() >= LayoutRegistry.MAX_LAYOUTS) || (layout != null && layout.keys.size() >= LayoutRegistry.MAX_KEYS))
            return no(subject, "registry_full");
        LayoutProfiler.Shape profile = env.profile(table, key);
        if (Eligibility.runtime(env.paxosOperations(table), profile == null ? 0 : profile.cas) != null)
            return no(subject, "lwt");

        // 7. headroom, early: the probe below reads the partition
        for (Diagnosis d : knowledge.activeDiagnoses())
            if (d.severity == Severity.CRITICAL && SATURATION.contains(d.type))
                return no(subject, "saturated");
        if (!env.membersNotAlive().isEmpty())
            return no(subject, "node_down");
        if (!cm.inProgressSequences.isEmpty())
            return no(subject, "topology");
        if (coolingDown(knowledge, target))
            return no(subject, "cooldown");

        // 2. row-count probe, run only when the cheap size makes a qualifying result plausible (ADR-043)
        OptionalDouble cheap = cheapSize(knowledge, subject);
        double trigger = probeTrigger(lc, spreadsLoad(cm, ksm));
        if (st.probed != null && st.sizeRefused && reprobeDue(st, cheap, trigger, lc, now))
        {
            st.probed = null;
            st.probe = null;
            st.sizeRefused = false;
        }
        if (st.probed == null)
        {
            if (st.probe == null)
            {
                if (cheap.isPresent() && cheap.getAsDouble() < nextProbeSize(st, trigger))
                    return no(subject, st.refusal + sizeDetail(st, cheap, trigger));
                st.sizeAtProbe = cheap.isPresent() ? cheap.getAsDouble() : Double.NaN;
                st.probeStartMillis = now;
                st.probe = env.probe(table, key, lc.max_probe_rows);
                LayoutAnnotations.set(subject, "probing");
                return Optional.empty();
            }
            if (!st.probe.isDone())
                return Optional.empty();
            try
            {
                st.probed = st.probe.get();
            }
            catch (Exception e)
            {
                logger.info("Autonomic partition split: the row-count probe of {} failed: {}", target, e.toString());
                st.probe = null;
                st.evaluations = 0;
                return no(subject, "probe_failed");
            }
        }
        PartitionProbe.Result probed = st.probed;
        if (probed.rows < lc.min_rows_for_split)
            return sizeRefusal(st, subject, "small", cheap, trigger);
        if (probed.hitMax)
            return sizeRefusal(st, subject, "too_large", cheap, trigger);
        st.sizeRefused = false;

        // 3. shape profile
        long window = lc.profile_window.toMilliseconds();
        if (profile == null || st.profileStartMillis == 0)
        {
            env.unwatch(table, key);
            // twice the window: a watch is swept when it expires, and the counters are read after one window
            env.watch(table, key, 2 * window);
            st.profileStartMillis = now;
            LayoutAnnotations.set(subject, "profiling");
            return Optional.empty();
        }
        if (now - st.profileStartMillis < window)
            return Optional.empty();
        env.unwatch(table, key);
        st.profileStartMillis = 0;
        if (profile.operations() < lc.min_samples)
        {
            st.evaluations = 0;
            return no(subject, "insufficient_profile");
        }
        if (Eligibility.runtime(env.paxosOperations(table), profile.cas) != null)
            return no(subject, "lwt");

        // 4. benefit
        double p = profile.pointFraction();
        if (p < lc.min_point_op_fraction)
            return no(subject, "fan_out");
        OptionalDouble read = knowledge.latest(Signals.PARTITION_READ_SHARE, subject);
        OptionalDouble write = knowledge.latest(Signals.PARTITION_WRITE_SHARE, subject);
        double share = Math.max(read.orElse(Double.NaN), write.orElse(Double.NaN));
        if (Double.isNaN(share))
            share = read.isPresent() ? read.getAsDouble() : write.orElse(Double.NaN);
        int buckets = layout != null ? layout.buckets : buckets(share, lc.target_share, lc.default_buckets);

        // 5. spread
        boolean sizeOnly = false;
        if (!spreadsLoad(cm, ksm))
        {
            if (probed.estimatedBytes() < LARGE_PARTITION_BYTES)
                return sizeRefusal(st, subject, "no_spread", cheap, trigger);
            sizeOnly = true;
        }

        // 6. cost
        long bytes = probed.estimatedBytes();
        int rf = ksm.replicationStrategy.getReplicationFactor().allReplicas;
        double throughput = lc.backfill_throughput.toBytesPerSecond();
        long deadline = PartitionSplitActuator.backfillWindowMillis(lc, table.params.gcGraceSeconds);
        if (throughput > 0 && bytes * rf * 1000.0 / throughput > deadline)
            return sizeRefusal(st, subject, "backfill_too_long", cheap, trigger);
        OptionalDouble free = knowledge.latest(Signals.DISK_FREE_BYTES, Subject.node());
        if (free.isPresent() && free.getAsDouble() < 2.0 * bytes)
            return sizeRefusal(st, subject, "disk", cheap, trigger);

        LayoutAnnotations.set(subject, "proposed(buckets=" + buckets + ')');
        return Optional.of(plan(diagnosis, target, table, buckets, profile, p, share, sizeOnly, probed));
    }

    /**
     * ADR-043: the cheap size from which a probe can qualify. It needs {@code min_rows_for_split} rows, and a row takes at
     * least {@link #MIN_ROW_DISK_BYTES} on disk; where the buckets would land on the same nodes it also needs
     * {@link #LARGE_PARTITION_BYTES} of the probe's units, which the cheap size reaches at that over {@link #SIZE_UNDERCOUNT}.
     */
    static double probeTrigger(AutonomicConfig.LayoutConfig lc, boolean spreadsLoad)
    {
        double byRows = (double) lc.min_rows_for_split * MIN_ROW_DISK_BYTES;
        return spreadsLoad ? byRows : Math.max(byRows, LARGE_PARTITION_BYTES / SIZE_UNDERCOUNT);
    }

    /** The smallest cheap size at which the next probe is started: the trigger, and double the size at the last probe. */
    private static double nextProbeSize(State st, double trigger)
    {
        return Double.isNaN(st.sizeAtProbe) ? trigger : Math.max(trigger, REPROBE_GROWTH * st.sizeAtProbe);
    }

    /** With a size signal: it has grown enough. Without one: {@code reprobe_interval} has passed since the last probe. */
    private static boolean reprobeDue(State st, OptionalDouble cheap, double trigger, AutonomicConfig.LayoutConfig lc, long now)
    {
        if (cheap.isPresent())
            return cheap.getAsDouble() >= nextProbeSize(st, trigger);
        long interval = lc.reprobe_interval.toMilliseconds();
        return interval > 0 && now - st.probeStartMillis >= interval;
    }

    /** The newest {@code partition.size_bytes} of the key, if a sensor reported it recently enough. */
    private OptionalDouble cheapSize(KnowledgeView knowledge, Subject subject)
    {
        AutonomicConfig.SamplingConfig sc = config.sampling;
        long maxAge = 3 * (sc.interval.toMilliseconds() + sc.window.toMilliseconds());
        Series series = knowledge.series(Signals.PARTITION_SIZE_BYTES, subject, maxAge);
        return series.isEmpty() ? OptionalDouble.empty() : OptionalDouble.of(series.values[series.size() - 1]);
    }

    private Optional<Plan> sizeRefusal(State st, Subject subject, String reason, OptionalDouble cheap, double trigger)
    {
        st.sizeRefused = true;
        st.refusal = reason;
        return no(subject, reason + sizeDetail(st, cheap, trigger));
    }

    /** What the operator needs to see why nothing happens: the sizes; empty without a size signal. */
    private String sizeDetail(State st, OptionalDouble cheap, double trigger)
    {
        if (cheap.isPresent())
        {
            StringBuilder sb = new StringBuilder();
            if (!Double.isNaN(st.sizeAtProbe))
                sb.append(",size_at_probe=").append((long) st.sizeAtProbe);
            return sb.append(",size_now=").append((long) cheap.getAsDouble())
                     .append(",probe_at=").append((long) Math.ceil(nextProbeSize(st, trigger))).toString();
        }
        // no size signal: the periodic fallback, which needs no detail
        return "";
    }

    private Plan plan(Diagnosis diagnosis, Subject target, TableMetadata table, int buckets, LayoutProfiler.Shape profile, double p,
                      double share, boolean sizeOnly, PartitionProbe.Result probed)
    {
        Map<String, String> params = PartitionSplitActuator.params(buckets, PartitionSplitActuator.MODE_SPLIT);
        params.putAll(SplitEvaluation.Baseline.of(profile, share, sizeOnly).toParams());
        VerifySpec verify = new VerifySpec(Signals.TABLE_COORDINATOR_READ_LATENCY_P99_MICROS, Subject.table(table.keyspace, table.name), true, 0.0,
                                           List.of(), 0, 0);
        Action action = new Action(TimeUUID.Generator.nextTimeUUID().asUUID(), PartitionSplitActuator.NAME, ActionScope.CLUSTER, target, params, verify);
        String rationale = String.format(Locale.ROOT, "%s takes %s of the operations of %s.%s; profile %s over %s; split into %d buckets"
                                         + " (about %d rows, about %d bytes, estimates)%s",
                                         target.detail, SplitEvaluation.format(share), table.keyspace, table.name, profile,
                                         config.layout.profile_window, buckets, probed.rows, probed.estimatedBytes(),
                                         sizeOnly ? "; expected_benefit=partition_size_only" : "");
        return new Plan(UUID.randomUUID(), diagnosis.id, NAME, List.of(action), rationale);
    }

    /**
     * {@code layout.orphaned} (T6.9 review): the preferred proposer proposes {@code layout resume} of the key, which
     * adopts it through {@code AdoptKey} and finishes its cleanup. The governor judges it like any other proposal
     * (approval manual by default); the actuator declares it a continuation, exempt from the cooldown its stopped
     * predecessor left. Nothing while the lane is claimed, a split action is live here, a member is down (a claim of
     * the lane needs them all) or the replicas' limit has passed (resume refuses that key).
     */
    private Optional<Plan> adoption(Diagnosis diagnosis, KnowledgeView knowledge)
    {
        if (!env.preferredProposer())
            return Optional.empty();
        Subject subject = diagnosis.subject;
        String keyHex = diagnosis.evidence.get("key_hex");
        if (keyHex == null || "true".equals(diagnosis.evidence.get("past_tombstone_deadline")))
            return Optional.empty();
        ClusterMetadata cm = env.metadata();
        TableMetadata table = cm.schema.getTableMetadata(subject.keyspace, subject.table);
        if (table == null || busy(cm, knowledge, null) || !env.membersNotAlive().isEmpty() || !cm.inProgressSequences.isEmpty())
            return Optional.empty();
        Subject target = PartitionSplitActuator.target(table.keyspace, table.name, keyHex);
        VerifySpec verify = new VerifySpec(Signals.TABLE_COORDINATOR_READ_LATENCY_P99_MICROS, Subject.table(table.keyspace, table.name), true, 0.0,
                                           List.of(), 0, 0);
        Action action = new Action(TimeUUID.Generator.nextTimeUUID().asUUID(), PartitionSplitActuator.NAME, ActionScope.CLUSTER, target,
                                   PartitionSplitActuator.params(0, PartitionSplitActuator.MODE_RESUME), verify);
        LayoutAnnotations.set(subject, "proposed(adoption)");
        return Optional.of(new Plan(UUID.randomUUID(), diagnosis.id, NAME, List.of(action),
                                    "partition " + keyHex + " of " + table.keyspace + '.' + table.name + " is past its commit point in "
                                    + diagnosis.evidence.get("phase") + " with no owner: adopt it and finish its cleanup"));
    }

    /**
     * {@code layout.reclaim_due} (ADR-036): the preferred proposer proposes the {@code reclaim} of the key, which
     * writes its base partition tombstone. Not while an action holds the lane, a member is down, or a metadata operation
     * runs; and only if the registry still shows a released {@code STEADY} key in {@code RESIDUE}, due. A continuation
     * of the split, so it is exempt from the cooldown.
     */
    private Optional<Plan> reclaim(Diagnosis diagnosis, KnowledgeView knowledge)
    {
        if (!env.preferredProposer())
            return Optional.empty();
        Subject subject = diagnosis.subject;
        String keyHex = diagnosis.evidence.get("key_hex");
        if (keyHex == null)
            return Optional.empty();
        ClusterMetadata cm = env.metadata();
        TableMetadata table = cm.schema.getTableMetadata(subject.keyspace, subject.table);
        if (table == null || busy(cm, knowledge, null) || !env.membersNotAlive().isEmpty() || !cm.inProgressSequences.isEmpty())
            return Optional.empty();
        TableLayout layout = LayoutRegistry.of(cm).layout(table.id);
        KeyState state;
        try
        {
            state = layout == null ? null : layout.keyState(ByteBufferUtil.hexToBytes(keyHex));
        }
        catch (RuntimeException e)
        {
            return Optional.empty();
        }
        if (state == null || state.phase != Phase.STEADY || state.actionId != null || state.baseState != KeyState.BaseState.RESIDUE
            || knowledge.nowMillis() < PartitionSplitActuator.reclaimDueMillis(state, table, config.layout))
            return Optional.empty();
        Subject target = PartitionSplitActuator.target(table.keyspace, table.name, keyHex);
        VerifySpec verify = new VerifySpec(Signals.TABLE_COORDINATOR_READ_LATENCY_P99_MICROS, Subject.table(table.keyspace, table.name), true, 0.0,
                                           List.of(), 0, 0);
        Action action = new Action(TimeUUID.Generator.nextTimeUUID().asUUID(), PartitionSplitActuator.NAME, ActionScope.CLUSTER, target,
                                   PartitionSplitActuator.params(0, PartitionSplitActuator.MODE_RECLAIM), verify);
        LayoutAnnotations.set(subject, "proposed(reclaim)");
        return Optional.of(new Plan(UUID.randomUUID(), diagnosis.id, NAME, List.of(action),
                                    "partition " + keyHex + " of " + table.keyspace + '.' + table.name
                                    + " is split and released and its reclaim is due: write the tombstone of its base residue"));
    }

    private Optional<Plan> no(Subject subject, String reason)
    {
        LayoutAnnotations.set(subject, LayoutAnnotations.notApplicable(reason));
        return Optional.empty();
    }

    /** One migration at a time, and none while an action of this key is proposed or running: nothing to say about it. */
    private static boolean busy(ClusterMetadata cm, KnowledgeView knowledge, Subject target)
    {
        if (knowledge.ledger().claims.containsKey(Lane.SCHEMA_LAYOUT))
            return true;
        for (ActionRecord r : knowledge.recentActions(LOOKBACK_MILLIS))
            if (r.action.actuator.equals(PartitionSplitActuator.NAME) && !r.state.isTerminal())
                return true;
        return false;
    }

    /** Design 7.2 step 7: after each abort of the key the wait doubles, from {@code cooldown_after_rollback} up to {@code cooldown_max}. */
    private boolean coolingDown(KnowledgeView knowledge, Subject target)
    {
        long now = knowledge.nowMillis();
        long base = config.safety.cooldown_after_rollback.toMilliseconds();
        long max = config.safety.cooldown_max.toMilliseconds();
        int aborts = 0;
        long last = 0;
        for (ActionRecord r : knowledge.recentActions(LOOKBACK_MILLIS))
            if (r.action.actuator.equals(PartitionSplitActuator.NAME) && r.action.target.equals(target)
                && (r.state == ActionState.ROLLED_BACK || r.state == ActionState.FAILED))
            {
                aborts++;
                last = Math.max(last, r.updatedMillis);
            }
        if (aborts == 0)
            return false;
        long wait = Math.min(max, base << Math.min(aborts - 1, 20));
        return now < last + wait;
    }

    /** Production: probes run on a thread that exists only while one runs. */
    static final class NodeEnv implements Env
    {
        private final ClusterCoordinator coordinator;

        NodeEnv(ClusterCoordinator coordinator)
        {
            this.coordinator = coordinator;
        }

        @Override
        public ClusterMetadata metadata()
        {
            return ClusterMetadata.current();
        }

        @Override
        public boolean preferredProposer()
        {
            return coordinator.preferredProposer();
        }

        @Override
        public Collection<NodeId> membersNotAlive()
        {
            return new ArrayList<>(coordinator.membersNotAlive());
        }

        @Override
        public Future<PartitionProbe.Result> probe(TableMetadata table, ByteBuffer key, long maxRows)
        {
            // a thread of its own that ends with the probe: a pool would keep an idle thread that nothing stops with the layer
            AsyncPromise<PartitionProbe.Result> result = new AsyncPromise<>();
            executorFactory().startThread("AutonomicLayoutProbe", () -> {
                try
                {
                    result.setSuccess(PartitionProbe.run(table, key, maxRows));
                }
                catch (Throwable t)
                {
                    result.tryFailure(t);
                }
            });
            return result;
        }

        @Override
        public long writeTimeoutMillis()
        {
            return org.apache.cassandra.config.DatabaseDescriptor.getWriteRpcTimeout(TimeUnit.MILLISECONDS);
        }
    }
}
