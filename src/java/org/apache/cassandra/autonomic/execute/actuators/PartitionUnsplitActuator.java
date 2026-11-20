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
package org.apache.cassandra.autonomic.execute.actuators;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator;
import org.apache.cassandra.autonomic.coord.ClusterLedger;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.execute.ActionContext;
import org.apache.cassandra.autonomic.execute.ClusterActionExecution;
import org.apache.cassandra.autonomic.execute.ClusterActuator;
import org.apache.cassandra.autonomic.execute.StepResult;
import org.apache.cassandra.autonomic.execute.Verdict;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.layout.AdoptKey;
import org.apache.cassandra.autonomic.layout.BasePurge;
import org.apache.cassandra.autonomic.layout.BeginUnsplit;
import org.apache.cassandra.autonomic.layout.EpochWatermarks;
import org.apache.cassandra.autonomic.layout.KeyState;
import org.apache.cassandra.autonomic.layout.LayoutEvents;
import org.apache.cassandra.autonomic.layout.LayoutOwnerSteps;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.LayoutVerifier;
import org.apache.cassandra.autonomic.layout.LayoutWrites;
import org.apache.cassandra.autonomic.layout.NaturalReplicas;
import org.apache.cassandra.autonomic.layout.PartitionProbe;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.ReleaseKey;
import org.apache.cassandra.autonomic.layout.RemoveKey;
import org.apache.cassandra.autonomic.layout.RowMapper;
import org.apache.cassandra.autonomic.layout.SplitEvaluation;
import org.apache.cassandra.autonomic.layout.TableLayout;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;

/**
 * The owner of a reverse migration ("unsplit", adaptive-schema design 11.4, ADR-036, T6.17f): moves a split key back to
 * the base table. Lane {@link Lane#SCHEMA_LAYOUT}, irreversible (the reverse commit point is {@code BASE_ONLY}), manual
 * approval. Target {@code PARTITION:<ks>.<table>:<key hex>}; param {@code mode}: {@value #MODE_AUTO} (the default),
 * {@value #MODE_FAST}, {@value #MODE_SLOW}, or {@value #MODE_RESUME} for a reverse key whose action holds no claim.
 *
 * <p><b>Mode</b> ({@link #validate}, design 11.5). Fast needs base state {@code RESIDUE} and
 * {@code now + estimate} before the reverse commit window end, the estimate being the copy (bytes x RF / backfill
 * throughput) plus {@code cutover_soak}. Slow needs {@code TOMBSTONED}, and is offered from {@code Tt + gc_grace}. A
 * rejection gives the earliest time a mode would be allowed. From {@code NEW_ONLY} or {@code CLEANUP} (owner answer 2)
 * the forward action, which holds no claim, hands the key over through {@code previousActionId}. A key stuck in
 * {@code CLEANUP} at {@code RECONCILE_PAST_DEADLINE} whose fast window is closed is unsplit in slow mode after this
 * action writes the base partition tombstone itself, once the forward replicas' limit plus {@code straggler_window}
 * has passed (owner answer 6; claim phase {@value #TOMBSTONING}).
 *
 * <p><b>Phases</b> (every wait is a STAY, every transition obeys rule O1 as in the split):
 * <ul>
 * <li>{@code REVERSE_ANNOUNCED}: acknowledgement; with signals, a baseline of the key over {@code baseline_window}.
 *     Slow mode: every natural base replica of K reports a clean purge for the generation (a placement change starts
 *     a new one); after {@code purge_timeout}, counted from the acknowledgement or from {@code Tt + gc_grace} if later,
 *     abort with {@value #UNSPLIT_RESIDUE}, carrying each node's published {@link BasePurge} reason and a remedy.</li>
 * <li>{@code REVERSE_DUAL_WRITE}: acknowledgement.</li>
 * <li>{@code REVERSE_BACKFILL}: the generation inherited from {@code BeginUnsplit} (whose reports are the purge
 *     reports) is never counted: the owner starts a new generation on entry, and replicas run only a generation
 *     started in the phase ({@link KeyState#reverseBackfillDue}). Complete when every natural replica of every bucket
 *     token of the incarnation reported it and no bucket token's placement changed since it started.</li>
 * <li>{@code REVERSE_VERIFY}: {@link LayoutVerifier} in {@link LayoutVerifier.Direction#REVERSE}; CONTENT_DIFFERS and
 *     MISSING_FROM_BASE fail (one re-backfill, {@code verify_retries}, then abort), MISSING_FROM_OVERFLOW is tolerated
 *     and counted.</li>
 * <li>{@code CUTBACK}: acknowledgement, {@code cutover_soak} with the C1 guard against the baseline, the reverse commit
 *     window, and with {@code commit_approval: manual} a second approval (owner answer 5), then {@code BASE_ONLY}.</li>
 * <li>{@code BASE_ONLY}, the reverse commit point; then {@code REVERSE_CLEANUP}: {@code straggler_window}, the reconcile
 *     generation over the bucket replicas, {@code RemoveKey}.</li>
 * <li>{@value #KEY_REMOVED}: once every node acknowledged the removal (invariant I20) and the write timeout passed, the
 *     bucket partitions of the incarnation are deleted at ALL, or the overflow table is dropped if the layout is
 *     gone with its last key.</li>
 * </ul>
 *
 * <p><b>Deadlines</b> (11.5). The replicas' limit is {@link KeyState#reverseReplicaLimitMillis} (before
 * {@code REVERSE_DUAL_WRITE} of a fast reverse it is projected from {@code frozenSince}). Before the commit point the
 * owner aborts at the limit less {@code straggler_window}, or {@code max_migration_duration} after
 * {@code REVERSE_DUAL_WRITE}; {@code CUTBACK} rolls back after the reverse commit window end. After the commit point a
 * reconcile pass that cannot finish before the limit ends the action FAILED ({@value #RECONCILE_PAST_DEADLINE}).
 *
 * <p><b>Abort and stop</b>. Before {@code BASE_ONLY}: {@code REVERSE_ANNOUNCED}, {@code REVERSE_DUAL_WRITE},
 * {@code REVERSE_BACKFILL} and {@code REVERSE_VERIFY} go to {@code STEADY}, {@code CUTBACK} through
 * {@code REVERSE_ROLLBACK}; the registry sets the base state (11.4); then, once every node acknowledged {@code STEADY},
 * {@code ReleaseKey} with that base state. A kill, a lost owner and a takeover take this path before the commit point
 * and continue forward after it (FC5); {@code BASE_ONLY} and {@code REVERSE_CLEANUP} are never aborted.
 */
public class PartitionUnsplitActuator implements ClusterActuator
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionUnsplitActuator.class);

    public static final String NAME = "partition_unsplit";
    public static final String PARAM_MODE = "mode";
    public static final String MODE_AUTO = "auto";
    public static final String MODE_FAST = "fast";
    public static final String MODE_SLOW = "slow";
    public static final String MODE_RESUME = "resume";

    /** The diagnosis of a slow reverse aborted on {@code purge_timeout}, and the start of its abort reason. */
    public static final String UNSPLIT_RESIDUE = "layout.unsplit_residue";
    /** An operator asked for an unsplit {@code --when-ready}; carries the time from which one is possible (T6.17g). */
    public static final String SPLIT_NOT_EFFECTIVE = "layout.split_not_effective";
    public static final String UNSPLIT_PENDING = "layout.unsplit_pending";
    /** A key in a reverse phase whose action holds no claim (T6.17g). */
    public static final String UNSPLIT_ORPHANED = "layout.unsplit_orphaned";
    /** Prefix of the node-local state of the requests of {@code layout unsplit --when-ready}: {@code <ks>.<table>/<key hex>} to the due time. */
    public static final String PENDING_PREFIX = "layout.unsplit_pending/";
    public static final String RECONCILE_PAST_DEADLINE = PartitionSplitActuator.RECONCILE_PAST_DEADLINE;

    // claim phases that are not phases of the key
    /** A stuck CLEANUP key: the base partition tombstone is written before {@code BeginUnsplit} (owner answer 6). */
    public static final String TOMBSTONING = "TOMBSTONING";
    public static final String ADOPTING = "ADOPTING";
    /** The claim state is written; {@code BeginUnsplit} follows (a commit does not change the claim state). */
    public static final String BEGINNING = "BEGINNING";
    public static final String KEY_REMOVED = PartitionSplitActuator.KEY_REMOVED;

    // claim state (at most Claim.MAX_STATE_ENTRIES)
    public static final String S_TABLE = "table";
    /** {@code <incarnation>/<buckets>/<overflow table id>}: what the bucket deletion needs after the key is gone. */
    public static final String S_KEY = "key";
    /** {@code FAST} or {@code SLOW}. */
    public static final String S_MODE = "mode";
    /** The forward action that handed a NEW_ONLY or CLEANUP key over. */
    public static final String S_PREVIOUS = "previous";
    /** {@code Tt} (microseconds) of the stuck key's base tombstone, written ahead of the tombstone. */
    public static final String S_TOMBSTONE = "tombstone";
    public static final String S_ACKED = LayoutOwnerSteps.S_ACKED;
    public static final String S_ABORT = "abort";
    public static final String S_VERIFIED = "verified";
    public static final String S_VERIFY_FAILED = "verify_failed";
    public static final String S_VERIFY_FAILURES = "verify_failures";
    /** MISSING_FROM_OVERFLOW rows tolerated by the verification that passed. */
    public static final String S_TOLERATED = "missing_from_overflow";
    public static final String S_COMMIT_APPROVED = "commit_approved";
    public static final String S_FINISHING = "finishing";
    /** An epoch at or after {@code RemoveKey}, recorded at the first step after it. */
    public static final String S_REMOVED = "removed";
    /** The key's baseline of REVERSE_ANNOUNCED, encoded, or {@value #NONE}. */
    public static final String S_BASELINE = "unsplit_baseline";
    static final String NONE = "none";

    static final int MAX_DELETE_ATTEMPTS = 3;

    private static final Set<Phase> PRE_COMMIT = EnumSet.of(Phase.REVERSE_ANNOUNCED, Phase.REVERSE_DUAL_WRITE, Phase.REVERSE_BACKFILL, Phase.REVERSE_VERIFY,
                                                            Phase.CUTBACK, Phase.REVERSE_ROLLBACK);

    /** What the actuator does to and reads from the cluster; replaced in unit tests. Actuator thread only. */
    public interface Env
    {
        ClusterMetadata metadata();

        /** Rule O1: every directory member that is not LEFT has reported an epoch at or after {@code epoch}. */
        boolean acknowledged(Epoch epoch);

        /** True if every directory member that is not LEFT is up (design 11.8). */
        boolean allAlive(ClusterMetadata metadata);

        void dropTable(String keyspace, String table) throws Exception;

        /** Deletes the {@code buckets} bucket partitions of {@code incarnation} of the key at ALL. */
        void deleteBuckets(TableMetadata logical, TableMetadata physical, int buckets, ByteBuffer key, int incarnation, long timestampMicros) throws Exception;

        /** Writes the partition tombstone of the key in the base table at ALL, bypassing the router. */
        void deleteBase(TableMetadata logical, ByteBuffer key, long timestampMicros) throws Exception;

        /** The natural replicas of the key in the base table. */
        Set<NodeId> baseReplicas(ClusterMetadata metadata, TableMetadata logical, ByteBuffer key);

        Epoch baseReplicasModified(ClusterMetadata metadata, TableMetadata logical, ByteBuffer key);

        /** The natural replicas of every bucket token of the incarnation; empty if any bucket token has none. */
        Set<NodeId> bucketReplicas(ClusterMetadata metadata, TableMetadata logical, TableMetadata physical, ByteBuffer key, int incarnation, int buckets);

        Epoch bucketReplicasModified(ClusterMetadata metadata, TableMetadata logical, TableMetadata physical, ByteBuffer key, int incarnation, int buckets);

        /** A reverse equivalence check of the key. */
        LayoutOwnerSteps.Check check(ClusterMetadata metadata, TableId logical, ByteBuffer key, int pageRows, long recheckDelayMillis, LongSupplier clock);

        /** The bytes of the bucket partitions of the incarnation, one copy; an estimate. */
        long estimatedBytes(TableMetadata logical, TableMetadata physical, ByteBuffer key, int incarnation, int buckets);

        /** What each of {@code nodes} published about its purge of the key (BasePurge), by node; absent if nothing. */
        Map<NodeId, BasePurge.Published> purgeReasons(ClusterMetadata metadata, TableId logical, ByteBuffer key, Set<NodeId> nodes);

        long writeTimeoutMillis();

        /** This node's wall clock, for {@link #validate}, which has no step context. */
        long nowMillis();

        /** The signals of this node; null where there are none, and the guard is then not judged. */
        default KnowledgeView knowledge()
        {
            return null;
        }
    }

    private final Env env;
    private final Supplier<AutonomicConfig> config;
    private String checkId;
    private LayoutOwnerSteps.Check check;
    private final Map<String, Integer> deleteAttempts = new LinkedHashMap<>();

    public PartitionUnsplitActuator(Env env, Supplier<AutonomicConfig> config)
    {
        this.env = env;
        this.config = config;
    }

    public static PartitionUnsplitActuator create(Supplier<AutonomicConfig> config)
    {
        return new PartitionUnsplitActuator(new NodeEnv(), config);
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Lane lane()
    {
        return Lane.SCHEMA_LAYOUT;
    }

    /** Always held for an operator's approval, whatever {@code cluster_approval} says (design 11.8, T6.17g). */
    @Override
    public boolean requiresManualApproval(Action action)
    {
        return true;
    }

    private static final java.util.regex.Pattern EARLIEST = java.util.regex.Pattern.compile("slow mode from (\\d+)");

    /**
     * The earliest time, in milliseconds, that a {@link #validate} rejection names ("slow mode from 1795052794028: ..."),
     * for {@code layout unsplit --when-ready}; empty if the rejection names none, which means waiting would not help.
     */
    public static java.util.OptionalLong earliestMillis(String rejection)
    {
        java.util.regex.Matcher m = EARLIEST.matcher(rejection == null ? "" : rejection);
        return m.find() ? java.util.OptionalLong.of(Long.parseLong(m.group(1))) : java.util.OptionalLong.empty();
    }

    /**
     * What a plan says of the action now: the mode it would start in and the estimate of a fast copy, or the rejection.
     * For {@code PartitionUnsplitPolicy}'s rationale.
     */
    public String modeNote(Action action)
    {
        PartitionSplitActuator.Target t = PartitionSplitActuator.Target.of(action.target);
        if (t.rejection != null)
            return t.rejection;
        String mode = action.params.getOrDefault(PARAM_MODE, MODE_AUTO);
        if (MODE_RESUME.equals(mode))
            return "resumes the reverse migration of an action that holds no claim";
        ClusterMetadata cm = env.metadata();
        TableMetadata logical = cm.schema.getTableMetadata(t.keyspace, t.table);
        TableLayout layout = logical == null ? null : LayoutRegistry.of(cm).layout(logical.id);
        KeyState key = layout == null ? null : layout.keyState(t.key);
        if (key == null)
            return "the key is not split";
        long now = env.nowMillis();
        Choice choice = choose(cm, logical, layout, t.key, key, mode, now);
        if (choice.rejection != null)
            return choice.rejection;
        if (choice.mode == KeyState.ReverseMode.SLOW)
            return "slow mode" + (choice.tombstoneFirst ? " (the base tombstone is written first)" : "");
        TableMetadata physical = cm.schema.getTableMetadata(layout.physical);
        return "fast mode, estimate " + estimateMillis(cm, logical, physical, t.key, key, layout.buckets) + " ms";
    }

    /**
     * Keys in a reverse phase whose action holds no claim and that no claim of the lane is adopting: the counterpart of
     * {@link PartitionSplitActuator#orphans}, proposed for {@code mode resume} (T6.17g).
     */
    public static java.util.List<PartitionSplitActuator.Orphan> orphans(ClusterMetadata cm)
    {
        java.util.List<PartitionSplitActuator.Orphan> out = new java.util.ArrayList<>();
        ClusterLedger ledger = ClusterLedger.of(cm);
        for (TableLayout layout : LayoutRegistry.of(cm).byLogical.values())
        {
            if (cm.schema.getTableMetadata(layout.logical) == null)
                continue;
            for (java.util.Map.Entry<ByteBuffer, KeyState> e : layout.keys.entrySet())
            {
                KeyState key = e.getValue();
                if (!key.phase.isReverse() || key.actionId == null || ledger.claim(key.actionId) != null)
                    continue;
                if (PartitionSplitActuator.claimOf(ledger, PartitionSplitActuator.target(layout.keyspace, layout.logicalName,
                                                                                         org.apache.cassandra.utils.ByteBufferUtil.bytesToHex(e.getKey()))) != null)
                    continue;
                out.add(new PartitionSplitActuator.Orphan(layout, e.getKey(), key, false));
            }
        }
        return out;
    }

    /** There is a commit point (BASE_ONLY) after which nothing is undone. */
    @Override
    public boolean reversible()
    {
        return false;
    }

    /** BASE_ONLY and REVERSE_CLEANUP: a stop leaves the key routed to the base copy, safe indefinitely (FC5). */
    @Override
    public boolean pastCommitPoint(Claim claim)
    {
        return Phase.BASE_ONLY.name().equals(claim.phase) || Phase.REVERSE_CLEANUP.name().equals(claim.phase);
    }

    public static Map<String, String> params(String mode)
    {
        Map<String, String> params = new LinkedHashMap<>();
        params.put(PARAM_MODE, mode);
        return params;
    }

    private AutonomicConfig.LayoutConfig lc()
    {
        return config.get().layout;
    }

    // ---------------------------------------------------------------------------------------------------------
    // validate and mode selection (design 11.5, 11.8)
    // ---------------------------------------------------------------------------------------------------------

    /** The mode a reverse can start in now, or why none can, with the earliest time one could. */
    static final class Choice
    {
        final KeyState.ReverseMode mode;
        /** The base tombstone has to be written first (owner answer 6). */
        final boolean tombstoneFirst;
        final String rejection;

        private Choice(KeyState.ReverseMode mode, boolean tombstoneFirst, String rejection)
        {
            this.mode = mode;
            this.tombstoneFirst = tombstoneFirst;
            this.rejection = rejection;
        }

        static Choice of(KeyState.ReverseMode mode, boolean tombstoneFirst)
        {
            return new Choice(mode, tombstoneFirst, null);
        }

        static Choice no(String why)
        {
            return new Choice(null, false, why);
        }
    }

    @Override
    public Optional<String> validate(Action action, KnowledgeView knowledge)
    {
        PartitionSplitActuator.Target t = PartitionSplitActuator.Target.of(action.target);
        if (t.rejection != null)
            return Optional.of(t.rejection);
        String mode = action.params.getOrDefault(PARAM_MODE, MODE_AUTO);
        if (!MODE_AUTO.equals(mode) && !MODE_FAST.equals(mode) && !MODE_SLOW.equals(mode) && !MODE_RESUME.equals(mode))
            return Optional.of("unknown mode " + PartitionSplitActuator.text(mode));
        ClusterMetadata cm = env.metadata();
        TableMetadata logical = cm.schema.getTableMetadata(t.keyspace, t.table);
        if (logical == null)
            return Optional.of("table " + t.keyspace + '.' + t.table + " does not exist");
        TableLayout layout = LayoutRegistry.of(cm).layout(logical.id);
        KeyState key = layout == null ? null : layout.keyState(t.key);
        if (key == null)
            return Optional.of("the key is not split: it is not in the layout of " + logical.keyspace + '.' + logical.name);
        if (MODE_RESUME.equals(mode))
            return validateResume(cm, key);
        if (!cm.inProgressSequences.isEmpty())
            return Optional.of("a metadata operation is in progress (design 4.7); try again when it is done");
        if (!env.allAlive(cm))
            return Optional.of("not every node of the cluster is up; an unsplit needs every node to acknowledge each phase");
        Choice choice = choose(cm, logical, layout, t.key, key, mode, env.nowMillis());
        return choice.rejection == null ? Optional.empty() : Optional.of(choice.rejection);
    }

    private Optional<String> validateResume(ClusterMetadata cm, KeyState key)
    {
        if (!key.phase.isReverse())
            return Optional.of("the key is in " + key.phase + ", not in a reverse phase; nothing to resume");
        if (key.actionId == null)
            return Optional.of("the key is in " + key.phase + " and owned by no action");
        if (ClusterLedger.of(cm).claim(key.actionId) != null)
            return Optional.of("the reverse migration of the key is running under action " + key.actionId);
        return Optional.empty();
    }

    /** Design 11.5: the mode {@code requested} ({@value #MODE_AUTO}: fast if allowed, else slow) if allowed now. */
    Choice choose(ClusterMetadata cm, TableMetadata logical, TableLayout layout, ByteBuffer keyBytes, KeyState key, String requested, long now)
    {
        boolean stuck = false;
        switch (key.phase)
        {
            case STEADY:
                if (key.actionId != null)
                    return Choice.no("the key is STEADY and still owned by action " + key.actionId + "; try again once it is released");
                break;
            case NEW_ONLY:
            case CLEANUP:
                // owner answer 2: the forward action hands the key over; it must have stopped
                if (key.actionId == null || ClusterLedger.of(cm).claim(key.actionId) != null)
                    return Choice.no("the split of the key is running under action " + key.actionId + " in " + key.phase
                                     + "; it hands the key over once it holds no claim");
                stuck = key.phase == Phase.CLEANUP && now >= forwardReplicaLimit(key, logical);
                break;
            default:
                return Choice.no("the key is in " + key.phase + "; an unsplit starts from STEADY, NEW_ONLY or CLEANUP");
        }
        TableMetadata physical = cm.schema.getTableMetadata(layout.physical);
        if (physical == null)
            return Choice.no("the overflow table of " + logical.keyspace + '.' + logical.name + " is missing");

        String fastNo = null;
        if (key.baseState != KeyState.BaseState.RESIDUE)
            fastNo = "fast mode needs the base residue, the base partition is " + key.baseState;
        else
        {
            long end = fastCommitWindowEnd(key, logical, physical, now);
            long estimate = estimateMillis(cm, logical, physical, keyBytes, key, layout.buckets);
            if (now + estimate >= end)
                fastNo = "fast mode closed: the copy and cutover_soak (estimate " + estimate + " ms) do not end before the reverse commit window end "
                         + end + " (now " + now + ')';
        }
        if (fastNo == null && !MODE_SLOW.equals(requested))
            return Choice.of(KeyState.ReverseMode.FAST, false);
        if (MODE_FAST.equals(requested))
            return Choice.no(fastNo + "; " + slowEarliest(key, logical, now, stuck));

        String slowNo = null;
        boolean tombstoneFirst = false;
        if (key.baseState == KeyState.BaseState.TOMBSTONED)
        {
            long from = slowFrom(key, logical);
            if (now < from)
                slowNo = "slow mode from " + from + ": the base tombstone at " + key.baseTombstoneMicros + " plus gc_grace_seconds " + logical.params.gcGraceSeconds;
        }
        else if (stuck)
        {
            long due = stuckTombstoneDue(key, logical);
            if (now < due)
                slowNo = "slow mode from " + due + ": the key is stuck in CLEANUP (" + RECONCILE_PAST_DEADLINE + "), and its base tombstone may be written once the "
                         + "replicas' limit " + forwardReplicaLimit(key, logical) + " plus straggler_window " + lc().straggler_window + " has passed (owner answer 6)";
            tombstoneFirst = true;
        }
        else
        {
            slowNo = "slow mode needs a base tombstone; " + slowEarliest(key, logical, now, false);
        }
        if (slowNo == null)
            return Choice.of(KeyState.ReverseMode.SLOW, tombstoneFirst);
        return Choice.no(MODE_SLOW.equals(requested) ? slowNo : (fastNo == null ? "" : fastNo + "; ") + slowNo);
    }

    /** For a rejection: when slow mode would be allowed. */
    private String slowEarliest(KeyState key, TableMetadata logical, long now, boolean stuck)
    {
        if (key.baseState == KeyState.BaseState.TOMBSTONED)
            return "slow mode from " + slowFrom(key, logical);
        if (stuck)
            return "slow mode from " + Math.max(now, stuckTombstoneDue(key, logical)) + ", after this action writes the base tombstone (owner answer 6)";
        if (key.phase == Phase.STEADY)
        {
            long reclaim = PartitionSplitActuator.reclaimDueMillis(key, logical, lc());
            return "slow mode from about " + (Math.max(now, reclaim) + logical.params.gcGraceSeconds * 1000L)
                   + ": the reclaim tombstone (due at " + reclaim + ") plus gc_grace_seconds " + logical.params.gcGraceSeconds;
        }
        return "slow mode only after the split completes and its base residue is reclaimed";
    }

    /** Slow mode: each replica purges after its deletion's local deletion time plus gc_grace; {@code Tt} stands for it. */
    private static long slowFrom(KeyState key, TableMetadata logical)
    {
        if (key.baseTombstoneMicros <= 0)
            return Long.MIN_VALUE;
        return key.baseTombstoneMicros / 1000 + logical.params.gcGraceSeconds * 1000L;
    }

    /** Owner answer 6: the forward replicas' limit plus {@code straggler_window}. */
    long stuckTombstoneDue(KeyState key, TableMetadata logical)
    {
        return forwardReplicaLimit(key, logical) + lc().straggler_window.toMilliseconds();
    }

    private long forwardReplicaLimit(KeyState key, TableMetadata logical)
    {
        long limit = LayoutOwnerSteps.forwardReplicaLimit(key, logical.params.gcGraceSeconds, env.writeTimeoutMillis());
        // a forward key without a dual write start has no limit to wait for: the base copy is the authoritative one
        return limit == Long.MIN_VALUE ? Long.MIN_VALUE : limit;
    }

    /** The smaller {@code gc_grace_seconds} of the two tables, as the replicas use ({@code BackfillTask}). */
    private static int gcGrace(TableMetadata logical, TableMetadata physical)
    {
        return physical == null ? logical.params.gcGraceSeconds : Math.min(logical.params.gcGraceSeconds, physical.params.gcGraceSeconds);
    }

    /** The fast replicas' limit of a reverse that would start now: {@code frozenSince + G - wt}. */
    private long projectedFastLimit(KeyState key, TableMetadata logical, TableMetadata physical, long now)
    {
        long frozen = key.frozenSinceMillis > 0 ? Math.min(key.frozenSinceMillis, now) : now;
        return frozen + gcGrace(logical, physical) * 1000L / 2 - env.writeTimeoutMillis();
    }

    private long fastCommitWindowEnd(KeyState key, TableMetadata logical, TableMetadata physical, long now)
    {
        return LayoutOwnerSteps.commitWindowEnd(projectedFastLimit(key, logical, physical, now), lc(), pollMillis());
    }

    /** Design 11.5: the backfill estimate (bytes x RF / throughput) plus {@code cutover_soak}. */
    private long estimateMillis(ClusterMetadata cm, TableMetadata logical, TableMetadata physical, ByteBuffer keyBytes, KeyState key, int buckets)
    {
        long bytes = env.estimatedBytes(logical, physical, keyBytes, key.incarnation, buckets);
        KeyspaceMetadata ksm = cm.schema.getKeyspaceMetadata(logical.keyspace);
        int rf = ksm == null ? 1 : Math.max(1, ksm.replicationStrategy.getReplicationFactor().allReplicas);
        double throughput = lc().backfill_throughput.toBytesPerSecond();
        long copy = throughput > 0 ? (long) Math.ceil(bytes * (double) rf * 1000.0 / throughput) : 0;
        return copy + lc().cutover_soak.toMilliseconds();
    }

    /** A resume of a key past the reverse commit point continues the committed migration. */
    @Override
    public boolean continuation(Action action)
    {
        if (!MODE_RESUME.equals(action.params.get(PARAM_MODE)))
            return false;
        PartitionSplitActuator.Target t = PartitionSplitActuator.Target.of(action.target);
        if (t.rejection != null)
            return false;
        ClusterMetadata cm = env.metadata();
        TableMetadata logical = cm.schema.getTableMetadata(t.keyspace, t.table);
        TableLayout layout = logical == null ? null : LayoutRegistry.of(cm).layout(logical.id);
        KeyState key = layout == null ? null : layout.keyState(t.key);
        return key != null && key.actionId != null && key.phase.pastCommit() && key.phase.isReverse();
    }

    // ---------------------------------------------------------------------------------------------------------
    // entry points
    // ---------------------------------------------------------------------------------------------------------

    private enum Mode
    {
        FORWARD,
        RECOVER,
        STOP
    }

    @Override
    public StepResult step(Action action, Claim claim, ActionContext ctx) throws Exception
    {
        if (ClusterActionExecution.INITIAL_PHASE.equals(claim.phase))
            return claimed(action, claim, ctx);
        return drive(claim, ctx, Mode.FORWARD);
    }

    @Override
    public StepResult recover(Claim claim, ActionContext ctx) throws Exception
    {
        return drive(claim, ctx, Mode.RECOVER);
    }

    @Override
    public StepResult abort(Action action, Claim claim, ActionContext ctx) throws Exception
    {
        return drive(claim, ctx, Mode.STOP);
    }

    private StepResult claimed(Action action, Claim claim, ActionContext ctx)
    {
        Optional<String> invalid = validate(action, null);
        if (invalid.isPresent())
            return StepResult.rolledBack("nothing was done: " + invalid.get());
        PartitionSplitActuator.Target t = PartitionSplitActuator.Target.of(action.target);
        ClusterMetadata cm = env.metadata();
        TableMetadata logical = cm.schema.getTableMetadata(t.keyspace, t.table);
        TableLayout layout = LayoutRegistry.of(cm).layout(logical.id);
        KeyState key = layout.keyState(t.key);
        Map<String, String> state = new TreeMap<>(claim.state);
        state.put(S_TABLE, logical.id.toString());
        state.put(S_KEY, key.incarnation + "/" + layout.buckets + '/' + layout.physical);
        String requested = action.params.getOrDefault(PARAM_MODE, MODE_AUTO);
        if (MODE_RESUME.equals(requested))
        {
            if (key.reverseMode != null)
                state.put(S_MODE, key.reverseMode.name());
            return StepResult.advance(ADOPTING, state, 0);
        }
        Choice choice = choose(cm, logical, layout, t.key, key, requested, ctx.nowMillis());
        if (choice.rejection != null)
            return StepResult.rolledBack("nothing was done: " + choice.rejection);
        state.put(S_MODE, choice.mode.name());
        if (key.phase != Phase.STEADY)
            state.put(S_PREVIOUS, key.actionId.toString());
        return StepResult.advance(choice.tombstoneFirst ? TOMBSTONING : BEGINNING, state, 0);
    }

    /** {@code BeginUnsplit} as {@link #claimed} chose it, unless the key changed since (a rejected commit would be retried forever). */
    private StepResult beginning(View v, ActionContext ctx, Mode mode)
    {
        Claim claim = v.claim;
        if (mode != Mode.FORWARD)
            return StepResult.rolledBack(stopped(mode) + "; the unsplit was not begun");
        KeyState key = v.key;
        KeyState.ReverseMode reverse = KeyState.ReverseMode.valueOf(claim.state.get(S_MODE));
        String previous = claim.state.get(S_PREVIOUS);
        boolean unchanged = key != null
                            && (previous == null ? key.phase == Phase.STEADY && key.actionId == null
                                                 : (key.phase == Phase.NEW_ONLY || key.phase == Phase.CLEANUP) && previous.equals(String.valueOf(key.actionId))
                                                   && ClusterLedger.of(v.cm).claim(key.actionId) == null)
                            && key.baseState == (reverse == KeyState.ReverseMode.FAST ? KeyState.BaseState.RESIDUE : KeyState.BaseState.TOMBSTONED);
        if (!unchanged)
            return StepResult.rolledBack("nothing was done: the key changed before the unsplit began (now " + (key == null ? "absent" : key.phase + ", " + key.baseState) + ')');
        BeginUnsplit begin = new BeginUnsplit(claim.actionId, claim.owner, claim.step, v.logical.id, v.target.key, reverse,
                                              previous == null ? null : UUID.fromString(previous), ctx.nowMillis(), leaseMillis());
        return StepResult.commit(begin.wrap(), Phase.REVERSE_ANNOUNCED.name(), claim.state, 0);
    }

    /** What one step sees: one metadata snapshot. */
    private static final class View
    {
        final Claim claim;
        final PartitionSplitActuator.Target target;
        final ClusterMetadata cm;
        final TableMetadata logical;
        final TableMetadata physical;
        final TableLayout layout;
        final KeyState key;
        final int incarnation;
        final int buckets;

        View(Claim claim, PartitionSplitActuator.Target target, ClusterMetadata cm, TableMetadata logical, TableMetadata physical, TableLayout layout,
             KeyState key, int incarnation, int buckets)
        {
            this.claim = claim;
            this.target = target;
            this.cm = cm;
            this.logical = logical;
            this.physical = physical;
            this.layout = layout;
            this.key = key;
            this.incarnation = incarnation;
            this.buckets = buckets;
        }

        String name()
        {
            return target.keyspace + '.' + target.table + " key 0x" + ByteBufferUtil.bytesToHex(target.key);
        }
    }

    private StepResult drive(Claim claim, ActionContext ctx, Mode mode) throws Exception
    {
        PartitionSplitActuator.Target t = PartitionSplitActuator.Target.of(claim);
        if (t.rejection != null)
            return StepResult.failed("partition_unsplit claim with an unusable target " + PartitionSplitActuator.text(claim.target) + ": " + t.rejection);
        String phase = claim.phase;
        if (ClusterActionExecution.INITIAL_PHASE.equals(phase))
            return StepResult.rolledBack("stopped before anything was written");
        ClusterMetadata cm = env.metadata();
        String tableId = claim.state.get(S_TABLE);
        TableMetadata logical = tableId == null ? cm.schema.getTableMetadata(t.keyspace, t.table) : cm.schema.getTableMetadata(TableId.fromString(tableId));
        String[] ids = claim.state.getOrDefault(S_KEY, "0/0/").split("/", 3);
        int incarnation = Integer.parseInt(ids[0]);
        int buckets = Integer.parseInt(ids[1]);
        TableMetadata physical = ids[2].isEmpty() ? null : cm.schema.getTableMetadata(TableId.fromString(ids[2]));
        if (logical == null)
            return StepResult.failed(PartitionSplitActuator.TABLE_DROPPED + ": " + t.keyspace + '.' + t.table + " was dropped during the unsplit");
        TableLayout layout = LayoutRegistry.of(cm).layout(logical.id);
        KeyState key = layout == null ? null : layout.keyState(t.key);
        View v = new View(claim, t, cm, logical, physical, layout, key, incarnation, buckets);

        if (KEY_REMOVED.equals(phase))
            return removed(v, ctx);
        if (TOMBSTONING.equals(phase))
            return tombstoning(v, ctx, mode);
        if (BEGINNING.equals(phase))
            return beginning(v, ctx, mode);
        if (ADOPTING.equals(phase) || Claim.RECOVERING.equals(phase))
            return adopt(v, ctx, mode);
        if (ReleaseKey.CLAIM_PHASE.equals(phase))
            return released(v);

        if (key == null)
            return StepResult.failed("the claim is in phase " + phase + " but " + v.name() + " is not in the layout registry");
        if (!claim.actionId.equals(key.actionId))
            return StepResult.failed(v.name() + " is owned by action " + key.actionId + ", not by this action " + claim.actionId);
        if (!key.phase.name().equals(phase))
            return StepResult.failed("the claim is in phase " + phase + " but " + v.name() + " is in " + key.phase);
        if (key.phase == Phase.STEADY)
            return steady(v, ctx);

        if (PRE_COMMIT.contains(key.phase))
        {
            String abort = mode != Mode.FORWARD ? stopped(mode) : claim.state.get(S_ABORT);
            if (abort == null && key.phase != Phase.REVERSE_ROLLBACK)
                abort = deadlinePassed(v, ctx);
            if (abort != null || key.phase == Phase.REVERSE_ROLLBACK)
                return abortStep(v, ctx, abort == null ? abortReason(claim, mode) : abort);
            return forward(v, ctx);
        }
        // BASE_ONLY, REVERSE_CLEANUP: past the reverse commit point, never aborted
        if (mode == Mode.STOP)
            return StepResult.committedCleanupPending(v.name() + " is past its reverse commit point in " + key.phase + "; it stays routed to the base copy");
        return forward(v, ctx);
    }

    private static String stopped(Mode mode)
    {
        return mode == Mode.STOP ? "the unsplit was stopped before the reverse commit point" : "the owner was lost or restarted before the reverse commit point";
    }

    private static String abortReason(Claim claim, Mode mode)
    {
        String reason = claim.state.get(S_ABORT);
        return reason != null ? reason : mode == Mode.FORWARD ? "aborted" : stopped(mode);
    }

    // ---------------------------------------------------------------------------------------------------------
    // the stuck key's tombstone (owner answer 6)
    // ---------------------------------------------------------------------------------------------------------

    private StepResult tombstoning(View v, ActionContext ctx, Mode mode)
    {
        Claim claim = v.claim;
        String tombstone = claim.state.get(S_TOMBSTONE);
        if (v.key == null || v.key.phase != Phase.CLEANUP || v.key.baseState != KeyState.BaseState.RESIDUE)
            return tombstone == null ? StepResult.rolledBack("the key left CLEANUP before its base tombstone was written; nothing was done")
                                     : StepResult.failed("the key left CLEANUP after its base tombstone " + tombstone + " was written ahead; the registry does not "
                                                         + "record it, so a fast unsplit of the key must not be started; an operator must check it");
        if (tombstone == null)
        {
            // nothing is written yet: a stop or a new owner just ends here
            if (mode != Mode.FORWARD)
                return StepResult.rolledBack(stopped(mode) + "; the base tombstone was not written");
            long due = stuckTombstoneDue(v.key, v.logical);
            if (ctx.nowMillis() < due)
                return StepResult.stay(Math.min(pollMillis(), due - ctx.nowMillis()));
            Map<String, String> state = new TreeMap<>(claim.state);
            state.put(S_TOMBSTONE, Long.toString(ctx.nowMillis() * 1000));
            return StepResult.advance(TOMBSTONING, state, 0);
        }
        // the tombstone may be written: whoever owns the claim now writes it (again) and records it, so that the registry
        // never holds RESIDUE for a tombstoned base partition
        long tt = Long.parseLong(tombstone);
        try
        {
            env.deleteBase(v.logical, v.target.key, tt);
        }
        catch (Exception e)
        {
            logger.info("Autonomic partition unsplit of {}: base partition tombstone not written yet: {}", v.name(), e.toString());
            return StepResult.stay(pollMillis());
        }
        UUID previous = UUID.fromString(claim.state.get(S_PREVIOUS));
        BeginUnsplit begin = new BeginUnsplit(claim.actionId, claim.owner, claim.step, v.logical.id, v.target.key, KeyState.ReverseMode.SLOW, previous, tt,
                                              ctx.nowMillis(), leaseMillis());
        return StepResult.commit(begin.wrap(), Phase.REVERSE_ANNOUNCED.name(), claim.state, 0);
    }

    // ---------------------------------------------------------------------------------------------------------
    // takeover and resume
    // ---------------------------------------------------------------------------------------------------------

    private StepResult adopt(View v, ActionContext ctx, Mode mode) throws Exception
    {
        Claim claim = v.claim;
        KeyState key = v.key;
        if (key == null)
        {
            // removed by RemoveKey (the reconcile generation was being finished), or never begun
            if (claim.state.containsKey(S_FINISHING))
                return StepResult.advance(KEY_REMOVED, claim.state, 0);
            return StepResult.rolledBack("the key is not in the layout registry; nothing to recover");
        }
        if (claim.state.containsKey(S_TOMBSTONE) && key.phase == Phase.CLEANUP && key.baseState == KeyState.BaseState.RESIDUE)
            // the owner was lost between the write-ahead of the tombstone and BeginUnsplit
            return StepResult.advance(TOMBSTONING, claim.state, 0);
        boolean ours = key.phase.isReverse() || (key.phase == Phase.STEADY && key.actionId != null);
        if (!ours || key.actionId == null)
            return StepResult.rolledBack("the key is in " + key.phase + (key.actionId == null ? " and owned by no action" : " owned by " + key.actionId)
                                         + "; the unsplit was not begun or is over");
        if (claim.actionId.equals(key.actionId))
            return StepResult.advance(key.phase.name(), claim.state, 0);
        if (ClusterLedger.of(v.cm).claim(key.actionId) != null)
            return StepResult.failed("the key is owned by action " + key.actionId + ", which holds a claim");
        if (mode == Mode.STOP && key.phase.pastCommit())
            return StepResult.committedCleanupPending(v.name() + " is past its reverse commit point in " + key.phase + " and was not adopted");
        AdoptKey adopt = new AdoptKey(claim.actionId, claim.owner, claim.step, v.logical.id, v.target.key, key.phase, key.actionId, ctx.nowMillis(), leaseMillis());
        return StepResult.commit(adopt.wrap(), key.phase.name(), claim.state, 0);
    }

    // ---------------------------------------------------------------------------------------------------------
    // forward
    // ---------------------------------------------------------------------------------------------------------

    private StepResult forward(View v, ActionContext ctx) throws Exception
    {
        KeyState key = v.key;
        if (!v.cm.inProgressSequences.isEmpty())
            return hold(v, "a metadata operation is in progress (design 4.7)");
        if (v.physical == null)
            return StepResult.failed("the overflow table of " + v.name() + " is missing");
        switch (key.phase)
        {
            case REVERSE_ANNOUNCED:
                return announced(v, ctx);
            case REVERSE_DUAL_WRITE:
                return advanceKey(v, ctx, Phase.REVERSE_BACKFILL, key.generation);
            case REVERSE_BACKFILL:
                // design gap of T6.17c: the generation inherited from BeginUnsplit holds the purge reports; never counted
                if (!key.reverseBackfillDue() || bucketPlacementChanged(v))
                    return advanceKey(v, ctx, Phase.REVERSE_BACKFILL, key.generation + 1);
                if (!complete(v))
                    return StepResult.stay(pollMillis());
                return advanceKey(v, ctx, Phase.REVERSE_VERIFY, key.generation);
            case REVERSE_VERIFY:
                return verify(v, ctx);
            case CUTBACK:
                return cutback(v, ctx);
            case BASE_ONLY:
            {
                Map<String, String> state = v.claim.state;
                if (state.containsKey(S_VERIFIED) || state.containsKey(S_VERIFY_FAILED) || state.containsKey(S_VERIFY_FAILURES) || state.containsKey(S_COMMIT_APPROVED))
                {
                    Map<String, String> next = new TreeMap<>(state);
                    next.keySet().removeAll(List.of(S_VERIFIED, S_VERIFY_FAILED, S_VERIFY_FAILURES, S_COMMIT_APPROVED));
                    return StepResult.advance(v.claim.phase, next, 0);
                }
                return advanceKey(v, ctx, Phase.REVERSE_CLEANUP, key.generation);
            }
            case REVERSE_CLEANUP:
                return cleanup(v, ctx);
            default:
                return StepResult.failed("no forward step from phase " + key.phase);
        }
    }

    private StepResult announced(View v, ActionContext ctx)
    {
        KeyState key = v.key;
        StepResult acked = ackedFor(v, ctx);
        if (acked != null)
            return acked;
        long now = ctx.nowMillis();
        // the key's baseline over baseline_window, for the guard of CUTBACK (design 11.4); a node without signals judges nothing
        if (!v.claim.state.containsKey(S_BASELINE))
        {
            KnowledgeView knowledge = env.knowledge();
            if (knowledge != null)
            {
                long end = key.phaseSinceMillis + lc().baseline_window.toMilliseconds();
                if (now < end)
                    return StepResult.stay(Math.min(pollMillis(), end - now));
            }
            Map<String, String> state = new TreeMap<>(v.claim.state);
            state.put(S_BASELINE, baseline(v, knowledge, now));
            return StepResult.advance(v.claim.phase, state, 0);
        }
        if (key.reverseMode == KeyState.ReverseMode.SLOW)
        {
            // every natural base replica of K reports a clean purge for the generation of BeginUnsplit (or a later one)
            if (env.baseReplicasModified(v.cm, v.logical, v.target.key).isAfter(key.generationEpoch))
                return advanceKey(v, ctx, Phase.REVERSE_ANNOUNCED, key.generation + 1);
            Set<NodeId> replicas = env.baseReplicas(v.cm, v.logical, v.target.key);
            if (replicas.isEmpty() || !key.backfillDone.containsAll(replicas))
            {
                long timeoutEnd = Math.max(ackedAt(v), slowFrom(key, v.logical)) + lc().purge_timeout.toMilliseconds();
                // each replica publishes when its own partition deletion may first be purged (11.5: an unknown Tt is learned
                // from the replicas): the timeout counts from the latest of those of the replicas that have not reported
                Set<NodeId> missing = new java.util.TreeSet<>(replicas);
                missing.removeAll(key.backfillDone);
                for (BasePurge.Published p : env.purgeReasons(v.cm, v.logical.id, v.target.key, missing).values())
                    if (p.purgeFromMillis != Long.MIN_VALUE)
                        timeoutEnd = Math.max(timeoutEnd, p.purgeFromMillis + lc().purge_timeout.toMilliseconds());
                if (now >= timeoutEnd)
                    return abortStep(v, ctx, residue(v, replicas, now));
                return StepResult.stay(Math.min(pollMillis(), timeoutEnd - now));
            }
        }
        return advanceKey(v, ctx, Phase.REVERSE_DUAL_WRITE, key.generation);
    }

    private static String baseline(View v, KnowledgeView knowledge, long now)
    {
        if (knowledge == null)
            return NONE;
        Subject subject = keySubject(v);
        SplitEvaluation.Window window = SplitEvaluation.Window.of(knowledge, subject, v.key.phaseSinceMillis, now);
        double tableWrite = SplitEvaluation.mean(knowledge, Signals.TABLE_COORDINATOR_WRITE_LATENCY_P99_MICROS,
                                                 Subject.table(v.target.keyspace, v.target.table), v.key.phaseSinceMillis, now);
        // the CUTBACK guard is the C1 guard, which does not judge the rest of the table (ADR-040)
        return SplitEvaluation.Baseline.announced(window, SplitEvaluation.Window.NONE, tableWrite, null).encode();
    }

    private static Subject keySubject(View v)
    {
        return Subject.partition(v.target.keyspace, v.target.table, ByteBufferUtil.bytesToHex(v.target.key));
    }

    /**
     * {@value #UNSPLIT_RESIDUE}: what each base replica that has not reported published about its purge, and what the
     * operator can do; recorded for the diagnosis, and returned as the abort reason.
     */
    private String residue(View v, Set<NodeId> replicas, long now)
    {
        Set<NodeId> missing = new java.util.TreeSet<>(replicas);
        missing.removeAll(v.key.backfillDone);
        Map<NodeId, BasePurge.Published> published = env.purgeReasons(v.cm, v.logical.id, v.target.key, missing);
        Map<String, String> nodes = new LinkedHashMap<>();
        long largest = Long.MIN_VALUE;
        boolean repair = false;
        boolean aboveTt = false;
        boolean waiting = false;
        boolean silent = false;
        for (NodeId node : missing)
        {
            BasePurge.Published p = published.get(node);
            if (p == null)
            {
                nodes.put(Integer.toString(node.id()), "no purge reason published (down, not gossiping, or not yet probed)");
                silent = true;
                continue;
            }
            nodes.put(Integer.toString(node.id()), p.reason + ": " + p.detail);
            switch (p.reason)
            {
                case STILL_SHADOWED:
                case ONLY_PURGE_REPAIRED_TOMBSTONES:
                    repair = true;
                    break;
                case RESIDUE_ABOVE_TT:
                    aboveTt = true;
                    largest = Math.max(largest, p.maxTimestamp);
                    break;
                default:
                    waiting = true;
            }
            if (p.detail.contains("pending repair") && !p.detail.contains(" 0 pending repair"))
                repair = true;
        }
        List<String> remedies = new ArrayList<>();
        if (repair)
            remedies.add("Run a repair of the key's token range of " + v.logical.keyspace + '.' + v.logical.name
                         + " on the nodes named, so that the deletion and the data it shadows reach one repair state and can be purged together "
                         + "(or set layout.force_purge_compaction: false and wait for natural compaction), then start the unsplit again.");
        if (aboveTt)
            remedies.add("Cells newer than the base tombstone survive the purge, the largest at timestamp " + largest
                         + "; they are never purged and need an operator decision (owner answer 3): delete them with a higher timestamp, or keep the key split.");
        if (waiting)
            remedies.add("A replica's clock has not passed the deletion's local deletion time plus gc_grace_seconds; check its clock, then try again later.");
        if (silent)
            remedies.add("Check the nodes that published no reason.");
        String remedy = String.join(" ", remedies);
        LayoutEvents.unsplitResidue(new LayoutEvents.UnsplitResidue(v.claim.actionId, v.target.keyspace, v.target.table, ByteBufferUtil.bytesToHex(v.target.key),
                                                                    nodes, remedy, largest, now));
        StringBuilder why = new StringBuilder(UNSPLIT_RESIDUE).append(": purge_timeout ").append(lc().purge_timeout)
                                                              .append(" passed without a clean purge on node(s) ");
        nodes.forEach((node, reason) -> why.append(node).append(" (").append(reason, 0, Math.min(reason.length(), 60)).append(") "));
        return why.append(remedy).toString();
    }

    private StepResult verify(View v, ActionContext ctx)
    {
        KeyState key = v.key;
        Map<String, String> state = v.claim.state;
        String generation = key.generation + "/" + key.generationEpoch.getEpoch();
        if (bucketPlacementChanged(v))
            return advanceKey(v, ctx, Phase.REVERSE_BACKFILL, key.generation + 1);
        if (generation.equals(state.get(S_VERIFIED)))
            return advanceKey(v, ctx, Phase.CUTBACK, key.generation);
        if (generation.equals(state.get(S_VERIFY_FAILED)))
            return advanceKey(v, ctx, Phase.REVERSE_BACKFILL, key.generation + 1);

        String id = v.claim.actionId + "/" + key.incarnation + '/' + generation;
        if (!id.equals(checkId) || check == null)
        {
            checkId = id;
            check = env.check(v.cm, v.logical.id, v.target.key, lc().verify_page_rows, lc().verify_recheck_delay.toMilliseconds(), ctx::nowMillis);
        }
        LayoutOwnerSteps.Run run = LayoutOwnerSteps.run(check, ctx, pollMillis());
        if (run.failed)
        {
            logger.info("Autonomic partition unsplit of {}: verification read failed, starting it again: {}", v.name(), run.error.toString());
            check = null;
            checkId = null;
            return StepResult.stay(run.delayMillis);
        }
        if (!run.done)
            return StepResult.stay(run.delayMillis);
        LayoutOwnerSteps.Check done = check;
        check = null;
        checkId = null;
        // roles swapped (ADR-030 mirrored): a base-only row is an unacknowledged write of C2, tolerated and counted
        long failing = done.count(LayoutVerifier.Kind.CONTENT_DIFFERS) + done.count(LayoutVerifier.Kind.MISSING_FROM_BASE);
        Map<String, String> next = new TreeMap<>(state);
        if (failing == 0 && !done.tooManyDifferences())
        {
            next.put(S_VERIFIED, generation);
            next.put(S_TOLERATED, Long.toString(done.count(LayoutVerifier.Kind.MISSING_FROM_OVERFLOW)));
            logger.info("Autonomic partition unsplit of {}: verification of generation {} passed: {}", v.name(), key.generation, done.summary());
            return StepResult.advance(Phase.REVERSE_VERIFY.name(), next, 0);
        }
        int failures = Integer.parseInt(state.getOrDefault(S_VERIFY_FAILURES, "0")) + 1;
        next.put(S_VERIFY_FAILURES, Integer.toString(failures));
        logger.warn("Autonomic partition unsplit of {}: verification of generation {} failed ({} of {}): {}", v.name(), key.generation,
                    failures, lc().verify_retries + 1, done.summary());
        if (failures > lc().verify_retries)
            next.put(S_ABORT, PartitionSplitActuator.text("reverse verification failed " + failures + " times: " + done.summary()));
        else
            next.put(S_VERIFY_FAILED, generation);
        return StepResult.advance(Phase.REVERSE_VERIFY.name(), next, 0);
    }

    private StepResult cutback(View v, ActionContext ctx)
    {
        KeyState key = v.key;
        StepResult acked = ackedFor(v, ctx);
        if (acked != null)
            return acked;
        long now = ctx.nowMillis();
        long remaining = ackedAt(v) + lc().cutover_soak.toMilliseconds() - now;
        if (remaining > 0)
            return StepResult.stay(Math.min(remaining, pollMillis()));
        SplitEvaluation.Result guard = guard(v, ctx);
        if (!guard.ok)
        {
            Map<String, String> state = new TreeMap<>(v.claim.state);
            state.put(S_ABORT, PartitionSplitActuator.text("C1 guard failed in CUTBACK: " + guard.detail));
            return StepResult.advance(v.claim.phase, state, 0);
        }
        long window = commitWindowEnd(v);
        if (now >= window)
        {
            Map<String, String> state = new TreeMap<>(v.claim.state);
            state.put(S_ABORT, PartitionSplitActuator.text("the reverse commit window closed at " + window + ": straggler_window " + lc().straggler_window
                                                           + " and reconcile_budget " + lc().reconcile_budget + " no longer end before the replicas' limit "
                                                           + replicaLimit(v)));
            return StepResult.advance(v.claim.phase, state, 0);
        }
        if (manualCommitApproval() && !"true".equals(v.claim.state.get(S_COMMIT_APPROVED)))
            return StepResult.stay(Math.max(1, Math.min(pollMillis(), window - now)));
        // the reverse commit point
        return advanceKey(v, ctx, Phase.BASE_ONLY, key.generation);
    }

    /** The C1 guard of design 7.3 over the soak, against the baseline of REVERSE_ANNOUNCED; not judged without one. */
    private SplitEvaluation.Result guard(View v, ActionContext ctx)
    {
        KnowledgeView knowledge = env.knowledge();
        String encoded = v.claim.state.get(S_BASELINE);
        if (knowledge == null || encoded == null || NONE.equals(encoded))
            return SplitEvaluation.guardC1(null, null, Double.NaN, Collections.emptyList(), lc());
        SplitEvaluation.Baseline baseline = SplitEvaluation.Baseline.fromMap(Map.of(SplitEvaluation.S_BASELINE, encoded));
        long now = ctx.nowMillis();
        long from = Math.max(ackedAt(v), now - lc().baseline_window.toMilliseconds());
        SplitEvaluation.Window window = SplitEvaluation.Window.of(knowledge, keySubject(v), from, now);
        double tableWrite = SplitEvaluation.mean(knowledge, Signals.TABLE_COORDINATOR_WRITE_LATENCY_P99_MICROS,
                                                 Subject.table(v.target.keyspace, v.target.table), from, now);
        return SplitEvaluation.guardC1(baseline, window, tableWrite, Collections.emptyList(), lc());
    }

    private StepResult cleanup(View v, ActionContext ctx)
    {
        KeyState key = v.key;
        long limit = replicaLimit(v);
        if (!key.reverseBackfillDue())
        {
            // entered with the generation of the backfill: wait for stragglers, then start the reconcile generation
            StepResult acked = ackedFor(v, ctx);
            if (acked != null)
                return acked;
            if (ctx.nowMillis() >= limit)
                return reconcilePastDeadline(v, limit, "before the reconcile pass started");
            long remaining = ackedAt(v) + lc().straggler_window.toMilliseconds() - ctx.nowMillis();
            if (remaining > 0)
                return StepResult.stay(Math.min(remaining, pollMillis()));
            return advanceKey(v, ctx, Phase.REVERSE_CLEANUP, key.generation + 1);
        }
        String generation = key.generation + "/" + key.generationEpoch.getEpoch();
        boolean finishing = generation.equals(v.claim.state.get(S_FINISHING));
        boolean done = complete(v);
        boolean moved = bucketPlacementChanged(v);
        if (!done || moved)
        {
            if (ctx.nowMillis() >= limit)
                return reconcilePastDeadline(v, limit, "with " + key.backfillDone.size() + " of " + bucketReplicas(v).size() + " bucket replicas reported");
            if (moved || finishing)
                return advanceKey(v, ctx, Phase.REVERSE_CLEANUP, key.generation + 1);
            return StepResult.stay(pollMillis());
        }
        if (!finishing)
        {
            Map<String, String> state = new TreeMap<>(v.claim.state);
            state.put(S_FINISHING, generation);
            return StepResult.advance(v.claim.phase, state, 0);
        }
        if (!env.acknowledged(key.phaseEpoch))
            return StepResult.stay(pollMillis());
        // RemoveKey itself is rejected unless every natural replica of every bucket token reported this generation
        RemoveKey remove = new RemoveKey(v.claim.actionId, v.claim.owner, v.claim.step, v.logical.id, v.target.key, ctx.nowMillis(), leaseMillis());
        return StepResult.commit(remove.wrap(), KEY_REMOVED, v.claim.state, 0);
    }

    private StepResult reconcilePastDeadline(View v, long limit, String where)
    {
        return StepResult.failed(RECONCILE_PAST_DEADLINE + ": the reverse reconcile pass of " + v.name() + " cannot complete before the replicas' limit " + limit
                                 + " (design 11.5, I18), " + where + ". The key stays in REVERSE_CLEANUP and reads and writes stay on the base copy; "
                                 + "the bucket partitions are not deleted. An operator must decide");
    }

    /**
     * After {@code RemoveKey} (invariant I20): every node acknowledged the removal, the write timeout passed, then the
     * bucket partitions of the incarnation are deleted at ALL, or the overflow table is dropped if no layout is left.
     */
    private StepResult removed(View v, ActionContext ctx) throws Exception
    {
        Claim claim = v.claim;
        if (v.key != null)
            return StepResult.failed("the claim is in " + KEY_REMOVED + " but " + v.name() + " is still in the layout registry");
        String removed = claim.state.get(S_REMOVED);
        if (removed == null)
        {
            Map<String, String> state = new TreeMap<>(claim.state);
            state.put(S_REMOVED, Long.toString(v.cm.epoch.getEpoch()));
            return StepResult.advance(KEY_REMOVED, state, 0);
        }
        String prefix = KEY_REMOVED + '/' + removed + '/';
        String acked = claim.state.get(S_ACKED);
        if (acked == null || !acked.startsWith(prefix))
        {
            if (!env.acknowledged(Epoch.create(Long.parseLong(removed))))
                return StepResult.stay(pollMillis());
            Map<String, String> state = new TreeMap<>(claim.state);
            state.put(S_ACKED, prefix + ctx.nowMillis());
            return StepResult.advance(KEY_REMOVED, state, 0);
        }
        long remaining = Long.parseLong(acked.substring(prefix.length())) + env.writeTimeoutMillis() - ctx.nowMillis();
        if (remaining > 0)
            return StepResult.stay(remaining);
        String cleanup;
        if (v.layout == null)
        {
            // the layout went with its last key: the overflow table goes with it (design 11.4)
            if (v.physical != null)
            {
                env.dropTable(v.physical.keyspace, v.physical.name);
                return StepResult.stay(0);
            }
            cleanup = "the overflow table was dropped";
        }
        else if (v.physical == null)
        {
            cleanup = "the overflow table is gone";
        }
        else
        {
            String attempt = claim.actionId + "/" + v.incarnation;
            int attempts = deleteAttempts.getOrDefault(attempt, 0);
            if (attempts < MAX_DELETE_ATTEMPTS)
            {
                try
                {
                    env.deleteBuckets(v.logical, v.physical, v.buckets, v.target.key, v.incarnation, ctx.nowMillis() * 1000);
                    cleanup = "the " + v.buckets + " bucket partitions of incarnation " + v.incarnation + " were deleted";
                }
                catch (Exception e)
                {
                    deleteAttempts.put(attempt, attempts + 1);
                    logger.info("Autonomic partition unsplit of {}: deleting the buckets of incarnation {} failed ({} of {}): {}",
                                v.name(), v.incarnation, attempts + 1, MAX_DELETE_ATTEMPTS, e.toString());
                    return StepResult.stay(pollMillis());
                }
            }
            else
            {
                cleanup = "the bucket partitions of incarnation " + v.incarnation + " could not be deleted; they are never read again";
            }
            deleteAttempts.remove(attempt);
        }
        return StepResult.committed(Verdict.INCONCLUSIVE, v.name() + " is unsplit (" + claim.state.getOrDefault(S_MODE, "?") + " mode); "
                                                          + S_TOLERATED + '=' + claim.state.getOrDefault(S_TOLERATED, "0") + "; " + cleanup);
    }

    // ---------------------------------------------------------------------------------------------------------
    // abort (design 11.4)
    // ---------------------------------------------------------------------------------------------------------

    private StepResult abortStep(View v, ActionContext ctx, String why)
    {
        KeyState key = v.key;
        switch (key.phase)
        {
            case REVERSE_ANNOUNCED:
            case REVERSE_DUAL_WRITE:
            case REVERSE_BACKFILL:
            case REVERSE_VERIFY:
                return recordedAbort(v, why, () -> advanceKey(v, ctx, Phase.STEADY, key.generation));
            case CUTBACK:
                return recordedAbort(v, why, () -> advanceKey(v, ctx, Phase.REVERSE_ROLLBACK, key.generation));
            case REVERSE_ROLLBACK:
                return advanceKey(v, ctx, Phase.STEADY, key.generation);
            default:
                return StepResult.failed("no abort from phase " + key.phase);
        }
    }

    private StepResult recordedAbort(View v, String why, Supplier<StepResult> transition)
    {
        if (v.claim.state.containsKey(S_ABORT))
            return transition.get();
        Map<String, String> state = new TreeMap<>(v.claim.state);
        state.put(S_ABORT, PartitionSplitActuator.text(why));
        return StepResult.advance(v.claim.phase, state, 0);
    }

    /** Back in STEADY, owned by this action: once every node acknowledged it, {@code ReleaseKey} with the base state of 11.4. */
    private StepResult steady(View v, ActionContext ctx)
    {
        StepResult acked = ackedFor(v, ctx);
        if (acked != null)
            return acked;
        KeyState key = v.key;
        ReleaseKey release = new ReleaseKey(v.claim.actionId, v.claim.owner, v.claim.step, v.logical.id, v.target.key, key.baseState,
                                            key.baseState == KeyState.BaseState.TOMBSTONED ? key.baseTombstoneMicros : 0, ctx.nowMillis(), leaseMillis());
        return StepResult.commit(release.wrap(), ReleaseKey.CLAIM_PHASE, v.claim.state, 0);
    }

    private StepResult released(View v)
    {
        String base = v.key == null ? "" : "; the base partition is " + v.key.baseState;
        return StepResult.rolledBack(v.name() + " stays split: " + abortReason(v.claim, Mode.FORWARD) + base);
    }

    // ---------------------------------------------------------------------------------------------------------
    // deadlines (design 11.5)
    // ---------------------------------------------------------------------------------------------------------

    /**
     * The replicas' limit: {@link KeyState#reverseReplicaLimitMillis} once {@code REVERSE_DUAL_WRITE} started; before it,
     * for a fast reverse, projected from {@code frozenSince}; for a slow one there is none yet ({@link Long#MAX_VALUE}).
     */
    long replicaLimit(View v)
    {
        KeyState key = v.key;
        int gc = gcGrace(v.logical, v.physical);
        if (key.reverseDualSinceMillis > 0)
            return key.reverseReplicaLimitMillis(gc, env.writeTimeoutMillis());
        if (key.reverseMode == KeyState.ReverseMode.FAST && key.frozenSinceMillis > 0)
            return key.frozenSinceMillis + gc * 1000L / 2 - env.writeTimeoutMillis();
        return Long.MAX_VALUE;
    }

    /** The reverse commit window end: the replicas' limit less straggler_window, reconcile_budget and two polls. */
    long commitWindowEnd(View v)
    {
        long limit = replicaLimit(v);
        return limit == Long.MAX_VALUE ? Long.MAX_VALUE : LayoutOwnerSteps.commitWindowEnd(limit, lc(), pollMillis());
    }

    /** The owner's deadline before the commit: the limit less straggler_window, capped by max_migration_duration. */
    long preCommitDeadline(View v)
    {
        long limit = replicaLimit(v);
        long deadline = limit == Long.MAX_VALUE ? Long.MAX_VALUE : limit - lc().straggler_window.toMilliseconds();
        if (v.key.reverseDualSinceMillis > 0)
            deadline = Math.min(deadline, v.key.reverseDualSinceMillis + lc().max_migration_duration.toMilliseconds());
        return deadline;
    }

    private String deadlinePassed(View v, ActionContext ctx)
    {
        long deadline = preCommitDeadline(v);
        if (ctx.nowMillis() < deadline)
            return null;
        return "the reverse deadline passed in " + v.key.phase + " (deadline " + deadline + ", before the replicas' limit " + replicaLimit(v) + ')';
    }

    // ---------------------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------------------

    private StepResult advanceKey(View v, ActionContext ctx, Phase to, int generation)
    {
        return LayoutOwnerSteps.advance(v.claim, v.logical.id, v.target.key, v.key, to, generation, () -> env.acknowledged(v.key.phaseEpoch),
                                        ctx.nowMillis(), leaseMillis(), pollMillis());
    }

    private StepResult ackedFor(View v, ActionContext ctx)
    {
        return LayoutOwnerSteps.ackedFor(v.claim, v.key, () -> env.acknowledged(v.key.phaseEpoch), ctx.nowMillis(), pollMillis());
    }

    private static long ackedAt(View v)
    {
        return LayoutOwnerSteps.ackedAt(v.claim, v.key);
    }

    private Set<NodeId> bucketReplicas(View v)
    {
        return env.bucketReplicas(v.cm, v.logical, v.physical, v.target.key, v.key.incarnation, v.layout.buckets);
    }

    /** Design 11.4: a reverse pass of the current generation is due and every replica of every bucket token reported it. */
    private boolean complete(View v)
    {
        if (!v.key.reverseBackfillDue())
            return false;
        Set<NodeId> replicas = bucketReplicas(v);
        return !replicas.isEmpty() && v.key.backfillDone.containsAll(replicas);
    }

    private boolean bucketPlacementChanged(View v)
    {
        return env.bucketReplicasModified(v.cm, v.logical, v.physical, v.target.key, v.key.incarnation, v.layout.buckets).isAfter(v.key.generationEpoch);
    }

    private StepResult hold(View v, String why)
    {
        logger.debug("Autonomic partition unsplit of {} holds in {}: {}", v.name(), v.claim.phase, why);
        return StepResult.stay(pollMillis());
    }

    private boolean manualCommitApproval()
    {
        return !"auto".equalsIgnoreCase(lc().commit_approval);
    }

    private long pollMillis()
    {
        return LayoutOwnerSteps.pollMillis(lc());
    }

    private long leaseMillis()
    {
        return config.get().coordination.lease.toMilliseconds();
    }

    // ---------------------------------------------------------------------------------------------------------
    // operator requests
    // ---------------------------------------------------------------------------------------------------------

    /**
     * The second approval of {@code commit_approval: manual} (owner answer 5): {@code nodetool autonomic approve <id>}
     * for an unsplit in CUTBACK. Returns false if {@code actionId} is not such an action.
     */
    public static boolean approveCommit(ClusterCoordinator coordinator, UUID actionId)
    {
        for (int attempt = 0; attempt < 3; attempt++)
        {
            Claim claim = coordinator.ledger().claim(actionId);
            if (claim == null || !NAME.equals(claim.actuator) || !Phase.CUTBACK.name().equals(claim.phase))
                return false;
            if ("true".equals(claim.state.get(S_COMMIT_APPROVED)))
                return true;
            Map<String, String> state = new TreeMap<>(claim.state);
            state.put(S_COMMIT_APPROVED, "true");
            if (coordinator.advance(claim, claim.phase, state).ok())
                return true;
        }
        throw new IllegalStateException("The reverse commit approval of action " + actionId + " could not be committed; try again");
    }

    // ---------------------------------------------------------------------------------------------------------
    // the node
    // ---------------------------------------------------------------------------------------------------------

    static final class NodeEnv implements Env
    {
        private final PartitionSplitActuator.NodeEnv split = new PartitionSplitActuator.NodeEnv();

        @Override
        public ClusterMetadata metadata()
        {
            return ClusterMetadata.current();
        }

        @Override
        public boolean acknowledged(Epoch epoch)
        {
            return EpochWatermarks.allAtLeast(epoch);
        }

        @Override
        public boolean allAlive(ClusterMetadata cm)
        {
            for (NodeId id : EpochWatermarks.required(cm))
            {
                InetAddressAndPort endpoint = cm.directory.endpoint(id);
                if (endpoint == null || !FailureDetector.instance.isAlive(endpoint))
                    return false;
            }
            return true;
        }

        @Override
        public void dropTable(String keyspace, String table)
        {
            split.dropTable(keyspace, table);
        }

        @Override
        public void deleteBuckets(TableMetadata logical, TableMetadata physical, int buckets, ByteBuffer key, int incarnation, long timestampMicros)
        {
            long nowInSec = timestampMicros / 1_000_000;
            List<Mutation> mutations = new ArrayList<>(buckets);
            for (int bucket = 0; bucket < buckets; bucket++)
                mutations.add(new Mutation(PartitionUpdate.fullPartitionDelete(physical, RowMapper.physicalKey(logical, physical, key, incarnation, bucket),
                                                                               timestampMicros, nowInSec)));
            LayoutWrites.writeAsOwner(mutations, ConsistencyLevel.ALL);
        }

        @Override
        public void deleteBase(TableMetadata logical, ByteBuffer key, long timestampMicros)
        {
            split.deleteBase(logical, key, timestampMicros);
        }

        @Override
        public Set<NodeId> baseReplicas(ClusterMetadata cm, TableMetadata logical, ByteBuffer key)
        {
            return NaturalReplicas.of(cm, logical, key);
        }

        @Override
        public Epoch baseReplicasModified(ClusterMetadata cm, TableMetadata logical, ByteBuffer key)
        {
            return NaturalReplicas.lastModified(cm, logical, key);
        }

        @Override
        public Set<NodeId> bucketReplicas(ClusterMetadata cm, TableMetadata logical, TableMetadata physical, ByteBuffer key, int incarnation, int buckets)
        {
            return NaturalReplicas.ofBuckets(cm, logical, physical, key, incarnation, buckets);
        }

        @Override
        public Epoch bucketReplicasModified(ClusterMetadata cm, TableMetadata logical, TableMetadata physical, ByteBuffer key, int incarnation, int buckets)
        {
            return NaturalReplicas.bucketsLastModified(cm, logical, physical, key, incarnation, buckets);
        }

        @Override
        public LayoutOwnerSteps.Check check(ClusterMetadata cm, TableId logical, ByteBuffer key, int pageRows, long recheckDelayMillis, LongSupplier clock)
        {
            LayoutVerifier verifier = LayoutVerifier.forPartition(cm, logical, key, LayoutVerifier.Direction.REVERSE,
                                                                  LayoutVerifier.defaultTolerated(LayoutVerifier.Direction.REVERSE), ConsistencyLevel.ALL,
                                                                  pageRows, LayoutVerifier.DEFAULT_RECHECK_ATTEMPTS, recheckDelayMillis, clock);
            return new LayoutOwnerSteps.Check()
            {
                public LayoutVerifier.Status step()
                {
                    return verifier.step();
                }

                public long nextDueMillis()
                {
                    return verifier.nextDueMillis();
                }

                public long count(LayoutVerifier.Kind kind)
                {
                    return verifier.count(kind);
                }

                public boolean tooManyDifferences()
                {
                    return verifier.tooManyDifferences();
                }

                public String summary()
                {
                    return verifier.summary();
                }
            };
        }

        /**
         * The buckets this node holds, probed locally, scaled to all {@code buckets}; 0 if it holds none (the estimate
         * is then the soak alone, and the replicas' limit still stops a copy that takes too long).
         */
        @Override
        public long estimatedBytes(TableMetadata logical, TableMetadata physical, ByteBuffer key, int incarnation, int buckets)
        {
            if (!Keyspace.isInitialized())
                return 0;
            long bytes = 0;
            int held = 0;
            ClusterMetadata cm = ClusterMetadata.current();
            for (int bucket = 0; bucket < buckets; bucket++)
            {
                ByteBuffer bucketKey = RowMapper.physicalKey(logical, physical, key, incarnation, bucket).getKey();
                if (!NaturalReplicas.of(cm, physical, bucketKey).contains(cm.myNodeId()))
                    continue;
                try
                {
                    bytes += PartitionProbe.run(physical, bucketKey, DatabaseDescriptor.getAutonomicConfig().layout.max_probe_rows).estimatedBytes();
                    held++;
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    return 0;
                }
            }
            return held == 0 ? 0 : bytes * buckets / held;
        }

        @Override
        public Map<NodeId, BasePurge.Published> purgeReasons(ClusterMetadata cm, TableId logical, ByteBuffer key, Set<NodeId> nodes)
        {
            Map<NodeId, BasePurge.Published> out = new LinkedHashMap<>();
            String keyHex = ByteBufferUtil.bytesToHex(key);
            for (NodeId id : nodes)
            {
                InetAddressAndPort endpoint = cm.directory.endpoint(id);
                EndpointState state = endpoint == null ? null : Gossiper.instance.getEndpointStateForEndpoint(endpoint);
                VersionedValue value = state == null ? null : state.getApplicationState(BasePurge.PUBLISHED_STATE);
                if (value == null)
                    continue;
                for (BasePurge.Published p : BasePurge.parsePublished(value.value))
                    if (p.logical.equals(logical) && p.keyHex.equals(keyHex))
                        out.put(id, p);
            }
            return out;
        }

        @Override
        public long writeTimeoutMillis()
        {
            return DatabaseDescriptor.getWriteRpcTimeout(TimeUnit.MILLISECONDS);
        }

        @Override
        public long nowMillis()
        {
            return Clock.Global.currentTimeMillis();
        }

        @Override
        public KnowledgeView knowledge()
        {
            return AutonomicService.instance.knowledge();
        }
    }
}
