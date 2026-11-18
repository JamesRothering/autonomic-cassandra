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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
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
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.layout.AdoptKey;
import org.apache.cassandra.autonomic.layout.AdvanceLayout;
import org.apache.cassandra.autonomic.layout.Eligibility;
import org.apache.cassandra.autonomic.layout.EpochWatermarks;
import org.apache.cassandra.autonomic.layout.FinishKey;
import org.apache.cassandra.autonomic.layout.KeyState;
import org.apache.cassandra.autonomic.layout.LayoutEvents;
import org.apache.cassandra.autonomic.layout.LayoutHeader;
import org.apache.cassandra.autonomic.layout.LayoutProfiler;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.LayoutVerifier;
import org.apache.cassandra.autonomic.layout.LayoutWrites;
import org.apache.cassandra.autonomic.layout.NaturalReplicas;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.PhysicalTables;
import org.apache.cassandra.autonomic.layout.RegisterKey;
import org.apache.cassandra.autonomic.layout.ReleaseKey;
import org.apache.cassandra.autonomic.layout.RemoveKey;
import org.apache.cassandra.autonomic.layout.RowMapper;
import org.apache.cassandra.autonomic.layout.SplitEvaluation;
import org.apache.cassandra.autonomic.layout.TableLayout;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTransformation;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.LocalizeString;

/**
 * The owner of a partition split (adaptive-schema design 4.1 to 4.9, T6.9): moves one key of one logical table
 * through the phases of the layout registry. Lane {@link Lane#SCHEMA_LAYOUT}, irreversible (it has a commit point),
 * manual approval by default. Target {@code PARTITION:<ks>.<table>:<key hex>}; params {@code buckets} and {@code mode}
 * ({@code split}, or {@code resume} for a key whose migration has no live action).
 *
 * <p><b>Source of truth.</b> The registry holds the key's phase, epochs and backfill reports; every registry change is
 * committed together with the claim (FC2), so the claim's phase is the key's phase. The claim's state holds only what
 * the registry cannot: the ids written ahead of the overflow table's creation, the time at which a phase was
 * acknowledged by every node (for the waits that start then), verification results, the base tombstone's timestamp
 * written ahead of FinishKey, and operator requests (abort, commit approval). Nothing kept in memory is needed by a
 * new owner: an equivalence check in progress is kept in memory and simply starts again.
 *
 * <p><b>Rule O1.</b> No {@code AdvanceLayout}, {@code RemoveKey} or {@code FinishKey} is returned before
 * {@link Env#acknowledged} is true for the current {@link KeyState#phaseEpoch}: every directory member that has not
 * left has reported that epoch. There is no timeout that proceeds without it. A node that does not answer holds the
 * key where it is; before the commit point the deadline then turns the migration to the abort path, which obeys O1
 * too and therefore also waits, in a phase where the base copy is authoritative.
 *
 * <p><b>Phases of the claim.</b> {@code CLAIMED}: the layout and overflow table ids are written ahead
 * ({@code CREATING}). {@code CREATING}: the overflow table is created (unless the table already has a layout), then
 * {@code RegisterKey}. From there the claim has the key's phase, then {@code KEY_REMOVED} after {@code RemoveKey} or
 * {@code STEADY} after {@code FinishKey}. {@code ADOPTING} (resume) and {@code RECOVERING} (takeover) end with
 * {@code AdoptKey}, which hands the key to this action.
 *
 * <p><b>Forward</b> (design 4.1; every wait is a STAY): ANNOUNCED, after every node acknowledged it, waits the
 * drain of ADR-028; DUAL_WRITE waits for acknowledgement; BACKFILL waits until {@code backfillDone} holds every natural
 * replica of the key at the current generation and the placement has not changed since the generation started (4.7,
 * else a new generation); VERIFY runs {@link LayoutVerifier} at ALL (ADR-030: CONTENT_DIFFERS and
 * MISSING_FROM_OVERFLOW send the key back to BACKFILL with a new generation up to {@code verify_retries} times, then
 * abort; MISSING_FROM_BASE is tolerated and counted); CUTOVER soaks for {@code cutover_soak} after acknowledgement
 * and, with {@code commit_approval: manual}, waits for a second approval; NEW_ONLY is the commit point, taken only while
 * {@link #commitWindowEnd} has not passed; CLEANUP waits {@code straggler_window} after acknowledgement, starts the
 * reconcile generation and waits for it; then the base tombstone's timestamp is written ahead, completion and
 * placement are checked again on every step until {@code FinishKey} (a change starts a new reconcile generation), and
 * {@code FinishKey} itself is rejected unless every natural replica at its epoch reported (ADR-033). In STEADY, once
 * every node acknowledged STEADY, the base partition tombstone is written at ALL and {@code ReleaseKey} ends the
 * action's ownership of the key. The tombstone is written after FinishKey, not before as 4.1 lists it: in CLEANUP a
 * replica that becomes a natural replica of the key without having reported would run the reconcile pass over a base
 * partition that already holds the tombstone, and copy it into every bucket.
 *
 * <p><b>Topology</b> (4.7): while a multi-step metadata operation is in progress nothing moves forward.
 *
 * <p><b>Deadlines</b> (4.5, ADR-031). The commit point is taken only before {@link #commitWindowEnd}: the replicas'
 * limit less {@code straggler_window}, {@code reconcile_budget} and two acknowledgement polls; later, CUTOVER rolls
 * back. A table whose {@code gc_grace_seconds} cannot fit {@code cutover_soak} and those windows is refused by
 * {@link #validate}. Before the commit point, at
 * {@code dualWriteSince + min(max_migration_duration, gc_grace/2 - straggler_window)}, and always strictly before
 * the replicas' own limit {@code dualWriteSince + gc_grace/2 - write timeout}, the migration takes the abort path
 * (in ANNOUNCED: {@code max_migration_duration} after registration). After the commit point there is no abort: a
 * reconcile pass that has not completed when the replicas' limit is reached ends the action FAILED with detail
 * {@value #RECONCILE_PAST_DEADLINE}; the key stays in CLEANUP, which routes to the overflow copy, and the base
 * tombstone is not written.
 *
 * <p><b>Abort</b> (4.9): ANNOUNCED, DUAL_WRITE, BACKFILL and VERIFY go to ABORTING; CUTOVER through ROLLBACK_DUAL.
 * In ABORTING, once every node acknowledged it (every coordinator then reads and writes only the base table), the N
 * bucket partitions of this incarnation are deleted at ALL (best effort), then {@code RemoveKey}, then, if the layout
 * has no key left, the overflow table is dropped.
 *
 * <p><b>Stop, restart and takeover</b> (4.4, FC5): {@link #pastCommitPoint} is true in NEW_ONLY and CLEANUP; a kill,
 * shutdown or restart there releases the claim as COMMITTED_CLEANUP_PENDING and the key stays in its C3 phase, owned by
 * an action without a claim: {@link #orphans orphaned}. Diagnosis {@value #ORPHANED} is raised for it, and the
 * preferred proposer proposes its adoption ({@code mode resume}, as {@code nodetool autonomic layout resume} does);
 * such a resume is a {@link #continuation}, exempt from the cooldown the stopped action left. Before the commit point,
 * {@link #abort} and {@link #recover} drive the abort path. A new owner adopts the key, then aborts before the commit
 * point and continues forward after it.
 */
public class PartitionSplitActuator implements ClusterActuator
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionSplitActuator.class);

    public static final String NAME = "partition_split";
    public static final String PARAM_BUCKETS = "buckets";
    public static final String PARAM_MODE = "mode";
    public static final String MODE_SPLIT = "split";
    public static final String MODE_RESUME = "resume";

    // phases of the claim that are not phases of the key
    public static final String CREATING = "CREATING";
    public static final String ADOPTING = "ADOPTING";
    /** The claim phase {@code RemoveKey} commits. */
    public static final String KEY_REMOVED = "KEY_REMOVED";

    // claim state
    public static final String S_TABLE = "table";
    public static final String S_LAYOUT = "layout";
    public static final String S_PHYSICAL = "physical";
    public static final String S_BUCKETS = "buckets";
    public static final String S_HASHED = "hashed";
    public static final String S_MODE = "mode";
    /** {@code <phase>/<phase epoch>/<millis>}: when every node had acknowledged that phase epoch. */
    public static final String S_ACKED = "acked";
    /** {@code <generation>/<generation epoch>}: a verification of that generation passed. */
    public static final String S_VERIFIED = "verified";
    /** {@code <generation>/<generation epoch>}: a verification of that generation failed. */
    public static final String S_VERIFY_FAILED = "verify_failed";
    public static final String S_VERIFY_FAILURES = "verify_failures";
    /** MISSING_FROM_BASE rows tolerated by the verification that passed (ADR-030). */
    public static final String S_TOLERATED = "missing_from_base";
    /** Why the migration takes the abort path; set once, never cleared. */
    public static final String S_ABORT = "abort";
    public static final String S_COMMIT_APPROVED = "commit_approved";
    /** Timestamp (microseconds) of the base partition tombstone, written ahead of FinishKey. */
    public static final String S_TOMBSTONE = "tombstone";
    /** {@code <generation>/<generation epoch>}: the reconcile generation was complete and FinishKey follows. */
    public static final String S_FINISHING = "finishing";

    public static final String RECONCILE_PAST_DEADLINE = "RECONCILE_PAST_DEADLINE";
    public static final String TABLE_DROPPED = "TABLE_DROPPED";
    /** Added to the ADR-028 drain. */
    static final long DRAIN_MARGIN_MILLIS = 1000;
    static final int MAX_DELETE_ATTEMPTS = 3;
    private static final int MAX_TEXT = 300;

    private static final Set<Phase> PRE_COMMIT = EnumSet.of(Phase.ANNOUNCED, Phase.DUAL_WRITE, Phase.BACKFILL, Phase.VERIFY, Phase.CUTOVER,
                                                            Phase.ROLLBACK_DUAL, Phase.ABORTING);

    /** One equivalence check; {@link LayoutVerifier} in production. */
    public interface Check
    {
        LayoutVerifier.Status step();

        /** See {@link LayoutVerifier#nextDueMillis}. */
        long nextDueMillis();

        long count(LayoutVerifier.Kind kind);

        boolean tooManyDifferences();

        String summary();
    }

    /** What the actuator does to and reads from the cluster; replaced in unit tests. Actuator thread only. */
    public interface Env
    {
        ClusterMetadata metadata();

        /** Rule O1: every directory member that is not LEFT has reported an epoch at or after {@code epoch}. */
        boolean acknowledged(Epoch epoch);

        /** Creates the overflow table of design 2.1 by ordinary DDL; returns once it is in this node's schema. */
        void createTable(TableMetadata logical, UUID layoutId, TableId physicalId) throws Exception;

        void dropTable(String keyspace, String table) throws Exception;

        /** Deletes the bucket partitions of {@code incarnation} of the key at ALL. */
        void deleteBuckets(TableMetadata logical, TableMetadata physical, TableLayout layout, ByteBuffer key, int incarnation, long timestampMicros) throws Exception;

        /** Writes the partition tombstone of the key in the base table at ALL, bypassing the router. */
        void deleteBase(TableMetadata logical, ByteBuffer key, long timestampMicros) throws Exception;

        /** The natural (read) replicas of the key, as node ids. */
        Set<NodeId> replicas(ClusterMetadata metadata, TableMetadata logical, ByteBuffer key);

        /** The epoch at which the key's replica group was last modified. */
        Epoch replicasModified(ClusterMetadata metadata, TableMetadata logical, ByteBuffer key);

        Check check(ClusterMetadata metadata, TableId logical, ByteBuffer key, int pageRows, long recheckDelayMillis, LongSupplier clock);

        /** ADR-028: the largest of the write, counter write, CAS contention, range and truncate request timeouts. */
        long requestTimeoutsMillis();

        /** The write request timeout; replicas stop copying this long before their limit (BackfillTask). */
        long writeTimeoutMillis();

        /** This node's wall clock, for {@link #validate}, which has no step context. */
        long nowMillis();

        /** The signals of this node (design 7.3); null where there are none, and stages A and B are then not judged. */
        default KnowledgeView knowledge()
        {
            return null;
        }
    }

    private final Env env;
    private final Supplier<AutonomicConfig> config;
    /** The equivalence check of the key, by {@link #checkId}; actuator thread only. */
    private String checkId;
    private Check check;
    private final Map<String, Integer> deleteAttempts = new LinkedHashMap<>();

    public PartitionSplitActuator(Env env, Supplier<AutonomicConfig> config)
    {
        this.env = env;
        this.config = config;
    }

    public static PartitionSplitActuator create(Supplier<AutonomicConfig> config)
    {
        return new PartitionSplitActuator(new NodeEnv(), config);
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

    /** There is a commit point (NEW_ONLY) after which nothing is undone. */
    @Override
    public boolean reversible()
    {
        return false;
    }

    /** NEW_ONLY and CLEANUP: a stop leaves the key routed to the overflow copy, safe indefinitely (FC5). */
    @Override
    public boolean pastCommitPoint(Claim claim)
    {
        return Phase.NEW_ONLY.name().equals(claim.phase) || Phase.CLEANUP.name().equals(claim.phase);
    }

    public static Map<String, String> params(int buckets, String mode)
    {
        Map<String, String> params = new LinkedHashMap<>();
        if (buckets > 0)
            params.put(PARAM_BUCKETS, Integer.toString(buckets));
        params.put(PARAM_MODE, mode);
        return params;
    }

    private AutonomicConfig.LayoutConfig layoutConfig()
    {
        return config.get().layout;
    }

    // ---------------------------------------------------------------------------------------------------------
    // the target
    // ---------------------------------------------------------------------------------------------------------

    /** The key of a target, or why it is not one. */
    static final class Target
    {
        final String keyspace;
        final String table;
        final ByteBuffer key;
        final String rejection;

        private Target(String keyspace, String table, ByteBuffer key, String rejection)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.key = key;
            this.rejection = rejection;
        }

        static Target of(Subject subject)
        {
            if (subject.kind != Subject.Kind.PARTITION || subject.keyspace == null || subject.table == null || subject.detail == null)
                return new Target(null, null, null, "target must be a partition, PARTITION:<keyspace>.<table>:<key hex>");
            ByteBuffer key;
            try
            {
                key = ByteBufferUtil.hexToBytes(subject.detail);
            }
            catch (RuntimeException e)
            {
                return new Target(null, null, null, "the partition key '" + text(subject.detail) + "' is not hex");
            }
            if (key.remaining() == 0 || key.remaining() > LayoutRegistry.MAX_KEY_BYTES)
                return new Target(null, null, null, "the partition key has " + key.remaining() + " bytes, the limit is " + LayoutRegistry.MAX_KEY_BYTES);
            if (subject.asString().getBytes(StandardCharsets.UTF_8).length > Claim.MAX_TARGET_BYTES)
                return new Target(null, null, null, "the target is longer than the " + Claim.MAX_TARGET_BYTES + " bytes a claim can hold");
            return new Target(subject.keyspace, subject.table, key, null);
        }

        static Target of(Claim claim)
        {
            return of(Subject.parse(claim.target));
        }
    }

    public static Subject target(String keyspace, String table, String keyHex)
    {
        return Subject.partition(keyspace, table, LocalizeString.toLowerCaseLocalized(keyHex, Locale.ROOT));
    }

    // ---------------------------------------------------------------------------------------------------------
    // validate: the same path for the operator's split and for a policy's proposal
    // ---------------------------------------------------------------------------------------------------------

    @Override
    public Optional<String> validate(Action action, KnowledgeView knowledge)
    {
        Target t = Target.of(action.target);
        if (t.rejection != null)
            return Optional.of(t.rejection);
        String mode = action.params.getOrDefault(PARAM_MODE, MODE_SPLIT);
        if (!MODE_SPLIT.equals(mode) && !MODE_RESUME.equals(mode))
            return Optional.of("unknown mode " + text(mode));
        ClusterMetadata cm = env.metadata();
        KeyspaceMetadata ksm = cm.schema.getKeyspaces().getNullable(t.keyspace);
        TableMetadata logical = ksm == null ? null : ksm.tables.getNullable(t.table);
        if (logical == null)
            return Optional.of("table " + t.keyspace + '.' + t.table + " does not exist");
        LayoutRegistry registry = LayoutRegistry.of(cm);
        TableLayout layout = registry.layout(logical.id);
        KeyState state = layout == null ? null : layout.keyState(t.key);
        if (MODE_RESUME.equals(mode))
            return validateResume(cm, logical, t.key, state);

        AutonomicConfig.LayoutConfig lc = layoutConfig();
        if (lc.excluded_tables != null && lc.excluded_tables.contains(logical.keyspace + '.' + logical.name))
            return Optional.of(logical.keyspace + '.' + logical.name + " is in layout.excluded_tables");
        if (registry.byLogical.values().stream().anyMatch(l -> l.physical.equals(logical.id)))
            return Optional.of(logical.keyspace + '.' + logical.name + " is an overflow table");
        String blocked = Eligibility.describe(Eligibility.structural(logical, ksm));
        if (blocked != null)
            return Optional.of("table " + logical.keyspace + '.' + logical.name + " is not eligible: " + blocked);
        String lwt = LayoutProfiler.runtime(logical, profile(logical.id, t.key));
        if (lwt != null)
            return Optional.of("table " + logical.keyspace + '.' + logical.name + " is not eligible: " + lwt);
        try
        {
            logical.partitionKeyType.validate(t.key);
        }
        catch (RuntimeException e)
        {
            return Optional.of("the key is not a valid partition key of " + logical.keyspace + '.' + logical.name + ": " + text(e.getMessage()));
        }
        String noTime = migrationDoesNotFit(logical);
        if (noTime != null)
            return Optional.of(noTime);
        if (state != null)
            return Optional.of("the key is already in the layout of " + logical.keyspace + '.' + logical.name + " in phase " + state.phase);
        int buckets;
        try
        {
            buckets = Integer.parseInt(action.params.getOrDefault(PARAM_BUCKETS, Integer.toString(lc.default_buckets)));
        }
        catch (NumberFormatException e)
        {
            return Optional.of("buckets must be a number");
        }
        if (buckets < LayoutRegistry.MIN_BUCKETS || buckets > LayoutRegistry.MAX_BUCKETS)
            return Optional.of("buckets " + buckets + " is outside " + LayoutRegistry.MIN_BUCKETS + ".." + LayoutRegistry.MAX_BUCKETS);
        if (layout == null && registry.byLogical.size() >= LayoutRegistry.MAX_LAYOUTS)
            return Optional.of("the registry already has " + LayoutRegistry.MAX_LAYOUTS + " layouts");
        if (layout != null && layout.keys.size() >= LayoutRegistry.MAX_KEYS)
            return Optional.of("the layout of " + logical.keyspace + '.' + logical.name + " already has " + LayoutRegistry.MAX_KEYS + " keys");
        if (layout != null && action.params.containsKey(PARAM_BUCKETS) && layout.buckets != buckets)
            return Optional.of("the layout of " + logical.keyspace + '.' + logical.name + " has " + layout.buckets + " buckets; every key of a table has the same");
        return Optional.empty();
    }

    /**
     * A resume of a key past its commit point (NEW_ONLY, CLEANUP, or STEADY without its base tombstone) continues
     * the migration that was committed; it makes no new change (T6.9 review, finding of T6.13). A resume before the
     * commit point aborts and is new work; a split is always new work.
     */
    @Override
    public boolean continuation(Action action)
    {
        if (!MODE_RESUME.equals(action.params.get(PARAM_MODE)))
            return false;
        Target t = Target.of(action.target);
        if (t.rejection != null)
            return false;
        ClusterMetadata cm = env.metadata();
        TableMetadata logical = cm.schema.getTableMetadata(t.keyspace, t.table);
        TableLayout layout = logical == null ? null : LayoutRegistry.of(cm).layout(logical.id);
        KeyState key = layout == null ? null : layout.keyState(t.key);
        return key != null && key.actionId != null && key.phase.coordinatorClass == 3;
    }

    private static LayoutProfiler.Shape profile(TableId table, ByteBuffer key)
    {
        LayoutProfiler.Watch watch = LayoutProfiler.get(table, key);
        return watch == null ? null : watch.shape();
    }

    private Optional<String> validateResume(ClusterMetadata cm, TableMetadata logical, ByteBuffer key, KeyState state)
    {
        if (state == null)
            return Optional.of("the key is not in the layout of " + logical.keyspace + '.' + logical.name + "; nothing to resume");
        if (state.actionId != null && ClusterLedger.of(cm).claim(state.actionId) != null)
            return Optional.of("the migration of the key is running under action " + state.actionId);
        if (state.actionId == null)
            return Optional.of("the key is split and its base partition tombstone is written; nothing to resume");
        if (state.phase == Phase.CLEANUP && !complete(cm, logical, key, state) && env.nowMillis() >= replicaLimit(state, logical))
            return Optional.of("the key is in CLEANUP and its reconcile pass can no longer complete before the tombstone deadline (ADR-031); "
                               + "it stays routed to the overflow copy and its base copy is not deleted; it needs an operator decision");
        return Optional.empty();
    }

    // ---------------------------------------------------------------------------------------------------------
    // entry points
    // ---------------------------------------------------------------------------------------------------------

    private enum Mode
    {
        /** {@link #step}: the action as proposed. */
        FORWARD,
        /** {@link #recover}: restart or takeover; abort before the commit point, forward after it. */
        RECOVER,
        /** {@link #abort}: kill, shutdown or a step that keeps failing; abort before the commit point, release after. */
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

    /** Write-ahead of everything the later steps and a new owner need to find and clean up what this action creates. */
    private StepResult claimed(Action action, Claim claim, ActionContext ctx)
    {
        Optional<String> invalid = validate(action, null);
        if (invalid.isPresent())
            return StepResult.rolledBack("nothing was done: " + invalid.get());
        Target t = Target.of(action.target);
        ClusterMetadata cm = env.metadata();
        TableMetadata logical = cm.schema.getTableMetadata(t.keyspace, t.table);
        Map<String, String> state = new TreeMap<>(claim.state);
        state.put(S_TABLE, logical.id.toString());
        String mode = action.params.getOrDefault(PARAM_MODE, MODE_SPLIT);
        state.put(S_MODE, mode);
        // the profile the policy proposed the split with: what stages A and B compare with, as one entry
        SplitEvaluation.Baseline baseline = SplitEvaluation.Baseline.fromMap(action.params);
        if (baseline != null)
            state.put(SplitEvaluation.S_BASELINE, baseline.encode());
        TableLayout layout = LayoutRegistry.of(cm).layout(logical.id);
        if (MODE_RESUME.equals(mode))
        {
            state.put(S_PHYSICAL, layout.physical.toString());
            KeyState key = layout.keyState(t.key);
            if (key.phase == Phase.STEADY)
                // finished, and still owned: its base tombstone was not written (a stop between FinishKey and it)
                state.put(S_TOMBSTONE, Long.toString(ctx.nowMillis() * 1000));
            if (PRE_COMMIT.contains(key.phase))
                state.put(S_ABORT, "resumed by an operator before the commit point: the migration is aborted");
            return StepResult.advance(ADOPTING, state, 0);
        }
        if (layout != null)
        {
            // every key of a table shares its layout and overflow table
            state.put(S_LAYOUT, layout.layoutId.toString());
            state.put(S_PHYSICAL, layout.physical.toString());
            state.put(S_BUCKETS, Integer.toString(layout.buckets));
            state.put(S_HASHED, Integer.toString(layout.hashedClusteringColumns));
        }
        else
        {
            // fresh ids: a layout id is never used again with the same physical table (ADR-027)
            state.put(S_LAYOUT, UUID.randomUUID().toString());
            state.put(S_PHYSICAL, TableId.generate().toString());
            state.put(S_BUCKETS, action.params.getOrDefault(PARAM_BUCKETS, Integer.toString(layoutConfig().default_buckets)));
            state.put(S_HASHED, Integer.toString(logical.clusteringColumns().size()));
        }
        return StepResult.advance(CREATING, state, 0);
    }

    // ---------------------------------------------------------------------------------------------------------
    // the step of every later phase
    // ---------------------------------------------------------------------------------------------------------

    /** What one step sees: one metadata snapshot. */
    private static final class View
    {
        final Claim claim;
        final Target target;
        final ClusterMetadata cm;
        final TableMetadata logical;
        final TableLayout layout;
        final KeyState key;

        View(Claim claim, Target target, ClusterMetadata cm, TableMetadata logical, TableLayout layout, KeyState key)
        {
            this.claim = claim;
            this.target = target;
            this.cm = cm;
            this.logical = logical;
            this.layout = layout;
            this.key = key;
        }

        String name()
        {
            return target.keyspace + '.' + target.table + " key 0x" + ByteBufferUtil.bytesToHex(target.key);
        }
    }

    private StepResult drive(Claim claim, ActionContext ctx, Mode mode) throws Exception
    {
        Target t = Target.of(claim);
        if (t.rejection != null)
            return StepResult.failed("partition_split claim with an unusable target " + text(claim.target) + ": " + t.rejection);
        ClusterMetadata cm = env.metadata();
        String tableId = claim.state.get(S_TABLE);
        TableMetadata logical = tableId == null ? cm.schema.getTableMetadata(t.keyspace, t.table)
                                                : cm.schema.getTableMetadata(TableId.fromString(tableId));
        String phase = claim.phase;

        if (ClusterActionExecution.INITIAL_PHASE.equals(phase))
            return StepResult.rolledBack("stopped before anything was written");
        if (CREATING.equals(phase))
        {
            if (logical == null || mode != Mode.FORWARD)
                return undoCreation(claim, cm, logical == null ? "the logical table was dropped before the key was registered" : stopped(mode));
            return creating(claim, ctx, cm, logical, t);
        }
        if (KEY_REMOVED.equals(phase))
            return removed(claim, cm, abortReason(claim, mode));

        TableLayout layout = logical == null ? null : LayoutRegistry.of(cm).layout(logical.id);
        KeyState key = layout == null ? null : layout.keyState(t.key);
        View v = new View(claim, t, cm, logical, layout, key);
        if (logical == null && (ADOPTING.equals(phase) || Claim.RECOVERING.equals(phase)))
            return undoCreation(claim, cm, "the logical table was dropped; nothing to recover");
        if (logical == null)
            return StepResult.failed(TABLE_DROPPED + ": " + t.keyspace + '.' + t.table + " was dropped during the migration; the overflow table went with it");

        if (ADOPTING.equals(phase) || Claim.RECOVERING.equals(phase))
            return adopt(v, ctx, mode);

        if (key == null)
            return StepResult.failed("the claim is in phase " + phase + " but " + v.name() + " is not in the layout registry");
        if (ReleaseKey.CLAIM_PHASE.equals(phase))
            return finished(v, ctx);
        if (key.phase == Phase.STEADY)
            return steady(v, ctx, mode);
        if (!claim.actionId.equals(key.actionId))
            return StepResult.failed(v.name() + " is owned by action " + key.actionId + ", not by this action " + claim.actionId);
        if (!key.phase.name().equals(phase))
            return StepResult.failed("the claim is in phase " + phase + " but " + v.name() + " is in " + key.phase);

        if (PRE_COMMIT.contains(key.phase))
        {
            String abort = mode != Mode.FORWARD ? stopped(mode) : claim.state.get(S_ABORT);
            if (abort == null && key.phase != Phase.ROLLBACK_DUAL && key.phase != Phase.ABORTING)
                abort = deadlinePassed(v, ctx);
            if (abort != null || key.phase == Phase.ROLLBACK_DUAL || key.phase == Phase.ABORTING)
                return abortStep(v, ctx, abort == null ? abortReason(claim, mode) : abort);
            return forward(v, ctx);
        }
        // NEW_ONLY, CLEANUP: past the commit point
        if (mode == Mode.STOP)
            return StepResult.committedCleanupPending(v.name() + " is past its commit point in " + key.phase + "; it stays routed to the overflow copy");
        return forward(v, ctx);
    }

    private static String stopped(Mode mode)
    {
        return mode == Mode.STOP ? "the action was stopped before the commit point" : "the owner was lost or restarted before the commit point";
    }

    private static String abortReason(Claim claim, Mode mode)
    {
        String reason = claim.state.get(S_ABORT);
        return reason != null ? reason : mode == Mode.FORWARD ? "aborted" : stopped(mode);
    }

    // ---------------------------------------------------------------------------------------------------------
    // before registration
    // ---------------------------------------------------------------------------------------------------------

    private StepResult creating(Claim claim, ActionContext ctx, ClusterMetadata cm, TableMetadata logical, Target t) throws Exception
    {
        TableId physicalId = TableId.fromString(claim.state.get(S_PHYSICAL));
        UUID layoutId = UUID.fromString(claim.state.get(S_LAYOUT));
        // what RegisterKey would reject is not retried: a rejected commit is asked again, forever
        String blocked = Eligibility.describe(Eligibility.structural(logical, cm.schema.getKeyspaceMetadata(logical.keyspace)));
        TableLayout existing = LayoutRegistry.of(cm).layout(logical.id);
        if (blocked == null && existing != null && existing.keyState(t.key) != null)
            blocked = "the key was registered by action " + existing.keyState(t.key).actionId;
        if (blocked == null && existing != null && (!existing.layoutId.equals(layoutId) || existing.keys.size() >= LayoutRegistry.MAX_KEYS))
            blocked = "the layout of the table changed since this action started";
        if (blocked == null && existing == null && LayoutRegistry.of(cm).byLogical.size() >= LayoutRegistry.MAX_LAYOUTS)
            blocked = "the registry already has " + LayoutRegistry.MAX_LAYOUTS + " layouts";
        if (blocked != null)
            return undoCreation(claim, cm, "not registered: " + blocked);
        if (!cm.inProgressSequences.isEmpty())
            return StepResult.stay(pollMillis());
        TableMetadata physical = cm.schema.getTableMetadata(physicalId);
        if (physical == null)
        {
            env.createTable(logical, layoutId, physicalId);
            return StepResult.stay(0);
        }
        LayoutHeader header = new LayoutHeader(layoutId, logical.id, physicalId, logical.keyspace, logical.name, physical.name,
                                               TableLayout.Transform.HASH_SPLIT, LayoutRegistry.HASH_ALGO,
                                               Integer.parseInt(claim.state.get(S_BUCKETS)), Integer.parseInt(claim.state.get(S_HASHED)));
        RegisterKey register = new RegisterKey(claim.actionId, claim.owner, claim.step, header, t.key, ctx.nowMillis(), leaseMillis());
        return StepResult.commit(register.wrap(), Phase.ANNOUNCED.name(), claim.state, 0);
    }

    /** Before RegisterKey, or after RemoveKey: drops the overflow table if no layout uses it. */
    private StepResult undoCreation(Claim claim, ClusterMetadata cm, String why) throws Exception
    {
        String drop = dropUnusedOverflow(claim, cm);
        return drop == null ? StepResult.rolledBack(why) : StepResult.stay(0);
    }

    /** Null once there is nothing to drop; otherwise the drop was submitted. */
    private String dropUnusedOverflow(Claim claim, ClusterMetadata cm) throws Exception
    {
        String id = claim.state.get(S_PHYSICAL);
        if (id == null)
            return null;
        TableId physicalId = TableId.fromString(id);
        for (TableLayout l : LayoutRegistry.of(cm).byLogical.values())
            if (l.physical.equals(physicalId))
                return null;
        TableMetadata physical = cm.schema.getTableMetadata(physicalId);
        if (physical == null)
            return null;
        env.dropTable(physical.keyspace, physical.name);
        return physical.name;
    }

    private StepResult removed(Claim claim, ClusterMetadata cm, String why) throws Exception
    {
        return undoCreation(claim, cm, why);
    }

    // ---------------------------------------------------------------------------------------------------------
    // takeover and resume
    // ---------------------------------------------------------------------------------------------------------

    private StepResult adopt(View v, ActionContext ctx, Mode mode) throws Exception
    {
        Claim claim = v.claim;
        if (v.key == null)
            // never registered, or already removed: only the overflow table may be left
            return undoCreation(claim, v.cm, "the key is not in the layout registry; nothing to recover");
        if (v.key.phase == Phase.STEADY && v.key.actionId == null)
            // finished and released by its action: nothing is left to do
            return finished(v, ctx);
        if (v.key.phase == Phase.STEADY && !claim.state.containsKey(S_TOMBSTONE))
        {
            // owned, so its base tombstone may never have been written: write it again, after the adoption
            Map<String, String> state = new TreeMap<>(claim.state);
            state.put(S_TOMBSTONE, Long.toString(ctx.nowMillis() * 1000));
            return StepResult.advance(claim.phase, state, 0);
        }
        if (mode == Mode.STOP && !PRE_COMMIT.contains(v.key.phase) && v.key.phase != Phase.STEADY)
            return StepResult.committedCleanupPending(v.name() + " is past its commit point in " + v.key.phase + " and was not adopted");
        if (claim.actionId.equals(v.key.actionId))
            return StepResult.failed("the claim is in phase " + claim.phase + " but already owns " + v.name());
        AdoptKey adopt = new AdoptKey(claim.actionId, claim.owner, claim.step, v.logical.id, v.target.key, v.key.phase, v.key.actionId,
                                      ctx.nowMillis(), leaseMillis());
        return StepResult.commit(adopt.wrap(), v.key.phase.name(), claim.state, 0);
    }

    // ---------------------------------------------------------------------------------------------------------
    // forward
    // ---------------------------------------------------------------------------------------------------------

    private StepResult forward(View v, ActionContext ctx) throws Exception
    {
        KeyState key = v.key;
        if (!v.cm.inProgressSequences.isEmpty())
            return hold(v, "a metadata operation is in progress (design 4.7)");
        switch (key.phase)
        {
            case ANNOUNCED:
            {
                StepResult acked = ackedFor(v, ctx);
                if (acked != null)
                    return acked;
                long drain = Math.max(layoutConfig().cas_drain.toMilliseconds(), env.requestTimeoutsMillis()) + DRAIN_MARGIN_MILLIS;
                long remaining = ackedAt(v) + drain - ctx.nowMillis();
                if (remaining > 0)
                    return StepResult.stay(Math.min(remaining, pollMillis()));
                return advanceKey(v, ctx, Phase.DUAL_WRITE, key.generation);
            }
            case DUAL_WRITE:
                return advanceKey(v, ctx, Phase.BACKFILL, key.generation);
            case BACKFILL:
                if (placementChanged(v))
                    return advanceKey(v, ctx, Phase.BACKFILL, key.generation + 1);
                if (!complete(v.cm, v.logical, v.target.key, key))
                    return StepResult.stay(pollMillis());
                return advanceKey(v, ctx, Phase.VERIFY, key.generation);
            case VERIFY:
                return verify(v, ctx);
            case CUTOVER:
            {
                StepResult acked = ackedFor(v, ctx);
                if (acked != null)
                    return acked;
                long remaining = ackedAt(v) + layoutConfig().cutover_soak.toMilliseconds() - ctx.nowMillis();
                if (remaining > 0)
                    return StepResult.stay(Math.min(remaining, pollMillis()));
                // stage A (design 7.3): a failure takes the abort path, which in CUTOVER goes through ROLLBACK_DUAL
                SplitEvaluation.Result stageA = stageA(v, ctx);
                if (!stageA.ok)
                {
                    Map<String, String> state = new TreeMap<>(v.claim.state);
                    state.put(S_ABORT, text("stage A failed: " + stageA.detail));
                    return StepResult.advance(v.claim.phase, state, 0);
                }
                // the commit point only while the reconcile pass can still end before the replicas' limit (ADR-031)
                long window = commitWindowEnd(key, v.logical);
                if (ctx.nowMillis() >= window)
                {
                    Map<String, String> state = new TreeMap<>(v.claim.state);
                    state.put(S_ABORT, text("the commit window closed at " + window + ": straggler_window " + layoutConfig().straggler_window
                                            + " and reconcile_budget " + layoutConfig().reconcile_budget + " no longer end before the replicas' "
                                            + "tombstone deadline " + replicaLimit(key, v.logical)));
                    return StepResult.advance(v.claim.phase, state, 0);
                }
                if (manualCommitApproval() && !"true".equals(v.claim.state.get(S_COMMIT_APPROVED)))
                    return StepResult.stay(Math.max(1, Math.min(pollMillis(), window - ctx.nowMillis())));
                // the commit point
                return advanceKey(v, ctx, Phase.NEW_ONLY, key.generation);
            }
            case NEW_ONLY:
                return advanceKey(v, ctx, Phase.CLEANUP, key.generation);
            case CLEANUP:
                return cleanup(v, ctx);
            default:
                return StepResult.failed("no forward step from phase " + key.phase);
        }
    }

    private SplitEvaluation.Result stageA(View v, ActionContext ctx)
    {
        KnowledgeView knowledge = env.knowledge();
        if (knowledge == null)
            return SplitEvaluation.stageA(null, null, layoutConfig());
        SplitEvaluation.Window window = SplitEvaluation.Window.of(knowledge, keySubject(v), ackedAt(v), ctx.nowMillis());
        return SplitEvaluation.stageA(SplitEvaluation.Baseline.fromMap(v.claim.state), window, layoutConfig());
    }

    /** Stage B over the last {@code verify_window}, ending now: the key has been split for at least {@code straggler_window}. */
    private SplitEvaluation.Result stageB(View v, ActionContext ctx)
    {
        KnowledgeView knowledge = env.knowledge();
        if (knowledge == null)
            return SplitEvaluation.stageB(null, null, Double.NaN, layoutConfig());
        long window = layoutConfig().verify_window.toMilliseconds();
        Subject key = keySubject(v);
        SplitEvaluation.Window signals = SplitEvaluation.Window.of(knowledge, key, ctx.nowMillis() - window, ctx.nowMillis());
        double share = SplitEvaluation.busiestShare(knowledge, key, v.layout.keyspace, v.layout.physicalName, window);
        return SplitEvaluation.stageB(SplitEvaluation.Baseline.fromMap(v.claim.state), signals, share, layoutConfig());
    }

    private static Subject keySubject(View v)
    {
        return Subject.partition(v.target.keyspace, v.target.table, ByteBufferUtil.bytesToHex(v.target.key));
    }

    private StepResult verify(View v, ActionContext ctx)
    {
        KeyState key = v.key;
        Map<String, String> state = v.claim.state;
        String generation = key.generation + "/" + key.generationEpoch.getEpoch();
        if (placementChanged(v))
            return advanceKey(v, ctx, Phase.BACKFILL, key.generation + 1);
        if (generation.equals(state.get(S_VERIFIED)))
            return advanceKey(v, ctx, Phase.CUTOVER, key.generation);
        if (generation.equals(state.get(S_VERIFY_FAILED)))
            return advanceKey(v, ctx, Phase.BACKFILL, key.generation + 1);

        String id = v.claim.actionId + "/" + key.incarnation + '/' + generation;
        if (!id.equals(checkId) || check == null)
        {
            checkId = id;
            check = env.check(v.cm, v.logical.id, v.target.key, layoutConfig().verify_page_rows,
                              layoutConfig().verify_recheck_delay.toMilliseconds(), ctx::nowMillis);
        }
        LayoutVerifier.Status status;
        // a bounded unit under action_step_timeout: pages until half of the step's time is used
        long budgetEnd = ctx.nowMillis() + Math.max(1, (ctx.stepDeadlineMillis() - ctx.nowMillis()) / 2);
        try
        {
            do
            {
                status = check.step();
            }
            while (status == LayoutVerifier.Status.RUNNING && check.nextDueMillis() == 0 && ctx.nowMillis() < budgetEnd && !ctx.killed());
        }
        catch (RuntimeException e)
        {
            // a read at ALL that failed (a replica down or slow): start over later; the deadline bounds the wait
            logger.info("Autonomic partition split of {}: verification read failed, starting it again: {}", v.name(), e.toString());
            check = null;
            checkId = null;
            return StepResult.stay(pollMillis());
        }
        if (status == LayoutVerifier.Status.RUNNING)
        {
            long due = check.nextDueMillis();
            return StepResult.stay(due == 0 ? 0 : Math.max(1, Math.min(pollMillis(), due - ctx.nowMillis())));
        }
        Check done = check;
        check = null;
        checkId = null;
        long failing = done.count(LayoutVerifier.Kind.CONTENT_DIFFERS) + done.count(LayoutVerifier.Kind.MISSING_FROM_OVERFLOW);
        Map<String, String> next = new TreeMap<>(state);
        if (failing == 0 && !done.tooManyDifferences())
        {
            // ADR-030: rows only in the overflow copy are unacknowledged writes; tolerated, and counted
            next.put(S_VERIFIED, generation);
            next.put(S_TOLERATED, Long.toString(done.count(LayoutVerifier.Kind.MISSING_FROM_BASE)));
            logger.info("Autonomic partition split of {}: verification of generation {} passed: {}", v.name(), key.generation, done.summary());
            return StepResult.advance(Phase.VERIFY.name(), next, 0);
        }
        int failures = Integer.parseInt(state.getOrDefault(S_VERIFY_FAILURES, "0")) + 1;
        next.put(S_VERIFY_FAILURES, Integer.toString(failures));
        logger.warn("Autonomic partition split of {}: verification of generation {} failed ({} of {}): {}", v.name(), key.generation,
                    failures, layoutConfig().verify_retries + 1, done.summary());
        if (failures > layoutConfig().verify_retries)
        {
            next.put(S_ABORT, text("verification failed " + failures + " times: " + done.summary()));
            LayoutEvents.verifyFailed(v.claim.actionId, v.target.keyspace, v.target.table, ByteBufferUtil.bytesToHex(v.target.key),
                                      text(done.summary()), ctx.nowMillis());
        }
        else
            next.put(S_VERIFY_FAILED, generation);
        return StepResult.advance(Phase.VERIFY.name(), next, 0);
    }

    private StepResult cleanup(View v, ActionContext ctx)
    {
        KeyState key = v.key;
        long limit = replicaLimit(key, v.logical);
        if (!key.backfillDue())
        {
            // entered with the generation of the backfill: wait for stragglers, then start the reconcile generation
            StepResult acked = ackedFor(v, ctx);
            if (acked != null)
                return acked;
            long remaining = ackedAt(v) + layoutConfig().straggler_window.toMilliseconds() - ctx.nowMillis();
            if (ctx.nowMillis() >= limit)
                return reconcilePastDeadline(v, limit, "before the reconcile pass started");
            if (remaining > 0)
                return StepResult.stay(Math.min(remaining, pollMillis()));
            return advanceKey(v, ctx, Phase.CLEANUP, key.generation + 1);
        }
        // every step until FinishKey re-checks completion and placement on its own metadata (ADR-033)
        String generation = key.generation + "/" + key.generationEpoch.getEpoch();
        boolean finishing = generation.equals(v.claim.state.get(S_FINISHING));
        boolean done = complete(v.cm, v.logical, v.target.key, key);
        boolean moved = placementChanged(v);
        if (!done || moved)
        {
            if (ctx.nowMillis() >= limit)
                return reconcilePastDeadline(v, limit, "with " + key.backfillDone.size() + " of " + replicaCount(v) + " replicas reported");
            // a generation that was complete and no longer is: a replica joined since; it runs a pass of its own
            if (moved || finishing)
                return advanceKey(v, ctx, Phase.CLEANUP, key.generation + 1);
            return StepResult.stay(pollMillis());
        }
        if (!finishing)
        {
            // every replica reported the reconcile generation: FinishKey, then the tombstone, whose timestamp is fixed here
            Map<String, String> state = new TreeMap<>(v.claim.state);
            // what verification and the commit approval recorded is not needed past the commit point
            state.keySet().removeAll(List.of(S_VERIFIED, S_VERIFY_FAILED, S_VERIFY_FAILURES, S_COMMIT_APPROVED));
            state.put(S_FINISHING, generation);
            state.put(S_TOMBSTONE, Long.toString(ctx.nowMillis() * 1000));
            return StepResult.advance(Phase.CLEANUP.name(), state, 0);
        }
        if (!env.acknowledged(key.phaseEpoch))
            return waitForAck();
        // FinishKey itself is rejected unless every natural replica at its epoch reported this generation
        FinishKey finish = new FinishKey(v.claim.actionId, v.claim.owner, v.claim.step, v.logical.id, v.target.key, ctx.nowMillis(), leaseMillis());
        return StepResult.commit(finish.wrap(), Phase.STEADY.name(), v.claim.state, 0);
    }

    private StepResult reconcilePastDeadline(View v, long limit, String where)
    {
        return StepResult.failed(RECONCILE_PAST_DEADLINE + ": the reconcile pass of " + v.name() + " cannot complete before the replicas' tombstone deadline "
                                 + limit + " (ADR-031), " + where + ". The key stays in CLEANUP and reads and writes stay on the overflow copy; "
                                 + "the base partition tombstone is not written. An operator must decide (see T6.17)");
    }

    /**
     * STEADY, owned by this action: every node acknowledged it, then the base partition tombstone at ALL (ADR-033),
     * then {@code ReleaseKey}, after which the key is owned by nobody.
     */
    private StepResult steady(View v, ActionContext ctx, Mode mode)
    {
        Claim claim = v.claim;
        if (v.key.actionId == null)
            // released already: the tombstone was written
            return finished(v, ctx);
        if (!claim.actionId.equals(v.key.actionId))
            return StepResult.failed(v.name() + " is owned by action " + v.key.actionId + ", not by this action " + claim.actionId);
        if (!Phase.STEADY.name().equals(claim.phase))
            return StepResult.failed("the claim is in phase " + claim.phase + " but " + v.name() + " is STEADY");
        String tombstone = claim.state.get(S_TOMBSTONE);
        if (tombstone == null)
        {
            Map<String, String> state = new TreeMap<>(claim.state);
            state.put(S_TOMBSTONE, Long.toString(ctx.nowMillis() * 1000));
            return StepResult.advance(claim.phase, state, 0);
        }
        // no replica has a pass due once every node has STEADY; the wait lets a chunk read before that be written
        StepResult acked = ackedFor(v, ctx);
        if (acked != null)
            return acked;
        long remaining = ackedAt(v) + env.writeTimeoutMillis() - ctx.nowMillis();
        if (remaining > 0)
            return StepResult.stay(remaining);
        try
        {
            env.deleteBase(v.logical, v.target.key, Long.parseLong(tombstone));
        }
        catch (Exception e)
        {
            logger.info("Autonomic partition split of {}: base partition tombstone not written yet: {}", v.name(), e.toString());
            // a stop leaves the key owned and orphaned: the preferred proposer adopts it, or `layout resume`, and writes it again
            return mode == Mode.STOP ? StepResult.committedCleanupPending(v.name() + " is STEADY; its base partition tombstone was not written: " + e)
                                     : StepResult.stay(pollMillis());
        }
        ReleaseKey release = new ReleaseKey(claim.actionId, claim.owner, claim.step, v.logical.id, v.target.key, ctx.nowMillis(), leaseMillis());
        return StepResult.commit(release.wrap(), ReleaseKey.CLAIM_PHASE, claim.state, 0);
    }

    /** The end of a split: the base tombstone is written and the key released. Stage B decides the verdict. */
    private StepResult finished(View v, ActionContext ctx)
    {
        SplitEvaluation.Result stageB = stageB(v, ctx);
        String tombstone = v.claim.state.get(S_TOMBSTONE);
        return StepResult.committed(stageB.verdict, v.name() + " is split into " + v.layout.buckets + " buckets of " + v.layout.keyspace + '.'
                                                    + v.layout.physicalName + "; " + S_TOLERATED + '=' + v.claim.state.getOrDefault(S_TOLERATED, "0")
                                                    + "; stage B " + stageB.verdict + ": " + stageB.detail
                                                    + (tombstone == null ? "" : "; base partition tombstone written at " + tombstone));
    }

    // ---------------------------------------------------------------------------------------------------------
    // abort (design 4.9)
    // ---------------------------------------------------------------------------------------------------------

    private StepResult abortStep(View v, ActionContext ctx, String why) throws Exception
    {
        KeyState key = v.key;
        switch (key.phase)
        {
            case ANNOUNCED:
            case DUAL_WRITE:
            case BACKFILL:
            case VERIFY:
                return recordedAbort(v, why, () -> advanceKey(v, ctx, Phase.ABORTING, key.generation));
            case CUTOVER:
                return recordedAbort(v, why, () -> advanceKey(v, ctx, Phase.ROLLBACK_DUAL, key.generation));
            case ROLLBACK_DUAL:
                return advanceKey(v, ctx, Phase.ABORTING, key.generation);
            case ABORTING:
            {
                // every coordinator now reads and writes the base table only: nobody reads the copy being deleted
                if (!env.acknowledged(key.phaseEpoch))
                    return waitForAck();
                String attempt = v.claim.actionId + "/" + key.incarnation;
                int attempts = deleteAttempts.getOrDefault(attempt, 0);
                TableMetadata physical = v.cm.schema.getTableMetadata(v.layout.physical);
                if (attempts < MAX_DELETE_ATTEMPTS && physical != null)
                {
                    try
                    {
                        env.deleteBuckets(v.logical, physical, v.layout, v.target.key, key.incarnation, ctx.nowMillis() * 1000);
                        deleteAttempts.put(attempt, MAX_DELETE_ATTEMPTS);
                    }
                    catch (Exception e)
                    {
                        deleteAttempts.put(attempt, attempts + 1);
                        logger.info("Autonomic partition split of {}: deleting the buckets of incarnation {} failed ({} of {}): {}",
                                    v.name(), key.incarnation, attempts + 1, MAX_DELETE_ATTEMPTS, e.toString());
                        return StepResult.stay(pollMillis());
                    }
                }
                deleteAttempts.remove(attempt);
                RemoveKey remove = new RemoveKey(v.claim.actionId, v.claim.owner, v.claim.step, v.logical.id, v.target.key, ctx.nowMillis(), leaseMillis());
                return StepResult.commit(remove.wrap(), KEY_REMOVED, v.claim.state, 0);
            }
            default:
                return StepResult.failed("no abort from phase " + key.phase);
        }
    }

    /** The reason of an abort is written to the claim before the first abort transition, so the audit trail keeps it. */
    private StepResult recordedAbort(View v, String why, Supplier<StepResult> transition)
    {
        if (v.claim.state.containsKey(S_ABORT))
            return transition.get();
        Map<String, String> state = new TreeMap<>(v.claim.state);
        state.put(S_ABORT, text(why));
        return StepResult.advance(v.claim.phase, state, 0);
    }

    // ---------------------------------------------------------------------------------------------------------
    // deadlines
    // ---------------------------------------------------------------------------------------------------------

    /** The time at which the replicas stop copying for the key (ADR-031, {@code BackfillTask.pastDeadline}). */
    long replicaLimit(KeyState key, TableMetadata logical)
    {
        long limit = key.backfillLimitMillis(logical.params.gcGraceSeconds);
        return limit == Long.MIN_VALUE ? Long.MIN_VALUE : limit - env.writeTimeoutMillis();
    }

    /** The owner's deadline before the commit point: design 4.5, and strictly before the replicas' limit. */
    long preCommitDeadline(KeyState key, TableMetadata logical)
    {
        AutonomicConfig.LayoutConfig lc = layoutConfig();
        long max = lc.max_migration_duration.toMilliseconds();
        if (key.dualWriteSinceMillis <= 0)
            return key.phaseSinceMillis + max;
        long byDesign = key.dualWriteSinceMillis + Math.min(max, logical.params.gcGraceSeconds * 1000L / 2 - lc.straggler_window.toMilliseconds());
        return Math.min(byDesign, replicaLimit(key, logical) - 1);
    }

    /** Two all-node acknowledgements (NEW_ONLY, CLEANUP) come between the commit and the straggler wait. */
    static final int COMMIT_ACK_POLLS = 2;

    /**
     * The latest time of the commit point (T6.9 review): after it, {@code straggler_window}, the acknowledgements
     * before it and {@code reconcile_budget} no longer end before the replicas' limit. The acknowledgements are
     * bounded below only (rule O1 has no timeout); {@code reconcile_budget} is the margin for the rest.
     */
    long commitWindowEnd(KeyState key, TableMetadata logical)
    {
        long limit = replicaLimit(key, logical);
        if (limit == Long.MIN_VALUE)
            return Long.MIN_VALUE;
        return limit - afterCommitMillis();
    }

    private long afterCommitMillis()
    {
        AutonomicConfig.LayoutConfig lc = layoutConfig();
        return lc.straggler_window.toMilliseconds() + lc.reconcile_budget.toMilliseconds() + COMMIT_ACK_POLLS * pollMillis();
    }

    /**
     * Null if one migration fits {@code gc_grace_seconds} under the configured windows: from DUAL_WRITE at least
     * {@code cutover_soak} before the commit point, then {@link #afterCommitMillis}, all before half of
     * {@code gc_grace_seconds} less the write timeout (the replicas' limit, ADR-031). Otherwise why not, with the numbers.
     */
    String migrationDoesNotFit(TableMetadata logical)
    {
        AutonomicConfig.LayoutConfig lc = layoutConfig();
        long available = logical.params.gcGraceSeconds * 1000L / 2 - env.writeTimeoutMillis();
        long needed = lc.cutover_soak.toMilliseconds() + afterCommitMillis();
        if (available > needed)
            return null;
        return "gc_grace_seconds " + logical.params.gcGraceSeconds + " of " + logical.keyspace + '.' + logical.name + " leaves no time for a migration: "
               + "half of it less the write timeout is " + available + " ms, and cutover_soak " + lc.cutover_soak + " + straggler_window "
               + lc.straggler_window + " + reconcile_budget " + lc.reconcile_budget + " + " + COMMIT_ACK_POLLS + " x ack_poll_interval "
               + lc.ack_poll_interval + " need " + needed + " ms";
    }

    private String deadlinePassed(View v, ActionContext ctx)
    {
        long deadline = preCommitDeadline(v.key, v.logical);
        if (ctx.nowMillis() < deadline)
            return null;
        return v.key.dualWriteSinceMillis <= 0
               ? "max_migration_duration passed in ANNOUNCED (deadline " + deadline + ')'
               : "backfill_deadline passed in " + v.key.phase + " (deadline " + deadline + ", before the replicas' tombstone deadline "
                 + replicaLimit(v.key, v.logical) + ')';
    }

    // ---------------------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------------------

    /** O1, then the transition with the claim advanced in the same commit. */
    private StepResult advanceKey(View v, ActionContext ctx, Phase to, int generation)
    {
        if (!env.acknowledged(v.key.phaseEpoch))
            return waitForAck();
        AdvanceLayout advance = new AdvanceLayout(v.claim.actionId, v.claim.owner, v.claim.step, v.logical.id, v.target.key, v.key.phase, to,
                                                  generation, ctx.nowMillis(), leaseMillis());
        return StepResult.commit(advance.wrap(), to.name(), v.claim.state, 0);
    }

    private StepResult waitForAck()
    {
        return StepResult.stay(pollMillis());
    }

    private StepResult hold(View v, String why)
    {
        logger.debug("Autonomic partition split of {} holds in {}: {}", v.name(), v.claim.phase, why);
        return StepResult.stay(pollMillis());
    }

    /**
     * Null once the time at which every node acknowledged the current phase epoch is in the claim; otherwise the step
     * that waits for the acknowledgement or records it.
     */
    private StepResult ackedFor(View v, ActionContext ctx)
    {
        if (ackedAt(v) >= 0)
            return null;
        if (!env.acknowledged(v.key.phaseEpoch))
            return waitForAck();
        Map<String, String> state = new TreeMap<>(v.claim.state);
        state.put(S_ACKED, ackPrefix(v) + ctx.nowMillis());
        return StepResult.advance(v.claim.phase, state, 0);
    }

    private static String ackPrefix(View v)
    {
        return v.key.phase.name() + '/' + v.key.phaseEpoch.getEpoch() + '/';
    }

    /** -1 if the claim has no acknowledgement time of the current phase epoch. */
    private static long ackedAt(View v)
    {
        String acked = v.claim.state.get(S_ACKED);
        String prefix = ackPrefix(v);
        if (acked == null || !acked.startsWith(prefix))
            return -1;
        return Long.parseLong(acked.substring(prefix.length()));
    }

    /**
     * Design 4.5 completion test: replicas have a pass due for the current generation and every natural replica of the
     * key reported it.
     */
    boolean complete(ClusterMetadata cm, TableMetadata logical, ByteBuffer key, KeyState state)
    {
        if (!state.backfillDue())
            return false;
        Set<NodeId> replicas = env.replicas(cm, logical, key);
        return !replicas.isEmpty() && state.backfillDone.containsAll(replicas);
    }

    private int replicaCount(View v)
    {
        return env.replicas(v.cm, v.logical, v.target.key).size();
    }

    /** Design 4.5 and 4.7: the key's replica group changed after the generation started. */
    private boolean placementChanged(View v)
    {
        return env.replicasModified(v.cm, v.logical, v.target.key).isAfter(v.key.generationEpoch);
    }

    private boolean manualCommitApproval()
    {
        return !"auto".equalsIgnoreCase(layoutConfig().commit_approval);
    }

    private long pollMillis()
    {
        return Math.max(1, layoutConfig().ack_poll_interval.toMilliseconds());
    }

    private long leaseMillis()
    {
        return config.get().coordination.lease.toMilliseconds();
    }

    static String text(String s)
    {
        return s == null ? "" : s.length() <= MAX_TEXT ? s : s.substring(0, MAX_TEXT);
    }

    // ---------------------------------------------------------------------------------------------------------
    // operator requests, from any node (nodetool autonomic layout abort, approve)
    // ---------------------------------------------------------------------------------------------------------

    /** The claim of the migration of the key, or null. */
    public static Claim claimOf(ClusterLedger ledger, Subject target)
    {
        Claim claim = ledger.claims.get(Lane.SCHEMA_LAYOUT);
        return claim != null && NAME.equals(claim.actuator) && claim.target.equals(target.asString()) ? claim : null;
    }

    /**
     * {@code nodetool autonomic layout abort}: records the request in the claim; the owner takes the abort path at its
     * next step. Refused after the commit point.
     *
     * @return what happened, for the operator
     * @throws IllegalStateException if refused
     */
    public static String requestAbort(ClusterCoordinator coordinator, Supplier<ClusterMetadata> metadata, Subject target, String reason)
    {
        for (int attempt = 0; attempt < 3; attempt++)
        {
            // the key and the claim from one snapshot: a claim read later may already be past the commit point
            ClusterMetadata cm = metadata.get();
            Claim claim = claimOf(ClusterLedger.of(cm), target);
            Target t = Target.of(target);
            if (t.rejection != null)
                throw new IllegalStateException(t.rejection);
            TableMetadata logical = cm.schema.getTableMetadata(t.keyspace, t.table);
            TableLayout layout = logical == null ? null : LayoutRegistry.of(cm).layout(logical.id);
            KeyState key = layout == null ? null : layout.keyState(t.key);
            boolean claimPastCommit = claim != null && pastCommit(claim.phase);
            if ((key != null && !PRE_COMMIT.contains(key.phase)) || claimPastCommit)
                throw new IllegalStateException("The split of " + target.asString() + " is past its commit point (phase "
                                                + (key != null && !PRE_COMMIT.contains(key.phase) ? key.phase : claim.phase)
                                                + "): it cannot be aborted. The way back is a reverse migration, `nodetool autonomic layout unsplit`, "
                                                + "which is not built yet (T6.17)");
            if (claim == null)
            {
                if (key == null)
                    throw new IllegalStateException("No migration of " + target.asString() + " is in progress");
                throw new IllegalStateException("No action owns the migration of " + target.asString() + " (phase " + key.phase
                                                + "); `nodetool autonomic layout resume` takes it over and aborts it");
            }
            if (claim.state.containsKey(S_ABORT))
                return "The migration of " + target.asString() + " is already being aborted: " + claim.state.get(S_ABORT);
            Map<String, String> state = new TreeMap<>(claim.state);
            state.put(S_ABORT, text(reason));
            ClusterCoordinator.Outcome outcome = coordinator.advance(claim, claim.phase, state);
            if (outcome.ok())
                return "Abort of " + target.asString() + " requested in phase " + claim.phase + "; action " + claim.actionId + " takes the abort path";
        }
        throw new IllegalStateException("The abort request for " + target.asString() + " could not be committed; try again");
    }

    /**
     * The second approval of {@code commit_approval: manual}: {@code nodetool autonomic approve <id>} for a split in
     * CUTOVER. Returns false if {@code actionId} is not such an action.
     */
    public static boolean approveCommit(ClusterCoordinator coordinator, UUID actionId)
    {
        for (int attempt = 0; attempt < 3; attempt++)
        {
            Claim claim = coordinator.ledger().claim(actionId);
            if (claim == null || !NAME.equals(claim.actuator) || !Phase.CUTOVER.name().equals(claim.phase))
                return false;
            if ("true".equals(claim.state.get(S_COMMIT_APPROVED)))
                return true;
            Map<String, String> state = new TreeMap<>(claim.state);
            state.put(S_COMMIT_APPROVED, "true");
            if (coordinator.advance(claim, claim.phase, state).ok())
                return true;
        }
        throw new IllegalStateException("The commit approval of action " + actionId + " could not be committed; try again");
    }

    /** True for a claim phase that is a key phase past the commit point. */
    private static boolean pastCommit(String claimPhase)
    {
        for (Phase p : Phase.values())
            if (p.name().equals(claimPhase))
                return p.coordinatorClass == 3;
        return ReleaseKey.CLAIM_PHASE.equals(claimPhase);
    }

    /** The diagnosis of an orphaned key, and the start of its status. */
    public static final String ORPHANED = "layout.orphaned";

    /** A key past its commit point that no action owns: nothing finishes it until it is adopted (T6.9 review). */
    public static final class Orphan
    {
        public final TableLayout layout;
        public final ByteBuffer key;
        public final KeyState state;
        /** The replicas' limit has passed in CLEANUP: the reconcile pass can no longer run and adoption is refused. */
        public final boolean pastLimit;

        Orphan(TableLayout layout, ByteBuffer key, KeyState state, boolean pastLimit)
        {
            this.layout = layout;
            this.key = key;
            this.state = state;
            this.pastLimit = pastLimit;
        }

        public String keyHex()
        {
            return ByteBufferUtil.bytesToHex(key);
        }
    }

    private static boolean reconcileExpired(KeyState key, TableMetadata logical, long nowMillis, long writeTimeoutMillis)
    {
        return key.phase.coordinatorClass == 3 && key.phase != Phase.STEADY && key.dualWriteSinceMillis > 0
               && nowMillis >= key.backfillLimitMillis(logical.params.gcGraceSeconds) - writeTimeoutMillis;
    }

    /**
     * Keys in NEW_ONLY, CLEANUP, or STEADY without their base tombstone, of a table that exists, whose action holds no
     * claim and that no claim of the lane is adopting.
     */
    public static List<Orphan> orphans(ClusterMetadata cm, long nowMillis, long writeTimeoutMillis)
    {
        List<Orphan> out = new ArrayList<>();
        ClusterLedger ledger = ClusterLedger.of(cm);
        for (TableLayout layout : LayoutRegistry.of(cm).byLogical.values())
        {
            TableMetadata logical = cm.schema.getTableMetadata(layout.logical);
            if (logical == null)
                continue;
            for (Map.Entry<ByteBuffer, KeyState> e : layout.keys.entrySet())
            {
                KeyState key = e.getValue();
                if (key.phase.coordinatorClass != 3 || key.actionId == null || ledger.claim(key.actionId) != null)
                    continue;
                if (claimOf(ledger, target(layout.keyspace, layout.logicalName, ByteBufferUtil.bytesToHex(e.getKey()))) != null)
                    continue;
                out.add(new Orphan(layout, e.getKey(), key, reconcileExpired(key, logical, nowMillis, writeTimeoutMillis)));
            }
        }
        return out;
    }

    /** {@code nodetool autonomic layout status}: one record per key of every layout. */
    public static List<Map<String, String>> status(ClusterMetadata cm, long nowMillis, AutonomicConfig.LayoutConfig lc, long writeTimeoutMillis)
    {
        List<Map<String, String>> out = new ArrayList<>();
        ClusterLedger ledger = ClusterLedger.of(cm);
        for (TableLayout layout : LayoutRegistry.of(cm).byLogical.values())
        {
            TableMetadata logical = cm.schema.getTableMetadata(layout.logical);
            for (Map.Entry<ByteBuffer, KeyState> e : layout.keys.entrySet())
            {
                KeyState key = e.getValue();
                Map<String, String> m = new LinkedHashMap<>();
                m.put("table", layout.keyspace + '.' + layout.logicalName);
                m.put("key", ByteBufferUtil.bytesToHex(e.getKey()));
                m.put("phase", key.phase.name());
                m.put("overflow", layout.keyspace + '.' + layout.physicalName);
                m.put("buckets", Integer.toString(layout.buckets));
                m.put("incarnation", Integer.toString(key.incarnation));
                m.put("generation", Integer.toString(key.generation));
                m.put("backfill_done", key.backfillDone.toString());
                Claim claim = key.actionId == null ? null : ledger.claim(key.actionId);
                m.put("action", key.actionId == null ? "none" : key.actionId.toString());
                String status;
                Claim lane = claimOf(ledger, target(layout.keyspace, layout.logicalName, ByteBufferUtil.bytesToHex(e.getKey())));
                if (logical == null)
                    status = "orphan (logical table dropped)";
                else if (key.phase == Phase.STEADY && key.actionId == null)
                    status = "split";
                else if (claim == null && lane != null)
                    status = "being adopted by action " + lane.actionId + " on node " + lane.owner.id();
                else if (claim == null)
                {
                    if (reconcileExpired(key, logical, nowMillis, writeTimeoutMillis))
                        status = ORPHANED + ": " + RECONCILE_PAST_DEADLINE + ": reads and writes use the overflow copy; base copy kept; needs an operator";
                    else if (PRE_COMMIT.contains(key.phase))
                        status = "no owner; `layout resume` aborts it";
                    else if (key.phase == Phase.STEADY)
                        status = ORPHANED + ": split, but no owner, past the commit point, and the base partition tombstone was not written; "
                                 + "the preferred proposer adopts it, or `layout resume` on the lowest live node finishes it";
                    else
                        status = ORPHANED + ": no owner, past the commit point; the preferred proposer adopts it, "
                                 + "or `layout resume` on the lowest live node finishes it";
                }
                else if (claim.state.containsKey(S_ABORT))
                    status = "aborting: " + claim.state.get(S_ABORT);
                else if (key.phase == Phase.CUTOVER && !"auto".equalsIgnoreCase(lc.commit_approval) && !"true".equals(claim.state.get(S_COMMIT_APPROVED)))
                    status = "running; the commit point waits for `nodetool autonomic approve " + claim.actionId + '`';
                else
                    status = "running on node " + claim.owner.id();
                m.put("status", status);
                m.put("orphaned", Boolean.toString(status.startsWith(ORPHANED)));
                out.add(m);
            }
        }
        return out;
    }

    // ---------------------------------------------------------------------------------------------------------
    // the node
    // ---------------------------------------------------------------------------------------------------------

    /** Production: the metadata log, the epoch watermark verb, DDL through {@code Schema.submit}, writes at ALL. */
    static final class NodeEnv implements Env
    {
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
        public void createTable(TableMetadata logical, UUID layoutId, TableId physicalId)
        {
            try
            {
                submit(logical.keyspace, PhysicalTables.derive(logical, layoutId, physicalId));
            }
            catch (AlreadyExistsException e)
            {
                // an earlier attempt of the same create
                if (ClusterMetadata.current().schema.getTableMetadata(physicalId) == null)
                    throw e;
            }
        }

        @Override
        public void dropTable(String keyspace, String table)
        {
            submit(keyspace, "DROP TABLE IF EXISTS " + ColumnIdentifier.maybeQuote(keyspace) + '.'
                             + ColumnIdentifier.maybeQuote(table));
        }

        private static void submit(String keyspace, String cql)
        {
            CQLStatement statement = QueryProcessor.getStatement(cql, ClientState.forInternalCalls(keyspace));
            Schema.instance.submit((SchemaTransformation) statement);
        }

        @Override
        public void deleteBuckets(TableMetadata logical, TableMetadata physical, TableLayout layout, ByteBuffer key, int incarnation, long timestampMicros)
        {
            long nowInSec = timestampMicros / 1_000_000;
            List<Mutation> mutations = new ArrayList<>(layout.buckets);
            for (int bucket = 0; bucket < layout.buckets; bucket++)
                mutations.add(new Mutation(PartitionUpdate.fullPartitionDelete(physical, RowMapper.physicalKey(logical, physical, key, incarnation, bucket),
                                                                               timestampMicros, nowInSec)));
            LayoutWrites.writeAsOwner(mutations, ConsistencyLevel.ALL);
        }

        @Override
        public void deleteBase(TableMetadata logical, ByteBuffer key, long timestampMicros)
        {
            Mutation mutation = new Mutation(PartitionUpdate.fullPartitionDelete(logical, key, timestampMicros, timestampMicros / 1_000_000));
            LayoutWrites.writeAsOwner(Collections.singletonList(mutation), ConsistencyLevel.ALL);
        }

        @Override
        public Set<NodeId> replicas(ClusterMetadata cm, TableMetadata logical, ByteBuffer key)
        {
            return NaturalReplicas.of(cm, logical, key);
        }

        @Override
        public Epoch replicasModified(ClusterMetadata cm, TableMetadata logical, ByteBuffer key)
        {
            return NaturalReplicas.lastModified(cm, logical, key);
        }

        @Override
        public Check check(ClusterMetadata cm, TableId logical, ByteBuffer key, int pageRows, long recheckDelayMillis, LongSupplier clock)
        {
            LayoutVerifier verifier = LayoutVerifier.forPartition(cm, logical, key, ConsistencyLevel.ALL, pageRows, LayoutVerifier.DEFAULT_RECHECK_ATTEMPTS,
                                                                  recheckDelayMillis, clock);
            return new Check()
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

        @Override
        public long requestTimeoutsMillis()
        {
            long max = DatabaseDescriptor.getWriteRpcTimeout(TimeUnit.MILLISECONDS);
            max = Math.max(max, DatabaseDescriptor.getCounterWriteRpcTimeout(TimeUnit.MILLISECONDS));
            max = Math.max(max, DatabaseDescriptor.getCasContentionTimeout(TimeUnit.MILLISECONDS));
            max = Math.max(max, DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS));
            return Math.max(max, DatabaseDescriptor.getTruncateRpcTimeout(TimeUnit.MILLISECONDS));
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
