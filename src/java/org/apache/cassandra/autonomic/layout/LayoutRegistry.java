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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.extensions.ExtensionValue;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;

/**
 * The layout registry of design 2.3: the {@link TableLayout} of every logical table that has one. Immutable.
 *
 * <p>The {@code with*} methods are the whole logic of the layout transformations, written like those of
 * {@code ClusterLedger}: pure functions of their arguments (no clock, configuration or local state, epochs and
 * times are passed in), returning the new registry or the reason for the rejection. All size bounds of design 2.3
 * are enforced here, and again by the constructors so that a registry beyond them cannot be written or read.
 */
public final class LayoutRegistry
{
    /**
     * The legacy format: no base state, no reverse path. A format 1 value decodes with the rules of design 11.5, and a
     * registry of this format is written as format 1 (so that a node that cannot read format 2 can still read it).
     */
    public static final int FORMAT_VERSION = 1;
    /** Adds the base state fields of {@link KeyState} and the reverse phases (design 11.5). */
    public static final int FORMAT_VERSION_2 = 2;
    /** The newest format this code reads and writes. */
    public static final int LATEST_FORMAT_VERSION = FORMAT_VERSION_2;
    /**
     * The upgrade gate of format 2: the first release whose nodes read it. A registry is upgraded from format 1 to
     * format 2 only by a transformation that needs it, and only if every node of the directory runs this release or a
     * later one; until then such a transformation is rejected and no format 2 value exists in the log.
     */
    public static final CassandraVersion FORMAT_2_MIN_RELEASE = new CassandraVersion("7.0").familyLowerBound.get();
    public static final int HASH_ALGO = 1;
    public static final int MAX_LAYOUTS = 8;
    public static final int MAX_KEYS = 8;
    public static final int MAX_KEY_BYTES = 1024;
    public static final int MAX_BACKFILL_NODES = 32;
    /** Nodes of {@link KeyState#hintsClear}: every directory member reports, not only the replicas (ADR-035). */
    public static final int MAX_HINT_REPORTS = 1024;
    public static final int MIN_BUCKETS = 2;
    public static final int MAX_BUCKETS = 64;
    /** Exclusive: {@code incarnation * 256 + bucket} must fit an int. */
    public static final int MAX_INCARNATION = 1 << 23;
    /** The bound of {@code SchemaConstants.NAME_LENGTH}, repeated so that nothing but the format is loaded to replay the log. */
    public static final int MAX_NAME_LENGTH = 48;

    public static final LayoutRegistry EMPTY = new LayoutRegistry(FORMAT_VERSION, ImmutableMap.of());

    /**
     * The most nodes {@link KeyState#backfillDone} holds in the phase: {@link #MAX_BACKFILL_NODES}, and in the reverse
     * phases {@link #MAX_HINT_REPORTS}, because reverse reports come from the replicas of every bucket partition,
     * up to {@code N x RF} nodes (design 11.8, owner answer 4).
     */
    public static int maxBackfillReports(Phase phase)
    {
        return phase.isReverse() ? MAX_HINT_REPORTS : MAX_BACKFILL_NODES;
    }

    /**
     * Why a registry may not be upgraded to format 2 at this metadata, or null if it may: some node of the directory
     * runs a release older than {@link #FORMAT_2_MIN_RELEASE}.
     */
    public static String formatGateRefusal(ClusterMetadata metadata)
    {
        NodeVersion min = metadata == null ? null : metadata.directory.clusterMinVersion;
        if (min == null)
            return "the cluster's minimum node version is unknown";
        if (min.cassandraVersion.compareTo(FORMAT_2_MIN_RELEASE) < 0)
            return "layout registry format 2 needs every node at " + FORMAT_2_MIN_RELEASE + " or later, the oldest is " + min.cassandraVersion;
        return null;
    }

    public final int formatVersion;
    /** By logical table id, in id order. */
    public final ImmutableMap<TableId, TableLayout> byLogical;

    public LayoutRegistry(int formatVersion, Map<TableId, TableLayout> byLogical)
    {
        if (byLogical.size() > MAX_LAYOUTS)
            throw new IllegalArgumentException("registry has " + byLogical.size() + " layouts, the limit is " + MAX_LAYOUTS);
        this.formatVersion = formatVersion;
        this.byLogical = ImmutableSortedMap.copyOf(byLogical);
    }

    /** The registry of the given metadata; {@link #EMPTY} if none was ever written. */
    public static LayoutRegistry of(ClusterMetadata metadata)
    {
        ExtensionValue<?> value = metadata == null ? null : metadata.extensions.get(LayoutRegistryValue.KEY);
        return value instanceof LayoutRegistryValue ? ((LayoutRegistryValue) value).getValue() : EMPTY;
    }

    public static boolean present(ClusterMetadata metadata)
    {
        return metadata != null && metadata.extensions.get(LayoutRegistryValue.KEY) instanceof LayoutRegistryValue;
    }

    /** The layout of the logical table, or null. */
    public TableLayout layout(TableId logical)
    {
        return byLogical.get(logical);
    }

    // ---------------------------------------------------------------------------------------------------------
    // pure transitions
    // ---------------------------------------------------------------------------------------------------------

    /** Outcome of a transition: the resulting registry, or why it was rejected. */
    public static final class Result
    {
        /** Null if rejected. */
        public final LayoutRegistry registry;
        /** Null if accepted. */
        public final String rejection;
        /** True if accepted and the registry is the one the transition was applied to (an idempotent re-apply). */
        public final boolean unchanged;

        private Result(LayoutRegistry registry, String rejection, boolean unchanged)
        {
            this.registry = registry;
            this.rejection = rejection;
            this.unchanged = unchanged;
        }

        public boolean isRejected()
        {
            return rejection != null;
        }

        @Override
        public String toString()
        {
            return isRejected() ? "Rejected{" + rejection + '}' : "Accepted{" + registry + (unchanged ? ", unchanged}" : "}");
        }
    }

    private Result changed(TableId logical, TableLayout layout)
    {
        ImmutableSortedMap.Builder<TableId, TableLayout> map = ImmutableSortedMap.naturalOrder();
        byLogical.forEach((id, l) -> {
            if (!id.equals(logical))
                map.put(id, l);
        });
        if (layout != null)
            map.put(logical, formatVersion < FORMAT_VERSION_2 ? layout.asLegacy() : layout);
        return new Result(new LayoutRegistry(formatVersion, map.build()), null, false);
    }

    /**
     * This registry in format 2, or the reason it may not be: {@code gateRefusal} is the answer of
     * {@link #formatGateRefusal}, evaluated by the transformation, which alone sees the metadata.
     */
    private LayoutRegistry upgraded(String gateRefusal)
    {
        if (formatVersion >= FORMAT_VERSION_2)
            return this;
        if (gateRefusal != null)
            throw new IllegalStateException(gateRefusal);
        return new LayoutRegistry(FORMAT_VERSION_2, byLogical);
    }

    private static Result reject(String reason)
    {
        return rejected(reason);
    }

    static Result rejected(String reason)
    {
        return new Result(null, reason, false);
    }

    private String lookup(TableId logical, ByteBuffer key)
    {
        TableLayout layout = byLogical.get(logical);
        if (layout == null)
            return "table " + logical + " has no layout";
        if (layout.keyState(key) == null)
            return "key is not registered in the layout of " + layout.keyspace + '.' + layout.logicalName;
        return null;
    }

    /**
     * {@code RegisterKey}: creates the layout if absent, or requires {@code header} to equal the existing one, and
     * adds the key in {@link Phase#ANNOUNCED} with the layout's next incarnation.
     */
    public Result withRegisteredKey(LayoutHeader header, ByteBuffer key, UUID actionId, Epoch epoch, long nowMillis)
    {
        if (header == null || key == null || actionId == null)
            return reject("no layout, key or action id");
        String invalid = header.invalid();
        if (invalid != null)
            return reject(invalid);
        if (key.remaining() == 0 || key.remaining() > MAX_KEY_BYTES)
            return reject("partition key of " + key.remaining() + " bytes, the limit is " + MAX_KEY_BYTES);
        TableLayout existing = byLogical.get(header.logical);
        if (existing == null)
        {
            if (byLogical.size() >= MAX_LAYOUTS)
                return reject("the registry already has " + MAX_LAYOUTS + " layouts");
            for (TableLayout other : byLogical.values())
                if (other.physical.equals(header.physical) || other.layoutId.equals(header.layoutId))
                    return reject("layout " + other.layoutId + " already uses that physical table or layout id");
            TableLayout created = new TableLayout(header, 1, ImmutableMap.of(), 0);
            return changed(header.logical, created.withNewKey(key, KeyState.announced(1, epoch, nowMillis, actionId)));
        }
        if (!existing.header().equals(header))
            return reject("the layout header differs from the registered " + existing.header());
        if (existing.keyState(key) != null)
            return reject("key is already registered in the layout of " + existing.keyspace + '.' + existing.logicalName);
        if (existing.keys.size() >= MAX_KEYS)
            return reject("the layout already has " + MAX_KEYS + " keys");
        if (existing.nextIncarnation >= MAX_INCARNATION)
            return reject("the layout has used all incarnations");
        return changed(header.logical, existing.withNewKey(key, KeyState.announced(existing.nextIncarnation, epoch, nowMillis, actionId)));
    }

    /**
     * {@code AdvanceLayout}: the key must be in {@code from} and owned by {@code actionId}. {@code from != to} must be
     * a transition of {@link Phase#isTransition}; {@code VERIFY -> BACKFILL} and {@code from == to} (only for
     * {@link Phase#restartable} phases) must carry the next generation, any other transition the current one.
     */
    public Result withAdvancedKey(TableId logical, ByteBuffer key, UUID actionId, Phase from, Phase to, int generation, Epoch epoch, long nowMillis)
    {
        if (logical == null || key == null || actionId == null || from == null || to == null)
            return reject("missing argument");
        String missing = lookup(logical, key);
        if (missing != null)
            return reject(missing);
        TableLayout layout = byLogical.get(logical);
        KeyState state = layout.keyState(key);
        if (state.phase != from)
            return reject("key is in phase " + state.phase + ", not " + from);
        if (!actionId.equals(state.actionId))
            return reject("key is owned by action " + state.actionId + ", not " + actionId);
        boolean restart = from == to;
        if (restart ? !from.restartable() : !Phase.isTransition(from, to))
            return reject("illegal transition " + from + " -> " + to);
        boolean newGeneration = restart || (from == Phase.VERIFY && to == Phase.BACKFILL) || (from == Phase.REVERSE_VERIFY && to == Phase.REVERSE_BACKFILL);
        int expected = newGeneration ? state.generation + 1 : state.generation;
        if (generation != expected)
            return reject("transition " + from + " -> " + to + " requires generation " + expected + ", not " + generation + " (current " + state.generation + ')');
        return changed(logical, layout.withKey(key, state.advanced(to, generation, epoch, nowMillis)));
    }

    /**
     * {@code ReportBackfill}: adds the node to {@link KeyState#backfillDone} of the current generation. Idempotent.
     * The report must name the current generation and the epoch at which it started: generation numbers start over
     * when a key is removed and registered again, the epoch does not.
     */
    public Result withBackfillReported(TableId logical, ByteBuffer key, int generation, Epoch generationEpoch, NodeId node)
    {
        if (logical == null || key == null || generationEpoch == null || node == null)
            return reject("missing argument");
        String missing = lookup(logical, key);
        if (missing != null)
            return reject(missing);
        TableLayout layout = byLogical.get(logical);
        KeyState state = layout.keyState(key);
        if (!state.phase.acceptsBackfillReports())
            return reject("key is in phase " + state.phase + ", which takes no backfill reports");
        if (!state.reportsDue())
            return reject("key is in phase " + state.phase + " with the generation of its backfill; the reconcile generation has not started");
        if (state.generation != generation)
            return reject("key is at generation " + state.generation + ", not " + generation);
        if (!state.generationEpoch.equals(generationEpoch))
            return reject("generation " + generation + " of the key started at epoch " + state.generationEpoch.getEpoch() + ", not " + generationEpoch.getEpoch());
        if (state.backfillDone.contains(node))
            return new Result(this, null, true);
        if (state.backfillDone.size() >= maxBackfillReports(state.phase))
            return reject("backfill_done already has " + maxBackfillReports(state.phase) + " nodes");
        return changed(logical, layout.withKey(key, state.withBackfillDone(node)));
    }

    /**
     * {@code ReportHints} (ADR-035): adds the node to {@link KeyState#hintsClear}. Idempotent. Accepted only while the
     * key is in {@link Phase#ANNOUNCED} at {@code phaseEpoch}: a report made for an earlier attempt, or for a phase the
     * key has left, does not count.
     */
    public Result withHintsReported(TableId logical, ByteBuffer key, Epoch phaseEpoch, NodeId node)
    {
        if (logical == null || key == null || phaseEpoch == null || node == null)
            return reject("missing argument");
        String missing = lookup(logical, key);
        if (missing != null)
            return reject(missing);
        TableLayout layout = byLogical.get(logical);
        KeyState state = layout.keyState(key);
        if (state.phase != Phase.ANNOUNCED)
            return reject("key is in phase " + state.phase + ", which takes no hint reports");
        if (!state.phaseEpoch.equals(phaseEpoch))
            return reject("key entered ANNOUNCED at epoch " + state.phaseEpoch.getEpoch() + ", not " + phaseEpoch.getEpoch());
        if (state.hintsClear.contains(node))
            return new Result(this, null, true);
        if (state.hintsClear.size() >= MAX_HINT_REPORTS)
            return reject("hints_clear already has " + MAX_HINT_REPORTS + " nodes");
        return changed(logical, layout.withKey(key, state.withHintsClear(node)));
    }

    /**
     * {@code AdoptKey}: the key must be in {@code phase} and owned by {@code previous} (a {@link Phase#STEADY} key is
     * owned only until its base partition tombstone is written); it becomes owned by {@code actionId} and nothing else
     * changes. Already owned by {@code actionId}: unchanged.
     */
    public Result withAdoptedKey(TableId logical, ByteBuffer key, Phase phase, UUID previous, UUID actionId)
    {
        if (logical == null || key == null || phase == null || previous == null || actionId == null)
            return reject("missing argument");
        String missing = lookup(logical, key);
        if (missing != null)
            return reject(missing);
        TableLayout layout = byLogical.get(logical);
        KeyState state = layout.keyState(key);
        if (state.phase != phase)
            return reject("key is in phase " + state.phase + ", not " + phase);
        if (state.actionId == null)
            return reject("key is in phase " + state.phase + " and owned by no action");
        if (actionId.equals(state.actionId))
            return new Result(this, null, true);
        if (!previous.equals(state.actionId))
            return reject("key is owned by action " + state.actionId + ", not " + previous);
        return changed(logical, layout.withKey(key, state.withAction(actionId)));
    }

    /** {@code RemoveKey} of an aborted forward migration: the key must be {@link Phase#ABORTING}. */
    public Result withoutKey(TableId logical, ByteBuffer key, UUID actionId)
    {
        return withoutKey(logical, key, actionId, null);
    }

    /**
     * {@code RemoveKey}: the key must be {@link Phase#ABORTING}, or {@link Phase#REVERSE_CLEANUP} with its reconcile
     * generation reported by every one of {@code bucketReplicas}, the natural replicas of every bucket token of its
     * incarnation at this epoch (design 11.4, ADR-033 mirrored); it must be owned by {@code actionId}. The layout goes
     * with its last key.
     */
    public Result withoutKey(TableId logical, ByteBuffer key, UUID actionId, Set<NodeId> bucketReplicas)
    {
        if (logical == null || key == null || actionId == null)
            return reject("missing argument");
        String missing = lookup(logical, key);
        if (missing != null)
            return reject(missing);
        TableLayout layout = byLogical.get(logical);
        KeyState state = layout.keyState(key);
        if (state.phase != Phase.ABORTING && state.phase != Phase.REVERSE_CLEANUP)
            return reject("key is in phase " + state.phase + ", not " + Phase.ABORTING + " or " + Phase.REVERSE_CLEANUP);
        if (!actionId.equals(state.actionId))
            return reject("key is owned by action " + state.actionId + ", not " + actionId);
        if (state.phase == Phase.REVERSE_CLEANUP)
        {
            if (!state.reportsDue())
                return reject("the reconcile generation of the key has not started");
            if (bucketReplicas == null || bucketReplicas.isEmpty())
                return reject("the buckets of the key have no natural replica in the placement");
            String unreported = unreported(bucketReplicas, state);
            if (unreported != null)
                return reject(unreported);
        }
        TableLayout remaining = layout.withoutKey(key);
        return changed(logical, remaining.keys.isEmpty() ? null : remaining);
    }

    /** The rejection for replicas that have not reported the current generation of the key, or null if all have. */
    private static String unreported(Set<NodeId> replicas, KeyState state)
    {
        Set<NodeId> unreported = new TreeSet<>(replicas);
        unreported.removeAll(state.backfillDone);
        if (unreported.isEmpty())
            return null;
        StringBuilder ids = new StringBuilder();
        for (NodeId node : unreported)
            ids.append(ids.length() == 0 ? "" : ", ").append(node.id());
        return "natural replicas [" + ids + "] have not reported reconcile generation " + state.generation;
    }

    /**
     * {@code BeginUnsplit} (design 11.4): moves a key to {@link Phase#REVERSE_ANNOUNCED} in {@code mode}, owned by
     * {@code actionId}, in a new generation; upgrades the registry to format 2 if it is not (see
     * {@link #formatGateRefusal}; {@code gateRefusal} is its answer).
     *
     * <p>Accepted from {@link Phase#STEADY} owned by nobody, and from {@link Phase#NEW_ONLY} or {@link Phase#CLEANUP}
     * owned by {@code previousActionId} (the forward action that handed it over, owner answer 2). {@code FAST} needs
     * base state {@code RESIDUE}, {@code SLOW} needs {@code TOMBSTONED}. Whether the windows of 11.5 still allow the
     * mode is for the action to decide: it has the clock, the configuration and the estimate.
     */
    public Result withBegunReverse(TableId logical, ByteBuffer key, UUID actionId, UUID previousActionId, KeyState.ReverseMode mode,
                                   String gateRefusal, Epoch epoch, long nowMillis)
    {
        if (logical == null || key == null || actionId == null || mode == null)
            return reject("missing argument");
        String missing = lookup(logical, key);
        if (missing != null)
            return reject(missing);
        TableLayout layout = byLogical.get(logical);
        KeyState state = layout.keyState(key);
        switch (state.phase)
        {
            case STEADY:
                if (state.actionId != null)
                    return reject("key is in " + Phase.STEADY + " and still owned by action " + state.actionId + ", its base tombstone is not yet written");
                if (previousActionId != null)
                    return reject("key is in " + Phase.STEADY + " and owned by no action, not by " + previousActionId);
                break;
            case NEW_ONLY:
            case CLEANUP:
                if (state.actionId == null || !state.actionId.equals(previousActionId))
                    return reject("key is in phase " + state.phase + " and owned by action " + state.actionId + ", not " + previousActionId);
                break;
            default:
                return reject("key is in phase " + state.phase + ", BeginUnsplit needs " + Phase.STEADY + ", " + Phase.NEW_ONLY + " or " + Phase.CLEANUP);
        }
        if (mode == KeyState.ReverseMode.FAST && state.baseState != KeyState.BaseState.RESIDUE)
            return reject("fast mode needs the base partition in state RESIDUE, it is " + state.baseState);
        if (mode == KeyState.ReverseMode.SLOW && state.baseState != KeyState.BaseState.TOMBSTONED)
            return reject("slow mode needs the base partition in state TOMBSTONED, it is " + state.baseState);
        LayoutRegistry target;
        try
        {
            target = upgraded(gateRefusal);
        }
        catch (IllegalStateException e)
        {
            return reject(e.getMessage());
        }
        return target.changed(logical, layout.withKey(key, state.reverseBegun(mode, actionId, epoch, nowMillis)));
    }

    /**
     * {@code FinishKey}: the key must be {@link Phase#CLEANUP}, owned by {@code actionId}, with its reconcile generation
     * reported by every one of {@code replicas}, the natural replicas of its token at this epoch (ADR-033); it becomes
     * {@link Phase#STEADY}, still owned by {@code actionId} until its base partition tombstone is written
     * ({@link #withReleasedKey}).
     */
    public Result withFinishedKey(TableId logical, ByteBuffer key, UUID actionId, Set<NodeId> replicas, Epoch epoch, long nowMillis)
    {
        if (logical == null || key == null || actionId == null || replicas == null)
            return reject("missing argument");
        String missing = lookup(logical, key);
        if (missing != null)
            return reject(missing);
        TableLayout layout = byLogical.get(logical);
        KeyState state = layout.keyState(key);
        if (state.phase != Phase.CLEANUP)
            return reject("key is in phase " + state.phase + ", not " + Phase.CLEANUP);
        if (!actionId.equals(state.actionId))
            return reject("key is owned by action " + state.actionId + ", not " + actionId);
        if (!state.backfillDue())
            return reject("the reconcile generation of the key has not started");
        if (replicas.isEmpty())
            return reject("the key has no natural replica in the placement");
        String unreported = unreported(replicas, state);
        if (unreported != null)
            return reject(unreported);
        return changed(logical, layout.withKey(key, state.finished(epoch, nowMillis)));
    }

    /** {@code ReleaseKey} of a key whose base partition tombstone was written at an unknown time (the format 1 form). */
    public Result withReleasedKey(TableId logical, ByteBuffer key, UUID actionId)
    {
        return withReleasedKey(logical, key, actionId, KeyState.BaseState.TOMBSTONED, 0, null);
    }

    /**
     * {@code ReleaseKey}: the key must be {@link Phase#STEADY} and owned by {@code actionId}; it becomes owned by
     * nobody, with the base state the owner reports: {@code TOMBSTONED} (its base partition tombstone is written, at
     * {@code tombstoneMicros}, 0 if unknown) or {@code RESIDUE} (none is, {@code tombstoneMicros} must be 0). Already
     * owned by nobody: unchanged. Anything but {@code TOMBSTONED} with an unknown time is not a format 1 value, and
     * upgrades the registry to format 2 ({@code gateRefusal} as in {@link #withBegunReverse}).
     */
    public Result withReleasedKey(TableId logical, ByteBuffer key, UUID actionId, KeyState.BaseState baseState, long tombstoneMicros, String gateRefusal)
    {
        if (logical == null || key == null || actionId == null || baseState == null)
            return reject("missing argument");
        if (tombstoneMicros < 0 || (baseState == KeyState.BaseState.RESIDUE && tombstoneMicros != 0))
            return reject("base state " + baseState + " cannot have a tombstone time of " + tombstoneMicros);
        String missing = lookup(logical, key);
        if (missing != null)
            return reject(missing);
        TableLayout layout = byLogical.get(logical);
        KeyState state = layout.keyState(key);
        if (state.phase != Phase.STEADY)
            return reject("key is in phase " + state.phase + ", not " + Phase.STEADY);
        if (state.actionId == null)
            return new Result(this, null, true);
        if (!actionId.equals(state.actionId))
            return reject("key is owned by action " + state.actionId + ", not " + actionId);
        LayoutRegistry target = this;
        if (baseState != KeyState.BaseState.TOMBSTONED || tombstoneMicros != 0)
        {
            try
            {
                target = upgraded(gateRefusal);
            }
            catch (IllegalStateException e)
            {
                return reject(e.getMessage());
            }
        }
        return target.changed(logical, layout.withKey(key, state.released(baseState, tombstoneMicros)));
    }

    /**
     * A key past its commit point ({@link Phase#NEW_ONLY}, {@link Phase#CLEANUP}, {@link Phase#BASE_ONLY},
     * {@link Phase#REVERSE_CLEANUP}, or {@link Phase#STEADY} without its base partition tombstone) owned by the action, as {@code ks.table key 0x..}; null if there is none.
     */
    public String postCommitKeyOwnedBy(UUID actionId)
    {
        if (actionId == null)
            return null;
        for (TableLayout layout : byLogical.values())
            for (Map.Entry<ByteBuffer, KeyState> e : layout.keys.entrySet())
            {
                KeyState state = e.getValue();
                if (actionId.equals(state.actionId) && (state.phase == Phase.NEW_ONLY || state.phase == Phase.CLEANUP || state.phase == Phase.STEADY
                                                        || state.phase == Phase.BASE_ONLY || state.phase == Phase.REVERSE_CLEANUP))
                    return layout.keyspace + '.' + layout.logicalName + " key 0x" + ByteBufferUtil.bytesToHex(e.getKey()) + " in " + state.phase;
            }
        return null;
    }

    /** {@code RemoveOrphanLayout}: removes the layout of the logical table, whatever the state of its keys. */
    public Result withoutLayout(TableId logical)
    {
        if (logical == null || !byLogical.containsKey(logical))
            return reject("table " + logical + " has no layout");
        return changed(logical, null);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof LayoutRegistry))
            return false;
        LayoutRegistry r = (LayoutRegistry) o;
        return formatVersion == r.formatVersion && byLogical.equals(r.byLogical);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(formatVersion, byLogical);
    }

    @Override
    public String toString()
    {
        return "LayoutRegistry{v" + formatVersion + ' ' + byLogical.values() + '}';
    }

    // ---------------------------------------------------------------------------------------------------------
    // Serialized form. The format version comes first and is independent of the TCM serialization version. A new
    // format may only be written once every node can read it; an unknown one fails loudly.
    // ---------------------------------------------------------------------------------------------------------

    static void serialize(LayoutRegistry r, DataOutputPlus out, Version version) throws IOException
    {
        if (r.formatVersion != FORMAT_VERSION && r.formatVersion != FORMAT_VERSION_2)
            throw new IOException("Cannot write autonomic layout registry format " + r.formatVersion);
        out.writeUnsignedVInt32(r.formatVersion);
        out.writeUnsignedVInt32(r.byLogical.size());
        for (TableLayout layout : r.byLogical.values())
            TableLayout.serialize(layout, out, version, r.formatVersion);
    }

    static LayoutRegistry deserialize(DataInputPlus in, Version version) throws IOException
    {
        int format = in.readUnsignedVInt32();
        if (format != FORMAT_VERSION && format != FORMAT_VERSION_2)
            throw new IOException("Unknown autonomic layout registry format " + format + "; this node reads formats " + FORMAT_VERSION + " and " + FORMAT_VERSION_2);
        int count = in.readUnsignedVInt32();
        if (count < 0 || count > MAX_LAYOUTS)
            throw new IOException("Corrupt autonomic layout registry: " + count + " layouts");
        ImmutableMap.Builder<TableId, TableLayout> layouts = ImmutableMap.builder();
        for (int i = 0; i < count; i++)
        {
            TableLayout layout = TableLayout.deserialize(in, version, format);
            layouts.put(layout.logical, layout);
        }
        return new LayoutRegistry(format, layouts.build());
    }

    static long serializedSize(LayoutRegistry r, Version version)
    {
        long size = TypeSizes.sizeofUnsignedVInt(r.formatVersion) + TypeSizes.sizeofUnsignedVInt(r.byLogical.size());
        for (TableLayout layout : r.byLogical.values())
            size += TableLayout.serializedSize(layout, version, r.formatVersion);
        return size;
    }
}
