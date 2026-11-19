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
import java.util.Collection;
import java.util.Objects;
import java.util.UUID;

import com.google.common.collect.ImmutableSortedSet;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.Version;

/**
 * State of one split partition inside a {@link TableLayout} (design 2.3). Immutable; the {@code with*} methods are
 * the only way to change it and always return a new object.
 */
public final class KeyState
{
    /** What the base partition of the key holds (design 11.5). The {@link #id} is the serialized form. */
    public enum BaseState
    {
        /** No base tombstone since the last freeze: the base partition still holds the pre-split data. */
        RESIDUE(0),
        /** The reclaim tombstone was written; see {@link KeyState#baseTombstoneMicros}. */
        TOMBSTONED(1);

        public final int id;

        BaseState(int id)
        {
            this.id = id;
        }

        /** Null if the id is unknown. */
        static BaseState fromId(int id)
        {
            for (BaseState s : values())
                if (s.id == id)
                    return s;
            return null;
        }
    }

    /** How a reverse migration treats the base residue (design 11.4). The {@link #id} is the serialized form. */
    public enum ReverseMode
    {
        FAST(1),
        SLOW(2);

        public final int id;

        ReverseMode(int id)
        {
            this.id = id;
        }

        /** Null if the id is unknown. */
        static ReverseMode fromId(int id)
        {
            for (ReverseMode m : values())
                if (m.id == id)
                    return m;
            return null;
        }
    }

    /**
     * {@link #frozenSinceMillis} of a key whose residue window has already closed: a time so long past that every
     * limit derived from it has ended (design 11.4, abort of a slow reverse from a partial copy).
     */
    public static final long WINDOW_CLOSED_MILLIS = 1;

    public final Phase phase;
    /** Epoch at which this phase was committed. */
    public final Epoch phaseEpoch;
    /** Proposer-supplied time at which this phase was committed. */
    public final long phaseSinceMillis;
    public final int incarnation;
    /** Backfill generation, starts at 1. */
    public final int generation;
    public final Epoch generationEpoch;
    /** Replicas that finished {@link #generation}; sorted, at most {@link LayoutRegistry#MAX_BACKFILL_NODES}. */
    public final ImmutableSortedSet<NodeId> backfillDone;
    /**
     * The action that owns the current migration. In {@link Phase#STEADY} it stays until that action has written the
     * base partition tombstone ({@code ReleaseKey}), then it is null (ADR-033).
     */
    public final UUID actionId;
    /**
     * Proposer-supplied time at which this incarnation entered {@link Phase#DUAL_WRITE}; 0 before that. No later
     * transition changes it. From it every replica derives the time after which it copies nothing more (ADR-031).
     */
    public final long dualWriteSinceMillis;
    /**
     * Nodes that reported, for this {@link Phase#ANNOUNCED} phase epoch, that they hold no hint created before the split
     * for a natural replica of the key ({@code ReportHints}, ADR-035); sorted, at most
     * {@link LayoutRegistry#MAX_HINT_REPORTS}. Empty in every other phase: any transition clears it.
     */
    public final ImmutableSortedSet<NodeId> hintsClear;
    /** Registry format 2 (design 11.5); a format 1 value decodes through {@link #legacyBaseState}. */
    public final BaseState baseState;
    /** Proposer time of the commit at which the key last entered C3 from a lower class; 0 if it never did. */
    public final long frozenSinceMillis;
    /** {@code Tt} of the reclaim tombstone, in microseconds; 0 if unknown or {@link BaseState#RESIDUE}. */
    public final long baseTombstoneMicros;
    /** Fixed by {@code BeginUnsplit}; null outside the reverse path. */
    public final ReverseMode reverseMode;
    /** Proposer time of {@link Phase#REVERSE_DUAL_WRITE}; 0 before it. */
    public final long reverseDualSinceMillis;

    /** A state that never entered {@link Phase#DUAL_WRITE}, or a hand-built one for which that does not matter. */
    public KeyState(Phase phase, Epoch phaseEpoch, long phaseSinceMillis, int incarnation, int generation, Epoch generationEpoch,
                    Collection<NodeId> backfillDone, UUID actionId)
    {
        this(phase, phaseEpoch, phaseSinceMillis, incarnation, generation, generationEpoch, backfillDone, actionId, 0);
    }

    public KeyState(Phase phase, Epoch phaseEpoch, long phaseSinceMillis, int incarnation, int generation, Epoch generationEpoch,
                    Collection<NodeId> backfillDone, UUID actionId, long dualWriteSinceMillis)
    {
        this(phase, phaseEpoch, phaseSinceMillis, incarnation, generation, generationEpoch, backfillDone, actionId, dualWriteSinceMillis,
             ImmutableSortedSet.of());
    }

    public KeyState(Phase phase, Epoch phaseEpoch, long phaseSinceMillis, int incarnation, int generation, Epoch generationEpoch,
                    Collection<NodeId> backfillDone, UUID actionId, long dualWriteSinceMillis, Collection<NodeId> hintsClear)
    {
        this(phase, phaseEpoch, phaseSinceMillis, incarnation, generation, generationEpoch, backfillDone, actionId, dualWriteSinceMillis, hintsClear,
             legacyBaseState(phase, actionId), legacyFrozenSince(phase, actionId, dualWriteSinceMillis), 0, null, 0);
    }

    public KeyState(Phase phase, Epoch phaseEpoch, long phaseSinceMillis, int incarnation, int generation, Epoch generationEpoch,
                    Collection<NodeId> backfillDone, UUID actionId, long dualWriteSinceMillis, Collection<NodeId> hintsClear,
                    BaseState baseState, long frozenSinceMillis, long baseTombstoneMicros, ReverseMode reverseMode, long reverseDualSinceMillis)
    {
        if (frozenSinceMillis < 0 || baseTombstoneMicros < 0 || reverseDualSinceMillis < 0)
            throw new IllegalArgumentException("frozen since, base tombstone or reverse dual write time is negative");
        this.baseState = Objects.requireNonNull(baseState);
        this.frozenSinceMillis = frozenSinceMillis;
        this.baseTombstoneMicros = baseTombstoneMicros;
        this.reverseMode = reverseMode;
        this.reverseDualSinceMillis = reverseDualSinceMillis;
        if (dualWriteSinceMillis < 0)
            throw new IllegalArgumentException("dual write start " + dualWriteSinceMillis + " is negative");
        this.dualWriteSinceMillis = dualWriteSinceMillis;
        if (incarnation < 1 || incarnation >= LayoutRegistry.MAX_INCARNATION)
            throw new IllegalArgumentException("incarnation " + incarnation + " is out of range");
        if (generation < 1)
            throw new IllegalArgumentException("generation " + generation + " is not positive");
        if (backfillDone.size() > LayoutRegistry.maxBackfillReports(phase))
            throw new IllegalArgumentException("backfill_done has " + backfillDone.size() + " nodes, the limit is " + LayoutRegistry.maxBackfillReports(phase) + " in " + phase);
        this.phase = Objects.requireNonNull(phase);
        this.phaseEpoch = Objects.requireNonNull(phaseEpoch);
        this.phaseSinceMillis = phaseSinceMillis;
        this.incarnation = incarnation;
        this.generation = generation;
        this.generationEpoch = Objects.requireNonNull(generationEpoch);
        this.backfillDone = ImmutableSortedSet.copyOf(backfillDone);
        this.actionId = actionId;
        if (hintsClear.size() > LayoutRegistry.MAX_HINT_REPORTS)
            throw new IllegalArgumentException("hints_clear has " + hintsClear.size() + " nodes, the limit is " + LayoutRegistry.MAX_HINT_REPORTS);
        if (!hintsClear.isEmpty() && phase != Phase.ANNOUNCED)
            throw new IllegalArgumentException("hints_clear is kept in ANNOUNCED only, not in " + phase);
        this.hintsClear = ImmutableSortedSet.copyOf(hintsClear);
    }

    /** A key just registered: {@link Phase#ANNOUNCED}, generation 1, nothing backfilled. */
    static KeyState announced(int incarnation, Epoch epoch, long nowMillis, UUID actionId)
    {
        return new KeyState(Phase.ANNOUNCED, epoch, nowMillis, incarnation, 1, epoch, ImmutableSortedSet.of(), Objects.requireNonNull(actionId));
    }

    /**
     * Format 1 carried no base state (design 11.5). A released {@code STEADY} key decodes as {@code TOMBSTONED}: today
     * {@code ReleaseKey} follows the tombstone. Any other key has not written one.
     */
    static BaseState legacyBaseState(Phase phase, UUID actionId)
    {
        return phase == Phase.STEADY && actionId == null ? BaseState.TOMBSTONED : BaseState.RESIDUE;
    }

    /**
     * An owned {@code STEADY}, {@code NEW_ONLY} or {@code CLEANUP} key decodes with {@code frozenSince} = its dual write
     * start, earlier than the real freeze and therefore conservative. A released {@code STEADY} key has no use for it,
     * and gets the same. Keys that never reached C3 are not frozen.
     */
    static long legacyFrozenSince(Phase phase, UUID actionId, long dualWriteSinceMillis)
    {
        return phase == Phase.NEW_ONLY || phase == Phase.CLEANUP || phase == Phase.STEADY ? dualWriteSinceMillis : 0;
    }

    /** This state as a format 1 value would decode it: nothing a format 1 value cannot carry is kept. */
    KeyState asLegacy()
    {
        return new KeyState(phase, phaseEpoch, phaseSinceMillis, incarnation, generation, generationEpoch, backfillDone, actionId, dualWriteSinceMillis, hintsClear);
    }

    /**
     * The state after moving to {@code to}; a different {@code generation} clears {@link #backfillDone}, and every
     * transition clears {@link #hintsClear}. Leaving the reverse path clears {@link #backfillDone} too (the report set
     * is larger there, 11.8).
     *
     * <p>Base state (design 11.5): entering C3 from a lower class sets {@link #frozenSinceMillis}; entering
     * {@link Phase#REVERSE_DUAL_WRITE} sets {@link #reverseDualSinceMillis}; and an abort of the reverse to
     * {@link Phase#STEADY} sets the base state by the abort rules of 11.4:
     * <ul>
     * <li>from {@code REVERSE_ANNOUNCED}: unchanged (nothing was copied);
     * <li>from {@code REVERSE_VERIFY} or {@code REVERSE_ROLLBACK} (the generation was complete): {@code RESIDUE} frozen now;
     * <li>from {@code REVERSE_DUAL_WRITE} or {@code REVERSE_BACKFILL}: {@code RESIDUE}, in fast mode with the old
     * {@code frozenSince}, in slow mode with the window closed.
     * </ul>
     */
    KeyState advanced(Phase to, int toGeneration, Epoch epoch, long nowMillis)
    {
        boolean restarted = toGeneration != generation;
        boolean clearDone = restarted || (phase.isReverse() && !to.isReverse());
        BaseState base = baseState;
        long frozen = frozenSinceMillis;
        long tombstone = baseTombstoneMicros;
        if (to.coordinatorClass == 3 && phase.coordinatorClass < 3)
            frozen = nowMillis;
        ReverseMode mode = reverseMode;
        long dual = reverseDualSinceMillis;
        if (to == Phase.REVERSE_DUAL_WRITE && phase == Phase.REVERSE_ANNOUNCED)
            dual = nowMillis;
        if (to == Phase.STEADY && phase.isReverse())
        {
            if (phase != Phase.REVERSE_ANNOUNCED)
            {
                base = BaseState.RESIDUE;
                tombstone = 0;
                if (phase == Phase.REVERSE_VERIFY || phase == Phase.REVERSE_ROLLBACK)
                    frozen = nowMillis;
                else if (reverseMode == ReverseMode.SLOW)
                    frozen = WINDOW_CLOSED_MILLIS;
                else
                    frozen = frozenSinceMillis;
            }
            mode = null;
            dual = 0;
        }
        return new KeyState(to, epoch, nowMillis, incarnation, toGeneration, restarted ? epoch : generationEpoch,
                            clearDone ? ImmutableSortedSet.<NodeId>of() : backfillDone, actionId,
                            to == Phase.DUAL_WRITE && phase == Phase.ANNOUNCED ? nowMillis : dualWriteSinceMillis, ImmutableSortedSet.of(),
                            base, frozen, tombstone, mode, dual);
    }

    /**
     * {@code BeginUnsplit} (design 11.4): {@link Phase#REVERSE_ANNOUNCED} owned by {@code newActionId}, in a new
     * generation (a slow reverse collects its purge reports in it), with {@code mode}. The base state is kept.
     */
    KeyState reverseBegun(ReverseMode mode, UUID newActionId, Epoch epoch, long nowMillis)
    {
        return new KeyState(Phase.REVERSE_ANNOUNCED, epoch, nowMillis, incarnation, generation + 1, epoch, ImmutableSortedSet.of(), newActionId,
                            dualWriteSinceMillis, ImmutableSortedSet.of(), baseState, frozenSinceMillis, baseTombstoneMicros, mode, 0);
    }

    /** The base state {@code TOMBSTONED(tt)}, everything else unchanged (a stuck key's tombstone, owner answer 6). */
    KeyState withBaseTombstone(long tombstoneMicros)
    {
        return new KeyState(phase, phaseEpoch, phaseSinceMillis, incarnation, generation, generationEpoch, backfillDone, actionId, dualWriteSinceMillis, hintsClear,
                            BaseState.TOMBSTONED, frozenSinceMillis, tombstoneMicros, reverseMode, reverseDualSinceMillis);
    }

    KeyState withBackfillDone(NodeId node)
    {
        return new KeyState(phase, phaseEpoch, phaseSinceMillis, incarnation, generation, generationEpoch,
                            ImmutableSortedSet.<NodeId>naturalOrder().addAll(backfillDone).add(node).build(), actionId, dualWriteSinceMillis, hintsClear,
                            baseState, frozenSinceMillis, baseTombstoneMicros, reverseMode, reverseDualSinceMillis);
    }

    /** {@code ReportHints} (ADR-035): the node holds no hint created before the split for the key's replicas. */
    KeyState withHintsClear(NodeId node)
    {
        return new KeyState(phase, phaseEpoch, phaseSinceMillis, incarnation, generation, generationEpoch, backfillDone, actionId, dualWriteSinceMillis,
                            ImmutableSortedSet.<NodeId>naturalOrder().addAll(hintsClear).add(node).build(),
                            baseState, frozenSinceMillis, baseTombstoneMicros, reverseMode, reverseDualSinceMillis);
    }

    /** The same state owned by another action ({@code AdoptKey}). */
    KeyState withAction(UUID newActionId)
    {
        return new KeyState(phase, phaseEpoch, phaseSinceMillis, incarnation, generation, generationEpoch, backfillDone,
                            Objects.requireNonNull(newActionId), dualWriteSinceMillis, hintsClear,
                            baseState, frozenSinceMillis, baseTombstoneMicros, reverseMode, reverseDualSinceMillis);
    }

    /** {@link Phase#STEADY}, still owned by the action until its base partition tombstone is written. */
    KeyState finished(Epoch epoch, long nowMillis)
    {
        return new KeyState(Phase.STEADY, epoch, nowMillis, incarnation, generation, generationEpoch, backfillDone, actionId, dualWriteSinceMillis,
                            ImmutableSortedSet.of(), baseState, frozenSinceMillis, baseTombstoneMicros, reverseMode, reverseDualSinceMillis);
    }

    /** The same state owned by no action ({@code ReleaseKey}), with the base state the owner reports. */
    KeyState released(BaseState base, long tombstoneMicros)
    {
        return new KeyState(phase, phaseEpoch, phaseSinceMillis, incarnation, generation, generationEpoch, backfillDone, null, dualWriteSinceMillis,
                            hintsClear, base, frozenSinceMillis, tombstoneMicros, reverseMode, reverseDualSinceMillis);
    }

    /**
     * True if replicas have a backfill pass to run for {@link #generation} and may report it: in {@link Phase#BACKFILL}
     * always; in {@link Phase#CLEANUP} only for a generation that was started there, the reconcile pass. A key enters
     * {@code CLEANUP} with the generation of its backfill, which is not to be run again.
     */
    public boolean backfillDue()
    {
        return phase == Phase.BACKFILL || (phase == Phase.CLEANUP && generationEpoch.equals(phaseEpoch));
    }

    /**
     * True if replicas have a reverse pass to run for {@link #generation} (design 11.4, T6.17c): in
     * {@link Phase#REVERSE_BACKFILL} and {@link Phase#REVERSE_CLEANUP}, only for a generation that was started in the
     * phase. {@code REVERSE_BACKFILL} entered from {@code REVERSE_DUAL_WRITE} keeps the generation of
     * {@code BeginUnsplit}, whose {@link #backfillDone} may hold the purge reports of a slow reverse; the owner starts
     * a new generation before replicas copy. {@code REVERSE_CLEANUP} is entered with the generation of the backfill,
     * as {@code CLEANUP} is.
     */
    public boolean reverseBackfillDue()
    {
        return (phase == Phase.REVERSE_BACKFILL || phase == Phase.REVERSE_CLEANUP) && generationEpoch.equals(phaseEpoch);
    }

    /**
     * True if {@code ReportBackfill} counts for the current generation: {@link #backfillDue()} in the forward phases;
     * in {@link Phase#REVERSE_ANNOUNCED} (the purge reports of a slow reverse) and {@link Phase#REVERSE_BACKFILL}
     * always; in {@link Phase#REVERSE_CLEANUP} only for a generation that was started there, the reconcile pass.
     * Deliberately not {@link #backfillDue()}: that is the replicas' forward pass, which must never run in a reverse
     * phase (the reverse pass is a different task).
     */
    public boolean reportsDue()
    {
        return backfillDue() || phase == Phase.REVERSE_BACKFILL || phase == Phase.REVERSE_ANNOUNCED
               || (phase == Phase.REVERSE_CLEANUP && generationEpoch.equals(phaseEpoch));
    }

    /**
     * The time after which no replica writes a reverse backfill or reconcile chunk into base (invariants I16, I18;
     * design 11.5), with {@code G = gc_grace_seconds / 2} and {@code wt} the write timeout. Fast mode:
     * {@code min(frozenSince, reverseDualSince) + G - wt}; slow mode: {@code reverseDualSince + G - wt}.
     * {@code Long.MIN_VALUE} before {@code REVERSE_DUAL_WRITE}: nothing may be written.
     */
    public long reverseReplicaLimitMillis(int gcGraceSeconds, long writeTimeoutMillis)
    {
        if (reverseDualSinceMillis <= 0)
            return Long.MIN_VALUE;
        long start = reverseMode == ReverseMode.SLOW || frozenSinceMillis <= 0 ? reverseDualSinceMillis : Math.min(frozenSinceMillis, reverseDualSinceMillis);
        return start + gcGraceSeconds * 1000L / 2 - writeTimeoutMillis;
    }

    /**
     * The time after which no replica writes a backfill chunk for this incarnation (invariant I9, ADR-031): half of
     * {@code gc_grace_seconds} after dual write started. {@code Long.MIN_VALUE} if the start is not recorded: nothing
     * may then be copied.
     */
    public long backfillLimitMillis(int gcGraceSeconds)
    {
        return dualWriteSinceMillis <= 0 ? Long.MIN_VALUE : dualWriteSinceMillis + gcGraceSeconds * 1000L / 2;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof KeyState))
            return false;
        KeyState k = (KeyState) o;
        return phase == k.phase && phaseEpoch.equals(k.phaseEpoch) && phaseSinceMillis == k.phaseSinceMillis
               && incarnation == k.incarnation && generation == k.generation && generationEpoch.equals(k.generationEpoch)
               && backfillDone.equals(k.backfillDone) && Objects.equals(actionId, k.actionId) && dualWriteSinceMillis == k.dualWriteSinceMillis
               && hintsClear.equals(k.hintsClear) && baseState == k.baseState && frozenSinceMillis == k.frozenSinceMillis
               && baseTombstoneMicros == k.baseTombstoneMicros && reverseMode == k.reverseMode && reverseDualSinceMillis == k.reverseDualSinceMillis;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(phase, phaseEpoch, phaseSinceMillis, incarnation, generation, generationEpoch, backfillDone, actionId, dualWriteSinceMillis, hintsClear,
                            baseState, frozenSinceMillis, baseTombstoneMicros, reverseMode, reverseDualSinceMillis);
    }

    @Override
    public String toString()
    {
        return "KeyState{" + phase + " phaseEpoch=" + phaseEpoch.getEpoch() + " since=" + phaseSinceMillis + " incarnation=" + incarnation
               + " generation=" + generation + " generationEpoch=" + generationEpoch.getEpoch() + " backfillDone=" + backfillDone
               + " action=" + actionId + " dualWriteSince=" + dualWriteSinceMillis + (hintsClear.isEmpty() ? "" : " hintsClear=" + hintsClear)
               + " base=" + baseState + " frozenSince=" + frozenSinceMillis + " tt=" + baseTombstoneMicros
               + (reverseMode == null ? "" : " reverse=" + reverseMode + " reverseDualSince=" + reverseDualSinceMillis) + '}';
    }

    static void serialize(KeyState k, DataOutputPlus out, Version version, int format) throws IOException
    {
        out.writeByte(k.phase.id);
        Epoch.serializer.serialize(k.phaseEpoch, out, version);
        out.writeLong(k.phaseSinceMillis);
        out.writeUnsignedVInt32(k.incarnation);
        out.writeUnsignedVInt32(k.generation);
        Epoch.serializer.serialize(k.generationEpoch, out, version);
        out.writeUnsignedVInt32(k.backfillDone.size());
        for (NodeId node : k.backfillDone)
            NodeId.serializer.serialize(node, out, version);
        out.writeBoolean(k.actionId != null);
        if (k.actionId != null)
        {
            out.writeLong(k.actionId.getMostSignificantBits());
            out.writeLong(k.actionId.getLeastSignificantBits());
        }
        out.writeLong(k.dualWriteSinceMillis);
        out.writeUnsignedVInt32(k.hintsClear.size());
        for (NodeId node : k.hintsClear)
            NodeId.serializer.serialize(node, out, version);
        if (format >= 2)
        {
            out.writeByte(k.baseState.id);
            out.writeLong(k.frozenSinceMillis);
            out.writeLong(k.baseTombstoneMicros);
            out.writeByte(k.reverseMode == null ? 0 : k.reverseMode.id);
            out.writeLong(k.reverseDualSinceMillis);
        }
    }

    static KeyState deserialize(DataInputPlus in, Version version, int format) throws IOException
    {
        int phaseId = in.readByte();
        Phase phase = Phase.fromId(phaseId);
        if (phase == null)
            throw new IOException("Unknown autonomic layout phase id " + phaseId);
        Epoch phaseEpoch = Epoch.serializer.deserialize(in, version);
        long since = in.readLong();
        int incarnation = in.readUnsignedVInt32();
        int generation = in.readUnsignedVInt32();
        Epoch generationEpoch = Epoch.serializer.deserialize(in, version);
        int nodes = in.readUnsignedVInt32();
        if (nodes < 0 || nodes > LayoutRegistry.maxBackfillReports(phase))
            throw new IOException("Corrupt autonomic layout registry: " + nodes + " backfill nodes");
        ImmutableSortedSet.Builder<NodeId> done = ImmutableSortedSet.naturalOrder();
        for (int i = 0; i < nodes; i++)
            done.add(NodeId.serializer.deserialize(in, version));
        UUID actionId = in.readBoolean() ? new UUID(in.readLong(), in.readLong()) : null;
        long dualWriteSince = in.readLong();
        int clear = in.readUnsignedVInt32();
        if (clear < 0 || clear > LayoutRegistry.MAX_HINT_REPORTS)
            throw new IOException("Corrupt autonomic layout registry: " + clear + " hint reports");
        ImmutableSortedSet.Builder<NodeId> hintsClear = ImmutableSortedSet.naturalOrder();
        for (int i = 0; i < clear; i++)
            hintsClear.add(NodeId.serializer.deserialize(in, version));
        try
        {
            if (format < 2)
                return new KeyState(phase, phaseEpoch, since, incarnation, generation, generationEpoch, done.build(), actionId, dualWriteSince, hintsClear.build());
            BaseState base = BaseState.fromId(in.readByte());
            long frozenSince = in.readLong();
            long tombstone = in.readLong();
            int modeId = in.readByte();
            ReverseMode mode = modeId == 0 ? null : ReverseMode.fromId(modeId);
            if (base == null || (modeId != 0 && mode == null))
                throw new IOException("Corrupt autonomic layout registry: unknown base state or reverse mode");
            return new KeyState(phase, phaseEpoch, since, incarnation, generation, generationEpoch, done.build(), actionId, dualWriteSince, hintsClear.build(),
                                base, frozenSince, tombstone, mode, in.readLong());
        }
        catch (IllegalArgumentException e)
        {
            throw new IOException("Corrupt autonomic layout registry: " + e.getMessage(), e);
        }
    }

    static long serializedSize(KeyState k, Version version, int format)
    {
        long size = 1 + Epoch.serializer.serializedSize(k.phaseEpoch, version) + Long.BYTES
                    + TypeSizes.sizeofUnsignedVInt(k.incarnation) + TypeSizes.sizeofUnsignedVInt(k.generation)
                    + Epoch.serializer.serializedSize(k.generationEpoch, version) + TypeSizes.sizeofUnsignedVInt(k.backfillDone.size());
        for (NodeId node : k.backfillDone)
            size += NodeId.serializer.serializedSize(node, version);
        size += 1 + (k.actionId == null ? 0 : 2 * Long.BYTES) + Long.BYTES + TypeSizes.sizeofUnsignedVInt(k.hintsClear.size());
        for (NodeId node : k.hintsClear)
            size += NodeId.serializer.serializedSize(node, version);
        return format >= 2 ? size + 1 + Long.BYTES + Long.BYTES + 1 + Long.BYTES : size;
    }
}
