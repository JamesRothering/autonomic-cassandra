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
    /** The action that owns the current migration; null in {@link Phase#STEADY}. */
    public final UUID actionId;
    /**
     * Proposer-supplied time at which this incarnation entered {@link Phase#DUAL_WRITE}; 0 before that. No later
     * transition changes it. From it every replica derives the time after which it copies nothing more (ADR-031).
     */
    public final long dualWriteSinceMillis;

    /** A state that never entered {@link Phase#DUAL_WRITE}, or a hand-built one for which that does not matter. */
    public KeyState(Phase phase, Epoch phaseEpoch, long phaseSinceMillis, int incarnation, int generation, Epoch generationEpoch,
                    Collection<NodeId> backfillDone, UUID actionId)
    {
        this(phase, phaseEpoch, phaseSinceMillis, incarnation, generation, generationEpoch, backfillDone, actionId, 0);
    }

    public KeyState(Phase phase, Epoch phaseEpoch, long phaseSinceMillis, int incarnation, int generation, Epoch generationEpoch,
                    Collection<NodeId> backfillDone, UUID actionId, long dualWriteSinceMillis)
    {
        if (dualWriteSinceMillis < 0)
            throw new IllegalArgumentException("dual write start " + dualWriteSinceMillis + " is negative");
        this.dualWriteSinceMillis = dualWriteSinceMillis;
        if (incarnation < 1 || incarnation >= LayoutRegistry.MAX_INCARNATION)
            throw new IllegalArgumentException("incarnation " + incarnation + " is out of range");
        if (generation < 1)
            throw new IllegalArgumentException("generation " + generation + " is not positive");
        if (backfillDone.size() > LayoutRegistry.MAX_BACKFILL_NODES)
            throw new IllegalArgumentException("backfill_done has " + backfillDone.size() + " nodes, the limit is " + LayoutRegistry.MAX_BACKFILL_NODES);
        this.phase = Objects.requireNonNull(phase);
        this.phaseEpoch = Objects.requireNonNull(phaseEpoch);
        this.phaseSinceMillis = phaseSinceMillis;
        this.incarnation = incarnation;
        this.generation = generation;
        this.generationEpoch = Objects.requireNonNull(generationEpoch);
        this.backfillDone = ImmutableSortedSet.copyOf(backfillDone);
        this.actionId = actionId;
    }

    /** A key just registered: {@link Phase#ANNOUNCED}, generation 1, nothing backfilled. */
    static KeyState announced(int incarnation, Epoch epoch, long nowMillis, UUID actionId)
    {
        return new KeyState(Phase.ANNOUNCED, epoch, nowMillis, incarnation, 1, epoch, ImmutableSortedSet.of(), Objects.requireNonNull(actionId));
    }

    /** The state after moving to {@code to}; a different {@code generation} clears {@link #backfillDone}. */
    KeyState advanced(Phase to, int toGeneration, Epoch epoch, long nowMillis)
    {
        boolean restarted = toGeneration != generation;
        return new KeyState(to, epoch, nowMillis, incarnation, toGeneration, restarted ? epoch : generationEpoch,
                            restarted ? ImmutableSortedSet.<NodeId>of() : backfillDone, actionId,
                            to == Phase.DUAL_WRITE && phase == Phase.ANNOUNCED ? nowMillis : dualWriteSinceMillis);
    }

    KeyState withBackfillDone(NodeId node)
    {
        return new KeyState(phase, phaseEpoch, phaseSinceMillis, incarnation, generation, generationEpoch,
                            ImmutableSortedSet.<NodeId>naturalOrder().addAll(backfillDone).add(node).build(), actionId, dualWriteSinceMillis);
    }

    KeyState finished(Epoch epoch, long nowMillis)
    {
        return new KeyState(Phase.STEADY, epoch, nowMillis, incarnation, generation, generationEpoch, backfillDone, null, dualWriteSinceMillis);
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
               && backfillDone.equals(k.backfillDone) && Objects.equals(actionId, k.actionId) && dualWriteSinceMillis == k.dualWriteSinceMillis;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(phase, phaseEpoch, phaseSinceMillis, incarnation, generation, generationEpoch, backfillDone, actionId, dualWriteSinceMillis);
    }

    @Override
    public String toString()
    {
        return "KeyState{" + phase + " phaseEpoch=" + phaseEpoch.getEpoch() + " since=" + phaseSinceMillis + " incarnation=" + incarnation
               + " generation=" + generation + " generationEpoch=" + generationEpoch.getEpoch() + " backfillDone=" + backfillDone
               + " action=" + actionId + " dualWriteSince=" + dualWriteSinceMillis + '}';
    }

    static void serialize(KeyState k, DataOutputPlus out, Version version) throws IOException
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
    }

    static KeyState deserialize(DataInputPlus in, Version version) throws IOException
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
        if (nodes < 0 || nodes > LayoutRegistry.MAX_BACKFILL_NODES)
            throw new IOException("Corrupt autonomic layout registry: " + nodes + " backfill nodes");
        ImmutableSortedSet.Builder<NodeId> done = ImmutableSortedSet.naturalOrder();
        for (int i = 0; i < nodes; i++)
            done.add(NodeId.serializer.deserialize(in, version));
        UUID actionId = in.readBoolean() ? new UUID(in.readLong(), in.readLong()) : null;
        long dualWriteSince = in.readLong();
        try
        {
            return new KeyState(phase, phaseEpoch, since, incarnation, generation, generationEpoch, done.build(), actionId, dualWriteSince);
        }
        catch (IllegalArgumentException e)
        {
            throw new IOException("Corrupt autonomic layout registry: " + e.getMessage(), e);
        }
    }

    static long serializedSize(KeyState k, Version version)
    {
        long size = 1 + Epoch.serializer.serializedSize(k.phaseEpoch, version) + Long.BYTES
                    + TypeSizes.sizeofUnsignedVInt(k.incarnation) + TypeSizes.sizeofUnsignedVInt(k.generation)
                    + Epoch.serializer.serializedSize(k.generationEpoch, version) + TypeSizes.sizeofUnsignedVInt(k.backfillDone.size());
        for (NodeId node : k.backfillDone)
            size += NodeId.serializer.serializedSize(node, version);
        return size + 1 + (k.actionId == null ? 0 : 2 * Long.BYTES) + Long.BYTES;
    }
}
