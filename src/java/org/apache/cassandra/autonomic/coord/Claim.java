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
package org.apache.cassandra.autonomic.coord;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.membership.NodeId;

/**
 * A lease on a lane of the cluster ledger (overview 6.2). Immutable. {@link #step} is the compare-and-set token of
 * {@code AdvanceAction}; all times are wall-clock milliseconds supplied by the proposer.
 */
public final class Claim
{
    /** Phase of a claim that took over an expired one (overview 6.4). */
    public static final String RECOVERING = "RECOVERING";

    public static final int MAX_STATE_ENTRIES = 16;
    /** Bound of the serialized {@link #state}. */
    public static final int MAX_STATE_BYTES = 4096;
    /** Bound of the UTF-8 length of {@link #actuator} and {@link #phase}. */
    public static final int MAX_NAME_BYTES = 128;
    /** Bound of the UTF-8 length of {@link #target}. */
    public static final int MAX_TARGET_BYTES = 1024;

    public final UUID actionId;
    public final String actuator;
    public final String target;
    public final NodeId owner;
    /** Actuator-defined, for example "PROVISIONING". */
    public final String phase;
    public final int step;
    public final long claimedAtMillis;
    public final long leaseExpiresAtMillis;
    /** Small actuator state, sorted by key: at most {@link #MAX_STATE_ENTRIES} entries and {@link #MAX_STATE_BYTES} serialized. */
    public final ImmutableMap<String, String> state;

    public Claim(UUID actionId, String actuator, String target, NodeId owner, String phase, int step,
                 long claimedAtMillis, long leaseExpiresAtMillis, Map<String, String> state)
    {
        this.actionId = Objects.requireNonNull(actionId);
        this.actuator = Objects.requireNonNull(actuator);
        this.target = Objects.requireNonNull(target);
        this.owner = Objects.requireNonNull(owner);
        this.phase = Objects.requireNonNull(phase);
        this.step = step;
        this.claimedAtMillis = claimedAtMillis;
        this.leaseExpiresAtMillis = leaseExpiresAtMillis;
        this.state = ImmutableSortedMap.copyOf(state);
    }

    public Claim withStep(int newStep)
    {
        return new Claim(actionId, actuator, target, owner, phase, newStep, claimedAtMillis, leaseExpiresAtMillis, state);
    }

    /** The claim after one advance: next step, new phase and state, renewed lease. */
    public Claim advanced(String newPhase, Map<String, String> newState, long newLeaseExpiresAtMillis)
    {
        return new Claim(actionId, actuator, target, owner, newPhase, step + 1, claimedAtMillis, newLeaseExpiresAtMillis, newState);
    }

    public boolean heldBy(UUID id, NodeId node)
    {
        return actionId.equals(id) && owner.equals(node);
    }

    /** Null if every bound holds, otherwise why the claim may not be written to the ledger. Nothing is truncated. */
    public String invalid()
    {
        if (step < 0)
            return "negative step";
        String name = invalidName("actuator", actuator, MAX_NAME_BYTES);
        if (name == null)
            name = invalidName("phase", phase, MAX_NAME_BYTES);
        if (name == null)
            name = invalidName("target", target, MAX_TARGET_BYTES);
        return name != null ? name : invalidState(state);
    }

    static String invalidName(String what, String value, int maxBytes)
    {
        if (value == null || value.isEmpty())
            return what + " is empty";
        int bytes = utf8Length(value);
        return bytes > maxBytes ? what + " is " + bytes + " bytes, the limit is " + maxBytes : null;
    }

    /** Null if the state respects the entry and size bounds. */
    public static String invalidState(Map<String, String> state)
    {
        if (state == null)
            return "claim state is null";
        if (state.size() > MAX_STATE_ENTRIES)
            return "claim state has " + state.size() + " entries, the limit is " + MAX_STATE_ENTRIES;
        for (Map.Entry<String, String> e : state.entrySet())
            if (e.getKey() == null || e.getValue() == null)
                return "claim state has a null key or value";
        long bytes = stateSize(state);
        return bytes > MAX_STATE_BYTES ? "claim state is " + bytes + " bytes serialized, the limit is " + MAX_STATE_BYTES : null;
    }

    static int utf8Length(String s)
    {
        return TypeSizes.sizeof(s) - Short.BYTES;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof Claim))
            return false;
        Claim c = (Claim) o;
        return actionId.equals(c.actionId) && actuator.equals(c.actuator) && target.equals(c.target) && owner.equals(c.owner)
               && phase.equals(c.phase) && step == c.step && claimedAtMillis == c.claimedAtMillis
               && leaseExpiresAtMillis == c.leaseExpiresAtMillis && state.equals(c.state);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(actionId, actuator, target, owner, phase, step, claimedAtMillis, leaseExpiresAtMillis, state);
    }

    @Override
    public String toString()
    {
        return "Claim{" + actionId + ' ' + actuator + ' ' + target + " owner=" + owner.id() + " phase=" + phase + " step=" + step
               + " claimedAt=" + claimedAtMillis + " leaseExpiresAt=" + leaseExpiresAtMillis + " state=" + state + '}';
    }

    // Serialized form, format 1. The state map is sorted by key, so equal claims serialize identically.

    static void serialize(Claim c, DataOutputPlus out) throws IOException
    {
        out.writeLong(c.actionId.getMostSignificantBits());
        out.writeLong(c.actionId.getLeastSignificantBits());
        out.writeUTF(c.actuator);
        out.writeUTF(c.target);
        out.writeInt(c.owner.id());
        out.writeUTF(c.phase);
        out.writeInt(c.step);
        out.writeLong(c.claimedAtMillis);
        out.writeLong(c.leaseExpiresAtMillis);
        serializeState(c.state, out);
    }

    static Claim deserialize(DataInputPlus in) throws IOException
    {
        UUID id = new UUID(in.readLong(), in.readLong());
        String actuator = in.readUTF();
        String target = in.readUTF();
        NodeId owner = new NodeId(in.readInt());
        String phase = in.readUTF();
        int step = in.readInt();
        long claimedAt = in.readLong();
        long leaseExpiresAt = in.readLong();
        return new Claim(id, actuator, target, owner, phase, step, claimedAt, leaseExpiresAt, deserializeState(in));
    }

    static long serializedSize(Claim c)
    {
        return 2 * Long.BYTES + TypeSizes.sizeof(c.actuator) + TypeSizes.sizeof(c.target) + Integer.BYTES + TypeSizes.sizeof(c.phase)
               + Integer.BYTES + 2 * Long.BYTES + stateSize(c.state);
    }

    static void serializeState(Map<String, String> state, DataOutputPlus out) throws IOException
    {
        if (state.size() > 0xFFFF)
            throw new IOException("claim state has " + state.size() + " entries");
        out.writeShort(state.size());
        for (Map.Entry<String, String> e : state.entrySet())
        {
            out.writeUTF(e.getKey());
            out.writeUTF(e.getValue());
        }
    }

    static ImmutableMap<String, String> deserializeState(DataInputPlus in) throws IOException
    {
        int size = in.readUnsignedShort();
        Map<String, String> state = new LinkedHashMap<>();
        for (int i = 0; i < size; i++)
        {
            String key = in.readUTF();
            state.put(key, in.readUTF());
        }
        return ImmutableSortedMap.copyOf(state);
    }

    static long stateSize(Map<String, String> state)
    {
        long size = Short.BYTES;
        for (Map.Entry<String, String> e : state.entrySet())
            size += TypeSizes.sizeof(e.getKey()) + TypeSizes.sizeof(e.getValue());
        return size;
    }
}
