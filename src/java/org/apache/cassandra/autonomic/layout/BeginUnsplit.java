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
import java.util.Objects;
import java.util.UUID;

import org.apache.cassandra.autonomic.coord.ClusterLedger;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Starts the reverse migration of a split key (design 11.4, ADR-036): the key moves to
 * {@link Phase#REVERSE_ANNOUNCED} in a new generation, owned by the action that holds the
 * {@link org.apache.cassandra.autonomic.coord.Lane#SCHEMA_LAYOUT} lane, with the mode fixed for the whole reverse. The
 * claim advances in the same commit.
 *
 * <p>Accepted from {@link Phase#STEADY} owned by nobody, and from {@link Phase#NEW_ONLY} and {@link Phase#CLEANUP}
 * owned by {@link #previousActionId}, a forward action that holds no claim any more (as for {@code AdoptKey}). The
 * first commit of format 2 values: the registry is upgraded here, if every node of the directory can read format 2.
 */
public final class BeginUnsplit extends LayoutTransformation
{
    public static final String NAME = "org.apache.cassandra.autonomic.layout.BeginUnsplit";
    public static final Serializer serializer = new Serializer();

    private final ClaimStep claim;
    public final TableId logical;
    public final ByteBuffer keyBytes;
    public final KeyState.ReverseMode mode;
    /** The forward action that owns a {@code NEW_ONLY} or {@code CLEANUP} key; null for a {@code STEADY} key. */
    public final UUID previousActionId;

    public BeginUnsplit(UUID actionId, NodeId owner, int expectedStep, TableId logical, ByteBuffer keyBytes, KeyState.ReverseMode mode,
                        UUID previousActionId, long nowMillis, long leaseMillis)
    {
        this(new ClaimStep(actionId, owner, expectedStep, nowMillis, leaseMillis), logical, keyBytes, mode, previousActionId);
    }

    private BeginUnsplit(ClaimStep claim, TableId logical, ByteBuffer keyBytes, KeyState.ReverseMode mode, UUID previousActionId)
    {
        this.claim = claim;
        this.logical = Objects.requireNonNull(logical);
        this.keyBytes = TableLayout.key(keyBytes);
        this.mode = Objects.requireNonNull(mode);
        this.previousActionId = previousActionId;
    }

    public UUID actionId()
    {
        return claim.actionId;
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    ClaimStep claimStep()
    {
        return claim;
    }

    @Override
    String claimPhase()
    {
        return Phase.REVERSE_ANNOUNCED.name();
    }

    @Override
    LayoutRegistry.Result apply(LayoutRegistry registry, ClusterMetadata prev)
    {
        if (previousActionId != null && !previousActionId.equals(claim.actionId) && ClusterLedger.of(prev).claim(previousActionId) != null)
            return LayoutRegistry.rejected("action " + previousActionId + " still holds a claim");
        return registry.withBegunReverse(logical, keyBytes, claim.actionId, previousActionId, mode, LayoutRegistry.formatGateRefusal(prev),
                                         prev.nextEpoch(), claim.nowMillis);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof BeginUnsplit))
            return false;
        BeginUnsplit b = (BeginUnsplit) o;
        return claim.equals(b.claim) && logical.equals(b.logical) && keyBytes.equals(b.keyBytes) && mode == b.mode
               && Objects.equals(previousActionId, b.previousActionId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(claim, logical, keyBytes, mode, previousActionId);
    }

    @Override
    public String toString()
    {
        return "BeginUnsplit{" + claim + ' ' + logical + " key=" + ByteBufferUtil.bytesToHex(keyBytes) + ' ' + mode
               + (previousActionId == null ? "" : " from " + previousActionId) + '}';
    }

    public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, BeginUnsplit>
    {
        private Serializer()
        {
        }

        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            BeginUnsplit b = (BeginUnsplit) t;
            out.writeByte(FORMAT);
            b.claim.serialize(out);
            b.logical.serialize(out);
            writeKey(b.keyBytes, out);
            out.writeByte(b.mode.id);
            out.writeBoolean(b.previousActionId != null);
            if (b.previousActionId != null)
                writeUuid(b.previousActionId, out);
        }

        @Override
        public BeginUnsplit deserialize(DataInputPlus in, Version version) throws IOException
        {
            checkFormat(in.readByte(), NAME);
            ClaimStep claim = ClaimStep.deserialize(in);
            TableId logical = TableId.deserialize(in);
            ByteBuffer key = readKey(in);
            int id = in.readByte();
            KeyState.ReverseMode mode = KeyState.ReverseMode.fromId(id);
            if (mode == null)
                throw new IOException("Unknown reverse mode " + id + " in autonomic transformation " + NAME);
            return new BeginUnsplit(claim, logical, key, mode, in.readBoolean() ? readUuid(in) : null);
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            BeginUnsplit b = (BeginUnsplit) t;
            return 1 + ClaimStep.SERIALIZED_SIZE + b.logical.serializedSize() + keySize(b.keyBytes) + 1 + 1 + (b.previousActionId == null ? 0 : 2 * Long.BYTES);
        }
    }
}
