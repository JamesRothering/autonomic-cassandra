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
 * Records that the owner of a {@link Phase#STEADY} key has written its base partition tombstone (ADR-033): the key is
 * owned by no action from here on, and the claim advances to {@link #CLAIM_PHASE} in the same commit. Until then a
 * STEADY key keeps its action, so that a key whose owner was lost before the tombstone can be found and adopted. No
 * routing state changes (ADR-027).
 *
 * <p>It carries the base state of the partition after the release (design 11.5): {@code TOMBSTONED} with the tombstone
 * time {@code Tt} (0 if unknown), or {@code RESIDUE}. The first is what every release meant before format 2.
 */
public final class ReleaseKey extends LayoutTransformation
{
    public static final String NAME = "org.apache.cassandra.autonomic.layout.ReleaseKey";
    public static final Serializer serializer = new Serializer();
    /** The phase of the claim after this commit. */
    public static final String CLAIM_PHASE = "KEY_RELEASED";
    /** Adds {@link #baseState} and {@link #baseTombstoneMicros}. */
    static final int FORMAT_2 = 2;

    private final ClaimStep claim;
    public final TableId logical;
    public final ByteBuffer keyBytes;
    /** What the base partition holds after the release (design 11.5). */
    public final KeyState.BaseState baseState;
    /** {@code Tt} of the base tombstone in microseconds; 0 if unknown, and always 0 for {@code RESIDUE}. */
    public final long baseTombstoneMicros;

    /** The release that follows a base partition tombstone of unknown time: the only one before format 2. */
    public ReleaseKey(UUID actionId, NodeId owner, int expectedStep, TableId logical, ByteBuffer keyBytes, long nowMillis, long leaseMillis)
    {
        this(actionId, owner, expectedStep, logical, keyBytes, KeyState.BaseState.TOMBSTONED, 0, nowMillis, leaseMillis);
    }

    public ReleaseKey(UUID actionId, NodeId owner, int expectedStep, TableId logical, ByteBuffer keyBytes, KeyState.BaseState baseState,
                      long baseTombstoneMicros, long nowMillis, long leaseMillis)
    {
        this(new ClaimStep(actionId, owner, expectedStep, nowMillis, leaseMillis), logical, keyBytes, baseState, baseTombstoneMicros);
    }

    private ReleaseKey(ClaimStep claim, TableId logical, ByteBuffer keyBytes, KeyState.BaseState baseState, long baseTombstoneMicros)
    {
        this.claim = claim;
        this.logical = Objects.requireNonNull(logical);
        this.keyBytes = TableLayout.key(keyBytes);
        this.baseState = Objects.requireNonNull(baseState);
        this.baseTombstoneMicros = baseTombstoneMicros;
    }

    /** True if the release is not what format 1 meant, so that the registry must be in format 2 to hold it. */
    private boolean needsFormat2()
    {
        return baseState != KeyState.BaseState.TOMBSTONED || baseTombstoneMicros != 0;
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
        return CLAIM_PHASE;
    }

    @Override
    LayoutRegistry.Result apply(LayoutRegistry registry, ClusterMetadata prev)
    {
        return registry.withReleasedKey(logical, keyBytes, claim.actionId, baseState, baseTombstoneMicros, LayoutRegistry.formatGateRefusal(prev));
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof ReleaseKey))
            return false;
        ReleaseKey r = (ReleaseKey) o;
        return claim.equals(r.claim) && logical.equals(r.logical) && keyBytes.equals(r.keyBytes) && baseState == r.baseState
               && baseTombstoneMicros == r.baseTombstoneMicros;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(claim, logical, keyBytes, baseState, baseTombstoneMicros);
    }

    @Override
    public String toString()
    {
        return "ReleaseKey{" + claim + ' ' + logical + " key=" + ByteBufferUtil.bytesToHex(keyBytes) + ' ' + baseState + " tt=" + baseTombstoneMicros + '}';
    }

    public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, ReleaseKey>
    {
        private Serializer()
        {
        }

        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            ReleaseKey r = (ReleaseKey) t;
            // the format 1 form when it says the same, so that a node that reads only format 1 can still read the log
            out.writeByte(r.needsFormat2() ? FORMAT_2 : FORMAT);
            r.claim.serialize(out);
            r.logical.serialize(out);
            writeKey(r.keyBytes, out);
            if (r.needsFormat2())
            {
                out.writeByte(r.baseState.id);
                out.writeLong(r.baseTombstoneMicros);
            }
        }

        @Override
        public ReleaseKey deserialize(DataInputPlus in, Version version) throws IOException
        {
            int format = checkFormat(in.readByte(), FORMAT_2, NAME);
            ClaimStep claim = ClaimStep.deserialize(in);
            TableId logical = TableId.deserialize(in);
            ByteBuffer key = readKey(in);
            if (format == FORMAT)
                return new ReleaseKey(claim, logical, key, KeyState.BaseState.TOMBSTONED, 0);
            int id = in.readByte();
            KeyState.BaseState base = KeyState.BaseState.fromId(id);
            if (base == null)
                throw new IOException("Unknown base state " + id + " in autonomic transformation " + NAME);
            return new ReleaseKey(claim, logical, key, base, in.readLong());
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            ReleaseKey r = (ReleaseKey) t;
            return 1 + ClaimStep.SERIALIZED_SIZE + r.logical.serializedSize() + keySize(r.keyBytes) + (r.needsFormat2() ? 1 + Long.BYTES : 0);
        }
    }
}
