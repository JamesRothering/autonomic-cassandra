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
 * Hands a key whose migration lost its action to the action that now holds the {@link
 * org.apache.cassandra.autonomic.coord.Lane#SCHEMA_LAYOUT} lane, and advances that action's claim to the key's phase in
 * the same commit (T6.9). A takeover (overview 6.4) and {@code nodetool autonomic layout resume} replace the action
 * id, while every other layout transformation requires {@link KeyState#actionId} to be the committing action. Nothing
 * else of the key changes: not its phase, phase epoch, generation or reports, so routing (ADR-027) is unaffected.
 *
 * <p>Accepted when the key is in {@code phase}, is owned by {@code previousActionId}, and that action holds no lane of
 * the ledger; or, unchanged, when it is already owned by this action. A key in {@link Phase#STEADY} is owned only until
 * its base partition tombstone is written, and only then can it be adopted.
 */
public final class AdoptKey extends LayoutTransformation
{
    public static final String NAME = "org.apache.cassandra.autonomic.layout.AdoptKey";
    public static final Serializer serializer = new Serializer();

    private final ClaimStep claim;
    public final TableId logical;
    public final ByteBuffer keyBytes;
    public final Phase phase;
    public final UUID previousActionId;

    public AdoptKey(UUID actionId, NodeId owner, int expectedStep, TableId logical, ByteBuffer keyBytes, Phase phase, UUID previousActionId,
                    long nowMillis, long leaseMillis)
    {
        this(new ClaimStep(actionId, owner, expectedStep, nowMillis, leaseMillis), logical, keyBytes, phase, previousActionId);
    }

    private AdoptKey(ClaimStep claim, TableId logical, ByteBuffer keyBytes, Phase phase, UUID previousActionId)
    {
        this.claim = claim;
        this.logical = Objects.requireNonNull(logical);
        this.keyBytes = TableLayout.key(keyBytes);
        this.phase = Objects.requireNonNull(phase);
        this.previousActionId = Objects.requireNonNull(previousActionId);
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
        return phase.name();
    }

    @Override
    LayoutRegistry.Result apply(LayoutRegistry registry, ClusterMetadata prev)
    {
        if (!previousActionId.equals(claim.actionId) && ClusterLedger.of(prev).claim(previousActionId) != null)
            return LayoutRegistry.rejected("action " + previousActionId + " still holds a claim");
        return registry.withAdoptedKey(logical, keyBytes, phase, previousActionId, claim.actionId);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof AdoptKey))
            return false;
        AdoptKey a = (AdoptKey) o;
        return claim.equals(a.claim) && logical.equals(a.logical) && keyBytes.equals(a.keyBytes) && phase == a.phase
               && previousActionId.equals(a.previousActionId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(claim, logical, keyBytes, phase, previousActionId);
    }

    @Override
    public String toString()
    {
        return "AdoptKey{" + claim + ' ' + logical + " key=" + ByteBufferUtil.bytesToHex(keyBytes) + ' ' + phase + " from " + previousActionId + '}';
    }

    public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, AdoptKey>
    {
        private Serializer()
        {
        }

        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            AdoptKey a = (AdoptKey) t;
            out.writeByte(FORMAT);
            a.claim.serialize(out);
            a.logical.serialize(out);
            writeKey(a.keyBytes, out);
            out.writeByte(a.phase.id);
            writeUuid(a.previousActionId, out);
        }

        @Override
        public AdoptKey deserialize(DataInputPlus in, Version version) throws IOException
        {
            checkFormat(in.readByte(), NAME);
            ClaimStep claim = ClaimStep.deserialize(in);
            TableId logical = TableId.deserialize(in);
            ByteBuffer key = readKey(in);
            Phase phase = readPhase(in);
            return new AdoptKey(claim, logical, key, phase, readUuid(in));
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            AdoptKey a = (AdoptKey) t;
            return 1 + ClaimStep.SERIALIZED_SIZE + a.logical.serializedSize() + keySize(a.keyBytes) + 1 + 2 * Long.BYTES;
        }
    }
}
