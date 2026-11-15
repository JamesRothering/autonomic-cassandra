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
 * Moves a key from phase {@code from} to {@code to} (design 2.4, 4.2), or restarts the backfill generation
 * ({@code from == to}, {@code generation} one higher), and advances the claim to the new phase in the same commit.
 */
public final class AdvanceLayout extends LayoutTransformation
{
    public static final String NAME = "org.apache.cassandra.autonomic.layout.AdvanceLayout";
    public static final Serializer serializer = new Serializer();

    private final ClaimStep claim;
    public final TableId logical;
    public final ByteBuffer keyBytes;
    public final Phase from;
    public final Phase to;
    public final int generation;

    public AdvanceLayout(UUID actionId, NodeId owner, int expectedStep, TableId logical, ByteBuffer keyBytes, Phase from, Phase to,
                         int generation, long nowMillis, long leaseMillis)
    {
        this(new ClaimStep(actionId, owner, expectedStep, nowMillis, leaseMillis), logical, keyBytes, from, to, generation);
    }

    private AdvanceLayout(ClaimStep claim, TableId logical, ByteBuffer keyBytes, Phase from, Phase to, int generation)
    {
        this.claim = claim;
        this.logical = Objects.requireNonNull(logical);
        this.keyBytes = TableLayout.key(keyBytes);
        this.from = Objects.requireNonNull(from);
        this.to = Objects.requireNonNull(to);
        this.generation = generation;
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
        return to.name();
    }

    @Override
    LayoutRegistry.Result apply(LayoutRegistry registry, ClusterMetadata prev)
    {
        return registry.withAdvancedKey(logical, keyBytes, claim.actionId, from, to, generation, prev.nextEpoch(), claim.nowMillis);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof AdvanceLayout))
            return false;
        AdvanceLayout a = (AdvanceLayout) o;
        return claim.equals(a.claim) && logical.equals(a.logical) && keyBytes.equals(a.keyBytes) && from == a.from && to == a.to && generation == a.generation;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(claim, logical, keyBytes, from, to, generation);
    }

    @Override
    public String toString()
    {
        return "AdvanceLayout{" + claim + ' ' + logical + " key=" + ByteBufferUtil.bytesToHex(keyBytes) + ' ' + from + " -> " + to + " generation=" + generation + '}';
    }

    public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, AdvanceLayout>
    {
        private Serializer()
        {
        }

        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            AdvanceLayout a = (AdvanceLayout) t;
            out.writeByte(FORMAT);
            a.claim.serialize(out);
            a.logical.serialize(out);
            writeKey(a.keyBytes, out);
            out.writeByte(a.from.id);
            out.writeByte(a.to.id);
            out.writeInt(a.generation);
        }

        @Override
        public AdvanceLayout deserialize(DataInputPlus in, Version version) throws IOException
        {
            checkFormat(in.readByte(), NAME);
            ClaimStep claim = ClaimStep.deserialize(in);
            TableId logical = TableId.deserialize(in);
            ByteBuffer key = readKey(in);
            Phase from = readPhase(in);
            Phase to = readPhase(in);
            return new AdvanceLayout(claim, logical, key, from, to, in.readInt());
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            AdvanceLayout a = (AdvanceLayout) t;
            return 1 + ClaimStep.SERIALIZED_SIZE + a.logical.serializedSize() + keySize(a.keyBytes) + 2 + Integer.BYTES;
        }
    }
}
