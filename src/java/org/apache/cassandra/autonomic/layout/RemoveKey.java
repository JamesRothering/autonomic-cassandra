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

/** Removes a key in {@link Phase#ABORTING}, and its layout with the last key (design 2.4), and advances the claim in the same commit. */
public final class RemoveKey extends LayoutTransformation
{
    public static final String NAME = "org.apache.cassandra.autonomic.layout.RemoveKey";
    public static final Serializer serializer = new Serializer();

    private final ClaimStep claim;
    public final TableId logical;
    public final ByteBuffer keyBytes;

    public RemoveKey(UUID actionId, NodeId owner, int expectedStep, TableId logical, ByteBuffer keyBytes, long nowMillis, long leaseMillis)
    {
        this(new ClaimStep(actionId, owner, expectedStep, nowMillis, leaseMillis), logical, keyBytes);
    }

    private RemoveKey(ClaimStep claim, TableId logical, ByteBuffer keyBytes)
    {
        this.claim = claim;
        this.logical = Objects.requireNonNull(logical);
        this.keyBytes = TableLayout.key(keyBytes);
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
        return "KEY_REMOVED";
    }

    @Override
    LayoutRegistry.Result apply(LayoutRegistry registry, ClusterMetadata prev)
    {
        return registry.withoutKey(logical, keyBytes, claim.actionId);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof RemoveKey))
            return false;
        RemoveKey r = (RemoveKey) o;
        return claim.equals(r.claim) && logical.equals(r.logical) && keyBytes.equals(r.keyBytes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(claim, logical, keyBytes);
    }

    @Override
    public String toString()
    {
        return "RemoveKey{" + claim + ' ' + logical + " key=" + ByteBufferUtil.bytesToHex(keyBytes) + '}';
    }

    public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, RemoveKey>
    {
        private Serializer()
        {
        }

        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            RemoveKey r = (RemoveKey) t;
            out.writeByte(FORMAT);
            r.claim.serialize(out);
            r.logical.serialize(out);
            writeKey(r.keyBytes, out);
        }

        @Override
        public RemoveKey deserialize(DataInputPlus in, Version version) throws IOException
        {
            checkFormat(in.readByte(), NAME);
            ClaimStep claim = ClaimStep.deserialize(in);
            TableId logical = TableId.deserialize(in);
            return new RemoveKey(claim, logical, readKey(in));
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            RemoveKey r = (RemoveKey) t;
            return 1 + ClaimStep.SERIALIZED_SIZE + r.logical.serializedSize() + keySize(r.keyBytes);
        }
    }
}
