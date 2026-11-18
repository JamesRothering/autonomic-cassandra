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

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Reports that a node holds no hint, created before the split, for any natural replica of the key (ADR-035). Committed
 * by every directory member while the key is in {@link Phase#ANNOUNCED}, by {@link HintReport}; it needs no claim and
 * is idempotent. The report names the ANNOUNCED phase epoch it was made for, so that a late report of an earlier
 * attempt does not count for a key registered again.
 */
public final class ReportHints extends LayoutTransformation
{
    public static final String NAME = "org.apache.cassandra.autonomic.layout.ReportHints";
    public static final Serializer serializer = new Serializer();

    public final TableId logical;
    public final ByteBuffer keyBytes;
    /** {@link KeyState#phaseEpoch} of the ANNOUNCED phase, as the reporting node saw it. */
    public final Epoch phaseEpoch;
    public final NodeId nodeId;

    public ReportHints(TableId logical, ByteBuffer keyBytes, Epoch phaseEpoch, NodeId nodeId)
    {
        this.logical = Objects.requireNonNull(logical);
        this.keyBytes = TableLayout.key(keyBytes);
        this.phaseEpoch = Objects.requireNonNull(phaseEpoch);
        this.nodeId = Objects.requireNonNull(nodeId);
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    ClaimStep claimStep()
    {
        return null;
    }

    @Override
    String claimPhase()
    {
        throw new IllegalStateException("ReportHints has no claim");
    }

    @Override
    LayoutRegistry.Result apply(LayoutRegistry registry, ClusterMetadata prev)
    {
        return registry.withHintsReported(logical, keyBytes, phaseEpoch, nodeId);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof ReportHints))
            return false;
        ReportHints r = (ReportHints) o;
        return logical.equals(r.logical) && keyBytes.equals(r.keyBytes) && phaseEpoch.equals(r.phaseEpoch) && nodeId.equals(r.nodeId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(logical, keyBytes, phaseEpoch, nodeId);
    }

    @Override
    public String toString()
    {
        return "ReportHints{" + logical + " key=" + ByteBufferUtil.bytesToHex(keyBytes) + " phaseEpoch=" + phaseEpoch.getEpoch() + " node=" + nodeId.id() + '}';
    }

    public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, ReportHints>
    {
        private Serializer()
        {
        }

        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            ReportHints r = (ReportHints) t;
            out.writeByte(FORMAT);
            r.logical.serialize(out);
            writeKey(r.keyBytes, out);
            Epoch.serializer.serialize(r.phaseEpoch, out, version);
            out.writeInt(r.nodeId.id());
        }

        @Override
        public ReportHints deserialize(DataInputPlus in, Version version) throws IOException
        {
            checkFormat(in.readByte(), NAME);
            TableId logical = TableId.deserialize(in);
            ByteBuffer key = readKey(in);
            Epoch phaseEpoch = Epoch.serializer.deserialize(in, version);
            return new ReportHints(logical, key, phaseEpoch, new NodeId(in.readInt()));
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            ReportHints r = (ReportHints) t;
            return 1 + r.logical.serializedSize() + keySize(r.keyBytes) + Integer.BYTES + Epoch.serializer.serializedSize(r.phaseEpoch, version);
        }
    }
}
