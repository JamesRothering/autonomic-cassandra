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
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Reports that a replica finished a backfill generation (design 2.4). Committed by replicas, so it needs no claim
 * and is idempotent.
 */
public final class ReportBackfill extends LayoutTransformation
{
    public static final String NAME = "org.apache.cassandra.autonomic.layout.ReportBackfill";
    public static final Serializer serializer = new Serializer();

    public final TableId logical;
    public final ByteBuffer keyBytes;
    public final int generation;
    public final NodeId nodeId;

    public ReportBackfill(TableId logical, ByteBuffer keyBytes, int generation, NodeId nodeId)
    {
        this.logical = Objects.requireNonNull(logical);
        this.keyBytes = TableLayout.key(keyBytes);
        this.generation = generation;
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
        throw new IllegalStateException("ReportBackfill has no claim");
    }

    @Override
    LayoutRegistry.Result apply(LayoutRegistry registry, ClusterMetadata prev)
    {
        return registry.withBackfillReported(logical, keyBytes, generation, nodeId);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof ReportBackfill))
            return false;
        ReportBackfill r = (ReportBackfill) o;
        return logical.equals(r.logical) && keyBytes.equals(r.keyBytes) && generation == r.generation && nodeId.equals(r.nodeId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(logical, keyBytes, generation, nodeId);
    }

    @Override
    public String toString()
    {
        return "ReportBackfill{" + logical + " key=" + ByteBufferUtil.bytesToHex(keyBytes) + " generation=" + generation + " node=" + nodeId.id() + '}';
    }

    public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, ReportBackfill>
    {
        private Serializer()
        {
        }

        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            ReportBackfill r = (ReportBackfill) t;
            out.writeByte(FORMAT);
            r.logical.serialize(out);
            writeKey(r.keyBytes, out);
            out.writeInt(r.generation);
            out.writeInt(r.nodeId.id());
        }

        @Override
        public ReportBackfill deserialize(DataInputPlus in, Version version) throws IOException
        {
            checkFormat(in.readByte(), NAME);
            TableId logical = TableId.deserialize(in);
            ByteBuffer key = readKey(in);
            return new ReportBackfill(logical, key, in.readInt(), new NodeId(in.readInt()));
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            ReportBackfill r = (ReportBackfill) t;
            return 1 + r.logical.serializedSize() + keySize(r.keyBytes) + 2 * Integer.BYTES;
        }
    }
}
