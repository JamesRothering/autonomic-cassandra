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
import java.util.Objects;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

/**
 * Removes the layout of a logical table that no longer exists, after {@code DROP TABLE} or {@code DROP KEYSPACE}
 * (design 2.4, 4.8). Needs no claim.
 */
public final class RemoveOrphanLayout extends LayoutTransformation
{
    public static final String NAME = "org.apache.cassandra.autonomic.layout.RemoveOrphanLayout";
    public static final Serializer serializer = new Serializer();

    public final TableId logical;

    public RemoveOrphanLayout(TableId logical)
    {
        this.logical = Objects.requireNonNull(logical);
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
        throw new IllegalStateException("RemoveOrphanLayout has no claim");
    }

    @Override
    LayoutRegistry.Result apply(LayoutRegistry registry, ClusterMetadata prev)
    {
        if (prev.schema.getTableMetadata(logical) != null)
            return LayoutRegistry.rejected("table " + logical + " exists, its layout is not an orphan");
        return registry.withoutLayout(logical);
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof RemoveOrphanLayout && logical.equals(((RemoveOrphanLayout) o).logical);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(logical);
    }

    @Override
    public String toString()
    {
        return "RemoveOrphanLayout{" + logical + '}';
    }

    public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, RemoveOrphanLayout>
    {
        private Serializer()
        {
        }

        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            out.writeByte(FORMAT);
            ((RemoveOrphanLayout) t).logical.serialize(out);
        }

        @Override
        public RemoveOrphanLayout deserialize(DataInputPlus in, Version version) throws IOException
        {
            checkFormat(in.readByte(), NAME);
            return new RemoveOrphanLayout(TableId.deserialize(in));
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            return 1 + ((RemoveOrphanLayout) t).logical.serializedSize();
        }
    }
}
