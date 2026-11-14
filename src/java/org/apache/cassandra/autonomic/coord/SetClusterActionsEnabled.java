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

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

/** Sets the operator's gate for cluster-scoped actions; creates the ledger if absent. Always succeeds. */
public final class SetClusterActionsEnabled extends LedgerTransformation
{
    public static final String NAME = "org.apache.cassandra.autonomic.coord.SetClusterActionsEnabled";
    public static final Serializer serializer = new Serializer();

    public final boolean enabled;

    public SetClusterActionsEnabled(boolean enabled)
    {
        this.enabled = enabled;
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    ClusterLedger.Result apply(ClusterLedger ledger, ClusterMetadata prev)
    {
        return ledger.withEnabled(enabled);
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof SetClusterActionsEnabled && ((SetClusterActionsEnabled) o).enabled == enabled;
    }

    @Override
    public int hashCode()
    {
        return Boolean.hashCode(enabled);
    }

    @Override
    public String toString()
    {
        return "SetClusterActionsEnabled{" + enabled + '}';
    }

    public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, SetClusterActionsEnabled>
    {
        private Serializer()
        {
        }

        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            out.writeByte(FORMAT);
            out.writeBoolean(((SetClusterActionsEnabled) t).enabled);
        }

        @Override
        public SetClusterActionsEnabled deserialize(DataInputPlus in, Version version) throws IOException
        {
            checkFormat(in.readByte(), NAME);
            return new SetClusterActionsEnabled(in.readBoolean());
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            return 2;
        }
    }
}
