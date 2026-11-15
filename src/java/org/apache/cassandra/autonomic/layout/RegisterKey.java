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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Adds a partition key to the layout of a logical table, creating the layout if there is none (design 2.4), and
 * advances the claim to {@link Phase#ANNOUNCED} in the same commit.
 */
public final class RegisterKey extends LayoutTransformation
{
    public static final String NAME = "org.apache.cassandra.autonomic.layout.RegisterKey";
    public static final Serializer serializer = new Serializer();

    private final ClaimStep claim;
    public final LayoutHeader header;
    public final ByteBuffer keyBytes;

    public RegisterKey(UUID actionId, NodeId owner, int expectedStep, LayoutHeader header, ByteBuffer keyBytes, long nowMillis, long leaseMillis)
    {
        this(new ClaimStep(actionId, owner, expectedStep, nowMillis, leaseMillis), header, keyBytes);
    }

    private RegisterKey(ClaimStep claim, LayoutHeader header, ByteBuffer keyBytes)
    {
        this.claim = claim;
        this.header = Objects.requireNonNull(header);
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
        return Phase.ANNOUNCED.name();
    }

    @Override
    LayoutRegistry.Result apply(LayoutRegistry registry, ClusterMetadata prev)
    {
        if (!prev.inProgressSequences.isEmpty())
            return reject("a metadata operation is in progress");
        String invalid = header.invalid();
        if (invalid != null)
            return reject(invalid);
        TableMetadata logical = prev.schema.getTableMetadata(header.logical);
        if (logical == null)
            return reject("logical table " + header.logical + " does not exist");
        TableMetadata physical = prev.schema.getTableMetadata(header.physical);
        if (physical == null)
            return reject("physical table " + header.physical + " does not exist");
        if (!logical.keyspace.equals(header.keyspace) || !logical.name.equals(header.logicalName))
            return reject("table " + header.logical + " is " + logical.keyspace + '.' + logical.name + ", not " + header.keyspace + '.' + header.logicalName);
        if (!physical.keyspace.equals(header.keyspace) || !physical.name.equals(header.physicalName))
            return reject("table " + header.physical + " is " + physical.keyspace + '.' + physical.name + ", not " + header.keyspace + '.' + header.physicalName);
        String mismatch = shapeMismatch(logical, physical);
        if (mismatch != null)
            return reject("physical table does not match the logical table: " + mismatch);
        if (header.hashedClusteringColumns > logical.clusteringColumns().size())
            return reject("hashed clustering columns " + header.hashedClusteringColumns + " exceeds the " + logical.clusteringColumns().size()
                          + " clustering columns of " + logical.keyspace + '.' + logical.name);
        return registry.withRegisteredKey(header, keyBytes, claim.actionId, prev.nextEpoch(), claim.nowMillis);
    }

    private static LayoutRegistry.Result reject(String reason)
    {
        return LayoutRegistry.rejected(reason);
    }

    /**
     * Null if {@code physical} has the shape of design 2.1 for {@code logical}: the partition key of the logical table
     * plus a trailing int bucket column, and identical clustering, static and regular columns. (This is the structural
     * part of {@code PhysicalTables.matches}; T6.2 owns the full check.)
     */
    static String shapeMismatch(TableMetadata logical, TableMetadata physical)
    {
        List<ColumnMetadata> lpk = logical.partitionKeyColumns();
        List<ColumnMetadata> ppk = physical.partitionKeyColumns();
        if (ppk.size() != lpk.size() + 1)
            return "partition key has " + ppk.size() + " columns, expected " + (lpk.size() + 1);
        for (int i = 0; i < lpk.size(); i++)
            if (!same(lpk.get(i), ppk.get(i)))
                return "partition key column " + i + " differs";
        ColumnMetadata bucket = ppk.get(lpk.size());
        if (!bucket.name.toString().equals(TableLayout.BUCKET_COLUMN) || !bucket.type.equals(Int32Type.instance))
            return "last partition key column is not an int named " + TableLayout.BUCKET_COLUMN;
        if (!sameColumns(logical.clusteringColumns(), physical.clusteringColumns()))
            return "clustering columns differ";
        if (!sameColumns(new ArrayList<>(logical.staticColumns()), new ArrayList<>(physical.staticColumns())))
            return "static columns differ";
        if (!sameColumns(new ArrayList<>(logical.regularColumns()), new ArrayList<>(physical.regularColumns())))
            return "regular columns differ";
        return null;
    }

    private static boolean same(ColumnMetadata a, ColumnMetadata b)
    {
        return a.name.equals(b.name) && a.type.equals(b.type) && a.kind == b.kind;
    }

    private static boolean sameColumns(List<ColumnMetadata> a, List<ColumnMetadata> b)
    {
        if (a.size() != b.size())
            return false;
        for (int i = 0; i < a.size(); i++)
            if (!same(a.get(i), b.get(i)))
                return false;
        return true;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof RegisterKey))
            return false;
        RegisterKey r = (RegisterKey) o;
        return claim.equals(r.claim) && header.equals(r.header) && keyBytes.equals(r.keyBytes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(claim, header, keyBytes);
    }

    @Override
    public String toString()
    {
        return "RegisterKey{" + claim + ' ' + header + " key=" + ByteBufferUtil.bytesToHex(keyBytes) + '}';
    }

    public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, RegisterKey>
    {
        private Serializer()
        {
        }

        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            RegisterKey r = (RegisterKey) t;
            out.writeByte(FORMAT);
            r.claim.serialize(out);
            LayoutHeader.serialize(r.header, out);
            writeKey(r.keyBytes, out);
        }

        @Override
        public RegisterKey deserialize(DataInputPlus in, Version version) throws IOException
        {
            checkFormat(in.readByte(), NAME);
            ClaimStep claim = ClaimStep.deserialize(in);
            LayoutHeader header = LayoutHeader.deserialize(in);
            return new RegisterKey(claim, header, readKey(in));
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            RegisterKey r = (RegisterKey) t;
            return 1 + ClaimStep.SERIALIZED_SIZE + LayoutHeader.serializedSize(r.header) + keySize(r.keyBytes);
        }
    }
}
