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

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.TreeSet;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;

/**
 * The natural (read) replicas of a key of a logical table in the placements of one metadata (design 4.5). A pure
 * function of the metadata: used by the owner's completion test and by {@code FinishKey} (ADR-033).
 */
public final class NaturalReplicas
{
    private NaturalReplicas()
    {
    }

    /** Null if the keyspace is gone or its placement has no replica group for the key's token. */
    private static VersionedEndpoints.ForToken forToken(ClusterMetadata cm, TableMetadata logical, ByteBuffer key)
    {
        KeyspaceMetadata keyspace = cm.schema.getKeyspaces().getNullable(logical.keyspace);
        if (keyspace == null)
            return null;
        try
        {
            // the token in the metadata's partitioner, the one its placements are in
            return cm.placement(keyspace.params.replication).reads.forToken(cm.partitioner.getToken(key));
        }
        catch (IllegalStateException e)
        {
            // no replica group covers the token: an empty placement
            return null;
        }
    }

    /** The node ids of the natural replicas; empty if there are none. */
    public static Set<NodeId> of(ClusterMetadata cm, TableMetadata logical, ByteBuffer key)
    {
        Set<NodeId> out = new TreeSet<>();
        VersionedEndpoints.ForToken forToken = forToken(cm, logical, key);
        if (forToken == null)
            return out;
        for (InetAddressAndPort endpoint : forToken.get().endpoints())
        {
            NodeId id = cm.directory.peerId(endpoint);
            if (id != null)
                out.add(id);
        }
        return out;
    }

    /** The epoch at which the key's replica group was last modified; {@link Epoch#EMPTY} if there is none. */
    public static Epoch lastModified(ClusterMetadata cm, TableMetadata logical, ByteBuffer key)
    {
        VersionedEndpoints.ForToken forToken = forToken(cm, logical, key);
        return forToken == null ? Epoch.EMPTY : forToken.lastModified();
    }
}
