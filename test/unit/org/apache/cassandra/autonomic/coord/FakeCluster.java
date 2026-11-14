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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.MembershipUtils;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Test-only metadata log shared by several fake nodes. A commit really executes the transformation against the
 * log's metadata, so the ledger rules under test are the production ones; what is faked is the commit path, the
 * clock and the failure detector. Each node has its own view ({@link Node#seen}) that may lag behind the log.
 */
public class FakeCluster
{
    public enum CommitMode
    {
        /** Applied (or rejected) and visible to the committing node at once. */
        APPLY,
        /** Lost before it reached the log: a timeout with nothing applied. */
        DROP,
        /** Applied to the log, but the node times out and does not see the result until {@link Node#catchUp()}. */
        APPLY_UNSEEN
    }

    public final AtomicLong now = new AtomicLong(1_000_000_000_000L);
    public volatile ClusterMetadata log;
    /** Transformations that were applied to the log, in order. */
    public final List<Transformation> applied = new ArrayList<>();
    /** Every commit attempt that reached {@link Node#commit}, applied or not. */
    public int attempts;
    private long ids;

    public FakeCluster(int nodes)
    {
        ClusterMetadata metadata = new ClusterMetadata(Murmur3Partitioner.instance, Directory.EMPTY, DistributedSchema.empty());
        for (int i = 1; i <= nodes; i++)
            metadata = register(metadata, i, NodeVersion.CURRENT);
        log = metadata;
    }

    public static ClusterMetadata register(ClusterMetadata metadata, int i, NodeVersion version)
    {
        NodeAddresses addresses = new NodeAddresses(MembershipUtils.endpoint(i));
        metadata = metadata.transformer().register(addresses, new Location("dc1", "rack1"), version).build().metadata;
        NodeId id = metadata.directory.peerId(addresses.broadcastAddress);
        return metadata.transformer().withNodeState(id, NodeState.JOINED).build().metadata;
    }

    public ClusterLedger ledger()
    {
        return ClusterLedger.of(log);
    }

    /** The id the directory gave the i-th registered node. */
    public NodeId id(int i)
    {
        return log.directory.peerId(MembershipUtils.endpoint(i));
    }

    public Node node(int i)
    {
        return new Node(id(i));
    }

    public class Node implements ClusterCoordinator.Environment
    {
        public final NodeId self;
        public volatile ClusterMetadata seen = log;
        public final Set<NodeId> down = new HashSet<>();
        public volatile CommitMode mode = CommitMode.APPLY;
        public volatile long lease = 600_000;
        public volatile long margin = 60_000;
        public volatile long cooldownAfterCommit = 0;
        public volatile long cooldownAfterRollback = 0;
        public final List<Transformation> attempted = new ArrayList<>();

        Node(NodeId self)
        {
            this.self = self;
        }

        /** The node's view catches up with the log; returns the view it had before. */
        public ClusterMetadata catchUp()
        {
            ClusterMetadata before = seen;
            seen = log;
            return before;
        }

        @Override
        public ClusterMetadata metadata()
        {
            return seen;
        }

        @Override
        public NodeId self()
        {
            return self;
        }

        @Override
        public boolean isAlive(NodeId node)
        {
            return !down.contains(node);
        }

        @Override
        public ClusterCoordinator.Committed commit(Transformation transformation, long timeoutMillis)
        {
            attempts++;
            attempted.add(transformation);
            if (mode == CommitMode.DROP)
                return new ClusterCoordinator.Committed(seen, "timeout");
            Transformation.Result result = transformation.execute(log);
            String failure = null;
            if (result.isSuccess())
            {
                log = result.success().metadata;
                applied.add(transformation);
            }
            else
            {
                failure = result.rejected().code + ": " + result.rejected().reason;
            }
            if (mode == CommitMode.APPLY_UNSEEN)
                return new ClusterCoordinator.Committed(seen, "timeout");
            seen = log;
            return new ClusterCoordinator.Committed(seen, failure);
        }

        @Override
        public long nowMillis()
        {
            return now.get();
        }

        @Override
        public long leaseMillis()
        {
            return lease;
        }

        @Override
        public long leaseSafetyMarginMillis()
        {
            return margin;
        }

        @Override
        public long commitTimeoutMillis()
        {
            return 30_000;
        }

        @Override
        public long cooldownAfterCommitMillis()
        {
            return cooldownAfterCommit;
        }

        @Override
        public long cooldownAfterRollbackMillis()
        {
            return cooldownAfterRollback;
        }

        @Override
        public UUID newActionId()
        {
            return FakeCluster.this.newActionId();
        }
    }

    public UUID newActionId()
    {
        return TimeUUID.atUnixMicrosWithLsbAsUUID(now.get() * 1000 + ids++, 9L);
    }
}
