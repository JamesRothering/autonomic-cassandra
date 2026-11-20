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

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/** The coordinator's environment on a running node: {@code ClusterMetadataService} and the failure detector. */
public class TcmEnvironment implements ClusterCoordinator.Environment
{
    private final LongSupplier clock;
    private final Supplier<AutonomicConfig> config;

    public TcmEnvironment(LongSupplier clock, Supplier<AutonomicConfig> config)
    {
        this.clock = clock;
        this.config = config;
    }

    @Override
    public ClusterMetadata metadata()
    {
        return ClusterMetadata.currentNullable();
    }

    @Override
    public NodeId self()
    {
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        return metadata == null ? null : metadata.myNodeId();
    }

    @Override
    public boolean isAlive(NodeId node)
    {
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        if (metadata == null)
            return false;
        InetAddressAndPort endpoint = metadata.directory.endpoint(node);
        return endpoint != null && FailureDetector.instance.isAlive(endpoint);
    }

    /**
     * Always the deadline-bounded overload: the default commit path retries for up to {@code cms_commit_timeout}.
     * Neither handler decides anything; the caller reads the ledger of the returned metadata.
     */
    @Override
    public ClusterCoordinator.Committed commit(Transformation transformation, long timeoutMillis)
    {
        String[] failure = new String[1];
        try
        {
            ClusterMetadata latest = ClusterMetadataService.instance().commit(transformation,
                                                                              metadata -> metadata,
                                                                              (code, message) -> {
                                                                                  failure[0] = code + ": " + message;
                                                                                  return ClusterMetadata.current();
                                                                              },
                                                                              nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis));
            return new ClusterCoordinator.Committed(latest, failure[0]);
        }
        catch (Throwable t)
        {
            // for example a timeout while waiting for the committed epoch to be enacted locally: outcome unknown
            if (t instanceof Error)
                throw (Error) t;
            return new ClusterCoordinator.Committed(ClusterMetadata.current(), t.toString());
        }
    }

    @Override
    public long nowMillis()
    {
        return clock.getAsLong();
    }

    @Override
    public long leaseMillis()
    {
        return TimeUnit.SECONDS.toMillis(config.get().coordination.lease.toSeconds());
    }

    @Override
    public long leaseSafetyMarginMillis()
    {
        return TimeUnit.SECONDS.toMillis(config.get().coordination.lease_safety_margin.toSeconds());
    }

    @Override
    public long commitTimeoutMillis()
    {
        // half of action_step_timeout: a step whose commit gives up still ends well before the stall check pauses the layer
        return config.get().safety.action_step_timeout.toMilliseconds() / 2;
    }

    @Override
    public long cooldownAfterCommitMillis()
    {
        return config.get().safety.cooldown_after_commit.toMilliseconds();
    }

    @Override
    public long cooldownAfterRollbackMillis()
    {
        return config.get().safety.cooldown_after_rollback.toMilliseconds();
    }

    @Override
    public UUID newActionId()
    {
        return TimeUUID.Generator.nextTimeUUID().asUUID();
    }
}
