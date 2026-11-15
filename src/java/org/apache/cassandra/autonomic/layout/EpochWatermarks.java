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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

/**
 * Rule O1 (design 4.3): "every node has acknowledged epoch E". Every directory member that is not LEFT must answer
 * {@code TCM_CURRENT_EPOCH_REQ} with an epoch at or beyond the target. There is no quorum fallback, a node that does
 * not answer counts as not having acknowledged, and nothing here reports true on a timeout.
 *
 * <p>The verb is answered by the node's metadata handler whether or not the autonomic layer is enabled there (A2);
 * that a node which answers also routes by the registry is a T6.3 matter and is not established by this class.
 */
public final class EpochWatermarks
{
    private static final Logger logger = LoggerFactory.getLogger(EpochWatermarks.class);

    /** {@code layout.ack_poll_interval}; FC9 has not added the configuration block yet, so this is the design default. */
    public static final long ACK_POLL_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(5);

    private EpochWatermarks()
    {
    }

    /** The nodes whose acknowledgement O1 needs: every directory member that is not LEFT. */
    public static List<NodeId> required(ClusterMetadata metadata)
    {
        List<NodeId> nodes = new ArrayList<>();
        for (NodeId id : metadata.directory.peerIds())
            if (metadata.directory.peerState(id) != NodeState.LEFT)
                nodes.add(id);
        return nodes;
    }

    /**
     * Pure core of O1: true iff every required node has an answer at or beyond the target. A missing answer is false.
     * An empty required set is true (a directory always contains the asking node, so that does not arise in practice).
     */
    public static boolean satisfied(Collection<NodeId> required, Map<NodeId, Epoch> answers, Epoch target)
    {
        for (NodeId id : required)
        {
            Epoch answer = answers.get(id);
            if (answer == null || !answer.isEqualOrAfter(target))
                return false;
        }
        return true;
    }

    /**
     * One round of O1: asks every required node once and waits up to the RPC timeout for the answers. Returns false if
     * any node answered with a lower epoch, failed or did not answer in time. The calling node answers from its own
     * metadata. Blocks; not for a request thread.
     */
    public static boolean allAtLeast(Epoch target)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        Map<NodeId, Epoch> answers = new ConcurrentHashMap<>();
        NodeId self = metadata.myNodeId();
        List<NodeId> required = required(metadata);
        Map<NodeId, AsyncPromise<Void>> pending = new ConcurrentHashMap<>();
        for (NodeId id : required)
        {
            if (id.equals(self))
            {
                answers.put(id, metadata.epoch);
                continue;
            }
            InetAddressAndPort to = metadata.directory.endpoint(id);
            if (to == null)
                continue;
            AsyncPromise<Void> done = new AsyncPromise<>();
            pending.put(id, done);
            MessagingService.instance().sendWithCallback(Message.out(Verb.TCM_CURRENT_EPOCH_REQ, metadata.epoch), to,
                                                         new RequestCallbackWithFailure<Epoch>()
            {
                @Override
                public void onResponse(Message<Epoch> msg)
                {
                    answers.put(id, msg.payload);
                    done.trySuccess(null);
                }

                @Override
                public void onFailure(InetAddressAndPort from, RequestFailure failure)
                {
                    logger.debug("Epoch request to {} failed: {}", from, failure.reason);
                    done.trySuccess(null);
                }
            });
        }
        long deadline = Clock.Global.nanoTime() + DatabaseDescriptor.getRpcTimeout(TimeUnit.NANOSECONDS);
        for (AsyncPromise<Void> done : pending.values())
            done.awaitUninterruptibly(Math.max(0, deadline - Clock.Global.nanoTime()), TimeUnit.NANOSECONDS);
        boolean ok = satisfied(required, answers, target);
        if (!ok)
            logger.debug("Not every node has acknowledged epoch {}: answers {}", target, answers);
        return ok;
    }

    /**
     * Repeats {@link #allAtLeast} every {@link #ACK_POLL_INTERVAL_MILLIS} until it is true or {@code abandon} says to
     * stop; in that case it returns false. It never returns true because time has passed.
     */
    public static boolean await(Epoch target, BooleanSupplier abandon) throws InterruptedException
    {
        while (!abandon.getAsBoolean())
        {
            if (allAtLeast(target))
                return true;
            Thread.sleep(ACK_POLL_INTERVAL_MILLIS);
        }
        return false;
    }
}
