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
package org.apache.cassandra.autonomic.elastic;

/**
 * Supplies capacity for scale-out (overview 8.3). The provider's job ends at "a Cassandra process configured to join
 * this cluster has been started"; the join itself is the new node's normal startup.
 *
 * <p>An implementation is named by {@code autonomic.scale_out.provider} and constructed once when the layer starts,
 * through a public constructor taking the {@code Map<String, String>} of {@code autonomic.scale_out.parameters}, or
 * a public no-argument constructor. The constructor, {@link #name} and {@link #capabilities} must not block or do
 * I/O: they run on the thread that starts the layer and {@code capabilities} is read once, then.
 *
 * <p>The other methods are only called on the actuator thread of the node that owns the scale-out claim, one at a
 * time, each bounded by {@code autonomic.scale_out.provider_call_timeout}: a call that overruns is interrupted and
 * counted as failed with an unknown outcome. After a change of owner another node repeats calls for the same
 * action, so every method must be idempotent as documented, across nodes.
 */
public interface ScaleOutProvider
{
    /** What the provider can do. Immutable. */
    final class Capabilities
    {
        public static final Capabilities NONE = new Capabilities(false, false, 0, 0, 0);

        public final boolean canAdd;
        public final boolean canRemove;
        public final int minNodes;
        /** No scale-out is proposed when the cluster already has this many members. */
        public final int maxNodes;
        public final long typicalProvisionMillis;

        public Capabilities(boolean canAdd, boolean canRemove, int minNodes, int maxNodes, long typicalProvisionMillis)
        {
            this.canAdd = canAdd;
            this.canRemove = canRemove;
            this.minNodes = minNodes;
            this.maxNodes = maxNodes;
            this.typicalProvisionMillis = typicalProvisionMillis;
        }

        @Override
        public String toString()
        {
            return "Capabilities{canAdd=" + canAdd + ", canRemove=" + canRemove + ", nodes=" + minNodes + ".." + maxNodes + '}';
        }
    }

    String name();

    Capabilities capabilities();

    /**
     * Asks for one node. Idempotent on {@code request.actionId}: a repeat, from any node, returns the ticket of the
     * first call and provisions nothing more. Must return quickly; provisioning is asynchronous.
     */
    ProvisionTicket requestNode(ScaleRequest request) throws ProviderException;

    ProvisionStatus status(ProvisionTicket ticket) throws ProviderException;

    /** Undo of {@link #requestNode} before the node has started to join. Idempotent; not an error if nothing is left. */
    void cancel(ProvisionTicket ticket) throws ProviderException;

    /** Releases the capacity of a node whose state is LEFT. Only called by operator-initiated flows; never by the layer. */
    void releaseNode(String hostId, String actionId) throws ProviderException;
}
