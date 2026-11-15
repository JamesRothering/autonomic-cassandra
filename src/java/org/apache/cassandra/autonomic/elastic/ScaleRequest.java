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

import java.util.Objects;
import java.util.UUID;

/** A request for one more node. {@link #actionId} is the idempotency key of {@link ScaleOutProvider#requestNode}. */
public final class ScaleRequest
{
    public final UUID actionId;
    public final String datacenter;
    public final String rack;
    /** For the provider's own audit; may differ between repeats of the same request. */
    public final String reason;

    public ScaleRequest(UUID actionId, String datacenter, String rack, String reason)
    {
        this.actionId = Objects.requireNonNull(actionId);
        this.datacenter = Objects.requireNonNull(datacenter);
        this.rack = Objects.requireNonNull(rack);
        this.reason = reason == null ? "" : reason;
    }

    @Override
    public String toString()
    {
        return "ScaleRequest{" + actionId + ' ' + datacenter + '/' + rack + '}';
    }
}
